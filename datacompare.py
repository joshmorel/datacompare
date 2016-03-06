# -*- coding: utf-8 -*-
import pandas as pd
import pyodbc
import datetime
import re 
import numpy as np
import codecs
import configparser as cp


def get_file_paths(path):
    """ Creates dictionary of paths to files in a specific directory where keys are the file name (before first period).
    Paths can then be passed to DataComp initialization like sqls["left"]

    Parameters
    ----------
    path : Directory path where scripts are stored 
    
    Returns
    -------
    get_file_paths : dict
        Dictionary of file paths in directory with file names (before first period) 
        as keys.
    
    Examples
    --------
    >>> sqls = "C:/mysqlfiles/"
    """   
    import os
    file_paths = {}
    file_list = os.listdir(path)
    for file in file_list:
        file_name = file.rsplit(".",1)
        file_paths[file_name[0]] = path + file
    return file_paths

## DataComp class

class DataComp(object):
    """ Container for comparison of two of data-sets a left and right, from SQL, txt or DataFrame.
    On initialization loads left data from either sql or txt. To complete comparison use add_right_data and compare_data methods.

    Parameters
    ----------
    cnxn_path : Directory path to an ini file with connection information. See cnxn_example.ini in github repo for example.
    left_cnxn_name : The connection name of the left data set in the ini file
    left_script_path: Directory path to SQL script. If None, then expected is txt file and therefore no script
        default None
    datetofrom: Two-element tuple containing date from and date to, to insert into SQL script. Will be ignored for txt source. Must be YYYY-MM-DD format.
        default ("2015-04-01","2015-04-02")
        
    Examples
    --------
    >>> dc1 = DataComp("C:/cnxn.ini","sales",sqls["left"],("2015-04-01","2015-05-01"))
    """

    def __init__(self, cnxn_path,left_cnxn_name,left_script_path = None,datetofrom=("2015-04-01","2015-04-02"),sql_timeout = 30,**kwargs):
        """Gets connection information from ini file and loads left data into class attribute"""
        self.cnxn_path = cnxn_path
        self.sql_timeout = sql_timeout
        self.left_cnxn_name = left_cnxn_name
        self.left_script_path = left_script_path
        self.datetofrom = datetofrom
        self.left_data, self.left_sql = self._get_data(cnxn_name = self.left_cnxn_name, script_path = self.left_script_path,**kwargs)
        self.right_cnxn_name = None
        self.right_script_path = None
        self.right_sql = None
        self.right_data = None

    def add_right_data(self,right_cnxn_name,right_script_path = None, DataFrame = None,**kwargs):
        """Add right data for comparison, unlike left data, right can be from DataFrame.
        For DataFrame set right_cnxn_name = None and set DataFrame to a named DataFrame object in scope

        Parameters
        ----------
        right_cnxn_name : The connection name of the right data set in the ini file
            set right_cnxn_name = None for DataFrame
        right_script_path: Directory path to SQL script. If None, then expected is txt file and therefore no script
            default None
        DataFrame: A pandas DataFrame, can be used if right_cnxn_name = None

        Examples
        --------
        >>> dc1.add_right_data(right_cnxn_name="sales_sql",right_script_path = sqls["sales"])
        >>> dc1.add_right_data(right_cnxn_name="sales_txt")
        >>> dc1.add_right_data(right_cnxn_name=None,DataFrame = mydf)

        """
        self.right_cnxn_name = right_cnxn_name
        self.right_script_path = right_script_path
        if right_cnxn_name is None:
            self.right_data = self._index_data(df=DataFrame)
        else:
            self.right_data, self.right_sql = self._get_data(cnxn_name = self.right_cnxn_name, script_path = self.right_script_path,**kwargs)

    def _get_data(self,cnxn_name,script_path,**kwargs):
        """Gets data from either text source or SQL source based on what is stored in ini file for specified name
        Also returns the actual sql for inspection, troubleshooting
        """
        if script_path is None:
            expected_type = 'txt'
        else: 
            expected_type = 'sql'
        cnxn_info = self._get_cnxn_info(cnxn_name = cnxn_name,expected_type = expected_type)

        if script_path is None:
            sql_script = None
            data = self._populate_dataframe_from_txt(txt_path = cnxn_info[1],**kwargs)
        else:
            sql_script = self._load_sql_script(path=script_path,datetofrom=self.datetofrom)
            data = self._populate_dataframe_from_sql(cnxn_string = cnxn_info[1],cnxn_name = cnxn_name, sql_script = sql_script)
        return data, sql_script

    def _get_cnxn_info(self,cnxn_name,expected_type):
        """Gets sql connection string or txt path from ini file"""
        parser = cp.ConfigParser()
        parser.read(self.cnxn_path)
        assert parser.has_section(cnxn_name), "The cnxn_name provided is not in the ini file specified." + self.cnxn_path
        cnxn_type = parser.get(cnxn_name,'cnxn_type')
        assert cnxn_type == expected_type, "The expected type, " + expected_type + ", is not the same as the cnxn_type, " + cnxn_type +\
            ".\nExpected type is txt if script_path is None, otherwise sql"
        cnxn_string =  parser.get(cnxn_name,'cnxn_string')
        return (cnxn_type,cnxn_string)

    def _load_sql_script(self,path,datetofrom=("2015-04-01","2015-04-02")):
        '''Load SQL and changes all date occurrence pairs to to and from in datetofrom parameter'''
        if not isinstance(path,str):
            raise TypeError('sql path must be string')
        if not isinstance(datetofrom,tuple) or not len(datetofrom) == 2:
            raise TypeError('dates to and from must be in tuple of length 2')
        for d in datetofrom:
            try:
                datetime.datetime.strptime(d,'%Y-%m-%d')
            except ValueError:
                raise ValueError("incorrect date format, should be YYYY-MM-DD")
        # SQL scripts worked with in SSMS may be saved with utf-8 BOM mark which must be replaced with empty string for pyodbc execution
        with open(path,'rb') as f:
            sql_script_bytes = f.read()
        sql_script_bytes = sql_script_bytes.replace(codecs.BOM_UTF8,b'')
        sql_script = sql_script_bytes.decode(encoding='utf-8')
        # Find all occurrences of a date in YYYY-MM-DD format
        ms = list(re.finditer('\\d{4}-\\d{2}-\\d{2}',sql_script))
        # For each pair of date occurrences, insert in first date of tuple then second
        for i, m in enumerate(ms):
            sql_script = sql_script[:m.start()] + datetofrom[i%2] + sql_script[m.end():]    
        return sql_script        

    def _populate_dataframe_from_sql(self,cnxn_string,cnxn_name,sql_script):
        '''With a valid SQL script and connection, loads SQL data into dataframe'''
        with pyodbc.connect(cnxn_string) as cnxn:
            try:
                cnxn.timeout = self.sql_timeout
                df = pd.read_sql(sql_script,cnxn)
                assert df.shape[0] > 0, "The SQL executed does not return any rows, check " + sql_script
                ## Basic assumption is left most column is primary key for comparison. This can be set later
                df = self._index_data(df)
                return df
            except TypeError:
                raise AssertionError("SQL script loaded but could not return a dataframe, check to make sure a result-set is actually returned")
    
    def _populate_dataframe_from_txt(self,txt_path,**kwargs):
        """Creates dataframe from txt file, **kwargs for read_table keyword arguments such as non-tab sep"""
        df = pd.read_table(txt_path,**kwargs)
        df = self._index_data(df)
        return df    
           
    def compare_data(self):
        """ Completely compare the loaded left and right data sets.
        Displaying both testing message and returning six item dictionary for exploration including:
            left_data: The left data loaded, but subset to include only shared columns with right
            right_data: The right data loaded, but subset to include only shared columns with left
            left_not_right_data: If any, rows in left data not found in right based on shared primary key (default is left most in each data set, unless set_key method used)
            right_not_left_data: If any, rows in right data not found in left based on shared primary key
            diff_summary: Summary level difference at a data-set/column level
            diff_values: Matched rows where at least one value is different, columns with at least one different are moved to left of PK, with different values delimited by pipe
            
        Returns
        -------
        compare_data : dict
            Dictionary of data frames described above
            
        Examples
        --------
        >>> dict_data = dc1.compare_data()
        >>> dict_data["diff_summary"]
                         LeftRowCount  RightRowCount  CommonRowCount  DiffValCount  \
             -PK                10            11             9           0   
             NumericCol         10            11             9           3   
             StringCol          10            11             9           1   
             
                         LeftSums  RightSums  TotalDiff  TotalPctDiff  LeftMeans  RightMeans  
             -PK            NaN        NaN        NaN           NaN        NaN         NaN  
             NumericCol     470        410        60            0.1463414  47.0       41.0
             StringCol      NaN        NaN        NaN           NaN        NaN         NaN                
        
        """
        
        ## Can only compare once data has been added to right
        assert self.right_data is not None, "right data has not yet been added, use add_right_data function"

        ## Can only compare data where each set's PK is unique
        assert self.left_data.index.duplicated().sum() == 0, "left PK is not unique, use set_key method on unique columns. \n Example: " + \
            str(self.left_data.index[self.left_data.index.duplicated()].values[0])

        assert self.right_data.index.duplicated().sum() == 0, "right PK is not unique, use set_key method on unique columns. \n Example: " + \
            str(self.right_data.index[self.right_data.index.duplicated()].values[0])

        ## Only want to compare those columns that match while also reporting on those columns in one dataset but not in others
        self._subset_on_common()

        ## Once the above checks are complete the difference summary table can start to be built
        
        self.diff_summary = pd.DataFrame(data= {"LeftRowCount" : self.left_data.shape[0], \
            "RightRowCount" : self.right_data.shape[0]},index=self.left_data.columns)

        self.diff_summary["CommonRowCount"] = np.nan
        self.diff_summary["DiffValCount"] = np.nan
        self.diff_summary["LeftSums"] = np.nan
        self.diff_summary["RightSums"] = np.nan
        self.diff_summary["TotalDiff"] = np.nan
        self.diff_summary["TotalPctDiff"] = np.nan
        self.diff_summary["LeftMeans"] = np.nan
        self.diff_summary["RightMeans"] = np.nan

        ## Compare rows in two sets based on common key and report on differences
        self._compare_row_counts()
        ## Compare values for matched rows
        self._compare_values()
        ## Return data for inspection as dictionary as this can be easily explored in spyder IDE (and likely others...)
        d = {}
        
        ## Convert all numeric nulls to -99999 to avoid spyder issue #2991 from causing warning issues
        d["left_data"] = self.left_data.applymap(self._convert_for_display)
        d["right_data"] = self.right_data.applymap(self._convert_for_display)
        d["left_not_right_data"] = self.left_not_right_data.applymap(self._convert_for_display)
        d["right_not_left_data"] = self.right_not_left_data.applymap(self._convert_for_display)
        d["diff_summary"] = self.diff_summary.applymap(self._convert_for_display)
        d["diff_values"] = self.diff_values
        return d
        
    def _subset_on_common(self):
        """ Find common columns in two data sets, and subset each set on those columns"""
        def common_elements(list1, list2):
            return [element for element in list1 if element in list2]
        def excluded_elements(list1, list2):
            return [element for element in list1 if element not in list2]
        
        left_cols = self.left_data.columns.tolist()
        right_cols = self.right_data.columns.tolist()   
        common_cols = common_elements(right_cols,left_cols)
        common_cols.sort()
        
        left_cols_not_right = excluded_elements(left_cols,common_cols)
        right_cols_not_left = excluded_elements(right_cols,common_cols)
      
        print("Common columns from both data compared\n",common_cols,"\n\nColumns in left but not in right\n",\
            left_cols_not_right,"\n\nColumns in right but not in left\n",right_cols_not_left)
            
        assert len(common_cols) >= 2, "Require at least one common column in data sets for comparison"

        self.common_cols = common_cols
        
    def _compare_row_counts(self):
        """ Find rows not common to both sets, report on them, and store these findings"""

        pk_left = self.left_data.index
        pk_right = self.right_data.index
        
        self.left_not_right_data = self.left_data[~pk_left.isin(pk_right)]
        self.right_not_left_data = self.right_data[~pk_right.isin(pk_left)]
        
        common_pks = pk_left[pk_left.isin(pk_right)]
        
        self.diff_summary["CommonRowCount"] = common_pks.shape[0]

        #For numeric columns except PK, want to calculate summary statistics before subsetting datasets on common PKs
        for col in self.left_data.columns:
            if self.left_data[col].dtype in [np.int32,np.int64,np.float32,np.float64] and col != "-PK":
                self.diff_summary.loc[col,"LeftSums"] = self.left_data[col].sum()
                self.diff_summary.loc[col,"RightSums"] = self.right_data[col].sum()
                self.diff_summary.loc[col,"TotalDiff"] = self.diff_summary.loc[col,"LeftSums"] - self.diff_summary.loc[col,"RightSums"]
                if self.diff_summary.loc[col,"LeftSums"] != 0:
                    self.diff_summary.loc[col,"TotalPctDiff"] = self.diff_summary.loc[col,"TotalDiff"] / self.diff_summary.loc[col,"LeftSums"]
                self.diff_summary.loc[col,"LeftMeans"] = self.left_data[col].mean()
                self.diff_summary.loc[col,"RightMeans"] = self.right_data[col].mean()
        
        self.common_pks = common_pks
        
        print("Rows matched in both sets\n",str(self.diff_summary["CommonRowCount"]),"\n\nRows in left set not in right\n",\
            str(self.left_not_right_data.shape[0]),"\n\nRows in right set not in left\n",str(self.right_not_left_data.shape[0]))        
            
    def _compare_values(self):
        """ Compare the values of those rows commont to both sets"""

        ## Can only compare common cols and row pks        
        left_data_comp = self.left_data.loc[self.common_pks][self.common_cols]
        right_data_comp = self.right_data.loc[self.common_pks][self.common_cols]

        ## First convert all elements to string, including conversion of null values as easier to work with, assume data type validation is not a concern
        
        left_data_str = left_data_comp.applymap(self._convert_to_str)
        right_data_str = right_data_comp.applymap(self._convert_to_str)

        ## Return the position of the values which are not equal
        diff_array = np.where(left_data_str.values != right_data_str.values)

        ## Count up the number of difference by column using positional index
        colsdiff = diff_array[1]
        colsdiff_counts = np.bincount(colsdiff)
        
        ## Need to pad val diff summary to total length of columns
        ## as colsdiff_counts will only be as long as the position of final column with any errors
        
        colsdiff_length = self.diff_summary.shape[0]
        colsdiff_counts = np.lib.pad(colsdiff_counts, (0,colsdiff_length-len(colsdiff_counts)), 'constant', constant_values=(0))
        self.diff_summary["DiffValCount"] = colsdiff_counts
        
        if len(diff_array[0]) == 0:
            self.diff_values = None
        else:
            rows_with_diff = np.unique(diff_array[0])
            cols_with_diff = np.unique(diff_array[1])        
             
            ## Pass only columns and rows which have at least one difference
            diff_panel = pd.Panel({"left":left_data_str.iloc[rows_with_diff,cols_with_diff],\
                "right":right_data_str.iloc[rows_with_diff,cols_with_diff]})
            diff_values = diff_panel.apply(self._report_diff, axis=0)
            print("For matched rows,",str(len(rows_with_diff)),"of the rows have at least one different value among",str(len(cols_with_diff)),"columns flagged with differences")
            
            ## What to see all columns for contextual info for rows with at least some differences, but columns with differences at front
            diff_rows = left_data_str.iloc[rows_with_diff,:]
            diff_rows = diff_rows[diff_rows.columns.delete(cols_with_diff)]
            diff_values = pd.concat([diff_values,diff_rows],axis=1)
            self.diff_values = diff_values

    def _convert_to_str(self,obj):
        """to be used to convert all objects to strings and any None or NaN to blank string for ease of comparison"""
        if pd.isnull(obj):
            return ''
        elif isinstance(obj,bool) or isinstance(obj,np.bool_):
            return str(int(obj))
        elif isinstance(obj,float):
            return str(int(round(obj,0)))
        else:
            return str(obj)

    def _convert_for_display(self,obj):
        """Convert nulls for display prior to loading into dictionary due to spyder issue
        -99999 is null until this issue is solved"""
        if pd.isnull(obj):
            if isinstance(obj,float):
                return -99999.0
            elif isinstance(obj,int):
                return -99999
            else:
                return obj
        else:
            return obj
    
    def _report_diff(self,x):
        """Returns difference from two sets with left and right values delimited by pipe"""
        return x[0] if x[0] == x[1] else '{} | {}'.format(*x)
    
    def set_key(self,side="left",col = "-PK"):
        """ Sets the primary key on both, left or right data set to another named column for comparison
    
        Parameters
        ----------
        side : the data set to set key on, either "left", "right" or "both"
            default "left"
        col : the column name to set as a key
        Examples
        --------
        >>> dc1.set_key("both","Employee")
        """   
        if side == "both":
            assert self.right_data is not None, "Right data set must exist"
            assert col in self.left_data.columns, "Column specified as key must be column in left data set, check spelling and case"
            assert col in self.right_data.columns, "Column specified as key must be column in right data set, confirm right exists, check spelling and case"

            self.left_data = self._index_data(df = self.left_data,col = col)
            self.right_data = self._index_data(df = self.right_data,col = col)

        elif side == "left":
            assert col in self.left_data.columns, "Column specified as key must be column in left data set, check spelling and case"
            self.left_data = self._index_data(df = self.left_data,col = col)
        elif side == "right":
            assert self.right_data is not None, "Right data set must exist"
            assert col in self.right_data.columns, "Column specified as key must be column in right data set, confirm right exists, or check spelling and case"
            self.right_data = self._index_data(df = self.right_data,col = col)
        else:
            raise AssertionError('side must be both, left or right')
            
    def _index_data(self,df,col=None):
        """PK for comparison is left-most (position 0) by default unless specified (e.g. via the set_key function)"""
        if col is None:
            df.insert(0,"-PK",df.iloc[:,0])
        else:
            df["-PK"] = df[col]
        df = df.sort_values(by = ["-PK"])
        df.index = df["-PK"]
        return df

        