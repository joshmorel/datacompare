# TODO: Document better
# TODO: Add setkey
# TODO: Add option to load data from dataframe
# TODO: Add option to load left from txt

import pandas as pd
import pyodbc
import datetime
import re 
import numpy as np
import codecs


#Helper functions to get all sqls from a directory
def get_file_paths(path):
    '''Stores all file paths for list of files in specified directory path as dictionary'''
    import os
    file_paths = {}
    file_list = os.listdir(path)
    for file in file_list:
        file_name = file.rsplit(".",1)
        file_paths[file_name[0]] = path + file
    return file_paths

## Way with class

class DataComp(object):
    '''Initializes with the left object'''
    def __init__(self, cnxn_path,left_cnxn_name,left_script_path,datetofrom=("2015-04-01","2015-04-02")):
        self.cnxn_path = cnxn_path
        self.left_cnxn_name = left_cnxn_name
        self.left_script_path = left_script_path
        self.datetofrom = datetofrom
        self.left_sql = self._load_sql_script(path=self.left_script_path,datetofrom=self.datetofrom)
        self.left_data = self._populate_dataframe_from_sql(cnxn_path = self.cnxn_path,cnxn_name = self.left_cnxn_name, sql_script = self.left_sql)
        self.right_cnxn_name = None
        self.right_script_path = None
        self.right_sql = None
        self.right_data = None

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
        f = open(path,'rb')
        sql_script_bytes = f.read()
        f.close()
        sql_script_bytes = sql_script_bytes.replace(codecs.BOM_UTF8,b'')
        sql_script = sql_script_bytes.decode(encoding='utf-8')
        # Find all occurrences of a date in YYYY-MM-DD format
        ms = list(re.finditer('\\d{4}-\\d{2}-\\d{2}',sql_script))
        # For each pair of date occurrences, insert in first date of tuple then second
        for i, m in enumerate(ms):
            sql_script = sql_script[:m.start()] + datetofrom[i%2] + sql_script[m.end():]    
        return sql_script        

    def _populate_dataframe_from_sql(self,cnxn_path,cnxn_name,sql_script):
        '''With a valid SQL script and connection, loads SQL data into dataframe'''
        cnxns = self._get_cnxn_strings(path = self.cnxn_path)
        cnxn_string = cnxns[cnxn_name]
        cnxn = pyodbc.connect(cnxn_string)
        try:
            df = pd.read_sql(sql_script,cnxn)
            assert df.shape[0] > 0, "The SQL executed does not return any rows, check " + sql_script
            ## Converting all elements to string with no null values is easier to work with, assume data type validation is not an issue
            df = df.applymap(self._convert_to_str)
            ## Basic assumption is left most column is primary key for comparison. This can be set later
            df.insert(0,"-PK",df.iloc[:,0])
            df = df.sort_values(by = ["-PK"])
            return df
        except TypeError:
            raise AssertionError("SQL script loaded but could not return a dataframe, check to make sure a result-set is actually returned")
    
    def _populate_dataframe_from_txt(self,cnxn_path,cnxn_name):
        '''With a valid path to text file'''
        cnxns = self._get_cnxn_strings(path = self.cnxn_path)
        cnxn_string = cnxns[cnxn_name]
        df = pd.read_table(cnxn_string)
        ## Converting all elements to string with no null values is easier to work with, assume data type validation is not an issue
        df = df.applymap(self._convert_to_str)
        ## Basic assumption is left most column is primary key for comparison. This can be set later
        df.insert(0,"-PK",df.iloc[:,0])
        df = df.sort_values(by = ["-PK"])
        return df    

    def _convert_to_str(self,obj):
        '''to be used to convert all objects to strings and any None or NaN to blank string for ease of comparison'''
        if pd.isnull(obj):
            return ''
        elif isinstance(obj,bool) or isinstance(obj,np.bool_):
            return str(int(obj))
        elif isinstance(obj,float):
            return str(int(round(obj,0)))
        else:
            return str(obj)
            
    def _get_cnxn_strings(self,path):
        '''Gets connection strings and stores in dictionary, assumes first row is headers, 
        first columns is name, second column is connection string'''
        cnxn_dict = {}
        with open(path, 'r') as f:
            next(f)
            for line in f:
                splitLine = line.split(sep='\t')
                cnxn_dict[splitLine[0]] = splitLine[1]
        return cnxn_dict
        
    def add_right_data(self,right_cnxn_name,right_script_path,source = "SQL",DataFrame = None):
        self.right_cnxn_name = right_cnxn_name
        self.right_script_path = right_script_path
        if source == "SQL":
            self.right_sql = self._load_sql_script(path=self.right_script_path,datetofrom=self.datetofrom)
            self.right_data = self._populate_dataframe_from_sql(cnxn_path = self.cnxn_path,cnxn_name = self.right_cnxn_name, sql_script = self.right_sql)
        elif source == "txt":
            self.right_data = self._populate_dataframe_from_txt(cnxn_path = self.cnxn_path,cnxn_name = self.right_cnxn_name)
        else:
            df = DataFrame
            df.insert(0,"-PK",df.iloc[:,0])
            df = df.sort_values(by = ["-PK"])
            self.right_data = df

    def compare_data(self):
        if self.right_data is None:
            raise ValueError('right data has not yet been added, use add_right_data function')
           
        self._check_duplicate_pk()
        ## Only want to compare those columns that match, but still report on those columns in one dataset but not in others
        self._subset_on_common()
        ## Compare rows in two sets based on common key and report on differences
        self._compare_row_counts()
        ## Compare values for matched rows
        self._compare_values()
        d = {}
        d["left_data"] = self.left_data
        d["right_data"] = self.right_data
        d["left_not_right_data"] = self.left_not_right_data
        d["right_not_left_data"] = self.right_not_left_data
        d["diff_values"] = self.diff_values
        return d
        
    def _check_duplicate_pk(self):
        left_pk_count = pd.value_counts(self.left_data["-PK"])
        left_pk_count = left_pk_count[left_pk_count>1]
        assert left_pk_count.shape[0] == 0, "PK of left data set is not unique, cannot complete comparison"
        right_pk_count = pd.value_counts(self.right_data["-PK"])
        right_pk_count = right_pk_count[right_pk_count>1]
        assert right_pk_count.shape[0] == 0, "PK of right data set is not unique, cannot complete comparison"
       
   
    def _subset_on_common(self):
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
            
        self.right_data = self.right_data[common_cols]
        self.left_data = self.left_data[common_cols]
        
    
    def _compare_row_counts(self):
        merged_data = pd.merge(self.left_data, self.right_data, how='outer', on="-PK")
        right_not_left_pks = merged_data[pd.isnull(merged_data.iloc[:,1])][["-PK"]]
        left_not_right_pks = merged_data[pd.isnull(merged_data.iloc[:,-1])][["-PK"]]
        common_pks = pd.merge(self.left_data, self.right_data, how='inner', on="-PK")[["-PK"]]
        
        self.left_not_right_data = pd.merge(self.left_data, left_not_right_pks, how='inner', on="-PK")
        self.right_not_left_data = pd.merge(self.right_data, right_not_left_pks, how='inner', on="-PK")
        
        self.left_data = pd.merge(self.left_data, common_pks, how='inner', on="-PK")
        self.right_data = pd.merge(self.right_data, common_pks, how='inner', on="-PK")
        
        print("Rows matched in both sets\n",str(self.left_data.shape[0]),"\n\nRows in left set not in right\n",\
            str(self.left_not_right_data.shape[0]),"\n\nRows in right set not in left\n",str(self.right_not_left_data.shape[0]))        

    def _compare_values(self):

        #Return the position of the values which are not equal
        diff_array = np.where(self.left_data.values != self.right_data.values) 
        
        if len(diff_array[0]) == 0:
            self.diff_values = None
        else:
            rows_with_diff = diff_array[0]
            cols_with_diff = np.unique(diff_array[1])        
             
            ## Pass only columns and rows which have at least one difference
            diff_panel = pd.Panel({"left":self.left_data.iloc[rows_with_diff,cols_with_diff],\
                "right":self.right_data.iloc[rows_with_diff,cols_with_diff]})
            diff_values = diff_panel.apply(self._report_diff, axis=0)
            print("For matched rows,",str(len(rows_with_diff)),"of the rows have at least one different value among",str(len(cols_with_diff)),"columns flagged with differences")
            
            ## What to see all columns for contextual info for rows with at least some differences, but columns with differences at front
            diff_rows = self.left_data.iloc[rows_with_diff,:]
            diff_rows = diff_rows[diff_rows.columns.delete(cols_with_diff)]
            diff_values = pd.concat([diff_values,diff_rows],axis=1)
            self.diff_values = diff_values

            
    def _report_diff(self,x):
        return x[0] if x[0] == x[1] else '{} | {}'.format(*x)
