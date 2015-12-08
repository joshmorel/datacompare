# For efficiency, working with list of SQL files which should be stored in one folder per dataset, per source
def get_file_paths(path):
    '''Stores all file paths for list of files in specified directory path as dictionary'''
    import os
    file_paths = {}
    file_list = os.listdir(path)
    for file in file_list:
        file_name = file.rsplit(".",1)
        file_paths[file_name[0]] = path + file
    return file_paths

hlabSQLs = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/HLAB/Validation/HLAB SQL/")
edwSQLs = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/HLAB/Validation/EDW SQL/")
dmrtSQLs = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/HLAB/Validation/DMRT SQL/")
bbSQL = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/BloodBank/Validation/BloodBank SQL/")


# For highest ease of troubleshooting, return un-matched values delimited by pipe
def report_diff(x):
    return x[0] if x[0] == x[1] else '{} | {}'.format(*x)

def replace_sql_dates(sql_script,datetofrom = ("2015-04-01","2015-04-07")):
    '''Takes a SQL script and replaces all date occurrences in YYYY-MM-DD format 
    with provided date to and from pair'''
    import re
    import datetime
    if not isinstance(sql_script,str):
        raise TypeError('sql script must be string')
    if not isinstance(datetofrom,tuple) or not len(datetofrom) == 2:
        raise TypeError('dates to and from must be in tuple of length 2')
    for d in datetofrom:
        try:
            datetime.datetime.strptime(d,'%Y-%m-%d')
        except ValueError:
            raise ValueError("incorrect date format, should be YYYY-MM-DD")
    sql_script = sql_script
    # Find all occurrences of a date in YYYY-MM-DD format
    ms = list(re.finditer('\\d{4}-\\d{2}-\\d{2}',sql_script))
    # For each pair of date occurrences, insert in first date of tuple then second
    for i, m in enumerate(ms):
        sql_script = sql_script[:m.start()] + datetofrom[i%2] + sql_script[m.end():]    
    return sql_script


def convert_to_str(obj):
    '''to be used to convert all objects to strings and any None or NaN to blank string for ease of comparison'''
    import pandas as pd
    import numpy as np
    if pd.isnull(obj):
        return ''
    elif isinstance(obj,bool) or isinstance(obj,np.bool_):
        return str(int(obj))
    elif isinstance(obj,float):
        return str(int(round(obj,0)))
    else:
        return str(obj)

def compare_result_sets(cnxn_path,db1_name,db1_sql_path,db2_name,db2_sql_path,datetofrom=("2015-04-01","2015-04-07")):
    import pandas as pd
    from pandas.util.testing import assert_frame_equal
    import pyodbc
    dataframes = {}
    cnxn = pd.read_table(cnxn_path)
    db1_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db1_name])[0]
    db1_cnxn = pyodbc.connect(db1_cnxn_string)
    # Replace each pair of date occurrences with datetofrom, where first tuple value is from date, second is to date
    db1_sql = open(db1_sql_path,'r').read()
    db1_sql = replace_sql_dates(db1_sql,datetofrom)
   
    dataframes[db1_name] = pd.read_sql(db1_sql,db1_cnxn)
    dataframes[db1_name] = dataframes[db1_name].applymap(convert_to_str)

    db2_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db2_name])[0]
    db2_cnxn = pyodbc.connect(db2_cnxn_string)

    # Replace each pair of date occurrences with datetofrom, where first tuple value is from date, second is to date
    db2_sql = open(db2_sql_path,'r').read()
    db2_sql  = replace_sql_dates(db2_sql ,datetofrom)

    dataframes[db2_name] = pd.read_sql(db2_sql,db2_cnxn)
    dataframes[db2_name] = dataframes[db2_name].applymap(convert_to_str)

    if len(dataframes[db1_name]) == len(dataframes[db2_name]):
        print("row counts match")
        if len(dataframes[db1_name].columns) == len(dataframes[db2_name].columns):
            print("column counts match")
            try:
                assert_frame_equal(dataframes[db1_name],dataframes[db2_name])
                print('values match')
                return dataframes
            except AssertionError as inst:
                # Assertion error provides sufficient information if column names do not match
                if inst.args[0][0:17] == "DataFrame.columns":
                    print(inst.args)
                    return {db1_name: dataframes[db1_name].columns, db2_name: dataframes[db2_name].columns}
                # More information requried to troubleshoot at row level if specific values do not match
                else:
                    print("values do not match")
                    rows_with_diff = (dataframes[db1_name] != dataframes[db2_name]).max(axis=1)
                    df_panel = pd.Panel(dataframes)
                    diff = df_panel.apply(report_diff, axis=0)
                    # In troubleshooting, only need to review those with different columns
                    dataframes["diff"] = diff[rows_with_diff]
                    return dataframes
        else:
            print("number of columns do not match")
            cols = {}
            cols[db1_name] = dataframes[db1_name].columns.tolist()
            cols[db2_name] = dataframes[db2_name].columns.tolist()            
            return cols
            
    else:
        dataframes["mismatched"] = pd.merge(dataframes[db1_name], dataframes[db2_name], how='outer', on="PK")
        print("mismatched rows")
        return dataframes
