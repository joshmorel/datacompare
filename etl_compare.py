# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import pyodbc
import sys
sys.version_info

# Compares values in two panels and if not matched, returns to values separated by pipe
def report_diff(x):
    return x[0] if x[0] == x[1] else '{} | {}'.format(*x)

def compare_result_sets(cnxn_path,db1_name,db1_sql_path,db2_name,db2_sql_path,datetofrom=("2015-04-01","2015-04-07")):
    import pandas as pd
    import pyodbc
    import numpy as np
    import re
    dataframes = {}
    cnxn = pd.read_table(cnxn_path)
    db1_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db1_name])[0]
    db1_cnxn = pyodbc.connect(db1_cnxn_string)

    # Replace each pair of date occurrences with datetofrom, where first tuple value is from date, second is to date
    db1_sql = open(db1_sql_path,'r').read()
    ms = list(re.finditer('\\d{4}[- ]*\\d{2}[- ]*\\d{2}',db1_sql))
    for i, m in enumerate(ms):
        db1_sql = db1_sql[:m.start()] + datetofrom[i%2] + db1_sql[m.end():]
    
    dataframes[db1_name] = pd.read_sql(db1_sql,db1_cnxn)
    db1_fillna_tocompare = dataframes[db1_name].fillna(method="ffill").fillna(method="bfill")
    db2_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db2_name])[0]
    db2_cnxn = pyodbc.connect(db2_cnxn_string)

    # Replace each pair of date occurrences with datetofrom, where first tuple value is from date, second is to date
    db2_sql = open(db2_sql_path,'r').read()
    ms = list(re.finditer('\\d{4}[- ]*\\d{2}[- ]*\\d{2}',db2_sql ))
    for i, m in enumerate(ms):
        db2_sql = db2_sql[:m.start()] + datetofrom[i%2] + db2_sql[m.end():]

    dataframes[db2_name] = pd.read_sql(db2_sql,db2_cnxn)
    db2_fillna_tocompare = dataframes[db2_name].fillna(method="ffill").fillna(method="bfill")
    if len(dataframes[db1_name]) == len(dataframes[db2_name]):
        print("row counts match")
        if len(dataframes[db1_name].columns) == len(dataframes[db2_name].columns):
            print("column counts match")
            if np.mean(dataframes[db1_name].columns == dataframes[db2_name].columns) == 1:
                print("columns match")

                if (db1_fillna_tocompare == db2_fillna_tocompare).mean().mean() == 1:
                    print("values match")
                    return dataframes
                else:
                    print("values do not match")
                    hasdiffcol = (dataframes[db1_name] != dataframes[db2_name]).max(axis=1)
                    df_panel = pd.Panel(dataframes)
                    diff = df_panel.apply(report_diff, axis=0)
                    dataframes["diff"] = diff[hasdiffcol]
                    return dataframes
            else:
                print("column names do not match")
                cols = {}
                cols[db1_name] = dataframes[db1_name].columns.tolist()
                cols[db2_name] = dataframes[db2_name].columns.tolist()            
                return cols
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
