#takes list of two values, compares and if different returns values concatenated with pipe 
def report_diff(x):
    return x[0] if x[0] == x[1] else '{} | {}'.format(*x)

#requires connection text file (cnxn_path) with two columns:
#first is name (referenced in db1_name, db2_name
#second is connection string which is likely dependent on local drivers

#db1_sql_path and db2_sql_path contains sql script to extract data to compare
#must be same columns and types 
def get_connect(cnxn_path,db1_name,db1_sql_path,db2_name,db2_sql_path):
    import pyodbc
    import pandas
    dataframes = {}
    cnxn = pandas.read_table(cnxn_path)
    db1_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db1_name])[0]
    db1_cnxn = pyodbc.connect(db1_cnxn_string)
    db1_sql = open(db1_sql_path,'r').read()
    dataframes[db1_name] = pandas.read_sql(db1_sql,db1_cnxn)
    db1_fillna_tocompare = dataframes[db1_name].fillna(method="ffill").fillna(method="bfill")
    db2_cnxn_string = list(cnxn["cnxn_string"][cnxn["cnxn_name"] == db2_name])[0]
    db2_cnxn = pyodbc.connect(db2_cnxn_string)
    db2_sql = open(db2_sql_path,'r').read()
    dataframes[db2_name] = pandas.read_sql(db2_sql,db2_cnxn)
    db2_fillna_tocompare = dataframes[db2_name].fillna(method="ffill").fillna(method="bfill")
    if len(dataframes[db1_name]) == len(dataframes[db2_name]):
        print("row counts match")
        if (db1_fillna_tocompare == db2_fillna_tocompare).mean().mean() == 1:
            print("values match")
            return dataframes
        else:
            print("values do not match")
            hasdiffcol = (dataframes[db1_name] != dataframes[db2_name]).max(axis=1)
            df_panel = pandas.Panel(dataframes)
            diff = df_panel.apply(report_diff, axis=0)
            dataframes["diff"] = diff[hasdiffcol]
            return dataframes
    else:
        dataframes["mismatched"] = pandas.merge(d[db1_name], d[db2_name], how='outer', on="PK")
        print("mismatched rows")
        return dataframes
