# -*- coding: utf-8 -*-
"""
Created on Sun Apr 10 17:41:28 2016

@author: Josh
"""

## Building sqlite database with data and also save to csv for usage example

import os
import pyodbc
import pandas as pd
os.chdir(os.path.join(os.environ.get("USERPROFILE"),'datacompare/docs/example'))

connection_string = "Driver=SQLite3 ODBC Driver;Database=sqlite.db"
cnxn  = pyodbc.connect(connection_string)
cursor = cnxn.cursor()

cursor.execute("CREATE TABLE sales_new(salesdate DATETIME, product VARCHAR(255), sales_quantity INTEGER, sales_amount FLOAT)")
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-01','Bicycle',10,round(10*250.41,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-02','Bicycle',5,round(5*250.41,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-03','Bicycle',2,round(2*250.41,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-05','Bicycle',7,round(7*250.41,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-01','Jersey',8,round(8*52.4,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-02','Jersey',4,round(4*52.4,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-03','Jersey',3,round(3*52.4,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-05','Jersey',1,round(1*52.4,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-01','Helmet',10,round(10*27,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-02','Helmet',None,None)
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-03','Helmet',5,round(5*27,2))
cursor.execute("INSERT INTO sales_new VALUES (?,?,?,?)", '2015-04-05','Helmet',4,round(4*27,2))
cursor.commit()

cursor.execute("CREATE TABLE sales_old(salesdate DATETIME, product VARCHAR(255), sales_quantity INTEGER, sales_amount FLOAT)")
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-01','Bicycle',10,round(10*240.80,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-02','Bicycle',5,round(5*250.41,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-03','Bicycle',2,round(2*250.41,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-04','Bicycle',4,round(4*250.41,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-05','Bicycle',7,round(7*250.41,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-01','Jersey',8,round(8*52.4,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-02','Jersey',4,round(4*52.4,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-03','Jersey',3,round(3*52.4,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-04','Jersey',2,round(2*52.4,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-05','Jersey',1,round(1*52.4,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-01','Helmet',10,round(10*27,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-02','Helmet',1,round(1*27,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-03','Helmet',5,round(5*27,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-04','Helmet',8,round(8*27,2))
cursor.execute("INSERT INTO sales_old VALUES (?,?,?,?)", '2015-04-05','Helmet',4,round(4*27,2))
cursor.commit()

sales_old = pd.read_sql("SELECT * FROM sales_old",cnxn)
sales_old.to_csv("example_sales.csv",index=False)

cnxn.close()