# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 10:09:40 2016

@author: Josh
"""
import os
os.chdir("C:/Users/Josh.Josh-PC/datacompare/tests")
##import datacompare as dc
##import imp
##imp.reload(dc)
import pyodbc

connection_string = "Driver=SQLite3 ODBC Driver;Database=sqlite.db"
cnxn  = pyodbc.connect(connection_string)
cursor = cnxn.cursor()

cursor.execute("CREATE TABLE t1(mypk INT NOT NULL, numint INT, numfloat REAL, numchar VARCHAR(255), sometext VARCHAR(255), adate DATETIME, abit BIT)")
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 1,1,1.1,'10','mynameis','2015-10-01',1)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 2,2,2.0,'2','blah','2015-04-01',0)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 3,2,2.0,'2','blahz','2015-04-02',1)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 4,2,2.0,'2','blahzzz','2015-04-03',0)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 5,2,2.0,'2','smt','2015-04-04',1)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 6,2,2.0,'2','tdt','2015-04-05',0)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 7,2,2.0,'2','frr','2015-04-10 04:01',1)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 8,2,2.0,'2','gg','2015-06-01 11:00',1)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 9,2,2.0,'2','sdafs','2012-04-01 10:00',0)
cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?)", 10,None,None,None,None,None,None)
cursor.commit()

cursor.execute("CREATE TABLE t2(mypk INT NOT NULL, numint INT, numfloat REAL, numchar VARCHAR(255), sometext VARCHAR(255), adate DATETIME, abit BIT)")
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 1,1,1.1,'10','mynameis','2015-11-01',1)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 2,2,2.0,'2','blah','2015-04-01',0)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 3,3,2.0,'2','blahz','2015-04-02',1)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 4,2,2.1,'2','blahzzz','2015-04-03',0)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 5,2,2.0,'80','doughlas','2015-04-04',1)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 8,2,2.0,'2','grad','2015-06-01 11:00',1)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 9,2,2.0,'2','sdafs','2012-04-01 10:00',0)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 10,None,None,None,None,None,None)
cursor.execute("INSERT INTO t2 VALUES (?,?,?,?,?,?,?)", 11,2,2.0,'2','sdafs','2012-04-01 10:00',0)
cursor.commit()

cursor.execute("select * from t1")
cursor.fetchall()
cursor.execute("select * from t2")
cursor.fetchall()


#cursor.execute("DROP TABLE t1; DROP TABLE t2;")
#cursor.commit()
cnxn.close()