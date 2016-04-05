import sys
import os
sys.path.insert(0,os.path.join(os.environ.get("USERPROFILE"),'datacompare'))
import datacompare as dc
import imp
imp.reload(dc)

import unittest
import pandas as pd

test_cnxn_path = "test_cnxn.ini"

##These tests uses a small sqlite database with two tables t1 & t2 with 10 & 9 rows respectively, and all data types applicable to most RDBMSs. 
##These can be combined in any number of ways with SQL to meet most testing needs.
##To use sqlite install sqlite and then ODBC driver appilable to your OS & bitness of Python

class TestMyFunctions(unittest.TestCase):

    ##Section 1: Loading & comparison tests 
    def test_load_normal(self):
    ## Simply loading data
        print("test_load_normal")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
        self.assertEqual(testdc.left_data.shape[0],10,"Normal Test failed")
    def test_load_from_utf8_bom(self):
    ## Using read for a file that may or may not have a UTF-8 BOM can cause issues, so need to test both file types
        print("test_load_from_utf8_bom")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_load_utf8.sql",datetofrom=('2015-09-29','2015-09-30'))
        self.assertTrue(testdc.left_data.shape[0]>0)
    def test_equal_sets(self):
    ## Same exact result sets
        print("test_compare_equal")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("sqlitedb","test_normal.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue(testdc_dict["diff_summary"]["diff_val_count"].max() == '0',"Same result set comparison failed")
    def test_compare_diffrows(self):
    ## Differing number of rows in each
        print("test_compare_diffrows")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "GDELT",left_script_path = "test_compare_diffrow_left.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("GDELT","test_compare_diffrow_right.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue((testdc_dict["left_not_right_data"].shape[0] > 0) and (testdc_dict["right_not_left_data"].shape[0] > 0)\
            and (testdc_dict["diff_values"] is None),"Differing rows test failed" \
            and (max(testdc_dict["diff_summary"]["right_count"]) > max(testdc_dict["diff_summary"]["common_row_count"])))
    def test_diffvals(self):
    ## Same rows, but different values in some rows
        print("test_compare_rowsame_diffval")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_compare_diffval_left.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("sqlitedb","test_compare_diffval_right.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue((testdc_dict["left_not_right_data"].shape[0] == 0) and (testdc_dict["right_not_left_data"].shape[0] == 0)\
            and (testdc_dict["diff_values"].shape[0] > 1) and (sum(testdc_dict["diff_summary"]["diff_val_count"].apply(int)) > 0),"Differing values failed")
    def test_diffnull(self):
    ## Same rows, but different values due to nulls in some rows
        print("test_compare_rowsame_diffnull")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_compare_diffnull_left.sql",datetofrom=('2015-05-29','2015-09-30'))
        testdc.add_right_data("sqlitedb","test_compare_diffnull_right.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue((testdc_dict["left_not_right_data"].shape[0] == 0) and (testdc_dict["right_not_left_data"].shape[0] == 0)\
            and (testdc_dict["diff_values"].shape[0] > 5),"Differing values with nulls failed")
    def test_setkey(self):
    ## Test setting of key on both sides
        print("test_compare_setkey")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_compare_setkey_left.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("sqlitedb","test_compare_setkey_right.sql")
        testdc.set_key(col="mypk",side="both")
        testdc_dict = testdc.compare_data()
        self.assertTrue((testdc_dict["left_not_right_data"].shape[0] == 0) and (testdc_dict["right_not_left_data"].shape[0] == 0)\
            and (testdc_dict["diff_values"] is None),"Same result set comparison failed")
    def test_sql_intfloatequal(self):
    ## Test to confirm if int and float compared where values are equal, the result is passed
        print("test_sql_intfloatequal")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "GDELT",left_script_path = "test_compare_intvsfloat_same_left.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("GDELT","test_compare_intvsfloat_same_right.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue(testdc_dict["diff_values"] is None,"Same int float compare failed")


    ##Section 2: Assertion error tests
    def test_assert_norows(self):
    ## To confirm assertion error if empty result set returned
        print("test_load_norows")
        with self.assertRaises(AssertionError):
            dc.DataComp(test_cnxn_path,"sqlitedb","test_load_norows.sql",('2010-09-29','2011-09-30'))
    def test_assert_nocols(self):
    ## To confirm assertion error if query not returning a result set (no columns)        
        print("test_load_nocols")
        with self.assertRaises(AssertionError):
            dc.DataComp(test_cnxn_path,"sqlitedb","test_load_nocols.sql",('2015-09-29','2015-09-30'))
    def test_assert_duplicatepk(self):
    ## To confirm assertion error if duplicate primary key in col attempting to compare on
        print("test_load_pkduplicate")
        with self.assertRaises(AssertionError):
            testdc = dc.DataComp(test_cnxn_path,"sqlitedb","test_load_pkduplicate.sql",('2015-09-29','2015-09-30'))
            testdc.add_right_data("sqlitedb","test_normal.sql")
            testdc.compare_data()
    def test_assert_nosharedcol(self):
    ## To confirm assertion error if there are no shared columns to compare except PK
        print("test_compare_nosharedcol")
        with self.assertRaises(AssertionError):
            testdc = dc.DataComp(test_cnxn_path,"sqlitedb","test_normal.sql",('2015-09-29','2015-09-30'))
            mydf = pd.DataFrame({"a":[1,2],"b":["a","b"]})
            testdc.add_right_data(right_cnxn_name = None,right_script_path = None,DataFrame = mydf)
            testdc.compare_data()
    def test_setkey_asserterror1(self):
    ## To confirm assertion error if try to set key on non-existant column 
        print("test_setkey_asserterror1")
        with self.assertRaises(AssertionError):
            testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
            testdc.set_key(col="hello",side="left")
    def test_setkey_asserterror2(self):
    ## To confirm assertion error if try to set key on right set if does not exist
        print("test_setkey_asserterror2")
        with self.assertRaises(AssertionError):
            testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "sqlitedb",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
            testdc.set_key(col="mynameis",side="both")
    def test_ini_missing_section(self):
    ## To confirm assertion error if provided cnxn name does not exist in .ini file
        print("test_ini_missing_section")
        with self.assertRaises(AssertionError):
            dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "notinini",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
    def test_ini_unexpected_type_txt(self):
    ## To confirm assertion error if they ini type is txt but sql expected
        print("test_ini_unexpected_type_txt")
        with self.assertRaises(AssertionError):
            dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "txt_unexpected",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
    def test_ini_unexpected_type_sql(self):
        print("test_ini_unexpected_type_sql")
        with self.assertRaises(AssertionError):
            dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "GDELT_sql_unexpected",left_script_path = None,datetofrom=('2015-09-29','2015-09-30'))
    def test_load_txt_pipe(self):
    ## To confirm assertion error if they ini type is sql but ini expected
        print("test_load_txt_pipe")
        testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "txt_pipe",sep="|")
        self.assertEqual(testdc.left_data.shape[0],10,"Normal Test failed")


    def test_sql_uniqucols(self):
        print("test_sql_uniqucols")
        with self.assertRaises(AssertionError):
            testdc = dc.DataComp(cnxn_path = test_cnxn_path,left_cnxn_name = "GDELT",left_script_path = "test_unique_columns.sql",datetofrom=('2015-09-29','2015-09-30'))
            testdc.add_right_data("GDELT","test_unique_columns.sql")
            testdc.compare_data()
    
if __name__ == '__main__':
    unittest.main(exit=False)