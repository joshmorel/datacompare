import os
os.chdir("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development")
os.getcwd()
import DataCompare as dc
os.chdir("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/5Testing/tests")

## Test Cases

import unittest

class TestMyFunctions(unittest.TestCase):
    def test_load_normal(self):
        testdc = dc.DataComp(cnxn_path = "test_cnxn.txt",left_cnxn_name = "GDELT",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
        self.assertEqual(testdc.left_data.shape,(1636, 21),"Normal Test failed")
    def test_load_norows(self):
        with self.assertRaises(AssertionError):
            dc.DataComp("test_cnxn.txt","GDELT","test_load_norows.sql",('2015-09-29','2015-09-30'))
    def test_load_nocols(self):
        with self.assertRaises(AssertionError):
            dc.DataComp("test_cnxn.txt","GDELT","test_load_nocols.sql",('2015-09-29','2015-09-30'))
    def test_load_from_utf8_bom(self):
        testdc = dc.DataComp(cnxn_path = "test_cnxn.txt",left_cnxn_name = "GDELT",left_script_path = "test_load_utf8.sql",datetofrom=('2015-09-29','2015-09-30'))
        self.assertTrue(testdc.left_data.shape[0]>0)
    def test_compare_equal(self):
        testdc = dc.DataComp(cnxn_path = "test_cnxn.txt",left_cnxn_name = "GDELT",left_script_path = "test_normal.sql",datetofrom=('2015-09-29','2015-09-30'))
        testdc.add_right_data("GDELT","test_normal.sql")
        testdc_dict = testdc.compare_data()
        self.assertTrue((testdc_dict["left_not_right_data"].shape[0] == 0) and (testdc_dict["right_not_left_data"].shape[0] == 0)\
            and (testdc_dict["diff_values"] is None),"Same result set comparison failed")
    def test_compare_nosharedcol(self):
        pass
    def test_compare_diffrows(self):
        pass
    def test_compare_rowsame_diffval(self):
        pass
   
if __name__ == '__main__':
    unittest.main(exit=False)