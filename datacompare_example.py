import os
os.chdir("C:/Users/Josh.Josh-PC/datacompare")
import datacompare as dc
import imp
imp.reload(dc)

SQLs = dc.get_file_paths("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development/SQL/")
SQLtest = dc.get_file_paths("C:/Users/Josh.Josh-PC/datacompare/tests/")
working_cnxn_path = "C:/Users/Josh.Josh-PC/datacompare/tests/test_cnxn.ini"


## Small data set - different vals
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLtest["test_compare_diffval_left"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLtest["test_compare_diffval_right"])          
data_to_inspect = dc1.compare_data()

## Small data set - different columns
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLtest["test_compare_diffrow_left"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLtest["test_compare_diffrow_right"])          
data_to_inspect = dc1.compare_data()



## Small data set - different vals in two cols
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLtest["test_compare_diffval_twocol_left"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLtest["test_compare_diffval_twocol_right"])
data_to_inspect = dc1.compare_data()

## txt data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT_CA_TXT",right_script_path = None)
data_to_inspect = dc1.compare_data()

## duplicate data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLtest["test_load_pkduplicate"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLtest["test_normal"])
data_to_inspect = dc1.compare_data()