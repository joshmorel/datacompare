import os
os.chdir("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development")
os.getcwd()
from DataCompare import DataComp
from DataCompare import get_file_paths

SQLs = get_file_paths("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development/SQL/")
working_cnxn_path = "C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development/cnxn.txt" 

## Small data set
dc1 = DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_CA2"])          
data_to_inspect = dc1.compare_data()

## txt data set
dc1 = DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT_CA_TXT",right_script_path = None,source="txt")
data_to_inspect = dc1.compare_data()


## duplicate data set
dc1 = DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_CA_duplicate"])          
data_to_inspect = dc1.compare_data()


## Large data set
dc1 = DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_ALL"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_ALL2"])
data_to_inspect = dc1.compare_data()



edw = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/EDW SQL/")
dmrt = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/DMRT SQL/")
bb = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/BloodBank/Validation/BloodBank SQL/")
wr = get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/WR SQL/")


