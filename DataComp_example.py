import os
os.chdir("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development")
os.getcwd()
import DataCompare as dc

SQLs = dc.get_file_paths("C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development/SQL/")
working_cnxn_path = "C:/Users/Josh.Josh-PC/Documents/Beginning Software Engineering/MyProjectDataCompare/4Development/cnxn.txt" 

## Small data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_CA2"])          
data_to_inspect = dc1.compare_data()

## txt data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT_CA_TXT",right_script_path = None,source="txt")
data_to_inspect = dc1.compare_data()


## duplicate data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_CA_duplicate"])          
data_to_inspect = dc1.compare_data()


## Large data set
dc1 = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_ALL"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_ALL2"])
data_to_inspect = dc1.compare_data()



## Equal data set 
dce = dc.DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_CA"],datetofrom=('2015-09-29','2015-09-30'))
dce.add_right_data("GDELT",SQLs["GDELTfact_CA"])
data_to_inspect = dce.compare_data()




edw = dc.get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/EDW SQL/")
dmrt = dc.get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/DMRT SQL/")
bb = dc.get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/BloodBank/Validation/BloodBank SQL/")
wr = dc.get_file_paths("//GRH202/GRH Project Library/Data Warehouse/Execution/systems/WinRecs/Validation/WR SQL/")





import time
start_time = time.time()
dc1 = DataComp(cnxn_path = working_cnxn_path,left_cnxn_name = "GDELT",left_script_path = SQLs["GDELTfact_ALL"],datetofrom=('2015-09-29','2015-09-30'))
dc1.add_right_data("GDELT",SQLs["GDELTfact_ALL2"])          
data_to_inspect = dc1.compare_data()
print("--- %s seconds ---" % (time.time() - start_time))
