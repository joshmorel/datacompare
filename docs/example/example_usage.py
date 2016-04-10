# -*- coding: utf-8 -*-
"""
Created on Sun Apr 10 17:41:28 2016

@author: Josh
"""

import os
##Changing to datacompare location if saved in userhome/datacompare in Windows. Change as necessary for other file systems/setup.
os.chdir(os.path.join(os.environ.get('USERPROFILE'),'datacompare'))
import datacompare as dc

os.chdir(os.path.join('docs','example'))

my_files = dc.get_file_paths(os.getcwd() + os.sep)
my_cnxns = 'cnxn_example.ini'

## Simple example of comparison of two SQL result sets
my_datacomp = dc.DataComp(cnxn_path = my_cnxns,left_cnxn_name = 'salesdb',left_script_path = my_files['example_sales_new'],datetofrom=('2015-04-01','2015-04-10'))
my_datacomp.add_right_data(right_cnxn_name = 'salesdb',right_script_path = my_files['example_sales_old'])          
comp_result = my_datacomp.compare_data()

## Example with CSV file, pre-processing is usually required before comparison

my_datacomp.add_right_data(right_cnxn_name = 'sales_old_csv',right_script_path = None,sep=',')
## Concatenate date and product to make as primary key for comparison
my_datacomp.right_data['date_product'] = my_datacomp.right_data['salesdate'].str.cat(my_datacomp.right_data['product'],sep=' - ')
my_datacomp.set_key(side='right',col='date_product')
## Convert SQL datetime to 10 character string for comparison to csv date interpreted as string
my_datacomp.left_data['salesdate'] = my_datacomp.left_data['salesdate'].astype(str).str[0:10]
comp_result = my_datacomp.compare_data()
