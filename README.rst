datacompare
------------------

Background
---------------
We work in Data Warehousing with SSIS for ETL and the options for easy-to-build and interact with data-comparison for testing and exploration purposes are limited.

We don't have Informatica or anything super fancy and our stand-by of Excel copy/paste/vlookup is not sufficient and too manual. So I'm creating something with Python, ideally 
used in spyder, for interactive yet largely automated comparison of data.

Features
---------------
    - Compare data from SQL (via pyodbc), txt or DataFrame
    - Show rows in one set not in the other and vice versa
    - Summarize difference between sets - including counts, null counts, sums and means
    - Show values different in every column for matched rows, with difference made clearly available

Caution
---------------
This is very much small scale, organizational use. However, if anyone stumbles upon finds this useful please fork it but it's not in an installable package at this point.

License
---------------
MIT

Usage
---------------

    - Save to, let's say, a "datacompare" sub-directory within your home user directory (Windows file system in this example)
    - Write SQLs that produce result sets for comparison, or save a txt or csv file with data for comparison
    - Store connection string or file location in .ini file as in docs/example/cnxn_example.ini
    - Open spyder and produce some code as follows

.. code-block:: python

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


Output
---------------
First you will see a printed message in the IPython Console

.. figure:: https://raw.githubusercontent.com/joshmorel/datacompare/master/docs/example/datacompare_message.png
   :alt: datacompare message

You can then open the returned dict in the spyder variable explorer. diff_summary displays counts, null counts, sums, means and differences by column (depending on data type)

.. figure:: https://raw.githubusercontent.com/joshmorel/datacompare/master/docs/example/datacompare_diff_summary.png
   :alt: datacompare diff_summary
   
You can also look at the diff_values, which shows the specific value differences for each row with at least one difference delimited by a pipe.

.. figure:: https://raw.githubusercontent.com/joshmorel/datacompare/master/docs/example/datacompare_diff_values.png
   :alt: datacompare diff_values
   
Rows in one set and not in the other can also be viewed in left_not_right_data and right_not_left_data

.. figure:: https://raw.githubusercontent.com/joshmorel/datacompare/master/docs/example/datacompare_right_not_left_data.png
   :alt: datacompare right_not_left_data
   :width: 50%
   
Future Direction
------------------
    - Make datacompare an installable package 
    - Automated ETL functionality, although `etlTest <https://github.com/OpenDataAlex/etlTest/>`_ seems like it might fulfill this need
    - Data quality tests with definable rules to provide flagging of data quality issues besides just equality between sets (e.g. no missing values, outliers flags, consistent results over date range)