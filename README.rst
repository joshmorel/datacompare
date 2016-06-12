datacompare
------------------

Background
---------------

We work in Data Warehousing with SSIS for ETL and the options for easy-to-build and interact with data-comparison for testing and exploration purposes are limited.
The SQL unit test with C# does seem promising, but only for automated testing, not exploratory comparing.

Features
---------------
    - Compare data from SQL (via pyodbc), txt or DataFrame
    - Get rows in one set not in the other and vice versa and view in session or dump to text
    - Show values different in every column for matched rows and view in session or dump to text

Caution
---------------
I am increasing my skills in python by leaps and bounds (thanks Talk Python to Me!) but am no expert. So I can't promise this will work in anything besides
latest version of python and with MS SQL, texts or sqlite.

License
---------------
MIT

Usage
---------------

    - PIP install from GitHub > pip install git+git://github.com/joshmorel/datacompare.git@master
    - Open an interactive python interpreter and set working directory to something which contains these files https://github.com/joshmorel/datacompare/docs/example
    - Run the following code (assume sqlite 3 ODBC driver is installed)

.. code-block:: pycon

    >>> import datacompare as dc
    >>> sql_texts = dc.get_sql_texts('.')
    >>> connection_string = dc.get_connection_info('connection_file.ini', 'salesdb')
    >>> left = dc.CompareDataFrame.from_sql(sql_texts['example_sales_new'], connection_string,params=['2015-04-01', '2016-04-02'])
    >>> right = dc.CompareDataFrame.from_sql(sql_texts['example_sales_old'], connection_string,params=['2015-04-01', '2016-04-02'])
    >>> in_left_not_right, in_right_not_left = left.get_member_difference(right, limit=2, to_file=False)
    >>> value_differences = left.get_value_difference(right, to_file=False, limit=2, value_precision=2)
    >>> print('Rows in left not in right\n\n {}\n\nRows in right not in left\n\n{}\n\nValue differences\n\n {}'.format(in_left_not_right,in_right_not_left,value_differences))


Would result in the following:

.. code-block:: pycon
    Rows in left not in right

                                 date_product  salesdate product  sales_quantity  \
    date_product
    2015-05-05 - Helmet  2015-05-05 - Helmet 2015-05-05  Helmet             4.0

                         sales_amount
    date_product
    2015-05-05 - Helmet           1.0

    Rows in right not in left

                                  date_product  salesdate  product  \
    date_product
    2015-04-04 - Bicycle  2015-04-04 - Bicycle 2015-04-04  Bicycle
    2015-04-04 - Helmet    2015-04-04 - Helmet 2015-04-04   Helmet

                          sales_quantity  sales_amount
    date_product
    2015-04-04 - Bicycle               4       1001.64
    2015-04-04 - Helmet                8        216.00

    Value differences

                           values_different sales_quantity_12   sales_amount_2  \
    date_product
    2015-04-01 - Bicycle                 2         10.0 | 10  2504.1 | 2408.0
    2015-04-01 - Helmet                  1         10.0 | 10            270.0

                         product_0 salesdate_0
    date_product
    2015-04-01 - Bicycle   Bicycle  2015-04-01
    2015-04-01 - Helmet     Helmet  2015-04-01


Future Direction
------------------
    - Automated ETL testing functionality
    - Interactive command line session with iter function to generate chunks of rows
