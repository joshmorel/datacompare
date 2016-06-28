datacompare
------------------

Background
---------------

We work in Data Warehousing with SSIS for ETL and the options for easy-to-build and interact with data-comparison for testing and exploration purposes are limited.
The SQL unit test with C# does seem promising, but only for automated testing, not exploratory comparing.

Features
---------------
    - Automatic & interactive comparison of results sets leveraging py.test and pyodbc
    - Compare data from databases and flat files
    - Check result set membership equality & value equality

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
    - Install py.test > pip install -U pytest
    - Install sqlite 3 ODBC driver & set-up as in https://github.com/joshmorel/datacompare/tree/master/docs/example
    - Create file test_with_pytest_example.py as below


.. code-block:: python

    import datacompare as dc

    connection_string = dc.get_connection_info('connection_file.ini', 'salesdb')
    sql_texts = dc.get_sql_texts()

    result = dc.CompareDataFrame.from_sql(sql_texts['example_sales_new'], connection_string,
                                          params=['2015-04-01', '2015-04-02'])

    expected = dc.CompareDataFrame.from_sql(sql_texts['example_sales_old'], connection_string,
                                            params=['2015-04-01', '2015-04-02'])


    def test_with_pytest_members_are_same():

        left, right = result.get_member_difference(expected)

        assert left == right


    def test_with_pytest_values_are_same():

        left, right = result.create_value_comparable_lists(expected)

        assert left == right



Run in terminal in directory where above saved and get the following result::

    >py.test -vv

    E         Full diff:
    E         - [ValueCompare(Index='2015-04-01 - Bicycle', sales_quantity='10.0', sales_amount='2504.0'),
    E         ?                                                                                   ^ ^
    E         + [ValueCompare(Index='2015-04-01 - Bicycle', sales_quantity='10.0', sales_amount='2408.0'),
    E         ?                                                                                   ^ ^



