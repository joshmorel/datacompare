import pyodbc
import os
import sys
import datacompare as dc


def main():
    """
    This is simple any example which can be run from command line with
    >python example_usage.py
    Should result in three text files. For this example, value_difference.txt is most interesting
    """

    connection_string = dc.get_connection_info('connection_file.ini', 'salesdb')
    sql_texts = dc.get_sql_texts()

    result = dc.CompareDataFrame.from_sql(sql_texts['example_sales_new'], connection_string,
                                          params=['2015-04-01', '2015-04-02'])

    expected = dc.CompareDataFrame.from_sql(sql_texts['example_sales_old'], connection_string,
                                            params=['2015-04-01', '2015-04-02'])

    # In this example, we will dump both left right and value differences to existing directory
    # Running this example from command line:
    # >python example_usage.py

    result.get_member_difference(expected, to_file=True)
    result.get_value_difference(expected, to_file=True)


main()
