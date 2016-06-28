import datacompare as dc
import pandas as pd
import numpy as np
import os

tests_folder = os.path.dirname(__file__)
os.chdir(tests_folder)


#TODO: Update to test results

def test_dump_member_difference():
    # ensure all functionality works as expected when primary key is not first in compared sets
    result = dc.CompareDataFrame(
        {"Chars": ["z", "left", "b", "c"], "Extra": ["zero", "one", "two", "three"], "Num": [0, 1, 2, 3]},
        columns=['Chars', 'Extra', 'Num'], primary_key="Num")
    expected = dc.CompareDataFrame(
        {"Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"], "Nums": [1, 2, 3, 4]},
        columns=['Chars', 'Extra', 'Nums'], primary_key="Nums")

    left, right = result.get_member_difference(expected)

    assert 'z' in left['Chars'].values
    assert 'd' in right['Chars'].values


def test_dump_value_difference(text_files):
    result = dc.CompareDataFrame(
        {"Nums": [0, 1, 2, 3], "Chars": ["z", "left", "b", "c"], "Extra": ["zero", "one", "two", "three"]},
        columns=['Nums', 'Chars', 'Extra'])
    expected = dc.CompareDataFrame(
        {"Nums": [1, 2, 3, 4], "Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"]},
        columns=['Nums', 'Chars', 'Extra'])

    result.create_value_comparable_lists(expected, to_file=True)

    value_difference = pd.read_csv('value_difference.txt', sep='\t')

    # Expect matches to have no separator, and non-matches to have separator with left value on left
    assert "left | right" in value_difference['Chars_1'].values


def test_sql_dump_value_difference(text_files):
    connection_string = dc.get_connection_info(os.path.join(tests_folder, 'connection_file.ini'), 'sqlitedb')

    assert connection_string == "Driver=SQLite3 ODBC Driver;Database=sqlite.db"

    sql_text = "SELECT * FROM t1"
    result = dc.CompareDataFrame.from_sql(sql_text, connection_string)

    expected = dc.CompareDataFrame(
        pd.DataFrame(
            {"Nums": [1, 2, 3, 4], "Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"]},
            columns=['Nums', 'Chars', 'Extra']))
    result.create_value_comparable_lists(expected, to_file=True)
    value_difference = pd.read_csv('value_difference.txt', sep='\t')
    assert "left | right" in value_difference['Chars_1'].values


def test_sql_date_parsing(text_files):
    connection_string = dc.get_connection_info(os.path.join(tests_folder, 'connection_file.ini'), 'sqlitedb')

    assert connection_string == "Driver=SQLite3 ODBC Driver;Database=sqlite.db"

    sql_texts = dc.get_sql_texts(tests_folder)
    result = dc.CompareDataFrame.from_sql(sql_texts['sql_use_date_prompt'], connection_string, params=['2014-04-01', '2014-04-04'])

    expected = dc.CompareDataFrame(
        {"Nums": [1, 2, 3, 4], "Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"]},
        columns=['Nums', 'Chars', 'Extra'])

    result.get_member_difference(expected, to_file=True)

    in_right_not_in_left = pd.read_csv('in_right_not_in_left.txt', sep='\t')
    in_left_not_in_right = pd.read_csv('in_left_not_in_right.txt', sep='\t')

    # Due to date filtering, expect two rows although 3 in t1
    assert in_right_not_in_left.shape[0] == 2
    assert in_left_not_in_right.shape[0] == 0


def test_dump_values_same(text_files):
    # Test that NaN/null are equivalent as required for ETL testing
    connection_string = dc.get_connection_info(os.path.join(tests_folder, 'connection_file.ini'), 'sqlitedb')

    sql_texts = dc.get_sql_texts(tests_folder)
    result = dc.CompareDataFrame.from_sql(sql_texts['sql_lite_nulls'], connection_string)

    # Side effect of above that ints and floats may not always be equal when expected
    expected = dc.CompareDataFrame(
        {'Nums': [1, 2, 3], 'NumNulls': [np.nan, 3.3, 4.7], 'SomeInt': [1.0,2.0,3.0]},
        columns=['Nums', 'NumNulls','SomeInt'])

    result.create_value_comparable_lists(expected, to_file=True, value_precision=1)

    value_difference = pd.read_csv('value_difference.txt', sep='\t')
    print('The different values are: {}'.format(value_difference))
    assert value_difference.shape[0] == 0
