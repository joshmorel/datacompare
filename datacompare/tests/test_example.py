import datacompare as dc
import os
import pandas as pd
tests_folder = os.path.dirname(__file__)

# py.test -vv test_example.py


def test_compare_member_difference():
    # ensure all functionality works as expected when primary key is not first in compared sets
    result = dc.CompareDataFrame(
        {"Chars": ["z", "left", "b", "c"], "Extra": ["zero", "one", "two", "three"], "Num": [0, 1, 2, 3]},
        columns=['Chars', 'Extra', 'Num'], primary_key="Num")
    expected = dc.CompareDataFrame(
        {"Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"], "Nums": [1, 2, 3, 4]},
        columns=['Chars', 'Extra', 'Nums'], primary_key="Nums")

    left, right = result.get_member_difference(expected)

    assert left == right

    # expected_left = pd.DataFrame(data=None,columns=left.columns,index=left.index)


def test_compare_value_difference(text_files):

    connection_string = dc.get_connection_info(os.path.join(tests_folder, 'connection_file.ini'), 'sqlitedb')

    assert connection_string == "Driver=SQLite3 ODBC Driver;Database=sqlite.db"

    sql_text = "SELECT * FROM t1"
    result = dc.CompareDataFrame.from_sql(sql_text, connection_string)

    expected = dc.CompareDataFrame(
        pd.DataFrame(
            {"Nums": [1, 2, 3, 4], "Chars": ["right", "b", "c", "d"], "Extra": ["one", "two", "three", "four"]},
            columns=['Nums', 'Chars', 'Extra']))
    left, right = result.create_value_comparable_lists(expected)

    assert left == right
