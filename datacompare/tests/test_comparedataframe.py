from datacompare.comparedataframe import CompareDataFrame

import pandas as pd


def test_dump_difference(frame):
    result = CompareDataFrame(
        pd.DataFrame({"Nums": [1, 2, 3], "Chars": ["a", "b", "c"]}, columns=['Nums', 'Chars']))
    expected = CompareDataFrame(
        pd.DataFrame({"Nums": [1, 2, 3, 4], "Chars": ["a", "b", "c", "d"]}, columns=['Nums', 'Chars']))

    result.dump_difference(expected)

    in_right_not_in_left = pd.read_csv('in_right_not_in_left.txt', sep='\t')

    assert 4 in in_right_not_in_left["Nums"].values