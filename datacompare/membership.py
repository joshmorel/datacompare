import pandas as pd
from datacompare.comparedataframe import CompareDataFrame

result = CompareDataFrame(pd.DataFrame({"Nums": [0,1, 2, 3], "Chars": ["zoo","a", "b", "c"]}, columns=['Nums', 'Chars']))
expected = CompareDataFrame(
    pd.DataFrame({"Nums": [1, 2, 3, 4, 5], "Chars": ["a", "b", "c", "d", "e"]}, columns=['Nums', 'Chars']))


def dump_difference(left, right, limit=100):
    """

    Parameters
    ----------
    left: A CompareDataFrame, typically the result you are testing against an expected result
    right: A CompareDataFrame, typically the expected
    limit: The number of rows to limit in the dump, default 100
    -------

    """

    shared_primary_keys = left.index[left.index.isin(right.index)]
    right[~right.index.isin(shared_primary_keys)][:limit].to_csv('in_right_not_in_left.txt')
    left[~left.index.isin(shared_primary_keys)][:limit].to_csv('in_left_not_in_right.txt')


#dump_difference(result, expected)


def show_difference(left, right):
    """

    Parameters
    ----------
    left: A CompareDataFrame, typically the result you are testing against an expected result
    right: A CompareDataFrame, typically the expected
    -------

    """

    shared_primary_keys = left.index[left.index.isin(right.index)]
    in_right_not_in_left = right[~right.index.isin(shared_primary_keys)]
    in_left_not_in_right = left[~left.index.isin(shared_primary_keys)]

    for r in in_right_not_in_left.itertuples(name='InRightNotInLeft'):
        yield r

    for r in in_left_not_in_right.itertuples(name='InLeftNotInRight'):
        yield r


# difference = show_difference(result, expected)
#
# for i in difference:
#     print(i)

#while True:
#    print(next(show_difference(result, expected)))

    # right[~right.index.isin(shared_primary_keys)][:limit].to_csv('in_right_not_in_left.txt')
    # left[~left.index.isin(shared_primary_keys)][:limit].to_csv('in_left_not_in_right.txt')


# def show_difference(left, right):
#
#
# def compare_membership(left, right):
#     shared_primary_keys = left.index[left.index.isin(right.index)]
#     in_right_not_in_left = right[~right.index.isin(shared_primary_keys)]
#
#     for r in in_right_not_in_left.itertuples():
#         yield r
# for row in in_right_not_in_left.itertuples()

# return in_right_not_in_left
# yield in_right_not_in_left
# return right[~right.index.isin(shared_primary_keys)]

# print(shared_primary_keys)


# left_pks[left_pks.isin(right_pks)]
# pdt.assert_index_equal(left=result.index, right=expected.index)
