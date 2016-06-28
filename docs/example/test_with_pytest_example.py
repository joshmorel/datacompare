# From terminal in current directory run:
# py.test -vv
# that's it!

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
