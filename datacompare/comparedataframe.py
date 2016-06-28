import pandas as pd
from datacompare import util as dc_util
import pyodbc
import logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s- %(message)s')
logging.debug('Start of program')


class CompareDataFrame(pd.DataFrame):
    """
    A CompareDataFrame is a pandas.DataFrame with specific attributes and methods for ease of
    comparison of two data sets, particularly for ETL testing

    Examples:
    result = datacompare.CompareDataFrame(
        {'Chars': ['z', 'z', 'b', 'a'], 'Nums': [0, 1, 2, 3]},
        columns=['Chars','Nums'], primary_key="Nums")

    Keyword Arguments
    -----------------
    primary_key: str, default None
        The primary key for which to compare at rows against, if None defaults to left-most of set
    """

    _metadata = ['primary_key']

    def __init__(self, *args, **kwargs):
        primary_key = kwargs.pop('primary_key', None)
        super(CompareDataFrame, self).__init__(*args, **kwargs)
        if primary_key is None:
            self.primary_key = self.columns[0]
        else:
            self.primary_key = primary_key
            # self._index_on_primary_key()

    # def _index_on_primary_key(self):
    #     self.set_index(self.primary_key, drop=False, inplace=True)
    #     self.sort_index(inplace=True)

    def set_primary_key(self, column_as_primary_key):
        assert column_as_primary_key in self.columns, "Must be single existing column in dataframe"
        self.primary_key = column_as_primary_key
        # self._index_on_primary_key()

    @classmethod
    def from_sql(cls, sql, connection_string, primary_key=None, index_col=None, coerce_float=True, params=None):

        """
        Returns a CompareDataFrame corresponding to the result of the query string.

        Examples:
        sql = "SELECT col1, col2 FROM t1"
        connection_string = 'Driver=SQLite3 ODBC Driver;Database=sqlite.db'
        result = datacompare.CompareDataFrame(sql, connection_string)

        Parameters
        ----------
        sql : str
            SQL select query
        connection_string : str
            Connection string for the database
        primary_key : str, default None
            The primary key for which to compare at rows against, if None defaults to left-most of set

        See the documentation for pandas.read_sql for further explanation
        of the following parameters:
        index_col, coerce_float, params

        """

        sql_connection = pyodbc.connect(connection_string)
        try:
            df = pd.read_sql(sql, con=sql_connection, index_col=index_col, coerce_float=coerce_float, params=params)
            assert df.shape[0] > 0, 'No rows were returned by the SQL script: {}'.format(sql)
            return CompareDataFrame(df, primary_key=primary_key)
        finally:
            sql_connection.close()

    def get_member_difference(self, right, limit=100):

        """
        Using CompareDataFrame.primary_key gets members (rows) of object (left) not in right and vice versa to pass to py.test for assert

        Examples:

       `python
        left, right = result.create_value_comparable_lists(expected)

        assert left == right
        `

        `shell

        py.test

        E       assert [InLeftOnly(I...zero', Num=0)] == [InRightOnly(I...our', Nums=4)]
        E         At index 0 diff: InLeftOnly(Index=0, Chars='z', Extra='zero', Num=0) != InRightOnly(Index=4, Chars='d', Extra='four', Nums=4)
        E         Full diff:
        E         - [InLeftOnly(Index=0, Chars='z', Extra='zero', Num=0)]
        E         ?    ^^^            ^         ^          ^^ -       ^
        E         + [InRightOnly(Index=4, Chars='d', Extra='four', Nums=4)]
        E         ?    ^^^^            ^         ^          ^^^       + ^
        `

        Parameters
        ----------
        right : CompareDataFrame
            Data set to compare object against
        limit : int, default 100
            Number of members not found in other set to return per set

        """

        # indexed_self = pd.DataFrame(data= primary_key

        left_index = pd.Index(self[self.primary_key])
        right_index = pd.Index(right[right.primary_key])

        assert left_index.is_unique, 'Index of left (self) set must be unique'
        assert right_index.is_unique, 'Index of right set must be unique'

        shared_index_values = left_index[left_index.isin(right_index)]
        in_left_not_in_right = self[~self[self.primary_key].isin(shared_index_values)]
        in_right_not_in_left = right[~right[right.primary_key].isin(shared_index_values)]

        logging.debug('in right not in left {} '.format(in_right_not_in_left))

        i = limit
        rows_left = []
        for row in in_left_not_in_right.itertuples(index=True, name='InLeftOnly'):
            rows_left.append(row)
            if i < 0:
                break
            i -= 1

        i = limit
        rows_right = []
        for row in in_right_not_in_left.itertuples(index=True, name='InRightOnly'):
            logging.debug('right row is {} '.format(row))
            rows_right.append(row)
            if i < 0:
                break
            i -= 1

        logging.debug('rows right {} '.format(rows_right))
        return rows_left, rows_right

    def create_value_comparable_lists(self, right, value_precision=0):

        """
        Using CompareDataFrame.primary_key produce two lists of rows with matching keys and columns to then perform an equality test with py.test

        Examples:

        `python
        left, right = result.create_value_comparable_lists(expected)

        assert left == right
        `

        `shell

        py.test

        E       assert [ValueCompare...xtra='three')] == [ValueCompare(...xtra='three')]
        E         At index 0 diff: ValueCompare(Index=1, Nums='1.0', Chars='left', Extra='one') != ValueCompare(Index=1, Nums='1.0', Chars='right', Extra='one')
        E         Full diff:
        E         - [ValueCompare(Index=1, Nums='1.0', Chars='left', Extra='one'),
        E         ?                                           ^^^
        E         + [ValueCompare(Index=1, Nums='1.0', Chars='right', Extra='one'),
        E         ?                                           ^^^^
        E         ValueCompare(Index=2, Nums='2.0', Chars='b', Extra='two'),
        E         ValueCompare(Index=3, Nums='3.0', Chars='c', Extra='three')]
        `

        Parameters
        ----------
        right : CompareDataFrame
            Data set to compare object against
        value_precision : int, default 0
            For numeric data types, the degree precision to round to

        """

        left_index = pd.Index(self[self.primary_key])
        right_index = pd.Index(right[right.primary_key])

        assert left_index.is_unique, 'Index of left (self) set must be unique'
        assert right_index.is_unique, 'Index of right set must be unique'

        shared_index_values = left_index[left_index.isin(right_index)]
        shared_columns = self.columns[self.columns.isin(right.columns)]

        assert len(shared_columns) >= 2, "Require at least one common column in data sets beside primary key"
        assert shared_columns.is_unique, 'Columns to compare sets against must be unique'

        left_data_to_compare = self[self[self.primary_key].isin(shared_index_values)][shared_columns].set_index(
            self.primary_key).sort_index()

        right_data_to_compare = right[right[right.primary_key].isin(shared_index_values)][shared_columns].set_index(
            right.primary_key).sort_index()

        standardized_left = dc_util.clean_frame(left_data_to_compare, precision=value_precision)
        standardized_right = dc_util.clean_frame(right_data_to_compare, precision=value_precision)

        rows_left = []
        for row in standardized_left.itertuples(index=True, name='ValueCompare'):
            rows_left.append(row)

        rows_right = []
        for row in standardized_right.itertuples(index=True, name='ValueCompare'):
            rows_right.append(row)

        return rows_left, rows_right

    @property
    def _constructor(self):
        return CompareDataFrame


logging.debug('End of program')
