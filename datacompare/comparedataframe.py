import pandas as pd
import datacompare.util as dc_util
import numpy as np
import os
import pyodbc


# TODO: Add command line functionality so this can executed from a tests file for automated testing

# TODO: Data security considerations

# TODO: Test in additional databases beyond sqlite ms sql


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
        self._index_on_primary_key()

    def _index_on_primary_key(self):
        self.index = self[self.primary_key]
        self.sort_index(inplace=True)

    def set_primary_key(self, column_as_primary_key):
        assert column_as_primary_key in self.columns, "Must be single existing column in dataframe"
        self.primary_key = column_as_primary_key
        self._index_on_primary_key()

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
            df = pd.read_sql(sql, con = sql_connection, index_col=index_col, coerce_float=coerce_float,params=params)
            assert df.shape[0] > 0, 'No rows were returned by the SQL script: {}'.format(sql)
            return CompareDataFrame(df, primary_key=primary_key)
        finally:
            sql_connection.close()

    def get_member_difference(self, right, limit=100, to_file=True, path='.', ):

        """
        Using CompareDataFrame.primary_key gets members (rows) of object (left) not in right and vice versa.
        Dumps to file or returns as tuple of frames

        Examples:
        result.get_member_difference(expected)

        left, right = result.get_member_difference(expected, to_file=False)

        Parameters
        ----------
        right : CompareDataFrame
            Data set to compare object against
        limit : int, default 100
            Number of rows to return
        to_file : bool, default True
            Dumps two tab-separated text files in path, 'in_left_not_in_right.txt' & 'in_right_not_in_left.txt'
            Otherwise, return tuple of frames
        path : string, default = '.'
            The directory to dump the files to

        """
        shared_primary_keys = self.index[self.index.isin(right.index)]

        in_left_not_in_right = self[~self.index.isin(shared_primary_keys)][:limit]
        in_right_not_in_left = right[~right.index.isin(shared_primary_keys)][:limit]

        if to_file:
            in_left_not_in_right.to_csv(os.path.join(path, 'in_left_not_in_right.txt'), sep='\t')
            in_right_not_in_left.to_csv(os.path.join(path, 'in_right_not_in_left.txt'), sep='\t')
        else:
            return in_left_not_in_right, in_right_not_in_left

    def get_value_difference(self, right, limit=100, to_file=True, path='.', value_precision=0):

        """
        Using CompareDataFrame.primary_key compare all column of object (left) against another CompareDataFrame (right)
        Dumps findings to file or returns as frame

        Examples:
        result.get_value_difference(expected)

        differences = result.get_member_difference(expected, to_file=False)

        Parameters
        ----------
        right : CompareDataFrame
            Data set to compare object against
        limit : int, default 100
            Number of rows to return
        to_file : bool, default True
            Dumps two tab-separated text files in path, 'value_difference.txt'
            Otherwise, return frame
        path : string, default = '.'
            The directory to dump the files to
        value_precision : int, default 0
            For numeric data types, the degree precision to round to

        """

        shared_primary_keys = self.index[self.index.isin(right.index)]
        shared_columns = self.columns[self.columns.isin(right.columns)]
        assert len(shared_columns) >= 2, "Require at least one common column in data sets beside primary key"

        left_data_to_compare = self.loc[shared_primary_keys][shared_columns]
        right_data_to_compare = right.loc[shared_primary_keys][shared_columns]

        # To product final data frame only do not include primary key column
        shared_columns_to_return = list(set(shared_columns).difference({left_data_to_compare.primary_key}))

        # List of frames for each column with value differences by row and formatted display of different values
        column_values = [dc_util.compare_column_values(left_data_to_compare[col],
                                                       right_data_to_compare[col],
                                                       precision=value_precision)
                         for col in shared_columns_to_return]

        # Start with empty frame to which to add both value difference counts by row index and columns
        values_frame = pd.DataFrame({'values_different': [0] * len(left_data_to_compare.index)},
                                    index=left_data_to_compare.index)

        # Build frame and also list of tuples of column name/difference pairs for presentation of returned set
        column_difference_counts = []
        for col in column_values:
            values_frame['values_different'] = values_frame['values_different'] + col.iloc[:, 0]
            values_frame[col.iloc[:, 1].name] = col.iloc[:, 1]
            column_difference_counts.append((col.iloc[:, 1].name, sum(col.iloc[:, 0])))

        # Show columns with most difference at left, and return only rows with at least one difference
        sorted_column_names = ['values_different'] + [col[0] for col in
                                                      sorted(column_difference_counts, key=lambda c: c[1],
                                                             reverse=True)]
        rows_to_return = values_frame[values_frame['values_different'] > 0][sorted_column_names]

        # Tag columns with difference count suffix to facilitate inspection
        tagged_column_names = ['values_different'] + ['{}_{}'.format(col[0], col[1]) for col in
                                                      sorted(column_difference_counts, key=lambda c: c[1],
                                                             reverse=True)]
        rows_to_return.columns = tagged_column_names

        if to_file:
            rows_to_return[:limit].to_csv(os.path.join(path, 'value_difference.txt'),sep='\t')
        else:
            return rows_to_return[:limit]

    # TODO: Truly add iterative/interactive functionality to show chunk of values missing in sets
    # def _iter_member_difference(self, right):
    #     pass

    # TODO: Add iterative/interactive functionality to show chunk of values with different values

    # def iter_value_difference(self, right):
    #     pass

    @property
    def _constructor(self):
        return CompareDataFrame


