import pandas as pd
import datacompare.util as dc_util
import numpy as np
import os
import pyodbc


# TODO: Add command line functionality so this can exectued from a tests file for automated testing

# TODO: Do we need security  - ssl of database file if on intranet? Is this necessary?

# TODO: Encrypted password for Oracle etc? - test on Postgres


class CompareDataFrame(pd.DataFrame):
    """
    A special frame with a primary key for comparison
    """

    # normal properties
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

        # TODO: Write docstring
        sql_connection = pyodbc.connect(connection_string)
        try:
            df = pd.read_sql(sql, con = sql_connection, index_col=index_col, coerce_float=coerce_float,params=params)
            assert df.shape[0] > 0, 'No rows were returned by the SQL script: {}'.format(sql)
            return CompareDataFrame(df, primary_key=primary_key)
        finally:
            sql_connection.close()

    def dump_member_difference(self, right, limit=100, path='.'):
        """

        Parameters
        ----------
        right: A CompareDataFrame, typically the expected where self is the result
        limit: The number of rows to limit in the dump, default 100
        path: The directory path at which to dump the data, default is working directory
        -------

        """
        shared_primary_keys = self.index[self.index.isin(right.index)]
        right[~right.index.isin(shared_primary_keys)][:limit].to_csv(os.path.join(path, 'in_right_not_in_left.txt'),
                                                                     sep='\t')
        self[~self.index.isin(shared_primary_keys)][:limit].to_csv(os.path.join(path, 'in_left_not_in_right.txt'),
                                                                   sep='\t')

    # TODO: Truly add iterative/interactive functionality to show chunk of values missing in sets

    def show_member_difference(self, right):
        """

        Parameters
        ----------
        right: A CompareDataFrame, typically the expected
        -------

        """

        shared_primary_keys = self.index[self.index.isin(right.index)]
        in_right_not_in_left = right[~right.index.isin(shared_primary_keys)]
        in_left_not_in_right = self[~self.index.isin(shared_primary_keys)]

        for r in in_right_not_in_left.itertuples(name='InRightNotInLeft'):
            yield r

        for r in in_left_not_in_right.itertuples(name='InLeftNotInRight'):
            yield r

    def dump_value_difference(self, right, limit=100, path='.'):

        # TODO: Make this cleaner, potential iterate through tuples?
        shared_primary_keys = self.index[self.index.isin(right.index)]

        shared_columns = self.columns[self.columns.isin(right.columns)]
        assert len(shared_columns) >= 1, "Require at least one common column in data sets for comparison"

        left_data_to_compare = self.loc[shared_primary_keys][shared_columns]
        right_data_to_compare = right.loc[shared_primary_keys][shared_columns]

        # Return the position of the values which are not equal
        value_difference_positions = np.where(left_data_to_compare.values != right_data_to_compare.values)

        # TODO: Return functionality which shows contextual columns that did not have differences

        if len(value_difference_positions[0]) == 0:
            # Make empty file if no value differences in sets
            open(os.path.join(path, 'value_difference.txt'), 'a').close()
        else:
            # If differences, compare values for only columns/rows with with at least one difference
            rows_with_different_values = list(np.unique(value_difference_positions[0]))
            # Include primary key plus with columns with differences
            primary_key_iloc = list(left_data_to_compare.columns).index(right.primary_key)
            columns_with_different_values = [primary_key_iloc] + (list(np.unique(value_difference_positions[1])))
            columns_wout_different_values = list(
                set(range(len(shared_columns))) - set(np.unique(value_difference_positions[1])))
            panel_left_right = pd.Panel(
                {"left": left_data_to_compare.iloc[rows_with_different_values, columns_with_different_values], \
                 "right": right_data_to_compare.iloc[rows_with_different_values, columns_with_different_values]})
            different_values = panel_left_right.apply(dc_util.compare_value_pair, axis=0)
            different_values.to_csv(os.path.join(path, 'value_difference.txt'),
                                    sep='\t')

    # TODO: Add iterative/interactive functionality to show chunk of values with different values



    @property
    def _constructor(self):
        return CompareDataFrame


            #
    # def _copy_attrs(self, df):
    #     for attr in self._attributes_.split(","):
    #         df.__dict__[attr] = getattr(self, attr, None)
    #
    #
    # def __init__(self, primary_key=None):
    #     super(CompareDataFrame, self).__init__(self, primary_key)
    #     #super().__init__(data, index, columns, dtype, copy)
    #
    #     if primary_key is None:
    #         self.primary_key = self.columns[0]
    #     else:
    #         self.primary_key = primary_key
    #
    #     # Column or columns considered the primary key
    #     # Defaults to first column loaded
    #     # self.primary_key = self.columns[0]
    #     self._index_on_primary_key()
    #
