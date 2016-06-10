from pandas import DataFrame
import os

class CompareDataFrame(DataFrame):
    """
    A special frame with a primary key for comparison
    """

    # normal properties
    _metadata = ['primary_key']

    def __init__(self, *args, **kw):
        super(CompareDataFrame, self).__init__(*args, **kw)
        self.primary_key = self.columns[0]
        self._index_on_primary_key()

    @property
    def _constructor(self):
        return CompareDataFrame

    def _index_on_primary_key(self):
        self.index = self[self.primary_key]
        self.sort_index(inplace=True)

    def set_primary_key(self, column_as_primary_key):
        assert column_as_primary_key in self.columns, "Must be single existing column in dataframe"
        self.primary_key = column_as_primary_key
        self._index_on_primary_key()

    def dump_difference(self, right, limit=100, path='.'):
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

    def show_difference(self, right):
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
