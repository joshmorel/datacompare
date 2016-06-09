from pandas import DataFrame


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
