import os
import configparser as cp
import pandas as pd
import codecs

def check_equality(x, y):
    if x == y or (x in ('nan','None','NaT') and y in ('nan','None','NaT')):
        return True
    else:
        return False


def compare_value_pair(x, y):
    return (0, x) if check_equality(x, y) else (1, '{} | {}'.format(x, y))


def clean_series(s, precision=0):
    # Convert bools, ints, floats to floats then round as different sources may have different type but same meaning
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64', 'bool']
    # ETL testing should treat two Nulls as equivalent but Numpy does not, so need to convert to str
    if s.dtype in numerics:
        rounded_string = s.astype(float).round(precision).astype(str)
    else:
        rounded_string = s.astype(str)
    return rounded_string


def compare_column_values(left, right, precision=0):
    clean_left = clean_series(left, precision=precision)
    clean_right = clean_series(right, precision=precision)

    column_dump = pd.DataFrame([compare_value_pair(x, y) for (x, y) in zip(clean_left, clean_right)],
                               columns=['is_different', left.name], index=left.index)
    return column_dump


def get_sql_texts(path='.'):
    """ Creates dictionary of SQL texts from specified directory.
    Paths can then be passed to CompareDataFrame object creation sqls["left"]

    Parameters
    ----------
    path : str, default '.'
        Directory with SQL scripts. At this time, encoding must be UTF-8, ANSI or ASCII

    Returns
    -------
    get_sql_texts : dict
        Dictionary of file paths in directory with file names (before first period)
        as keys.

    Examples
    --------
    >>> sqls = get_sql_texts('.')
    """
    sql_texts = {}
    file_list = os.listdir(path)
    for file in file_list:
        if file.split('.')[-1] == 'sql':
            file_name = file.split('.')[0]
            with open(os.path.join(path, file), encoding='utf-8') as fin:
                sql_texts[file_name] = fin.read()
    return sql_texts


def get_connection_info(connection_file, connection_name):
    """

    Parameters
    ----------
    connection_file: file where connection names and strings are stored
    connection_name: the connection name in the file

    Returns
    -------

    Examples
    --------
    >>> sqlite_connection_string = get_connection_info(os.path.join('.','assets','connection_file.ini'),'sqlitedb')

    """
    parser = cp.ConfigParser()
    parser.read(connection_file)
    assert parser.has_section(
        connection_name), "The connection name was not found in: {}".format(connection_file)
    connection_string = parser.get(connection_name, 'connection_string')
    return connection_string
