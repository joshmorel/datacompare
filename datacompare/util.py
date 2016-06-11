import os
import configparser as cp


def compare_value_pair(value_pair):
    """Returns difference from values in a 2-element list with left and right values delimited by pipe"""
    return str(value_pair[0]) if value_pair[0] == value_pair[1] else '{} | {}'.format(*value_pair)


def get_sql_texts(path):
    """ Creates dictionary of SQL texts from specified directory.
    Paths can then be passed to CompareDataFrame object creation sqls["left"]

    Parameters
    ----------
    path : Directory path where scripts are stored

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
            with open(file,encoding='utf-8') as fin:
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
