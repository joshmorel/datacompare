import pytest
import pyodbc
import os


@pytest.fixture(scope='session', autouse=True)
def sqlite_connection(request):
    connection_string = 'Driver=SQLite3 ODBC Driver;Database=sqlite.db'
    sql_connection = pyodbc.connect(connection_string)
    try:
        with sql_connection.cursor() as sql_cursor:
            sql_cursor.execute(
                "CREATE TABLE t1(Nums INT NOT NULL, "
                "Chars VARCHAR(255), "
                "Extra VARCHAR(255), "
                "SomeDate DATETIME, "
                "NumNulls DECIMAL(10,2), "
                "DateNulls DATETIME , "
                "DateFirstNull DATETIME, "
                "SomeDateTime DATETIME,"
                "SomeInt INT)")
            sql_cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?,?,?)", 1, 'left', 'one', '2014-04-01', None,
                               '2014-04-01', None, '2014-04-01 12:34',1)
            sql_cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?,?,?)", 2, 'b', 'two', '2014-04-02', 3.3,
                               '2014-04-02', '2014-04-01', '2014-04-01 12:34',2)
            sql_cursor.execute("INSERT INTO t1 VALUES (?,?,?,?,?,?,?,?,?)", 3, 'c', 'three', '2015-04-02', 4.7, None,
                               '2014-04-02', '2014-05-01',3)
    finally:
        sql_connection.close()

    def teardown():
        sql_connection = pyodbc.connect(connection_string)
        try:
            with sql_connection.cursor() as sql_cursor:
                sql_cursor.execute("DROP TABLE t1")
        finally:
            sql_connection.close()

    request.addfinalizer(teardown)
    return None

@pytest.fixture(scope='function')
def text_files(request):
    print('\nRunning the text file fixture set-up in {}'.format(os.getcwd()))
    pass

    def teardown():
        print('\nRunning the text file fixture teardown')
        file_list = os.listdir('.')
        for file in file_list:
            if file.split('.')[-1] == 'txt':
                os.unlink(file)

    request.addfinalizer(teardown)
    return None
