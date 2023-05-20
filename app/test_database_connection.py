import os
import pytest
import pyodbc


from dotenv import load_dotenv


@pytest.fixture(scope='session')
def sql_server_connection():
    load_dotenv('.env')

    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NEW_NAME')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_USER_PASSWORD')

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)

    yield conn

    conn.close()


def test_sql_server_connection(sql_server_connection):
    assert sql_server_connection is not None

    cursor = sql_server_connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result[0] == 1

    cursor.close()
