import pyodbc

from config import Config


class MyDatabase:

    def __init__(self) -> None:
        self.database = Config.DB_NAME
        self.user = Config.DB_USERNAME
        self.password = Config.DB_USER_PASSWORD
        self.server = Config.DB_SERVER
        self.port = Config.DB_PORT
        self.connection = None
        self.cursor = None

    def get_connection(self):
        try:
            server = self.server
            database = self.database
            username = self.user
            password = self.password
            port = self.port
            driver = '{ODBC Driver 17 for SQL Server}'

            self.connection = pyodbc.connect(
                f'DRIVER={driver};SERVER={server};DATABASE={database};PORT={port};UID={username};PWD={password};LoginTimeout=30;Trusted_Connection=yes;')

            self.cursor = self.connection.cursor()

        except pyodbc.Error as ex:
            self.connection = None
            self.cursor = None
            print(f"An error occurred while connecting to the database: {ex}")

        else:
            print("Connection created successfully.")
            return self.cursor

    def close_connection(self):
        try:
            self.connection.close()
        except pyodbc.Error as ex:
            print(
                f"An error occurred while closing the database connection: {ex}")
