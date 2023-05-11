import pyodbc

from config import Config
from my_database import MyDatabase


class DatabaseManager:

    def __init__(self) -> None:
        self.db = MyDatabase()
        self.cursor = self.db.get_connection()

    def is_database_exist(self):
        try:
            sql_exist = f"""SELECT 1 FROM sys.databases WHERE name='{Config.DB_NEW_NAME}'"""
            self.cursor.execute(sql_exist)
            result = self.cursor.fetchone()

            if result[0] is None:
                self.create_database()

        except pyodbc.Error as e:
            print(f"There is a problem with database connection: {e}")

    def is_table_exist(self, table_name):
        try:
            sql_exist = f"""SELECT OBJECT_ID(N'[{Config.DB_NEW_NAME}].[dbo].[{table_name}]', N'U')"""
            self.cursor.execute(sql_exist)
            result = self.cursor.fetchone()

            if result[0] is None:
                self.create_tables(table_name)

        except pyodbc.Error as e:
            print(f"There is a problem with database connection: {e}")

    def create_database(self):
        try:
            sql_create = f"""IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='{Config.DB_NEW_NAME}') 
            CREATE DATABASE {Config.DB_NEW_NAME} USE {Config.DB_NEW_NAME}"""
            self.cursor.execute(sql_create)
        except pyodbc.Error as e:
            print(f"There is a problem with create database: {e}")
        else:
            print("Database created successfully.")

    def create_tables(self, table_name):
        try:
            table_name = None
            match table_name:
                case "circuits":
                    sql_create_table_circuits = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[circuits]') AND type in (N'U'))
                        CREATE TABLE circuits (
                                circuitId int NOT NULL PRIMARY KEY,
                                circuitRef varchar(64) NOT NULL,
                                name varchar(100) NOT NULL,
                                location varchar(64) NOT NULL,
                                country varchar(64) NOT NULL,
                                lat float(5) NOT NULL,
                                lng float(5) NOT NULL,
                                alt int
                                )""")
                case "":
                    pass
            self.cursor.execute(sql_create_table_circuits)
        except pyodbc.Error as e:
            print(f"There is a problem with create tables: {e}")
        else:
            print("Tables created successfully.")
