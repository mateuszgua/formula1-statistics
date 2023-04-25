from config import Config
from my_database import MyDatabase


class DatabaseManager:

    def __init__(self) -> None:
        self.db = MyDatabase()
        self.cursor = self.db.get_connection()

    def create_database(self):
        try:
            sql_create = f"""IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='{Config.DB_NEW_NAME}') CREATE DATABASE {Config.DB_NEW_NAME}"""
            self.cursor.execute(sql_create)
        except:
            print(f"There is a problem with create database")
        else:
            print("Database created successfully.")

    def create_tables(self):
        try:
            pass

            # sql_create_table = (
            # f"""IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='tbl_name' and xtype='U') CREATE TABLE tbl_name (Name varchar(64) not null)""")
            # self.db.sql_execute(sql_create)
        except:
            pass
        finally:
            print("Tables created successfully.")
