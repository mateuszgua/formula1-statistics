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
            sql_use_db = (
                f"""USE {Config.DB_NEW_NAME}""")
            self.cursor.execute(sql_use_db)

            match table_name:
                case "circuits":
                    sql_create_table = (
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
                    self.cursor.execute(sql_create_table)

                case "constructor_results":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[constructor_results]') AND type in (N'U'))
                        CREATE TABLE constructor_results (
                                constructorResultsId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                constructorId int NOT NULL,
                                points float(5) NOT NULL,
                                status varchar(2)
                                )""")
                    self.cursor.execute(sql_create_table)

                case "constructors":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[constructors]') AND type in (N'U'))
                        CREATE TABLE constructors (
                                constructorId int NOT NULL PRIMARY KEY,
                                constructorRef varchar(64) NOT NULL,
                                name varchar(64) NOT NULL,
                                nationality varchar(64) NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "constructor_standings":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[constructor_standings]') AND type in (N'U'))
                        CREATE TABLE constructor_standings (
                                constructorStandingsId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                constructorId int NOT NULL,
                                points float(5) NOT NULL,
                                position int NOT NULL,
                                wins int NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "drivers":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[drivers]') AND type in (N'U'))
                        CREATE TABLE drivers (
                                driverId int NOT NULL PRIMARY KEY,
                                driverRef varchar(64) NOT NULL,
                                number int,
                                code varchar(5),
                                forename varchar(30) NOT NULL,
                                surname varchar(30) NOT NULL,
                                dob date NOT NULL,
                                nationality varchar(30) NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "driver_standings":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[driver_standings]') AND type in (N'U'))
                        CREATE TABLE driver_standings (
                                driverStandingsId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                points float(5) NOT NULL,
                                position int NOT NULL,
                                wins int NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "lap_times":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[lap_times]') AND type in (N'U'))
                        CREATE TABLE lap_times (
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                lap int NOT NULL,
                                position int NOT NULL,
                                time varchar(20) NOT NULL,
                                milliseconds int NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "pit_stops":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pit_stops]') AND type in (N'U'))
                        CREATE TABLE pit_stops (
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                stop int NOT NULL,
                                lap int NOT NULL,
                                time varchar(10) NOT NULL,
                                duration varchar(10) NOT NULL,
                                milliseconds int NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "qualifying":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[qualifying]') AND type in (N'U'))
                        CREATE TABLE qualifying (
                                qualifyId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                constructorId int NOT NULL,
                                number int NOT NULL,
                                position int NOT NULL,
                                q1 varchar(10),
                                q2 varchar(10),
                                q3 varchar(10)
                                )""")
                    self.cursor.execute(sql_create_table)

                case "races":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[races]') AND type in (N'U'))
                        CREATE TABLE races (
                                raceId int NOT NULL PRIMARY KEY,
                                year int NOT NULL,
                                round int NOT NULL,
                                circuitId int NOT NULL,
                                name varchar(30) NOT NULL,
                                date date NOT NULL,
                                time varchar(10),
                                fp1_date date,
                                fp1_time varchar(10),
                                fp2_date date,
                                fp2_time varchar(10),
                                fp3_date date,
                                fp3_time varchar(10),
                                quali_date date,
                                quali_time varchar(10),
                                sprint_date date,
                                sprint_time varchar(10)
                                )""")
                    self.cursor.execute(sql_create_table)

                case "results":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[results]') AND type in (N'U'))
                        CREATE TABLE results (
                                resultId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                constructorId int NOT NULL,
                                number varchar(50) NOT NULL,
                                grid int NOT NULL,
                                position varchar(50),
                                positionOrder varchar(50) NOT NULL,
                                points float(5) NOT NULL,
                                laps int NOT NULL,
                                time varchar(50),
                                milliseconds int,
                                fastestLap int,
                                rank varchar(50),
                                fastestLapTime varchar(50),
                                fastestLapSpeed float(5),
                                statusId int
                                )""")
                    self.cursor.execute(sql_create_table)

                case "seasons":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[seasons]') AND type in (N'U'))
                        CREATE TABLE seasons (
                                year int NOT NULL PRIMARY KEY
                                )""")
                    self.cursor.execute(sql_create_table)

                case "sprint_results":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sprint_results]') AND type in (N'U'))
                        CREATE TABLE sprint_results (
                                resultId int NOT NULL PRIMARY KEY,
                                raceId int NOT NULL,
                                driverId int NOT NULL,
                                constructorId int NOT NULL,
                                number int NOT NULL,
                                grid int NOT NULL,
                                position int,
                                positionOrder int NOT NULL,
                                points float(5) NOT NULL,
                                laps int NOT NULL,
                                time varchar(20),
                                milliseconds int,
                                fastestLap int,
                                fastestLapTime varchar(20),
                                statusId int NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)

                case "status":
                    sql_create_table = (
                        f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[status]') AND type in (N'U'))
                        CREATE TABLE status (
                                statusId int NOT NULL PRIMARY KEY,
                                status varchar(30) NOT NULL
                                )""")
                    self.cursor.execute(sql_create_table)
        except pyodbc.Error as e:
            print(
                f"There is a problem with create tables {table_name} desc: {e}")
        else:
            print(f"Table {table_name} created successfully.")

    def drop_table(self, table_name):
        try:
            sql_delete = (
                f"""IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
                    DROP TABLE [dbo].[{table_name}]""")
            self.cursor.execute(sql_delete)
        except pyodbc.Error as e:
            print(
                f"There is a problem with delete table {table_name} desc: {e}")
        else:
            print(f"Table {table_name} deleted successfully.")
