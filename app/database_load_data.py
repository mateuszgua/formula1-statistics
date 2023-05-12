import pyodbc

from config import Config
from my_database import MyDatabase
from transform_datas import TransformData


class FillTables:

    def __init__(self) -> None:
        self.db = MyDatabase()
        self.cursor = self.db.get_connection()

    def fill_table(self, table_name, df):
        match table_name:
            case "circuits":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[circuits] VALUES (?,?,?,?,?,?,?,?)"""
            case "constructor_results":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[constructor_results] VALUES (?,?,?,?,?)"""
            case "constructors":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[constructors] VALUES (?,?,?,?)"""
            case "constructor_standings":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[constructor_standings] VALUES (?,?,?,?,?,?)"""
            case "drivers":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[drivers] VALUES (?,?,?,?,?,?,?,?)"""
            case "driver_standings":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[driver_standings] VALUES (?,?,?,?,?,?)"""
            case "lap_times":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[lap_times] VALUES (?,?,?,?,?,?)"""
            case "pit_stops":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[pit_stops] VALUES (?,?,?,?,?,?,?)"""
            case "qualifying":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[qualifying] VALUES (?,?,?,?,?,?,?,?,?)"""
            case "races":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[races] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            case "results":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[results] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            case "seasons":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[seasons] VALUES (?)"""
            case "sprint_results":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[sprint_results] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            case "status":
                sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[status] VALUES (?,?)"""
        try:
            print(f"Load data in table: {table_name}")
            df_to_list = df.rdd.map(lambda x: x).collect()
            # self.cursor.fast_executemany = True
            self.cursor.executemany(sql, df_to_list)
            self.cursor.commit()
            self.cursor.close()
        except pyodbc.Error as e:
            print(
                f"There is a problem with load data in table {table_name} error: {e}")
        else:
            print(f"{len(df)} rows loaded successfully.")
