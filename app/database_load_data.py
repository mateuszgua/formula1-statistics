import pyodbc

from config import Config
from my_database import MyDatabase
from transform_datas import TransformData


class FillTables:

    def __init__(self) -> None:
        self.db = MyDatabase()
        self.cursor = self.db.get_connection()

    def fill_circuits(self, df):
        try:
            sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[circuits] VALUES (?,?,?,?,?,?,?,?)"""
            df_to_list = df.rdd.map(lambda x: x).collect()
            self.cursor.executemany(sql, df_to_list)
            self.cursor.commit()
            self.cursor.close()
        except pyodbc.Error as e:
            print(f"There is a problem with load data in table: {e}")
        else:
            print(f"{len(df)} rows loaded successfully.")
