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
            # df.to_sql("TableName", engine, schema="SchemaName", if_exists="append", index=False)
            print('Test1')
            sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[circuits] VALUES (?,?,?,?,?,?,?,?)"""
            # df_to_list = df.values.tolist()
            df_to_list = df.rdd.map(lambda x: x).collect()
            print(df_to_list)
            # self.cursor.fast_executemany = True
            self.cursor.executemany(sql, df_to_list)
            print('Test3')
            self.cursor.commit()
            self.cursor.close()
            # for index, row in df.iterrows():
            # self.cursor.execute("""INSERT INTO [].[dbo].[circuits] (circuitId,circuitRef,name,location,country,lat,lng,alt) values(?,?,?,?,?,?,?,?)""",
            # row.circuitId, row.circuitRef, row.name, row.location, row.country, row.lat, row.lng, row.alt)
            # self.cursor.commit()
            # self.cursor.close()
            # sql = f"""INSERT INTO [{Config.DB_NEW_NAME}].[dbo].[circuits] VALUES (%s, %s, %s, %s, %s, %s, %s, %s,)"""
            # df_to_list = df.values.tolist()
            # self.cursor.executemany(sql, df_to_list)
            # self.cursor.commit()
            # self.cursor.close()

        except pyodbc.Error as e:
            print(f"There is a problem with load data in table: {e}")
        else:
            print(f"{len(df)} rows loaded successfully.")
            # print(f" rows loaded successfully.")
