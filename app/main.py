from my_database import MyDatabase
from database_manager import DatabaseManager
from database_reader import DatabaseReader
from transform_datas import TransformData
from database_load_data import FillTables

manager = DatabaseManager()

tables_names = ['circuits', 'constructor_results', 'constructors', 'constructor_standings', 'drivers', 'driver_standings', 'lap_times', '',
                'pit_stops', 'qualifying', 'races', 'results', 'seasons', 'sprint_results', 'status']
try:
    manager.is_database_exist()

    for table_name in tables_names:
        manager.is_table_exist(table_name)

    db_reader = DatabaseReader()
    for table_name in tables_names:
        table_is_empty = db_reader.is_empty(table_name)

        if table_is_empty is True:
            transform_data = TransformData()
            fill_table = FillTables()

            dataframe = transform_data.transform_circuits()
            fill_table.fill_circuits(dataframe)
        else:
            print(f"Table: {table_name} have data")
except:
    pass
else:
    pass

# Get connection from db
# db = MyDatabase()
# db.get_connection()

# Create DB


# manager.create_database()
# manager.create_tables()
