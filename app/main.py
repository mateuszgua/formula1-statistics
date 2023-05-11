from my_database import MyDatabase
from database_manager import DatabaseManager
from database_reader import DatabaseReader
from transform_datas import TransformData
from database_load_data import FillTables

manager = DatabaseManager()

tables_names = ['circuits', 'constructor_results', 'constructors', 'constructor_standings', 'drivers', 'driver_standings', 'lap_times',
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
            match table_name:
                case "circuits":
                    dataframe = transform_data.transform_circuits()
                    fill_table.fill_circuits(dataframe)

                case "constructor_results":
                    dataframe = transform_data.transform_constructor_results()
                    fill_table.fill_constructor_results(dataframe)

                case "constructors":
                    dataframe = transform_data.transform_constructors()
                    fill_table.fill_constructors(dataframe)

                case "constructor_standings":
                    dataframe = transform_data.transform_constructor_standings()
                    fill_table.fill_constructor_standings(dataframe)
                case "drivers":
                    dataframe = transform_data.transform_drivers()
                    fill_table.fill_drivers(dataframe)
                case "driver_standings":
                    dataframe = transform_data.transform_driver_standings()
                    fill_table.fill_driver_standings(dataframe)
                case "lap_times":
                    dataframe = transform_data.transform_lap_times()
                    fill_table.fill_lap_times(dataframe)
                case "pit_stops":
                    dataframe = transform_data.transform_pit_stops()
                    fill_table.fill_pit_stops(dataframe)
                case "qualifying":
                    dataframe = transform_data.transform_qualifying()
                    fill_table.fill_qualifying(dataframe)
                case "races":
                    dataframe = transform_data.transform_races()
                    fill_table.fill_races(dataframe)
                case "results":
                    dataframe = transform_data.transform_results()
                    fill_table.fill_results(dataframe)
                case "seasons":
                    dataframe = transform_data.transform_seasons()
                    fill_table.fill_seasons(dataframe)
                case "sprint_results":
                    dataframe = transform_data.transform_sprint_results()
                    fill_table.fill_sprint_results(dataframe)
                case "status":
                    dataframe = transform_data.transform_status()
                    fill_table.fill_status(dataframe)
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
