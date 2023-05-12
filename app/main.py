from my_database import MyDatabase
from database_manager import DatabaseManager
from database_reader import DatabaseReader
from transform_datas import TransformData
from database_load_data import FillTables

manager = DatabaseManager()

tables_names = ['circuits', 'constructor_results', 'constructors', 'constructor_standings', 'drivers', 'driver_standings', 'lap_times',
                'pit_stops', 'qualifying', 'races', 'results', 'seasons', 'sprint_results', 'status']
# tables_names = ['results']
# manager.drop_table('results')
try:
    manager.is_database_exist()

    for table_name in tables_names:
        manager.is_table_exist(table_name)

    db_reader = DatabaseReader()
    for table_name in tables_names:
        table_is_empty = db_reader.is_empty(table_name)

        if table_is_empty is True:
            transform_data = TransformData()
            load_data = FillTables()
            match table_name:
                case "circuits":
                    dataframe = transform_data.transform_circuits()
                case "constructor_results":
                    dataframe = transform_data.transform_constructor_results()
                case "constructors":
                    dataframe = transform_data.transform_constructors()
                case "constructor_standings":
                    dataframe = transform_data.transform_constructor_standings()
                case "drivers":
                    dataframe = transform_data.transform_drivers()
                case "driver_standings":
                    dataframe = transform_data.transform_driver_standings()
                case "lap_times":
                    dataframe = transform_data.transform_lap_times()
                case "pit_stops":
                    dataframe = transform_data.transform_pit_stops()
                case "qualifying":
                    dataframe = transform_data.transform_qualifying()
                case "races":
                    dataframe = transform_data.transform_races()
                case "results":
                    dataframe = transform_data.transform_results()
                case "seasons":
                    dataframe = transform_data.transform_seasons()
                case "sprint_results":
                    dataframe = transform_data.transform_sprint_results()
                case "status":
                    dataframe = transform_data.transform_status()
            load_data.fill_table(table_name, dataframe)
        else:
            print(f"Table: {table_name} have data")
except:
    print(f"Error in main program...")
