from get_list_from_s3 import get_list_from_s3
from get_df_spark import get_df_spark


file_path_circuits = 'data/source/circuits.csv'
file_path_constructor_results = 'data/source/constructor_results.csv'
file_path_constructor_standings = 'data/source/constructor_standings.csv'
file_path_constructors = 'data/source/constructors.csv'
file_path_driver_standings = 'data/source/driver_standings.csv'
file_path_drivers = 'data/source/drivers.csv'
file_path_lap_times = 'data/source/lap_times.csv'
file_path_pit_stops = 'data/source/pit_stops.csv'
file_path_qualifying = 'data/source/qualifying.csv'
file_path_races = 'data/source/races.csv'
file_path_results = 'data/source/results.csv'
file_path_seasons = 'data/source/seasons.csv'
file_path_sprint_results = 'data/source/sprint_results.csv'
file_path_status = 'data/source/status.csv'

# Check if table is empty, when True call bellow function:


def transform_circuits():
    header, data = get_list_from_s3(file_path_circuits)
    dataframe = get_df_spark(header, data)
    dataframe = dataframe.drop("url")
    dataframe.show()


def transform_constructor_results():
    header, data = get_list_from_s3(file_path_constructor_results)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_constructor_standings():
    header, data = get_list_from_s3(file_path_constructor_standings)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_constructor_constructors():
    header, data = get_list_from_s3(file_path_constructors)
    dataframe = get_df_spark(header, data)
    dataframe = dataframe.drop("url")
    dataframe.show()


def transform_driver_standings():
    header, data = get_list_from_s3(file_path_driver_standings)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_drivers():
    header, data = get_list_from_s3(file_path_drivers)
    dataframe = get_df_spark(header, data)
    dataframe = dataframe.drop("url")
    dataframe.show()


def transform_lap_times():
    header, data = get_list_from_s3(file_path_lap_times)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_pit_stops():
    header, data = get_list_from_s3(file_path_pit_stops)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_qualifying():
    header, data = get_list_from_s3(file_path_qualifying)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_races():
    header, data = get_list_from_s3(file_path_races)
    dataframe = get_df_spark(header, data)
    dataframe = dataframe.drop("url")
    dataframe.show()


def transform_results():
    header, data = get_list_from_s3(file_path_results)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_seasons():
    header, data = get_list_from_s3(file_path_seasons)
    dataframe = get_df_spark(header, data)
    dataframe = dataframe.drop("url")
    dataframe.show()


def transform_sprint_results():
    header, data = get_list_from_s3(file_path_sprint_results)
    dataframe = get_df_spark(header, data)
    dataframe.show()


def transform_status():
    header, data = get_list_from_s3(file_path_status)
    dataframe = get_df_spark(header, data)
    dataframe.show()
