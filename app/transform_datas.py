from get_list_from_s3 import get_list_from_s3
from get_df_spark import get_df_spark

from pyspark.sql import functions as F

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


class TransformData:

    def __init__(self) -> None:
        pass

    def transform_circuits(self):
        header, data = get_list_from_s3(file_path_circuits)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        dataframe = dataframe.withColumn("alt",
                                         F.when(dataframe["alt"] == "\\N", None).
                                         otherwise(dataframe["alt"]))
        return dataframe

    def transform_constructor_results(self):
        header, data = get_list_from_s3(file_path_constructor_results)
        dataframe = get_df_spark(header, data)
        return dataframe

    def transform_constructor_standings(self):
        header, data = get_list_from_s3(file_path_constructor_standings)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        return dataframe

    def transform_constructor_constructors(self):
        header, data = get_list_from_s3(file_path_constructors)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        return dataframe

    def transform_driver_standings(self):
        header, data = get_list_from_s3(file_path_driver_standings)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        return dataframe

    def transform_drivers(self):
        header, data = get_list_from_s3(file_path_drivers)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        return dataframe

    def transform_lap_times(self):
        header, data = get_list_from_s3(file_path_lap_times)
        dataframe = get_df_spark(header, data)
        return dataframe

    def transform_pit_stops(self):
        header, data = get_list_from_s3(file_path_pit_stops)
        dataframe = get_df_spark(header, data)
        return dataframe

    def transform_qualifying(self):
        header, data = get_list_from_s3(file_path_qualifying)
        dataframe = get_df_spark(header, data)
        return dataframe

    def transform_races(self):
        header, data = get_list_from_s3(file_path_races)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        return dataframe

    def transform_results(self):
        header, data = get_list_from_s3(file_path_results)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        return dataframe

    def transform_seasons(self):
        header, data = get_list_from_s3(file_path_seasons)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        return dataframe

    def transform_sprint_results(self):
        header, data = get_list_from_s3(file_path_sprint_results)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        return dataframe

    def transform_status(self):
        header, data = get_list_from_s3(file_path_status)
        dataframe = get_df_spark(header, data)
        return dataframe
