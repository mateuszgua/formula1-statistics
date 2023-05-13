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
        dataframe = dataframe.withColumn("status",
                                         F.when(dataframe["status"] == "\\N", None).
                                         otherwise(dataframe["status"]))
        return dataframe

    def transform_constructor_standings(self):
        header, data = get_list_from_s3(file_path_constructor_standings)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        return dataframe

    def transform_constructors(self):
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
        dataframe = dataframe.withColumn("number",
                                         F.when(dataframe["number"] == "\\N", None).
                                         otherwise(dataframe["number"]))
        dataframe = dataframe.withColumn("code",
                                         F.when(dataframe["code"] == "\\N", None).
                                         otherwise(dataframe["code"]))
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
        dataframe = dataframe.withColumn("q1",
                                         F.when(dataframe["q1"] == "\\N", None).
                                         otherwise(dataframe["q1"]))
        dataframe = dataframe.withColumn("q2",
                                         F.when(dataframe["q2"] == "\\N", None).
                                         otherwise(dataframe["q2"]))
        dataframe = dataframe.withColumn("q3",
                                         F.when(dataframe["q3"] == "\\N", None).
                                         otherwise(dataframe["q3"]))
        return dataframe

    def transform_races(self):
        header, data = get_list_from_s3(file_path_races)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("url")
        dataframe = dataframe.withColumn("time",
                                         F.when(dataframe["time"] == "\\N", None).
                                         otherwise(dataframe["time"]))
        dataframe = dataframe.withColumn("fp1_date",
                                         F.when(dataframe["fp1_date"] == "\\N", None).
                                         otherwise(dataframe["fp1_date"]))
        dataframe = dataframe.withColumn("fp1_time",
                                         F.when(dataframe["fp1_time"] == "\\N", None).
                                         otherwise(dataframe["fp1_time"]))
        dataframe = dataframe.withColumn("fp2_date",
                                         F.when(dataframe["fp2_date"] == "\\N", None).
                                         otherwise(dataframe["fp2_date"]))
        dataframe = dataframe.withColumn("fp2_time",
                                         F.when(dataframe["fp2_time"] == "\\N", None).
                                         otherwise(dataframe["fp2_time"]))
        dataframe = dataframe.withColumn("fp3_date",
                                         F.when(dataframe["fp3_date"] == "\\N", None).
                                         otherwise(dataframe["fp3_date"]))
        dataframe = dataframe.withColumn("fp3_time",
                                         F.when(dataframe["fp3_time"] == "\\N", None).
                                         otherwise(dataframe["fp3_time"]))
        dataframe = dataframe.withColumn("quali_date",
                                         F.when(dataframe["quali_date"] == "\\N", None).
                                         otherwise(dataframe["quali_date"]))
        dataframe = dataframe.withColumn("quali_time",
                                         F.when(dataframe["quali_time"] == "\\N", None).
                                         otherwise(dataframe["quali_time"]))
        dataframe = dataframe.withColumn("sprint_date",
                                         F.when(dataframe["sprint_date"] == "\\N", None).
                                         otherwise(dataframe["sprint_date"]))
        dataframe = dataframe.withColumn("sprint_time",
                                         F.when(dataframe["sprint_time"] == "\\N", None).
                                         otherwise(dataframe["sprint_time"]))
        return dataframe

    def transform_results(self):
        header, data = get_list_from_s3(file_path_results)
        dataframe = get_df_spark(header, data)
        dataframe = dataframe.drop("positionText")
        dataframe = dataframe.withColumn("position",
                                         F.when(dataframe["position"] == "\\N", None).
                                         otherwise(dataframe["position"]))
        dataframe = dataframe.withColumn("time",
                                         F.when(dataframe["time"] == "\\N", None).
                                         otherwise(dataframe["time"]))
        dataframe = dataframe.withColumn("milliseconds",
                                         F.when(dataframe["milliseconds"] == "\\N", None).
                                         otherwise(dataframe["milliseconds"]))
        dataframe = dataframe.withColumn("fastestLap",
                                         F.when(dataframe["fastestLap"] == "\\N", None).
                                         otherwise(dataframe["fastestLap"]))
        dataframe = dataframe.withColumn("rank",
                                         F.when(dataframe["rank"] == "\\N", None).
                                         otherwise(dataframe["rank"]))
        dataframe = dataframe.withColumn("fastestLapTime",
                                         F.when(dataframe["fastestLapTime"] == "\\N", None).
                                         otherwise(dataframe["fastestLapTime"]))
        dataframe = dataframe.withColumn("fastestLapSpeed",
                                         F.when(dataframe["fastestLapSpeed"] == "\\N", None).
                                         otherwise(dataframe["fastestLapSpeed"]))
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
        dataframe = dataframe.withColumn("position",
                                         F.when(dataframe["position"] == "\\N", None).
                                         otherwise(dataframe["position"]))
        dataframe = dataframe.withColumn("time",
                                         F.when(dataframe["time"] == "\\N", None).
                                         otherwise(dataframe["time"]))
        dataframe = dataframe.withColumn("milliseconds",
                                         F.when(dataframe["milliseconds"] == "\\N", None).
                                         otherwise(dataframe["milliseconds"]))
        dataframe = dataframe.withColumn("fastestLap",
                                         F.when(dataframe["fastestLap"] == "\\N", None).
                                         otherwise(dataframe["fastestLap"]))
        dataframe = dataframe.withColumn("fastestLapTime",
                                         F.when(dataframe["fastestLapTime"] == "\\N", None).
                                         otherwise(dataframe["fastestLapTime"]))
        return dataframe

    def transform_status(self):
        header, data = get_list_from_s3(file_path_status)
        dataframe = get_df_spark(header, data)
        return dataframe
