from get_list_from_s3 import get_list_from_s3
from get_df_spark import get_df_spark
# get data from S3 create dataframe in other file and transform data

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
    pass
    header, data = get_list_from_s3(file_path_circuits)
    dataframe = get_df_spark(header, data)
    dataframe.show()
    # Load data to db
    # return info?


# headers = next(records)

# print(type(records))
# print('headers: %s' % (headers))
# for eachRecord in records:
#     print(eachRecord)
