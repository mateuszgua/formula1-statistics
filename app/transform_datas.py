from get_list_from_s3 import get_list_from_s3
from get_df_spark import get_df_spark
# get data from S3 create dataframe in other file and transform data

file_path = 'data/source/circuits.csv'
file_path_1 = 'data/source/constructor_results.csv'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'
file_path_ = 'data/source/'

header, data = get_list_from_s3(file_path)
# headers = next(records)

# print(type(records))
# print('headers: %s' % (headers))
# for eachRecord in records:
#     print(eachRecord)

dataframe = get_df_spark(header, data)
dataframe.show()
