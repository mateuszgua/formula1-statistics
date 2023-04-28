from get_list_from_s3 import get_list_from_s3
from my_database import MyDatabase
from database_manager import DatabaseManager
from get_df_spark import get_df_spark

file_path = 'data/source/circuits.csv'

header, data = get_list_from_s3(file_path)
# headers = next(records)

# print(type(records))
# print('headers: %s' % (headers))
# for eachRecord in records:
#     print(eachRecord)

dataframe = get_df_spark(header, data)
dataframe.show()
# db = MyDatabase()
# db.get_connection()

# manager = DatabaseManager()
# manager.create_database()
