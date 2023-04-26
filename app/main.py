from app.get_object_from_s3 import get_object_from_s3
from my_database import MyDatabase
from database_manager import DatabaseManager
from get_csv_file import get_csv_file

file_path = 'data/source/circuits.csv'

records = get_object_from_s3(file_path)
headers = next(records)

# print(type(records))
# print('headers: %s' % (headers))
# for eachRecord in records:
#     print(eachRecord)

get_csv_file(records)

# db = MyDatabase()
# db.get_connection()

# manager = DatabaseManager()
# manager.create_database()
