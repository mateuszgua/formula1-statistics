from app.get_object_from_s3 import get_object_from_s3


file_path = 'data/source/circuits.csv'

records = get_object_from_s3(file_path)
headers = next(records)

print('headers: %s' % (headers))
for eachRecord in records:
    print(eachRecord)
