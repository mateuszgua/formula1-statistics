import csv
import boto3

from config import Config


config = Config

s3 = boto3.client(
    's3',
    aws_access_key_id=config.AWS_KEY_ID,
    aws_secret_access_key=config.AWS_ACCESS_KEY,
    region_name=config.AWS_REGION_NAME
)

obj = s3.get_object(Bucket=config.AWS_BUCKET,
                    Key='data/source/circuits.csv')
data = obj['Body'].read().decode('utf-8').splitlines()
records = csv.reader(data)
headers = next(records)
print('headers: %s' % (headers))
for eachRecord in records:
    print(eachRecord)
