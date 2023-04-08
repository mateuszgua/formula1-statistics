import csv

from get_s3bucket_client import get_client_s3
from config import Config


def get_object_from_s3(file_path):
    config = Config()
    s3 = get_client_s3()

    obj = s3.get_object(Bucket=config.AWS_BUCKET,
                        Key=file_path)
    data = obj['Body'].read().decode('utf-8').splitlines()
    records = csv.reader(data)

    return records
