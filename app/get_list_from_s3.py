import csv

from get_s3bucket_client import get_client_s3
from config import Config


def get_list_from_s3(file_path):
    try:
        s3 = get_client_s3()
        obj = s3.get_object(Bucket=Config.AWS_BUCKET,
                            Key=file_path)
        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)
        csvData = []
        csvHeaders = headers
        for eachRecord in records:
            csvData.append(eachRecord)
    except:
        print(f"Error with get list from s3 bucket")
    else:
        return csvHeaders, csvData
