import boto3

from config import Config


def get_client_s3():
    config = Config

    s3 = boto3.client(
        's3',
        aws_access_key_id=config.AWS_KEY_ID,
        aws_secret_access_key=config.AWS_ACCESS_KEY,
        region_name=config.AWS_REGION_NAME
    )

    return s3
