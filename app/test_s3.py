import boto3
import pytest


@pytest.fixture(scope='session')
def s3_client():
    return boto3.client('s3')


def test_s3_bucket_exists(s3_client):
    bucket_name = 'formula-statistics'
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    assert bucket_name in buckets, f"S3 bucket '{bucket_name}' not found"
