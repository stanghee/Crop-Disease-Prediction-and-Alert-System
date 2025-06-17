import os
import boto3
from botocore.exceptions import ClientError

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )

def ensure_bucket_exists(bucket_name):
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError:
        client.create_bucket(Bucket=bucket_name)

def upload_image(bucket_name, key, image_bytes):
    client = get_minio_client()
    client.put_object(Bucket=bucket_name, Key=key, Body=image_bytes)