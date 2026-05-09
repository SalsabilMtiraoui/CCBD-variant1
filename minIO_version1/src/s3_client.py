import os
import boto3
from dotenv import load_dotenv

_ENV_FILE = ".env.minio"


def _load_env():
    load_dotenv(_ENV_FILE)


def get_s3_client() -> boto3.client:
    _load_env()
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )


def get_bucket_name() -> str:
    _load_env()
    return os.getenv("S3_BUCKET_NAME")
