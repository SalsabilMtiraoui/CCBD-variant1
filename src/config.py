import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()


def get_bucket_name(storage):
    if storage == "minio":
        return os.getenv("MINIO_BUCKET")
    if storage == "aws":
        return os.getenv("AWS_BUCKET")
    if storage == "azure":
        return os.getenv("AZURE_CONTAINER")
    raise ValueError(f"Unknown storage backend: {storage}")


def get_s3_client(storage):
    import boto3

    if storage == "minio":
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name=os.getenv("MINIO_REGION", "eu-central-1"),
        )

    if storage == "aws":
        return boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )

    raise ValueError("S3 client only supports minio and aws")


_azure_client = None

def get_azure_container_client():
    global _azure_client
    if _azure_client is None:
        service = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_CONNECTION_STRING")
        )
        _azure_client = service.get_container_client(os.getenv("AZURE_CONTAINER"))
    return _azure_client