import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

def get_s3_client():
    account_name = os.getenv("S3_ACCESS_KEY")
    account_key = os.getenv("S3_SECRET_KEY")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"BlobEndpoint={endpoint};"
    )
    return BlobServiceClient.from_connection_string(conn_str)

def get_bucket_name():
    return os.getenv("S3_BUCKET_NAME")