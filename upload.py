
#Upload S3 (CSV + Parquet)

import os
import time
from s3_client import get_s3_client, get_bucket_name
from dotenv import load_dotenv

load_dotenv()

client = get_s3_client()
BUCKET = get_bucket_name()

def upload_file(local_path, blob_key):
    file_size = os.path.getsize(local_path)
    blob_client = client.get_blob_client(container=BUCKET, blob=blob_key)
    start = time.time()
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    elapsed = time.time() - start
    throughput = (file_size / (1024 * 1024)) / elapsed
    print(f" Upload: {blob_key} | {file_size/(1024*1024):.1f} MB | {throughput:.2f} MB/s")
    return throughput

if __name__ == "__main__":
    # Test avec un petit fichier
    with open("test_upload.txt", "w") as f:
        f.write("test data\n" * 10000)
    upload_file("test_upload.txt", "test/test_upload.txt")
    os.remove("test_upload.txt")