#Download S3

import os
import time
from s3_client import get_s3_client, get_bucket_name
from dotenv import load_dotenv

load_dotenv()

client = get_s3_client()
BUCKET = get_bucket_name()

def download_file(blob_key, local_path):
    blob_client = client.get_blob_client(container=BUCKET, blob=blob_key)
    props = blob_client.get_blob_properties()
    file_size = props.size
    start = time.time()
    with open(local_path, "wb") as f:
        data = blob_client.download_blob().readall()
        f.write(data)
    elapsed = time.time() - start
    throughput = (file_size / (1024 * 1024)) / elapsed
    print(f" Download: {blob_key} | {file_size/(1024*1024):.1f} MB | {throughput:.2f} MB/s")
    return throughput

if __name__ == "__main__":
    download_file("test/test_upload.txt", "test_downloaded.txt")
    os.remove("test_downloaded.txt")