import os
from s3_client import get_s3_client, get_bucket_name

client = get_s3_client()
BUCKET = get_bucket_name()

def test_upload():
    with open("test_file.txt", "w") as f:
        f.write("Hello from CCBD project!\n" * 100)
    blob_client = client.get_blob_client(container=BUCKET, blob="test/test_file.txt")
    with open("test_file.txt", "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    print("Upload OK :test/test_file.txt")

def test_list():
    container_client = client.get_container_client(BUCKET)
    for blob in container_client.list_blobs(name_starts_with="test/"):
        print(f"   {blob.name}  ({blob.size} bytes)")

def test_download():
    blob_client = client.get_blob_client(container=BUCKET, blob="test/test_file.txt")
    data = blob_client.download_blob().readall()
    print(f"Download OK {len(data)} bytes lus")

def cleanup():
    blob_client = client.get_blob_client(container=BUCKET, blob="test/test_file.txt")
    blob_client.delete_blob()
    os.remove("test_file.txt")
    print("Cleaning OK")

if __name__ == "__main__":
    test_upload()
    test_list()
    test_download()
    cleanup()
    print("\ok")