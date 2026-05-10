import argparse
from config import get_bucket_name, get_s3_client, get_azure_container_client


def print_result(storage, location, downloaded):
    print(f"Uploaded, downloaded, deleted: {location}")
    print(f"Downloaded content: {downloaded.decode()}")
    print(f"{storage} connection works.")


def test_s3(storage):
    bucket, key, body = get_bucket_name(storage), "test/hello.txt", b"Hello from CCBD S3 test!"
    s3 = get_s3_client(storage)

    print(f"Using bucket: {bucket}")
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    downloaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    s3.delete_object(Bucket=bucket, Key=key)

    print_result(storage, f"s3://{bucket}/{key}", downloaded)


def test_azure():
    container_name, blob_name, body = get_bucket_name("azure"), "test/hello.txt", b"Hello from CCBD Azure test!"
    container = get_azure_container_client()
    blob = container.get_blob_client(blob_name)

    print(f"Using container: {container_name}")
    blob.upload_blob(body, overwrite=True)
    downloaded = blob.download_blob().readall()
    blob.delete_blob()

    print_result("azure", f"azure://{container_name}/{blob_name}", downloaded)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", choices=["minio", "aws", "azure"], default="minio")
    args = parser.parse_args()

    print(f"Selected storage: {args.storage}")

    if args.storage in ["minio", "aws"]:
        test_s3(args.storage)
    else:
        test_azure()


if __name__ == "__main__":
    main()