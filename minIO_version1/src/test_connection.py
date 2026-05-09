from src.s3_client import get_s3_client, get_bucket_name


def main():
    s3 = get_s3_client()
    bucket = get_bucket_name()

    key = "test/hello.txt"
    body = b"Hello from CCBD MinIO test!"

    print(f"Using bucket: {bucket}")

    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"Uploaded: s3://{bucket}/{key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    downloaded = response["Body"].read()
    print(f"Downloaded content: {downloaded.decode()}")

    s3.delete_object(Bucket=bucket, Key=key)
    print("Deleted test object.")

    print("MinIO S3 connection works.")


if __name__ == "__main__":
    main()