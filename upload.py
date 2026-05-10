from pathlib import Path
import time
import argparse

from config import get_bucket_name, get_s3_client, get_azure_container_client


def dataset_path(size, file_type):
    if file_type == "csv":
        return Path(f"data/raw/data_{size}/csv"), f"raw/data_{size}/csv"
    return Path(f"data/curated/data_{size}/parquet"), f"curated/data_{size}/parquet"


def list_remote(storage, prefix):
    if storage in ["minio", "aws"]:
        s3, bucket = get_s3_client(storage), get_bucket_name(storage)
        return [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])]

    container = get_azure_container_client()
    return [b.name for b in container.list_blobs(name_starts_with=prefix)]


def delete_prefix(storage, prefix):
    names = list_remote(storage, prefix)

    if storage in ["minio", "aws"]:
        s3, bucket = get_s3_client(storage), get_bucket_name(storage)
        for name in names:
            s3.delete_object(Bucket=bucket, Key=name)
    else:
        container = get_azure_container_client()
        for name in names:
            container.delete_blob(name)

    return len(names)


def upload_file(storage, local_file, remote_name):
    if storage in ["minio", "aws"]:
        s3, bucket = get_s3_client(storage), get_bucket_name(storage)
        s3.upload_file(str(local_file), bucket, remote_name)
    else:
        container = get_azure_container_client()
        blob_client = container.get_blob_client(remote_name)
        with open(local_file, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)


def upload_dataset(storage, size, file_type, clean=False, verbose=True):
    local_dir, prefix = dataset_path(size, file_type)

    if clean:
        deleted = delete_prefix(storage, prefix)
        if verbose:
            print(f"Deleted {deleted} old objects from {prefix} \n")

    files = [p for p in local_dir.rglob("*") if p.is_file()]
    total_bytes = sum(p.stat().st_size for p in files)

    start = time.time()

    for p in files:
        remote_name = f"{prefix}/{p.relative_to(local_dir).as_posix()}"
        file_start = time.time()
        upload_file(storage, p, remote_name)

        if verbose:
            sec = time.time() - file_start
            mb = p.stat().st_size / (1024 ** 2)
            print(f"Uploaded {p.name}: {mb:.1f} MB in {sec:.2f}s")

    seconds = time.time() - start
    mbps = (total_bytes / (1024 ** 2)) / seconds if seconds else 0

    return total_bytes, len(files), seconds, mbps, prefix


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="minio", choices=["minio", "aws", "azure"])
    parser.add_argument("--size", default="S")
    parser.add_argument("--file-type", default="csv", choices=["csv", "parquet"])
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--quiet", action="store_true")

    args = parser.parse_args()

    total_bytes, n_files, seconds, mbps, prefix = upload_dataset(
        storage=args.storage,
        size=args.size.upper(),
        file_type=args.file_type,
        clean=args.clean,
        verbose=not args.quiet,
    )

    print("\nUpload complete:")
    print(f"  Prefix  : {prefix}")
    print(f"  Files   : {n_files}")
    print(f"  Size    : {total_bytes / (1024 ** 2):.1f} MB")
    print(f"  Time    : {seconds:.2f}s")
    print(f"  Speed   : {mbps:.1f} MB/s")