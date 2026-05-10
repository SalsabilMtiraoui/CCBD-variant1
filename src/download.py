from pathlib import Path
import shutil
import time
import argparse

from config import get_bucket_name, get_s3_client, get_azure_container_client


def list_remote(storage, prefix):
    if storage in ["minio", "aws"]:
        s3, bucket = get_s3_client(storage), get_bucket_name(storage)
        return [
            {"name": o["Key"], "size": o["Size"]}
            for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        ]

    container = get_azure_container_client()
    return [
        {"name": b.name, "size": b.size or 0}
        for b in container.list_blobs(name_starts_with=prefix)
    ]


def download_file(storage, remote_name, local_file):
    local_file.parent.mkdir(parents=True, exist_ok=True)

    if storage in ["minio", "aws"]:
        s3, bucket = get_s3_client(storage), get_bucket_name(storage)
        s3.download_file(bucket, remote_name, str(local_file))
    else:
        container = get_azure_container_client()
        blob_client = container.get_blob_client(remote_name)
        with open(local_file, "wb") as f:
            f.write(blob_client.download_blob().readall())


def download_dataset(storage, size, file_type, clean=False, verbose=True):
    prefix = (
        f"raw/data_{size}/csv"
        if file_type == "csv"
        else f"curated/data_{size}/parquet"
    )

    local_dir = Path(f"data/tmp/data_{size}/{file_type}")

    if clean:
        shutil.rmtree(local_dir, ignore_errors=True)
        print(f"Emptied folder: {local_dir} \n")

    local_dir.mkdir(parents=True, exist_ok=True)

    objects = list_remote(storage, prefix)
    total_bytes = sum(o["size"] for o in objects)

    start = time.time()

    for obj in objects:
        remote_name = obj["name"]
        local_file = local_dir / remote_name.replace(prefix, "").lstrip("/")

        file_start = time.time()
        download_file(storage, remote_name, local_file)

        if verbose:
            sec = time.time() - file_start
            mb = obj["size"] / (1024 ** 2)
            print(f"Downloaded {local_file.name}: {mb:.1f} MB in {sec:.2f}s")

    seconds = time.time() - start
    mbps = (total_bytes / (1024 ** 2)) / seconds if seconds else 0

    return total_bytes, len(objects), seconds, mbps, prefix



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="minio", choices=["minio", "aws", "azure"])
    parser.add_argument("--size", default="S")
    parser.add_argument("--file-type", default="csv", choices=["csv", "parquet"])
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--quiet", action="store_true")

    args = parser.parse_args()

    total_bytes, n_files, seconds, mbps, prefix = download_dataset(
        storage=args.storage,
        size=args.size.upper(),
        file_type=args.file_type,
        clean=args.clean,
        verbose=not args.quiet,
    )

    print("\nDownload complete:")
    print(f"  Prefix  : {prefix}")
    print(f"  Files   : {n_files}")
    print(f"  Size    : {total_bytes / (1024 ** 2):.1f} MB")
    print(f"  Time    : {seconds:.2f}s")
    print(f"  Speed   : {mbps:.1f} MB/s")