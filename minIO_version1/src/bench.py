import argparse
import csv
import time
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from src.s3_client import get_s3_client, get_bucket_name
from src.upload import upload_dir
from src.download import download_prefix


def list_stats(s3, bucket, prefix):
    start = time.time()
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    seconds = time.time() - start
    total_bytes = sum(obj["Size"] for obj in objs)
    return len(objs), total_bytes, seconds


def query_parquet(path):
    start = time.time()

    dataset = ds.dataset(path, format="parquet")

    table = dataset.to_table(
        columns=["ts", "region", "event_type", "value"],
        filter=(ds.field("region") == "EU"),
    )

    df = table.to_pandas()
    result = df.groupby("event_type")["value"].agg(["count", "mean"])

    return time.time() - start


def query_csv(path):
    start = time.time()

    files = list(Path(path).rglob("*.csv"))
    dfs = []

    for f in files:
        df = pd.read_csv(f, parse_dates=["ts"])
        df = df[df["region"] == "EU"]
        dfs.append(df)

    df = pd.concat(dfs)
    result = df.groupby("event_type")["value"].agg(["count", "mean"])

    return time.time() - start


def write_result(row):
    Path("results").mkdir(exist_ok=True)
    path = Path("results/results.csv")
    exists = path.exists()

    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def bench_one(storage, size, file_type):
    s3 = get_s3_client()
    bucket = get_bucket_name()

    if file_type == "csv":
        local_src = f"data/raw/data_{size}/csv"
        prefix = f"raw/data_{size}/csv"
        local_download = f"data/tmp/{storage}/data_{size}/csv"
    else:
        local_src = f"data/curated/data_{size}/parquet"
        prefix = f"curated/data_{size}/parquet"
        local_download = f"data/tmp/{storage}/data_{size}/parquet"

    upload_bytes, upload_objects, upload_sec, upload_mbps = upload_dir(
        s3, bucket, local_src, prefix, clean=True
    )

    objects, stored_bytes, listing_sec = list_stats(s3, bucket, prefix)

    download_bytes, download_objects, download_sec, download_mbps = download_prefix(
        s3, bucket, prefix, local_download, clean=True
    )

    if file_type == "parquet":
        query_sec = query_parquet(local_download)
    else:
        query_sec = query_csv(local_download)

    row = {
        "storage": storage,
        "size": size,
        "file_type": file_type,
        "objects": objects,
        "stored_bytes": stored_bytes,
        "upload_seconds": round(upload_sec, 3),
        "upload_mbps": round(upload_mbps, 3),
        "download_seconds": round(download_sec, 3),
        "download_mbps": round(download_mbps, 3),
        "listing_seconds": round(listing_sec, 3),
        "query_seconds": round(query_sec, 3),
    }

    write_result(row)
    print(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="minio")
    parser.add_argument("--size", default="S")
    parser.add_argument("--file-type", choices=["csv", "parquet", "both"], default="both")
    args = parser.parse_args()

    if args.file_type in ["csv", "both"]:
        bench_one(args.storage, args.size, "csv")

    if args.file_type in ["parquet", "both"]:
        bench_one(args.storage, args.size, "parquet")


if __name__ == "__main__":
    main()