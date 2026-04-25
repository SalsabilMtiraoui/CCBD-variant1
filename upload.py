import os
import time
import argparse
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
    print(f"  Uploaded {blob_key} | {file_size/10**6:.1f} MB | {throughput:.2f} MB/s | {elapsed:.1f}s")
    return throughput, elapsed, file_size

def upload_dataset(size_label, data_dir="data"):
    csv_path = os.path.join(data_dir, f"dataset_{size_label}.csv")
    parquet_path = os.path.join(data_dir, f"dataset_{size_label}.parquet")
    dataset_id = f"financial_{size_label}"

    results = {}

    print(f"\nUploading CSV ({size_label})...")
    tp, elapsed, size = upload_file(
        csv_path,
        f"raw/{dataset_id}/csv/dataset_{size_label}.csv"
    )
    results["csv_upload_throughput_MBs"] = round(tp, 3)
    results["csv_upload_time_s"] = round(elapsed, 3)
    results["csv_size_bytes"] = size

    print(f"\nUploading Parquet ({size_label})...")
    tp, elapsed, size = upload_file(
        parquet_path,
        f"curated/{dataset_id}/parquet/dataset_{size_label}.parquet"
    )
    results["parquet_upload_throughput_MBs"] = round(tp, 3)
    results["parquet_upload_time_s"] = round(elapsed, 3)
    results["parquet_size_bytes"] = size

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    args = parser.parse_args()
    upload_dataset(args.size)