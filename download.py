import os
import time
import argparse
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
        f.write(blob_client.download_blob().readall())
    elapsed = time.time() - start
    throughput = (file_size / (1024 * 1024)) / elapsed
    print(f"  Downloaded {blob_key} | {file_size/10**6:.1f} MB | {throughput:.2f} MB/s | {elapsed:.1f}s")
    os.remove(local_path)
    return throughput, elapsed, file_size

def download_dataset(size_label):
    dataset_id = f"financial_{size_label}"
    results = {}

    print(f"\nDownloading CSV ({size_label})...")
    tp, elapsed, size = download_file(
        f"raw/{dataset_id}/csv/dataset_{size_label}.csv",
        f"temp_csv_{size_label}.csv"
    )
    results["csv_download_throughput_MBs"] = round(tp, 3)
    results["csv_download_time_s"] = round(elapsed, 3)

    print(f"\nDownloading Parquet ({size_label})...")
    tp, elapsed, size = download_file(
        f"curated/{dataset_id}/parquet/dataset_{size_label}.parquet",
        f"temp_parquet_{size_label}.parquet"
    )
    results["parquet_download_throughput_MBs"] = round(tp, 3)
    results["parquet_download_time_s"] = round(elapsed, 3)

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    args = parser.parse_args()
    download_dataset(args.size)