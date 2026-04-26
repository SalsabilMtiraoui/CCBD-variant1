#Serves to complete/do the benchmarks

import os
import time
import csv
import argparse
import pandas as pd
import pyarrow.dataset as ds
from s3_client import get_s3_client, get_bucket_name
from upload import upload_dataset
from download import download_dataset
from dataset_gen import generate_dataset
from dotenv import load_dotenv

load_dotenv()
client = get_s3_client()
BUCKET = get_bucket_name()

RESULTS_DIR = "results"
RESULTS_FILE = os.path.join(RESULTS_DIR, "results.csv")

FIELDNAMES = [
    "size", "format",
    "stored_bytes", "stored_gb",
    "upload_throughput_MBs", "upload_time_s",
    "download_throughput_MBs", "download_time_s",
    "listing_time_s",
    "query_time_s", "query_rows_returned",
]

def run_listing(prefix):
    container_client = client.get_container_client(BUCKET)
    start = time.time()
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    elapsed = time.time() - start
    return elapsed, len(blobs)

def run_query_parquet(local_path):
    """Requête analytique fixe sur Parquet avec pyarrow.dataset"""
    import pyarrow.compute as pc
    dataset = ds.dataset(local_path, format="parquet")
    start = time.time()
    table = dataset.to_table(
        filter=(
            (ds.field("region") == "Europe") &
            (ds.field("ts") >= pd.Timestamp("2022-01-01")) &
            (ds.field("ts") <= pd.Timestamp("2022-06-30"))
        ),
        columns=["event_type", "value"]
    )
    df = table.to_pandas()
    result = df.groupby("event_type")["value"].agg(["count", "mean"])
    elapsed = time.time() - start
    print(f"  Parquet query: {len(df):,} rows in {elapsed:.2f}s")
    print(result)
    return elapsed, len(df)

def run_query_csv(local_path):
    """Requête analytique sur CSV — lecture par chunks pour gérer les gros fichiers"""
    start = time.time()
    results = []
    total_rows = 0

    for chunk in pd.read_csv(local_path, parse_dates=["ts"], chunksize=500_000):
        filtered = chunk[
            (chunk["region"] == "Europe") &
            (chunk["ts"] >= pd.Timestamp("2022-01-01")) &
            (chunk["ts"] <= pd.Timestamp("2022-06-30"))
        ]
        if len(filtered) > 0:
            results.append(
                filtered.groupby("event_type")["value"].agg(["sum", "count"])
            )
        total_rows += len(filtered)

    # Agrège tous les chunks
    if results:
        combined = pd.concat(results).groupby("event_type").sum()
        combined["mean"] = combined["sum"] / combined["count"]
        combined = combined[["count", "mean"]]
    else:
        combined = pd.DataFrame()

    elapsed = time.time() - start
    print(f"  CSV query done: {total_rows:,} rows in {elapsed:.2f}s")
    print(combined)
    return elapsed, total_rows

def run_benchmark(size_label, skip_generate=False, skip_upload=False):
    print(f"\n{'='*60}")
    print(f"BENCHMARK SIZE {size_label}")
    print(f"{'='*60}")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    dataset_id = f"financial_{size_label}"
    csv_path = f"data/dataset_{size_label}.csv"
    parquet_path = f"data/dataset_{size_label}.parquet"

    # 1. Génération
    if not skip_generate:
        generate_dataset(size_label)

    # 2. Upload
    if not skip_upload:
        upload_results = upload_dataset(size_label)
    else:
        upload_results = {
            "csv_upload_throughput_MBs": None,
            "csv_upload_time_s": None,
            "parquet_upload_throughput_MBs": None,
            "parquet_upload_time_s": None,
            "csv_size_bytes": os.path.getsize(csv_path) if os.path.exists(csv_path) else None,
            "parquet_size_bytes": os.path.getsize(parquet_path) if os.path.exists(parquet_path) else None,
        }

    # 3. Download
    download_results = download_dataset(size_label)

    # 4. Listing
    print(f"\nListing prefixes...")
    csv_list_time, _ = run_listing(f"raw/{dataset_id}/csv/")
    parquet_list_time, _ = run_listing(f"curated/{dataset_id}/parquet/")
    print(f"  CSV listing    : {csv_list_time:.3f}s")
    print(f"  Parquet listing: {parquet_list_time:.3f}s")

    # 5. Query — télécharge temporairement pour la requête
    print(f"\nRunning analytics queries...")

    # Download CSV pour query
    blob = client.get_blob_client(
        container=BUCKET,
        blob=f"raw/{dataset_id}/csv/dataset_{size_label}.csv"
    )
    with open("temp_query.csv", "wb") as f:
        f.write(blob.download_blob().readall())
    csv_query_time, csv_rows = run_query_csv("temp_query.csv")
    os.remove("temp_query.csv")

    # Download Parquet pour query
    blob = client.get_blob_client(
        container=BUCKET,
        blob=f"curated/{dataset_id}/parquet/dataset_{size_label}.parquet"
    )
    with open("temp_query.parquet", "wb") as f:
        f.write(blob.download_blob().readall())
    parquet_query_time, parquet_rows = run_query_parquet("temp_query.parquet")
    os.remove("temp_query.parquet")

    # 6. Écriture résultats
    write_header = not os.path.exists(RESULTS_FILE)
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()

        writer.writerow({
            "size": size_label, "format": "csv",
            "stored_bytes": upload_results.get("csv_size_bytes"),
            "stored_gb": round(upload_results.get("csv_size_bytes", 0) / 10**9, 3),
            "upload_throughput_MBs": upload_results.get("csv_upload_throughput_MBs"),
            "upload_time_s": upload_results.get("csv_upload_time_s"),
            "download_throughput_MBs": download_results.get("csv_download_throughput_MBs"),
            "download_time_s": download_results.get("csv_download_time_s"),
            "listing_time_s": round(csv_list_time, 4),
            "query_time_s": round(csv_query_time, 4),
            "query_rows_returned": csv_rows,
        })

        writer.writerow({
            "size": size_label, "format": "parquet",
            "stored_bytes": upload_results.get("parquet_size_bytes"),
            "stored_gb": round(upload_results.get("parquet_size_bytes", 0) / 10**9, 3),
            "upload_throughput_MBs": upload_results.get("parquet_upload_throughput_MBs"),
            "upload_time_s": upload_results.get("parquet_upload_time_s"),
            "download_throughput_MBs": download_results.get("parquet_download_throughput_MBs"),
            "download_time_s": download_results.get("parquet_download_time_s"),
            "listing_time_s": round(parquet_list_time, 4),
            "query_time_s": round(parquet_query_time, 4),
            "query_rows_returned": parquet_rows,
        })

    print(f"\nResults saved to {RESULTS_FILE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full benchmark CSV vs Parquet")
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()
    run_benchmark(args.size, skip_generate=args.skip_generate, skip_upload=args.skip_upload)