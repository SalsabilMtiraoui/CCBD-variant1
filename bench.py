"""
bench.py — Full benchmark pipeline: CSV vs Parquet on object storage
CCBD SP26 — Variant 1
Authors: Salsabil Mtiraoui + Erulan Ibraimov
Supports: MinIO (Docker) | Azure Blob Storage | AWS S3
"""

import argparse
import csv
import os
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from urllib.parse import urlparse

from config import get_bucket_name
from dataset_gen import generate_dataset
from download import download_dataset, list_remote
from upload import upload_dataset

# ─── Results schema ──────────────────────────────────────────────────
FIELDS = [
    "bench_ts", "storage", "size", "file_type",
    "objects", "size_mb",
    "generate_seconds", "generate_mbps",
    "upload_seconds",   "upload_mbps",
    "download_seconds", "download_mbps",
    "list_seconds",
    "query_region", "query_start", "query_end",
    "query_seconds", "query_rows", "query_groups",
]

RESULTS_FILE = Path("results/results.csv")

# ─── Query parameters (same for all runs) ────────────────────────────
QUERY_REGION = "Europe"
QUERY_START  = "2022-01-01"
QUERY_END    = "2022-06-30"

# ─── Paths ───────────────────────────────────────────────────────────

def local_dir(size: str, file_type: str) -> Path:
    if file_type == "csv":
        return Path(f"data/raw/data_{size}/csv")
    return Path(f"data/curated/data_{size}/parquet")


def remote_prefix(size: str, file_type: str) -> str:
    if file_type == "csv":
        return f"raw/data_{size}/csv"
    return f"curated/data_{size}/parquet"


def folder_stats(path: Path):
    files = [p for p in path.rglob("*") if p.is_file()] if path.exists() else []
    total_bytes = sum(p.stat().st_size for p in files)
    return len(files), total_bytes, total_bytes / 1024**2


# ─── Remote filesystem for direct query ──────────────────────────────

def _parse_azure_conn(conn_str: str) -> dict:
    return {k: v for item in conn_str.split(";")
            for k, v in ([item.split("=", 1)] if "=" in item else [[None, None]])}


def get_remote_filesystem(storage: str):
    """Return a (pyarrow.filesystem, base_path) for direct remote query."""
    bucket = get_bucket_name(storage)

    if storage in ("minio", "aws"):
        kwargs = {
            "access_key": os.getenv("MINIO_ACCESS_KEY" if storage == "minio"
                                    else "AWS_ACCESS_KEY_ID"),
            "secret_key": os.getenv("MINIO_SECRET_KEY" if storage == "minio"
                                    else "AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("MINIO_REGION", "eu-central-1") if storage == "minio"
                      else os.getenv("AWS_REGION"),
        }
        endpoint = os.getenv("MINIO_ENDPOINT") if storage == "minio" else None
        if endpoint:
            parsed = urlparse(endpoint)
            kwargs["endpoint_override"] = parsed.netloc or parsed.path
            kwargs["scheme"] = parsed.scheme or "http"
        return pafs.S3FileSystem(**kwargs), bucket

    if storage == "azure":
        conn = _parse_azure_conn(os.getenv("AZURE_CONNECTION_STRING", ""))
        fs = pafs.AzureFileSystem(
            account_name=conn.get("AccountName"),
            account_key=conn.get("AccountKey"),
        )
        return fs, bucket

    raise ValueError(f"Unknown storage: {storage!r}")


def csv_schema_format():
    """PyArrow CSV format with explicit schema for remote reading."""
    schema = pa.schema([
        ("ts",         pa.timestamp("s")),
        ("user_id",    pa.int64()),
        ("region",     pa.string()),
        ("event_type", pa.string()),
        ("value",      pa.float64()),
        ("currency",   pa.string()),
        ("status",     pa.string()),
    ])
    return ds.CsvFileFormat(
        convert_options=pacsv.ConvertOptions(column_types=schema)
    )


# ─── Benchmark steps ─────────────────────────────────────────────────

def bench_generate(size: str, file_type: str, clean: bool = False) -> dict:
    info = generate_dataset(label=size, file_type=file_type,
                            clean=clean, multi_file=False)
    path = local_dir(size, file_type)
    objects, total_bytes, size_mb = folder_stats(path)
    return {
        "objects":          objects,
        "size_mb":          round(size_mb, 2),
        "generate_seconds": info["generate_seconds"],
        "generate_mbps":    info["generate_mbps"],
    }


def bench_upload(storage: str, size: str, file_type: str,
                 clean: bool = False) -> dict:
    total_bytes, objects, seconds, speed, _ = upload_dataset(
        storage=storage, size=size, file_type=file_type,
        clean=clean, verbose=True,
    )
    return {
        "objects":        objects,
        "size_mb":        round(total_bytes / 1024**2, 2),
        "upload_seconds": round(seconds, 3),
        "upload_mbps":    round(speed, 2),
    }


def bench_download(storage: str, size: str, file_type: str,
                   clean: bool = False) -> dict:
    total_bytes, objects, seconds, speed, _ = download_dataset(
        storage=storage, size=size, file_type=file_type,
        clean=clean, verbose=True,
    )
    return {
        "objects":          objects,
        "size_mb":          round(total_bytes / 1024**2, 2),
        "download_seconds": round(seconds, 3),
        "download_mbps":    round(speed, 2),
    }


def bench_list(storage: str, size: str, file_type: str) -> dict:
    prefix = remote_prefix(size, file_type)
    start = time.time()
    objects = list_remote(storage, prefix)
    elapsed = time.time() - start
    total_bytes = sum(o["size"] for o in objects)
    print(f"  Listed {len(objects)} objects in {elapsed:.3f}s")
    return {
        "objects":      len(objects),
        "size_mb":      round(total_bytes / 1024**2, 2),
        "list_seconds": round(elapsed, 4),
    }


def bench_query(storage: str, size: str, file_type: str,
                region: str = QUERY_REGION,
                start_ts: str = QUERY_START,
                end_ts: str   = QUERY_END) -> dict:
    """
    Run the fixed analytics query directly on remote storage.
    Uses PyArrow filesystem — no need to download first.
    """
    filesystem, bucket = get_remote_filesystem(storage)
    prefix = remote_prefix(size, file_type)
    path   = f"{bucket}/{prefix}"
    fmt    = "parquet" if file_type == "parquet" else csv_schema_format()

    dataset = ds.dataset(path, filesystem=filesystem, format=fmt)

    filt = (
        (ds.field("region") == region)
        & (ds.field("ts") >= pd.Timestamp(start_ts).to_pydatetime())
        & (ds.field("ts") <  pd.Timestamp(end_ts).to_pydatetime())
    )

    start = time.time()
    table = dataset.to_table(columns=["event_type", "value"], filter=filt)
    grouped = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean"),
    ])
    elapsed = time.time() - start

    print(f"\n  Query done in {elapsed:.2f}s | {table.num_rows:,} rows matched")
    print(grouped.to_pandas().to_string(index=False))

    return {
        "query_region":  region,
        "query_start":   start_ts,
        "query_end":     end_ts,
        "query_seconds": round(elapsed, 4),
        "query_rows":    table.num_rows,
        "query_groups":  grouped.num_rows,
    }


# ─── Results writer ──────────────────────────────────────────────────

def write_results(row: dict):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_header = not RESULTS_FILE.exists()
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in FIELDS})
    print(f"\nResults saved → {RESULTS_FILE}")


# ─── Runner ──────────────────────────────────────────────────────────

def run_benchmark(
    storage:    str  = "minio",
    size:       str  = "S",
    file_type:  str  = "csv",
    operations: list = None,
    clean:      bool = False,
) -> dict:
    ALL_OPS = ["generate", "upload", "download", "list", "query"]
    ops = ALL_OPS if (operations is None or "all" in operations) else operations
    size = size.upper()

    row = {
        "bench_ts":  pd.Timestamp.now("UTC").isoformat(),
        "storage":   storage,
        "size":      size,
        "file_type": file_type,
    }

    print(f"\n{'='*60}")
    print(f"BENCHMARK | storage={storage} | size={size} | format={file_type}")
    print(f"Operations: {', '.join(ops)}")
    print(f"{'='*60}")

    if "generate" in ops:
        print("\n[1/5] Generating data...")
        row.update(bench_generate(size, file_type, clean=clean))

    if "upload" in ops:
        print("\n[2/5] Uploading...")
        row.update(bench_upload(storage, size, file_type, clean=clean))

    if "download" in ops:
        print("\n[3/5] Downloading...")
        row.update(bench_download(storage, size, file_type, clean=clean))

    if "list" in ops:
        print("\n[4/5] Listing...")
        row.update(bench_list(storage, size, file_type))

    if "query" in ops:
        print("\n[5/5] Running analytics query...")
        row.update(bench_query(storage, size, file_type))

    write_results(row)
    return row


# ─── CLI ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CCBD SP26 Variant 1 — Benchmark CSV vs Parquet"
    )
    parser.add_argument("--storage",   default="minio",
                        choices=["minio", "aws", "azure"])
    parser.add_argument("--size",      default="S",
                        choices=["S", "M", "L"])
    parser.add_argument("--file-type", default="both",
                        choices=["both", "csv", "parquet"])
    parser.add_argument("--operation", nargs="+", default=["all"],
                        choices=["all", "generate", "upload",
                                 "download", "list", "query"])
    parser.add_argument("--clean",     action="store_true")
    args = parser.parse_args()

    file_types = ["csv", "parquet"] if args.file_type == "both" else [args.file_type]

    for ft in file_types:
        run_benchmark(
            storage    = args.storage,
            size       = args.size,
            file_type  = ft,
            operations = args.operation,
            clean      = args.clean,
        )


if __name__ == "__main__":
    main()