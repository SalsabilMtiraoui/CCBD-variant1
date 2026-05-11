import argparse
import csv
import os
import shutil
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from dataset_gen import generate_dataset
from upload import upload_dataset
from download import download_dataset, list_remote
from config import get_bucket_name


FIELDS = [
    "bench_ts", "storage", "size", "file_type", "operation",
    "objects", "size_mb",
    "generate_seconds", "generate_mbps",
    "upload_seconds", "upload_mbps",
    "download_seconds", "download_mbps",
    "list_seconds",
    "query_filter_region", "query_start_ts", "query_end_ts", "query_grouped_by",
    "query_seconds", "query_rows", "query_result_groups",
]

FILE_TYPES = ["csv", "parquet"]
OPERATIONS = ["generate", "upload", "download", "list", "query"]


# ---------- paths ----------
 
def local_dir(size, file_type):
    if file_type == "csv":
        return Path(f"data/raw/data_{size}/csv")
    return Path(f"data/curated/data_{size}/parquet")
 
 
def remote_prefix(size, file_type):
    if file_type == "csv":
        return f"raw/data_{size}/csv"
    return f"curated/data_{size}/parquet"

def parse_azure_connection_string(conn_str):
    parts = {}
    for item in conn_str.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            parts[key] = value
    return parts
 
 
def folder_stats(path):
    files = [p for p in path.rglob("*") if p.is_file()] if path.exists() else []
    total_bytes = sum(p.stat().st_size for p in files)
    return len(files), total_bytes, total_bytes / (1024 ** 2)
 
 
def mbps(total_bytes, seconds):
    return 0 if seconds == 0 else (total_bytes / (1024 ** 2)) / seconds
 
 
# ---------- CSV output ----------
 
def empty_row(bench_ts, storage, size, file_type, operation):
    row = {k: "" for k in FIELDS}
    row.update({
        "bench_ts": bench_ts,
        "storage": storage,
        "size": size,
        "file_type": file_type,
        "operation": operation,
    })
    return row
 
 
def write_results(rows, out_file):
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_file.exists()
    with open(out_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in FIELDS})
 
 
# ---------- endpoint filesystem for query ----------
 
def endpoint_dataset_path(storage, size, file_type):
    bucket_or_container = get_bucket_name(storage)
    prefix = remote_prefix(size, file_type)
 
    if storage in ("aws", "minio"):
        endpoint = os.getenv("MINIO_ENDPOINT") if storage == "minio" else None
        kwargs = {
            "access_key": os.getenv("MINIO_ACCESS_KEY") if storage == "minio" else os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_key": os.getenv("MINIO_SECRET_KEY") if storage == "minio" else os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("MINIO_REGION", "eu-central-1") if storage == "minio" else os.getenv("AWS_REGION"),
        }
        if endpoint:
            parsed = urlparse(endpoint)
            kwargs["endpoint_override"] = parsed.netloc or parsed.path
            kwargs["scheme"] = parsed.scheme or "http"
        filesystem = pafs.S3FileSystem(**kwargs)
        return filesystem, f"{bucket_or_container}/{prefix}"
 
    if storage == "azure":
        conn = parse_azure_connection_string(os.getenv("AZURE_CONNECTION_STRING", ""))

        filesystem = pafs.AzureFileSystem(
            account_name=conn.get("AccountName"),
            account_key=conn.get("AccountKey"),
        )

        return filesystem, f"{bucket_or_container}/{prefix}"
 
    raise ValueError(f"Unknown storage: {storage}")
 
 
def csv_format():
    schema = pa.schema([
        ("ts", pa.timestamp("s")),
        ("user_id", pa.int64()),
        ("region", pa.string()),
        ("event_type", pa.string()),
        ("value", pa.float64()),
        ("currency", pa.string()),
        ("status", pa.string()),
    ])
    return ds.CsvFileFormat(
        convert_options=pacsv.ConvertOptions(column_types=schema)
    )
 
 
# ---------- benchmark steps ----------
 
def bench_generate(size, file_type, clean=False, quiet=False):
    target_dir = local_dir(size, file_type)
 
    if clean:
        shutil.rmtree(target_dir, ignore_errors=True)
 
    should_generate = clean or not target_dir.exists()
 
    start = time.time()
    if should_generate:
        generate_dataset(label=size, clean=clean, file_type=file_type)
    seconds = time.time() - start
 
    objects, total_bytes, size_mb = folder_stats(target_dir)
    if not quiet:
        print(f"Generated data_{size} ({file_type}) in {seconds:.2f}s")
 
    return {
        "objects": objects,
        "size_mb": round(size_mb, 2),
        "generate_seconds": round(seconds, 3),
        "generate_mbps": round(mbps(total_bytes, seconds), 2),
    }
 
 
def bench_upload(storage, size, file_type, clean=False, quiet=False):
    total_bytes, objects, seconds, speed, _ = upload_dataset(
        storage=storage,
        size=size,
        file_type=file_type,
        clean=clean,
        verbose=not quiet,
    )
    return {
        "objects": objects,
        "size_mb": round(total_bytes / (1024 ** 2), 2),
        "upload_seconds": round(seconds, 3),
        "upload_mbps": round(speed, 2),
    }
 
 
def bench_download(storage, size, file_type, clean=False, quiet=False):
    total_bytes, objects, seconds, speed, _ = download_dataset(
        storage=storage,
        size=size,
        file_type=file_type,
        clean=clean,
        verbose=not quiet,
    )
    return {
        "objects": objects,
        "size_mb": round(total_bytes / (1024 ** 2), 2),
        "download_seconds": round(seconds, 3),
        "download_mbps": round(speed, 2),
    }
 
 
def bench_list(storage, size, file_type):
    prefix = remote_prefix(size, file_type)
    start = time.time()
    objects = list_remote(storage, prefix)
    seconds = time.time() - start
    total_bytes = sum(o["size"] for o in objects)

    print(f"Finished listing in {seconds:.2f}s")
    return {
        "objects": len(objects),
        "size_mb": round(total_bytes / (1024 ** 2), 2),
        "list_seconds": round(seconds, 3),
    }
 
 
def bench_query(
    storage,
    size,
    file_type,
    region="Eurozone",
    start_ts="2026-04-10",
    end_ts="2026-04-20",
):
    filesystem, path = endpoint_dataset_path(storage, size, file_type)
    fmt = "parquet" if file_type == "parquet" else csv_format()
    dataset = ds.dataset(path, filesystem=filesystem, format=fmt)
 
    filt = (
        (ds.field("region") == region)
        & (ds.field("ts") >= pd.Timestamp(start_ts).to_pydatetime())
        & (ds.field("ts") < pd.Timestamp(end_ts).to_pydatetime())
    )
 
    start = time.time()
    table = dataset.to_table(columns=["event_type", "value"], filter=filt)
    grouped = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean"),
    ])
    seconds = time.time() - start

    print(f"\nQuerry results are finished in {seconds:.2f}s:")
    print(grouped.to_pandas().to_string(index=False))
 
    return {
        "query_filter_region": region,
        "query_start_ts": start_ts,
        "query_end_ts": end_ts,
        "query_grouped_by": "event_type",
        "query_seconds": round(seconds, 3),
        "query_rows": table.num_rows,
        "query_result_groups": grouped.num_rows,
    }
 
 
# ---------- runner ----------
 
def run_benchmark(
    storage="minio",
    size="S",
    file_type="csv",
    operations=("all",),
    clean=False,
    out_file="results/results.csv",
    quiet=False,
):
    operations = OPERATIONS if "all" in operations else list(operations)
    size = size.upper()
 
    row = empty_row(
        bench_ts=pd.Timestamp.now("UTC").isoformat(),
        storage=storage,
        size=size,
        file_type=file_type,
        operation="+".join(operations),
    )
 
    if "generate" in operations:
        row.update(bench_generate(size, file_type, clean=clean, quiet=quiet))
 
    if "upload" in operations:
        row.update(bench_upload(storage, size, file_type, clean=clean, quiet=quiet))
 
    if "download" in operations:
        row.update(bench_download(storage, size, file_type, clean=clean, quiet=quiet))
 
    if "list" in operations:
        row.update(bench_list(storage, size, file_type))
 
    if "query" in operations:
        row.update(bench_query(storage, size, file_type))
 
    write_results([row], out_file)
 
    if not quiet:
        print(f"Saved: {storage} {size} {file_type}")
 
    return row
 
 
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="minio", choices=["minio", "aws", "azure"])
    parser.add_argument("--size", default="S")
    parser.add_argument("--file-type", default="both", choices=["both"] + FILE_TYPES)
    parser.add_argument("--operation", nargs="+", default=["all"], choices=["all"] + OPERATIONS)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--out", default="results/results.csv")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
 
    file_types = FILE_TYPES if args.file_type == "both" else [args.file_type]

    for file_type in file_types:
        run_benchmark(
            storage=args.storage,
            size=args.size.upper(),
            file_type=file_type,
            operations=args.operation,
            clean=args.clean,
            out_file=args.out,
            quiet=args.quiet,
        )
 
 
if __name__ == "__main__":
    main()