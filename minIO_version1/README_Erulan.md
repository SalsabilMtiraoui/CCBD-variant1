# CCBD Project — Variant 1: Parquet vs CSV

This project compares **CSV** and **Parquet** on object storage.

The benchmark measures:

- stored size
- upload throughput
- download throughput
- listing time
- query runtime

The current version uses **MinIO** as an S3-compatible storage system.

---

## Project structure

```text
V1/
├── data/
│   ├── raw/          # local CSV files
│   ├── curated/      # local Parquet files
│   └── tmp/          # downloaded benchmark files
│
├── src/
│   ├── dataset_gen.py
│   ├── s3_client.py
│   ├── test_connection.py
│   ├── upload.py
│   ├── download.py
│   └── bench.py
│
├── results/
│   └── results.csv
│
├── notebooks/
│   └── analysis.ipynb
│
├── README.md
├── docker-compose.yml
├── .env.minio
└── requirements.txt
```

---

## Dataset

The generated dataset has this schema:

```text
ts, user_id, region, event_type, value
```

Dataset sizes:

```text
S = 5,000,000 rows
M = 25,000,000 rows
L = 100,000,000 rows
```

Timestamps are generated inside April 2026.  
The random generation is reproducible using a fixed seed.

---

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Start MinIO

Start MinIO:

Configure the docker-compose.yml

```bash
docker compose up -d
```

Open the MinIO console:

```text
http://localhost:9001
```

Default login:

```text
username: minioadmin
password: minioadmin
```

Create a bucket <bucket name>

---

## Environment file

Create `.env.minio` in the project root:

```env
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET= <bucket name>
MINIO_REGION=us-east-1
```

---

## Test MinIO connection

```bash
python -m src.test_connection
```

Expected result:

```text
MinIO S3 connection works.
```

---

## Generate data

Generate one dataset:

```bash
python src/dataset_gen.py --label S --chunk-rows 1000000 --seed 42 --single-file
```

Generate all sizes:

```bash
python src/dataset_gen.py --label S --chunk-rows 5000000 --seed 42 --single-file
python src/dataset_gen.py --label M --chunk-rows 5000000 --seed 42 --single-file
python src/dataset_gen.py --label L --chunk-rows 5000000 --seed 42 --single-file
```

This creates local CSV and Parquet files:

```text
data/raw/data_S/csv/S.csv
data/curated/data_S/parquet/S.parquet
```

---

## Run benchmark

Run benchmark for one size:

```bash
python -m src.bench --storage minio --size S --file-type both
```

Run benchmark for all sizes:

```bash
python -m src.bench --storage minio --size S --file-type both
python -m src.bench --storage minio --size M --file-type both
python -m src.bench --storage minio --size L --file-type both
```

Results are saved to:

```text
results/results.csv
```

The result columns are:

```text
storage, size, file_type, objects, stored_bytes,
upload_seconds, upload_mbps,
download_seconds, download_mbps,
listing_seconds, query_seconds
```

---

## Analysis

Open:

```text
notebooks/analysis.ipynb
```

The notebook loads `results/results.csv` and compares CSV vs Parquet using tables and plots.

Main comparisons:

- storage size
- upload throughput
- download throughput
- query runtime
- Parquet size reduction
- Parquet query speedup

---

## Notes

The current benchmark uses local MinIO, so upload and download speeds are affected by local Docker, disk, and OS caching.

