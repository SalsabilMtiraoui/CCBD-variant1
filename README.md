# CCBD Variant 1: CSV vs Parquet on Object Storage

**Cloud Computing and Big Data**  
**University of Neuchâtel**

## Team Members

- Salsabil Mtiraoui
- Erulan Ibraimov

---

## Project Overview

This project benchmarks **CSV** and **Apache Parquet** storage formats for a synthetic financial transaction dataset stored on object storage.

The benchmark supports three storage backends:

1. **MinIO** — running locally with Docker
2. **Azure Blob Storage**
3. **AWS S3**

The main goal is to compare how CSV and Parquet behave for big-data style workloads in terms of:

1. **Generation time** — time needed to create the dataset locally
2. **Storage size** — size occupied by CSV vs Parquet
3. **Upload throughput** — upload speed to object storage
4. **Download throughput** — download speed from object storage
5. **List time** — time needed to list objects in the bucket
6. **Query performance** — analytical query time using `pyarrow.dataset`

---

## Dataset Schema

Synthetic financial transaction events:

| Column | Type | Description |
|---|---|---|
| `ts` | timestamp | Transaction timestamp between April 1 and April 30, 2026 |
| `user_id` | int | User identifier from 1 to 1,000,000 |
| `region` | string | One of: Eurozone, US, UK, Canada, Switzerland |
| `event_type` | string | One of: payment, withdrawal, transfer, deposit |
| `value` | float | Transaction amount from event-specific log-normal distributions |
| `currency` | string | One of: USD, EUR, GBP, CAD, CHF |
| `status` | string | One of: completed, pending, failed |

---

## Dataset Sizes

| Label | Rows | Files per format |
|---|---:|---:|
| S | 5,000,000 | 1 file |
| M | 25,000,000 | 5 files |
| L | 100,000,000 | 20 files |

Chunk size is fixed at 5,000,000 rows per file.

---

## Project Structure

```
CCBD-variant1/
│
├── .env                    # environment variables (S3/Azure credentials)
├── .env.example            # template for env variables
├── .gitignore
├── requirements.txt
├── docker-compose.yml
├── CCBD-variant1.code-workspace
├── README.md
├── video_report_links.md
│
# ------- MAIN RUNNABLE SCRIPTS
├── dataset_gen.py          # 1st - generate synthetic dataset (S/M/L)
├── upload.py               # 2nd - upload raw + curated to object storage
├── download.py             # 3rd - download from object storage
├── bench.py                # 4th - run full benchmark → produces results.csv
├── test_connection.py      # utility - test endpoint connection (--storage minio/azure/aws)
│
# ------- CONFIGURATION
├── config.py               # shared settings (endpoints, bucket names)
│
# ------- ANALYSIS
├── analysis.ipynb          # plots + interpretation of results.csv
├── dashboard.html          # optional visual dashboard
│
# ------- DATA (gitignored)
├── data/
│   ├── raw/
│   │   ├── data_S/csv/     # S: 1 file
│   │   ├── data_M/csv/     # M: 5 files
│   │   └── data_L/csv/     # L: 20 files
│   ├── curated/
│   │   ├── data_S/parquet/ # S: 1 file
│   │   ├── data_M/parquet/ # M: 5 files
│   │   └── data_L/parquet/ # L: 20 files
│   └── tmp/                # download benchmark folder, overwritten each run to save space
│       ├── data_S/
│       ├── data_M/
│       └── data_L/
│
# ------- RESULTS
└── results/
    └── results.csv         # benchmark outputs (upload/download throughput, query times)
```

---

## Dependencies

Required software:

- Python 3.10+
- Docker and Docker Compose (only required for local MinIO)

Python packages:

```text
boto3
pyarrow
pandas
matplotlib
python-dotenv
jupyter
ipykernel
tqdm
azure-storage-blob
```

Install all dependencies:

```bash
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file in the project root (or copy `.env.example`):

```env
# -----------------------
# MinIO config
# -----------------------
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ccbd
MINIO_REGION=eu-central-1

# -----------------------
# AWS S3 config
# -----------------------
AWS_ACCESS_KEY_ID=<your_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_secret_access_key>
AWS_BUCKET=<your_bucket_name>
AWS_REGION=eu-north-1

# -----------------------
# Azure Blob config
# -----------------------
AZURE_CONNECTION_STRING=<your_azure_connection_string>
AZURE_CONTAINER=<your_container_name>
```

> **Never commit real credentials to GitHub.**

---

# How to Reproduce Results

## Option A: Local with Docker and MinIO

This is the easiest and recommended option. No cloud account required.

### Step 1 — Start MinIO

```bash
docker compose up -d
```

This starts two services:

| Service | URL |
|---|---|
| MinIO S3 API | `http://localhost:9000` |
| MinIO Web Console | `http://localhost:9001` |

### Step 2 — Create a bucket

1. Open `http://localhost:9001` in your browser
2. Log in with `minioadmin` / `minioadmin`
3. Create a bucket named `ccbd`
4. Set `MINIO_BUCKET=ccbd` in your `.env`

### Step 3 — Test the connection

```bash
python test_connection.py --storage minio
```

Expected: uploads a small test object, downloads it, deletes it, and confirms the connection works.

### Step 4 — Run the benchmark

```bash
python bench.py --storage minio --size S
python bench.py --storage minio --size M
python bench.py --storage minio --size L
```

Each command runs the full pipeline for both CSV and Parquet: generate → upload → download → list → query.

Results are saved to `results/results.csv`.

---

## Option B: Azure Blob Storage

### Step 1 — Create an Azure Storage Account

1. Go to the [Azure Portal](https://portal.azure.com)
2. Search for **Storage accounts** and create a new one
3. Choose a region (e.g. North Europe or Switzerland North)

### Step 2 — Create a container

1. Inside your storage account, go to **Data storage → Containers**
2. Create a container named `ccbd`
3. Keep access level set to **Private**

### Step 3 — Get the connection string

1. Inside your storage account, go to **Security + networking → Access keys**
2. Copy the **Connection string** for Key 1
3. Paste it into `.env`:

```env
AZURE_CONNECTION_STRING=<your_connection_string>
AZURE_CONTAINER=ccbd
```

### Step 4 — Test the connection

```bash
python test_connection.py --storage azure
```

### Step 5 — Run the benchmark

```bash
python bench.py --storage azure --size S
python bench.py --storage azure --size M
python bench.py --storage azure --size L
```

---

## Option C: AWS S3

### Step 1 — Create an S3 bucket

1. Go to the [AWS Console](https://console.aws.amazon.com/s3)
2. Click **Create bucket**
3. Give it a unique name (e.g. `ccbd-yourname`)
4. Choose a region (e.g. `eu-north-1`)
5. Leave all other settings as default and confirm

### Step 2 — Create an IAM user and get credentials

1. Go to **IAM → Users** and click **Create user**
2. Give the user a name (e.g. `ccbd-bench`)
3. On the permissions step, choose **Attach policies directly**
4. Search for and attach **AmazonS3FullAccess** (or create a restricted policy for your bucket only)
5. After the user is created, go to **Security credentials → Access keys**
6. Click **Create access key**, select **Other**, and download the key
7. Copy both the Access Key ID and Secret Access Key into `.env`:

```env
AWS_ACCESS_KEY_ID=<your_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_secret_access_key>
AWS_BUCKET=ccbd-yourname
AWS_REGION=eu-north-1
```

### Step 3 — Test the connection

```bash
python test_connection.py --storage aws
```

### Step 4 — Run the benchmark

```bash
python bench.py --storage aws --size S
python bench.py --storage aws --size M
python bench.py --storage aws --size L
```

---

## Running Individual Steps

Each step of the pipeline can also be run separately.

### Generate data only

```bash
python dataset_gen.py --label S --file-type csv --seed 42 --clean
python dataset_gen.py --label S --file-type parquet --seed 42 --clean
```

### Upload only

```bash
python upload.py --storage minio --size S --file-type csv
python upload.py --storage minio --size S --file-type parquet
```

### Download only

```bash
python download.py --storage minio --size S --file-type csv
python download.py --storage minio --size S --file-type parquet
```

### Run only specific operations

```bash
python bench.py --storage minio --size S --operation list query
python bench.py --storage minio --size S --operation generate upload
```

---

## Analytics Query

The benchmark runs a fixed analytical query on both CSV and Parquet using `pyarrow.dataset`.

**Default parameters:**

| Parameter | Default value |
|---|---|
| Region filter | `Eurozone` |
| Time range | `2026-04-10` to `2026-04-20` |
| Group by | `event_type` |
| Aggregations | `count(value)`, `mean(value)` |

The default region is `Eurozone`. We also tested with `Switzerland` to evaluate query performance across different data selectivities. To change the region:

```bash
python bench.py --storage minio --size S --region Switzerland
```

Available region options: `Eurozone`, `US`, `UK`, `Canada`, `Switzerland`.

---

## Output Format

Results are appended to `results/results.csv` after each run.

| Field | Description |
|---|---|
| `bench_ts` | Benchmark timestamp |
| `storage` | Backend: `minio`, `azure`, or `aws` |
| `size` | Dataset size: S, M, or L |
| `file_type` | `csv` or `parquet` |
| `operation` | Operations performed |
| `objects` | Number of files/objects |
| `size_mb` | Total size in MB |
| `generate_seconds` | Dataset generation time |
| `generate_mbps` | Generation throughput |
| `upload_seconds` | Upload time |
| `upload_mbps` | Upload throughput |
| `download_seconds` | Download time |
| `download_mbps` | Download throughput |
| `list_seconds` | Object listing time |
| `query_seconds` | Query execution time |
| `query_rows` | Rows returned after filtering |
| `query_result_groups` | Number of groups in result |

---

## Reproducibility Notes

- Dataset generation is deterministic with `--seed 42` (default)
- Each chunk uses a derived seed so chunks are deterministic but not identical
- Parquet files are written with Snappy compression
- CSV and Parquet are stored under separate prefixes:

```
raw/data_<SIZE>/csv/
curated/data_<SIZE>/parquet/
```

- Downloaded files go to `data/tmp/data_<SIZE>/` and are overwritten on each run to save disk space
- MinIO via Docker is recommended for fully local reproducibility
- Network conditions will affect upload/download results on Azure and AWS

---

## Cleaning Up

### Stop MinIO

```bash
docker compose down
```

### Stop MinIO and delete all stored data

```bash
docker compose down -v
```

### Delete local generated data and results

Linux/macOS:

```bash
rm -rf data results
```

Windows PowerShell:

```powershell
Remove-Item -Recurse -Force data, results
```

---

## Limitations & Threats to Validity

- Results depend on local hardware, disk speed, and available RAM
- Cloud results depend on network conditions and provider-side throttling
- MinIO does not represent real WAN cloud performance but ensures reproducibility
- Synthetic data may not perfectly represent real financial workloads
- Single benchmark runs do not provide statistical confidence intervals
- Size L requires significant disk space (~15 GB) and runtime
