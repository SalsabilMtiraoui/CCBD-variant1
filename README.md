# CCBD Variant 1: CSV vs Parquet on Object Storage
**Cloud Computing and Big Data**  
**University of Neuchâtel**

## Team Members
- Salsabil Mtiraoui
- Erulan Ibraimov

## Project Overview

This project benchmarks **CSV** and **Apache Parquet** storage formats for a synthetic financial transaction dataset stored on object storage.

The benchmark supports three storage backends:

1. **MinIO** running locally with Docker
2. **Azure Blob Storage**
3. **AWS S3**

The main goal is to compare how CSV and Parquet behave for big-data style workloads in terms of:

1. **Generation time**: time needed to create the dataset locally
2. **Storage size**: size occupied by CSV vs Parquet
3. **Upload throughput**: upload speed to object storage
4. **Download throughput**: download speed from object storage
5. **List time**: time needed to list objects in the bucket/container
6. **Query performance**: analytical query time using `pyarrow.dataset`

The most important part of this project is reproducibility. The instructions below explain how to run the benchmark locally with Docker/MinIO and how to run the same benchmark on Azure Blob Storage or AWS S3.

---

## Dataset Schema

Synthetic financial transaction events:

| Column | Type | Description |
|---|---|---|
| `ts` | timestamp | Transaction timestamp between April 1 and April 30, 2026 |
| `user_id` | int | User identifier from 1 to 1,000,000 |
| `region` | string | Region: Eurozone, US, UK, Canada, Switzerland |
| `event_type` | string | Transaction type: payment, withdrawal, transfer, deposit |
| `value` | float | Transaction amount generated from event-specific log-normal distributions |
| `currency` | string | Currency: USD, EUR, GBP, CAD, CHF |
| `status` | string | Transaction status: completed, pending, failed |

---

## Dataset Sizes

| Label | Rows | Default chunk size | Output |
|---|---:|---:|---|
| S | 5,000,000 | 5,000,000 rows | 1 file per format |
| M | 25,000,000 | 5,000,000 rows | 5 files per format |
| L | 100,000,000 | 5,000,000 rows | 20 files per format |

The default chunk size is `5,000,000` rows. This keeps memory usage reasonable and creates multiple objects for larger datasets.

Generated files are stored locally as:

```text
data/raw/data_<SIZE>/csv/
data/curated/data_<SIZE>/parquet/
```

Downloaded files are stored in:

```text
data/tmp/data_<SIZE>/<file_type>/
```

Benchmark results are appended to:

```text
results/results.csv
```

---

## Dependencies

Required software:

- Python 3.10+
- Docker and Docker Compose, only required for local MinIO
- A terminal or shell
- Optional: Jupyter or VS Code for analysis notebooks

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

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Project Structure

```text
ccbd/
├── docker-compose.yml
├── requirements.txt
├── .env
└── src/
    ├── bench.py
    ├── config.py
    ├── dataset_gen.py
    ├── download.py
    ├── test_connection.py
    └── upload.py
```

Main scripts:

| Script | Purpose |
|---|---|
| `src/test_connection.py` | Tests MinIO, AWS S3, or Azure Blob access |
| `src/dataset_gen.py` | Generates synthetic CSV or Parquet datasets |
| `src/upload.py` | Uploads generated files to object storage |
| `src/download.py` | Downloads files from object storage |
| `src/bench.py` | Runs the full benchmark pipeline and writes results |
| `src/config.py` | Loads credentials and creates MinIO/AWS/Azure clients |

---

## Configuration

Create a `.env` file in the project root.

Example:

```env
# -----------------------
# MinIO config
# -----------------------
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=<minio_bucket>
MINIO_REGION=eu-central-1

# -----------------------
# AWS S3 config
# -----------------------
AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
AWS_BUCKET=<your_s3_bucket_name>
AWS_REGION=eu-north-1

# -----------------------
# Azure Blob config
# -----------------------
AZURE_CONNECTION_STRING=<your_azure_connection_string>
AZURE_CONTAINER=<your_container_name>
```

Do not commit real cloud credentials to GitHub.

---

# How to Reproduce Results

## Option A: Reproduce Locally with Docker and MinIO

This is the easiest and recommended reproducibility path because it does not require a real cloud account.

### Step 1: Start MinIO with Docker

From the project root, run:

```bash
docker compose up -d
```

This starts MinIO with:

| Service | URL |
|---|---|
| MinIO S3 API | `http://localhost:9000` |
| MinIO Web Console | `http://localhost:9001` |

Default login credentials:

```text
Username: minioadmin
Password: minioadmin
```

### Step 2: Create a MinIO bucket

Open the MinIO console:

```text
http://localhost:9001
```

Then:

1. Log in with `minioadmin` / `minioadmin`
2. Create a bucket, for example `ccbd`
3. Put the same bucket name in `.env`:

```env
MINIO_BUCKET=ccbd
```

### Step 3: Test the MinIO connection

```bash
python src/test_connection.py --storage minio
```

Expected behavior:

- Uploads a small test object
- Downloads it again
- Deletes it
- Prints that the MinIO connection works

### Step 4: Run the full benchmark on MinIO

Run the benchmark for both CSV and Parquet:

```bash
python src/bench.py --storage minio --size S 
```

For larger sizes:

```bash
python src/bench.py --storage minio --size M 
python src/bench.py --storage minio --size L 
```

The command performs:

1. Dataset generation
2. Upload to MinIO
3. Download from MinIO
4. Object listing
5. Query execution
6. Result writing to `results/results.csv`

---

## Option B: Reproduce on Azure Blob Storage

Azure is almost the same workflow as MinIO. The main difference is that Azure uses a **connection string** and a **container** instead of the S3-style endpoint, access key, secret key, and bucket.

### Step 1: Create an Azure Storage Account

In the Azure Portal:

1. Go to **Storage accounts**
2. Create a new storage account
3. Choose a region, for example North Europe or Switzerland North
4. After creation, open the storage account

### Step 2: Create a Blob container

Inside the storage account:

1. Go to **Data storage → Containers**
2. Create a new container, for example `ccbd`
3. Keep the access level private

### Step 3: Get the Azure connection string

Inside the storage account:

1. Go to **Security + networking → Access keys**
2. Copy the connection string for Key 1 or Key 2
3. Paste it into `.env`

```env
AZURE_CONNECTION_STRING=<your_azure_connection_string>
AZURE_CONTAINER=ccbd
```

### Step 4: Test the Azure connection

```bash
python src/test_connection.py --storage azure
```

Expected behavior:

- Uploads a small test blob
- Downloads it again
- Deletes it
- Prints that the Azure connection works

### Step 5: Run the full benchmark on Azure

```bash
python src/bench.py --storage azure --size S 
```

For larger sizes:

```bash
python src/bench.py --storage azure --size M 
python src/bench.py --storage azure --size L 
```

The benchmark writes results to:

```text
results/results.csv
```

---

## Option C: Reproduce on AWS S3

AWS also follows the same benchmark workflow as MinIO. Since MinIO implements the S3 API, the AWS version mainly changes the credentials and removes the local endpoint.

### Step 1: Create an S3 bucket

In the AWS Console:

1. Go to **S3**
2. Create a bucket, for example `ccbd-your-name`
3. Choose a region, for example `eu-north-1`
4. Keep default private access settings

### Step 2: Create or select AWS credentials

You need an IAM user or role with permission to access the bucket.

Minimum required S3 actions:

```text
s3:PutObject
s3:GetObject
s3:DeleteObject
s3:ListBucket
```

For a simple student benchmark, an IAM user with restricted access to only the benchmark bucket is recommended.

Example IAM policy, replace the bucket name with your bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::ccbd-your-name"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::ccbd-your-name/*"
    }
  ]
}
```

### Step 3: Configure `.env` for AWS

```env
AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
AWS_BUCKET=ccbd-your-name
AWS_REGION=eu-north-1
```

### Step 4: Test the AWS connection

```bash
python src/test_connection.py --storage aws
```

Expected behavior:

- Uploads a small object to S3
- Downloads it again
- Deletes it
- Prints that the AWS connection works

### Step 5: Run the full benchmark on AWS

```bash
python src/bench.py --storage aws --size S 
```

For larger sizes:

```bash
python src/bench.py --storage aws --size M 
python src/bench.py --storage aws --size L
```

---

## Running Individual Steps

Instead of running the full benchmark, each step can be executed separately.

### Generate data only

CSV:

```bash
python src/dataset_gen.py --label S --file-type csv --seed 42 --clean
```

Parquet:

```bash
python src/dataset_gen.py --label S --file-type parquet --seed 42 --clean
```

### Upload only

```bash
python src/upload.py --storage minio --size S --file-type csv --clean
python src/upload.py --storage minio --size S --file-type parquet --clean
```

Use `--storage azure` or `--storage aws` to run the same step on Azure or AWS.

### Download only

```bash
python src/download.py --storage minio --size S --file-type csv --clean
python src/download.py --storage minio --size S --file-type parquet --clean
```

### List and query only

The `bench.py` script can run selected operations:

```bash
python src/bench.py --storage minio --size S --file-type both --operation list query
```

### Run only generation and upload

```bash
python src/bench.py --storage minio --size S --file-type both --operation generate upload --clean
```

---

## Analytics Query

The benchmark runs the same analytical query on CSV and Parquet using `pyarrow.dataset`.

Query logic:

```python
# Filter:
# region = "Eurozone"
# ts >= "2026-04-10"
# ts <  "2026-04-20"

# Aggregate:
# count(value) and mean(value)
# grouped by event_type
```

The relevant code in `src/bench.py` is:

```python
filt = (
    (ds.field("region") == region)
    & (ds.field("ts") >= pd.Timestamp(start_ts).to_pydatetime())
    & (ds.field("ts") < pd.Timestamp(end_ts).to_pydatetime())
)

table = dataset.to_table(columns=["event_type", "value"], filter=filt)

grouped = table.group_by("event_type").aggregate([
    ("value", "count"),
    ("value", "mean"),
])
```

Default query parameters:

| Parameter | Value |
|---|---|
| Region | `Eurozone` |
| Start timestamp | `2026-04-10` |
| End timestamp | `2026-04-20` |
| Group by | `event_type` |
| Aggregations | `count(value)`, `mean(value)` |

---

## Output Format

Results are saved to `results/results.csv`.

The output contains fields such as:

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
| `query_rows` | Number of rows after filtering |
| `query_result_groups` | Number of groups returned |

---

## Reproducibility Notes

- The dataset is deterministic when the same `--seed` is used.
- Default seed is `42`.
- Each generated chunk uses a derived seed based on dataset size and chunk number, so chunks remain deterministic but not identical.
- Default chunk size is `5,000,000` rows.
- Parquet files are written with Snappy compression.
- CSV and Parquet are stored under separate prefixes:

```text
raw/data_<SIZE>/csv/
curated/data_<SIZE>/parquet/
```

- MinIO was used through Docker for the local reproducibility setup.
- Azure uses `azure-storage-blob` and an Azure connection string.
- AWS and MinIO use `boto3` and the S3 API.
- Network conditions can strongly affect upload and download times on Azure and AWS.
- Size L can require significant disk space and runtime.

---

## Cleaning Up

### Stop MinIO

```bash
docker compose down
```

### Stop MinIO and remove stored data

```bash
docker compose down -v
```

### Remove local generated data and results

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

- Results depend on local hardware, disk speed, and available RAM.
- Cloud results depend on network conditions and provider-side throttling.
- MinIO is useful for reproducibility but does not represent real WAN cloud performance.
- Synthetic transactions may not perfectly represent real financial workloads.
- Single benchmark runs do not provide statistical confidence intervals.