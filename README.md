# CCBD-variant1 : Parquet vs CSV Format Trade-offs
**Cloud Computing and Big Data**   
**University of Neuchâtel**

## Team Members
- Salsabil Mtiraoui
- Erulan Ibraimov 

## Project Overview

This project benchmarks **Apache Parquet** vs **CSV** as storage formats
for a synthetic financial transaction dataset stored on **Azure Blob Storage**.

We evaluate three criteria across three dataset sizes (S/M/L):
1. **Storage size**: how much space each format occupies
2. **Transfer time**: upload and download throughput (MB/s)
3. **Query performance**: analytics query time using pyarrow.dataset

### Key Results

| Criterion | Winner | Factor |
|---|---|---|
| Storage size | Parquet | 68% smaller |
| Upload throughput | Parquet | up to 15x faster |
| Download throughput | Parquet | up to 3x faster |
| Query time | Parquet | 35x to 56x faster |

---

## Dataset Schema

Synthetic financial transaction events:

| Column | Type | Description |
|---|---|---|
| ts | timestamp | Transaction timestamp (2022-2024) |
| user_id | int | User identifier (1 to 500,000) |
| region | string | Geographic region (Europe/US/Asia/Africa/MiddleEast) |
| event_type | string | Transaction type (deposit/withdrawal/transfer/payment) |
| value | float | Transaction amount in local currency (1.0 to 50,000.0) |
| currency | string | Currency (USD/EUR/GBP/CHF/JPY) |
| status | string | Transaction status (completed/pending/failed) |

---

### Dataset Sizes

| Label | Rows | CSV Size | Parquet Size |
|---|---|---|---|
| S | 5,000,000 | ~322 MB | ~102 MB |
| M | 25,000,000 | ~1.61 GB | ~512 MB |
| L | 100,000,000 | ~6.44 GB | ~2.05 GB |

Sizes S/M/L follow the alternative row-count definition from the project spec
(5M / 25M / 100M rows as suggested).

---
## Dependencies
Python 3.10+
boto3
pyarrow
pandas
matplotlib
python-dotenv
azure-storage-blob
jupyter
ipykernel

Install all dependencies:

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your Azure credentials:

```bash
cp .env.example .env
```

`.env` file:

```env
S3_ENDPOINT_URL=https://<your-storage-account>.blob.core.windows.net
S3_ACCESS_KEY=<your-storage-account-name>
S3_SECRET_KEY=<your-access-key>
S3_BUCKET_NAME=<your-container-name>
S3_REGION=northeurope
```

To find your Azure credentials:
1. Go to portal.azure.com
2. Navigate to your Storage Account
3. Security + networking → Access keys → Copy Key 1

---

## How to Reproduce Results

### Step 1: Test connection

```bash
python test_connexion.py
```

### Step 2: Run full benchmark (recommended)

```bash
# Size S (~17 minutes)
python bench.py --size S

# Size M (~2 hours)
python bench.py --size M

# Size L (~8 hours)
python bench.py --size L
```

### Step 3: Or run steps individually

```bash
# Generate data only
python dataset_gen.py --size S --seed 42

# Upload only
python upload.py --size S

# Download only
python download.py --size S

# Benchmark skipping generation and upload
python bench.py --size S --skip-generate --skip-upload
```

### Step 4: Analyse results

Open `analysis.ipynb` in Jupyter or VS Code and run all cells.

Results are saved to `results/results.csv`.

---

## Analytics Query (Fixed)

The benchmark runs this query on both CSV and Parquet:

```python
# Filter: region = Europe, ts between 2022-01-01 and 2022-06-30
# Aggregate: count and mean(value) grouped by event_type
```

With pyarrow.dataset (Parquet):
```python
dataset = ds.dataset(path, format="parquet")
table = dataset.to_table(
    filter=(
        (ds.field("region") == "Europe") &
        (ds.field("ts") >= pd.Timestamp("2022-01-01")) &
        (ds.field("ts") <= pd.Timestamp("2022-06-30"))
    ),
    columns=["event_type", "value"]
)
```

With pandas (CSV):
```python
df = pd.read_csv(path, parse_dates=["ts"], chunksize=500_000)
# filter + groupby on each chunk
```

---

## Cost Model

Based on the simplified pricing model from the project specification:

| Category | Price |
|---|---|
| Storage | 0.020 CHF/GB/month |
| PUT/LIST requests | 0.010 CHF/1,000 requests |
| GET requests | 0.001 CHF/1,000 requests |
| Egress (download) | 0.090 CHF/GB |
| Ingress (upload) | 0.000 CHF/GB (free) |

---

## Reproducibility Notes

- Random seed fixed at `seed=42` for all dataset generations
- Chunk size fixed at `500,000` rows (~50 MB RAM per chunk)
- All benchmarks run on Windows 11, Python 3.13, Azure Blob Storage (North Europe)
- Network conditions may vary, Azure throttling observed on small files
- Size L CSV query uses chunk-based reading due to RAM constraints (6.4 GB file)

---

## Limitations & Threats to Validity

- Single benchmark run per size (no statistical confidence intervals)
- Azure network conditions vary between runs
- Synthetic data distribution may not reflect real financial transactions
- Local machine RAM (8 GB) limits in-memory processing for size L CSV
- azure-storage-blob used instead of boto3 due to Azure authentication incompatibility



