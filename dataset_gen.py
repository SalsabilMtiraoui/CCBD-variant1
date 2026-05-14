import argparse
import shutil
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#  Dataset sizes (number of rows per label) 
# Using the row-count alternative from the project spec (5M / 25M / 100M)
SIZES = {"S": 5_000_000, "M": 25_000_000, "L": 100_000_000}
# Timestamp range: all transactions occur in April 2026
PERIOD_START = pd.Timestamp("2026-04-01")
PERIOD_END = pd.Timestamp("2026-04-30")

N_USERS = 1_000_000
# Region distribution: Eurozone and US are the dominant markets
REGIONS = np.array(["Eurozone", "US", "UK", "Canada", "Switzerland"])
REGION_PROBS = np.array([0.35, 0.35, 0.15, 0.10, 0.05])
# Event type distribution: payments are the most frequent transaction
EVENT_TYPES = np.array(["payment", "withdrawal", "transfer", "deposit"])
EVENT_TYPE_PROBS = np.array([0.45, 0.17, 0.20, 0.18])

# Log-normal value parameters (mu, sigma) per event type 
# Log-normal is realistic for financial amounts: most transactions are small,
# but occasional large transactions create a right-skewed distribution
VALUE_PARAMS = {
    "payment":    (3.25, 0.95),   # Small amounts (like subscriptions, online shopping)
    "withdrawal": (4.60, 0.55),   # Medium amounts, low variance
    "transfer":   (5.30, 1.25),   # Large amounts, high variance
    "deposit":    (5.55, 1.05),   # Large amounts (salaries, savings)
}
# Currency: each region has a native currency
CURRENCIES = np.array(["USD", "EUR", "GBP", "CAD", "CHF"])
REGION_TO_CURRENCY = {
    "Eurozone": "EUR",
    "US": "USD",
    "UK": "GBP",
    "Canada": "CAD",
    "Switzerland": "CHF",
}

# 10% of transactions use a foreign currency (realistic cross-border payments)
FOREIGN_CURRENCY_PROBS = np.array([0.40, 0.30, 0.10, 0.10, 0.10])
# Transaction status: most transactions complete successfully
STATUS = np.array(["completed", "pending", "failed"])
STATUS_PROBS = {
    "payment": [0.95, 0.02, 0.03],
    "withdrawal": [0.93, 0.04, 0.03],
    "transfer": [0.90, 0.05, 0.05],  # Highest failure rate (cross-border complexity)
    "deposit": [0.96, 0.03, 0.01],
}
# Precompute arrays for fast vectorised generation
EVENT_MEANS = np.array([VALUE_PARAMS[e][0] for e in EVENT_TYPES])
EVENT_STDS = np.array([VALUE_PARAMS[e][1] for e in EVENT_TYPES])
STATUS_PROB_MATRIX = np.array([STATUS_PROBS[e] for e in EVENT_TYPES])

REGION_CURRENCY_CODE = np.array([
    np.where(CURRENCIES == REGION_TO_CURRENCY[r])[0][0]
    for r in REGIONS
])



def generate_chunk(n, rng):
    """
    Generate n rows of synthetic financial transactions.
    Uses vectorised numpy operations for performance — no Python loops per row.
    All columns are generated simultaneously from the same random state.
    """
    # Uniform timestamps within the April 2026 period
    period_seconds = int((PERIOD_END - PERIOD_START).total_seconds()) + 24 * 3600
    ts = PERIOD_START + pd.to_timedelta(rng.integers(0, period_seconds, n), unit="s")

    user_id = rng.integers(1, N_USERS + 1, n)
    # Sample region and event type indices using their designed probabilities
    region_code = rng.choice(len(REGIONS), n, p=REGION_PROBS)

    event_code = rng.choice(len(EVENT_TYPES), n, p=EVENT_TYPE_PROBS)
    # Log-normal transaction values — parameters vary per event type
    value = rng.lognormal(EVENT_MEANS[event_code], EVENT_STDS[event_code]).round(2)

    # Default currency = region's native currency
    currency_code = REGION_CURRENCY_CODE[region_code].copy()
    # 10% of transactions override with a foreign currency
    use_foreign = rng.random(n) < 0.10
    currency_code[use_foreign] = rng.choice(
        len(CURRENCIES),
        use_foreign.sum(),
        p=FOREIGN_CURRENCY_PROBS,
    )

 # Status: derive from a CDF comparison to avoid per-row loops
    status_cdf  = STATUS_PROB_MATRIX[event_code].cumsum(axis=1)
    status_code = (rng.random(n)[:, None] > status_cdf[:, :2]).sum(axis=1)

    # Use pd.Categorical for low-cardinality string columns
    # that reduces memory usage and improves Parquet compression via dictionary encoding

    return pd.DataFrame({
        "ts": ts,
        "user_id": user_id,
        "region": pd.Categorical.from_codes(region_code, REGIONS),
        "event_type": pd.Categorical.from_codes(event_code, EVENT_TYPES),
        "value": value,
        "currency": pd.Categorical.from_codes(currency_code, CURRENCIES),
        "status": pd.Categorical.from_codes(status_code, STATUS),
    })


def write_part(df, raw_dir, parquet_dir, file_id, file_type):
    """
    Write one chunk to disk in the requested format.
    Returns (path, elapsed_seconds).

    CSV: raw/<size>/csv/<file_id>.csv
    Parquet: curated/<size>/parquet/<file_id>.parquet (Snappy compression)
    """
    if file_type == "csv":
        raw_dir.mkdir(parents=True, exist_ok=True)

        csv_path = raw_dir / f"{file_id}.csv"

        start = time.time()
        df.to_csv(csv_path, index=False)
        csv_sec = time.time() - start

        return csv_path, csv_sec

    if file_type == "parquet":
        parquet_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = parquet_dir / f"{file_id}.parquet"

        start = time.time()
        # preserve_index=False avoids writing the pandas RangeIndex as a column
        table = pa.Table.from_pandas(df, preserve_index=False)
        # Snappy: fast compression with good ratio — default for Parquet in production
        pq.write_table(table, parquet_path, compression="snappy")
        parquet_sec = time.time() - start

        return parquet_path, parquet_sec

    raise ValueError("--file-type must be either 'csv' or 'parquet'.")


def gb_size(path):
    return path.stat().st_size / (1024 ** 3)


def generate_dataset(label="S", rows=None, chunk_rows=5_000_000, seed=42, clean=False, file_type="csv"):
    """
    Generate a full synthetic financial dataset in chunks.

    Chunked generation avoids loading all rows into RAM at once:
    - S (5M rows):   1 chunk  → 1 file
    - M (25M rows):  5 chunks → 5 files
    - L (100M rows): 20 chunks → 20 files

    Each chunk uses a deterministic but unique seed derived from the base seed,
    the dataset size, and the chunk index — ensuring reproducibility without
    identical chunks.

    File size policy: 5,000,000 rows/chunk ≈ 300 MB CSV / 80 MB Parquet per file.
    This keeps individual files manageable for upload and avoids the small-files problem.
    """
    label = label.upper()
    file_type = file_type.lower()

    if file_type not in ("csv", "parquet"):
        raise ValueError("--file-type must be either 'csv' or 'parquet'.")

    rows = rows or SIZES.get(label)
    if rows is None:
        raise ValueError("Unknown label. Use --rows for custom labels.")

    if chunk_rows <= 0:
        raise ValueError("--chunk-rows must be greater than 0.")

    raw_dir = Path(f"data/raw/data_{label}/csv")
    parquet_dir = Path(f"data/curated/data_{label}/parquet")

    if clean:
        # Remove only the target format's directory to avoid deleting the other
        if file_type == "csv":
            shutil.rmtree(raw_dir, ignore_errors=True)
        else:
            shutil.rmtree(parquet_dir, ignore_errors=True)

    print(f"Generating {rows:,} rows for data_{label} as {file_type}")

    total_rows = 0
    total_size = 0
    total_sec = 0

    for i, start in enumerate(range(0, rows, chunk_rows), start=1):
        n = min(chunk_rows, rows - start)
        # Derive a unique seed per chunk: base + dataset_size + chunk_index
        # This ensures different chunks have different random states while
        # remaining fully deterministic given the same --seed argument
        # Used a different seed for each chunk depending on rows to ensure variability while keeping it deterministic
        chunk_seed = seed + rows + i
        chunk_rng = np.random.default_rng(chunk_seed)

        df = generate_chunk(n, chunk_rng)
        path, seconds = write_part(df, raw_dir, parquet_dir, f"{label}_{i:03d}", file_type)

        size = gb_size(path)

        total_rows += n
        total_size += size
        total_sec += seconds

        print(f"\nChunk {label}_{i:03d} generated:")
        print(f"  Rows    : {n:,}")
        print(f"  {file_type.upper():7}: {size:.3f} GB in ({seconds:.2f}s) -> {path}")

    output_dir = raw_dir if file_type == "csv" else parquet_dir

    print(f"\nDataset {label} generated:")
    print(f"  Rows    : {total_rows:,}")
    print(f"  {file_type.upper():7}: {total_size:.3f} GB in ({total_sec:.2f}s) -> {output_dir}")

    return {
        "label": label,
        "rows": total_rows,
        "file_type": file_type,
        f"{file_type}_size_mb": round(total_size * 1024, 3),
        f"{file_type}_seconds": round(total_sec, 3),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="S")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--chunk-rows", type=int, default=5_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--file-type", default="csv", choices=["csv", "parquet"])
    args = parser.parse_args()

    generate_dataset(
        label=args.label,
        rows=args.rows,
        chunk_rows=args.chunk_rows,
        seed=args.seed,
        clean=args.clean,
        file_type=args.file_type,
    )