import argparse
import shutil
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SIZES = {"S": 5_000_000, "M": 25_000_000, "L": 100_000_000}

PERIOD_START = pd.Timestamp("2026-04-01")
PERIOD_END = pd.Timestamp("2026-04-30")

N_USERS = 1_000_000

REGIONS = np.array(["Eurozone", "US", "UK", "Canada", "Switzerland"])
REGION_PROBS = np.array([0.35, 0.35, 0.15, 0.10, 0.05])

EVENT_TYPES = np.array(["payment", "withdrawal", "transfer", "deposit"])
EVENT_TYPE_PROBS = np.array([0.45, 0.17, 0.20, 0.18])

VALUE_PARAMS = {
    "payment": (3.25, 0.95),
    "withdrawal": (4.6, 0.55),
    "transfer": (5.3, 1.25),
    "deposit": (5.55, 1.05),
}

CURRENCIES = np.array(["USD", "EUR", "GBP", "CAD", "CHF"])
REGION_TO_CURRENCY = {
    "Eurozone": "EUR",
    "US": "USD",
    "UK": "GBP",
    "Canada": "CAD",
    "Switzerland": "CHF",
}
FOREIGN_CURRENCY_PROBS = np.array([0.40, 0.30, 0.10, 0.10, 0.10])

STATUS = np.array(["completed", "pending", "failed"])
STATUS_PROBS = {
    "payment": [0.95, 0.02, 0.03],
    "withdrawal": [0.93, 0.04, 0.03],
    "transfer": [0.90, 0.05, 0.05],
    "deposit": [0.96, 0.03, 0.01],
}

EVENT_MEANS = np.array([VALUE_PARAMS[e][0] for e in EVENT_TYPES])
EVENT_STDS = np.array([VALUE_PARAMS[e][1] for e in EVENT_TYPES])
STATUS_PROB_MATRIX = np.array([STATUS_PROBS[e] for e in EVENT_TYPES])

REGION_CURRENCY_CODE = np.array([
    np.where(CURRENCIES == REGION_TO_CURRENCY[r])[0][0]
    for r in REGIONS
])



def generate_chunk(n, rng):
    period_seconds = int((PERIOD_END - PERIOD_START).total_seconds()) + 24 * 3600
    ts = PERIOD_START + pd.to_timedelta(rng.integers(0, period_seconds, n), unit="s")

    user_id = rng.integers(1, N_USERS + 1, n)

    region_code = rng.choice(len(REGIONS), n, p=REGION_PROBS)

    event_code = rng.choice(len(EVENT_TYPES), n, p=EVENT_TYPE_PROBS)

    value = rng.lognormal(EVENT_MEANS[event_code], EVENT_STDS[event_code]).round(2)

    currency_code = REGION_CURRENCY_CODE[region_code].copy()
    use_foreign = rng.random(n) < 0.10
    currency_code[use_foreign] = rng.choice(
        len(CURRENCIES),
        use_foreign.sum(),
        p=FOREIGN_CURRENCY_PROBS,
    )

    status_cdf = STATUS_PROB_MATRIX[event_code].cumsum(axis=1)
    status_code = (rng.random(n)[:, None] > status_cdf[:, :2]).sum(axis=1)

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
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, parquet_path, compression="snappy")
        parquet_sec = time.time() - start

        return parquet_path, parquet_sec

    raise ValueError("--file-type must be either 'csv' or 'parquet'.")


def gb_size(path):
    return path.stat().st_size / (1024 ** 3)


def generate_dataset(label="S", rows=None, chunk_rows=5_000_000, seed=42, clean=False, file_type="csv"):
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