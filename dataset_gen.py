import argparse
import shutil
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Dataset sizes (rows) ───────────────────────────────────────────
SIZES = {
    "S":  5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}

# ─── Schema constants ────────────────────────────────────────────────
PERIOD_START = pd.Timestamp("2022-01-01")
PERIOD_END   = pd.Timestamp("2023-12-31")
N_USERS      = 500_000

REGIONS      = np.array(["Europe", "US", "Asia", "Africa", "MiddleEast"])
REGION_PROBS = np.array([0.35, 0.30, 0.15, 0.12, 0.08])

EVENT_TYPES      = np.array(["deposit", "withdrawal", "transfer", "payment"])
EVENT_TYPE_PROBS = np.array([0.18, 0.17, 0.20, 0.45])

# Log-normal params (mu, sigma) per event type realistic financial amounts
VALUE_PARAMS = {
    "deposit":    (5.55, 1.05),
    "withdrawal": (4.60, 0.55),
    "transfer":   (5.30, 1.25),
    "payment":    (3.25, 0.95),
}

CURRENCIES = np.array(["USD", "EUR", "GBP", "CHF", "JPY"])
REGION_TO_CURRENCY = {
    "Europe":     "EUR",
    "US":         "USD",
    "Asia":       "JPY",
    "Africa":     "USD",
    "MiddleEast": "USD",
}

STATUS       = np.array(["completed", "pending", "failed"])
STATUS_PROBS = {
    "deposit":    [0.96, 0.03, 0.01],
    "withdrawal": [0.93, 0.04, 0.03],
    "transfer":   [0.90, 0.05, 0.05],
    "payment":    [0.95, 0.02, 0.03],
}

# Precomputed arrays for fast vectorised generation
EVENT_MEANS        = np.array([VALUE_PARAMS[e][0] for e in EVENT_TYPES])
EVENT_STDS         = np.array([VALUE_PARAMS[e][1] for e in EVENT_TYPES])
STATUS_PROB_MATRIX = np.array([STATUS_PROBS[e]    for e in EVENT_TYPES])
REGION_CURRENCY_IDX = np.array([
    np.where(CURRENCIES == REGION_TO_CURRENCY[r])[0][0]
    for r in REGIONS
])

CHUNK_ROWS_DEFAULT = 5_000_000  # ~50 MB RAM per chunk


# ─── Core generation ─────────────────────────────────────────────────

def generate_chunk(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate n rows of synthetic financial transactions."""
    period_seconds = int((PERIOD_END - PERIOD_START).total_seconds())
    ts = PERIOD_START + pd.to_timedelta(
        rng.integers(0, period_seconds, n), unit="s"
    )
    user_id     = rng.integers(1, N_USERS + 1, n)
    region_code = rng.choice(len(REGIONS),     n, p=REGION_PROBS)
    event_code  = rng.choice(len(EVENT_TYPES), n, p=EVENT_TYPE_PROBS)

    # Log-normal values — realistic transaction amounts
    value = np.round(
        rng.lognormal(EVENT_MEANS[event_code], EVENT_STDS[event_code]), 2
    )

    # Currency = region default (no foreign currency randomisation for simplicity)
    currency_code = REGION_CURRENCY_IDX[region_code].copy()

    # Status — event-type-specific probabilities
    status_cdf  = STATUS_PROB_MATRIX[event_code].cumsum(axis=1)
    status_code = (rng.random(n)[:, None] > status_cdf[:, :2]).sum(axis=1)

    return pd.DataFrame({
        "ts":         ts,
        "user_id":    user_id,
        "region":     pd.Categorical.from_codes(region_code, REGIONS),
        "event_type": pd.Categorical.from_codes(event_code, EVENT_TYPES),
        "value":      value,
        "currency":   pd.Categorical.from_codes(currency_code, CURRENCIES),
        "status":     pd.Categorical.from_codes(status_code, STATUS),
    })


def write_chunk(df: pd.DataFrame, out_dir: Path, file_id: str,
                file_type: str, parquet_writer_ref: list) -> tuple:
    """Write one chunk to disk. Returns (path, seconds)."""
    if file_type == "csv":
        path = out_dir / f"{file_id}.csv"
        t0 = time.time()
        df.to_csv(path, index=False)
        return path, time.time() - t0

    if file_type == "parquet":
        path = out_dir / f"{file_id}.parquet"
        t0 = time.time()
        table = pa.Table.from_pandas(df, preserve_index=False)
        if parquet_writer_ref[0] is None:
            # Single-file mode: keep writer open across chunks
            parquet_writer_ref[0] = pq.ParquetWriter(path, table.schema,
                                                      compression="snappy")
        parquet_writer_ref[0].write_table(table)
        return path, time.time() - t0

    raise ValueError(f"Unknown file_type: {file_type!r}. Use 'csv' or 'parquet'.")


# ─── Main entry point ────────────────────────────────────────────────

def generate_dataset(
    label: str       = "S",
    rows: int        = None,
    chunk_rows: int  = CHUNK_ROWS_DEFAULT,
    seed: int        = 42,
    clean: bool      = False,
    file_type: str   = "csv",
    multi_file: bool = False,
) -> dict:
    """
    Generate a synthetic financial dataset.

    Parameters
    ----------
    label      : S / M / L (or custom with rows=)
    rows       : override row count
    chunk_rows : rows per chunk (default 5M ≈ 50 MB RAM)
    seed       : base random seed (reproducible)
    clean      : delete existing output before generating
    file_type  : 'csv' or 'parquet'
    multi_file : if True, write one file per chunk (like Erulan's version)
                 if False, write one single file (like Salsabil's version)
    """
    label     = label.upper()
    file_type = file_type.lower()
    rows      = rows or SIZES.get(label)

    if rows is None:
        raise ValueError(f"Unknown label '{label}'. Use S/M/L or pass --rows.")
    if file_type not in ("csv", "parquet"):
        raise ValueError("--file-type must be 'csv' or 'parquet'.")

    # Output directories — same layout as Erulan's version
    raw_dir     = Path(f"data/raw/data_{label}/csv")
    parquet_dir = Path(f"data/curated/data_{label}/parquet")
    out_dir     = raw_dir if file_type == "csv" else parquet_dir

    if clean:
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*55}")
    print(f"Generating dataset {label} | {file_type.upper()} | {rows:,} rows")
    print(f"Chunk size : {chunk_rows:,} rows | seed : {seed}")
    print(f"Output     : {out_dir}")
    print(f"{'='*55}")

    total_rows = total_bytes = total_sec = 0
    parquet_writer_ref = [None]   # mutable container for single-file parquet writer
    last_path = None

    for i, start_row in enumerate(range(0, rows, chunk_rows), start=1):
        n          = min(chunk_rows, rows - start_row)
        chunk_seed = seed + rows + i          # deterministic but unique per chunk
        rng        = np.random.default_rng(chunk_seed)
        df         = generate_chunk(n, rng)

        # File ID: single file → always same name; multi-file → numbered
        file_id = f"{label}_{i:03d}" if multi_file else f"dataset_{label}"

        # In single-file CSV mode, append manually
        if file_type == "csv" and not multi_file:
            path = out_dir / f"dataset_{label}.csv"
            t0 = time.time()
            df.to_csv(path, mode="a", header=(i == 1), index=False)
            sec = time.time() - t0
        else:
            path, sec = write_chunk(df, out_dir, file_id, file_type,
                                    parquet_writer_ref)

        size_bytes  = path.stat().st_size
        total_rows += n
        total_bytes += size_bytes
        total_sec  += sec
        last_path   = path

        print(f"  Chunk {i:03d}: {n:,} rows | "
              f"{size_bytes/1024**2:.1f} MB | {sec:.2f}s")

    # Close single-file Parquet writer
    if parquet_writer_ref[0] is not None:
        parquet_writer_ref[0].close()

    total_gb = total_bytes / 1024**3
    print(f"\nDataset {label} ({file_type}) complete:")
    print(f"  Total rows  : {total_rows:,}")
    print(f"  Total size  : {total_gb:.3f} GB")
    print(f"  Total time  : {total_sec:.2f}s")
    print(f"  Output dir  : {out_dir}")

    return {
        "label":          label,
        "file_type":      file_type,
        "rows":           total_rows,
        "size_mb":        round(total_bytes / 1024**2, 2),
        "size_gb":        round(total_gb, 3),
        "generate_seconds": round(total_sec, 3),
        "generate_mbps":  round((total_bytes / 1024**2) / total_sec, 2)
                          if total_sec > 0 else 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic financial dataset — CCBD SP26 Variant 1"
    )
    parser.add_argument("--label",      default="S",
                        help="Dataset size label: S, M, or L")
    parser.add_argument("--rows",       type=int,
                        help="Override row count (ignores --label rows)")
    parser.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS_DEFAULT,
                        help=f"Rows per chunk (default {CHUNK_ROWS_DEFAULT:,})")
    parser.add_argument("--seed",       type=int, default=42,
                        help="Random seed (default 42)")
    parser.add_argument("--clean",      action="store_true",
                        help="Delete existing output before generating")
    parser.add_argument("--file-type",  default="csv",
                        choices=["csv", "parquet"],
                        help="Output format: csv or parquet")
    parser.add_argument("--multi-file", action="store_true",
                        help="Write one file per chunk (default: single file)")
    args = parser.parse_args()

    generate_dataset(
        label      = args.label,
        rows       = args.rows,
        chunk_rows = args.chunk_rows,
        seed       = args.seed,
        clean      = args.clean,
        file_type  = args.file_type,
        multi_file = args.multi_file,
    )