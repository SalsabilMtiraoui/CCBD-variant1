import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


REGIONS = ["EU", "US", "APAC", "UK", "CH"]
EVENT_TYPES = ["deposit", "withdrawal", "payment", "transfer"]

SIZES = {
    "S": 5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}


def generate_chunk(rows, rng):
    april_start = np.datetime64("2026-04-01T00:00:00")
    april_seconds = 30 * 24 * 60 * 60

    ts = april_start + rng.integers(0, april_seconds, rows).astype("timedelta64[s]")

    return pd.DataFrame({
        "ts": ts,
        "user_id": rng.integers(1, 1_000_001, rows),
        "region": rng.choice(REGIONS, rows),
        "event_type": rng.choice(EVENT_TYPES, rows),
        "value": rng.uniform(1, 10_000, rows).round(2),
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="S")
    parser.add_argument("--rows", type=int, default=None)
    parser.add_argument("--chunk-rows", type=int, default=500_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--single-file", action="store_true")
    args = parser.parse_args()

    args.label = args.label.upper()

    if args.rows is None:
        if args.label not in SIZES:
            raise ValueError("Unknown label. Use --rows for custom labels.")
        args.rows = SIZES[args.label]

    raw_dir = Path(f"data/raw/data_{args.label}/csv")
    parquet_dir = Path(f"data/curated/data_{args.label}/parquet")

    shutil.rmtree(raw_dir, ignore_errors=True)
    shutil.rmtree(parquet_dir, ignore_errors=True)

    raw_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    chunks = (args.rows + args.chunk_rows - 1) // args.chunk_rows

    writer = None

    for i in tqdm(range(chunks), desc="Generating"):
        n = min(args.chunk_rows, args.rows - i * args.chunk_rows)
        df = generate_chunk(n, rng)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if args.single_file:
            csv_path = raw_dir / f"{args.label}.csv"
            parquet_path = parquet_dir / f"{args.label}.parquet"

            df.to_csv(csv_path, mode="a", header=(i == 0), index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_path,
                    table.schema,
                    compression="snappy",
                )
            writer.write_table(table)

        else:
            file_id = f"{args.label}_{i + 1:02d}"

            df.to_csv(raw_dir / f"{file_id}.csv", index=False)
            pq.write_table(
                table,
                parquet_dir / f"{file_id}.parquet",
                compression="snappy",
            )

    if writer is not None:
        writer.close()

    csv_size = sum(p.stat().st_size for p in raw_dir.rglob("*.csv"))
    parquet_size = sum(p.stat().st_size for p in parquet_dir.rglob("*.parquet"))

    print("Done.")
    print(f"Rows: {args.rows:,}")
    print(f"CSV size: {csv_size / 1_000_000:.2f} MB")
    print(f"Parquet size: {parquet_size / 1_000_000:.2f} MB")
    print(f"Parquet is {(1 - parquet_size / csv_size) * 100:.1f}% smaller")
    print(f"CSV saved in: {raw_dir}")
    print(f"Parquet saved in: {parquet_dir}")


if __name__ == "__main__":
    main()