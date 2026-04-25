import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import argparse

REGIONS = ["Europe", "US", "Asia", "Africa", "MiddleEast"]
EVENT_TYPES = ["deposit", "withdrawal", "transfer", "payment"]
CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY"]
STATUSES = ["completed", "pending", "failed"]

# Nombre de lignes par taille (comme suggéré par le sujet)
SIZES = {
    "S": 5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}

def generate_dataset(size_label, seed=42, output_dir="data"):
    np.random.seed(seed)
    total_rows = SIZES[size_label]

    print(f"\n{'='*50}")
    print(f"Generating dataset {size_label}")
    print(f"Target rows : {total_rows:,}")
    print(f"{'='*50}")

    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"dataset_{size_label}.csv")
    parquet_path = os.path.join(output_dir, f"dataset_{size_label}.parquet")

    for p in [csv_path, parquet_path]:
        if os.path.exists(p):
            os.remove(p)

    chunk_size = 500_000
    rows_generated = 0
    parquet_writer = None
    csv_written = False
    start_date = pd.Timestamp("2022-01-01")

    while rows_generated < total_rows:
        rows = min(chunk_size, total_rows - rows_generated)
        random_seconds = np.random.randint(0, 2 * 365 * 24 * 3600, size=rows)
        timestamps = start_date + pd.to_timedelta(random_seconds, unit="s")

        df = pd.DataFrame({
            "ts": timestamps,
            "user_id": np.random.randint(1, 500_000, size=rows),
            "region": np.random.choice(REGIONS, size=rows),
            "event_type": np.random.choice(EVENT_TYPES, size=rows),
            "value": np.random.uniform(1.0, 50_000.0, size=rows).round(2),
            "currency": np.random.choice(CURRENCIES, size=rows),
            "status": np.random.choice(STATUSES, size=rows),
        })

        df.to_csv(csv_path, mode="a", header=not csv_written, index=False)
        csv_written = True

        table = pa.Table.from_pandas(df)
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(parquet_path, table.schema)
        parquet_writer.write_table(table)

        rows_generated += rows
        print(f"  Progress: {rows_generated:,} / {total_rows:,} rows")

    if parquet_writer:
        parquet_writer.close()

    csv_size = os.path.getsize(csv_path) / 10**9
    parquet_size = os.path.getsize(parquet_path) / 10**9

    print(f"\nDataset {size_label} generated:")
    print(f"  Rows    : {total_rows:,}")
    print(f"  CSV     : {csv_size:.3f} GB -> {csv_path}")
    print(f"  Parquet : {parquet_size:.3f} GB -> {parquet_path}")
    print(f"  Parquet is {(1 - parquet_size/csv_size)*100:.1f}% smaller than CSV")

    return csv_path, parquet_path, {
        "size_label": size_label,
        "total_rows": total_rows,
        "csv_size_gb": round(csv_size, 3),
        "parquet_size_gb": round(parquet_size, 3),
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic financial dataset")
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output_dir", type=str, default="data")
    args = parser.parse_args()
    generate_dataset(args.size, seed=args.seed, output_dir=args.output_dir)