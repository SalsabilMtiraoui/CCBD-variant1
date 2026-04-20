#generate data (need the different sizes)
#ts (timestamp), user_id (int), region (string), event_type (string), value (float)
# on va partir sur des évenement financier
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import argparse

# Schéma financier
REGIONS = ["Europe", "US", "Asia", "Africa", "MiddleEast"]
EVENT_TYPES = ["deposit", "withdrawal", "transfer", "payment"]
CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY"]
STATUSES = ["completed", "pending", "failed"]

# Tailles cibles en bytes
SIZES = {
    "S": 1 * 10**9,
    "M": 5 * 10**9,
    "L": 15 * 10**9,
}

def generate_dataset(size_label, seed=42, output_dir="data"):
    np.random.seed(seed)
    target_bytes = SIZES[size_label]

    # ~120 bytes par ligne estimé
    estimated_rows = target_bytes // 18 #CHECK WHAT SIZE TO PUT EXACTLY
    print(f"Génération taille {size_label} — ~{estimated_rows:,} lignes...")

    os.makedirs(output_dir, exist_ok=True)

    chunk_size = 500_000
    total_rows = 0
    csv_path = os.path.join(output_dir, f"dataset_{size_label}.csv")
    parquet_path = os.path.join(output_dir, f"dataset_{size_label}.parquet")

    # Supprimer fichiers existants
    for p in [csv_path, parquet_path]:
        if os.path.exists(p):
            os.remove(p)

    parquet_writer = None
    csv_written = False
    start_date = pd.Timestamp("2022-01-01")

    while total_rows < estimated_rows:
        rows = min(chunk_size, estimated_rows - total_rows)

        # Timestamps aléatoires sur 2 ans
        random_seconds = np.random.randint(0, 2 * 365 * 24 * 3600, size=rows)
        timestamps = start_date + pd.to_timedelta(random_seconds, unit="s")

        df = pd.DataFrame({
            "ts": timestamps,
            "user_id": np.random.randint(1, 500_000, size=rows),
            "region": np.random.choice(REGIONS, size=rows),
            "event_type": np.random.choice(EVENT_TYPES, size=rows),
            "amount": np.random.uniform(1.0, 50_000.0, size=rows).round(2),
            "currency": np.random.choice(CURRENCIES, size=rows),
            "status": np.random.choice(STATUSES, size=rows),
        })

        # CSV
        df.to_csv(csv_path, mode="a", header=not csv_written, index=False)
        csv_written = True

        # Parquet
        table = pa.Table.from_pandas(df)
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(parquet_path, table.schema)
        parquet_writer.write_table(table)

        total_rows += rows
        print(f"  {total_rows:,} / {estimated_rows:,} lignes générées...")

    if parquet_writer:
        parquet_writer.close()

    csv_size = os.path.getsize(csv_path) / 10**9
    parquet_size = os.path.getsize(parquet_path) / 10**9

    print(f"\n✅ Dataset {size_label} généré :")
    print(f"   CSV     : {csv_size:.3f} GB → {csv_path}")
    print(f"   Parquet : {parquet_size:.3f} GB → {parquet_path}")
    print(f"   Compression Parquet : {(1 - parquet_size/csv_size)*100:.1f}% plus petit que CSV")

    return csv_path, parquet_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic financial dataset")
    parser.add_argument("--size", choices=["S", "M", "L"], default="S",
                        help="Dataset size: S=1GB, M=5GB, L=15GB")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--output_dir", type=str, default="data",
                        help="Output directory")
    args = parser.parse_args()

    generate_dataset(args.size, seed=args.seed, output_dir=args.output_dir)