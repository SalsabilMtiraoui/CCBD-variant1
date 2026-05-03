from pathlib import Path
import time


def delete_prefix(s3, bucket, prefix):
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for obj in objs:
        s3.delete_object(Bucket=bucket, Key=obj["Key"])


def upload_dir(s3, bucket, local_dir, prefix, clean=True):
    local_dir = Path(local_dir)

    if clean:
        delete_prefix(s3, bucket, prefix)

    files = [p for p in local_dir.rglob("*") if p.is_file()]
    total_bytes = sum(p.stat().st_size for p in files)

    start = time.time()

    for p in files:
        key = f"{prefix}/{p.relative_to(local_dir)}"
        s3.upload_file(str(p), bucket, key)

    seconds = time.time() - start
    mbps = (total_bytes / 1_000_000) / seconds if seconds else 0

    return total_bytes, len(files), seconds, mbps