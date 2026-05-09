from pathlib import Path
import shutil
import time


def download_prefix(s3, bucket, prefix, local_dir, clean=True):
    local_dir = Path(local_dir)

    if clean:
        shutil.rmtree(local_dir, ignore_errors=True)

    local_dir.mkdir(parents=True, exist_ok=True)

    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    total_bytes = sum(obj["Size"] for obj in objs)

    start = time.time()

    for obj in objs:
        key = obj["Key"]
        out = local_dir / key.replace(prefix, "").lstrip("/")
        out.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(out))

    seconds = time.time() - start
    mbps = (total_bytes / 1_000_000) / seconds if seconds else 0

    return total_bytes, len(objs), seconds, mbps