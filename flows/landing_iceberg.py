"""Landing flow: lê CSV locais e grava chunks diretamente no MinIO (S3) em `landing/`."""
import os
import time
from pathlib import Path
from typing import List

import boto3
from prefect import flow, task

# Endpoint e credenciais MinIO (já usadas nos outros flows)
ENDPOINT = os.getenv("AWS_ENDPOINT", "http://minio.lakehouse.svc.cluster.local:9000")
AWS_KWARGS = dict(
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
    endpoint_url=ENDPOINT,
)

BUCKET = os.getenv("BUCKET", "datalake")
PREFIX = os.getenv("LANDING_PREFIX", "landing/")

def boto(service):
    return boto3.client(service, **AWS_KWARGS)

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def list_csv(folder: str = "csv") -> List[Path]:
    return sorted(Path(folder).glob("*.csv"))

@task(log_prints=True)
def push_csv_in_chunks(csv_path: Path, max_bytes: int = 900 * 1024) -> List[str]:

    s3 = boto("s3")
    dataset = csv_path.stem.lower()
    lines = csv_path.read_text().splitlines()
    header = lines[0] + "\n"

    keys = []
    chunk = header
    size = len(chunk.encode())
    ts = int(time.time() * 1000)
    part = 0

    for line in lines[1:]:
        encoded = (line + "\n").encode()
        if size + len(encoded) > max_bytes:
            key = f"{PREFIX}{dataset}/{ts}_{part}.csv"
            s3.put_object(Bucket=BUCKET, Key=key, Body=chunk.encode())
            keys.append(key)
            part += 1
            chunk = header + line + "\n"
            size = len(chunk.encode())
        else:
            chunk += line + "\n"
            size += len(encoded)

    if size > len(header.encode()):
        key = f"{PREFIX}{dataset}/{ts}_{part}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=chunk.encode())
        keys.append(key)

    print(f"✅ Enviadas {len(keys)} parte(s) para dataset '{dataset}'")
    return keys

# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="landing-to-minio")
def ingest_folder_flow(folder: str = "csv") -> List[str]:
    all_keys: List[str] = []
    csv_files = list_csv(folder)
    for keys in push_csv_in_chunks.map(csv_files):
        all_keys.extend(keys)
    return all_keys

# Execução local
if __name__ == "__main__":
    uploaded = ingest_folder_flow("csv")
    print("Objetos enviados:")
    for k in uploaded:
        print(" -", k)