import boto3
import os
import time
from pathlib import Path
from typing import List

from prefect import flow, task, get_run_logger

ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_KWARGS = dict(
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    endpoint_url=ENDPOINT,
)


def boto(service):  # helper
    return boto3.client(service, **AWS_KWARGS)


@task
def list_csv(folder: str = "csv") -> List[Path]:
    return sorted(Path(folder).glob("*.csv"))


@task
def push_csv_in_chunks(csv_path: Path, max_bytes: int = 900*1024):
    kin = boto("kinesis")
    dataset = csv_path.stem.lower()
    stream = {
        "albums":  "albums-stream",
        "bands":   "bands-stream",
        "reviews": "reviews-stream",
    }[dataset]

    lines = csv_path.read_text().splitlines()
    header = lines[0] + "\n"

    chunk = header
    size = len(chunk.encode())
    for line in lines[1:]:
        encoded = (line + "\n").encode()
        if size + len(encoded) > max_bytes:
            kin.put_record(StreamName=stream, Data=chunk.encode(), PartitionKey=dataset)
            chunk = header + line + "\n"
            size = len((header + line + "\n").encode())
        else:
            chunk += line + "\n"
            size += len(encoded)

    if size > len(header.encode()):
        kin.put_record(StreamName=stream, Data=chunk.encode(), PartitionKey=dataset)

@task(log_prints=True, retries=3, retry_delay_seconds=30)
def wait_firehose(bucket="csv-batch-bucket", prefix="landing/") -> List[str]:
    """Espera pelo menos 1 objeto novo no prefixo landing/."""
    s3, log = boto("s3"), get_run_logger()
    while True:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if resp.get("KeyCount", 0):
            keys = [o["Key"] for o in resp["Contents"]]
            log.info(f"🔥 Chegaram {len(keys)} arquivo(s) no S3")
            return keys
        log.info("⏳ Aguardando Firehose… (15 s)")
        time.sleep(15)


@flow
def ingest_folder_flow(folder: str = "csv") -> list[str]:
    files = list_csv(folder)
    push_csv_in_chunks.map(files)
    keys = wait_firehose()
    return keys


if __name__ == "__main__":
    objs = ingest_folder_flow("csv")
    print("Objetos S3:", objs)
