from __future__ import annotations
import io
import os
import boto3
import polars as pl
from typing import List
from collections import Counter
from prefect import flow, task

ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_KWARGS = dict(
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    endpoint_url=ENDPOINT,
)

BUCKET = "csv-batch-bucket"
LANDING_PREFIX = "landing/"
BRONZE_PREFIX = "bronze"


def boto(service):  # helper
    return boto3.client(service, **AWS_KWARGS)


def normalize_and_dedupe(cols: List[str]) -> List[str]:
    normalized = [c.strip().lower().replace(" ", "_") for c in cols]
    counts = Counter()
    out: List[str] = []
    for col in normalized:
        counts[col] += 1
        if counts[col] == 1:
            out.append(col)
        else:
            out.append(f"{col}_{counts[col]}")
    return out


@task
def ensure_bucket() -> None:
    s3 = boto("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)


@task
def list_landing_csv(prefix: str = LANDING_PREFIX) -> List[str]:
    s3 = boto("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o.get("Key")]


@task(log_prints=True)
def csv_s3_to_parquet(key: str) -> str:
    print(f"📥 Processando arquivo: {key}")
    s3 = boto("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = io.BytesIO(obj["Body"].read())

    try:
        df = pl.read_csv(data)
    except Exception as e:
        print(f"❌ Falha ao ler CSV de {key}: {e}")
        return ""

    df.columns = normalize_and_dedupe(df.columns)

    df = df.unique()

    parts = key.split("/")
    if len(parts) < 2:
        raise ValueError(f"Chave inesperada: {key}")
    dataset = parts[1]

    parquet_key = f"{BRONZE_PREFIX}/{dataset}/{dataset}.parquet"

    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=parquet_key, Body=buf.getvalue())
    print(f"✅ Parquet salvo em: s3://{BUCKET}/{parquet_key}")
    return f"s3://{BUCKET}/{parquet_key}"


@flow(name="landing→bronze-flow")
def landing_to_bronze_flow() -> List[str]:
    ensure_bucket()
    landing_keys = list_landing_csv()
    parquet_keys = csv_s3_to_parquet.map(landing_keys)
    return parquet_keys


if __name__ == "__main__":
    parquet_uris = landing_to_bronze_flow()
    print("\n🟢 Parquets gerados:")
    for uri in parquet_uris:
        print(" •", uri)
