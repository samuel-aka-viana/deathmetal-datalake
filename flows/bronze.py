from __future__ import annotations

import io
import os
from collections import Counter
from typing import List

import boto3
import polars as pl
from prefect import flow, task

# ─── Configuração AWS/LocalStack ─────────────────────────────────────
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


def boto(service: str):
    return boto3.client(service, **AWS_KWARGS)


def normalize_and_dedupe(columns: List[str]) -> List[str]:
    normalized = [c.strip().lower().replace(" ", "_") for c in columns]
    counts = Counter()
    result: List[str] = []
    for col in normalized:
        counts[col] += 1
        if counts[col] == 1:
            result.append(col)
        else:
            result.append(f"{col}_{counts[col]}")
    return result


@task
def ensure_bucket() -> None:
    s3 = boto("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"✅ Bucket {BUCKET} existe.")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"✅ Bucket {BUCKET} criado.")


@task
def list_landing_csv(prefix: str = LANDING_PREFIX) -> List[str]:
    s3 = boto("s3")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in response.get("Contents", []) if o.get("Key") and o["Key"]]


@task(log_prints=True)
def csv_s3_to_parquet(key: str) -> str:
    print(f"📥 Processando CSV: {key}")
    s3 = boto("s3")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = io.BytesIO(obj["Body"].read())
    except Exception as e:
        print(f"❌ Erro ao acessar o arquivo no S3: {e}")
        return ""

    try:
        df = pl.read_csv(data, infer_schema_length=5000)
    except Exception as e:
        print(f"❌ Erro ao ler CSV: {e}")
        return ""

    df.columns = normalize_and_dedupe(df.columns)
    df = df.unique()

    parts = key.split("/")
    if len(parts) < 2:
        print(f"⚠️ Nome do dataset não identificado em: {key}")
        return ""

    dataset = parts[1].replace(".csv", "").strip()
    if not dataset:
        print(f"⚠️ Dataset inválido extraído de: {key}")
        return ""

    parquet_key = f"{BRONZE_PREFIX}/{dataset}/{dataset}.parquet"
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)

    s3.put_object(Bucket=BUCKET, Key=parquet_key, Body=buf.getvalue())
    print(f"✅ Parquet salvo: s3://{BUCKET}/{parquet_key}")
    return f"s3://{BUCKET}/{parquet_key}"


@flow(name="landing-to-bronze-flow")
def landing_to_bronze_flow() -> List[str]:
    print("🚀 Iniciando pipeline Landing → Bronze")
    ensure_bucket()
    landing_keys = list_landing_csv()
    if not landing_keys:
        print("⚠️ Nenhum arquivo CSV encontrado na camada landing.")
        return []

    print(f"📂 {len(landing_keys)} arquivos CSV encontrados.")
    parquet_keys = csv_s3_to_parquet.map(landing_keys)
    return parquet_keys


if __name__ == "__main__":
    parquet_uris = landing_to_bronze_flow()
    print("\n📦 Arquivos Parquet gerados:")
    for uri in parquet_uris:
        if uri:
            print(f" • {uri}")
