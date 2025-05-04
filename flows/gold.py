import io
import os
from typing import Dict

import boto3
import polars as pl
from prefect import flow, task

# ─── Config LocalStack ──────────────────────────────────────────────
ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_KWARGS = dict(
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    endpoint_url=ENDPOINT,
)

BUCKET = "csv-batch-bucket"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"


def boto(service: str):
    return boto3.client(service, **AWS_KWARGS)


# ─── Util ───────────────────────────────────────────────────────────
def read_parquet_lazy_from_s3(path: str) -> pl.LazyFrame:
    s3 = boto("s3")
    path_parts = path.replace("s3://", "").split("/")
    bucket, key = path_parts[0], "/".join(path_parts[1:])
    response = s3.get_object(Bucket=bucket, Key=key)
    return pl.read_parquet(io.BytesIO(response["Body"].read())).lazy()


@task
def ensure_bucket():
    s3 = boto("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)


@task
def clear_gold_prefix():
    s3 = boto("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=GOLD_PREFIX)
    if 'Contents' in resp:
        for obj in resp['Contents']:
            s3.delete_object(Bucket=BUCKET, Key=obj['Key'])


@task
def read_silver_lazy(dataset_name: str) -> pl.LazyFrame:
    path = f"s3://{BUCKET}/{SILVER_PREFIX}/{dataset_name}/{dataset_name}.parquet"
    return read_parquet_lazy_from_s3(path)


@task
def write_gold_dataset(df: pl.LazyFrame, name: str) -> str:
    collected = df.collect()
    if collected.is_empty():
        print(f"⚠️ Dataset '{name}' vazio. Não será salvo.")
        return ""
    s3 = boto("s3")
    buf = io.BytesIO()
    collected.write_parquet(buf, compression="snappy")
    buf.seek(0)
    key = f"{GOLD_PREFIX}/{name}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"✅ Escrito: {key}")
    return f"s3://{BUCKET}/{key}"


@task
def preprocess_reviews(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.rename({"id": "review_id", "album": "album_id"})


@task
def create_top10_by_country(music: pl.LazyFrame, reviews: pl.LazyFrame) -> pl.LazyFrame:
    reviews = preprocess_reviews(reviews)
    return (
        reviews.join(music, on="album_id", how="left")
        .group_by(["country", "band_id", "band_name"])
        .agg([
            pl.count().alias("review_count"),
            pl.mean("score").alias("avg_score")
        ])
        .sort(["country", "review_count"], descending=True)
        .group_by("country")
        .head(10)
    )


@task
def create_band_avg_scores(music: pl.LazyFrame, reviews: pl.LazyFrame) -> pl.LazyFrame:
    reviews = preprocess_reviews(reviews)
    return (
        reviews.join(music, on="album_id", how="left")
        .group_by(["band_id", "band_name", "country"])
        .agg([
            pl.count().alias("review_count"),
            pl.mean("score").alias("avg_score"),
            pl.min("score").alias("min_score"),
            pl.max("score").alias("max_score"),
            pl.std("score").alias("std_score")
        ])
        .sort("avg_score", descending=True)
    )


@task
def create_brazilian_bands(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns([
            pl.col("country").str.to_lowercase().str.strip_chars().alias("country_normalized")
        ])
        .filter(pl.col("country_normalized").is_in(["brazil", "brasil"]))
        .sort("avg_score", descending=True)
    )


@task
def create_band_album_counts(music: pl.LazyFrame) -> pl.LazyFrame:
    return (
        music.group_by(["band_id", "band_name", "country"])
        .agg(pl.count().alias("album_count"))
        .sort("album_count", descending=True)
    )


# ─── Flow Principal ─────────────────────────────────────────────────
@flow(name="gold-transform-flow")
def gold_flow() -> Dict[str, str]:
    print("\n🚀 Iniciando Gold Flow...")

    ensure_bucket()
    clear_gold_prefix()

    results = {}

    try:
        music = read_silver_lazy("music_catalog")
        reviews = read_silver_lazy("reviews")
    except Exception as e:
        print(f"❌ Erro ao carregar arquivos da camada Silver: {e}")
        return {}

    if music.collect().is_empty() or reviews.collect().is_empty():
        print("⚠️ Dados da camada Silver ausentes ou vazios.")
        return {}

    top10 = create_top10_by_country(music, reviews)
    results["top10_by_country"] = write_gold_dataset(top10, "top10_by_country")

    avg_scores = create_band_avg_scores(music, reviews)
    results["band_avg_scores"] = write_gold_dataset(avg_scores, "band_avg_scores")

    brazil = create_brazilian_bands(avg_scores)
    results["brazilian_bands"] = write_gold_dataset(brazil, "brazilian_bands")

    counts = create_band_album_counts(music)
    results["band_album_counts"] = write_gold_dataset(counts, "band_album_counts")

    return results


# ─── Execução CLI ───────────────────────────────────────────────────
if __name__ == "__main__":
    result = gold_flow()
    print("\n📦 Camada Gold finalizada:")
    for name, uri in result.items():
        print(f" • {name}: {uri}")
