from __future__ import annotations

import io
import os

import boto3
import polars as pl
from prefect import flow, task

ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_KWARGS = dict(
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    endpoint_url=ENDPOINT,
)

BUCKET = os.getenv("S3_BUCKET", "csv-batch-bucket")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold")


def boto(service: str):
    return boto3.client(service, **AWS_KWARGS)


@task
def read_parquet_prefix(prefix: str) -> pl.DataFrame:
    s3 = boto("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [o['Key'] for o in resp.get('Contents', []) if o['Key'].endswith('.parquet')]
    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj['Body'].read()
        dfs.append(pl.read_parquet(io.BytesIO(data)))
    return pl.concat(dfs) if dfs else pl.DataFrame()


@task
def write_parquet(df: pl.DataFrame, prefix: str, filename: str) -> None:
    if df.is_empty():
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    key = f"{prefix}/{filename}.parquet"
    boto("s3").put_object(Bucket=BUCKET, Key=key, Body=buf.read())


@task
def compute_top10_by_country(bands: pl.DataFrame, reviews: pl.DataFrame) -> pl.DataFrame:
    joined = reviews.join(bands, left_on="band_id", right_on="id")
    counts = (
        joined
        .groupby(["country", "band_id", "name"])
        .agg(pl.count().alias("review_count"))
    )
    top10 = (
        counts
        .sort(by=["country", "review_count"], reverse=True)
        .groupby("country")
        .head(10)
    )
    return top10


@task
def compute_avg_score(bands: pl.DataFrame, reviews: pl.DataFrame) -> pl.DataFrame:
    joined = reviews.join(bands, left_on="band_id", right_on="id")
    avg_scores = (
        joined
        .groupby(["band_id", "name", "country"])
        .agg(pl.mean("score").alias("avg_score"))
        .sort("avg_score", reverse=True)
    )
    return avg_scores


@task
def filter_brazilian(avg_scores: pl.DataFrame) -> pl.DataFrame:
    return avg_scores.filter(pl.col("country") == "Brazil")


@flow
def gold_flow():
    bands_df = read_parquet_prefix(f"{SILVER_PREFIX}/bands")
    reviews_df = read_parquet_prefix(f"{SILVER_PREFIX}/reviews")

    top10 = compute_top10_by_country(bands_df, reviews_df)
    avg_scores = compute_avg_score(bands_df, reviews_df)
    brazilian = filter_brazilian(avg_scores)

    write_parquet(top10, GOLD_PREFIX, "top10_by_country")
    write_parquet(avg_scores, GOLD_PREFIX, "band_avg_scores")
    write_parquet(brazilian, GOLD_PREFIX, "brazilian_bands")


if __name__ == "__main__":
    gold_flow()
