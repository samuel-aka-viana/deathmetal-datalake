from __future__ import annotations

import io
import os
from typing import Dict

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

BUCKET = "csv-batch-bucket"
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"


def boto(service: str):
    return boto3.client(service, **AWS_KWARGS)


@task
def ensure_bucket() -> None:
    s3 = boto("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)


@task
def read_bronze_parquet(key: str) -> pl.LazyFrame:
    s3 = boto("s3")

    path_parts = key.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    object_key = "/".join(path_parts[1:])

    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    parquet_bytes = response["Body"].read()

    return pl.read_parquet(io.BytesIO(parquet_bytes))


@task
def transform_albums(df: pl.DataFrame) -> pl.DataFrame:
    return (df
    .with_columns([
        pl.col("id").cast(pl.Int64),
        pl.col("band").cast(pl.Int64),
        pl.col("year").cast(pl.Int64)
    ])
    )


@task
def transform_bands(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns([
            pl.col("id").cast(pl.Int64),
            pl.col("formed_in").cast(pl.Int64, strict=False),
            pl.when(pl.col("status") == "Active")
            .then(pl.lit("Active"))
            .otherwise(pl.col("status"))
            .alias("status"),
            pl.col("active")
            .str.extract(r"(\d{4})", 0)
            .cast(pl.Int64, strict=False)
            .alias("start_year")
        ])
    )


@task
def transform_reviews(df: pl.DataFrame) -> pl.DataFrame:
    return (df
    .with_columns([
        pl.col("id").cast(pl.Int64),
        pl.col("album").cast(pl.Int64),
        pl.col("score").cast(pl.Float64),
        pl.col("content").str.replace_all(r"\|", ",").alias("content")
    ])
    )


@task
def create_music_catalog(albums_df: pl.DataFrame, bands_df: pl.DataFrame) -> pl.DataFrame:
    albums = albums_df.rename({
        "id": "album_id",
        "title": "album_title",
        "band": "band_id",
    })

    bands = bands_df.rename({
        "id": "band_id",
        "name": "band_name",
    })

    return (
        albums
        .join(bands, on="band_id", how="left")
        .select([
            "album_id", "album_title", "year",
            "band_id", "band_name", "country",
            "genre", "theme",
        ])
    )


@task
def create_album_reviews(albums_df: pl.DataFrame, reviews_df: pl.DataFrame) -> pl.DataFrame:
    albums = albums_df.rename({"id": "album_id", "title": "album_title"})
    reviews = reviews_df.rename({"id": "review_id", "album": "album_id"})

    return (
        reviews
        .join(albums, on="album_id", how="left")
        .select([
            "review_id", "album_id", "album_title",
            "score", "content",
        ])
    )


@task
def write_silver_parquet(df: pl.DataFrame, dataset_name: str) -> str:
    s3 = boto("s3")

    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)

    key = f"{SILVER_PREFIX}/{dataset_name}/{dataset_name}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{BUCKET}/{key}"


@flow(name="silver-transform-flow")
def silver_transform_flow(bronze_paths: Dict[str, str]) -> Dict[str, str]:
    ensure_bucket()
    result = {}

    dfs = {}
    for name, path in bronze_paths.items():
        dfs[name] = read_bronze_parquet(path)

    transformed = {}
    if "albums" in dfs:
        transformed["albums"] = transform_albums(dfs["albums"])
        result["albums"] = write_silver_parquet(transformed["albums"], "albums")

    if "bands" in dfs:
        transformed["bands"] = transform_bands(dfs["bands"])
        result["bands"] = write_silver_parquet(transformed["bands"], "bands")

    if "reviews" in dfs:
        transformed["reviews"] = transform_reviews(dfs["reviews"])
        result["reviews"] = write_silver_parquet(transformed["reviews"], "reviews")

    if "albums" in transformed and "bands" in transformed:
        music_catalog = create_music_catalog(transformed["albums"], transformed["bands"])
        result["music_catalog"] = write_silver_parquet(music_catalog, "music_catalog")

    if "albums" in transformed and "reviews" in transformed:
        album_reviews = create_album_reviews(transformed["albums"], transformed["reviews"])
        result["album_reviews"] = write_silver_parquet(album_reviews, "album_reviews")

    return result


# ──────────────────────────── CLI ───────────────────────────────
if __name__ == "__main__":
    example_bronze_paths = {
        "albums": f"s3://{BUCKET}/{BRONZE_PREFIX}/albums/albums.parquet",
        "bands": f"s3://{BUCKET}/{BRONZE_PREFIX}/bands/bands.parquet",
        "reviews": f"s3://{BUCKET}/{BRONZE_PREFIX}/reviews/reviews.parquet"
    }

    silver_paths = silver_transform_flow(example_bronze_paths)

    print("\n✨ Camada Silver:")
    for name, path in silver_paths.items():
        print(f" • {name}: {path}")
