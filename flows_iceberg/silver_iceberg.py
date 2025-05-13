"""Silver flow usando Daft → lê Bronze Iceberg, transforma e grava Silver Iceberg"""
from __future__ import annotations

import os

import daft
from daft import col, DataType
from prefect import flow, task
from pyiceberg.catalog import load_catalog

CATALOG = load_catalog(
    "nessie",
    uri=os.getenv("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1"),
    warehouse=os.getenv("WAREHOUSE", "s3a://datalake/warehouse"),
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def read_daft(table_id: str) -> daft.DataFrame:
    table = CATALOG.load_table(table_id)
    return daft.read_iceberg(table)


def write_daft(df: daft.DataFrame, table_id: str, mode: str = "append"):
    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)
    table = CATALOG.load_table(table_id)
    df.write_iceberg(table, mode=mode)


# ---------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------
@task
def transform_albums(df: daft.DataFrame) -> daft.DataFrame:
    return (
        df
        .with_column("id", col("id").cast(DataType.int64()))
        .with_column("band", col("band").cast(DataType.int64()))
        .with_column("year", col("year").cast(DataType.int64()))
    )


@task
def transform_bands(df: daft.DataFrame) -> daft.DataFrame:
    return (
        df
        .with_column("id", col("id").cast(DataType.int64()))
        .with_column("formed_in", col("formed_in").cast(DataType.int64()))
    )


@task
def transform_reviews(df: daft.DataFrame) -> daft.DataFrame:
    return (
        df
        .filter(~col("id").cast(DataType.string()).str.contains("id"))
        .with_column("id", col("id").cast(DataType.int64()))
        .with_column("album", col("album").cast(DataType.int64()))
        .with_column("score", col("score").cast(DataType.float64()))
    )


@task
def join_music_catalog(albums: daft.DataFrame, bands: daft.DataFrame) -> daft.DataFrame:
    albums_mod = (
        albums
        .with_column_renamed({"id": "album_id", "title": "album_title", "band": "band_id"})
    )
    bands_mod = (
        bands.with_column_renamed({"id": "band_id", "name": "band_name"})
    )
    return albums_mod.join(bands_mod, on="band_id", how="left")


# ---------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------
@flow(name="silver-daft-flow")
def silver_flow():
    albums_df = transform_albums(read_daft("bronze.albums"))
    bands_df = transform_bands(read_daft("bronze.bands"))
    reviews_df = transform_reviews(read_daft("bronze.reviews"))

    write_daft(albums_df, "silver.albums")
    write_daft(bands_df, "silver.bands")
    write_daft(reviews_df, "silver.reviews")

    catalog_df = join_music_catalog(albums_df, bands_df)
    write_daft(catalog_df, "silver.music_catalog")


if __name__ == "__main__":
    silver_flow()
