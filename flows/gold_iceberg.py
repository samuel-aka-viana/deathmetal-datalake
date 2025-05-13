"""Gold flow usando Daft – rank corrigido sem `.rank()`"""
from __future__ import annotations
import os
import daft
from daft import col
from pyiceberg.catalog import load_catalog
from prefect import flow, task

CATALOG = load_catalog(
    "nessie",
    uri=os.getenv("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1"),
    warehouse=os.getenv("WAREHOUSE", "s3a://datalake/warehouse"),
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def read_df(table_id: str) -> daft.DataFrame:
    return daft.read_iceberg(CATALOG.load_table(table_id))

def write_df(df: daft.DataFrame, table_id: str, mode: str = "append"):
    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)
    df.write_iceberg(CATALOG.load_table(table_id), mode=mode)

# ---------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------
@task
def create_top10_by_country(music: daft.DataFrame, reviews: daft.DataFrame) -> daft.DataFrame:
    reviews_mod = reviews.with_column_renamed({"id": "review_id", "album": "album_id"})
    joined = reviews_mod.join(music, on="album_id", how="left")

    grouped = (
        joined.groupby(["country", "band_id", "band_name"])
        .agg(
            col("review_id").count().alias("review_count"),
            col("score").mean().alias("avg_score"),
        )
    )

    # Ordena por país e review_count desc, depois pega as 10 primeiras de cada país
    top10 = (
        grouped.sort(["country", "review_count"], descending=[False, True])
               .groupby("country")
               .head(10)
    )
    return top10

@task
def create_band_avg_scores(music: daft.DataFrame, reviews: daft.DataFrame) -> daft.DataFrame:
    reviews_mod = reviews.with_column_renamed({"id": "review_id", "album": "album_id"})
    joined = reviews_mod.join(music, on="album_id", how="left")
    return (
        joined.groupby(["band_id", "band_name", "country"])
        .agg(
            col("review_id").count().alias("review_count"),
            col("score").mean().alias("avg_score"),
            col("score").min().alias("min_score"),
            col("score").max().alias("max_score"),
        )
        .sort("avg_score", descending=True)
    )

# ---------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------
@flow(name="gold-daft-flow")
def gold_flow():
    music = read_df("silver.music_catalog")
    reviews = read_df("silver.reviews")

    top10 = create_top10_by_country(music, reviews)
    write_df(top10, "gold.top10_by_country")

    avg_scores = create_band_avg_scores(music, reviews)
    write_df(avg_scores, "gold.band_avg_scores")

if __name__ == "__main__":
    gold_flow()