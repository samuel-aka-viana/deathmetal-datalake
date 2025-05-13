"""Silver flow OO – usa AlbumService, BandService, ReviewService"""
from __future__ import annotations

import daft
from src.domain.album_service import AlbumService
from src.domain.band_service import BandService
from src.domain.catalog import get_catalog
from src.domain.review_service import ReviewService
from prefect import flow, task

CATALOG = get_catalog()


def write(df: daft.DataFrame, table_id: str):
    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)
    df.write_iceberg(CATALOG.load_table(table_id), mode="append")


# -----------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------
@task
def albums_task() -> daft.DataFrame:
    return AlbumService().albums_silver()


@task
def bands_task() -> daft.DataFrame:
    return BandService().bands_silver()


@task
def reviews_task() -> daft.DataFrame:
    return ReviewService().reviews_silver()


@task
def build_catalog(albums: daft.DataFrame, bands: daft.DataFrame) -> daft.DataFrame:
    albums_mod = albums.with_column_renamed({"id": "album_id", "title": "album_title", "band": "band_id"})
    bands_mod = bands.with_column_renamed({"id": "band_id", "name": "band_name"})
    return albums_mod.join(bands_mod, on="band_id", how="left")


# -----------------------------------------------------------------
# Flow
# -----------------------------------------------------------------
@flow(name="silver-daft-flow")
def silver_flow():
    albums_df = albums_task()
    bands_df = bands_task()
    reviews_df = reviews_task()

    write(albums_df, "silver.albums")
    write(bands_df, "silver.bands")
    write(reviews_df, "silver.reviews")

    catalog_df = build_catalog(albums_df, bands_df)
    write(catalog_df, "silver.music_catalog")


if __name__ == "__main__":
    silver_flow()
