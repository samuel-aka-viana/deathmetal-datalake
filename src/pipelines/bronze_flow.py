
from prefect import flow, task
import daft
from src.domain.album_service import AlbumService
from src.domain.band_service import BandService
from src.domain.review_service import ReviewService
from src.domain.catalog import get_catalog

CATALOG = get_catalog()

def write(df: daft.DataFrame, table_id: str):
    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)
    df.write_iceberg(CATALOG.load_table(table_id), mode="append")

@task
def bronze_albums():
    svc = AlbumService()
    write(svc.albums_bronze(), "bronze.albums")

@task
def bronze_bands():
    write(BandService().bands_bronze(), "bronze.bands")

@task
def bronze_reviews():
    write(ReviewService().reviews_bronze(), "bronze.reviews")

@flow
def bronze_flow():
    bronze_albums()
    bronze_bands()
    bronze_reviews()

if __name__ == "__main__":
    bronze_flow()
