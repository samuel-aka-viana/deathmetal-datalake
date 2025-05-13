
import daft
from daft import col, DataType
from .catalog import get_catalog

class AlbumService:
    def __init__(self, branch: str = "main"):
        self.catalog = get_catalog(branch)

    def albums_bronze(self) -> daft.DataFrame:
        return daft.read_iceberg(self.catalog.load_table("bronze.albums"))

    def albums_silver(self) -> daft.DataFrame:
        df = self.albums_bronze()\
            .with_column("id", col("id").cast(DataType.int64()))\
            .with_column("band", col("band").cast(DataType.int64()))\
            .with_column("year", col("year").cast(DataType.int64()))
        return df
