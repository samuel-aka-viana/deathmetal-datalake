
import daft
from daft import col, DataType
from .catalog import get_catalog

class BandService:
    def __init__(self, branch: str = "main"):
        self.catalog = get_catalog(branch)

    def bands_bronze(self) -> daft.DataFrame:
        return daft.read_iceberg(self.catalog.load_table("bronze.bands"))

    def bands_silver(self) -> daft.DataFrame:
        return self.bands_bronze()\
            .with_column("id", col("id").cast(DataType.int64()))\
            .with_column("formed_in", col("formed_in").cast(DataType.int64()))
