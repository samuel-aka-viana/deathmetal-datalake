
import daft
from daft import col, DataType
from .catalog import get_catalog

class ReviewService:
    def __init__(self, branch: str = "main"):
        self.catalog = get_catalog(branch)

    def reviews_bronze(self):
        return daft.read_iceberg(self.catalog.load_table("bronze.reviews"))

    def reviews_silver(self):
        return self.reviews_bronze()\
            .filter(~col("id").cast(DataType.string()).str.contains("id"))\
            .with_column("id", col("id").cast(DataType.int64()))\
            .with_column("album", col("album").cast(DataType.int64()))\
            .with_column("score", col("score").cast(DataType.float64()))
