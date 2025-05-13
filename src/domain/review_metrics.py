
import daft
from daft import col
from .catalog import get_catalog

class ReviewMetrics:
    def __init__(self, branch: str = "main"):
        self.catalog = get_catalog(branch)

    def music_catalog(self):
        return daft.read_iceberg(self.catalog.load_table("silver.music_catalog"))

    def reviews(self):
        return daft.read_iceberg(self.catalog.load_table("silver.reviews"))

    def top10_by_country(self):
        reviews = self.reviews().rename({"id": "review_id", "album": "album_id"})
        music = self.music_catalog()
        joined = reviews.join(music, on="album_id", how="left")
        grouped = joined.groupby(["country", "band_id", "band_name"]).agg(
            col("review_id").count().alias("review_count"),
            col("score").mean().alias("avg_score")
        )
        return grouped.sort(["country", "review_count"], descending=[False, True])\
                      .groupby("country").head(10)

    def band_stats(self):
        reviews = self.reviews().rename({"id": "review_id", "album": "album_id"})
        music = self.music_catalog()
        joined = reviews.join(music, on="album_id", how="left")
        return joined.groupby(["band_id", "band_name", "country"]).agg(
            col("review_id").count().alias("review_count"),
            col("score").mean().alias("avg_score"),
            col("score").min().alias("min_score"),
            col("score").max().alias("max_score"),
        ).sort("avg_score", descending=True)
