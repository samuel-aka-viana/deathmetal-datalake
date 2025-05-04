import daft
from daft.io import IOConfig, S3Config

io_cfg = IOConfig(
    s3=S3Config(
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        key_id="test",
        access_key="test",
    )
)

df_bands = daft.read_parquet(
    "s3://csv-batch-bucket/silver/bands/bands.parquet",
    io_config=io_cfg
).sort(by=daft.col('id'))

df_bands.show()

df_albums = daft.read_parquet(
    "s3://csv-batch-bucket/silver/albums/albums.parquet",
    io_config=io_cfg
).sort(by=daft.col('id'))

df_albums = df_albums.with_columns_renamed({"id": "album_id"})

df_albums.show()
#
df_reviews = daft.read_parquet(
    "s3://csv-batch-bucket/silver/reviews/reviews.parquet",
    io_config=io_cfg
).sort(by=daft.col('id'))

df_reviews = df_reviews.with_columns_renamed({"album": "album_id"})
df_reviews.show()
#
df_bands_albums = df_albums.join(
    df_bands,
    how="left",
    left_on=daft.col("band"),
    right_on=daft.col("id")
).select(
    daft.col('album_id'),
    daft.col('title').alias("title_album"),
    daft.col('year').alias("year_album"),
    daft.col('name'),
    daft.col('country'),
    daft.col('status'),
    daft.col('formed_in'),
)

df_bands_albums.show()

df_albums_reviews = df_reviews.join(
    df_albums,
    how="right",
    left_on=daft.col("album_id"),
    right_on=daft.col("album_id")
).select(
    daft.col('id').alias("id_review"),
    daft.col('album_id'),
    daft.col('title').alias("title_review"),
    daft.col('score'),
    daft.col('content'),
)

df_albums_reviews.show()
#
df_fulldataset = df_bands_albums.join(
    df_albums_reviews,
    how="left",
    left_on=daft.col("album_id"),
    right_on=daft.col("album_id")
).select(
    daft.col('name'),
    daft.col('country'),
    daft.col('status'),
    daft.col('formed_in'),
    daft.col('title_album'),
    daft.col('year_album'),
    daft.col('title_review'),
    daft.col('score'),
    daft.col('content')
)

df_fulldataset.filter(daft.col('name') != 'None').filter(daft.col('title_review') != 'None').show()
