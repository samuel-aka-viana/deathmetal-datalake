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
df = daft.read_parquet(
    "s3://csv-batch-bucket/silver/album_reviews/*",
    io_config=io_cfg
)
df.show()