
import os, time, boto3
from pathlib import Path
from typing import List

class LandingService:
    """Envia CSV locais para o bucket MinIO/S3 na zona landing."""

    def __init__(self,
                 bucket: str = os.getenv("BUCKET", "datalake"),
                 endpoint: str = os.getenv("AWS_ENDPOINT", "http://minio.lakehouse.svc.cluster.local:9000"),
                 key: str = os.getenv("AWS_ACCESS_KEY_ID", "minio"),
                 secret: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
                 prefix: str = "landing/",
                 max_bytes: int = 900 * 1024):
        self.bucket = bucket
        self.prefix = prefix
        self.max_bytes = max_bytes
        self.s3 = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            endpoint_url=endpoint,
        )

    def chunk_and_upload(self, csv_path: Path) -> List[str]:
        """Divide CSV em partes <= max_bytes e envia; retorna chaves criadas."""
        dataset = csv_path.stem.lower()
        ts = int(time.time() * 1000)
        keys = []
        lines = csv_path.read_text().splitlines()
        header = lines[0] + "\n"
        chunk = header
        size = len(chunk.encode())
        part = 0
        for line in lines[1:]:
            encoded = (line + "\n").encode()
            if size + len(encoded) > self.max_bytes:
                key = f"{self.prefix}{dataset}/{ts}_{part}.csv"
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=chunk.encode())
                keys.append(key)
                part += 1
                chunk = header + line + "\n"
                size = len(chunk.encode())
            else:
                chunk += line + "\n"
                size += len(encoded)
        if size > len(header.encode()):
            key = f"{self.prefix}{dataset}/{ts}_{part}.csv"
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=chunk.encode())
            keys.append(key)
        return keys
