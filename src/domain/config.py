"""
Serviço centralizado para configurações do ambiente.
"""
import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class LakehouseConfig:
    """Configurações consolidadas do ambiente de data lakehouse."""
    # Nessie
    nessie_uri: str = os.getenv("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1")
    default_branch: str = os.getenv("NESSIE_BRANCH", "main")

    # S3/MinIO
    bucket: str = os.getenv("BUCKET", "datalake")
    s3_endpoint: str = os.getenv("AWS_ENDPOINT", "http://minio.lakehouse.svc.cluster.local:9000")
    s3_access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    s3_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

    # Caminhos
    warehouse_uri: str = os.getenv("WAREHOUSE", "s3a://datalake/warehouse")
    landing_prefix: str = os.getenv("LANDING_PREFIX", "landing/")

    # Limites
    chunk_size_bytes: int = int(os.getenv("CHUNK_SIZE_BYTES", 900 * 1024))  # ~900KB

    # Camadas
    bronze_namespace: str = os.getenv("BRONZE_NAMESPACE", "bronze")
    silver_namespace: str = os.getenv("SILVER_NAMESPACE", "silver")
    gold_namespace: str = os.getenv("GOLD_NAMESPACE", "gold")

    def get_bronze_table(self, table_name: str) -> str:
        """Retorna o ID completo de uma tabela na camada bronze."""
        return f"{self.bronze_namespace}.{table_name}"

    def get_silver_table(self, table_name: str) -> str:
        """Retorna o ID completo de uma tabela na camada silver."""
        return f"{self.silver_namespace}.{table_name}"

    def get_gold_table(self, table_name: str) -> str:
        """Retorna o ID completo de uma tabela na camada gold."""
        return f"{self.gold_namespace}.{table_name}"


@lru_cache
def get_config() -> LakehouseConfig:
    """Retorna uma instância singleton da configuração."""
    return LakehouseConfig()
