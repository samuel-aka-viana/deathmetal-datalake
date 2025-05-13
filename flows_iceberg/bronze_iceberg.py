"""
bronze_daft.py – converte CSV da landing em Iceberg (namespace bronze) usando Daft
"""
import os
from pathlib import Path
from typing import List

import daft
from pyiceberg.catalog import load_catalog
from prefect import flow, task

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
CATALOG = load_catalog(
    "nessie",
    uri=os.getenv("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1"),
    warehouse=os.getenv("WAREHOUSE", "s3a://datalake/warehouse"),
)
LANDING_PREFIX = Path("/data/landing")  # ou "s3://datalake/landing"
DATASETS = {"albums", "bands", "reviews"}  # validação simples


# ---------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------
@task
def list_csv() -> List[Path]:
    return sorted(p for p in LANDING_PREFIX.glob("*.csv") if p.stem in DATASETS)


@task(log_prints=True, retries=3, retry_delay_seconds=10)
def csv_to_iceberg(csv_path: Path) -> str:
    dataset = csv_path.stem.lower()
    table_id = f"bronze.{dataset}"

    # 1. Ler CSV em Daft DataFrame
    df = daft.read_csv(csv_path.as_posix(), infer_schema_length=5000)

    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)

    table = CATALOG.load_table(table_id)
    df.write_iceberg(table, mode="overwrite")
    print(f"✅ {len(df)} linhas -> {table_id}")
    return table_id


# ---------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------
@flow(name="bronze-daft-flow")
def bronze_flow():
    csv_files = list_csv()
    out = csv_to_iceberg.map(csv_files)
    return out


if __name__ == "__main__":
    bronze_flow()
