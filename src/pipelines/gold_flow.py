
from __future__ import annotations
from prefect import flow, task
from src.domain.review_metrics import ReviewMetrics
from src.domain.catalog import get_catalog
import daft

CATALOG = get_catalog()

def write(df: daft.DataFrame, table_id: str):
    if not CATALOG.table_exists(table_id):
        CATALOG.create_table(table_id, schema=df.to_arrow().schema)
    df.write_iceberg(CATALOG.load_table(table_id), mode="append")

@task
def top10_task(svc: ReviewMetrics):
    return svc.top10_by_country()

@task
def stats_task(svc: ReviewMetrics):
    return svc.band_stats()

@flow(name="gold-daft-flow")
def gold_flow():
    svc = ReviewMetrics()
    write(top10_task(svc), "gold.top10_by_country")
    write(stats_task(svc), "gold.band_avg_scores")

if __name__ == "__main__":
    gold_flow()
