
from pyiceberg.catalog import load_catalog
import os

def get_catalog(branch: str = "main"):
    return load_catalog(
        "nessie",
        uri=os.getenv("NESSIE_URI", "http://nessie.lakehouse.svc.cluster.local:19120/api/v1"),
        warehouse=os.getenv("WAREHOUSE", "s3a://datalake/warehouse"),
        reference=branch
    )
