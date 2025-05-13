
from __future__ import annotations
from pathlib import Path
from typing import List
from prefect import flow, task
from domain.landing_service import LandingService

@task
def list_csv(folder: str = "csv") -> List[Path]:
    return sorted(Path(folder).glob("*.csv"))

@task(log_prints=True)
def upload_csv(csv_path: Path) -> List[str]:
    service = LandingService()
    keys = service.chunk_and_upload(csv_path)
    print(f"✅ {csv_path.name}: {len(keys)} parte(s) enviadas")
    return keys

@flow(name="landing-flow")
def landing_flow(folder: str = "csv") -> List[str]:
    all_keys: List[str] = []
    for key_list in upload_csv.map(list_csv(folder)):
        all_keys.extend(key_list)
    return all_keys

if __name__ == "__main__":
    print(landing_flow("csv"))
