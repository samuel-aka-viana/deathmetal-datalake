"""
Landing flow refatorado com melhor tratamento de erros.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from src.domain.landing_service import LandingService

logger = logging.getLogger(__name__)


@task(
    name="list-csv-files",
    description="Lista arquivos CSV em uma pasta",
    tags=["csv", "landing"],
    retries=2,
    retry_delay_seconds=5
)
def list_csv(folder: str = "csv") -> List[Path]:
    """
    Lista todos os arquivos CSV em uma pasta.

    Args:
        folder: Caminho para a pasta contendo arquivos CSV

    Returns:
        Lista de caminhos para arquivos CSV
    """
    try:
        path = Path(folder)
        if not path.exists():
            logger.warning(f"Pasta não encontrada: {folder}")
            return []

        files = sorted(path.glob("*.csv"))
        logger.info(f"Encontrados {len(files)} arquivos CSV em {folder}")
        return files
    except Exception as e:
        logger.error(f"Erro ao listar arquivos CSV em {folder}: {str(e)}")
        raise


@task(
    name="upload-csv-chunks",
    description="Divide e envia um CSV para o bucket S3/MinIO",
    log_prints=True,
    tags=["csv", "s3", "landing"],
    retries=3,
    retry_delay_seconds=10
)
def upload_csv(csv_path: Path) -> List[str]:
    """
    Divide e envia um arquivo CSV para o bucket.

    Args:
        csv_path: Caminho para o arquivo CSV

    Returns:
        Lista de chaves S3 criadas
    """
    try:
        service = LandingService()
        keys = service.chunk_and_upload(csv_path)
        logger.info(f"✅ {csv_path.name}: {len(keys)} parte(s) enviadas")
        return keys
    except Exception as e:
        logger.error(f"Erro ao processar {csv_path}: {str(e)}")
        raise


@flow(
    name="landing-flow",
    description="Ingestão de CSVs para o bucket S3/MinIO",
    task_runner=ConcurrentTaskRunner(),
)
def landing_flow(folder: str = "csv") -> List[str]:
    """
    Flow de landing para processar CSVs e enviar para o bucket.

    Args:
        folder: Pasta contendo os arquivos CSV

    Returns:
        Lista de todas as chaves S3 criadas
    """
    all_keys: List[str] = []

    # Listar arquivos CSV
    csv_files = list_csv(folder)

    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em {folder}")
        return all_keys

    # Processar CSVs em paralelo
    for key_list in upload_csv.map(csv_files):
        if key_list:
            all_keys.extend(key_list)

    logger.info(f"Total de {len(all_keys)} partes enviadas para o bucket")
    return all_keys


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Executar o flow
    keys = landing_flow("csv")
    print(f"Total de chaves criadas: {len(keys)}")
