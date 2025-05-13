from __future__ import annotations

import logging

import daft
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from src.domain.album_service import AlbumService
from src.domain.band_service import BandService
from src.domain.config import get_config
from src.domain.iceberg_writer import write_to_iceberg
from src.domain.review_service import ReviewService

logger = logging.getLogger(__name__)
config = get_config()



@task(
    name="process-albums-bronze",
    description="Processa dados de álbuns para a camada bronze",
    tags=["bronze", "albums"],
    retries=2
)
def albums_task() -> daft.DataFrame:
    """Processa os dados de álbuns para a camada bronze."""
    try:
        logger.info("Processando álbuns para camada bronze")
        albums_df = AlbumService().albums_bronze()
        return albums_df
    except Exception as e:
        logger.error(f"Erro ao processar álbuns: {str(e)}")
        raise


@task(
    name="process-bands-bronze",
    description="Processa dados de bandas para a camada bronze",
    tags=["bronze", "bands"],
    retries=2
)
def bands_task() -> daft.DataFrame:
    """Processa os dados de bandas para a camada bronze."""
    try:
        logger.info("Processando bandas para camada bronze")
        bands_df = BandService().bands_bronze()
        return bands_df
    except Exception as e:
        logger.error(f"Erro ao processar bandas: {str(e)}")
        raise


@task(
    name="process-reviews-bronze",
    description="Processa dados de reviews para a camada bronze",
    tags=["bronze", "reviews"],
    retries=2
)
def reviews_task() -> daft.DataFrame:
    """Processa os dados de reviews para a camada bronze."""
    try:
        logger.info("Processando reviews para camada bronze")
        reviews_df = ReviewService().reviews_bronze()
        return reviews_df
    except Exception as e:
        logger.error(f"Erro ao processar reviews: {str(e)}")
        raise


# -----------------------------------------------------------------
# Flow
# -----------------------------------------------------------------
@flow(
    name="bronze-flow",
    description="Processa dados brutos para a camada Bronze",
    task_runner=ConcurrentTaskRunner(),
)
def bronze_flow():
    """Flow para processar dados brutos para a camada bronze."""
    try:
        logger.info("Iniciando processamento da camada Bronze")

        # Processar dados em paralelo
        albums_df = albums_task()
        bands_df = bands_task()
        reviews_df = reviews_task()

        # Validar resultados
        if albums_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de álbum encontrado nas fontes brutas")

        if bands_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de banda encontrado nas fontes brutas")

        if reviews_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de review encontrado nas fontes brutas")

        # Escrever tabelas
        write_to_iceberg(albums_df, config.get_bronze_table("albums"))
        write_to_iceberg(bands_df, config.get_bronze_table("bands"))
        write_to_iceberg(reviews_df, config.get_bronze_table("reviews"))

        logger.info("Processamento da camada Bronze concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro no flow Bronze: {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Executar o flow
    bronze_flow()
