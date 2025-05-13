"""
Silver flow refatorado para usar o IcebergWriter centralizado.
"""
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

# Configurar logging
logger = logging.getLogger(__name__)
config = get_config()


# -----------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------
@task(
    name="process-albums-silver",
    description="Processa dados de álbuns para a camada silver",
    tags=["silver", "albums"],
    retries=2
)
def albums_task() -> daft.DataFrame:
    """Processa os dados de álbuns para a camada silver."""
    try:
        logger.info("Processando álbuns para camada silver")
        return AlbumService().albums_silver()
    except Exception as e:
        logger.error(f"Erro ao processar álbuns: {str(e)}")
        raise


@task(
    name="process-bands-silver",
    description="Processa dados de bandas para a camada silver",
    tags=["silver", "bands"],
    retries=2
)
def bands_task() -> daft.DataFrame:
    """Processa os dados de bandas para a camada silver."""
    try:
        logger.info("Processando bandas para camada silver")
        return BandService().bands_silver()
    except Exception as e:
        logger.error(f"Erro ao processar bandas: {str(e)}")
        raise


@task(
    name="process-reviews-silver",
    description="Processa dados de reviews para a camada silver",
    tags=["silver", "reviews"],
    retries=2
)
def reviews_task() -> daft.DataFrame:
    """Processa os dados de reviews para a camada silver."""
    try:
        logger.info("Processando reviews para camada silver")
        return ReviewService().reviews_silver()
    except Exception as e:
        logger.error(f"Erro ao processar reviews: {str(e)}")
        raise


@task(
    name="build-music-catalog",
    description="Constrói o catálogo de música juntando álbuns e bandas",
    tags=["silver", "catalog"],
)
def build_catalog(albums: daft.DataFrame, bands: daft.DataFrame) -> daft.DataFrame:
    """
    Constrói o catálogo de música combinando dados de álbuns e bandas.

    Args:
        albums: DataFrame com dados de álbuns
        bands: DataFrame com dados de bandas

    Returns:
        DataFrame combinado com catálogo
    """
    try:
        logger.info("Construindo catálogo de música")
        albums_mod = albums.with_column_renamed({"id": "album_id", "title": "album_title", "band": "band_id"})
        bands_mod = bands.with_column_renamed({"id": "band_id", "name": "band_name"})
        return albums_mod.join(bands_mod, on="band_id", how="left")
    except Exception as e:
        logger.error(f"Erro ao construir catálogo: {str(e)}")
        raise


# -----------------------------------------------------------------
# Flow
# -----------------------------------------------------------------
@flow(
    name="silver-flow",
    description="Processa dados da camada Bronze para a camada Silver",
    task_runner=ConcurrentTaskRunner(),
)
def silver_flow():
    """Flow para processar dados da camada bronze para a camada silver."""
    try:
        logger.info("Iniciando processamento da camada Silver")

        # Processar dados em paralelo
        albums_df = albums_task()
        bands_df = bands_task()
        reviews_df = reviews_task()

        # Validar resultados
        if albums_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de álbum encontrado na camada bronze")

        if bands_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de banda encontrado na camada bronze")

        if reviews_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de review encontrado na camada bronze")

        # Escrever tabelas principais
        write_to_iceberg(albums_df, config.get_silver_table("albums"))
        write_to_iceberg(bands_df, config.get_silver_table("bands"))
        write_to_iceberg(reviews_df, config.get_silver_table("reviews"))

        # Construir e escrever o catálogo
        catalog_df = build_catalog(albums_df, bands_df)
        write_to_iceberg(catalog_df, config.get_silver_table("music_catalog"))

        logger.info("Processamento da camada Silver concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro no flow Silver: {str(e)}")
        raise


if __name__ == "__main__":
    # Configurar logging para console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Executar o flow
    silver_flow()
