"""
Gold flow refatorado para usar o IcebergWriter centralizado.
"""
from __future__ import annotations
import logging
import daft
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from src.domain.album_service import AlbumService
from src.domain.band_service import BandService
from src.domain.review_service import ReviewService
from src.domain.config import get_config
from src.domain.iceberg_writer import write_to_iceberg

# Configurar logging
logger = logging.getLogger(__name__)
config = get_config()


# -----------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------
@task(
    name="process-review-stats-gold",
    description="Processa estatísticas de reviews para a camada gold",
    tags=["gold", "reviews", "stats"],
    retries=2
)
def review_stats_task() -> daft.DataFrame:
    """Processa estatísticas de reviews para a camada gold."""
    try:
        logger.info("Processando estatísticas de reviews para camada gold")
        return ReviewService().review_stats_gold()
    except Exception as e:
        logger.error(f"Erro ao processar estatísticas de reviews: {str(e)}")
        raise


@task(
    name="process-band-popularity-gold",
    description="Processa dados de popularidade de bandas para a camada gold",
    tags=["gold", "bands", "popularity"],
    retries=2
)
def band_popularity_task() -> daft.DataFrame:
    """Processa dados de popularidade de bandas para a camada gold."""
    try:
        logger.info("Processando popularidade de bandas para camada gold")
        return BandService().band_popularity_gold()
    except Exception as e:
        logger.error(f"Erro ao processar popularidade de bandas: {str(e)}")
        raise


@task(
    name="process-top-albums-gold",
    description="Processa top álbuns por avaliação para a camada gold",
    tags=["gold", "albums", "top"],
    retries=2
)
def top_albums_task() -> daft.DataFrame:
    """Processa top álbuns por avaliação para a camada gold."""
    try:
        logger.info("Processando top álbuns para camada gold")
        return AlbumService().top_albums_gold()
    except Exception as e:
        logger.error(f"Erro ao processar top álbuns: {str(e)}")
        raise


@task(
    name="build-music-analytics",
    description="Constrói analítica consolidada do catálogo musical",
    tags=["gold", "analytics"],
)
def build_analytics(reviews_stats: daft.DataFrame, band_popularity: daft.DataFrame) -> daft.DataFrame:
    """
    Constrói uma visão analítica consolidada combinando estatísticas de reviews e popularidade de bandas.

    Args:
        reviews_stats: DataFrame com estatísticas de reviews
        band_popularity: DataFrame com dados de popularidade de bandas

    Returns:
        DataFrame combinado com analítica consolidada
    """
    try:
        logger.info("Construindo analítica consolidada de música")
        return reviews_stats.join(band_popularity, on="band_id", how="inner")
    except Exception as e:
        logger.error(f"Erro ao construir analítica consolidada: {str(e)}")
        raise


# -----------------------------------------------------------------
# Flow
# -----------------------------------------------------------------
@flow(
    name="gold-flow",
    description="Processa dados da camada Silver para a camada Gold",
    task_runner=ConcurrentTaskRunner(),
)
def gold_flow():
    """Flow para processar dados da camada silver para a camada gold."""
    try:
        logger.info("Iniciando processamento da camada Gold")

        # Processar dados em paralelo
        review_stats_df = review_stats_task()
        band_popularity_df = band_popularity_task()
        top_albums_df = top_albums_task()

        # Validar resultados
        if review_stats_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado estatístico de reviews encontrado na camada silver")

        if band_popularity_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de popularidade de bandas encontrado na camada silver")

        if top_albums_df.count().to_pandas().iloc[0, 0] == 0:
            logger.warning("Nenhum dado de top álbuns encontrado na camada silver")

        # Escrever tabelas principais
        write_to_iceberg(review_stats_df, config.get_gold_table("review_stats"))
        write_to_iceberg(band_popularity_df, config.get_gold_table("band_popularity"))
        write_to_iceberg(top_albums_df, config.get_gold_table("top_albums"))

        # Construir e escrever a analítica consolidada
        analytics_df = build_analytics(review_stats_df, band_popularity_df)
        write_to_iceberg(analytics_df, config.get_gold_table("music_analytics"))

        logger.info("Processamento da camada Gold concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro no flow Gold: {str(e)}")
        raise


if __name__ == "__main__":
    # Configurar logging para console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Executar o flow
    gold_flow()