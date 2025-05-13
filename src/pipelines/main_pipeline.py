import logging

from prefect import flow

from .bronze_flow import bronze_flow
from .gold_flow import gold_flow
from .landing_flow import landing_flow
from .silver_flow import silver_flow

# Configurar logging
logger = logging.getLogger(__name__)


@flow(
    name="lakehouse-pipeline",
    description="Pipeline completo de Lakehouse: Landing → Bronze → Silver → Gold",
)
def lakehouse_pipeline(csv_folder: str = "csv"):
    try:
        logger.info("Iniciando pipeline Lakehouse")

        logger.info("Iniciando etapa Landing")
        keys = landing_flow(folder=csv_folder)
        logger.info(f"Concluída etapa Landing: {len(keys)} arquivos processados")

        if not keys:
            logger.warning("Nenhum arquivo processado na etapa Landing. Pipeline será interrompido.")
            return

        logger.info("Iniciando etapa Bronze")
        bronze_flow()
        logger.info("Concluída etapa Bronze")

        logger.info("Iniciando etapa Silver")
        silver_flow()
        logger.info("Concluída etapa Silver")

        logger.info("Iniciando etapa Gold")
        gold_flow()
        logger.info("Concluída etapa Gold")

        logger.info("Pipeline Lakehouse concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro no pipeline Lakehouse: {str(e)}")
        raise

    return {"status": "success", "files_processed": len(keys) if keys else 0}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    lakehouse_pipeline()
