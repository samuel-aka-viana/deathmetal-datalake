import daft
from prefect import task

from .catalog import get_catalog


class IcebergWriter:
    def __init__(self, branch: str = "main"):
        self.catalog = get_catalog(branch)

    def write(self, df: daft.DataFrame, table_id: str, mode: str = "append"):
        try:
            if not self.catalog.table_exists(table_id):
                self.catalog.create_table(table_id, schema=df.to_arrow().schema)

            df.write_iceberg(self.catalog.load_table(table_id), mode=mode)
            return True
        except Exception as e:
            # Log do erro e reraise para o Prefect lidar
            print(f"Erro ao escrever na tabela {table_id}: {str(e)}")
            raise


@task
def write_to_iceberg(df: daft.DataFrame, table_id: str, branch: str = "main", mode: str = "append") -> bool:
    writer = IcebergWriter(branch)
    return writer.write(df, table_id, mode)
