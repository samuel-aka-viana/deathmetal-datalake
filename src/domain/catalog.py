"""
Versão refatorada do módulo catalog.py usando configuração centralizada.
"""
from pyiceberg.catalog import load_catalog

from .config import get_config


def get_catalog(branch: str = None):
    """
    Obtém um catálogo Iceberg conectado ao Nessie.

    Args:
        branch: Referência (branch) do Nessie. Se None, usa o padrão da configuração.
    """
    config = get_config()
    ref = branch if branch is not None else config.default_branch

    try:
        return load_catalog(
            "nessie",
            uri=config.nessie_uri,
            warehouse=config.warehouse_uri,
            reference=ref
        )
    except Exception as e:
        print(f"Erro ao conectar ao catálogo Nessie: {str(e)}")
        raise
