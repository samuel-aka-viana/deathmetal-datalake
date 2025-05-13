"""
Serviço LandingService refatorado com melhor tratamento de erros e configuração centralizada.
"""
import logging
import time
from pathlib import Path
from typing import List

import boto3

from .config import get_config

logger = logging.getLogger(__name__)


class LandingService:
    """Envia CSV locais para o bucket MinIO/S3 na zona landing."""

    def __init__(self, config=None):
        """
        Inicializa o serviço de landing.

        Args:
            config: Configuração opcional. Se None, usa a configuração global.
        """
        if config is None:
            config = get_config()

        self.bucket = config.bucket
        self.prefix = config.landing_prefix
        self.max_bytes = config.chunk_size_bytes

        try:
            self.s3 = boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id=config.s3_access_key,
                aws_secret_access_key=config.s3_secret_key,
                endpoint_url=config.s3_endpoint,
            )
            # Teste de conexão
            self.s3.list_buckets()
        except Exception as e:
            logger.error(f"Erro ao conectar ao S3/MinIO: {str(e)}")
            raise

    def chunk_and_upload(self, csv_path: Path) -> List[str]:
        """
        Divide CSV em partes <= max_bytes e envia; retorna chaves criadas.

        Args:
            csv_path: Caminho para o arquivo CSV a ser enviado

        Returns:
            Lista de chaves S3 criadas

        Raises:
            FileNotFoundError: Se o arquivo não existir
            ValueError: Se o arquivo estiver vazio
            Exception: Para outros erros durante upload
        """
        # Validação de entrada
        if not csv_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

        # Ler o conteúdo do arquivo
        try:
            lines = csv_path.read_text().splitlines()
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo {csv_path}: {str(e)}")
            raise

        if not lines:
            raise ValueError(f"Arquivo vazio: {csv_path}")

        dataset = csv_path.stem.lower()
        ts = int(time.time() * 1000)
        keys = []

        # Processar o arquivo em chunks
        header = lines[0] + "\n"
        chunk = header
        size = len(chunk.encode())
        part = 0

        for line in lines[1:]:
            encoded = (line + "\n").encode()
            if size + len(encoded) > self.max_bytes:
                # O chunk está cheio, vamos enviar
                key = f"{self.prefix}{dataset}/{ts}_{part}.csv"
                try:
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=chunk.encode())
                    keys.append(key)
                    logger.debug(f"Enviado chunk {part} para {key}")
                except Exception as e:
                    logger.error(f"Erro ao enviar chunk para {key}: {str(e)}")
                    raise

                part += 1
                chunk = header + line + "\n"
                size = len(chunk.encode())
            else:
                chunk += line + "\n"
                size += len(encoded)

        # Enviar o último chunk se houver dados
        if size > len(header.encode()):
            key = f"{self.prefix}{dataset}/{ts}_{part}.csv"
            try:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=chunk.encode())
                keys.append(key)
                logger.debug(f"Enviado chunk final {part} para {key}")
            except Exception as e:
                logger.error(f"Erro ao enviar chunk final para {key}: {str(e)}")
                raise

        return keys
