import os
from datetime import datetime
from ..utils.aws_connections import AWSConnector

class BaseSpatialMigrator:
    def __init__(self, config: dict):
        self.config = config
        self.aws_connector = AWSConnector({
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region': os.getenv('AWS_DEFAULT_REGION'),
            'db_host': os.getenv('PROD_POSTGRES_HOST'),
            'db_port': int(os.getenv('PROD_POSTGRES_PORT', '5432')),
            'db_user': os.getenv('PROD_POSTGRES_USER')
        })

    def get_connection_params(self) -> dict:
        """Obtiene parámetros de conexión con autenticación AWS"""
        password = self.aws_connector.get_rds_password()
        return {
            'user': os.getenv('PROD_POSTGRES_USER'),
            'password': password,
            'host': os.getenv('PROD_POSTGRES_HOST'),
            'port': os.getenv('PROD_POSTGRES_PORT'),
            'database': os.getenv('PROD_POSTGRES_DB')
        }

    def get_s3_location(self, timestamp: str) -> str:
        """Genera la ubicación S3 para los datos"""
        return (
            f"s3://{os.getenv('ATHENA_OUTPUT_BUCKET')}/"
            f"{os.getenv('S3_PREFIX')}/{timestamp}"
        )

    def get_athena_output_location(self) -> str:
        """Obtiene la ubicación de salida para consultas Athena"""
        return (
            f"s3://{os.getenv('ATHENA_OUTPUT_BUCKET')}/"
            f"{os.getenv('ATHENA_OUTPUT_PREFIX')}"
        )