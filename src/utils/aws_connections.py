# src/utils/aws_connections.py
import boto3
from botocore.exceptions import ClientError

class AWSConnector:
    def __init__(self, aws_config: dict):
        self.aws_config = aws_config
        self._rds_client = None
        self._s3_client = None
        self._athena_client = None

    def get_rds_password(self) -> str:
        """Obtiene el token de autenticación para RDS usando AWS IAM"""
        try:
            if not self._rds_client:
                self._rds_client = boto3.client(
                    'rds',
                    region_name=self.aws_config['region'],
                    aws_access_key_id=self.aws_config['access_key_id'],
                    aws_secret_access_key=self.aws_config['secret_access_key']
                )

            token = self._rds_client.generate_db_auth_token(
                DBHostname=self.aws_config['db_host'],
                Port=self.aws_config['db_port'],
                DBUsername=self.aws_config['db_user'],
                Region=self.aws_config['region']
            )
            return token
        except ClientError as e:
            raise Exception(f"Error generando token RDS: {str(e)}")

    @property
    def s3_client(self):
        """Obtiene cliente de S3"""
        if not self._s3_client:
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_config['access_key_id'],
                aws_secret_access_key=self.aws_config['secret_access_key'],
                region_name=self.aws_config['region']
            )
        return self._s3_client

    @property
    def athena_client(self):
        """Obtiene cliente de Athena"""
        if not self._athena_client:
            self._athena_client = boto3.client(
                'athena',
                aws_access_key_id=self.aws_config['access_key_id'],
                aws_secret_access_key=self.aws_config['secret_access_key'],
                region_name=self.aws_config['region']
            )
        return self._athena_client