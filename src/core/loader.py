import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

class SpatialDataLoader:
    def __init__(self, aws_config: dict):
        self.aws_config = aws_config
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')

    def save_to_parquet(self, df: pd.DataFrame, local_path: str):
        """Guarda datos en formato Parquet"""
        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_path)

    def upload_to_s3(self, local_path: str, s3_path: str):
        """Sube archivo a S3"""
        self.s3_client.upload_file(
            local_path,
            self.aws_config['bucket'],
            s3_path
        )

    def create_athena_table(self, database: str, table_name: str, schema: dict, s3_location: str):
        """Crea tabla en Athena"""
        columns = [f"{k} {v}" for k, v in schema.items()]
        create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
            {','.join(columns)}
        )
        STORED AS PARQUET
        LOCATION '{s3_location}/'
        """
        return self.athena_client.start_query_execution(
            QueryString=create_query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': f"s3://{self.aws_config['bucket']}/{self.aws_config['prefix']}"
            }
        )