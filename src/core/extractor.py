from typing import Optional, Tuple
import geopandas as gpd
from sqlalchemy import create_engine, text

class SpatialDataExtractor:
    def __init__(self, connection_params: dict):
        self.connection_params = connection_params
        self._engine = None

    @property
    def engine(self):
        if not self._engine:
            conn_string = (
                f"postgresql://{self.connection_params['user']}:{self.connection_params['password']}"
                f"@{self.connection_params['host']}:{self.connection_params['port']}"
                f"/{self.connection_params['database']}"
            )
            self._engine = create_engine(conn_string)
        return self._engine

    def get_table_count(self, schema: str, table: str) -> Tuple[int, int]:
        """Obtiene el conteo total y único de registros"""
        with self.engine.connect() as conn:
            total = conn.execute(text(f'SELECT COUNT(*) FROM {schema}.{table}')).scalar()
            unique = conn.execute(text(f'SELECT COUNT(DISTINCT id) FROM {schema}.{table}')).scalar()
        return total, unique

    def extract_data(self, schema: str, table: str, columns: list) -> gpd.GeoDataFrame:
        """Extrae datos espaciales de PostgreSQL"""
        columns_str = ', '.join(columns)
        query = f"""
        SELECT DISTINCT ON (id) {columns_str}
        FROM {schema}.{table}
        ORDER BY id
        """
        return gpd.read_postgis(query, self.engine, geom_col='geom')