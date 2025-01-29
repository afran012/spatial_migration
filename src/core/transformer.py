import pandas as pd

class SpatialDataTransformer:
    @staticmethod
    def transform_geometry(gdf: pd.DataFrame, geom_type: str = 'MULTIPOLYGON') -> pd.DataFrame:
        """Transforma geometrías a formato WKT"""
        df = pd.DataFrame(gdf)
        df['wkt_geometry'] = gdf.geom.apply(
            lambda x: x.wkt if x is not None else f'{geom_type} EMPTY'
        )
        return df.drop(columns=['geom'])

    @staticmethod
    def convert_dates_to_string(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
        """Convierte columnas de fecha a string"""
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        return df