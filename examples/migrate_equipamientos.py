from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Verificar variables críticas
required_vars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_DEFAULT_REGION',
    'PROD_POSTGRES_USER',
    'PROD_POSTGRES_HOST',
    'PROD_POSTGRES_PORT',
    'PROD_POSTGRES_DB',
    'ATHENA_DATABASE',
    'ATHENA_OUTPUT_BUCKET',
    'ATHENA_OUTPUT_PREFIX',
    'S3_PREFIX'
]

missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("Faltan las siguientes variables de entorno:")
    for var in missing_vars:
        print(f"- {var}")
    exit(1)

print("Variables de entorno verificadas correctamente")

# Resto del código existente
import sys
from pathlib import Path

# Añadir el directorio src al path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.core.base_migrator import BaseSpatialMigrator
from src.core.extractor import SpatialDataExtractor
from src.core.transformer import SpatialDataTransformer
from src.core.loader import SpatialDataLoader
import yaml
from datetime import datetime

class EquipamientosMigrator(BaseSpatialMigrator):
    def migrate(self):
        """Ejecuta la migración de equipamientos"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Inicializar componentes con autenticación AWS
            connection_params = self.get_connection_params()
            extractor = SpatialDataExtractor(connection_params)
            
            transformer = SpatialDataTransformer()
            
            loader = SpatialDataLoader({
                'bucket': os.getenv('ATHENA_OUTPUT_BUCKET'),
                'prefix': os.getenv('S3_PREFIX')
            })
            
            # Proceso de migración
            total, unique = extractor.get_table_count(
                self.config['source']['schema'],
                self.config['source']['table']
            )
            
            print(f"Registros encontrados: {total} (únicos: {unique})")
            
            gdf = extractor.extract_data(
                self.config['source']['schema'],
                self.config['source']['table'],
                self.config['source']['columns']
            )
            
            df = transformer.transform_geometry(gdf, self.config['geometry_type'])
            df = transformer.convert_dates_to_string(df, self.config.get('date_columns', []))
            
            # Ubicaciones S3
            s3_location = self.get_s3_location(timestamp)
            athena_output = self.get_athena_output_location()
            
            local_path = f"temp_{self.config['target']['table']}_{timestamp}.parquet"
            s3_path = f"{self.config['target']['prefix']}/{timestamp}/data.parquet"
            
            # Cargar datos
            loader.save_to_parquet(df, local_path)
            loader.upload_to_s3(local_path, s3_path)
            
            loader.create_athena_table(
                os.getenv('ATHENA_DATABASE'),
                self.config['target']['table'],
                self.config['target']['schema'],
                s3_location
            )
            
            return True
            
        except Exception as e:
            print(f"Error en la migración: {str(e)}")
            return False

def main():
    # Cargar configuración
    with open('config/table_schemas/equipamientos.yaml') as f:
        config = yaml.safe_load(f)
    
    migrator = EquipamientosMigrator(config)
    migrator.migrate()

if __name__ == "__main__":
    main()