# setup.py
from setuptools import setup, find_packages

setup(
    name="spatial_migration",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'geopandas',
        'pandas',
        'sqlalchemy',
        'boto3',
        'pyarrow',
        'python-dotenv',
        'pyyaml'
    ]
)