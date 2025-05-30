import os
import yaml
import logging
from typing import Dict, Any, List

# Database
DB_USER = os.environ.get('PG_LOC_DB_USER')
DB_PASS = os.environ.get('PG_LOC_DB_PASS')
DB_NAME = os.environ.get('PG_LOC_DB_NAME')
DB_HOST = os.environ.get('PG_LOC_DB_HOST', 'localhost')
DB_PORT = os.environ.get('PG_LOC_DB_PORT', '5432')
DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding=utf8"

# Config paths
DATA_CONFIG_PATH = os.environ['DATA_CONFIG_PATH']
DB_CONFIG_PATH = os.environ['DB_CONFIG_PATH']

# Data directory
CSV_FILES_DIR = os.environ['CSV_FILES_DIR']


# Loaders
def config_loader(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            logging.info(f"Config loaded from {config_path}")
            return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise

def csv_files_loader(csv_files_dir: str) -> List[str]:
    try:
        files = os.listdir(csv_files_dir)
        csv_files = [os.path.join(csv_files_dir, file) for file in files if file.endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {csv_files_dir}")

        logging.info(f"Found {len(csv_files)} CSV files in {csv_files_dir}")
        return csv_files
    except Exception as e:
        logging.error(f"Error loading CSV files: {e}")
        raise
