import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseBuilder:
    def __init__ (self):
        self.parquet_path = "data/us_nuclear_outages.parquet"
        self.db_path = "data/nuclear_outages.db"

    def build_database(self):
        """
        Transforms the flat Parquet file into a Star Schema and saves it to SQLite
        Note: I chose the Star Schema architecture because of this project's compatibility with DataWarehouse.
        """
        if not os.path.exists(self.parquet_path_path):
            logger.error(f"Source file not found: {self.parquet_path}. Run connector.py first")
            return

        logger.info("Reading raw Parquet data...")
        df_raw = pd.read_parquet(self.parquet_path)

        df_raw['period'] = pd.to_datetime(df_raw['period'])
        df_raw['outage'] = pd.to_numeric(df_raw['outage'], errors='coerce').fillna(0)
        df_raw['capacity'] = pd.to_numeric(df_raw['capacity'], error='coerce').fillna(0)
        df_raw['percentOutage'] = pd.to_numeric(df_raw['percentOutage'], errors['coerce']).fillna(0)

        
