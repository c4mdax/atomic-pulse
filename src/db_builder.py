import os
import sqlite3
import logging
import pandas as pd
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseBuilder:
    def __init__ (self):
        self.parquet_path = "data/us_nuclear_outages.parquet"
        self.db_path = "data/nuclear_outages.db"

    def build_database(self):
        """
        Transforms the flat Parquet file into a Star Schema and saves it to SQLite
        """
        if not os.path.exists(self.parquet_path):
            logger.error(f"Source file not found: {self.parquet_path}. Run connector.py first")
            return

        logger.info("Reading raw Parquet data...")
        df_raw = pd.read_parquet(self.parquet_path)

        df_raw['period'] = pd.to_datetime(df_raw['period'])
        df_raw['outage'] = pd.to_numeric(df_raw['outage'], errors='coerce').fillna(0)
        df_raw['capacity'] = pd.to_numeric(df_raw['capacity'], errors='coerce').fillna(0)
        df_raw['percentOutage'] = pd.to_numeric(df_raw['percentOutage'], errors='coerce').fillna(0)

        """
        Building dim_stastus_thresholds
        """
        logger.info("Building dimension: dim_status_thresholds...")
        status_data = [
            {'status_id': 1, 'label': 'Normal', 'min_percent': 0.0, 'max_percent': 5.0},
            {'status_id': 2, 'label': 'Warning', 'min_percent': 5.0, 'max_percent': 15.0},
            {'status_id': 3, 'label': 'Critical', 'min_percent': 15.0, 'max_percent': 100.0}
        ]
        dim_status = pd.DataFrame(status_data)

        """
        Building dim_date
        """
        logger.info("Building dimension: dim_date...")
        unique_dates = df_raw['period'].drop_duplicates().sort_values().reset_index(drop=True)
        dim_date = pd.DataFrame({'date_key': unique_dates})

        # Attributes
        dim_date['day_name'] = dim_date['date_key'].dt.day_name()
        dim_date['date_key'] = dim_date['date_key'].dt.strftime('%Y-%m-%d')

        """
        Building fct_nuclear_outages
        """
        logger.info("Building fact table: fct_nuclear_outages...")
        fct_outages = df_raw.copy()
        fct_outages['date_key'] = fct_outages['period'].dt.strftime('%Y-%m-%d')

        def assign_status(pct):
            if pct < 5.0: return 1
            elif pct < 15.0 : return 2
            else: return 3

        fct_outages['status_id'] = fct_outages['percentOutage'].apply(assign_status)
        fct_outages = fct_outages[['date_key', 'status_id', 'capacity', 'outage', 'percentOutage']]
        fct_outages.rename(columns={'capacity': 'capacity_mw', 'outage': 'outage_mw', 'percentOutage': 'percent_outage'}, inplace=True)
        fct_outages.reset_index(names='id', inplace=True)


        """
        Saving in SQLite
        """
        # logger.info(f"Saving Star Schema to SQLite database at {self.db_path}...")
        # os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # # Context manager to close connection
        # with sqlite3.connect(self.db_path) as conn:
        # #if_exists='replace' 
        #     dim_status.to_sql('dim_status_thresholds', conn, if_exists='replace', index=False)
        #     dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
        #     fct_outages.to_sql('fct_nuclear_outages', conn, if_exists='replace', index=False)
        # logger.info("Database successfully built and ready for the API")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            #foreign keys on
            cursor.execute("PRAGMA foreign_keys = ON;")
            # delete old boards
            cursor.execute("DROP TABLE IF EXISTS fct_nuclear_outages")
            cursor.execute("DROP TABLE IF EXISTS dim_status_thresholds")
            cursor.execute("DROP TABLE IF EXISTS dim_date")
            # dim_status_thresholds
            cursor.execute("""
                CREATE TABLE dim_status_thresholds (
                    status_id INTEGER PRIMARY KEY,
                    label TEXT,
                    min_percent REAL,
                    max_percent REAL
                )
            """)

            cursor.execute("""
                CREATE TABLE dim_date (
                    date_key TEXT PRIMARY KEY,
                    day_name TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE fct_nuclear_outages (
                    id INTEGER PRIMARY KEY,
                    date_key TEXT,
                    status_id INTEGER,
                    capacity_mw REAL,
                    outage_mw REAL,
                    percent_outage REAL,
                    FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
                    FOREIGN KEY (status_id) REFERENCES dim_status_thresholds (status_id)
                )
            """)

            dim_status.to_sql('dim_status_thresholds', conn, if_exists='append', index=False)
            dim_date.to_sql('dim_date', conn, if_exists='append', index=False)
            fct_outages.to_sql('fct_nuclear_outages', conn, if_exists='append', index=False)


if __name__ == "__main__":
    try:
        builder = DatabaseBuilder()
        builder.build_database()
    except Exception as e:
        logger.error(f"Failed to build database: {e}")
        
