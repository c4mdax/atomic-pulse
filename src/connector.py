"""
Data connector module for the US Energy Information Administration (EIA) API.
Handles data extraction, pagination, retries, and incremental loading for nuclear outages.
"""

import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class EIAConnector:
    """
    Client for extracting nuclear outage datasets from the EIA API.
    """

    def __init__(self):
        """
        Initializes the EIA connector.
        Configures the API key from environment variables, sets up the endpoint URL,
        and establishes an HTTP session with robust retry policies for transient errors.
        raises:
            ValueError: If the EIA_API_KEY environment variable is not set.
        """
        self.api_key = os.getenv("EIA_API_KEY")
        self.base_url = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"
        self.file_path = "data/us_nuclear_outages.parquet"
        if not self.api_key:
            logger.error("Critical: EIA_API_KEY not found in .env file.")
            raise ValueError("Missing API Key. Please check the .env file.")

        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

        
    def get_latest_date(self):
        """
        Retrieves the most recent extraction date from the local parquet storage.
        Used to determine the starting point for incremental data loads.
        Returns:
            str | None: The next date to fetch in 'YYYY-MM-DD' format,  or None if no existing data is found.
        """
        if os.path.exists(self.file_path):
            try:
                df_existing = pd.read_parquet(self.file_path, columns=["period"])
                if not df_existing.empty:
                    max_date = pd.to_datetime(df_existing['period']).max()
                    next_day = max_date + pd.Timedelta(days=1)
                    start_date_str = next_day.strftime('%Y-%m-%d')
                    logger.info(f"Existing data found. Fetching incrementally starting from: {start_date_str}")
                    return start_date_str
            except Exception as e:
                logger.warning(f"Could not read existing Parquet file. Defaulting to full load. Error: {e}")
        logger.info("No existing data found. Performing full historical extraction")
        return None
        
    def fetch_nuclear_outages(self, start_date=None):
        """
        Fetches nuclear outage data from the EIA API.
        Handles pagination automatically and applies optional date filters for incremental updates.
        Validates the response structure against required schema fields.
        Args:
            start_date (str, optional): The starting date for data extraction ('YYYY-MM-DD'). Defaults to None.
        Returns:
            pd.DataFrame | None: A DataFrame containing the extracted data, or None if extraction/validation fails.
        Raises:
            ValueError: If a fatal API authentication or connection error occurs on the first request.
        """
        all_data = []
        offset = 0
        limit = 5000
        required_fields = ["period", "outage", "capacity"]

        logger.info("Starting data extraction from EIA...")
        while True:    
            params = {
                "api_key":self.api_key,
                "frequency" : "daily",
                "data[0]":"capacity",
                "data[1]":"outage",
                "data[2]":"percentOutage",
                "sort[0][column]":"period",
                "sort[0][direction]":"desc",
                "offset":offset,
                "length":limit
            }

            if start_date:
                params["start"] = start_date

            try:
                response = self.session.get(self.base_url, params=params, timeout=20)
                response.raise_for_status()
                data_batch = response.json().get('response', {}).get('data', [])
                if not data_batch:
                    break
                all_data.extend(data_batch)
                logger.info(f"Fetched {len(all_data)} records so far (offset: {offset})...")

                if len(data_batch) < limit:
                    break

                offset += limit

            except Exception as e:
                logger.error(f"Graceful shutdown: Error during fetch at offset {offset}: {e}")
                if offset == 0:
                    raise ValueError(f"EIA API Authentication or Connection Error. Check your EIA API key or authentication. \n Error: {e}")
                break
        if not all_data:
            logger.info("No new data from the API. System is up to date.")
            return pd.DataFrame()

        df = pd.DataFrame(all_data)

        if not df.empty:
            missing = [field for field in required_fields if field not in df.columns]
            if missing:
                logger.warning(f"Validation failed: missing fields {missing}")
                return None
            return df
        return None
    
    def save_to_parquet(self, df_new):
        """
        Saves the extracted DataFrame to a compressed Parquet file.
        If the file already exists, it appends the new data, drops duplicates based on the 'period',
        and sorts the dataset in descending order before overwriting
        Args
            df_new (pd.DataFrame): The new dataset to be stored.
        """
        if df_new is None or df_new.empty:
            logger.info("No new data to save.")
            return
        try:
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            if os.path.exists(self.file_path):
                df_existing = pd.read_parquet(self.file_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)

                df_combined.drop_duplicates(subset=['period'], keep='last', inplace=True)
                df_combined.sort_values(by='period', ascending=False, inplace=True)

                df_combined.to_parquet(self.file_path, engine='pyarrow', compression='snappy', index=False)
                logger.info(f"Success: Appended {len(df_new)} new records.  Total records: {len(df_combined)}")
            else:
                df_new.to_parquet(self.file_path, engine='pyarrow', compression='snappy', index=False)
                logger.info(f"Success: Initial data exported to {self.file_path} ({len(df_new)} records).")
        except Exception as e:
            logger.error(f"Failed to save Parquet file: {e}")

if __name__ == "__main__":
    try:
        connector = EIAConnector()
        last_date = connector.get_latest_date()
        raw_df = connector.fetch_nuclear_outages(start_date = last_date)
        connector.save_to_parquet(raw_df)
    except Exception as e:
        logger.error(f"Process failed: {e}")
