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
    Handles data extraction from the US Energy Information Adminstration (EIA) API.
    Focuses on Nuclear Outgaes datasets to power the AtomicPulse pipeline.
    """

    def __init__(self):
        """
        Initializes the connector using the EIA_API_KEY from env variables.
        """
        self.api_key = os.getenv("EIA_API_KEY")
        self.base_url = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"

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
        Reads the existing parquet file to find the most recent extraction date.
        Returns a string 'YYYY-MM-DD' or None if full load is required.
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
        
    def fetch_nuclear_outages(self):
        """
        Queries the EIA API for the latest nuclear plant outage data.
        Returns:
           pd.DataFrame: Cleaned DataFrame containing outage events.
           or None if the request failed.
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
    
    def save_to_parquet(self, df, filename="data/us_nuclear_outages.parquet"):
        """
        Exports the DF to a compressed parquet file, using pyarrow.
        Args:
           df (pd.Dataframe): The data to be stored
           filename(str): Target path for the .parquet file.
        """
        if df is not None and not df.empty:
            try:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                df.to_parquet(filename, engine='pyarrow', compression='snappy', index=False)
                logger.info(f"Success: data exported to {filename}")
            except Exception as e:
                logger.error(f"Failed to save Parquet file: {e}")
        else:
            logger.warning("No valid data to save.")
                
if __name__ == "__main__":
    try:
        connector = EIAConnector()
        raw_df = connector.fetch_nuclear_outages()
        connector.save_to_parquet(raw_df)
    except Exception as e:
        logger.error(f"Process failed: {e}")
