import os
import requests
import pandas as pd
from dotenv import load_dotenv

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
            raise ValueError("Missing API Key. Please check the .env file.")

    def fetch_nuclear_outages(self):
        """
        Queries the EIA API for the latest nuclear plant outage data.
        Returns:
           pd.DataFrame: Cleaned DataFrame containing outage events.
           or None if the request failed.
        """
        params = {
            "api_key":self.api_key,
            "frequency" : "daily",
            "data[0]":"capacity",
            "data[1]":"outage",
            "data[2]":"percentOutage",
            "sort[0][column]":"period",
            "sort[0][direction]":"desc",
            "offset":0,
            "length":5000
        }

        try:
            print("Connecting to EIA Open Data API...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            json_data = response.json()
            raw_data = json_data['response']['data']
            return pd.DataFrame(raw_data)

        except Exception as e:
            print(f"Connection error: {e}")
            return None

    def save_to_parquet(self, df, filename="../data/us_nuclear_outages.parquet"):
        """
        Exports the DF to a compressed parquet file.
        Args:
           df (pd.Dataframe): The data to be stored
           filename(str): Target path for the .parquet file.
        """
        if df is not None and not df.empty:
            try:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                df.to_parquet(filename, engine='pyarrow', compression='snappy', index=False)
                print(f"Success: Dataset saved at {filename}")
                print(f"Total records: {len(df)}")
            except Exception as e:
                print(f"Error saving Parquet: {e}")
        else:
            print("No data available to save.")
                

if __name__ == "__main__":
    connector = EIAConnector()
    outages_df = connector.fetch_nuclear_outages()
    connector.save_to_parquet(outages_df)
    if outages_df is not None:
        print(f"Retrieved {len(outages_df)} records.")
    else:
        print("Failed to retrieve data. Check the API key connection.")
