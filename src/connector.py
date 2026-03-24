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
        self.base_url = "https://api.eia.gov/v2/nuclear/outages/data/"

        if not self.api_key:
            raise ValueError("Missing API Key. Please check the .env file.")

