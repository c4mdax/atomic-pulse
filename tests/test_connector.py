import pytest
import pandas as pd
from src.connector import EIAConnector

"""
Purpose: Follow TDD Standards
"""
def test_connector_initialization_error():
    """
    Test if the connector  fails if there's no key
    """
    import os
    os.environ.pop("EIA_API_KEY", None)
    with pytest.raises(ValueError):
        EIAConnector()
