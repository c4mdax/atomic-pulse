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

def test_save_to_parquet_creates_file(tmp_path):
    """
    Test if the connector create the us_nuclear_outages.parquet file and the data/ folder
    """
    connector = EIAConnector()
    df_test = pd.DataFrame({
        "period": ["2026-03-24", "2026-03-23"],
        "capacity": [95000, 95000],
        "outage": [5000, 4800]
    })
    temp_dir = tmp_path / "data"
    temp_file = temp_dir / "test_output.parquet"
    
    connector.save_to_parquet(df_test, filename=str(temp_file))
    assert os.path.exists(temp_file)

    df_read = pd.read_parquet(temp_file)
    assert len(df_read) == 2
    assert list(df_read.columns) == ["period", "capacity", "outage"]
    assert df_read.iloc[0]["capacity"] == 95000
