import pytest
from fastapi.testclient import TestClient
from src.api import app

"""
Purpose: Follow TDD Standards
"""
client = TestClient(app)

def test_read_root():
    """
    Test if the root's response is ok
    """
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status" : "¡Nuclear Outages API is Online!", "version": "1.0.0"}

def test_get_outages_format():
    """
    Test if the outages endpoint returns a list with correct parameters
    """
    response = client.get("/outages?limit=1")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if len(data) > 0:
        record = data[0]
        expected_keys = {"id", "date_key", "status_id", "capacity_mw", "outage_mw", "percent_outage"}
        assert expected_keys.issubset(record.keys())

def test_get_summary():
    """
    Test the analytic endpoint
    """
    response = client.get("/outages/summary")
    assert response.status_code == 200
    data = response.json()
    assert "total_records" in data
    assert "avg_outage_mw" in data
