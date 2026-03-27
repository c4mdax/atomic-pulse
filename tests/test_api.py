import pytest
from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)

def test_read_data_endpoint():
    """Test the mandatory /data endpoint"""
    response = client.get("/data?limit=5")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_data_filters():
    """Test if filters in /data return valid structures"""
    response = client.get("/data?min_outage=1000")
    assert response.status_code == 200
    data = response.json()
    if len(data) > 0:
        assert data[0]["outage_mw"] >= 1000

def test_refresh_endpoint():
    """Test the mandatory /refresh endpoint"""
    response = client.post("/refresh")
    assert response.status_code == 200
    assert "status" in response.json()
    assert response.json()["status"] == "success"

def test_summary_bonus():
    """Test the summary endpoint used by GUI"""
    response = client.get("/summary")
    assert response.status_code == 200
    assert "total_records" in response.json()
