import pytest
from fastapi.testclient import TestClient
from src.api import app

"""
Purpose: Follow TDD Standards
"""
cliente = TestClient(app)

def test_read_root():
    """
    Test if the root's response is ok
    """
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status" : "Nuclear Outages API is Online", "version: 1.0.0"}

def test_get_outages_format()

def test_get_summary()
