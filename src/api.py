import sqlite3
from fastapi import FastAPI, HTTPException
from typing import List
from src.models import OutageRead, OutageSummary

app = FastAPI(title="Nuclear Outages API", description="Nuclear Outages Data Access Layer")
DB_PATH = "data/nuclear_outages.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row #access rows by name
    return conn

@app.get("/")
def read_root():
    return {"status":"¡Nuclear Outages API is Online!", "version":"1.0.0"}

def get_all_outages(limit: int = 100):

def get_outage_summary():
    
