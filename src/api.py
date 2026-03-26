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

@app.get("/outages", response_model=List[OutageRead])
def get_all_outages(limit: int = 100):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM fct_nuclear_outages LIMIT ?"
            rows = cursor.execute(query, (limit,)).fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/outages/summary", response_model=OutageSummary)
def get_outage_summary():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT 
                    COUNT(*) as total_records, 
                    AVG(outage_mw) as avg_outage, 
                    MAX(outage_mw) as max_outage 
                FROM fct_nuclear_outages
            """
            row = cursor.execute(query).fetchone()
            return {
                "total_records": row["total_records"],
                "avg_outage_mw": round(row["avg_outage"], 2),
                "max_outage_mw": row["max_outage"]
            }
    except Exceptioin as e:
        raise HTTPException(status_code=500, detail=str(e))
    
