import sqlite3
from fastapi import FastAPI, HTTPException
from typing import List
from src.models import OutageRead, OutageSummary
import os

app = FastAPI(title="Nuclear Outages API", description="Nuclear Outages Data Access Layer")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "nuclear_outages.db")

def get_db_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found in: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row #access rows by name
    return conn


@app.get("/data", response_model=List[OutageRead])
def get_data(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    date: Optional[str] = None,
    min_outage: Optional[float] = None
):
    """Returns filtered nuclear outage data with pagination support."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM fct_nuclear_outages WHERE 1=1"
            params = []

            if date:
                query += " AND date_key = ?"
                params.append(date)
            if min_outage:
                query += " AND outage_mw >= ?"
                params.append(min_outage)

            query += " ORDER BY date_key DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            rows = cursor.execute(query, params).fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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
            agv_val = row["avg_outage"] if row["avg_outage"] is not None else 0.0
            max_val = row["max_outage"] if row["max_outage"] is not None else 0.0
            
            if not row or row["total_records"] == 0:
                return {"total_records":0, "avg_outage_mw": 0.0, "max_outage_mw":0.0}
            
            return {
                "total_records": row["total_records"],
                "avg_outage_mw": round(row["avg_outage"], 2),
                "max_outage_mw": row["max_outage"]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
