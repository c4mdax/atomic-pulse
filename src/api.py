import sqlite3
import os
from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from src.models import OutageRead, OutageSummary
from src.connector import EIAConnector
from src.db_builder import DatabaseBuilder

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

@app.post("/refresh")
def refresh_data():
    """
    Extracts new data from EIA API, appends to local parquet storage 
    and triggers the DatabaseBuilder to update the SQLite Star Schema.
    """
    try:
        connector = EIAConnector()
        last_date = connector.get_latest_date()
        raw_df = connector.fetch_nuclear_outages(start_date=last_date)
        
        if raw_df is not None and not raw_df.empty:
            connector.save_to_parquet(raw_df)
            
            builder = DatabaseBuilder()
            builder.build_database()
            
            return {
                "status": "success", 
                "message": "Data synchronized successfully", 
                "records_processed": len(raw_df)
            }
        
        return {
            "status": "success", 
            "message": "No new data found. Database is up to date.", 
            "records_processed": 0
        }

    except Exception as e:
        print(f"PIPELINE ERROR: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {str(e)}")
    
@app.get("/summary", response_model=OutageSummary)
def get_summary():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            row = cursor.execute("SELECT COUNT(*) as total, AVG(outage_mw) as avg, MAX(outage_mw) as max FROM fct_nuclear_outages").fetchone()
            return {
                "total_records": row["total"],
                "avg_outage_mw": round(row["avg"], 2) if row["avg"] else 0.0,
                "max_outage_mw": row["max"] if row["max"] else 0.0
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
