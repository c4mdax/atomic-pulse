"""
API module for the Nuclear Outages Data Pipeline.
Provides endpoints to access, filter, and refresh nuclear grid outage data.
"""

import sqlite3
import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Security, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security.api_key import APIKeyHeader
from typing import List, Optional
from src.models import OutageRead, OutageSummary
from src.connector import EIAConnector
from src.db_builder import DatabaseBuilder
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Nuclear Outages API", description="Nuclear Outages Data Access Layer")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "nuclear_outages.db")

def get_db_connection():
    """
    Establishes a read-only connection to the local SQLite database.
    Returns:
        sqlite3.Connection: Database connection object configured to return dictionary-like rows.
    Raises:
        FileNotFoundError: If the SQLite database file does not exist at DB_PATH.
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found in: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row #access rows by name
    return conn

API_KEY_NAME="X-APi-Key"
API_KEY = os.getenv("APP_API_KEY", "vegeta>goku123") #fallback
api_key_header_obj = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header_obj)):
    """
    Validates the provided API key against the environment configuration.
    Args:
        api_key_header (str): The APi key passed in the HTTP request headers.
    Returns:
        str: The validated API key.
    Raises:
        HTTPException: If the API key is invalid or missing (403 Forbidden).
    """
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=403, detail="Access denied, invalid or not found API Key" 
    )

@app.get("/data", response_model=List[OutageRead])
def get_data(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status_id: Optional[int] = None,
    api_key: str = Depends(get_api_key)
):
    """
    Retrieves filtered and paginated nuclear outage records from the database.
    Args:
        limit (int): Maximum number of records to return.
        offset (int): Number of records to skip.
        start_date (Optional[str]): Lower bound for the date filter (YYYY-MM-DD).
        end_date (Optional[str]): Upper bound for the date filter (YYYY-MM-DD).
        status_id (Optional[int]): Filter by severity level (1: Nominal, 2: Warning, 3: Critical).
        api_key (str): Security dependency injection.
    Returns:
        List[dict]: A list of outage records matching the criteria.
    Raises:
        HTTPException: If a database operation fails (500 Internal Server Error).
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM fct_nuclear_outages WHERE 1=1"
            params = []

            if start_date:
                query += " AND date_key >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date_key <= ?"
                params.append(end_date)
            if status_id:
                query += " AND status_id = ?"
                params.append(status_id)

            query += " ORDER BY date_key DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            rows = cursor.execute(query, params).fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
@app.post("/refresh")
def refresh_data(api_key: str = Depends(get_api_key)):
    """
    Triggers the ETL pipeline to fetch new data from the EIA API and rebuilds the local database.
    Args:
        api_key (str): Security dependency injection.
    Returns:
        dict: A status report detailing the synchronization outcome and records processed.
    Raises:
        HTTPException: If the extraction, transformation, or loading process fails (500).
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
def get_summary(api_key: str = Depends(get_api_key)):
    """
    Provides aggregated metrics (total count, average, maximum) for the stored outsge data.
    Args:
        api_key (str): Security dependency injection.
    Returns:
        dict: Summary statistics including total_records, avg_outage_mw, and max_outage_mw.
    Raises:
        HTTPException: If a database operation fails (500).
    """
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


app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_frontend():
    """
    Serves the main frontend interface (index.html).
    Returns:
        HTMLResponse: The HTML content of the dashboard.
    """
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())
