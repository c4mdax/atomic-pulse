import sqlite3
from fastapi import FastAPI, HTTPException
from typing import List
from src.models import OutageRead, OutageSummary

app = FastAPI(title="Nuclear Outages API", description="Nuclear Outages Data Access Layer")
DB_PATH = "data/nuclear_outages.db"

def get_db_connection():

def read_root():

def get_all_outages(limit: int = 100):

def get_outage_summary():
    
