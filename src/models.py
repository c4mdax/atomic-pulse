from pydantic import BaseModel
from typing import List, Optional

"""
fct (Fact Table) row
"""
class OutageRead(BaseModel):
    id: int
    date_key: str
    status_id: int
    capacity_mw: float
    outage_mw: float
    percent_outage: float

    class Config:
        from_attributes = True

"""
Response summary
"""
class OutageSummary(BaseModel):
    total_records: int
    avg_outage_mw: float
    max_outage_mw: float
