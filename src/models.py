from pydantic import BaseModel, ConfigDict
from typing import List, Optional

"""
fct (Fact Table) row
"""
class OutageRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    date_key: str
    status_id: int
    capacity_mw: float
    outage_mw: float
    percent_outage: float

"""
Response summary
"""
class OutageSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    total_records: int
    avg_outage_mw: float
    max_outage_mw: float
