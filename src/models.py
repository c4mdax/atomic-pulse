"""
Pydantic data models for the Nuclear Outages API.
Defines the schemas for data validation and API response serialization.
"""

from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class OutageRead(BaseModel):
    """
    Schema representing a single record from the fct_nuclear_outages fact table.
    Attributes:
        id(int): unique identifier for the outage record.
        date_key (str): Date of the outage in 'YYYY-MM-DD' format.
        status_id (int): Foreign key linking to the severity threshold (1, 2, or 3)
        capacity_mw (float): Total national nuclear capacity in megawatts on the given date.
        outage_mw (float): Total capacity lost due to outages in megawatts.
        percent_outage (float): Percentage of the national capacity currently offline
    """
    model_config = ConfigDict(from_attributes=True)
    id: int
    date_key: str
    status_id: int
    capacity_mw: float
    outage_mw: float
    percent_outage: float

class OutageSummary(BaseModel):
    """
    Schema for the aggregated metrics of historical outage data.
    Attributes:
        total_records (int): Total number of outage days stored in the database.
        avg_outage_mw (float): Historical average of megawatts lost per day
        max_outage_mw (float): The highest recorded single-day megawatt loss
    """
    model_config = ConfigDict(from_attributes=True)
    
    total_records: int
    avg_outage_mw: float
    max_outage_mw: float
