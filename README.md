# AtomicPulse: US Nuclear Outages Data Pipeline

## Overview
An end-to-end data engineering pipeline that extracts daily nuclear power outage data from the EIA Open Data API, processes it into a dimensional model, and serves it via a RESTful API and a lightweight frontend interface.
___
## Architecture & Technical Decisions
**Extra analysis**: https://c4mdax.github.io/posts/atomic-pulse/
### 1. Data Connector
- **Network Resilience:** Implemented robust HTTP requests using `urllib3`'s `Retry` strategy to gracefully handle transient API errors (502, 503) and rate limits (429).
- **Incremental Extraction:** The connector reads the latest processed date from local storage and only fetches new records, significantly reducing bandwidth and compute time compared to full historical loads.
- **Storage:** Raw data is compressed into `.parquet` using `pyarrow` (columnar storage with Snappy compression) before being loaded into the database.

### 2. Data Model (Star Schema)
To optimize OLAP queries, the data is normalized into a Star Schema within SQLite.

**Entities & Relationships:**
- `dim_status_thresholds` (Dimension)
  - `status_id` (PK)
  - `label`, `min_percent`, `max_percent`
- `dim_date` (Dimension)
  - `date_key` (PK)
  - `day_name`
- `fct_nuclear_outages` (Fact)
  - `id` (PK)
  - `date_key` (FK) -> Refers to `dim_date(date_key)`
  - `status_id` (FK) -> Refers to `dim_status_thresholds(status_id)`
  - `capacity_mw`, `outage_mw`, `percent_outage`

**Cardinality:**
- `dim_date` (1) to (N) `fct_nuclear_outages`: A single date can have multiple outage records (if tracking multiple plants/regions in the future, currently aggregated nationally).
- `dim_status_thresholds` (1) to (N) `fct_nuclear_outages`: A single severity status applies to many daily outage records.

![AtomicPulse_ERDiagram](ER_Diagram.jpg)
### 3. REST API
- **Framework:** Built with **FastAPI** for native async support, strict data validation (Pydantic), and auto-generated OpenAPI documentation.
- **Security:** Endpoints are protected via an `APIKeyHeader` injected using dependency injection (`Depends`), ensuring business logic remains clean.
- **Performance:** Pagination (`LIMIT` and `OFFSET`) is handled entirely at the database level to prevent memory overloads on the server.

### 4. Frontend Interface
- **Tech Stack:** Vanilla JavaScript, HTML, and CSS. Zero external heavy dependencies (no React/Angular) for maximum speed and simplicity.
- **AI Assistance:** My first implementation of the interface was very basic, as my strength and main focus is not the frontend; however, with the help of an AI prompt, I managed to obtain and standardize a clean, modern, and descriptive interface.
___
## Setup Instructions

### 1. Prerequisites
- Python 3.9+
- Git

### 2. Installation (venv is desired)
```bash
git clone https://github.com/c4mdax/nuclear-data-pipeline.git
cd nuclear-data-pipeline
python -m venv venvNuclear
source venvNuclear/bin/activate
```

### 3. Environment Variables
Create a `.env` file in the root directory:
```bash
EIA_API_KEY=your_official_eia_api_key_here
APP_API_KEY=vegeta>goku123
```
### 4. Running Locally
**Start the Server**
```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000
```
Navigate to `http://localhost:8000/`
___
## Cloud Deployment
- **Live URL:**
___
## Deliverables Checklist
- [x] Connector script (`src/connector.py`)
- [x] ER diagram (`ER_Diagram.jpg`)
- [x] API service (`src/api.py`)
- [x] Web application interface (`static/index.html`)
- [x] Cloud deployment
___
