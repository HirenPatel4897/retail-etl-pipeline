# Retail Price Intelligence ETL Pipeline

An end-to-end ETL pipeline that extracts retail product data from a public API, transforms and validates it, and loads it into Google BigQuery using a staging-to-production pattern.

## Architecture
```
Open Food Facts API → Extract → Transform → Load → BigQuery (Staging) → BigQuery (Production)
                                                          ↓
                                                   Audit Log Table
```

## Tech Stack

- **Python** — pipeline logic
- **Pandas** — data transformation and cleaning
- **Google BigQuery** — cloud data warehouse
- **Apache Airflow** — pipeline orchestration (scheduled runs)
- **Docker** — containerized deployment
- **Open Food Facts API** — free public retail product data

## Pipeline Stages

### Extract
- Pulls product data from the Open Food Facts API
- Handles pagination, rate limits, and API timeouts
- Returns raw pandas DataFrame

### Transform
- Removes duplicate records by product barcode
- Fills missing values for all key fields
- Standardizes text fields (strip, title case)
- Converts Unix timestamps to readable dates
- Adds pipeline metadata (extracted_at, pipeline_version)
- Flags data quality issues (missing_name, missing_nutriscore)

### Load
- Loads clean data to a **staging table** first
- Promotes staging to **production table** after verification
- Verifies row count after every load
- Writes every run to a **pipeline_audit_log** table for full observability

## Project Structure
```
retail-etl-pipeline/
│
├── etl/
│   ├── extract.py        # API extraction layer
│   ├── transform.py      # Data cleaning and validation
│   ├── load.py           # BigQuery loader with audit logging
│   └── pipeline.py       # Main pipeline runner
│
├── dags/
│   └── etl_dag.py        # Airflow DAG for scheduling
│
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variable template
└── README.md
```

## Setup

### Prerequisites
- Python 3.8+
- Google Cloud account with BigQuery API enabled
- GCP Service Account key (JSON) with BigQuery Admin role

### Installation

1. Clone the repo:
```bash
git clone https://github.com/HirenPatel4897/retail-etl-pipeline.git
cd retail-etl-pipeline
```

2. Create virtual environment:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create `.env` file:
```
GCP_PROJECT_ID=your-project-id
GCP_KEY_PATH=path/to/gcp-key.json
BQ_DATASET=retail_data
BQ_TABLE=product_prices
```

### Run the Pipeline
```bash
cd etl
python pipeline.py beverages
```

You can pass any food category:
```bash
python pipeline.py snacks
python pipeline.py dairy
```

## BigQuery Tables

| Table | Description |
|-------|-------------|
| `product_prices` | Production table — clean, verified data |
| `product_prices_staging` | Staging table — intermediate load target |
| `pipeline_audit_log` | Every pipeline run logged with status, rows, timestamp |

## Data Quality

Every record is flagged with one of:
- `ok` — passed all checks
- `missing_name` — product name was null
- `missing_nutriscore` — nutrition score was missing

## Key Engineering Decisions

- **Staging → Production pattern** — never write directly to production, protects data integrity
- **Audit logging** — every pipeline run recorded for full traceability and observability
- **Quality flags** — bad records are flagged not dropped, preserving raw data lineage
- **Graceful error handling** — pipeline logs failures and exits cleanly for Airflow retry logic