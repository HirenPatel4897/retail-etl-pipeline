# Retail Price Intelligence ETL Pipeline

An end-to-end ETL pipeline that extracts retail product data from a public API daily, transforms and validates it using Pandas, and loads it into Google BigQuery using a staging-to-production pattern. Fully orchestrated with Apache Airflow and containerized with Docker.

## Architecture

```
Open Food Facts API
        │
        ▼
   extract.py          ← pulls 100 products per run, handles errors + timeouts
        │
        ▼
  transform.py         ← removes duplicates, fills nulls, flags data quality issues
        │
        ▼
    load.py            ← loads to BigQuery staging → promotes to production
        │
        ▼
BigQuery (retail_data)
  ├── product_prices         ← production table (clean, verified data)
  ├── product_prices_staging ← staging table (intermediate load target)
  └── pipeline_audit_log     ← every run logged with status, rows, timestamp
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Pipeline logic |
| Pandas | Data transformation and cleaning |
| Google BigQuery | Cloud data warehouse |
| Apache Airflow | Pipeline orchestration and scheduling |
| Docker + Docker Compose | Containerized deployment |
| PostgreSQL | Airflow metadata database |
| Open Food Facts API | Free public retail product data (no API key needed) |

## Pipeline Stages

### Extract (`etl/extract.py`)
- Pulls product data from the Open Food Facts API by category (beverages, snacks, dairy, etc.)
- Handles pagination, rate limits, and API timeouts (30s timeout)
- Raises clean errors on 4xx/5xx responses using `raise_for_status()`
- Returns a raw pandas DataFrame

### Transform (`etl/transform.py`)
- Keeps only relevant columns, drops junk fields
- Removes duplicate records by product barcode (`code`)
- Fills missing values for all key fields (no silent NULLs)
- Standardizes text fields — strip whitespace, title case
- Converts Unix timestamps to readable datetime
- Adds pipeline metadata: `extracted_at`, `pipeline_version`
- Flags data quality issues per record:
  - `ok` — passed all checks
  - `missing_name` — product name was null
  - `missing_nutriscore` — nutrition score was missing

### Load (`etl/load.py`)
- Creates BigQuery dataset automatically if it doesn't exist
- Loads clean data to **staging table** first (`product_prices_staging`)
- Promotes staging to **production table** after successful load
- Verifies row count in production after every run
- Writes every pipeline run to `pipeline_audit_log` table with:
  - `run_timestamp`
  - `status` (SUCCESS / FAILED)
  - `rows_loaded`
  - `error_message`
  - `pipeline_version`

### Orchestration (`dags/etl_dag.py`)
- Apache Airflow DAG with `@daily` schedule
- 3 isolated tasks: `extract → transform → load`
- Data passed between tasks using Airflow XCom
- Retry logic: 3 retries with 5-minute delay between attempts
- `catchup=False` — no backfilling missed runs on startup

## Project Structure

```
retail-etl-pipeline/
│
├── etl/
│   ├── extract.py        # API extraction layer
│   ├── transform.py      # Data cleaning and quality flagging
│   ├── load.py           # BigQuery loader with staging pattern + audit log
│   └── pipeline.py       # Standalone pipeline runner (for testing)
│
├── dags/
│   └── etl_dag.py        # Airflow DAG — scheduling, retries, task dependencies
│
├── docker-compose.yml    # Runs Airflow webserver + scheduler + PostgreSQL
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variable template (never commit .env)
└── README.md
```

## Setup & Installation

### Prerequisites
- Python 3.8+
- Docker Desktop
- Google Cloud account with BigQuery API enabled
- GCP Service Account key (JSON) with BigQuery Admin role

### 1. Clone the repo
```bash
git clone https://github.com/HirenPatel4897/retail-etl-pipeline.git
cd retail-etl-pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
Create a `.env` file in the root folder:
```
GCP_PROJECT_ID=your-project-id
GCP_KEY_PATH=C:\full\path\to\gcp-key.json
BQ_DATASET=retail_data
BQ_TABLE=product_prices
```

### 5. Run pipeline standalone (test without Airflow)
```bash
cd etl
python pipeline.py beverages
```

### 6. Start Airflow with Docker
```bash
# Initialize Airflow database (first time only)
docker-compose up airflow-init

# Start Airflow webserver + scheduler
docker-compose up airflow-webserver airflow-scheduler -d
```

### 7. Access Airflow UI
Open `http://localhost:8080` in your browser.
- Username: `admin`
- Password: `admin`

Unpause the `retail_etl_pipeline` DAG and trigger it manually to test.

## BigQuery Output

After a successful run you will see three tables in the `retail_data` dataset:

| Table | Description |
|-------|-------------|
| `product_prices` | Production table — 100 clean, verified product records |
| `product_prices_staging` | Staging table — intermediate before production swap |
| `pipeline_audit_log` | Audit log — every run recorded with status and row count |

## Key Engineering Decisions

**Staging → Production pattern** — never write directly to production. Load to staging first, verify row count, then swap. Protects production data from bad or partial loads.

**Audit logging** — every pipeline run is written to `pipeline_audit_log` with timestamp, status, rows loaded, and error messages. Provides full observability into pipeline health over time.

**Data quality flags** — bad records are flagged with a `quality_flag` column instead of being silently dropped. This preserves raw data lineage while making data issues visible downstream.

**Graceful error handling** — every stage wraps errors in try/except, logs them, and exits with code 1 so Airflow knows to retry rather than marking the run as silently complete.

**XCom for task isolation** — each Airflow task is fully isolated. Data is serialized to JSON and passed via XCom, meaning a failure in one task doesn't corrupt data in another.

**Environment variables for all credentials** — no hardcoded passwords or API keys anywhere in the codebase. All sensitive config lives in `.env` which is gitignored.
