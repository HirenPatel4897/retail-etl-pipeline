import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ── BigQuery config ──
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_KEY_PATH = os.getenv("GCP_KEY_PATH")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

def load_to_bigquery(df):
    """
    Loads cleaned product data into Google BigQuery.
    Uses staging table first, then swaps to production.
    This protects production data from bad loads.
    """
    print(f"[{datetime.now()}] Starting BigQuery load. Rows to load: {len(df)}")

    if df.empty:
        print(f"[{datetime.now()}] WARNING: Empty dataframe, skipping BigQuery load")
        return

    try:
        # connect using service account key
        client = bigquery.Client.from_service_account_json(
            GCP_KEY_PATH,
            project=GCP_PROJECT_ID
        )

        # ── Step 1: Create dataset if it doesn't exist ──
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
        dataset_ref.location = "US"
        try:
            client.create_dataset(dataset_ref, exists_ok=True)
            print(f"[{datetime.now()}] Dataset {BQ_DATASET} ready")
        except Exception as e:
            print(f"[{datetime.now()}] Dataset already exists: {e}")

        # ── Step 2: Load to staging table first ──
        staging_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}_staging"
        production_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,  # auto detect schema from dataframe
        )

        # convert datetime columns to string for BigQuery compatibility
        df_load = df.copy()
        for col in df_load.select_dtypes(include=["datetime64[ns]"]).columns:
            df_load[col] = df_load[col].astype(str)

        print(f"[{datetime.now()}] Loading to staging table...")
        job = client.load_table_from_dataframe(
            df_load, staging_table, job_config=job_config
        )
        job.result()  # wait for job to complete
        print(f"[{datetime.now()}] Staging load complete")

        # ── Step 3: Copy staging to production ──
        copy_query = f"""
            CREATE OR REPLACE TABLE `{production_table}` AS
            SELECT * FROM `{staging_table}`
        """
        client.query(copy_query).result()
        print(f"[{datetime.now()}] Production table updated successfully")

        # ── Step 4: Verify row count ──
        count_query = f"SELECT COUNT(*) as total FROM `{production_table}`"
        result = client.query(count_query).result()
        for row in result:
            print(f"[{datetime.now()}] Verified: {row.total} rows in production table")

    except Exception as e:
        print(f"[{datetime.now()}] ERROR loading to BigQuery: {e}")
        raise


def log_pipeline_run(status, rows_loaded, error=None):
    """
    Logs every pipeline run to a audit log table in BigQuery.
    This is your data lineage and observability layer.
    """
    try:
        client = bigquery.Client.from_service_account_json(
            GCP_KEY_PATH,
            project=GCP_PROJECT_ID
        )

        audit_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.pipeline_audit_log"

        log_entry = [{
            "run_timestamp": str(datetime.now()),
            "status": status,
            "rows_loaded": rows_loaded,
            "error_message": str(error) if error else None,
            "pipeline_version": "1.0"
        }]

        df_log = pd.DataFrame(log_entry)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        client.load_table_from_dataframe(df_log, audit_table, job_config=job_config).result()
        print(f"[{datetime.now()}] Audit log updated: {status}")

    except Exception as e:
        print(f"[{datetime.now()}] WARNING: Could not write audit log: {e}")
