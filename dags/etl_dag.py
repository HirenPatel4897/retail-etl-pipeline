from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys

# add etl folder to path so Airflow can find our modules
sys.path.insert(0, '/opt/airflow/etl')

from extract import extract_product_data
from transform import transform_product_data
from load import load_to_bigquery, log_pipeline_run

# ── Default args — applied to every task ──
default_args = {
    'owner': 'hirenkumar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,                              # retry 3 times on failure
    'retry_delay': timedelta(minutes=5),       # wait 5 mins between retries
}

# ── DAG definition ──
dag = DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for retail product pricing data',
    schedule_interval='@daily',                # runs every day at midnight
    start_date=days_ago(1),
    catchup=False,                             # don't backfill missed runs
    tags=['retail', 'etl', 'bigquery'],
)

# ── Task functions ──

def extract_task(**context):
    """Extract product data from API and push to XCom"""
    df = extract_product_data(category="beverages")
    # store as JSON to pass between tasks
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"Extracted {len(df)} rows")

def transform_task(**context):
    """Pull raw data from XCom, transform, push clean data"""
    import pandas as pd
    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df_raw = pd.read_json(raw_json)
    df_clean = transform_product_data(df_raw)
    context['ti'].xcom_push(key='clean_data', value=df_clean.to_json())
    print(f"Transformed to {len(df_clean)} rows")

def load_task(**context):
    """Pull clean data from XCom and load to BigQuery"""
    import pandas as pd
    clean_json = context['ti'].xcom_pull(key='clean_data', task_ids='transform')
    df_clean = pd.read_json(clean_json)
    load_to_bigquery(df_clean)
    log_pipeline_run(status="SUCCESS", rows_loaded=len(df_clean))
    print(f"Loaded {len(df_clean)} rows to BigQuery")

# ── Task definitions ──
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# ── Task order — this defines the pipeline flow ──
extract >> transform >> load