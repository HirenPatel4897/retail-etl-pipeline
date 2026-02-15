import sys
import os
from datetime import datetime
from extract import extract_product_data
from transform import transform_product_data
from load import load_to_bigquery, log_pipeline_run

def run_pipeline(category="beverages"):
    """
    Main pipeline runner — ties extract, transform, load together.
    This is what Airflow will call on a schedule.
    """
    print(f"\n{'='*50}")
    print(f"PIPELINE START: {datetime.now()}")
    print(f"Category: {category}")
    print(f"{'='*50}\n")

    rows_loaded = 0
    
    try:
        # ── Step 1: Extract ──
        print(">> EXTRACT")
        df_raw = extract_product_data(category=category)
        print(f"   Raw rows extracted: {len(df_raw)}\n")

        # ── Step 2: Transform ──
        print(">> TRANSFORM")
        df_clean = transform_product_data(df_raw)
        print(f"   Clean rows after transform: {len(df_clean)}\n")

        # ── Step 3: Load ──
        print(">> LOAD")
        load_to_bigquery(df_clean)
        rows_loaded = len(df_clean)

        # ── Step 4: Log success ──
        log_pipeline_run(
            status="SUCCESS",
            rows_loaded=rows_loaded
        )

        print(f"\n{'='*50}")
        print(f"PIPELINE COMPLETE: {datetime.now()}")
        print(f"Total rows loaded: {rows_loaded}")
        print(f"{'='*50}\n")

    except Exception as e:
        # ── Log failure ──
        print(f"\n[{datetime.now()}] PIPELINE FAILED: {e}")
        log_pipeline_run(
            status="FAILED",
            rows_loaded=rows_loaded,
            error=e
        )
        sys.exit(1)


if __name__ == "__main__":
    # allows running from command line:
    # python pipeline.py beverages
    # python pipeline.py snacks
    category = sys.argv[1] if len(sys.argv) > 1 else "beverages"
    run_pipeline(category=category)