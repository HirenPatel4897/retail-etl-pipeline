import pandas as pd
from datetime import datetime

def transform_product_data(df):
    """
    Cleans and standardizes raw product data from Open Food Facts API.
    Handles nulls, duplicates, and data type issues.
    """
    print(f"[{datetime.now()}] Starting transformation. Input rows: {len(df)}")

    if df.empty:
        print(f"[{datetime.now()}] WARNING: Empty dataframe received, skipping transformation")
        return df

    # ── Step 1: Keep only relevant columns ──
    columns_needed = [
        "code", "product_name", "categories",
        "quantity", "stores", "countries",
        "nutriscore_grade", "last_modified_t"
    ]
    # only keep columns that actually exist in the response
    columns_present = [col for col in columns_needed if col in df.columns]
    df = df[columns_present].copy()

    # ── Step 2: Remove duplicates ──
    before = len(df)
    df.drop_duplicates(subset=["code"], inplace=True)
    after = len(df)
    print(f"[{datetime.now()}] Removed {before - after} duplicate records")

    # ── Step 3: Handle nulls ──
    df["product_name"] = df["product_name"].fillna("Unknown Product")
    df["categories"] = df["categories"].fillna("Uncategorized")
    df["stores"] = df["stores"].fillna("Unknown Store")
    df["countries"] = df["countries"].fillna("Unknown Country")
    df["nutriscore_grade"] = df["nutriscore_grade"].fillna("not_rated")
    df["quantity"] = df["quantity"].fillna("Unknown")

    # ── Step 4: Standardize text fields ──
    df["product_name"] = df["product_name"].str.strip().str.title()
    df["nutriscore_grade"] = df["nutriscore_grade"].str.strip().str.lower()

    # ── Step 5: Convert timestamp to readable date ──
    if "last_modified_t" in df.columns:
        df["last_modified_t"] = pd.to_numeric(df["last_modified_t"], errors="coerce")
        df["last_modified_date"] = pd.to_datetime(
            df["last_modified_t"], unit="s", errors="coerce"
        )
        df.drop(columns=["last_modified_t"], inplace=True)

    # ── Step 6: Add pipeline metadata ──
    df["extracted_at"] = datetime.now()
    df["pipeline_version"] = "1.0"

    # ── Step 7: Flag data quality issues ──
    df["quality_flag"] = "ok"
    df.loc[df["product_name"] == "Unknown Product", "quality_flag"] = "missing_name"
    df.loc[df["nutriscore_grade"] == "not_rated", "quality_flag"] = "missing_nutriscore"

    print(f"[{datetime.now()}] Transformation complete. Output rows: {len(df)}")
    print(f"[{datetime.now()}] Quality flags: {df['quality_flag'].value_counts().to_dict()}")

    return df