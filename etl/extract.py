import requests
import pandas as pd
from datetime import datetime

def extract_product_data(category="beverages", page_size=100):
    """
    Extracts product data from Open Food Facts API.
    No API key needed - completely free and public.
    """
    print(f"[{datetime.now()}] Starting extraction for category: {category}")
    
    url = "https://world.openfoodfacts.org/cgi/search.pl"
    
    params = {
        "action": "process",
        "tagtype_0": "categories",
        "tag_0": category,
        "page_size": page_size,
        "json": True,
        "fields": "code,product_name,categories,quantity,stores,countries,nutriscore_grade,last_modified_t"
    }

    try:
        print(f"[{datetime.now()}] Calling API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()  # raises error if status is 4xx or 5xx
        
        data = response.json()
        products = data.get("products", [])
        
        if not products:
            print(f"[{datetime.now()}] WARNING: No products returned from API")
            return pd.DataFrame()
        
        df = pd.DataFrame(products)
        print(f"[{datetime.now()}] Extracted {len(df)} products successfully")
        return df

    except requests.exceptions.Timeout:
        print(f"[{datetime.now()}] ERROR: API request timed out")
        raise

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ERROR: API request failed: {e}")
        raise