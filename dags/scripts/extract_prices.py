import requests
import json
import os
from datetime import datetime

def extract_petroleum_prices():
    API_KEY = os.getenv("EIA_API_KEY")
    if not API_KEY:
        raise ValueError("EIA_API_KEY is missing!")

    url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
    params = {
        "api_key": API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": "EMD_EPD2D_PTE_NUS_DPG", # U.S. No 2 Diesel Retail Prices
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 52 # extract 1 year data (52 weeks)
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    file_name = f"prices_{datetime.now().strftime('%Y%m%d')}.json"
    save_path = os.path.join("/opt/airflow/datalake/bronze", file_name)
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    with open(save_path, 'w') as f:
        json.dump(response.json(), f)
        
    print(f"Prices data saved to {save_path}")