import requests
import json
import os
from datetime import datetime

def extract_supply_estimates():
    API_KEY = os.getenv("EIA_API_KEY")
    if not API_KEY:
        raise ValueError("EIA_API_KEY is missing!")

    url = "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
    params = {
        "api_key": API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 100 # extract 100 weeks of data then filter in silver layer
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    file_name = f"supply_{datetime.now().strftime('%Y%m%d')}.json"
    save_path = os.path.join("/opt/airflow/datalake/bronze", file_name)
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    with open(save_path, 'w') as f:
        json.dump(response.json(), f)
        
    print(f"Supply data saved to {save_path}")