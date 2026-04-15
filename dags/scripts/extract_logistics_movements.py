import requests
import json
import os
from datetime import datetime

def extract_logistics_movements():
    API_KEY = os.getenv("EIA_API_KEY")
    if not API_KEY:
        raise ValueError("EIA_API_KEY is missing!")

    url = "https://api.eia.gov/v2/petroleum/move/neti/data/"
    
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 50 
    }

    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"Error Detail: {response.json()}")
        response.raise_for_status()
    
    data = response.json()
    
    file_name = f"movements_{datetime.now().strftime('%Y%m%d')}.json"
    save_path = os.path.join("/opt/airflow/datalake/bronze", file_name)
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    with open(save_path, 'w') as f:
        json.dump(data, f)
        
    print(f"Movements data successfully saved to {save_path}")