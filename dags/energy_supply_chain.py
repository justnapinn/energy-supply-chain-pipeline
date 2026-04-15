from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

DAGs_FOLDER = os.path.dirname(os.path.realpath(__file__))
SCRIPTS_FOLDER = os.path.join(DAGs_FOLDER, 'scripts')

if SCRIPTS_FOLDER not in sys.path:
    sys.path.append(SCRIPTS_FOLDER)

import extract_prices
import extract_supply
import extract_logistics_movements

default_args = {
    'owner': 'Jinnaphat',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'energy_supply_chain_pipeline',
    default_args=default_args,
    schedule='@weekly',
    catchup=False,
    tags=['energy', 'ingestion', 'bronze'],
) as dag:

    # 1. Start Task
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # 2. Extraction Tasks (Bronze)
    task_extract_prices = PythonOperator(
        task_id='extract_prices_bronze',
        python_callable=extract_prices.extract_petroleum_prices,
    )

    task_extract_supply = PythonOperator(
        task_id='extract_supply_bronze',
        python_callable=extract_supply.extract_supply_estimates,
    )

    task_extract_movements = PythonOperator(
        task_id='extract_movements_bronze',
        python_callable=extract_logistics_movements.extract_logistics_movements,
    )

    # 3. End Task
    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> [task_extract_prices, task_extract_supply, task_extract_movements] >> end_task