from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Jinnaphat',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'energy_supply_chain_gold',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['energy', 'analytics', 'gold', 'pyspark'],
) as dag:

    start_task = EmptyOperator(task_id='start_gold_layer')

    build_analytics = BashOperator(
        task_id='build_diesel_analytics',
        bash_command='python /opt/airflow/dags/scripts/spark/gold_analytics.py',
    )

    load_to_postgres = BashOperator(
        task_id='load_gold_to_postgres',
        bash_command='python /opt/airflow/dags/scripts/spark/gold_to_postgres.py',
    )

    end_task = EmptyOperator(task_id='end_gold_layer')

    start_task >> build_analytics >> load_to_postgres >> end_task