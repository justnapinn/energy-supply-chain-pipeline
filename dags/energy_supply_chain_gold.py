import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

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
    tags=['energy', 'analytics', 'gold', 'pyspark', 'postgres'],
) as dag:

    # 1. Start Task
    start_task = EmptyOperator(task_id='start_gold_layer')

    # 2. Build Star Schema (Spark Transformation)
    task_build_analytics = BashOperator(
        task_id='build_diesel_analytics',
        bash_command='python /opt/airflow/dags/scripts/spark/gold_analytics.py',
    )

    # 3. Load to PostgreSQL (Staging and Upsert)
    # We securely inject database credentials into the environment variables 
    # using Airflow's Jinja templating. This prevents the PySpark script from 
    # needing to import Airflow libraries, avoiding boundary and dependency issues.
    task_load_to_postgres = BashOperator(
        task_id='load_gold_to_postgres',
        bash_command='python /opt/airflow/dags/scripts/spark/gold_to_postgres.py',
        env={
            **os.environ,  # Inherit existing environment variables required for Java/Spark
            'DB_HOST': '{{ conn.my_postgres_conn.host }}',
            'DB_PORT': '{{ conn.my_postgres_conn.port }}',
            'DB_USER': '{{ conn.my_postgres_conn.login }}',
            'DB_PASS': '{{ conn.my_postgres_conn.password }}',
            'DB_NAME': '{{ conn.my_postgres_conn.schema | default("airflow", true) }}'
        }
    )

    # 4. End Task
    end_task = EmptyOperator(task_id='end_gold_layer')

    # Define Task Dependencies
    start_task >> task_build_analytics >> task_load_to_postgres >> end_task