from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Jinnaphat',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'energy_supply_chain_silver',
    default_args=default_args,
    schedule=None, # Set to None because we want to run Bronze first
    catchup=False,
    tags=['energy', 'transformation', 'silver', 'pyspark'],
) as dag:

    # 1. Start Task
    start_task = EmptyOperator(task_id='start_silver_layer')

    # 2. Spark Transformation Tasks
    # Use BashOperator for run Python file that has SparkSession inside.
    
    task_clean_prices = BashOperator(
        task_id='clean_prices_silver',
        bash_command='python /opt/airflow/dags/scripts/spark/clean_prices.py',
    )

    task_clean_supply = BashOperator(
        task_id='clean_supply_silver',
        bash_command='python /opt/airflow/dags/scripts/spark/clean_supply.py',
    )

    task_clean_movements = BashOperator(
        task_id='clean_logistics_movements_silver',
        bash_command='python /opt/airflow/dags/scripts/spark/clean_logistics_movements.py',
    )

    trigger_gold_layer = TriggerDagRunOperator(
        task_id='trigger_gold_layer',
        trigger_dag_id='energy_supply_chain_gold',
        wait_for_completion=False
    )

    # 3. End Task
    end_task = EmptyOperator(task_id='end_silver_layer')

    start_task >> [task_clean_prices, task_clean_supply, task_clean_movements] >> trigger_gold_layer >> end_task