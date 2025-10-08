import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Recommended: set a static start date (no "today"-like functions)
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="first_dag_stub",
    start_date=START_DATE,
    schedule=None,            # replaces schedule_interval=None
    catchup=False,
    max_active_tasks=1,       # replaces old DAG-level 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["example"],
) as dag:
    
    trigger = EmptyOperator(task_id=  "trigger")

    branch = EmptyOperator(task_id= "branch")

    trigger >> branch
