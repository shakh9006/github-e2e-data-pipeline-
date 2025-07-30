import sys
import logging

sys.path.append('/opt/airflow/project_config')

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from project_config.config import DEFAULT_ARGS

with DAG(
    dag_id="init_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 7, 29),
    schedule="@daily",
    catchup=False,
) as dag:
    logging.info("Init DAG")
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end