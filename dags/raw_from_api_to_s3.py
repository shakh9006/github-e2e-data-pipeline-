import sys
import logging

sys.path.append('/opt/airflow/project_config')

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.fetch_github_api import fetch_daily_repos_to_minio
from scripts.get_dates import get_dates
from config.config import DEFAULT_ARGS

DAG_ID = 'raw_from_api_to_s3'

TAGS = ['api', 's3']

SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

def fetch_github_repos_api_wrapper(**context):
    start_date, end_date = get_dates(**context)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    logging.info(f'Processing date: {start_date_str} to {end_date_str}')
    
    result = fetch_daily_repos_to_minio(start_date_str, end_date_str)
    if result:
        logging.info(f"Successfully processed {result['total_repos']} repositories")
        logging.info(f"File saved to: {result['file_path']}")
    else:
        logging.error("Failed to process repositories")
    
    return result

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=SHORT_DESCRIPTION,
    tags=TAGS,
    schedule="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 12, 31),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION
    
    start = EmptyOperator(task_id='start')

    fetch_github_repos_api = PythonOperator(
        task_id='fetch_github_repos_api',
        python_callable=fetch_github_repos_api_wrapper
    )

    end = EmptyOperator(task_id='end')

    (
        start
        >> fetch_github_repos_api
        >> end
    )