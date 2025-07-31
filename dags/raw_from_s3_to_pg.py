import sys
import logging

sys.path.append('/opt/airflow/project_config')

from scripts.minio_process import read_from_minio
from scripts.get_dates import get_dates
from scripts.pandas_process import load_to_pg

from datetime import datetime
from airflow import DAG
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from config.config import DEFAULT_ARGS, POSTGRES_DWH_CONN_ID

DAG_ID = 'raw_from_s3_to_pg'
TAGS = ['s3', 'pg']

SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

def fetch_daily_repos_from_s3_to_pg(**context):
    try:
        start_date, end_date = get_dates(**context)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        logging.info(f'Processing date: {start_date_str} to {end_date_str}')
        logging.info('fetch_daily_repos_from_s3_to_pg')

        file_name = f"github_repos_{start_date_str}_{end_date_str}_00-00-00.gz.parquet"
        object_path = f"raw/github_repos/{file_name}"

        logging.info(f"Looking for file: {object_path}")

        parquet_data = read_from_minio(object_path)
        load_to_pg(parquet_data)

        logging.info(f"Successfully fetched from s3")
    except Exception as e:
        logging.error(f"Error occurred while fetching from s3: {e}")
        return None

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=SHORT_DESCRIPTION,
    tags=TAGS,
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 12, 31),
    catchup=True,
    schedule='0 12 * * *',
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION
    
    start = EmptyOperator(task_id='start')

    sensor_on_raw = ExternalTaskSensor(
        task_id='sensor_on_raw',
        external_dag_id='raw_from_api_to_s3',
        allowed_states=['success'],
        mode='poke',
        timeout=360000,
        poke_interval=60,
    )

    create_database = PostgresOperator(
        task_id='database_init',
        postgres_conn_id=POSTGRES_DWH_CONN_ID,
        sql="""
            CREATE SCHEMA IF NOT EXISTS stg;
            CREATE SCHEMA IF NOT EXISTS ods;
            CREATE SCHEMA IF NOT EXISTS dm;
            
            CREATE TABLE IF NOT EXISTS ods.github_repos (
                repo_id BIGINT PRIMARY KEY,
                name TEXT,
                full_name TEXT,
                language TEXT,
                stargazers_count INT,
                watchers_count INT,
                forks_count INT,
                open_issues INT,
                owner_login TEXT,
                owner_type TEXT,
                owner_id BIGINT,
                visibility TEXT,
                private BOOLEAN,
                license_name TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                pushed_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
        """
    )
    
    fetch_daily_repos_from_s3_to_pg = PythonOperator(
        task_id='fetch_daily_repos_from_s3_to_pg',
        python_callable=fetch_daily_repos_from_s3_to_pg,
    )

    end = EmptyOperator(task_id='end')

    (
        start
        >> sensor_on_raw
        >> create_database
        >> fetch_daily_repos_from_s3_to_pg
        >> end
    )