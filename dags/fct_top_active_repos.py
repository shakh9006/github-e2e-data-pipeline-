import sys

sys.path.append('/opt/airflow/project_config')

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

from config.config import DEFAULT_ARGS, POSTGRES_DWH_CONN_ID

DAG_ID = 'fct_top_active_repos'

TAGS = ['dm']

TABLE_NAME = 'github_repos'

DM_SCHEMA = 'dm'
STG_SCHEMA = 'stg'
ODS_SCHEMA = 'ods'

SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

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
    start = EmptyOperator(task_id='start')

    sensor_on_raw = ExternalTaskSensor(
        task_id='sensor_on_raw',
        external_dag_id='raw_from_s3_to_pg',
        allowed_states=['success'],
        mode='poke',
        timeout=360000,
        poke_interval=60,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id='drop_stg_table_before',
        conn_id=POSTGRES_DWH_CONN_ID,
        autocommit=True,
        sql=f"""
            DROP TABLE IF EXISTS {STG_SCHEMA}.tmp_fct_top_active_repos;
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id='create_stg_table',
        conn_id=POSTGRES_DWH_CONN_ID,
        autocommit=True,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {STG_SCHEMA}.tmp_fct_top_active_repos AS
        SELECT 
            repo_id,
            full_name,
            language,
            stargazers_count,
            forks_count,
            open_issues,
            stargazers_count + watchers_count + forks_count as activity_score
        FROM {ODS_SCHEMA}.{TABLE_NAME}
        ORDER BY activity_score DESC
        LIMIT 50;
        """
    )

    create_target_table = SQLExecuteQueryOperator(
        task_id='create_target_table',
        conn_id=POSTGRES_DWH_CONN_ID,
        autocommit=True,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {DM_SCHEMA}.fct_top_active_repos (
            repo_id BIGINT PRIMARY KEY,
            full_name VARCHAR(255),
            language VARCHAR(255),
            stargazers_count INT,
            forks_count INT,
            open_issues INT,
            activity_score INT
        );
        """
    )

    update_target_table = SQLExecuteQueryOperator(
        task_id='update_target_table',
        conn_id=POSTGRES_DWH_CONN_ID,
        autocommit=True,
        sql=f"""
        DELETE FROM {DM_SCHEMA}.fct_top_active_repos;
        INSERT INTO {DM_SCHEMA}.fct_top_active_repos
        SELECT 
            *
        FROM {STG_SCHEMA}.tmp_fct_top_active_repos;
        """
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id='drop_stg_table_after',
        conn_id=POSTGRES_DWH_CONN_ID,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS {STG_SCHEMA}.tmp_fct_top_active_repos;
        """
    )


    end = EmptyOperator(task_id='end')

    (
        start
        >> sensor_on_raw
        >> drop_stg_table_before
        >> create_stg_table
        >> create_target_table
        >> update_target_table
        >> drop_stg_table_after
        >> end
    )