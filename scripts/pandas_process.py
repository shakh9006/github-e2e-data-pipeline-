import sys
import logging
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine

sys.path.append('/opt/airflow/project_config')

from config.config import POSTGRES_DWH_CONN_ID

def process_repos_with_pandas(repos):
    logging.info(f"Processing {len(repos)} repositories with pandas")

    processed_repos = []

    for repo in repos:
        processed_repos.append(repo)

    df = pd.DataFrame(processed_repos)

    logging.info(f"DataFrame created with shape: {df.shape}")

    return df

def load_to_pg(parquet_data):
    df = pd.read_parquet(parquet_data)
    logging.info(f"Clearing repositories from {df.shape[0]} rows n/a")

    processed_repos = []
    for index, row in df.iterrows():
        if not row.get('language'):
            continue

        owner = row['owner'] if isinstance(row.get('owner'), dict) else {}
        license_info = row['license'] if isinstance(row.get('license'), dict) else {}

        processed_repo = {
            'repo_id': row.get('id'),
            'name': row.get('name'),
            'full_name': row.get('full_name'),
            'language': row.get('language'),
            'stargazers_count': row.get('stargazers_count', 0),
            'watchers_count': row.get('watchers_count', 0),
            'forks_count': row.get('forks_count', 0),
            'open_issues': row.get('open_issues_count', 0),
            'owner_login': owner.get('login'),
            'owner_type': owner.get('type'),
            'owner_id': owner.get('id'),
            'visibility': row.get('visibility'),
            'private': row.get('private'),
            'license_name': license_info.get('name') if not license_info.get('name') else 'Unknown',
            'created_at': pd.to_datetime(row.get('created_at')),
            'updated_at': pd.to_datetime(row.get('updated_at')),
            'pushed_at': pd.to_datetime(row.get('pushed_at')),
            'ingested_at': datetime.now(),
        }

        processed_repos.append(processed_repo)

    engine = create_engine(f"postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres")

    logging.info(f"Processing {len(processed_repos)} repositories with pandas")

    processed_df = pd.DataFrame(processed_repos)
    existing_data = pd.read_sql("SELECT repo_id FROM ods.github_repos", engine)
    
    if not processed_df.empty:
        processed_df.drop_duplicates(subset=['repo_id'], inplace=True)

    if not existing_data.empty:
        filtered_data = processed_df[~processed_df['repo_id'].isin(existing_data['repo_id'])]
        filtered_data.drop_duplicates(subset="repo_id", inplace=True)
    else:
        filtered_data = processed_df

    if not filtered_data.empty:
        filtered_data.to_sql(
            name="github_repos",
            con=engine,
            schema="ods",
            if_exists="append",
            index=False
        )

        logging.info(f"Loaded {df.shape[0]} rows to database")
    else:
        logging.info("No new records to load")


    return None
