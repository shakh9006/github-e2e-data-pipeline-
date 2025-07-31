import sys
import logging
import requests
import time

sys.path.append("/opt/airflow/project_config")

from scripts.minio_process import save_to_minio
from scripts.pandas_process import process_repos_with_pandas

from config.config import (
    GITHUB_REPOS_URL,
    GITHUB_API_HEADERS,
    GITHUB_REQUEST_TIMEOUT,
)

def fetch_daily_repos_to_minio(start_date, end_date, timeout_seconds=GITHUB_REQUEST_TIMEOUT):
    logging.info(f"Starting to fetch 1000 repos for date: {start_date} to {end_date}")

    all_repos = []
    total_collected = 0

    try:
        for page in range(1, 11):
            logging.info(f"Fetching page {page}/10")
            data = repos_make_request(page, start_date)

            if not data or 'items' not in data:
                logging.warning(f"No data received for page: {page}")
                continue

            repos = data['items']
            all_repos.extend(repos)
            total_collected += len(repos)

            logging.info(f"Collected repos from page {page}. Total collected: {total_collected}")

            if len(repos) < 100:
                logging.info(f"Reached end of data at page: {page}")
                break
            
            if page < 10:
                logging.info(f"Waiting {timeout_seconds} seconds before next request...")
                time.sleep(timeout_seconds)

        if not all_repos:
            logging.error("No data received for any page")
            return None

        logging.info(f"Total repositories collected: {total_collected}")

        df = process_repos_with_pandas(all_repos)

        file_path = save_to_minio(df, start_date, end_date)

        return {
            'file_path': file_path,
            'total_repos': total_collected,
            'date': start_date,
        }
    except Exception as e:
        logging.error(f"Error occurred while fetching repos: {e}")
        return None

def repos_make_request(page, date):
    try:
        url = f'{GITHUB_REPOS_URL}'
        params = {
            'q': f'created:{date}',
            'page': page,
            'per_page': 100,
        }

        response = requests.get(url, headers=GITHUB_API_HEADERS, params=params)
        response.raise_for_status()
        logging.info(f"Fetched successfully for page {page} and date {date}")
        return response.json()
    except Exception as e:
        logging.error(f"Error occurred while fetching repos for page {page} and date: {date}: {e}")
        return None