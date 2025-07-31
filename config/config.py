from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# API
ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
GITHUB_REPOS_URL = 'https://api.github.com/search/repositories'
GITHUB_API_HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
# API Request timeout between requests (seconds)
GITHUB_REQUEST_TIMEOUT = int(os.getenv("GITHUB_REQUEST_TIMEOUT", "5"))

# S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ACCESS = os.getenv("MINIO_ACCESS")
MINIO_SECRET = os.getenv("MINIO_SECRET")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

POSTGRES_DWH_CONN_ID = os.getenv("POSTGRES_DWH_CONN_ID")

# Airflow default args
OWNER = 'swift'
RAW_LAYER = 'raw'
STG_LAYER = 'stg'
ODS_LAYER = 'ods'
DM_LAYER = 'dm'

DEFAULT_ARGS = {
    'owner': OWNER,
    'retries': 3,
    'retry_delay': timedelta(hours=1),
}