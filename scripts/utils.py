import yaml
import os
import logging
import psycopg2
from datetime import datetime
import time

def get_connection():
    """
    Create DB connection with retry logic.
    """

    for attempt in range(5):
        try:
             conn = psycopg2.connect(
                 host=os.getenv("POSTGRES_HOST", "localhost"),
                 port=os.getenv("POSTGRES_PORT", "5432"),
                 dbname=os.getenv("POSTGRES_DB", "airflow"),
                 user=os.getenv("POSTGRES_USER", "airflow"),
                 password=os.getenv("POSTGRES_PASSWORD", "airflow"),
             )
             return conn

        except Exception as e:
            logging.warning(f"DB connection failed (attempt {attempt+1}): {e}")
            time.sleep(2)

    raise Exception("Could not connect to database after retries")

def save_bad_records(df, name):
    os.makedirs("data/bad", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/bad/{name}_bad_{timestamp}.csv"

    #path = f"data/bad/{name}_bad.csv"
    df.to_csv(path, index=False)

def load_config(path="configs/mappings.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
