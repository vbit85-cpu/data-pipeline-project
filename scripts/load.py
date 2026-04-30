import logging
import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    """
    Create DB connection with retry logic.
    """

    for attempt in range(5):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database="airflow",
                user="airflow",
                password="airflow",
                port=5432
            )
            return conn

        except Exception as e:
            logging.warning(f"DB connection failed (attempt {attempt+1}): {e}")
            time.sleep(2)

    raise Exception("Could not connect to database after retries")


#def get_connection():
 #   Create PostgreSQL connection.
#    IMPORTANT: host must match docker-compose service name
#    return psycopg2.connect(
#        host="localhost",
#        database="airflow",
#        user="airflow",
#        password="airflow"
#    )

def load_to_postgres(df, table_name, batch_size=1000):
    """
    Fast batch insert using execute_values.
    """

    if df.empty:
        logging.warning(f"No data to load into {table_name}")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # Convert pandas/numpy types → native Python types
    df = df.where(pd.notnull(df), None)   # replace NaN with None
    df = df.astype(object)                # convert numpy types → python

    values = [tuple(row) for row in df.to_numpy()]

    # Extract column names
    columns = list(df.columns)
    columns_str = ", ".join(columns)

    # --- Build UPDATE part dynamically ---
    update_columns = [col for col in columns if col != "source_id"]

    update_str = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in update_columns
    ])

    # SQL template
    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES %s
        ON CONFLICT (source_id)
        DO UPDATE SET
        {update_str}
    """

    try:
        # Batch insert
        execute_values(
            cursor,
            query,
            values,
            page_size=batch_size
        )

        conn.commit()
        logging.info(f"Inserted {len(values)} rows into {table_name}")

    except Exception as e:
        conn.rollback()
        logging.exception(f"Error inserting into {table_name}: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

"""
def load_to_postgres(df, table):

    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cols = ",".join(row.index)
        vals = ",".join(["%s"] * len(row))

        query = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
        cur.execute(query, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
"""
