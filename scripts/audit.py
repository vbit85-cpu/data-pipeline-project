from utils import get_connection


def start_batch(batch_id):
    query = """
        INSERT INTO etl_batches (batch_id, status)
        VALUES (%s, 'running')
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (str(batch_id),))
        conn.commit()
    finally:
        conn.close()


def finish_batch(batch_id):
    query = """
        UPDATE etl_batches
        SET status = 'success',
            finished_at = now()
        WHERE batch_id = %s
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (str(batch_id),))
        conn.commit()
    finally:
        conn.close()


def fail_batch(batch_id, error_message):
    query = """
        UPDATE etl_batches
        SET status = 'failed',
            finished_at = now(),
            error_message = %s
        WHERE batch_id = %s
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (str(error_message), str(batch_id)))
        conn.commit()
    finally:
        conn.close()
