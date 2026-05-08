from utils import get_connection

def start_batch(batch_id):
    query = """
        INSERT INTO etl_batches (
            batch_id,
            started_at,
            status,
            error_message
        )
        VALUES (
            %s,
            now(),
            'started',
            NULL
        )
        ON CONFLICT (batch_id)
        DO UPDATE SET
            started_at = now(),
            finished_at = NULL,
            status = 'started',
            error_message = NULL;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (str(batch_id),))
        conn.commit()


def finish_batch(batch_id):
    query = """
        UPDATE etl_batches
        SET status = 'success',
            finished_at = now(),
            error_message = NULL
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
