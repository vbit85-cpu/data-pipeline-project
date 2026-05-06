import json
from utils import get_connection


def record_quality_check(
    batch_id,
    table_name,
    check_name,
    status,
    severity,
    failed_count=0,
    details=None,
    conn=None,
):
    """
    status: passed / failed
    severity: warning / critical
    details: dict, list, str, or None
    """

    if details is None:
        details_text = None
    elif isinstance(details, (dict, list)):
        details_text = json.dumps(details, default=str)
    else:
        details_text = str(details)

    query = """
        INSERT INTO etl_quality_checks (
            batch_id,
            table_name,
            check_name,
            status,
            severity,
            failed_count,
            details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    params = (
        str(batch_id),
        table_name,
        check_name,
        status,
        severity,
        failed_count,
        details_text,
    )
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
        return

    connection = get_connection()

    try:
        with connection.cursor() as cur:
            cur.execute(query, params)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def record_passed_check(batch_id, table_name, check_name, severity="warning", conn=None):
    record_quality_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name=check_name,
        status="passed",
        severity=severity,
        failed_count=0,
        details=None,
        conn=conn,
    )


def record_failed_check(batch_id, table_name, check_name, severity, failed_count, details=None, conn=None):
    record_quality_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name=check_name,
        status="failed",
        severity=severity,
        failed_count=failed_count,
        details=details,
        conn=conn,
    )
