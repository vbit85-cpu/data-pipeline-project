import io
import json
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
from psycopg2 import sql

from utils import get_connection


RESERVED_STAGING_COLUMNS = [
    "batch_id",
    "source_file",
    "source_row_number",
    "loaded_at",
    "raw_record",
]


class LoadError(Exception):
    pass


def quote_table_name(table_name: str) -> sql.Identifier:
    if not isinstance(table_name, str) or not table_name.strip():
        raise LoadError("table_name must be a non-empty string")

    return sql.Identifier(table_name)


def _prepare_staging_dataframe(
    df,
    batch_id,
    source_file,
    columns_config,
):
    if df.empty:
        raise LoadError("Cannot load empty dataframe to staging")

    if not source_file:
        raise LoadError("source_file must be provided")

    normalized_columns = list(columns_config.keys())

    missing_columns = [col for col in normalized_columns if col not in df.columns]
    if missing_columns:
        raise LoadError(
            f"DataFrame is missing normalized columns required by mappings: {missing_columns}"
        )

    result = pd.DataFrame(index=df.index)

    result["batch_id"] = str(batch_id)
    result["source_file"] = source_file

    if "__source_row_number" in df.columns:
        result["source_row_number"] = df["__source_row_number"].astype("int64")
    else:
        result["source_row_number"] = range(1, len(df) + 1)

    for col in normalized_columns:
        result[col] = df[col]

    raw_columns = [col for col in df.columns if not col.startswith("__")]

    result["raw_record"] = df[raw_columns].apply(
        lambda row: json.dumps(row.where(pd.notnull(row), None).to_dict(), default=str),
        axis=1,
    )

    return result


def _copy_dataframe_to_table(
    cursor,
    df: pd.DataFrame,
    table_name: str,
    columns: List[str],
) -> None:
    """
    Uses PostgreSQL COPY FROM STDIN for fast loading.
    """

    buffer = io.StringIO()

    copy_df = df[columns].copy()
    copy_df = copy_df.where(pd.notnull(copy_df), None)

    copy_df.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
    )

    buffer.seek(0)

    copy_sql = sql.SQL(
        """
        COPY {table} ({columns})
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E'\t',
            NULL '\\N',
            QUOTE '"',
            ESCAPE '"'
        )
        """
    ).format(
        table=quote_table_name(table_name),
        columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )

    cursor.copy_expert(copy_sql.as_string(cursor), buffer)


def copy_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    batch_id: str,
    columns_config: Dict[str, Any],
    source_file: Optional[str] = None
) -> int:
    """
    Idempotent staging load.

    Strategy:
    1. Prepare dataframe with metadata columns.
    2. COPY dataframe into temporary table.
    3. INSERT from temporary table into target stg table.
    4. ON CONFLICT(batch_id, source_file, source_row_number) DO NOTHING.

    This makes Airflow retries safe for the same batch_id.
    """

    source_file = source_file or table_name

    staging_df = _prepare_staging_dataframe(
        df=df,
        batch_id=batch_id,
        source_file=source_file,
        columns_config=columns_config,
    )

    temp_table = f"tmp_{table_name}_{uuid.uuid4().hex[:12]}"

    business_columns = list(columns_config.keys())

    insert_columns = [
        "batch_id",
        "source_file",
        "source_row_number",
        *business_columns,
        "raw_record",
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    CREATE TEMP TABLE {temp_table}
                    (LIKE {target_table} INCLUDING DEFAULTS)
                    ON COMMIT DROP
                    """
                ).format(
                    temp_table=quote_table_name(temp_table),
                    target_table=quote_table_name(table_name),
                )
            )

            _copy_dataframe_to_table(
                cursor=cur,
                df=staging_df,
                table_name=temp_table,
                columns=insert_columns,
            )

            insert_sql = sql.SQL(
                """
                INSERT INTO {target_table} ({columns})
                SELECT {columns}
                FROM {temp_table}
                ON CONFLICT (batch_id, source_file, source_row_number)
                DO NOTHING
                """
            ).format(
                target_table=quote_table_name(table_name),
                temp_table=quote_table_name(temp_table),
                columns=sql.SQL(", ").join(
                    sql.Identifier(col) for col in insert_columns
                ),
            )

            cur.execute(insert_sql)
            inserted_rows = cur.rowcount

        conn.commit()

    return inserted_rows
