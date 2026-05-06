import io
import re

import logging
import os
import time
import pandas as pd
from psycopg2 import sql
from utils import get_connection
from psycopg2.extras import execute_values


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(identifier: str):
    """
    Protects dynamic SQL identifiers like table names and column names.

    PostgreSQL identifiers should not be blindly interpolated into SQL strings.
    This validation is intentionally strict.
    """
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")


def _prepare_dataframe_for_load(df: pd.DataFrame, batch_id):
    """
    Adds batch_id and converts pandas NA/NaN values into None-compatible form.

    PostgreSQL COPY will receive missing values as \\N.
    """
    if df.empty:
        return df.copy()

    prepared_df = df.copy()
    prepared_df["batch_id"] = str(batch_id)

    prepared_df = prepared_df.astype(object)
    prepared_df = prepared_df.where(pd.notnull(prepared_df), None)

    return prepared_df


def copy_to_postgres(df: pd.DataFrame, table_name: str, batch_id, columns_config=None):
    """
    Bulk-loads dataframe into PostgreSQL using COPY.

    This function is intended for staging tables only:
        stg_entities
        stg_agents
        stg_orders
        ...

    """

    if df.empty:
        return

    _validate_identifier(table_name)

    prepared_df = _prepare_dataframe_for_load(df, batch_id)

    if columns_config:
        for column, meta in columns_config.items():
            if column not in prepared_df.columns:
                continue

            if meta.get("type") == "int":
                prepared_df[column] = prepared_df[column].apply(
                    lambda x: None if pd.isna(x) else str(int(x))
                )

            elif meta.get("type") == "float":
                prepared_df[column] = prepared_df[column].apply(
                    lambda x: None if pd.isna(x) else str(float(x))
                )

            elif meta.get("type") == "string":
                prepared_df[column] = prepared_df[column].apply(
                    lambda x: None if pd.isna(x) else str(x)
                )

    columns = list(prepared_df.columns)

    for column in columns:
        _validate_identifier(column)

    buffer = io.StringIO()

    prepared_df.to_csv(
        buffer,
        index=False,
        header=True,
        na_rep="\\N",
    )

    buffer.seek(0)

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            copy_sql = sql.SQL("""
                COPY {table_name} ({columns})
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER TRUE,
                    NULL '\\N'
                )
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(
                    sql.Identifier(column) for column in columns
                ),
            )

            cur.copy_expert(copy_sql.as_string(conn), buffer)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

def append_to_postgres(df: pd.DataFrame, table_name: str, batch_id):
    if df.empty:
        return

    _validate_identifier(table_name)
    prepared_df = _prepare_dataframe_for_load(df, batch_id)

    columns = list(prepared_df.columns)

    for column in columns:
        _validate_identifier(column)

    query = sql.SQL("""
        INSERT INTO {table_name} ({columns})
        VALUES %s
    """).format(
        table_name=sql.Identifier(table_name),
        columns=sql.SQL(", ").join(
            sql.Identifier(column) for column in columns
        ),
    )

    values = list(prepared_df.itertuples(index=False, name=None))

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            execute_values(cur, query.as_string(conn), values)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


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



