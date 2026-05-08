import argparse
import re
from pathlib import Path
from config_contract import validate_mappings_contract

from utils import get_connection

import yaml
from psycopg2 import sql


POSTGRES_TYPE_MAPPING = {
    "int": "INT",
    "float": "DOUBLE PRECISION",
    "string": "TEXT",
}

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(identifier: str):
    if not IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")


def load_config_from_path(config_path: str):
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)


    validate_mappings_contract(config)

    return config



def generate_staging_table_sql(table_name: str, columns: dict) -> str:
    validate_identifier(table_name)

    staging_table_name = f"stg_{table_name}"

    column_lines = [
    "    stg_id BIGSERIAL PRIMARY KEY",
    "    batch_id UUID NOT NULL",
    "    source_file TEXT NOT NULL",
    "    source_row_number BIGINT NOT NULL",
    "    loaded_at TIMESTAMP DEFAULT now()",
    "    raw_record JSONB"
    ]

    for column_name, column_cfg in columns.items():
        validate_identifier(column_name)

        column_type = column_cfg["type"]
        postgres_type = POSTGRES_TYPE_MAPPING[column_type]

        column_lines.append(f"    {column_name} TEXT")

    columns_sql = ",\n".join(column_lines)

    constraint_name = f"uq_stg_{table_name}_batch_file_row"

    return f"""CREATE TABLE IF NOT EXISTS {staging_table_name} (
{columns_sql},
    CONSTRAINT {constraint_name}
    UNIQUE (batch_id, source_file, source_row_number)
);"""



def generate_all_staging_sql(config: dict) -> str:
    validate_mappings_contract(config)

    statements = [
        "-- Auto-generated staging DDL",
        "-- Source: configs/mappings.yaml",
        "-- Do not edit manually unless you know what you are doing.",
        "",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
        "",
    ]

    for file_name, file_cfg in config["files"].items():
        table_name = file_cfg["table"]
        columns = file_cfg["columns"]

        statements.append(f"-- Staging table for: {file_name}")
        statements.append(generate_staging_table_sql(table_name, columns))
        statements.append("")

    return "\n".join(statements)


def write_sql_file(sql_text: str, output_path: str):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sql_text)

def ensure_staging_tables(config: dict):
    """
    Applies staging DDL directly to PostgreSQL.

    This is used by pipeline preflight and Airflow.
    """

    validate_mappings_contract(config)

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

            for file_name, file_cfg in config["files"].items():
                table_name = file_cfg["table"]
                staging_table_name = f"stg_{table_name}"

                validate_identifier(staging_table_name)

                column_definitions = [
                    sql.SQL("stg_id BIGSERIAL PRIMARY KEY"),
                    sql.SQL("batch_id UUID NOT NULL"),
                    sql.SQL("source_file TEXT NOT NULL"),
                    sql.SQL("source_row_number BIGINT NOT NULL"),
                    sql.SQL("loaded_at TIMESTAMP DEFAULT now()"),
                    sql.SQL("raw_record JSONB")
                ]

                for column_name, column_cfg in file_cfg["columns"].items():
                    validate_identifier(column_name)

                    postgres_type = POSTGRES_TYPE_MAPPING[column_cfg["type"]]

                    column_definitions.append(
                        sql.SQL("{} TEXT").format(
                            sql.Identifier(column_name),
                        )
                    )


                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        {}
                    );
                """).format(
                    sql.Identifier(staging_table_name),
                    sql.SQL(", ").join(column_definitions),
                )

                cur.execute(create_table_query)
                constraint_name = f"uq_{staging_table_name}_batch_file_row"
                cur.execute(
                    sql.SQL("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                               SELECT 1
                               FROM pg_constraint
                               WHERE conname = %s
                            ) THEN
                                ALTER TABLE {} ADD CONSTRAINT {} 
                                UNIQUE (batch_id, source_file, source_row_number);
                            END IF;
                        END
                        $$;
                    """).format(
                        sql.Identifier(staging_table_name),
                        sql.Identifier(constraint_name),
                    ),
                    [constraint_name],
                )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate PostgreSQL staging DDL from mappings.yaml"
    )

    parser.add_argument(
        "--config",
        default="configs/mappings.yaml",
        help="Path to mappings.yaml",
    )

    parser.add_argument(
        "--output",
        default="generated/staging_tables.sql",
        help="Output SQL file path",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply generated staging DDL directly to PostgreSQL",
    )

    args = parser.parse_args()

    config = load_config_from_path(args.config)
    sql_text = generate_all_staging_sql(config)

    write_sql_file(sql_text, args.output)

    print(f"Generated staging DDL: {args.output}")

    if args.apply:
        ensure_staging_tables(config)
        print("Applied staging DDL to PostgreSQL")

if __name__ == "__main__":
    main()
