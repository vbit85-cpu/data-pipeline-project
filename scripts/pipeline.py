import os
import logging
import uuid

from audit import start_batch, finish_batch, fail_batch
from graph import DependencyGraph
from utils import load_config, save_bad_records
from extract import extract_csv, normalize_columns, apply_types
from transform import apply_transformations
from load import copy_to_postgres
from core_loader import load_core_by_table
from logger import setup_logger
from validate import validate_schema, split_valid_invalid
from quality import record_failed_check, record_passed_check
from config_contract import validate_mappings_contract
from ddl_generator import ensure_staging_tables

def run_preflight():
    logger = setup_logger()

    logger.info("Running pipeline preflight checks")

    config = load_config()

    validate_mappings_contract(config)
    logger.info("Mappings contract validation passed")

    ensure_staging_tables(config)
    logger.info("Staging tables ensured")

    return True

def get_execution_plan():
    config = load_config()
    validate_mappings_contract(config)

    graph = DependencyGraph(config)
    levels = graph.get_levels()
    return config, levels

def create_batch():
    """
    Normal mode:
    - creates a new batch_id.

    Test/retry mode:
    - if BATCH_ID env var is provided, uses it.
    - useful for local idempotency testing.
    """

    forced_batch_id = os.getenv("BATCH_ID")

    if forced_batch_id:
        batch_id = forced_batch_id
    else:
        batch_id = str(uuid.uuid4())

    start_batch(batch_id)
    return str(batch_id)

def finish_existing_batch(batch_id):
    finish_batch(batch_id)


def fail_existing_batch(batch_id, error_message):
    fail_batch(batch_id, error_message)

def process_file_to_staging(name, cfg, batch_id):
    logger = setup_logger()
    base_path = os.getenv("DATA_PATH", "data")
    table_name = cfg["table"]
    source_file = cfg["path"]

    logger.info(f"Processing file: {name}, batch_id={batch_id}")

    path = os.path.join(base_path, source_file)

    if not os.path.isfile(path):
        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="file_exists",
            severity="critical",
            failed_count=1,
            details={"path": path},
        )
        raise FileNotFoundError(f"File not found: {path}")

    record_passed_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name="file_exists",
        severity="critical",
    )

    if os.path.getsize(path) == 0:
        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="file_not_empty",
            severity="critical",
            failed_count=1,
            details={"path": path},
        )
        raise ValueError(f"Empty file: {path}")

    record_passed_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name="file_not_empty",
        severity="critical",
    )

    df = extract_csv(path)
    df["__source_row_number"] = df.index + 2

    pk = cfg.get("primary_key")

    if pk not in df.columns:
        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="primary_key_exists",
            severity="critical",
            failed_count=1,
            details={"primary_key": pk, "columns": list(df.columns)},
        )
        raise ValueError(f"Primary key '{pk}' not found in file {name}")

    record_passed_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name="primary_key_exists",
        severity="critical",
    )


#    df.rename(columns={pk: "source_id"}, inplace=True)

    df = normalize_columns(df, cfg)

    df = apply_types(df, cfg)

    technical_columns = [col for col in df.columns if col.startswith("__")]

    validation_df = df.drop(columns=technical_columns, errors="ignore")

    errors = validate_schema(validation_df, cfg)


    if errors:
        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="schema_validation",
            severity="critical",
            failed_count=len(errors),
            details=errors,
        )
        raise ValueError(f"Schema errors in {name}: {errors}")

    record_passed_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name="schema_validation",
        severity="critical",
    )

    valid_df, invalid_df = split_valid_invalid(df, cfg)

    if not invalid_df.empty:
        logger.warning(f"{len(invalid_df)} bad records in {name}")
        save_bad_records(invalid_df, name)

        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="bad_records",
            severity="warning",
            failed_count=len(invalid_df),
            details={
                "bad_records_count": len(invalid_df),
                "bad_records_file": f"data/bad/{name}_bad_records.csv",
            },
        )
    else:
        record_passed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="bad_records",
            severity="warning",
        )

    if valid_df.empty:
        record_failed_check(
            batch_id=batch_id,
            table_name=table_name,
            check_name="valid_records_exist",
            severity="critical",
            failed_count=1,
            details={"message": "No valid records after validation"},
        )
        raise ValueError(f"No valid data in {name}")

    record_passed_check(
        batch_id=batch_id,
        table_name=table_name,
        check_name="valid_records_exist",
        severity="critical",
    )

    df_transformed = apply_transformations(valid_df, cfg.get("transformations"))

    inserted_rows = copy_to_postgres(
        df=df_transformed,
        table_name=f"stg_{cfg['table']}",
        batch_id=batch_id,
        columns_config=cfg["columns"],
        source_file=source_file,
    )

    logger.info(
        f"Loaded {inserted_rows} new records into stg_{cfg['table']}, "
        f"batch_id={batch_id}"
    )


def load_staging_table(file_name, batch_id):
    logger = setup_logger()
    config = load_config()
    validate_mappings_contract(config)

    if file_name not in config["files"]:
        raise ValueError(f"Unknown file config: {file_name}")

    cfg = config["files"][file_name]

    logger.info(f"Starting staging table task: {file_name}, batch_id={batch_id}")
    process_file_to_staging(file_name, cfg, batch_id)
    logger.info(f"Finished staging table task: {file_name}, batch_id={batch_id}")


def load_core_table(file_name, batch_id):
    logger = setup_logger()
    config = load_config()
    validate_mappings_contract(config)

    if file_name not in config["files"]:
        raise ValueError(f"Unknown file config: {file_name}")

    table_name = config["files"][file_name]["table"]

    logger.info(
        f"Starting core table task: file={file_name}, table={table_name}, batch_id={batch_id}"
    )

    from utils import get_connection

    with get_connection() as conn:
        load_core_by_table(conn, table_name, batch_id)

    logger.info(
        f"Finished core table task: file={file_name}, table={table_name}, batch_id={batch_id}"
    )



def load_staging_level(level_index, batch_id):
    logger = setup_logger()
    config, levels = get_execution_plan()

    level = levels[level_index]
    logger.info(f"Starting staging level {level_index}: {level}")

    for name in level:
        cfg = config["files"][name]
        process_file_to_staging(name, cfg, batch_id)

    logger.info(f"Finished staging level {level_index}")

def load_core_level(level_index, batch_id):
    logger = setup_logger()
    config, levels = get_execution_plan()

    level = levels[level_index]
    logger.info(f"Starting core level {level_index}: {level}")

    from utils import get_connection

    with get_connection() as conn:
        for file_name in level:
            table_name = config["files"][file_name]["table"]
            logger.info(f"Loading core table: {table_name}, batch_id={batch_id}")
            load_core_by_table(conn, table_name, batch_id)

    logger.info(f"Finished core level {level_index}")

def run_pipeline():
    """
    CLI/local full pipeline runner.
    Airflow should use create_batch/load_staging_level/load_core_level/finish_existing_batch.
    """
    logger = setup_logger()
    batch_id = create_batch()

    try:
        run_preflight()
        config, levels = get_execution_plan()

        logger.info(f"Execution levels: {levels}")
        logger.info(f"Starting batch: {batch_id}")

        for level_index in range(len(levels)):
            load_staging_level(level_index, batch_id)
            load_core_level(level_index, batch_id)

        finish_existing_batch(batch_id)
        logger.info(f"Batch finished successfully: {batch_id}")

    except Exception as e:
        fail_existing_batch(batch_id, str(e))
        logger.exception(f"Pipeline failed for batch {batch_id}: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
