import os
import logging

import uuid
from audit import start_batch, finish_batch, fail_batch
from graph import DependencyGraph
from utils import load_config, save_bad_records
from extract import extract_csv, apply_types
from transform import apply_transformations
from load import append_to_postgres
from core_loader import load_core_by_levels
from logger import setup_logger
from validate import validate_schema, split_valid_invalid


def run_pipeline():
    logger = setup_logger()
    config = load_config()

    base_path = os.getenv("DATA_PATH", "data")

    batch_id = uuid.uuid4()
    logger.info(f"Starting batch: {batch_id}")

    start_batch(batch_id)

    try:
        graph = DependencyGraph(config)
        levels = graph.get_levels()

        logger.info(f"Execution levels: {levels}")

        for level in levels:
            logger.info(f"Processing level: {level}")

            for name in level:
                cfg = config["files"][name]

                logger.info(f"Processing file: {name}")

                try:
                    path = os.path.join(base_path, cfg["path"])

                    if not os.path.isfile(path):
                        logger.warning(f"File not found: {path}")
                        continue

                    if os.path.getsize(path) == 0:
                        logger.warning(f"Empty file: {path}")
                        continue

                    df = extract_csv(path)

                    pk = cfg.get("primary_key")

                    if pk not in df.columns:
                        raise Exception(f"Primary key '{pk}' not found in file {name}")

                    df.rename(columns={pk: "source_id"}, inplace=True)

                    df = apply_types(df, cfg)

                    errors = validate_schema(df, cfg)
                    if errors:
                        logger.error(f"Schema errors in {name}: {errors}")
                        continue

                    valid_df, invalid_df = split_valid_invalid(df, cfg)

                    if not invalid_df.empty:
                        logger.warning(f"{len(invalid_df)} bad records in {name}")
                        save_bad_records(invalid_df, name)

                    if valid_df.empty:
                        logger.warning(f"No valid data in {name}")
                        continue

                    df_transformed = apply_transformations(valid_df, cfg.get("transformations"))

                    #load_to_postgres(df_transformed, f"stg_{cfg['table']}")
                    append_to_postgres(df_transformed, f"stg_{cfg['table']}", batch_id)
                    logger.info(f"Loaded {len(df_transformed)} records into {cfg['table']}")

                except Exception as e:
                    logger.exception(f"Critical error in {name}: {str(e)}")

        logger.info("Starting core load")
        load_core_by_levels(levels, config, batch_id)
        logger.info("Core load finished")

        finish_batch(batch_id)
    except Exception as e:
        fail_batch(batch_id, str(e))
        logger.exception(f"Pipeline failed for batch {batch_id}: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
