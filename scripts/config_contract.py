from pathlib import Path
from typing import Any

import yaml


from collections import defaultdict, deque


class ConfigContractError(Exception):
    """
    Raised when mappings.yaml violates the required ETL contract.
    """
    pass

ALLOWED_TYPES = {"int", "float", "string", "date", "datetime", "bool"}

RESERVED_STAGING_COLUMNS = {
    "stg_id",
    "batch_id",
    "loaded_at",
}


def validate_mappings_contract(config: dict):
    """
    Validates mappings.yaml.

    This includes two types of validation:

    1. Structural contract:
       - required sections exist
       - field types are correct
       - column definitions are valid

    2. Business contract:
       - every file must define normalized source_id
       - source_id must be int and required
       - dependency graph must be valid
       - no reserved staging columns are used

    Raises:
        ConfigContractError
    """

    if config is None:
        raise ConfigContractError("Config is empty")

    if not isinstance(config, dict):
        raise ConfigContractError("Config must be a dictionary")

    if "files" not in config:
        raise ConfigContractError("Config must contain top-level 'files' section")

    files = config["files"]

    if not isinstance(files, dict) or not files:
        raise ConfigContractError("'files' must be a non-empty dictionary")

    for file_name, file_cfg in files.items():
        _validate_file_config(file_name, file_cfg)

    _validate_dependencies(files)
    _validate_no_cycles(files)


def _validate_file_config(file_name: str, file_cfg: dict):
    if not isinstance(file_cfg, dict):
        raise ConfigContractError(f"{file_name}: file config must be a dictionary")

    required_fields = ["path", "table", "primary_key", "columns"]
    missing_fields = [
        field for field in required_fields
        if field not in file_cfg
    ]

    if missing_fields:
        raise ConfigContractError(
            f"{file_name}: missing required fields: {missing_fields}"
        )

    _validate_string_field(file_name, file_cfg, "path")
    _validate_string_field(file_name, file_cfg, "table")
    _validate_string_field(file_name, file_cfg, "primary_key")

    columns = file_cfg["columns"]

    if not isinstance(columns, dict) or not columns:
        raise ConfigContractError(
            f"{file_name}: 'columns' must be a non-empty dictionary"
        )

    _validate_business_source_id_contract(file_name, file_cfg, columns)
    _validate_reserved_columns(file_name, columns)

    for column_name, column_cfg in columns.items():
        _validate_column_config(file_name, column_name, column_cfg)

    transformations = file_cfg.get("transformations")
    if transformations is not None:
        _validate_transformations(file_name, transformations, columns)


def _validate_string_field(file_name: str, file_cfg: dict, field_name: str):
    if not isinstance(file_cfg[field_name], str) or not file_cfg[field_name].strip():
        raise ConfigContractError(
            f"{file_name}: '{field_name}' must be a non-empty string"
        )


def _validate_business_source_id_contract(file_name: str, file_cfg: dict, columns: dict):
    """
    Business rule:
    Every dataset must have normalized source_id.

    The physical CSV primary key can have any name:
        ent_id
        ag_id
        userID
        id

    But after normalization inside the pipeline it must become:
        source_id

    This is required because staging/core loaders depend on source_id.
    """

    if "source_id" not in columns:
        raise ConfigContractError(
            f"{file_name}: business contract violation: "
            f"normalized column 'source_id' is required. "
            f"Do not rename it to another column such as 'sce_id'."
        )

    source_id_cfg = columns["source_id"]

    if not isinstance(source_id_cfg, dict):
        raise ConfigContractError(
            f"{file_name}.source_id: config must be a dictionary"
        )

    source_id_type = source_id_cfg.get("type")
    if source_id_type != "int":
        raise ConfigContractError(
            f"{file_name}.source_id: must have type 'int', got '{source_id_type}'"
        )

    if source_id_cfg.get("required") is not True:
        raise ConfigContractError(
            f"{file_name}.source_id: must be required: true"
        )

    primary_key = file_cfg["primary_key"]
    source_mapping = source_id_cfg.get("source")

    if source_mapping is not None and source_mapping != primary_key:
        raise ConfigContractError(
            f"{file_name}.source_id.source must match primary_key. "
            f"primary_key='{primary_key}', source='{source_mapping}'"
        )


def _validate_reserved_columns(file_name: str, columns: dict):
    for column_name in columns:
        if column_name in RESERVED_STAGING_COLUMNS:
            raise ConfigContractError(
                f"{file_name}: column '{column_name}' is reserved "
                f"for staging technical metadata"
            )


def _validate_column_config(file_name: str, column_name: str, column_cfg: dict):
    if not isinstance(column_name, str) or not column_name.strip():
        raise ConfigContractError(
            f"{file_name}: column name must be a non-empty string"
        )

    if not isinstance(column_cfg, dict):
        raise ConfigContractError(
            f"{file_name}.{column_name}: column config must be a dictionary"
        )

    if "type" not in column_cfg:
        raise ConfigContractError(
            f"{file_name}.{column_name}: missing required field 'type'"
        )

    column_type = column_cfg["type"]

    if column_type not in ALLOWED_TYPES:
        raise ConfigContractError(
            f"{file_name}.{column_name}: unsupported type '{column_type}'. "
            f"Allowed types: {sorted(ALLOWED_TYPES)}"
        )

    if "required" not in column_cfg:
        raise ConfigContractError(
            f"{file_name}.{column_name}: missing required field 'required'"
        )

    if not isinstance(column_cfg["required"], bool):
        raise ConfigContractError(
            f"{file_name}.{column_name}.required must be boolean"
        )

    if "source" in column_cfg and not isinstance(column_cfg["source"], str):
        raise ConfigContractError(
            f"{file_name}.{column_name}.source must be string"
        )


def _validate_transformations(file_name: str, transformations, columns: dict):
    if not isinstance(transformations, list):
        raise ConfigContractError(
            f"{file_name}: transformations must be a list"
        )

    for index, transformation in enumerate(transformations):
        if not isinstance(transformation, dict):
            raise ConfigContractError(
                f"{file_name}.transformations[{index}]: must be a dictionary"
            )

        if "type" not in transformation:
            raise ConfigContractError(
                f"{file_name}.transformations[{index}]: missing 'type'"
            )

        if "column" not in transformation:
            raise ConfigContractError(
                f"{file_name}.transformations[{index}]: missing 'column'"
            )

        column = transformation["column"]

        if column not in columns:
            raise ConfigContractError(
                f"{file_name}.transformations[{index}]: "
                f"unknown column '{column}'"
            )

        transformation_type = transformation["type"]

        if transformation_type == "multiply":
            if "value" not in transformation:
                raise ConfigContractError(
                    f"{file_name}.transformations[{index}]: "
                    f"multiply transformation requires 'value'"
                )

            if columns[column]["type"] not in {"int", "float"}:
                raise ConfigContractError(
                    f"{file_name}.transformations[{index}]: "
                    f"multiply can only be applied to numeric columns"
                )

        else:
            raise ConfigContractError(
                f"{file_name}.transformations[{index}]: "
                f"unsupported transformation type '{transformation_type}'"
            )


def _validate_dependencies(files: dict):
    for file_name, file_cfg in files.items():
        depends_on = file_cfg.get("depends_on", [])

        if depends_on is None:
            depends_on = []

        if not isinstance(depends_on, list):
            raise ConfigContractError(
                f"{file_name}.depends_on must be a list"
            )

        for dependency in depends_on:
            if dependency not in files:
                raise ConfigContractError(
                    f"{file_name}: depends on unknown file config '{dependency}'"
                )

            if dependency == file_name:
                raise ConfigContractError(
                    f"{file_name}: cannot depend on itself"
                )


def _validate_no_cycles(files: dict):
    graph = defaultdict(list)
    in_degree = {file_name: 0 for file_name in files}

    for file_name, file_cfg in files.items():
        for dependency in file_cfg.get("depends_on", []):
            graph[dependency].append(file_name)
            in_degree[file_name] += 1

    queue = deque([
        file_name
        for file_name, degree in in_degree.items()
        if degree == 0
    ])

    visited = []

    while queue:
        current = queue.popleft()
        visited.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1

            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(visited) != len(files):
        raise ConfigContractError(
            "Dependency graph contains a cycle"
        )
