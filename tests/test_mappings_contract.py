import yaml
import pytest
from pathlib import Path
from collections import defaultdict, deque
from scripts.config_contract import validate_mappings_contract

CONFIG_PATH = Path("configs/mappings.yaml")

ALLOWED_TYPES = {"int", "float", "string", "date", "datetime", "bool"}

def load_mappings():
    assert CONFIG_PATH.exists(), f"Config file not found: {CONFIG_PATH}"

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    assert config is not None, "mappings.yaml is empty"
    return config


def test_config_has_files_section():
    config = load_mappings()

    assert "files" in config, "mappings.yaml must contain top-level 'files' section"
    assert isinstance(config["files"], dict), "'files' must be a dictionary"
    assert config["files"], "'files' section must not be empty"


def test_each_file_has_required_top_level_keys():
    config = load_mappings()

    required_keys = {"path", "table", "primary_key", "columns"}

    for file_name, file_cfg in config["files"].items():
        missing = required_keys - set(file_cfg.keys())

        assert not missing, (
            f"File config '{file_name}' is missing required keys: {missing}"
        )


def test_columns_section_is_valid():
    config = load_mappings()

    for file_name, file_cfg in config["files"].items():
        columns = file_cfg["columns"]

        assert isinstance(columns, dict), (
            f"'columns' for '{file_name}' must be a dictionary"
        )

        assert columns, f"'columns' for '{file_name}' must not be empty"

        assert "source_id" in columns, (
            f"'{file_name}' must contain normalized 'source_id' column"
        )

        for column_name, column_cfg in columns.items():
            assert isinstance(column_cfg, dict), (
                f"Column config '{file_name}.{column_name}' must be a dictionary"
            )

            assert "type" in column_cfg, (
                f"Column '{file_name}.{column_name}' must define 'type'"
            )

            assert column_cfg["type"] in ALLOWED_TYPES, (
                f"Column '{file_name}.{column_name}' has unsupported type "
                f"'{column_cfg['type']}'. Allowed types: {ALLOWED_TYPES}"
            )

            if "required" in column_cfg:
                assert isinstance(column_cfg["required"], bool), (
                    f"Column '{file_name}.{column_name}'.required must be boolean"
                )

            if "source" in column_cfg:
                assert isinstance(column_cfg["source"], str), (
                    f"Column '{file_name}.{column_name}'.source must be string"
                )


def test_depends_on_references_existing_files():
    config = load_mappings()
    files = config["files"]

    for file_name, file_cfg in files.items():
        depends_on = file_cfg.get("depends_on", [])

        assert isinstance(depends_on, list), (
            f"'depends_on' for '{file_name}' must be a list"
        )

        for dependency in depends_on:
            assert dependency in files, (
                f"'{file_name}' depends on unknown file config '{dependency}'"
            )


def test_no_self_dependencies():
    config = load_mappings()

    for file_name, file_cfg in config["files"].items():
        depends_on = file_cfg.get("depends_on", [])

        assert file_name not in depends_on, (
            f"'{file_name}' cannot depend on itself"
        )


def test_dependency_graph_has_no_cycles():
    config = load_mappings()
    files = config["files"]

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

    assert len(visited) == len(files), (
        "Dependency graph contains a cycle"
    )


def test_primary_key_is_declared_as_source_id_or_source_mapping():
    config = load_mappings()

    for file_name, file_cfg in config["files"].items():
        primary_key = file_cfg["primary_key"]
        source_id_cfg = file_cfg["columns"].get("source_id")

        assert source_id_cfg is not None, (
            f"'{file_name}' must define source_id column"
        )

        source_mapping = source_id_cfg.get("source")

        assert source_mapping is None or source_mapping == primary_key, (
            f"'{file_name}' source_id.source must match primary_key "
            f"if provided. primary_key={primary_key}, source={source_mapping}"
        )

def test_mappings_yaml_contract():
    assert CONFIG_PATH.exists(), f"Config file not found: {CONFIG_PATH}"

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    validate_mappings_contract(config)
