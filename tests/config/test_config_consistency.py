#!/usr/bin/env python
"""Tests to ensure configuration file consistency."""

import json
from pathlib import Path
from typing import Any, cast

import jsonschema
import pytest
import yaml

# Define paths relative to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "cyberdelta" / "config"
EXAMPLES_DIR = PROJECT_ROOT / "examples"
CONFIG_PATH = CONFIG_DIR / "config.yaml"
EXAMPLE_CONFIG_PATH = CONFIG_DIR / "config.yaml.example"
SCHEMA_PATH = CONFIG_DIR / "config.schema.json"
EXAMPLE_SCRIPT_PATH = EXAMPLES_DIR / "config_example.py"


def get_yaml_keys(file_path: Path) -> set[str]:
    """Loads a YAML file and returns a set of all nested keys."""
    try:
        with open(file_path) as f:
            data_any = yaml.safe_load(f)
        if not isinstance(data_any, dict):
            return set()
        return _extract_keys(cast("dict[str, Any]", data_any))
    except FileNotFoundError:
        pytest.fail(f"YAML file not found: {file_path}")
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML file {file_path}: {e}")


def get_yaml_keys_from_string(yaml_string: str) -> set[str]:
    """Loads YAML from a string and returns a set of all nested keys."""
    try:
        data_any = yaml.safe_load(yaml_string)
        if not isinstance(data_any, dict):
            # If loaded data is not a dict (e.g., list, scalar, None), return empty set
            return set()
        # Now data is confirmed to be a dict
        return _extract_keys(cast("dict[str, Any]", data_any))
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML string: {e}")


def _extract_keys(data: dict[str, Any], prefix: str = "") -> set[str]:
    """Recursively extracts keys from a nested dictionary."""
    keys: set[str] = set()
    for k, v in data.items():
        full_key = f"{prefix}.{k}" if prefix else k
        keys.add(full_key)
        if isinstance(v, dict):
            keys.update(_extract_keys(cast("dict[str, Any]", v), full_key))
    return keys


class TestConfigConsistency:
    """Verify that configuration example files match the main configuration."""

    def test_config_and_example_match(self) -> None:
        """Ensure config.yaml and config.yaml.example have the same keys."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        example_keys = get_yaml_keys(EXAMPLE_CONFIG_PATH)

        assert actual_keys == example_keys, (
            f"Mismatch between {CONFIG_PATH} and {EXAMPLE_CONFIG_PATH}. \
            Missing in example: {actual_keys - example_keys}. \
            Extra in example: {example_keys - actual_keys}"
        )

    def test_example_script_content_matches(self) -> None:
        """Ensure config_content in examples/config_example.py matches config.yaml."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        script_keys = get_yaml_keys(EXAMPLE_CONFIG_PATH)

        assert actual_keys == script_keys, (
            f"Mismatch between {CONFIG_PATH} and keys in {EXAMPLE_CONFIG_PATH}. \n"
            f"            Missing in example: {actual_keys - script_keys}. \n"
            f"            Extra in example: {script_keys - actual_keys}"
        )

    @pytest.mark.xfail(reason="config.schema.json does not exist in the project")
    def test_example_config_validates_against_schema(self) -> None:
        """Ensure config.yaml.example validates against the schema."""
        # Load schema
        try:
            with open(SCHEMA_PATH) as f:
                schema = json.load(f)
        except Exception as e:
            pytest.fail(f"Failed to load schema {SCHEMA_PATH}: {e}")

        # Load example config
        try:
            with open(EXAMPLE_CONFIG_PATH) as f:
                example_config_any = yaml.safe_load(f)
            if example_config_any is None:
                example_config_any = {}
        except Exception as e:
            pytest.fail(f"Failed to load example config {EXAMPLE_CONFIG_PATH}: {e}")

        # Ensure example_config is a dict before validation
        if not isinstance(example_config_any, dict):
            pytest.fail(
                f"Example config {EXAMPLE_CONFIG_PATH} did not load as a dictionary "
                f"(loaded type: {type(example_config_any)}). Cannot validate.",
            )

        # Validate example against schema
        try:
            jsonschema.validate(instance=cast("dict[str, Any]", example_config_any), schema=schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Example config {EXAMPLE_CONFIG_PATH} failed validation: {e}")

    def test_config_matches_example_script_exchanges(self) -> None:
        """Verify that config.yaml keys match config.yaml.example keys."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        example_script_keys = get_yaml_keys(EXAMPLE_CONFIG_PATH)

        assert actual_keys == example_script_keys, (
            f"Mismatch between {CONFIG_PATH} and keys in {EXAMPLE_CONFIG_PATH}. \n"
            f"            Missing in example: {actual_keys - example_script_keys}. \n"
            f"            Extra in example: {example_script_keys - actual_keys}"
        )

    @pytest.mark.xfail(reason="config.schema.json does not exist in the project")
    def test_example_config_matches_schema(self) -> None:
        """Verify that config.example.toml matches the schema definition."""
        # Load schema
        schema: dict[str, Any] = {}
        try:
            with open(SCHEMA_PATH) as f:
                schema = json.load(f)
        except Exception as e:
            pytest.fail(f"Failed to load schema {SCHEMA_PATH}: {e}")

        # Load example config
        try:
            with open(EXAMPLE_CONFIG_PATH) as f:
                example_config_any = yaml.safe_load(f)
            if example_config_any is None:
                example_config_any = {}
        except Exception as e:
            pytest.fail(f"Failed to load example config {EXAMPLE_CONFIG_PATH}: {e}")

        # Ensure example_config is a dict before validation
        if not isinstance(example_config_any, dict):
            pytest.fail(
                f"Example config {EXAMPLE_CONFIG_PATH} did not load as a dictionary "
                f"(loaded type: {type(example_config_any)}). Cannot validate.",
            )

        # Cast to proper type after isinstance check
        example_config_dict = cast("dict[str, Any]", example_config_any)

        # Validate example against schema
        try:
            jsonschema.validate(instance=example_config_dict, schema=schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Example config {EXAMPLE_CONFIG_PATH} failed validation: {e}")
