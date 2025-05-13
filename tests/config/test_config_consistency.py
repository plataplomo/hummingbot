#!/usr/bin/env python
"""Tests to ensure configuration file consistency."""

import json
import re
from pathlib import Path
from typing import Any, cast

import jsonschema
import pytest
import yaml

# Define paths relative to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
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
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            return set()
        return _extract_keys(cast(dict[str, Any], data))
    except FileNotFoundError:
        pytest.fail(f"YAML file not found: {file_path}")
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML file {file_path}: {e}")


def get_yaml_keys_from_string(yaml_string: str) -> set[str]:
    """Loads YAML from a string and returns a set of all nested keys."""
    try:
        data = yaml.safe_load(yaml_string)
        if not isinstance(data, dict):
            # If loaded data is not a dict (e.g., list, scalar, None), return empty set
            return set()
        # Now data is confirmed to be a dict
        return _extract_keys(data)  # No cast needed here, _extract_keys expects dict
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML string: {e}")


def _extract_keys(data: dict[str, Any], prefix: str = "") -> set[str]:
    """Recursively extracts keys from a nested dictionary."""
    keys: set[str] = set()
    for k, v in data.items():
        full_key = f"{prefix}.{k}" if prefix else k
        keys.add(full_key)
        if isinstance(v, dict):
            keys.update(_extract_keys(v, full_key))
    return keys


def extract_config_content_variable(script_path: Path) -> str | None:
    """Extracts the value of the config_content variable from the Python script."""
    try:
        with open(script_path) as f:
            script_content = f.read()

        # Find the start of the assignment (multi-line mode)
        # Looks for 'config_content =' followed by ''' or """
        start_match = re.search(
            r"^\s*config_content\s*=\s*(\"\"\"|''')", script_content, re.MULTILINE
        )
        if not start_match:
            print(f"Warning: Could not find start of config_content assignment in {script_path}")
            return None

        quote_type = start_match.group(1)  # Get the quote type used (''' or """)
        start_index = start_match.end()  # Index after the opening quotes

        # Find the corresponding closing quotes, making sure to escape them for re.search
        # We search in the rest of the string after the opening quotes
        end_match = re.search(re.escape(quote_type), script_content[start_index:], re.MULTILINE)
        if not end_match:
            print(f"Warning: Could not find end of config_content string in {script_path}")
            return None

        # Extract the content between the opening and closing quotes
        end_index = start_index + end_match.start()
        content = script_content[start_index:end_index]
        return content.strip()

    except FileNotFoundError:
        print(f"Warning: Example script not found: {script_path}")
        return None


class TestConfigConsistency:
    """Verify that configuration example files match the main configuration."""

    @pytest.mark.skipif(not CONFIG_PATH.exists(), reason=f"{CONFIG_PATH} not found")
    @pytest.mark.skipif(not EXAMPLE_CONFIG_PATH.exists(), reason=f"{EXAMPLE_CONFIG_PATH} not found")
    def test_config_and_example_match(self) -> None:
        """Ensure config.yaml and config.yaml.example have the same keys."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        example_keys = get_yaml_keys(EXAMPLE_CONFIG_PATH)

        assert actual_keys == example_keys, (
            f"Mismatch between {CONFIG_PATH} and {EXAMPLE_CONFIG_PATH}. \
            Missing in example: {actual_keys - example_keys}. \
            Extra in example: {example_keys - actual_keys}"
        )

    @pytest.mark.skipif(not CONFIG_PATH.exists(), reason=f"{CONFIG_PATH} not found")
    @pytest.mark.skipif(not EXAMPLE_SCRIPT_PATH.exists(), reason=f"{EXAMPLE_SCRIPT_PATH} not found")
    def test_example_script_content_matches(self) -> None:
        """Ensure config_content in examples/config_example.py matches config.yaml."""
        actual_keys = get_yaml_keys(CONFIG_PATH)

        config_content_str = extract_config_content_variable(EXAMPLE_SCRIPT_PATH)
        # Explicitly check for None and fail the test if extraction failed
        if config_content_str is None:
            pytest.fail(
                f"Failed to extract config_content variable from \
                f{EXAMPLE_SCRIPT_PATH}. Check the script."
            )

        # Cast loaded YAML to dict[str, Any] before passing to _extract_keys
        script_data = yaml.safe_load(config_content_str)
        if not isinstance(script_data, dict):
            pytest.fail("YAML string content is not a dictionary.")
        script_keys = _extract_keys(cast(dict[str, Any], script_data))

        assert actual_keys == script_keys, (
            f"Mismatch between {CONFIG_PATH} and config_content in {EXAMPLE_SCRIPT_PATH}. \
            Missing in script: {actual_keys - script_keys}. \
            Extra in script: {script_keys - actual_keys}"
        )

    @pytest.mark.skipif(not EXAMPLE_CONFIG_PATH.exists(), reason=f"{EXAMPLE_CONFIG_PATH} not found")
    @pytest.mark.skipif(not SCHEMA_PATH.exists(), reason=f"{SCHEMA_PATH} not found")
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
                example_config = yaml.safe_load(f)
            if example_config is None:
                example_config = {}
        except Exception as e:
            pytest.fail(f"Failed to load example config {EXAMPLE_CONFIG_PATH}: {e}")

        # Ensure example_config is a dict before validation
        if not isinstance(example_config, dict):
            pytest.fail(
                f"Example config {EXAMPLE_CONFIG_PATH} did not load as a dictionary "
                f"(loaded type: {type(example_config)}). Cannot validate."
            )

        # Validate example against schema
        try:
            jsonschema.validate(instance=example_config, schema=schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Example config {EXAMPLE_CONFIG_PATH} failed validation: {e}")

    @pytest.mark.skipif(not CONFIG_PATH.exists(), reason=f"{CONFIG_PATH} not found")
    @pytest.mark.skipif(not EXAMPLE_CONFIG_PATH.exists(), reason=f"{EXAMPLE_CONFIG_PATH} not found")
    def test_config_and_example_have_same_keys(self) -> None:
        """Verify that config.toml and config.example.toml have the same keys."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        example_keys = get_yaml_keys(EXAMPLE_CONFIG_PATH)

        assert actual_keys == example_keys, (
            f"Mismatch between {CONFIG_PATH} and {EXAMPLE_CONFIG_PATH}. \
            Missing in example: {actual_keys - example_keys}. \
            Extra in example: {example_keys - actual_keys}"
        )

    @pytest.mark.skipif(not CONFIG_PATH.exists(), reason=f"{CONFIG_PATH} not found")
    @pytest.mark.skipif(not EXAMPLE_SCRIPT_PATH.exists(), reason=f"{EXAMPLE_SCRIPT_PATH} not found")
    def test_config_matches_example_script_exchanges(self) -> None:
        """Verify that config.toml has the exchanges mentioned in example_script.py."""
        actual_keys = get_yaml_keys(CONFIG_PATH)
        script_exchanges = get_yaml_keys_from_string(
            extract_config_content_variable(EXAMPLE_SCRIPT_PATH)
        )

        assert actual_keys == script_exchanges, "Config exchanges differ from example script."

    @pytest.mark.skipif(not EXAMPLE_CONFIG_PATH.exists(), reason=f"{EXAMPLE_CONFIG_PATH} not found")
    @pytest.mark.skipif(not SCHEMA_PATH.exists(), reason=f"{SCHEMA_PATH} not found")
    def test_example_config_matches_schema(self) -> None:
        """Verify that config.example.toml matches the schema definition."""
        # Load schema
        try:
            with open(SCHEMA_PATH) as f:
                schema = json.load(f)
        except Exception as e:
            pytest.fail(f"Failed to load schema {SCHEMA_PATH}: {e}")

        # Load example config
        try:
            with open(EXAMPLE_CONFIG_PATH) as f:
                example_config = yaml.safe_load(f)
            if example_config is None:
                example_config = {}
        except Exception as e:
            pytest.fail(f"Failed to load example config {EXAMPLE_CONFIG_PATH}: {e}")

        # Ensure example_config is a dict before validation
        if not isinstance(example_config, dict):
            pytest.fail(
                f"Example config {EXAMPLE_CONFIG_PATH} did not load as a dictionary "
                f"(loaded type: {type(example_config)}). Cannot validate."
            )

        # Validate example against schema
        try:
            jsonschema.validate(instance=example_config, schema=schema)
        except jsonschema.ValidationError as e:
            pytest.fail(f"Example config {EXAMPLE_CONFIG_PATH} failed validation: {e}")
