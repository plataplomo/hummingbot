#!/usr/bin/env python3
"""Tests for the config_example.py script.

These tests ensure that the example script correctly:
1. Creates example configuration files
2. Loads configuration and secrets
3. Displays configuration information
4. Runs benchmarks
"""

import os
import sys
import tempfile
from collections.abc import Generator
from unittest.mock import patch

import pytest

# Add parent directory to path to import from project
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@pytest.fixture
def example_test_setup() -> Generator[tuple[str, str]]:
    """Set up test environment for example script tests."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        example_script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "examples",
            "config_example.py",
        )
        # Make sure the example script exists
        assert os.path.exists(example_script_path), (
            f"Example script not found at {example_script_path}"
        )
        yield temp_dir_name, example_script_path


def test_create_example(example_test_setup: tuple[str, str]) -> None:
    """Test that the script creates example files."""
    temp_dir_name, example_script = example_test_setup
    # Create a temporary directory for the config files
    config_dir = os.path.join(temp_dir_name, "config")
    os.makedirs(config_dir, exist_ok=True)

    # Create a temporary directory for cyberdelta/config
    cyberdelta_config_dir = os.path.join(temp_dir_name, "cyberdelta", "config")
    os.makedirs(cyberdelta_config_dir, exist_ok=True)

    # Set HOME to the temp directory for ~/.cyberdelta
    with patch.dict("os.environ", {"HOME": temp_dir_name}):
        script_dir = os.path.dirname(example_script)

        command = f"cd {script_dir} && python {os.path.basename(example_script)} --create-example"
        exit_code = os.system(command)

        assert exit_code == 0, f"Script failed with exit code {exit_code}"

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        cyberdelta_config_example = os.path.join(
            project_root,
            "cyberdelta",
            "config",
            "config.yaml.example",
        )
        root_config_example = os.path.join(project_root, "config", "config.example.yaml")
        home_config_example = os.path.join(temp_dir_name, ".cyberdelta", "secrets.yaml.example")

        files_created = (
            os.path.exists(cyberdelta_config_example)
            or os.path.exists(root_config_example)
            or os.path.exists(home_config_example)
        )

        assert files_created, "No example files were created in any of the expected locations"


def test_benchmark(example_test_setup: tuple[str, str]) -> None:
    """Test that the benchmark function runs."""
    temp_dir_name, example_script = example_test_setup
    config_path = os.path.join(temp_dir_name, "config.yaml")
    secrets_path = os.path.join(temp_dir_name, "secrets.yaml")

    with open(config_path, "w") as f:
        f.write("""
# General settings
general:
  log_level: INFO
  safe_mode: true

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    ws_url: "wss://api.hyperliquid.xyz/ws"
  backpack:
    enabled: true
    api_base_url: "https://api.backpack.exchange"
    ws_url: "wss://ws.backpack.exchange"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"
    params:
      funding_threshold: 0.0001

# Risk management
risk:
  global:
    max_position_usd: 1000.0
            """)

    with open(secrets_path, "w") as f:
        f.write("""
exchanges:
  hyperliquid:
    api_key: "test_key"
    api_secret: "test_secret"
  backpack:
    api_key: "test_key2"
    api_secret: "test_secret2"
            """)

    script_dir = os.path.dirname(example_script)
    command = (
        f"cd {script_dir} && python {os.path.basename(example_script)} "
        f"--benchmark --config {config_path} --secrets {secrets_path}"
    )
    exit_code = os.system(command)
    assert exit_code == 0, f"Benchmark failed with exit code {exit_code}"


def test_display_config(example_test_setup: tuple[str, str]) -> None:
    """Test that the script displays configuration correctly."""
    temp_dir_name, example_script = example_test_setup
    config_path = os.path.join(temp_dir_name, "config.yaml")
    secrets_path = os.path.join(temp_dir_name, "secrets.yaml")

    with open(config_path, "w") as f:
        f.write("""
# General settings
general:
  log_level: INFO
  safe_mode: true

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"

# Risk management
risk:
  global:
    max_position_usd: 1000.0
            """)

    with open(secrets_path, "w") as f:
        f.write("""
exchanges:
  hyperliquid:
    api_key: "test_api_key"
    api_secret: "test_api_secret"
  backpack:
    api_key: "test_api_key2"
    api_secret: "test_api_secret2"
            """)

    script_dir = os.path.dirname(example_script)
    command = (
        f"cd {script_dir} && python {os.path.basename(example_script)} "
        f"--config {config_path} --secrets {secrets_path}"
    )
    exit_code = os.system(command)
    assert exit_code == 0, f"Display config failed with exit code {exit_code}"
