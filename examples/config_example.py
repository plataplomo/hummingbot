#!/usr/bin/env python
"""
Example script demonstrating the usage of the CyberDeltaEngine configuration system.

This script shows how to:
1. Load the configuration and secrets from files
2. Access configuration values using dot notation
3. Create example configuration files

Usage:
    # Run the script using the default configuration paths
    python config_example.py

    # Specify custom paths for configuration files
    python config_example.py --config /path/to/config.yaml --secrets /path/to/secrets.yaml

    # Create example configuration files
    python config_example.py --create-example

The configuration system supports:
- Multiple storage locations (config files, environment variables)
- Validation of required configuration sections
- Secure storage of sensitive information like API keys
- Dot notation access to nested configuration values
"""

import argparse
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from cyberdelta.config.secrets_manager import SecretsManager

# Assuming the script is run from the project root, no need to modify sys.path
# If run from examples/, the relative import might work, but absolute is safer
# Correct imports based on project structure
from cyberdelta.utils.config import Config

# Get project root assuming the script is run from the project root
# or adjust relative path accordingly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CYBERDELTA_DIR = PROJECT_ROOT / "cyberdelta"
EXAMPLES_DIR = PROJECT_ROOT / "examples"

# Configuration paths (as used by the application)
# DEFAULT_CONFIG_DIR = CYBERDELTA_DIR / "config" # Defined but not used in this script directly
USER_CONFIG_DIR = Path.home() / ".cyberdelta"  # Define user_config_dir for global use
# DEFAULT_SECRETS_FILE = USER_CONFIG_DIR / "secrets.yaml" # Defined but not used directly

# Example file paths (within the examples directory)
EXAMPLE_CONFIG_BASE_SOURCE = EXAMPLES_DIR / "config_base.yaml"
EXAMPLE_CONFIG_CYBERDELTA_SOURCE = EXAMPLES_DIR / "config_cyberdelta.yaml"
EXAMPLE_SECRETS_SOURCE = EXAMPLES_DIR / "secrets_example.yaml"

# --- Helper Functions ---


def _print_dict(d: dict[str, Any], indent: int = 0) -> None:
    """Recursively prints a dictionary with indentation."""
    for key, value in d.items():
        print("  " * indent + f"{key}:", end="")
        if isinstance(value, dict):
            print()
            _print_dict(value, indent + 1)
        else:
            print(f" {value}")


def create_example_files() -> None:
    """Creates example configuration files in the appropriate locations."""
    # User-specific directory (e.g., ~/.cyberdelta/)
    USER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Destination paths for user config
    user_config_cyberdelta_dest = USER_CONFIG_DIR / "config_cyberdelta.yaml"
    user_secrets_dest = USER_CONFIG_DIR / "secrets.yaml"

    # Project-level base config (usually committed)
    project_config_base_dest = CYBERDELTA_DIR / "config" / "config_base.yaml"
    project_config_base_dest.parent.mkdir(parents=True, exist_ok=True)

    # Copy example files
    if EXAMPLE_CONFIG_BASE_SOURCE.exists():
        shutil.copy(EXAMPLE_CONFIG_BASE_SOURCE, project_config_base_dest)
        print(f"Copied example base config to: {project_config_base_dest}")
    else:
        print(f"Source file not found: {EXAMPLE_CONFIG_BASE_SOURCE}")

    if EXAMPLE_CONFIG_CYBERDELTA_SOURCE.exists():
        shutil.copy(EXAMPLE_CONFIG_CYBERDELTA_SOURCE, user_config_cyberdelta_dest)
        print(f"Copied example CyberDelta config to: {user_config_cyberdelta_dest}")
    else:
        print(f"Source file not found: {EXAMPLE_CONFIG_CYBERDELTA_SOURCE}")

    if EXAMPLE_SECRETS_SOURCE.exists():
        shutil.copy(EXAMPLE_SECRETS_SOURCE, user_secrets_dest)
        print(f"Copied example secrets to: {user_secrets_dest}")
    else:
        print(f"Source file not found: {EXAMPLE_SECRETS_SOURCE}")

    print("\nInstructions:")
    print(f"1. Review and edit the base configuration: {project_config_base_dest}")
    print(f"2. Create/edit your user-specific CyberDelta config: {user_config_cyberdelta_dest}")
    print(f"3. IMPORTANT: Edit your secrets file with your API keys: {user_secrets_dest}")
    print("   NEVER commit your secrets.yaml file to version control.")


def main() -> None:
    """Main function to demonstrate configuration loading."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Configuration system example")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to the main configuration file (e.g., config_cyberdelta.yaml)",
        default=str(USER_CONFIG_DIR / "config_cyberdelta.yaml"),  # Default to user config
    )
    parser.add_argument(
        "--secrets",
        type=str,
        help="Path to the secrets file",
        default=str(USER_CONFIG_DIR / "secrets.yaml"),  # Default to user secrets
    )
    parser.add_argument(
        "--create-example",
        action="store_true",
        help="Create example config and secrets files",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run a benchmark of the configuration system",
    )
    args = parser.parse_args()

    if args.create_example:
        create_example_files()
        return

    config_path = Path(args.config).resolve()
    secrets_path = Path(args.secrets).resolve()

    if args.benchmark:
        run_benchmark(config_path, secrets_path)
        return

    # Check if config files exist
    if not config_path.exists():
        print(f"Error: Configuration file not found at {config_path}")
        print("Consider running with --create-example first.")
        sys.exit(1)

    if not secrets_path.exists():
        print(f"Error: Secrets file not found at {secrets_path}")
        print("Consider running with --create-example first.")
        sys.exit(1)

    print(f"Loading configuration from: {config_path}")
    # Config expects the path to the *primary* config file (e.g., config_cyberdelta.yaml)
    # It will then load config_base.yaml from the expected relative location.
    config_loader = Config(config_path_or_data=str(config_path))  # Pass string path
    loaded_config = config_loader.as_dict()

    if not loaded_config:  # Assuming get_config() returns None or empty on failure
        print("Failed to load configuration.")
        sys.exit(1)

    print(
        f"Loading secrets from: {secrets_path}"
    )  # secrets_path is for info, SecretsManager finds its own path
    secrets_manager = SecretsManager()
    # Attempt to load secrets; load_secrets() returns bool, errors logged internally
    if not secrets_manager.load_secrets():
        print(
            f"Warning: Secrets could not be loaded. Path used by SecretsManager might be missing or invalid (e.g., {secrets_manager._get_secrets_path()})"
        )
        # loaded_secrets will be an empty dict if loading failed and was attempted
    loaded_secrets = secrets_manager.secrets  # Access the internal dict

    if not loaded_secrets:
        print("Failed to load secrets or no secrets found.")

    # Display configuration information
    print("\n=== Configuration Information (from Config object) ===")
    print(f"Safe Mode: {config_loader.get('general.safe_mode')}")
    print(f"Log Level: {config_loader.get('general.log_level')}")

    print("\n=== Full Loaded Configuration (for demonstration) ===")
    # _print_dict(loaded_config) # Printing the whole dict can be verbose

    # --- Display Exchange Information ---
    print("\n=== Exchange Information ===")
    exchanges = config_loader.get("exchanges", default={})
    if isinstance(exchanges, dict) and exchanges:
        for name, details in exchanges.items():
            if isinstance(details, dict):
                status = "Enabled" if details.get("enabled") else "Disabled"
                print(f"  - {name}: {status}")
                print(f"    API Base URL: {details.get('api_base_url')}")
                print(f"    WebSocket URL: {details.get('ws_url')}")
                rate_limit = details.get("rate_limit_per_minute")
                print(f"    Rate Limit: {rate_limit} per minute")
            else:
                print(f"  - {name}: Invalid config format")  # Should not happen with Pydantic
    else:
        print("No exchange configurations found or invalid format.")

    # --- Display Strategy Configuration ---
    print("\n=== Strategy Configuration ===")
    strategies = config_loader.get("strategies", default={})
    if isinstance(strategies, dict) and strategies:
        for name, details in strategies.items():
            if isinstance(details, dict):
                status = "Enabled" if details.get("enabled") else "Disabled"
                print(f"  - {name}: {status}")
                symbols = details.get("symbols", {})
                if isinstance(symbols, dict) and symbols:
                    print("    Symbols:")
                    for symbol_name, symbol_value in symbols.items():
                        print(f"      {symbol_name}: {symbol_value}")
                params = details.get("params", {})
                if isinstance(params, dict) and params:
                    print("    Parameters:")
                    for param_name, param_value in params.items():
                        print(f"      {param_name}: {param_value}")
            else:
                print(f"  - {name}: Invalid config format")
    else:
        print("No strategy configurations found or invalid format.")

    # --- Display Risk Management Configuration ---
    print("\n=== Risk Management Configuration ===")
    risk_config = config_loader.get("risk", default={})
    if isinstance(risk_config, dict) and risk_config:
        _print_dict(risk_config, indent=2)

    # Display circuit breakers
    print("\n=== Circuit Breakers ===")
    circuit_breakers = config_loader.get("circuit_breakers", default={})
    if isinstance(circuit_breakers, dict):
        print(f"  Enabled: {circuit_breakers.get('enabled')}")
    else:
        print("No circuit breaker configurations found or invalid format.")

    # Display API information (without exposing secret values)
    print("\n=== API Credentials Status (from SecretsManager) ===")
    if isinstance(exchanges, dict):
        for exchange_name in exchanges.keys():
            print(f"  - {exchange_name}")
            api_key = secrets_manager.get(f"exchanges.{exchange_name}.api_key")
            # Example: Check if a sub-key like 'public' exists for some exchanges' API keys
            # This is highly dependent on the actual structure of your secrets
            if isinstance(api_key, dict) and api_key.get("public"):
                print(
                    f"    API Key (Public Part): Set (ending with ...{api_key['public'][-4:] if api_key['public'] and len(api_key['public']) >= 4 else '****'})"
                )
            elif isinstance(api_key, str) and api_key:
                print(
                    f"    API Key: Set (ending with ...{api_key[-4:] if len(api_key) >= 4 else '****'})"
                )
            else:
                print("    API Key: Not Set or invalid format")

            # It's generally not safe to check for other secret parts like 'secret' or 'private_key' here,
            # even just to confirm they are set, as their mere existence can be sensitive.
            # The SecretsManager itself should handle validation of required fields if necessary.
    else:
        print("Exchange configuration missing, cannot check API credential status.")

    print("\n=== Individual Secret Retrieval Example ===")
    # Example of retrieving a specific secret
    hyperliquid_api_key = secrets_manager.get("exchanges.hyperliquid.api_key")
    if hyperliquid_api_key:
        # IMPORTANT: Do not print the actual key in real applications!
        # This is just to show it's loaded. For dict-type keys, access sub-keys.
        if isinstance(hyperliquid_api_key, dict):
            print(
                f"Hyperliquid API Key (Public Part): {hyperliquid_api_key.get('public', 'Not Set')}"
            )
        elif isinstance(hyperliquid_api_key, str):
            print(f"Hyperliquid API Key: Loaded (Value type: {type(hyperliquid_api_key)})")
    else:
        print("Hyperliquid API Key: Not found")

    # Example of retrieving a nested secret
    some_param = secrets_manager.get("some_arbitrary_group.service_x.password")
    if some_param:
        print(f"Some Arbitrary Service X Password: Loaded (Value type: {type(some_param)})")
    else:
        print("Some Arbitrary Service X Password: Not found")


def run_benchmark(config_main_path: Path, secrets_main_path: Path) -> None:
    """Runs a benchmark of the configuration system."""
    print("\n=== Benchmarking Configuration Loading ===")
    print(f"Using config: {config_main_path}")
    print(f"Using secrets: {secrets_main_path}")

    num_iterations = 100
    start_time = time.perf_counter()

    for _ in range(num_iterations):
        # Pass the string path directly to Config constructor
        cfg = Config(config_path_or_data=str(config_main_path))
        # SecretsManager finds its own path based on environment or defaults
        secrets_mgr = SecretsManager()
        if not secrets_mgr.secrets_loaded:  # Ensure they are loaded for benchmark
            secrets_mgr.load_secrets()
        _ = cfg.as_dict()  # Access some data
        _ = secrets_mgr.get("exchanges.hyperliquid.api_key")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    avg_time_ms = (total_time / num_iterations) * 1000
    print(f"Average time per iteration: {avg_time_ms:.4f} ms ({num_iterations} iterations)")

    print("\nBenchmark Notes:")
    print("- Times include object instantiation and file I/O.")
    print("- Real-world performance will also depend on config file size and complexity.")
    print(
        "- SecretsManager may have different performance based on encryption/decryption if implemented."
    )


if __name__ == "__main__":
    main()
