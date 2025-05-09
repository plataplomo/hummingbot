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

# Assuming the script is run from the project root, no need to modify sys.path
# If run from examples/, the relative import might work, but absolute is safer
# Correct imports based on project structure
from cyberdelta.utils.config import Config
from cyberdelta.utils.secrets import SecretsManager

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
    config_loader = Config(config_file_path=config_path)
    loaded_config = config_loader.get_config()

    if not loaded_config:  # Assuming get_config() returns None or empty on failure
        print("Failed to load configuration.")
        sys.exit(1)

    print(f"Loading secrets from: {secrets_path}")
    secrets_manager = SecretsManager(secrets_file_path=secrets_path)
    loaded_secrets = secrets_manager.get_secrets()

    if not loaded_secrets:
        print("Failed to load secrets.")
        # Decide if this is a fatal error for the example
        # sys.exit(1)

    # Display configuration information
    print("\n=== Configuration Information (from Config object) ===")
    print(f"Safe Mode: {config_loader.get('general.safe_mode')}")
    print(f"Log Level: {config_loader.get('general.log_level')}")

    print("\n=== Full Loaded Configuration (for demonstration) ===")
    # _print_dict(loaded_config) # Printing the whole dict can be verbose

    print("\n=== Exchange Information ===")
    exchanges = config_loader.get("exchanges", default_value={})
    if isinstance(exchanges, dict):
        for exchange_name, exchange_config in exchanges.items():
            if isinstance(exchange_config, dict):
                status = "Enabled" if exchange_config.get("enabled") else "Disabled"
                print(f"  - {exchange_name}: {status}")
                print(f"    API Base URL: {exchange_config.get('api_base_url')}")
                print(f"    WebSocket URL: {exchange_config.get('ws_url')}")
                rate_limit = exchange_config.get("rate_limit_per_minute")
                print(f"    Rate Limit: {rate_limit} per minute")
            else:
                print(
                    f"  - {exchange_name}: Invalid config format"
                )  # Should not happen with Pydantic
    else:
        print("No exchange configurations found or invalid format.")

    # Display strategies information
    strategies = config_loader.get("strategies", default_value={})
    print(
        f"\n=== Strategies Information ({len(strategies) if isinstance(strategies, dict) else 0}) ==="
    )
    if isinstance(strategies, dict):
        for strategy_name, strategy_config in strategies.items():
            if isinstance(strategy_config, dict):
                status = "Enabled" if strategy_config.get("enabled") else "Disabled"
                print(f"  - {strategy_name}: {status}")
                symbols = strategy_config.get("symbols", {})
                if isinstance(symbols, dict) and symbols:
                    print("    Symbols:")
                    for symbol_name, symbol_value in symbols.items():
                        print(f"      {symbol_name}: {symbol_value}")
                params = strategy_config.get("params", {})
                if isinstance(params, dict) and params:
                    print("    Parameters:")
                    for param_name, param_value in params.items():
                        print(f"      {param_name}: {param_value}")
            else:
                print(f"  - {strategy_name}: Invalid config format")
    else:
        print("No strategy configurations found or invalid format.")

    # Display risk management information
    print("\n=== Risk Management Information ===")
    risk = config_loader.get("risk", default_value={})
    if isinstance(risk, dict):
        global_risk = risk.get("global", {})
        if isinstance(global_risk, dict):
            print("  Global Risk Settings:")
            print(f"    Max Position Size: ${global_risk.get('max_position_usd')}")
            print(f"    Max Total Exposure: ${global_risk.get('max_total_exposure_usd')}")
            print(f"    Max Portfolio Leverage: {global_risk.get('max_portfolio_leverage')}x")

        strategy_risk_settings = risk.get("strategies", {})
        if isinstance(strategy_risk_settings, dict):
            print("  Strategy-Specific Risk Settings:")
            for strategy_name, risk_config in strategy_risk_settings.items():
                if isinstance(risk_config, dict):
                    print(f"    - {strategy_name}:")
                    print(f"      Max Position Size: ${risk_config.get('max_position_usd')}")
                    print(f"      Max Leverage: {risk_config.get('max_leverage')}x")
    else:
        print("No risk configurations found or invalid format.")

    # Display circuit breakers
    print("\n=== Circuit Breakers ===")
    circuit_breakers = config_loader.get("circuit_breakers", default_value={})
    if isinstance(circuit_breakers, dict):
        print(f"  Enabled: {circuit_breakers.get('enabled')}")
    else:
        print("No circuit breaker configurations found or invalid format.")

    # Display API information (without exposing secret values)
    print("\n=== API Credentials Status (from SecretsManager) ===")
    if isinstance(exchanges, dict):
        for exchange_name in exchanges.keys():
            print(f"  - {exchange_name}")
            api_key = secrets_manager.get_secret(f"exchanges.{exchange_name}.api_key")
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
    hyperliquid_api_key = secrets_manager.get_secret("exchanges.hyperliquid.api_key")
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
    some_param = secrets_manager.get_secret("some_arbitrary_group.service_x.password")
    if some_param:
        print(f"Some Arbitrary Service X Password: Loaded (Value type: {type(some_param)})")
    else:
        print("Some Arbitrary Service X Password: Not found")


def run_benchmark(config_main_path: Path, secrets_main_path: Path) -> None:
    """Runs a benchmark of the configuration loading system."""
    print("\n=== Benchmarking Configuration Loading ===")
    print(f"Using config: {config_main_path}")
    print(f"Using secrets: {secrets_main_path}")

    iterations = 1000
    total_config_time = 0
    total_secrets_time = 0

    # Benchmark Config loading
    start_time = time.perf_counter()
    for _ in range(iterations):
        cfg = Config(config_file_path=config_main_path)
        # cfg.load() # In the current Config, load is implicit in get_config or direct access
        _ = cfg.get_config()  # This triggers the actual loading and parsing logic
    end_time = time.perf_counter()
    total_config_time = end_time - start_time
    avg_config_time_ms = (total_config_time / iterations) * 1000
    print(f"Config loading: {avg_config_time_ms:.4f} ms per iteration ({iterations} iterations)")

    # Benchmark Secrets loading
    # Ensure CYBERDELTA_SECRETS_PATH is set for SecretsManager if it relies on it
    # For direct path usage, this is fine.
    start_time = time.perf_counter()
    for _ in range(iterations):
        sec = SecretsManager(secrets_file_path=secrets_main_path)
        # sec.load_secrets() # In current SecretsManager, load is implicit in get_secrets
        _ = sec.get_secrets()  # This triggers the actual loading
    end_time = time.perf_counter()
    total_secrets_time = end_time - start_time
    avg_secrets_time_ms = (total_secrets_time / iterations) * 1000
    print(f"Secrets loading: {avg_secrets_time_ms:.4f} ms per iteration ({iterations} iterations)")

    print("\nBenchmark Notes:")
    print("- Times include object instantiation and file I/O.")
    print("- Real-world performance will also depend on config file size and complexity.")
    print(
        "- SecretsManager may have different performance based on encryption/decryption if implemented."
    )


if __name__ == "__main__":
    main()
