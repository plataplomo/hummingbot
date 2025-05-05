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
import os
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
PROJECT_ROOT = Path(__file__).parent.parent
CYBERDELTA_DIR = PROJECT_ROOT / "cyberdelta"
EXAMPLES_DIR = PROJECT_ROOT / "examples"

# Configuration paths (as used by the application)
DEFAULT_CONFIG_DIR = CYBERDELTA_DIR / "config"
USER_CONFIG_DIR = Path.home() / ".cyberdelta"
DEFAULT_SECRETS_FILE = USER_CONFIG_DIR / "secrets.yaml"

# Example file paths (within the examples directory)
EXAMPLE_CONFIG_BASE = EXAMPLES_DIR / "config_base.yaml"
EXAMPLE_CONFIG_CYBERDELTA = EXAMPLES_DIR / "config_cyberdelta.yaml"
EXAMPLE_SECRETS = EXAMPLES_DIR / "secrets_example.yaml"

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


def main() -> None:
    """Main function to demonstrate configuration loading."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Configuration system example")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to the configuration file",
        default=os.path.join(os.path.dirname(__file__), "../cyberdelta/config/config.yaml"),
    )
    parser.add_argument(
        "--secrets",
        type=str,
        help="Path to the secrets file",
        default=os.path.join(Path.home(), ".cyberdelta/secrets.yaml"),
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
        print("Example configuration and secrets files created.")
        return

    if args.benchmark:
        run_benchmark(args.config, args.secrets)
        return

    # Initialize configuration managers
    config_path = os.path.abspath(args.config)
    secrets_path = os.path.abspath(args.secrets)

    # Check if config files exist
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        print("Use --create-example to create sample configuration files.")
        return

    if not os.path.exists(secrets_path):
        print(f"Error: Secrets file not found at {secrets_path}")
        print("Use --create-example to create sample configuration files.")
        return

    print(f"Loading configuration from: {config_path}")
    config = Config(config_path=config_path)
    if not config.load():
        print("Failed to load configuration")
        return

    print(f"Loading secrets from: {secrets_path}")
    # Set environment variable for SecretsManager
    os.environ["CYBERDELTA_SECRETS_PATH"] = secrets_path
    # Create secrets manager
    secrets = SecretsManager(secrets_file_path=secrets_path)
    if not secrets.load_secrets():
        print("Failed to load secrets")
        return

    # Display configuration information
    print("\n=== Configuration Information ===")
    print(f"Safe Mode: {config.get('general.safe_mode', False)}")
    print(f"Log Level: {config.get('general.log_level', 'Not Set')}")

    # Display exchanges information
    print("\n=== Exchange Information ===")
    exchanges = config.get("exchanges", {})
    for exchange_name, exchange_config in exchanges.items():
        status = "Enabled" if exchange_config.get("enabled", False) else "Disabled"
        print(f"  - {exchange_name}: {status}")
        print(f"    API Base URL: {exchange_config.get('api_base_url', 'Not Set')}")
        print(f"    WebSocket URL: {exchange_config.get('ws_url', 'Not Set')}")
        print(
            f"    Rate Limit: {exchange_config.get('rate_limit_per_minute', 'Not Set')} per minute"
        )

    # Display strategies information
    strategies = config.get("strategies", {})
    print(f"\n=== Strategies Information ({len(strategies)}) ===")
    for strategy_name, strategy_config in strategies.items():
        status = "Enabled" if strategy_config.get("enabled", False) else "Disabled"
        print(f"  - {strategy_name}: {status}")

        # Display strategy symbols
        symbols = strategy_config.get("symbols", {})
        if symbols:
            print("    Symbols:")
            for symbol_name, symbol_value in symbols.items():
                print(f"      {symbol_name}: {symbol_value}")

        # Display strategy parameters
        params = strategy_config.get("params", {})
        if params:
            print("    Parameters:")
            for param_name, param_value in params.items():
                print(f"      {param_name}: {param_value}")

    # Display risk management information
    print("\n=== Risk Management Information ===")
    risk = config.get("risk", {})

    global_risk = risk.get("global", {})
    print("  Global Risk Settings:")
    print(f"    Max Position Size: ${global_risk.get('max_position_usd', 'Not Set')}")
    print(f"    Max Total Exposure: ${global_risk.get('max_total_exposure_usd', 'Not Set')}")
    print(f"    Max Portfolio Leverage: {global_risk.get('max_portfolio_leverage', 'Not Set')}x")

    strategy_risk = risk.get("strategies", {})
    print("  Strategy-Specific Risk Settings:")
    for strategy_name, risk_config in strategy_risk.items():
        print(f"    - {strategy_name}:")
        print(f"      Max Position Size: ${risk_config.get('max_position_usd', 'Not Set')}")
        print(f"      Max Leverage: {risk_config.get('max_leverage', 'Not Set')}x")

    # Display circuit breakers
    print("\n=== Circuit Breakers ===")
    circuit_breakers = config.get("circuit_breakers", {})
    print(f"  Enabled: {circuit_breakers.get('enabled', False)}")

    # Display API information (without exposing secret values)
    print("\n=== API Credentials Status ===")
    for exchange_name in exchanges.keys():
        print(f"  - {exchange_name}")
        # Only show if a key exists, not its value
        has_key = bool(secrets.get(f"exchanges.{exchange_name}.api_key", None))
        has_secret = bool(secrets.get(f"exchanges.{exchange_name}.api_secret", None))
        print(f"    API Key: {'Present' if has_key else 'Missing'}")
        print(f"    API Secret: {'Present' if has_secret else 'Missing'}")

    # --- Display Loaded Config ---
    print("\n--- Final Merged Configuration: ---")
    # Use helper to print, assuming config.data holds the dict
    if config and hasattr(config, "data") and isinstance(config.data, dict):
        _print_dict(config.data)
    else:
        print("(Configuration object is empty or not loaded correctly)")

    # --- Display Loaded Secrets (Keys Only) ---
    print("\n--- Loaded Secrets (Keys Only): ---")
    if (
        secrets and hasattr(secrets, "_secrets") and isinstance(secrets._secrets, dict)
    ):  # Access internal for demo
        for key in secrets._secrets.keys():
            print(f"- {key}")
    else:
        print("(No secrets loaded or secrets object invalid)")

    # --- Example Access ---
    print("\n--- Example Access: ---")
    if config:
        db_host = config.get("database.host", "default_host")
        strategy_threshold = config.get("strategy.funding_rate.min_profit_threshold", 0.001)
        print(f"Database Host: {db_host}")
        print(f"Strategy Threshold: {strategy_threshold}")
    else:
        print("Cannot access config values.")

    if secrets:
        # Use default value if key might be missing
        api_key = secrets.get("exchanges.mock_hl.api_key", "<NOT_SET>")
        print(f"Mock HL API Key: {api_key}")
    else:
        print("Cannot access secrets values.")

    print(f"\nConfig file(s) used: {config.config_files_loaded}")
    print(f"User secrets file used: {secrets.secrets_file_path if secrets else 'None'}")


def create_example_files() -> None:
    """Create example configuration and secrets files in the correct locations."""
    # Define the primary config directory used by the application
    cyberdelta_config_dir = CYBERDELTA_DIR / "config"
    os.makedirs(cyberdelta_config_dir, exist_ok=True)

    # Define the recommended user secrets directory
    user_secrets_dir = USER_CONFIG_DIR
    os.makedirs(user_secrets_dir, exist_ok=True)

    # Example configuration content (ensure this matches the latest structure)
    config_content = """# CyberDeltaEngine Configuration (Example)
# Copy this file to config.yaml and customize it.

# General settings
general:
  log_level: INFO
  safe_mode: true
  state_file: "data/state.json"
  state_backup_directory: "data/state_backups"
  state_save_interval: 300
  state_backup_count: 5

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    ws_url: "wss://api.hyperliquid.xyz/ws"
    rate_limit_per_minute: 120
    symbols:
      BTC: "BTC"
      ETH: "ETH"

  backpack:
    enabled: true
    api_base_url: "https://api.backpack.exchange"
    ws_url: "wss://ws.backpack.exchange"
    rate_limit_per_minute: 120
    symbols:
      BTC: "BTC_USDC"
      ETH: "ETH_USDC"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    long_exchange: "hyperliquid"
    short_exchange: "backpack"
    symbol_long: "BTC"
    symbol_short: "BTC"
    params:
      funding_threshold: 0.0001
      max_price_spread_pct: 0.002
      min_profit_usd: 1.0

# Risk management
risk:
  global:
    max_position_usd: 1000.0
    max_total_exposure_usd: 5000.0

  # --- Simple Sizing Path (Optional Alternative to Kelly) ---
  use_simple_sizing_path: false
  simple_sizing_method: "fixed_fraction"
  simple_fixed_fraction: "0.01"
  simple_fixed_usd_size: "100.0"

# Execution parameters
execution:
  max_slippage_pct: 0.001
  max_retries: 3
  retry_delay_base_sec: 1.0
  settlement_delay: 2.0
  compensation:
    use_limit_orders: true
    limit_price_offset_pct: 0.05

# Safety Systems Configuration
safety_systems:
  circuit_breakers:
    enabled: true
    global_consecutive_failures: 5
    global_reset_timeout_sec: 300
    exchange_consecutive_failures: 3
    exchange_reset_timeout_sec: 180

  position_reconciliation:
    enabled: true
    check_interval_sec: 600
    max_discrepancy_pct: 0.01

  balance_monitoring:
    enabled: true
    check_interval_sec: 300
    min_balance_thresholds_usd:
      hyperliquid: 100.0
      backpack: 100.0

# Monitoring and Notifications
monitoring:
  notifications_enabled: true
  alert_methods: ["log"]
"""

    # Example secrets content
    secrets_content = """# CyberDeltaEngine Secrets Configuration (Example)
# Copy this file to your secrets location (e.g., ~/.cyberdelta/secrets.yaml)
# and add your actual credentials.
# IMPORTANT: DO NOT COMMIT YOUR ACTUAL SECRETS.

exchanges:
  hyperliquid:
    api_key: "YOUR_HYPERLIQUID_API_KEY"
    api_secret: "YOUR_HYPERLIQUID_API_SECRET"
  backpack:
    api_key: "YOUR_BACKPACK_API_KEY"
    api_secret: "YOUR_BACKPACK_API_SECRET"

notifications:
  telegram:
    bot_token: "YOUR_TELEGRAM_BOT_TOKEN"
    chat_id: "YOUR_TELEGRAM_CHAT_ID"
"""

    # --- Create example files ONLY --- #

    # Example config in cyberdelta/config/
    cyberdelta_config_example_path = cyberdelta_config_dir / "config.yaml.example"
    with open(cyberdelta_config_example_path, "w") as f:
        f.write(config_content)
    print(f"Example config created at: {cyberdelta_config_example_path}")

    # Example secrets in cyberdelta/config/
    cyberdelta_secrets_example_path = cyberdelta_config_dir / "secrets.yaml.example"
    with open(cyberdelta_secrets_example_path, "w") as f:
        f.write(secrets_content)
    print(f"Example secrets created at: {cyberdelta_secrets_example_path}")

    # Example secrets in user's home directory
    user_secrets_example_path = user_secrets_dir / "secrets.yaml.example"
    with open(user_secrets_example_path, "w") as f:
        f.write(secrets_content)
    print(f"Example secrets created at: {user_secrets_example_path}")

    # --- Remove creation of actual/root files --- #
    # Removed code that created files in root config/ and actual .yaml files

    print("\nIMPORTANT:")
    print(
        f"1. Review the example config: {cyberdelta_config_example_path}"  # No copy needed
    )
    print(
        f"2. Create/edit your user config: {user_config_dir / 'config.yaml'}"  # Example: config_base.yaml
    )
    print(
        f"3. Copy {user_secrets_example_path} to "
        f"{user_secrets_dir / 'secrets.yaml'} (recommended) or another secure location."
    )
    print("4. Add your actual API keys/secrets to your secrets file.")
    print(
        "5. Ensure 'secrets_file' in your user config.yaml points to your actual "
        "secrets file if not using the default ~/.cyberdelta/secrets.yaml."
    )


def run_benchmark(config_path: Path | str | None, secrets_path: Path | str | None) -> None:
    """Run a simple benchmark of the configuration system."""
    n_iterations = 1000

    print("Running Configuration System Benchmark")
    print("======================================")
    print(f"Config Path: {config_path}")
    print(f"Secrets Path: {secrets_path}")

    # Initialize Config and SecretsManager
    # Convert Path objects to string if necessary for Config/SecretsManager init
    config_path_str = (
        str(config_path) if config_path and isinstance(config_path, Path) else config_path
    )
    secrets_path_str = (
        str(secrets_path) if secrets_path and isinstance(secrets_path, Path) else secrets_path
    )

    # --- Benchmark Instantiation ---
    start_time = time.time()
    # Config likely loads automatically on instantiation
    config = Config(config_path=config_path_str)
    instantiation_time = time.time() - start_time

    start_time = time.time()
    # SecretsManager likely loads automatically on instantiation
    secrets = SecretsManager(secrets_file_path=secrets_path_str)  # Use correct parameter name
    secrets_instantiation_time = time.time() - start_time

    # --- Benchmark Value Access ---

    # Measure config value access time (1000 lookups)
    start_time = time.time()
    for _ in range(n_iterations):
        config.get("strategies.hl_perp_bp_spot.params.funding_threshold")
    config_access_time = time.time() - start_time

    # Measure secrets value access time (1000 lookups)
    start_time = time.time()
    for _ in range(n_iterations):
        secrets.get("exchanges.hyperliquid.api_key")
    secrets_access_time = time.time() - start_time

    # Print results
    print("\nResults:")
    print(f"  Config load time: {instantiation_time:.6f} seconds")
    print(f"  Secrets load time: {secrets_instantiation_time:.6f} seconds")
    print(f"  Config access time (1000 lookups): {config_access_time:.6f} seconds")
    print(f"  Secrets access time (1000 lookups): {secrets_access_time:.6f} seconds")
    print(f"  Average config lookup: {(config_access_time / n_iterations) * 1000000:.2f} ns")
    print(f"  Average secrets lookup: {(secrets_access_time / n_iterations) * 1000000:.2f} ns")


if __name__ == "__main__":
    main()
