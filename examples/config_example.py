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
import sys
from pathlib import Path

# Add the parent directory to sys.path for relative imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from cyberdelta.config import ConfigManager, SecretsManager


def main():
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
    config = ConfigManager(config_path)
    if not config.load():
        print("Failed to load configuration")
        return

    print(f"Loading secrets from: {secrets_path}")
    # Set environment variable for SecretsManager
    os.environ["CYBERDELTA_SECRETS_PATH"] = secrets_path
    # Create secrets manager
    secrets = SecretsManager()
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


def create_example_files():
    """Create example configuration and secrets files in the correct locations."""
    # Define the primary config directory used by the application
    cyberdelta_config_dir = Path(__file__).parent.parent / "cyberdelta" / "config"
    os.makedirs(cyberdelta_config_dir, exist_ok=True)

    # Define the recommended user secrets directory
    user_secrets_dir = Path.home() / ".cyberdelta"
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
        f"1. Copy {cyberdelta_config_example_path} to {cyberdelta_config_dir / 'config.yaml'} and customize."
    )
    print(
        f"2. Copy {user_secrets_example_path} to {user_secrets_dir / 'secrets.yaml'} (recommended) or another secure location."
    )
    print("3. Add your actual API keys/secrets to your secrets file.")
    print(
        "4. Ensure the CYBERDELTA_SECRETS_PATH environment variable points to your actual secrets file if not using the default ~/.cyberdelta/secrets.yaml."
    )


def run_benchmark(config_path, secrets_path):
    """Run a simple benchmark of the configuration system."""
    import time

    print("Running Configuration System Benchmark")
    print("======================================")

    # Measure configuration loading time
    start_time = time.time()
    config = ConfigManager(config_path)
    config.load()
    config_load_time = time.time() - start_time

    # Measure secrets loading time
    start_time = time.time()
    secrets = SecretsManager()
    os.environ["CYBERDELTA_SECRETS_PATH"] = secrets_path
    secrets.load_secrets()
    secrets_load_time = time.time() - start_time

    # Measure config value access time (1000 lookups)
    start_time = time.time()
    for _ in range(1000):
        config.get("strategies.hl_perp_bp_spot.params.funding_threshold")
    config_access_time = time.time() - start_time

    # Measure secrets value access time (1000 lookups)
    start_time = time.time()
    for _ in range(1000):
        secrets.get("exchanges.hyperliquid.api_key")
    secrets_access_time = time.time() - start_time

    # Print results
    print("\nResults:")
    print(f"  Config load time: {config_load_time:.6f} seconds")
    print(f"  Secrets load time: {secrets_load_time:.6f} seconds")
    print(f"  Config access time (1000 lookups): {config_access_time:.6f} seconds")
    print(f"  Secrets access time (1000 lookups): {secrets_access_time:.6f} seconds")
    print(f"  Average config lookup: {(config_access_time / 1000) * 1000000:.2f} ns")
    print(f"  Average secrets lookup: {(secrets_access_time / 1000) * 1000000:.2f} ns")


if __name__ == "__main__":
    main()
