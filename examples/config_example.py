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

import os
import sys
import argparse
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
    os.environ['CYBERDELTA_SECRETS_PATH'] = secrets_path
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
    exchanges = config.get('exchanges', {})
    for exchange_name, exchange_config in exchanges.items():
        status = 'Enabled' if exchange_config.get('enabled', False) else 'Disabled'
        print(f"  - {exchange_name}: {status}")
        print(f"    API Base URL: {exchange_config.get('api_base_url', 'Not Set')}")
        print(f"    WebSocket URL: {exchange_config.get('ws_url', 'Not Set')}")
        print(f"    Rate Limit: {exchange_config.get('rate_limit_per_minute', 'Not Set')} per minute")
    
    # Display strategies information
    strategies = config.get('strategies', {})
    print(f"\n=== Strategies Information ({len(strategies)}) ===")
    for strategy_name, strategy_config in strategies.items():
        status = 'Enabled' if strategy_config.get('enabled', False) else 'Disabled'
        print(f"  - {strategy_name}: {status}")
        
        # Display strategy symbols
        symbols = strategy_config.get('symbols', {})
        if symbols:
            print(f"    Symbols:")
            for symbol_name, symbol_value in symbols.items():
                print(f"      {symbol_name}: {symbol_value}")
        
        # Display strategy parameters
        params = strategy_config.get('params', {})
        if params:
            print(f"    Parameters:")
            for param_name, param_value in params.items():
                print(f"      {param_name}: {param_value}")
    
    # Display risk management information
    print("\n=== Risk Management Information ===")
    risk = config.get('risk', {})
    
    global_risk = risk.get('global', {})
    print(f"  Global Risk Settings:")
    print(f"    Max Position Size: ${global_risk.get('max_position_usd', 'Not Set')}")
    print(f"    Max Total Exposure: ${global_risk.get('max_total_exposure_usd', 'Not Set')}")
    print(f"    Max Portfolio Leverage: {global_risk.get('max_portfolio_leverage', 'Not Set')}x")
    
    strategy_risk = risk.get('strategies', {})
    print(f"  Strategy-Specific Risk Settings:")
    for strategy_name, risk_config in strategy_risk.items():
        print(f"    - {strategy_name}:")
        print(f"      Max Position Size: ${risk_config.get('max_position_usd', 'Not Set')}")
        print(f"      Max Leverage: {risk_config.get('max_leverage', 'Not Set')}x")
    
    # Display circuit breakers
    print("\n=== Circuit Breakers ===")
    circuit_breakers = config.get('circuit_breakers', {})
    print(f"  Enabled: {circuit_breakers.get('enabled', False)}")
    
    # Display API information (without exposing secret values)
    print("\n=== API Credentials Status ===")
    for exchange_name in exchanges.keys():
        print(f"  - {exchange_name}")
        # Only show if a key exists, not its value
        has_key = bool(secrets.get(f'exchanges.{exchange_name}.api_key', None))
        has_secret = bool(secrets.get(f'exchanges.{exchange_name}.api_secret', None))
        print(f"    API Key: {'Present' if has_key else 'Missing'}")
        print(f"    API Secret: {'Present' if has_secret else 'Missing'}")


def create_example_files():
    """Create example configuration and secrets files."""
    # Create paths for both locations
    cyberdelta_config_dir = os.path.join(os.path.dirname(__file__), "../cyberdelta/config")
    root_config_dir = os.path.join(os.path.dirname(__file__), "../config")
    
    for config_dir in [cyberdelta_config_dir, root_config_dir]:
        os.makedirs(config_dir, exist_ok=True)
    
    # Example configuration content matching the proper structure
    config_content = """# CyberDeltaEngine Configuration for Prototype 0.0.1

# General settings
general:
  log_level: INFO
  safe_mode: true  # Start in safe mode (read-only)
  state_file: "data/state.json"
  state_backup_directory: "data/state_backups"
  state_save_interval: 300  # seconds
  state_backup_count: 5     # Number of previous state files to keep

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    ws_url: "wss://api.hyperliquid.xyz/ws"
    rate_limit_per_minute: 120 # Placeholder - Actual logic needs per-endpoint handling
    symbols:
      # Map internal symbol names to exchange-specific symbols
      BTC: "BTC" 
      ETH: "ETH"
    
  backpack:
    enabled: true
    api_base_url: "https://api.backpack.exchange"
    ws_url: "wss://ws.backpack.exchange"
    rate_limit_per_minute: 120 # Placeholder - Actual logic needs per-endpoint handling
    symbols:
      BTC: "BTC_USDC" # Example Spot market
      # BTC: "BTC-PERP" # Example Perp market
      ETH: "ETH_USDC"

# Strategy configuration (Simplified for Prototype 0.0.1)
strategies:
  # Focus: HyperLiquid Perpetual vs. Backpack Spot
  hl_perp_bp_spot:
    enabled: true
    # Define symbols using the internal names (mapped above)
    long_exchange: "hyperliquid" # Where we go long (buy perpetual)
    short_exchange: "backpack"  # Where we go short (sell spot)
    symbol_long: "BTC"        # Internal symbol for the long leg
    symbol_short: "BTC"       # Internal symbol for the short leg
    
    params:
      funding_threshold: 0.0001  # Minimum positive funding rate on long leg (HL)
      max_price_spread_pct: 0.002 # Maximum allowed percentage difference between perp and spot price (0.2%)
      min_profit_usd: 1.0      # Minimum estimated profit in USD to consider the trade
      
# Risk management (Simplified for Prototype 0.0.1)
risk:
  global:
    max_position_usd: 1000.0      # Max size per single arbitrage position
    max_total_exposure_usd: 5000.0 # Max total value across all open positions
    
# Execution parameters
execution:
  # Slippage for market orders (if used, Taker orders preferred for prototype)
  max_slippage_pct: 0.001 # 0.1% maximum slippage tolerance
  
  # Retry logic for failed API calls (e.g., temporary network issues)
  max_retries: 3
  retry_delay_base_sec: 1.0 # Initial delay in seconds for exponential backoff

# Safety Systems Configuration
safety_systems:
  # Circuit Breakers to halt trading on excessive failures
  circuit_breakers:
    enabled: true
    # Global breaker (trips if *any* exchange hits its limit)
    global_consecutive_failures: 5 # Trips after 5 consecutive failures globally
    global_reset_timeout_sec: 300  # Reset after 5 minutes if tripped
    
    # Per-exchange breakers
    exchange_consecutive_failures: 3 # Trips after 3 consecutive failures for a specific exchange
    exchange_reset_timeout_sec: 180 # Reset after 3 minutes

  # Position reconciliation checker
  position_reconciliation:
    enabled: true
    check_interval_sec: 600 # Check every 10 minutes
    max_discrepancy_pct: 0.01 # Alert if discrepancy > 0.01% of expected size

  # Balance checker
  balance_monitoring:
    enabled: true
    check_interval_sec: 300 # Check every 5 minutes
    min_balance_thresholds_usd:
      hyperliquid: 100.0
      backpack: 100.0

# Monitoring and Notifications (Simplified placeholders)
monitoring:
  notifications_enabled: true
  alert_methods: ["log"] # Start simple, just log alerts
  # alert_methods: ["log", "telegram"] # Example for later
"""
    
    # Example secrets content matching the proper structure
    secrets_content = """# CyberDeltaEngine Secrets Configuration
# 
# IMPORTANT: DO NOT STORE REAL SECRETS IN THE REPOSITORY
# This is only an example file. Actual secrets should be stored outside the repository at:
# ~/.cyberdelta/secrets.yaml, /etc/cyberdelta/secrets.yaml, or a location specified by the CYBERDELTA_SECRETS_PATH environment variable.

# Exchange credentials
exchanges:
  # HyperLiquid exchange credentials
  hyperliquid:
    api_key: "YOUR_HYPERLIQUID_API_KEY"
    api_secret: "YOUR_HYPERLIQUID_API_SECRET"
    private_key: "YOUR_HYPERLIQUID_PRIVATE_KEY"  # If applicable
    passphrase: "YOUR_HYPERLIQUID_PASSPHRASE"    # If applicable

  # Backpack exchange credentials
  backpack:
    api_key: "YOUR_BACKPACK_API_KEY"
    api_secret: "YOUR_BACKPACK_API_SECRET"
    private_key: "YOUR_BACKPACK_PRIVATE_KEY"  # If applicable
    passphrase: "YOUR_BACKPACK_PASSPHRASE"    # If applicable

# Database credentials
database:
  host: "localhost"
  port: 5432
  username: "db_user"
  password: "db_password"
  database_name: "cyberdelta"

# Notification services
notifications:
  telegram:
    bot_token: "YOUR_TELEGRAM_BOT_TOKEN"
    chat_id: "YOUR_TELEGRAM_CHAT_ID"

  discord:
    webhook_url: "YOUR_DISCORD_WEBHOOK_URL"

# Other service credentials
third_party_services:
  service_name:
    api_key: "YOUR_SERVICE_API_KEY"
    api_secret: "YOUR_SERVICE_API_SECRET"
"""
    
    # Create example files in cyberdelta/config directory (used by the application)
    cyberdelta_config_example_path = os.path.join(cyberdelta_config_dir, "config.yaml.example")
    cyberdelta_secrets_example_path = os.path.join(cyberdelta_config_dir, "secrets.yaml.example")
    
    # Create example files in root config directory
    root_config_example_path = os.path.join(root_config_dir, "config.example.yaml")
    root_secrets_example_path = os.path.join(root_config_dir, "secrets.example.yaml")
    
    # Write example files to cyberdelta/config
    with open(cyberdelta_config_example_path, "w") as f:
        f.write(config_content)
    
    with open(cyberdelta_secrets_example_path, "w") as f:
        f.write(secrets_content)
    
    # Write example files to root/config
    with open(root_config_example_path, "w") as f:
        f.write(config_content)
    
    with open(root_secrets_example_path, "w") as f:
        f.write(secrets_content)
    
    # Create actual config files in cyberdelta/config (main location used by the application)
    cyberdelta_config_path = os.path.join(cyberdelta_config_dir, "config.yaml")
    
    # Create config files in root/config (used by the example script)
    root_config_path = os.path.join(root_config_dir, "config.yaml")
    root_secrets_path = os.path.join(root_config_dir, "secrets.yaml")
    
    # Write actual config files
    with open(cyberdelta_config_path, "w") as f:
        f.write(config_content)
    
    with open(root_config_path, "w") as f:
        f.write(config_content)
    
    with open(root_secrets_path, "w") as f:
        f.write(secrets_content)
    
    # Create a user secrets directory outside the repository (as recommended in the guide)
    home_dir = Path.home()
    user_secrets_dir = home_dir / '.cyberdelta'
    os.makedirs(user_secrets_dir, exist_ok=True)
    
    # Create or update example secrets in the user's home directory
    user_secrets_example_path = user_secrets_dir / 'secrets.yaml.example'
    with open(user_secrets_example_path, "w") as f:
        f.write(secrets_content)
    
    print(f"Example config created at:")
    print(f"  - {cyberdelta_config_example_path}")
    print(f"  - {root_config_example_path}")
    print(f"Example secrets created at:")
    print(f"  - {cyberdelta_secrets_example_path}")
    print(f"  - {root_secrets_example_path}")
    print(f"  - {user_secrets_example_path}")
    print(f"\nActual config files created at:")
    print(f"  - {cyberdelta_config_path}")
    print(f"  - {root_config_path}")
    print(f"  - {root_secrets_path}")
    print("\nIMPORTANT:")
    print("1. Copy secrets.yaml to ~/.cyberdelta/secrets.yaml (recommended secure location)")
    print("2. Add your actual API keys to the secrets file")
    print("3. Set CYBERDELTA_SECRETS_PATH environment variable to your secrets file location")


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
    os.environ['CYBERDELTA_SECRETS_PATH'] = secrets_path
    secrets.load_secrets()
    secrets_load_time = time.time() - start_time
    
    # Measure config value access time (1000 lookups)
    start_time = time.time()
    for _ in range(1000):
        config.get('strategies.hl_perp_bp_spot.params.funding_threshold')
    config_access_time = time.time() - start_time
    
    # Measure secrets value access time (1000 lookups)
    start_time = time.time()
    for _ in range(1000):
        secrets.get('exchanges.hyperliquid.api_key')
    secrets_access_time = time.time() - start_time
    
    # Print results
    print(f"\nResults:")
    print(f"  Config load time: {config_load_time:.6f} seconds")
    print(f"  Secrets load time: {secrets_load_time:.6f} seconds")
    print(f"  Config access time (1000 lookups): {config_access_time:.6f} seconds")
    print(f"  Secrets access time (1000 lookups): {secrets_access_time:.6f} seconds")
    print(f"  Average config lookup: {(config_access_time/1000)*1000000:.2f} ns")
    print(f"  Average secrets lookup: {(secrets_access_time/1000)*1000000:.2f} ns")


if __name__ == "__main__":
    main() 