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
        default=os.path.join(os.path.dirname(__file__), "../config/config.yaml"),
    )
    parser.add_argument(
        "--secrets",
        type=str,
        help="Path to the secrets file",
        default=os.path.join(os.path.dirname(__file__), "../config/secrets.yaml"),
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
    # Create secrets manager
    secrets = SecretsManager()
    # Set CYBERDELTA_SECRETS_PATH environment variable for SecretsManager
    os.environ['CYBERDELTA_SECRETS_PATH'] = secrets_path
    if not secrets.load_secrets():
        print("Failed to load secrets")
        return

    # Display configuration information
    print("\n=== Configuration Information ===")
    print(f"Safe Mode: {config.get('general.safe_mode', False)}")
    print(f"Log Level: {config.get('general.log_level', 'Not Set')}")
    print(f"Timezone: {config.get('general.timezone', 'Not Set')}")
    
    # Display exchanges information
    print("\n=== Exchange Information ===")
    exchanges = config.get('exchanges', {})
    for exchange_name, exchange_config in exchanges.items():
        status = 'Enabled' if exchange_config.get('enabled', False) else 'Disabled'
        print(f"  - {exchange_name}: {status}")
        print(f"    Base URL: {exchange_config.get('base_url', 'Not Set')}")
        print(f"    WebSocket URL: {exchange_config.get('websocket_url', 'Not Set')}")
    
    # Display strategies information
    strategies = config.get('strategies', {})
    print(f"\n=== Strategies Information ({len(strategies)}) ===")
    for strategy_name, strategy_config in strategies.items():
        status = 'Enabled' if strategy_config.get('enabled', False) else 'Disabled'
        print(f"  - {strategy_name}: {status}")
        print(f"    Min Rate Difference: {strategy_config.get('min_rate_difference', 'Not Set')}")
        print(f"    Max Position Size: {strategy_config.get('max_position_size', 'Not Set')}")
        symbols = strategy_config.get('symbols', [])
        print(f"    Symbols: {', '.join(symbols) if symbols else 'None'}")
    
    # Display risk management information
    print("\n=== Risk Management Information ===")
    risk = config.get('risk', {})
    print(f"  Max Drawdown: {risk.get('max_drawdown_percent', 'Not Set')}%")
    print(f"  Max Daily Loss: ${risk.get('max_daily_loss_usd', 'Not Set')}")
    
    global_risk = risk.get('global', {})
    print(f"  Global Risk Settings:")
    print(f"    Max Position Size: ${global_risk.get('max_position_usd', 'Not Set')}")
    print(f"    Max Leverage: {global_risk.get('max_leverage', 'Not Set')}x")
    
    # Display API information (without exposing secret values)
    print("\n=== API Information ===")
    apis = config.get('apis', {})
    for api_name, api_config in apis.items():
        print(f"  - {api_name}")
        # Display base URL
        print(f"    Base URL: {api_config.get('base_url', 'Not Set')}")
        # Only show if a key exists, not its value
        has_key = bool(secrets.get(f'{api_name}_api_key', None))
        has_secret = bool(secrets.get(f'{api_name}_api_secret', None))
        print(f"    API Key: {'Present' if has_key else 'Missing'}")
        print(f"    API Secret: {'Present' if has_secret else 'Missing'}")


def create_example_files():
    """Create example configuration and secrets files."""
    config_dir = os.path.join(os.path.dirname(__file__), "../config")
    os.makedirs(config_dir, exist_ok=True)
    
    example_config_path = os.path.join(config_dir, "config.example.yaml")
    example_secrets_path = os.path.join(config_dir, "secrets.example.yaml")
    
    # Example configuration content
    config_content = """# CyberDelta Configuration

# General settings
general:
  log_level: "INFO"
  timezone: "UTC"
  safe_mode: true

# Exchange configurations
exchanges:
  hyperliquid:
    enabled: true
    base_url: "https://api.hyperliquid.xyz"
    websocket_url: "wss://api.hyperliquid.xyz/ws"
  backpack:
    enabled: true
    base_url: "https://api.backpack.exchange"
    websocket_url: "wss://ws.backpack.exchange"

# API configurations
apis:
  hyperliquid:
    base_url: "https://api.hyperliquid.xyz"
    websocket_url: "wss://api.hyperliquid.xyz/ws"
  backpack:
    base_url: "https://api.backpack.exchange"
    websocket_url: "wss://ws.backpack.exchange"

# Strategy configurations  
strategies:
  funding_rate_arbitrage:
    enabled: true
    min_rate_difference: 0.0001
    max_position_size: 1000
    target_exchanges: ["hyperliquid", "backpack"]
    symbols:
      - "BTC-PERP"
      - "ETH-PERP"

# Risk management settings
risk:
  max_drawdown_percent: 5
  max_daily_loss_usd: 1000
  global:
    max_position_usd: 5000
    max_leverage: 2.0
"""
    
    # Example secrets content
    secrets_content = """# CyberDelta Secrets
# WARNING: This file contains sensitive information. 
# DO NOT share or commit this file to version control.

# API credentials
hyperliquid_api_key: "your_hyperliquid_api_key_here"
hyperliquid_api_secret: "your_hyperliquid_api_secret_here"

backpack_api_key: "your_backpack_api_key_here"
backpack_api_secret: "your_backpack_api_secret_here"
"""
    
    # Write example files
    with open(example_config_path, "w") as f:
        f.write(config_content)
    
    with open(example_secrets_path, "w") as f:
        f.write(secrets_content)
    
    # Also create the actual config and secrets files
    config_path = os.path.join(config_dir, "config.yaml")
    secrets_path = os.path.join(config_dir, "secrets.yaml")
    
    with open(config_path, "w") as f:
        f.write(config_content)
    
    with open(secrets_path, "w") as f:
        f.write(secrets_content)
    
    print(f"Example config created at: {example_config_path}")
    print(f"Example secrets created at: {example_secrets_path}")
    print(f"Config created at: {config_path}")
    print(f"Secrets created at: {secrets_path}")
    print("\nIMPORTANT: Edit secrets.yaml and add your actual API keys")


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
        config.get('strategies.funding_rate_arbitrage.min_rate_difference')
    config_access_time = time.time() - start_time
    
    # Measure secrets value access time (1000 lookups)
    start_time = time.time()
    for _ in range(1000):
        secrets.get('hyperliquid_api_key')
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