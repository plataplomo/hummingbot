#!/usr/bin/env python
"""Example script demonstrating the usage of the CyberDeltaEngine configuration system.

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
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from cyberdelta.config import get_app_settings, get_secrets_config

# Configure logging for the example
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

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


def _log_dict(d: dict[str, Any], indent: int = 0) -> None:
    """Recursively logs a dictionary with indentation for display purposes."""
    for key, value in d.items():
        prefix = "  " * indent + f"{key}:"
        if isinstance(value, dict):
            logger.info(prefix)
            _log_dict(value, indent + 1)
        else:
            logger.info(f"{prefix} {value}")


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
        logger.info(f"Copied example base config to: {project_config_base_dest}")
    else:
        logger.error(f"Source file not found: {EXAMPLE_CONFIG_BASE_SOURCE}")

    if EXAMPLE_CONFIG_CYBERDELTA_SOURCE.exists():
        shutil.copy(EXAMPLE_CONFIG_CYBERDELTA_SOURCE, user_config_cyberdelta_dest)
        logger.info(f"Copied example CyberDelta config to: {user_config_cyberdelta_dest}")
    else:
        logger.error(f"Source file not found: {EXAMPLE_CONFIG_CYBERDELTA_SOURCE}")

    if EXAMPLE_SECRETS_SOURCE.exists():
        shutil.copy(EXAMPLE_SECRETS_SOURCE, user_secrets_dest)
        logger.info(f"Copied example secrets to: {user_secrets_dest}")
    else:
        logger.error(f"Source file not found: {EXAMPLE_SECRETS_SOURCE}")

    logger.info("")
    logger.info("Instructions:")
    logger.info(f"1. Review and edit the base configuration: {project_config_base_dest}")
    logger.info(
        f"2. Create/edit your user-specific CyberDelta config: {user_config_cyberdelta_dest}",
    )
    logger.info(f"3. IMPORTANT: Edit your secrets file with your API keys: {user_secrets_dest}")
    logger.warning("   NEVER commit your secrets.yaml file to version control.")


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
        logger.error(f"Configuration file not found at {config_path}")
        logger.info("Consider running with --create-example first.")
        sys.exit(1)

    if not secrets_path.exists():
        logger.error(f"Secrets file not found at {secrets_path}")
        logger.info("Consider running with --create-example first.")
        sys.exit(1)

    logger.info(f"Loading configuration from: {config_path}")
    try:
        app_settings = get_app_settings()
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    logger.info(f"Loading secrets from: {secrets_path}")
    try:
        secrets_config = get_secrets_config()
        logger.info("Secrets loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load secrets: {e}")
        sys.exit(1)

    # Display configuration information
    logger.info("\n=== Configuration Information (from AppSettings) ===")
    logger.info(f"Safe Mode: {app_settings.general.safe_mode}")
    logger.info(f"Log Level: {app_settings.general.log_level}")

    logger.info("\n=== Full Loaded Configuration (for demonstration) ===")
    # Can display the full config dict if needed: app_settings.model_dump()

    # --- Display Exchange Information ---
    logger.info("\n=== Exchange Information ===")
    exchanges = app_settings.exchanges
    if exchanges:
        for name, exchange_config in exchanges.items():
            status = "Enabled" if exchange_config.enabled else "Disabled"
            logger.info(f"  - {name}: {status}")
            logger.info(f"    API Base URL: {exchange_config.api_base_url_mainnet}")
            logger.info(f"    WebSocket URL: {exchange_config.ws_url_mainnet}")
            logger.info(f"    Rate Limit: {exchange_config.rate_limit_per_minute} per minute")
    else:
        logger.info("No exchange configurations found.")

    # --- Display Strategy Configuration ---
    logger.info("\n=== Strategy Configuration ===")
    strategies = app_settings.strategies
    if strategies:
        strategy_config = strategies.hl_perp_bp_spot
        status = "Enabled" if strategy_config.enabled else "Disabled"
        logger.info(f"  - HyperLiquid-Backpack Funding Arbitrage: {status}")
        logger.info(f"    Symbol Long: {strategy_config.symbol_long}")
        logger.info(f"    Symbol Short: {strategy_config.symbol_short}")
        logger.info(f"    Long Exchange: {strategy_config.long_exchange}")
        logger.info(f"    Short Exchange: {strategy_config.short_exchange}")
        logger.info("    Parameters:")
        logger.info(f"      Funding Threshold: {strategy_config.params.funding_threshold}")
        logger.info(f"      Max Price Spread: {strategy_config.params.max_price_spread_pct}")
        logger.info(f"      Min Profit USD: {strategy_config.params.min_profit_usd}")
    else:
        logger.info("No strategy configurations found.")

    # --- Display Risk Management Configuration ---
    logger.info("\n=== Risk Management Configuration ===")
    risk_config = app_settings.risk
    if risk_config:
        global_risk = risk_config.global_risk
        logger.info(f"  Max Position Size: {global_risk.max_position_size_pct}%")
        logger.info(f"  Max Daily Loss: {global_risk.max_daily_loss_pct}%")
        logger.info(f"  Stop Loss: {global_risk.stop_loss_pct}%")

    # Display circuit breakers
    logger.info("\n=== Circuit Breakers ===")
    circuit_breakers = app_settings.safety_systems.circuit_breakers
    if circuit_breakers:
        logger.info(f"  Enabled: {circuit_breakers.enabled}")
        logger.info(f"  Max Drawdown: {circuit_breakers.max_drawdown_pct}%")
        logger.info(f"  Max Daily Loss: {circuit_breakers.max_daily_loss_pct}%")
    else:
        logger.info("No circuit breaker configurations found.")

    # Display API information (without exposing secret values)
    logger.info("\n=== API Credentials Status (from SecretsConfig) ===")
    if exchanges:
        for exchange_name in exchanges.keys():
            logger.info(f"  - {exchange_name}")
            if exchange_name in secrets_config.exchanges:
                exchange_secrets = secrets_config.exchanges[exchange_name]
                # Check if API key is configured (without showing actual values)
                if hasattr(exchange_secrets, "api_key") and exchange_secrets.api_key:
                    if isinstance(exchange_secrets.api_key, str):
                        api_key_str = exchange_secrets.api_key
                        masked_key = api_key_str[-4:] if len(api_key_str) >= 4 else "****"
                        logger.info(f"    API Key: Set (ending with ...{masked_key})")
                    else:
                        logger.info("    API Key: Set (complex structure)")
                else:
                    logger.info("    API Key: Not Set")

                if hasattr(exchange_secrets, "api_secret") and exchange_secrets.api_secret:
                    logger.info("    API Secret: Set")
                else:
                    logger.info("    API Secret: Not Set")
            else:
                logger.info("    No secrets configured for this exchange")
    else:
        logger.info("Exchange configuration missing, cannot check API credential status.")

    logger.info("\n=== Individual Secret Retrieval Example ===")
    # Example of retrieving a specific secret using the new system
    if "hyperliquid" in secrets_config.exchanges:
        hyperliquid_secrets = secrets_config.exchanges["hyperliquid"]
        if hasattr(hyperliquid_secrets, "api_key") and hyperliquid_secrets.api_key:
            logger.info("Hyperliquid API Key: Loaded successfully")
        else:
            logger.info("Hyperliquid API Key: Not found")
    else:
        logger.info("Hyperliquid exchange secrets: Not configured")

    # Note: Direct access to secrets should be done through the SecretsConfig model
    # rather than using a generic .get() method


def run_benchmark(config_main_path: Path, secrets_main_path: Path) -> None:
    """Runs a benchmark of the configuration system."""
    logger.info("\n=== Benchmarking Configuration Loading ===")
    logger.info(f"Using config: {config_main_path}")
    logger.info(f"Using secrets: {secrets_main_path}")

    num_iterations = 100
    start_time = time.perf_counter()

    for _ in range(num_iterations):
        # Use the new configuration system
        app_settings = get_app_settings()
        secrets_config = get_secrets_config()
        # Access some data to ensure it's actually loaded
        _ = app_settings.general.safe_mode
        _ = len(secrets_config.exchanges) if secrets_config.exchanges else 0

    end_time = time.perf_counter()
    total_time = end_time - start_time
    avg_time_ms = (total_time / num_iterations) * 1000
    logger.info(f"Average time per iteration: {avg_time_ms:.4f} ms ({num_iterations} iterations)")

    logger.info("\nBenchmark Notes:")
    logger.info("- Times include object instantiation and file I/O.")
    logger.info("- Real-world performance will also depend on config file size and complexity.")
    logger.info(
        "- Configuration system uses Pydantic models for validation and type safety.",
    )


if __name__ == "__main__":
    main()
