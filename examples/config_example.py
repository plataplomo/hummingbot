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
import shutil
import sys
import time
from pathlib import Path
from typing import cast

import structlog

from cyberdelta.config import ConfigurationError, get_app_settings, get_secrets_config
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, SecretsConfig
from cyberdelta.config.structlog_config import get_logger, setup_structlog


logger = get_logger(__name__)

# Get project root assuming the script is run from the project root
# or adjust relative path accordingly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CYBERDELTA_DIR = PROJECT_ROOT / "cyberdelta"
EXAMPLES_DIR = PROJECT_ROOT / "examples"

# Configuration paths (as used by the application)
USER_CONFIG_DIR = Path.home() / ".cyberdelta"  # Define user_config_dir for global use

# Example file paths (within the examples directory)
EXAMPLE_CONFIG_BASE_SOURCE = EXAMPLES_DIR / "config_base.yaml"
EXAMPLE_CONFIG_CYBERDELTA_SOURCE = EXAMPLES_DIR / "config_cyberdelta.yaml"
EXAMPLE_SECRETS_SOURCE = EXAMPLES_DIR / "secrets_example.yaml"

# Constants for display formatting
API_KEY_DISPLAY_SUFFIX_LENGTH = 4  # Show last 4 characters of API key for identification

# --- Helper Functions ---


def _log_dict(d: dict[str, object], indent: int = 0) -> None:
    """Recursively logs a dictionary with indentation for display purposes."""
    for key, value in d.items():
        prefix = "  " * indent + f"{key}:"
        if isinstance(value, dict):
            logger.info(prefix)
            _log_dict(cast("dict[str, object]", value), indent + 1)
        else:
            logger.info("config_value_display", message="%s %s", message_args=(prefix, value))


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
        logger.info(
            "file_copy_success",
            message="Copied example base config to: %s",
            message_args=(project_config_base_dest,),
        )
    else:
        logger.error(
            "file_not_found",
            message="Source file not found: %s",
            message_args=(EXAMPLE_CONFIG_BASE_SOURCE,),
        )

    if EXAMPLE_CONFIG_CYBERDELTA_SOURCE.exists():
        shutil.copy(EXAMPLE_CONFIG_CYBERDELTA_SOURCE, user_config_cyberdelta_dest)
        logger.info(
            "file_copy_success",
            message="Copied example CyberDelta config to: %s",
            message_args=(user_config_cyberdelta_dest,),
        )
    else:
        logger.error(
            "file_not_found",
            message="Source file not found: %s",
            message_args=(EXAMPLE_CONFIG_CYBERDELTA_SOURCE,),
        )

    if EXAMPLE_SECRETS_SOURCE.exists():
        shutil.copy(EXAMPLE_SECRETS_SOURCE, user_secrets_dest)
        logger.info(
            "file_copy_success",
            message="Copied example secrets to: %s",
            message_args=(user_secrets_dest,),
        )
    else:
        logger.error(
            "file_not_found",
            message="Source file not found: %s",
            message_args=(EXAMPLE_SECRETS_SOURCE,),
        )

    logger.info("")
    logger.info("Instructions:")
    logger.info(
        "config_instruction",
        message="1. Review and edit the base configuration: %s",
        message_args=(project_config_base_dest,),
    )
    logger.info(
        "config_instruction",
        message="2. Create/edit your user-specific CyberDelta config: %s",
        message_args=(user_config_cyberdelta_dest,),
    )
    logger.info(
        "config_instruction",
        message="3. IMPORTANT: Edit your secrets file with your API keys: %s",
        message_args=(user_secrets_dest,),
    )
    logger.warning("   NEVER commit your secrets.yaml file to version control.")


def _parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments with config path,
            secrets path, and flags for create-example and benchmark modes.
    """
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
    return parser.parse_args()


def _validate_config_files(config_path: Path, secrets_path: Path) -> None:
    """Validate that configuration files exist."""
    if not config_path.exists():
        logger.error(
            "config_file_not_found",
            message="Configuration file not found at %s",
            message_args=(config_path,),
        )
        logger.info("Consider running with --create-example first.")
        sys.exit(1)

    if not secrets_path.exists():
        logger.error(
            "secrets_file_not_found",
            message="Secrets file not found at %s",
            message_args=(secrets_path,),
        )
        logger.info("Consider running with --create-example first.")
        sys.exit(1)


def _load_configurations() -> tuple[AppSettings, SecretsConfig]:
    """Load application settings and secrets configuration.

    Returns:
        tuple[AppSettings, SecretsConfig]: A tuple containing the loaded
            application settings and secrets configuration.
    """
    logger.info("Loading configuration...")
    try:
        app_settings = get_app_settings()
        logger.info("Configuration loaded successfully.")
    except (ConfigurationError, ValueError, ImportError, OSError) as e:
        logger.exception(
            "config_load_failed",
            message="Failed to load configuration: %s",
            message_args=(e,),
        )
        sys.exit(1)

    logger.info("Loading secrets...")
    try:
        secrets_config = get_secrets_config()
        logger.info("Secrets loaded successfully.")
    except (ConfigurationError, ValueError, ImportError, OSError) as e:
        logger.exception(
            "secrets_load_failed",
            message="Failed to load secrets: %s",
            message_args=(e,),
        )
        sys.exit(1)

    return app_settings, secrets_config


def _display_basic_info(app_settings: AppSettings) -> None:
    """Display basic configuration information."""
    logger.info("\n=== Configuration Information (from AppSettings) ===")
    logger.info(
        "config_display",
        message="Safe Mode: %s",
        message_args=(app_settings.general.safe_mode,),
    )
    logger.info(
        "config_display",
        message="Log Level: %s",
        message_args=(app_settings.general.log_level,),
    )


def _display_exchange_info(app_settings: AppSettings) -> None:
    """Display exchange configuration information."""
    logger.info("\n=== Exchange Information ===")
    exchanges = app_settings.exchanges
    if exchanges:
        for name, exchange_config in exchanges.items():
            status = "Enabled" if exchange_config.enabled else "Disabled"
            logger.info("exchange_status", message="  - %s: %s", message_args=(name, status))
            logger.info(
                "exchange_config",
                message="    API Base URL: %s",
                message_args=(exchange_config.api_base_url_mainnet,),
            )
            logger.info(
                "exchange_config",
                message="    WebSocket URL: %s",
                message_args=(exchange_config.ws_url_mainnet,),
            )
            logger.info(
                "exchange_config",
                message="    Rate Limit: %s per minute",
                message_args=(exchange_config.rate_limit_per_minute,),
            )
    else:
        logger.info("exchange_config_empty", message="No exchange configurations found.")


def _display_strategy_info(app_settings: AppSettings) -> None:
    """Display strategy configuration information."""
    logger.info("\n=== Strategy Configuration ===")
    strategies = app_settings.strategies
    if strategies:
        strategy_config = strategies.hl_perp_bp_spot
        status = "Enabled" if strategy_config.enabled else "Disabled"
        logger.info(
            "strategy_status",
            message="  - HyperLiquid-Backpack Funding Arbitrage: %s",
            message_args=(status,),
        )
        logger.info(
            "strategy_config",
            message="    Symbol Long: %s",
            message_args=(strategy_config.symbol_long,),
        )
        logger.info(
            "strategy_config",
            message="    Symbol Short: %s",
            message_args=(strategy_config.symbol_short,),
        )
        logger.info(
            "strategy_config",
            message="    Long Exchange: %s",
            message_args=(strategy_config.long_exchange,),
        )
        logger.info(
            "strategy_config",
            message="    Short Exchange: %s",
            message_args=(strategy_config.short_exchange,),
        )
        logger.info("    Parameters:")
        logger.info(
            "strategy_param",
            message="      Funding Threshold: %s",
            message_args=(strategy_config.params.funding_threshold,),
        )
        logger.info(
            "strategy_param",
            message="      Max Price Spread: %s",
            message_args=(strategy_config.params.max_price_spread_pct,),
        )
        logger.info(
            "strategy_param",
            message="      Min Profit USD: %s",
            message_args=(strategy_config.params.min_profit_usd,),
        )
    else:
        logger.info("strategy_config_empty", message="No strategy configurations found.")


def _display_risk_info(app_settings: AppSettings) -> None:
    """Display risk management configuration."""
    logger.info("\n=== Risk Management Configuration ===")
    risk_config = app_settings.risk
    if risk_config:
        global_risk = risk_config.global_risk
        logger.info(
            "risk_config",
            message="  Max Position USD: %s",
            message_args=(global_risk.max_position_usd,),
        )
        logger.info(
            "risk_config",
            message="  Max Total Exposure USD: %s",
            message_args=(global_risk.max_total_exposure_usd,),
        )

    logger.info("\n=== Circuit Breakers ===")
    circuit_breakers = app_settings.safety_systems.circuit_breakers
    if circuit_breakers:
        logger.info(
            "circuit_breaker_config",
            message="  Enabled: %s",
            message_args=(circuit_breakers.enabled,),
        )
        logger.info(
            "circuit_breaker_config",
            message="  Global Consecutive Failures: %s",
            message_args=(circuit_breakers.global_consecutive_failures,),
        )
        logger.info(
            "circuit_breaker_config",
            message="  Global Reset Timeout: %ss",
            message_args=(circuit_breakers.global_reset_timeout_sec,),
        )
        logger.info(
            "circuit_breaker_config",
            message="  Exchange Consecutive Failures: %s",
            message_args=(circuit_breakers.exchange_consecutive_failures,),
        )
        logger.info(
            "circuit_breaker_config",
            message="  Exchange Reset Timeout: %ss",
            message_args=(circuit_breakers.exchange_reset_timeout_sec,),
        )
    else:
        logger.info(
            "circuit_breaker_config_empty",
            message="No circuit breaker configurations found.",
        )


def _display_exchange_credential_status(exchange_name: str, exchange_secrets: object) -> None:
    """Display credential status for a single exchange."""
    # Check if API key is configured (without showing actual values)
    if hasattr(exchange_secrets, "api_key") and getattr(exchange_secrets, "api_key", None):
        api_key = getattr(exchange_secrets, "api_key", None)
        if isinstance(api_key, str):
            api_key_str = api_key
            masked_key = (
                api_key_str[-API_KEY_DISPLAY_SUFFIX_LENGTH:]
                if len(api_key_str) >= API_KEY_DISPLAY_SUFFIX_LENGTH
                else "****"
            )
            logger.info(
                "credential_status",
                message="    API Key: Set (ending with ...%s)",
                message_args=(masked_key,),
            )
        else:
            logger.info("    API Key: Set (complex structure)")
    else:
        logger.info("    API Key: Not Set")

    # Check api_secret only for ApiKeyAuthSecrets
    if isinstance(exchange_secrets, ApiKeyAuthSecrets):
        if exchange_secrets.api_secret:
            logger.info("    API Secret: Set")
        else:
            logger.info("    API Secret: Not Set")
    elif hasattr(exchange_secrets, "private_key") and getattr(
        exchange_secrets,
        "private_key",
        None,
    ):
        logger.info("    Private Key: Set")
    else:
        logger.info("    Private Key: Not Set")


def _display_credentials_status(app_settings: AppSettings, secrets_config: SecretsConfig) -> None:
    """Display API credentials status without exposing secrets."""
    logger.info("\n=== API Credentials Status (from SecretsConfig) ===")
    exchanges = app_settings.exchanges
    if not exchanges:
        logger.info("Exchange configuration missing, cannot check API credential status.")
        return

    for exchange_name in exchanges:
        logger.info("exchange_credential_check", message="  - %s", message_args=(exchange_name,))
        if exchange_name in secrets_config.exchanges:
            exchange_secrets = secrets_config.exchanges[exchange_name]
            _display_exchange_credential_status(exchange_name, exchange_secrets)
        else:
            logger.info("    No secrets configured for this exchange")

    logger.info("\n=== Individual Secret Retrieval Example ===")
    # Example of retrieving a specific secret using the new system
    if "hyperliquid" in secrets_config.exchanges:
        hyperliquid_secrets = secrets_config.exchanges["hyperliquid"]
        if hasattr(hyperliquid_secrets, "api_key") and getattr(
            hyperliquid_secrets,
            "api_key",
            None,
        ):
            logger.info("Hyperliquid API Key: Loaded successfully")
        else:
            logger.info("Hyperliquid API Key: Not found")
    else:
        logger.info("Hyperliquid exchange secrets: Not configured")


def main() -> None:
    """Main function to demonstrate configuration loading."""
    # Initialize structlog for consistent logging
    try:
        app_settings = get_app_settings()
        setup_structlog(app_settings)
    except (ConfigurationError, ValueError, ImportError, OSError):
        # Fallback to basic setup if config loading fails

        # Use a minimal fallback logging setup for examples
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

    args = _parse_arguments()

    if args.create_example:
        create_example_files()
        return

    config_path = Path(args.config).resolve()
    secrets_path = Path(args.secrets).resolve()

    if args.benchmark:
        run_benchmark(config_path, secrets_path)
        return

    # Validate config files exist
    _validate_config_files(config_path, secrets_path)

    # Load configurations
    app_settings, secrets_config = _load_configurations()

    # Display all configuration information
    _display_basic_info(app_settings)
    _display_exchange_info(app_settings)
    _display_strategy_info(app_settings)
    _display_risk_info(app_settings)
    _display_credentials_status(app_settings, secrets_config)

    # Note: Direct access to secrets should be done through the SecretsConfig model
    # rather than using a generic .get() method


def run_benchmark(config_main_path: Path, secrets_main_path: Path) -> None:
    """Runs a benchmark of the configuration system."""
    logger.info("\n=== Benchmarking Configuration Loading ===")
    logger.info("benchmark_setup", message="Using config: %s", message_args=(config_main_path,))
    logger.info("benchmark_setup", message="Using secrets: %s", message_args=(secrets_main_path,))

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
    logger.info(
        "benchmark_result",
        message="Average time per iteration: %.4f ms (%s iterations)",
        message_args=(avg_time_ms, num_iterations),
    )

    logger.info("\nBenchmark Notes:")
    logger.info("- Times include object instantiation and file I/O.")
    logger.info("- Real-world performance will also depend on config file size and complexity.")
    logger.info(
        "- Configuration system uses Pydantic models for validation and type safety.",
    )


if __name__ == "__main__":
    main()
