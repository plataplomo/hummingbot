import os
import logging
from typing import Dict, Any, List, Optional

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
# Searches for .env file in the current directory or parent directories
load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config.yaml"

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

class Settings:
    """Loads and holds configuration settings."""

    def __init__(self, config_path: Optional[str] = None):
        """Loads configuration from YAML and environment variables.

        Args:
            config_path: Path to the YAML configuration file. Defaults to DEFAULT_CONFIG_PATH.
        """
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH

        self.config_path = config_path
        self._config: Dict[str, Any] = self._load_yaml_config()
        self._secrets: Dict[str, Optional[str]] = self._load_secrets()

        # --- Directly accessible attributes for common settings ---
        # Strategy Settings
        self.strategy_name: str = self._get_config(["strategy", "name"], required=True)
        self.active_exchanges: List[str] = self._get_config(["strategy", "active_exchanges"], required=True)
        self.symbols: List[str] = self._get_config(["strategy", "symbols"], required=True)

        # API Settings
        self.api_request_timeout: int = self._get_config(["api", "request_timeout"], default=10)
        self.api_retry_delay: int = self._get_config(["api", "retry_delay"], default=1)
        self.api_max_retries: int = self._get_config(["api", "max_retries"], default=5)

        # Logging Settings
        self.log_level: str = self._get_config(["logging", "level"], default="INFO").upper()
        self.log_format: str = self._get_config(["logging", "format"], default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.log_file: Optional[str] = self._get_config(["logging", "log_file"], default="trading_bot.log")
        self.enable_file_logging: bool = self._get_config(["logging", "enable_file_logging"], default=True)

    def _load_yaml_config(self) -> Dict[str, Any]:
        """Loads the configuration from the YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            if config is None:
                return {}
            logger.info(f"Configuration loaded successfully from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise ConfigError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file {self.config_path}: {e}")
            raise ConfigError(f"Error parsing configuration file {self.config_path}: {e}")

    def _load_secrets(self) -> Dict[str, Optional[str]]:
        """Loads secrets from environment variables."""
        secrets = {
            "BACKPACK_API_KEY": os.getenv("BACKPACK_API_KEY"),
            "BACKPACK_API_SECRET": os.getenv("BACKPACK_API_SECRET"),
            "HYPERLIQUID_WALLET_PRIVATE_KEY": os.getenv("HYPERLIQUID_WALLET_PRIVATE_KEY"),
            "PARADEX_WALLET_PRIVATE_KEY": os.getenv("PARADEX_WALLET_PRIVATE_KEY"),
            # Add other secrets as needed
        }
        # Log loaded secrets without exposing the values themselves
        loaded_keys = [key for key, value in secrets.items() if value is not None]
        if loaded_keys:
            logger.info(f"Loaded secrets for: {', '.join(loaded_keys)}")
        missing_keys = [key for key, value in secrets.items() if value is None]
        if missing_keys:
             logger.warning(f"Missing optional secrets: {', '.join(missing_keys)}")
        return secrets

    def _get_config(self, path: List[str], default: Any = None, required: bool = False) -> Any:
        """Helper to get a value from the nested config dictionary."""
        value = self._config
        try:
            for key in path:
                value = value[key]
            return value
        except KeyError:
            if required:
                key_path = '.'.join(path)
                logger.error(f"Missing required configuration key: {key_path}")
                raise ConfigError(f"Missing required configuration key: {key_path} in {self.config_path}")
            return default
        except TypeError: # Handle cases where path goes into a non-dict item
             if required:
                key_path = '.'.join(path)
                logger.error(f"Invalid configuration structure for key: {key_path}")
                raise ConfigError(f"Invalid configuration structure for key: {key_path} in {self.config_path}")
             return default

    def get_strategy_params(self) -> Dict[str, Any]:
        """Returns the parameters specific to the funding rate strategy."""
        return self._get_config(["funding_rate_params"], default={})

    def get_risk_params(self) -> Dict[str, Any]:
        """Returns risk management parameters."""
        return self._get_config(["risk"], default={})

    def get_collateral_params(self) -> Dict[str, Any]:
        """Returns collateral management parameters."""
        return self._get_config(["collateral"], default={})

    def get_exchange_config(self, exchange_name: str) -> Dict[str, Any]:
        """Returns configuration specific to an exchange."""
        return self._get_config(["exchanges", exchange_name], default={})

    def get_secret(self, secret_name: str) -> Optional[str]:
        """Retrieves a secret loaded from environment variables."""
        return self._secrets.get(secret_name)

# Global settings instance (can be imported elsewhere)
try:
    settings = Settings(config_path="strategy_math/coding_strategy/config.yaml")
except ConfigError as e:
    logger.critical(f"Failed to initialize settings: {e}")
    # Decide how to handle fatal config error - exit? raise?
    # For now, create a dummy settings object to avoid import errors elsewhere,
    # but log the critical failure.
    settings = None # Or a dummy object with default/None values
    # raise e # Or exit program

# Example Usage (demonstrates how other modules might use it):
if __name__ == "__main__":
    # Configure basic logging just for this example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if settings:
        logger.info(f"Strategy Name: {settings.strategy_name}")
        logger.info(f"Active Exchanges: {settings.active_exchanges}")
        logger.info(f"Log Level: {settings.log_level}")

        # Get specific parameter groups
        risk_params = settings.get_risk_params()
        logger.info(f"Base VaR Limit: {risk_params.get('base_var_limit')}")

        # Get exchange-specific config
        hl_config = settings.get_exchange_config("hyperliquid")
        logger.info(f"Hyperliquid REST Endpoint: {hl_config.get('rest_endpoint')}")

        # Get a secret (avoid logging the secret itself)
        bp_key = settings.get_secret("BACKPACK_API_KEY")
        if bp_key:
            logger.info("Backpack API Key is set.")
        else:
            logger.warning("Backpack API Key is NOT set.")
    else:
        logger.error("Settings object could not be initialized due to configuration errors.") 