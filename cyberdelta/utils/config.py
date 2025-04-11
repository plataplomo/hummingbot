import logging
import os
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration management for the application.

    Provides methods to load configuration from YAML files and environment variables,
    and to access configuration values with dot notation support.
    """

    def __init__(self, config_path_or_data: str | dict[str, Any] = None) -> None:
        """
        Initialize the configuration.

        Args:
            config_path_or_data: Path to YAML configuration file or a configuration dictionary
        """
        self.config_data: dict[str, Any] = {}
        self.config_path = None

        # Load configuration if provided
        if config_path_or_data:
            if isinstance(config_path_or_data, dict):
                # Directly use the provided config dictionary
                self.config_data = config_path_or_data
                logger.info("Loaded configuration from provided dictionary")
            else:
                # Assume it's a path to a config file
                self.load_config(config_path_or_data)

    def load_config(self, config_path: str) -> bool:
        """
        Load configuration from a YAML file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            True if configuration was loaded successfully, False otherwise
        """
        try:
            with open(config_path) as file:
                self.config_data = yaml.safe_load(file) or {}
                self.config_path = config_path
                logger.info(f"Loaded configuration from {config_path}")
                return True
        except Exception as e:
            logger.error(f"Error loading configuration from {config_path}: {str(e)}")
            return False

    def load_env_vars(self, prefix: str = "CYBERDELTA_") -> None:
        """
        Load configuration from environment variables.

        Environment variables should be prefixed with the specified prefix.
        For example, CYBERDELTA_EXCHANGES_HYPERLIQUID_ENABLED=true would set
        exchanges.hyperliquid.enabled to true.

        Args:
            prefix: Prefix for environment variables
        """
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Convert environment variable name to config path
                # e.g., CYBERDELTA_EXCHANGES_HYPERLIQUID_ENABLED -> exchanges.hyperliquid.enabled
                config_path = key[len(prefix) :].lower().replace("_", ".")

                # Convert value to appropriate type
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                elif value.isdigit():
                    value = int(value)
                elif value.replace(".", "", 1).isdigit() and value.count(".") == 1:
                    value = float(value)

                # Set the value
                self.set(config_path, value)
                logger.debug(f"Set configuration {config_path} from environment variable {key}")

    def get(self, key: str, default: Any | None = None) -> Any: 
        """
        Get a configuration value.

        Args:
            key: Configuration key with dot notation (e.g., 'exchanges.hyperliquid.enabled')
            default: Default value to return if key is not found

        Returns:
            Configuration value or default
        """
        parts = key.split(".")
        value = self.config_data

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default

        return value

    def set(self, key: str, value: Any) -> None: 
        """
        Set a configuration value.

        Args:
            key: Configuration key with dot notation (e.g., 'exchanges.hyperliquid.enabled')
            value: Value to set
        """
        parts = key.split(".")
        current = self.config_data

        # Navigate to the correct location
        for _i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # If the path exists but is not a dict, convert it to a dict
                current[part] = {}
            current = current[part]

        # Set the value
        current[parts[-1]] = value

    def save(self, path: str = None) -> bool:
        """
        Save configuration to a YAML file.

        Args:
            path: Path to save configuration to (defaults to the original path)

        Returns:
            True if configuration was saved successfully, False otherwise
        """
        save_path = path or self.config_path

        if not save_path:
            logger.error("No save path specified and no original path available")
            return False

        try:
            with open(save_path, "w") as file:
                yaml.dump(self.config_data, file, default_flow_style=False)
                logger.info(f"Saved configuration to {save_path}")
                return True
        except Exception as e:
            logger.error(f"Error saving configuration to {save_path}: {str(e)}")
            return False

    def merge(self, config_data: dict[str, Any]) -> None:
        """
        Merge configuration data.

        Args:
            config_data: Configuration data to merge
        """
        self._merge_dicts(self.config_data, config_data)

    def _merge_dicts(self, target: dict[str, Any], source: dict[str, Any]) -> None:
        """
        Recursively merge dictionaries.

        Args:
            target: Target dictionary to merge into
            source: Source dictionary to merge from
        """
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._merge_dicts(target[key], value)
            else:
                target[key] = value

    def as_dict(self) -> dict[str, Any]:
        """
        Get the configuration as a dictionary.

        Returns:
            Configuration dictionary
        """
        return self.config_data.copy()


def load_config(config_path: str) -> Config:
    """
    Load configuration from a file.

    Args:
        config_path: Path to configuration file

    Returns:
        Loaded configuration
    """
    config = Config()
    config.load_config(config_path)
    config.load_env_vars()
    return config
