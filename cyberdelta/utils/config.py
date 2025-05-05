import logging
import os
from typing import cast

import yaml

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration management for the application.

    Provides methods to load configuration from YAML files and environment variables,
    and to access configuration values with dot notation support.
    """

    def __init__(self, config_path_or_data: str | dict[str, object] | None = None) -> None:
        """
        Initialize the configuration.

        Args:
            config_path_or_data: Path to YAML configuration file or a configuration dictionary
        """
        self.config_data: dict[str, object] = {}
        self.config_path: str | None = None

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
                loaded_data = yaml.safe_load(file)
                self.config_data = loaded_data if loaded_data is not None else {}
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
        for key, value_str in os.environ.items():
            if key.startswith(prefix):
                # Convert environment variable name to config path
                # e.g., CYBERDELTA_EXCHANGES_HYPERLIQUID_ENABLED -> exchanges.hyperliquid.enabled
                config_path = key[len(prefix) :].lower().replace("_", ".")

                # Convert the string value to appropriate type
                typed_value: object

                # Convert value to appropriate type
                if value_str.lower() == "true":
                    typed_value = True
                elif value_str.lower() == "false":
                    typed_value = False
                elif value_str.isdigit():
                    typed_value = int(value_str)
                elif value_str.replace(".", "", 1).isdigit() and value_str.count(".") == 1:
                    typed_value = float(value_str)
                else:
                    # Default to string if no other type matches
                    typed_value = value_str

                # Set the value
                self.set(config_path, typed_value)
                logger.debug(f"Set configuration {config_path} from environment variable {key}")

    def get(self, key: str, default: object | None = None) -> object | None:
        """
        Get a configuration value.

        Args:
            key: Configuration key with dot notation (e.g., 'exchanges.hyperliquid.enabled')
            default: Default value to return if key is not found

        Returns:
            Configuration value or default
        """
        parts = key.split(".")
        value: object = self.config_data

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default

        return value

    def set(self, key: str, value: object) -> None:
        """
        Set a configuration value.

        Args:
            key: Configuration key with dot notation (e.g., 'exchanges.hyperliquid.enabled')
            value: Value to set
        """
        parts = key.split(".")
        current: object = self.config_data

        # Navigate to the correct location
        for _i, part in enumerate(parts[:-1]):
            if not isinstance(current, dict):
                raise TypeError(f"Cannot set key on non-dict object at {'.'.join(parts[:_i])}")
            current_dict = current if isinstance(current, dict) else {}
            if part not in current_dict:
                current_dict[part] = {}
            elif not isinstance(current_dict[part], dict):
                current_dict[part] = {}
            current = current_dict[part]

        if not isinstance(current, dict):
            raise TypeError(f"Cannot set key on non-dict object at {'.'.join(parts[:-1])}")
        current[parts[-1]] = value

    def save(self, path: str | None = None) -> bool:
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

    def merge(self, config_data: dict[str, object]) -> None:
        """
        Merge configuration data.

        Args:
            config_data: Configuration data to merge
        """
        self._merge_dicts(self.config_data, config_data)

    def _merge_dicts(self, target: dict[str, object], source: dict[str, object]) -> None:
        """
        Recursively merge dictionaries.

        Args:
            target: Target dictionary to merge into
            source: Source dictionary to merge from
        """
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                # Cast target[key] to the expected type for the recursive call
                # We know it's a dict due to the isinstance check
                target_dict = cast(dict[str, object], target[key])
                self._merge_dicts(target_dict, value)
            else:
                target[key] = value

    def as_dict(self) -> dict[str, object]:
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
