#!/usr/bin/env python

"""Configuration Manager for loading and validating application configuration."""

import os
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ValidationError

from .models.config_models import AppSettings
from .structlog_config import get_logger


logger = get_logger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails."""


class ConfigManager:
    """Manages loading and validation of configuration.

    This class handles loading configuration from file, validates it against
    the AppSettings Pydantic model, and provides access to configuration values
    through the validated model instance. The manager raises ConfigurationError
    on any loading or validation failure.
    """

    def __init__(self, config_path: str | None = None) -> None:
        """Initialize the ConfigManager.

        Args:
            config_path: Optional path to the configuration file.
                         If not provided, default locations will be checked.

        Raises:
            ConfigurationError: If configuration loading or validation fails.

        """
        self.settings: AppSettings | None = None
        self.config_path = Path(config_path) if config_path else self._get_default_config_path()
        self.loaded = False
        # Load configuration on instantiation - will raise ConfigurationError on failure
        try:
            self.load()
        except ConfigurationError:
            # Re-raise ConfigurationError from load() to make it explicit
            raise

    def load(self) -> None:
        """Load configuration from file and validate against AppSettings model.

        Raises:
            ConfigurationError: If file is not found, YAML parsing fails,
                               or Pydantic validation fails.

        """
        # Check if config file exists
        if not self.config_path.exists():
            logger.critical(
                "config_file_not_found",
                config_path=str(self.config_path),
                action="raising_configuration_error",
                message=f"Config file not found: {self.config_path}",
            )
            raise ConfigurationError(f"Config file not found: {self.config_path}")

        try:
            # Read and parse YAML file
            with open(self.config_path) as f:
                config_data_dict = yaml.safe_load(f)

            # Ensure loaded data is valid
            if config_data_dict is None or not isinstance(config_data_dict, dict):
                logger.critical(
                    "config_file_invalid_content",
                    config_path=str(self.config_path),
                    data_type=type(config_data_dict).__name__
                    if config_data_dict is not None
                    else "None",
                    action="raising_configuration_error",
                    message=f"Invalid or empty content in config file: {self.config_path}",
                )
                raise ConfigurationError(
                    f"Invalid or empty content in config file: {self.config_path}",
                )

        except (yaml.YAMLError, OSError) as e:
            logger.critical(
                "config_file_reading_error",
                config_path=str(self.config_path),
                error_type=type(e).__name__,
                error=str(e),
                action="raising_configuration_error",
                message=f"Error reading config file {self.config_path}: {e}",
                exc_info=True,
            )
            raise ConfigurationError(f"Error reading config file {self.config_path}: {e}") from e

        # Validate configuration against Pydantic model
        try:
            self.settings = AppSettings.model_validate(config_data_dict)
            self.loaded = True
            config_data_typed = cast("dict[str, Any]", config_data_dict)
            logger.info(
                "config_loaded_successfully",
                config_path=str(self.config_path),
                config_sections=list(config_data_typed.keys()),
                action="configuration_validated",
                message=f"AppSettings loaded and validated successfully from {self.config_path}",
            )

        except ValidationError as e:
            logger.critical(
                "config_validation_failed",
                config_path=str(self.config_path),
                validation_errors=e.errors(),
                error_count=len(e.errors()),
                action="raising_configuration_error",
                message=f"Application configuration validation failed for {self.config_path}: {e}",
                exc_info=True,
            )
            self.settings = None
            self.loaded = False
            raise ConfigurationError(
                f"Invalid application configuration in {self.config_path}: {e}",
            ) from e

    def _get_default_config_path(self) -> Path:
        """Get default configuration path.

        Checks environment variable and standard locations for config file.

        Returns:
            Path: Path to the configuration file

        """
        # Check environment variable first
        env_path = os.environ.get("CYBERDELTA_CONFIG_PATH")
        if env_path:
            return Path(env_path)

        # Look in standard locations
        cwd = Path.cwd()
        default_paths = [
            cwd / "config.yaml",
            cwd / "config" / "config.yaml",
            Path(__file__).parent / "config.yaml",
        ]

        for path in default_paths:
            if path.exists():
                return path

        return default_paths[0]  # Return first default as fallback

    def reload(self) -> None:
        """Reload configuration from file.

        Raises:
            ConfigurationError: If reload fails.

        """
        self.settings = None
        self.loaded = False
        try:
            self.load()
        except ConfigurationError:
            # Re-raise ConfigurationError from load() to make it explicit
            raise
