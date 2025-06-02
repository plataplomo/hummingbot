#!/usr/bin/env python

"""
Secrets Manager for securely loading API keys and other sensitive information.
This module ensures secrets are stored outside the source code repository.
"""

import logging
import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from .secrets_models import SecretsConfig

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration or secrets loading/validation fails."""

    pass


class SecretsManager:
    """
    Manages loading of secrets from secure location outside source tree.

    This class ensures that sensitive information like API keys and credentials
    are loaded from a secure location outside the Git repository, reducing the
    risk of accidentally committing secrets. Uses Pydantic validation for
    type safety and structure validation. The manager raises ConfigurationError
    on any loading or validation failure.
    """

    def __init__(self, secrets_path: str | None = None) -> None:
        """
        Initialize the SecretsManager.

        Args:
            secrets_path: Optional path to the secrets file.
                         If not provided, default locations will be checked.

        Raises:
            ConfigurationError: If secrets loading or validation fails.
        """
        self.secrets_data: SecretsConfig | None = None
        self.secrets_path = Path(secrets_path) if secrets_path else self._get_secrets_path()
        self.secrets_loaded = False
        # Load secrets on instantiation - will raise ConfigurationError on failure
        self.load()

    def load(self) -> None:
        """
        Load secrets from the configured location and validate against SecretsConfig model.

        Raises:
            ConfigurationError: If file is not found, YAML parsing fails,
                               or Pydantic validation fails.
        """
        # Check if secrets file exists
        if not self.secrets_path.exists():
            logger.critical(f"Secrets file not found: {self.secrets_path}")
            raise ConfigurationError(f"Secrets file not found: {self.secrets_path}")

        try:
            # Read and parse YAML file
            with open(self.secrets_path) as f:
                secrets_data_dict = yaml.safe_load(f)

            # Ensure loaded data is valid
            if secrets_data_dict is None or not isinstance(secrets_data_dict, dict):
                logger.critical(f"Invalid or empty content in secrets file: {self.secrets_path}")
                raise ConfigurationError(
                    f"Invalid or empty content in secrets file: {self.secrets_path}"
                )

        except (yaml.YAMLError, OSError) as e:
            logger.critical(f"Error reading secrets file {self.secrets_path}: {e}", exc_info=True)
            raise ConfigurationError(f"Error reading secrets file {self.secrets_path}: {e}") from e

        # Validate secrets against Pydantic model
        try:
            self.secrets_data = SecretsConfig.model_validate(secrets_data_dict)
            self.secrets_loaded = True
            logger.info(f"SecretsConfig loaded and validated successfully from {self.secrets_path}")

        except ValidationError as e:
            logger.critical(
                f"Secrets validation failed for {self.secrets_path}: {e}", exc_info=True
            )
            self.secrets_data = None
            self.secrets_loaded = False
            raise ConfigurationError(
                f"Invalid secrets configuration in {self.secrets_path}: {e}"
            ) from e

    def _get_secrets_path(self) -> Path:
        """
        Get the path to the secrets file from environment variable or default location.

        Returns:
            Path: The path to the secrets file
        """
        # Try environment variable first
        env_path_str = os.environ.get("CYBERDELTA_SECRETS_PATH")
        if env_path_str:
            env_path = Path(env_path_str).expanduser()
            logger.debug(f"Using secrets path from CYBERDELTA_SECRETS_PATH: {env_path}")
            return env_path

        # Use default location in user's home directory
        default_path = Path.home() / ".cyberdelta" / "secrets.yaml"
        logger.debug(f"CYBERDELTA_SECRETS_PATH not set, using default secrets path: {default_path}")
        return default_path

    def reload(self) -> None:
        """
        Reload secrets from file.

        Raises:
            ConfigurationError: If reload fails.
        """
        self.secrets_data = None
        self.secrets_loaded = False
        self.load()
