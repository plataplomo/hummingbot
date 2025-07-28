"""Secrets Manager for securely loading API keys and other sensitive information.

This module ensures secrets are stored outside the source code repository.
"""

import os
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ValidationError

from cyberdelta.exceptions.base import (
    ConfigFileInvalidError,
    ConfigFileNotFoundError,
    ConfigFileReadError,
    ConfigurationError,  # For backward compatibility
    ConfigValidationError,
)

from .secrets_models import SecretsConfig
from .structlog_config import get_logger


logger = get_logger(__name__)


# Alias for backward compatibility - external code may import ConfigurationError from here
__all__ = ["ConfigurationError", "SecretsManager"]


class SecretsManager:
    """Manages loading of secrets from secure location outside source tree.

    This class ensures that sensitive information like API keys and credentials
    are loaded from a secure location outside the Git repository, reducing the
    risk of accidentally committing secrets. Uses Pydantic validation for
    type safety and structure validation. The manager raises ConfigurationError
    on any loading or validation failure.
    """

    def __init__(self, secrets_path: str | None = None) -> None:
        """Initialize the SecretsManager.

        Args:
            secrets_path: Optional path to the secrets file.
                         If not provided, default locations will be checked.

        """
        self.secrets_data: SecretsConfig | None = None
        self.secrets_path = Path(secrets_path) if secrets_path else self._get_secrets_path()
        self.secrets_loaded = False
        # Load secrets on instantiation - will raise ConfigurationError on failure
        self.load()

    def load(self) -> None:
        """Load secrets from the configured location and validate against SecretsConfig model.

        Raises:
            ConfigFileNotFoundError: If the secrets file is not found.
            ConfigFileInvalidError: If the secrets file has invalid content.
            ConfigFileReadError: If there's an error reading the secrets file.
            ConfigValidationError: If Pydantic validation fails.

        """
        # Check if secrets file exists
        if not self.secrets_path.exists():
            logger.critical(
                "secrets_file_not_found",
                secrets_path=str(self.secrets_path),
                action="raising_configuration_error",
                message=f"Secrets file not found: {self.secrets_path}",
            )
            raise ConfigFileNotFoundError(str(self.secrets_path))

        try:
            # Read and parse YAML file
            with self.secrets_path.open(encoding="utf-8") as f:
                secrets_data_dict = yaml.safe_load(f)

            # Ensure loaded data is valid
            if secrets_data_dict is None or not isinstance(secrets_data_dict, dict):
                logger.critical(
                    "secrets_file_invalid_content",
                    secrets_path=str(self.secrets_path),
                    data_type=type(secrets_data_dict).__name__
                    if secrets_data_dict is not None
                    else "None",
                    action="raising_configuration_error",
                    message=f"Invalid or empty content in secrets file: {self.secrets_path}",
                )
                raise ConfigFileInvalidError(
                    str(self.secrets_path),
                    type(secrets_data_dict).__name__ if secrets_data_dict is not None else "None",
                )

        except (yaml.YAMLError, OSError) as e:
            logger.critical(
                "secrets_file_reading_error",
                secrets_path=str(self.secrets_path),
                error_type=type(e).__name__,
                error=str(e),
                action="raising_configuration_error",
                message=f"Error reading secrets file {self.secrets_path}: {e}",
                exc_info=True,
            )
            raise ConfigFileReadError(str(self.secrets_path), e) from e

        # Validate secrets against Pydantic model
        try:
            self.secrets_data = SecretsConfig.model_validate(secrets_data_dict)
            self.secrets_loaded = True
            secrets_data_typed = cast("dict[str, Any]", secrets_data_dict)
            logger.info(
                "secrets_loaded_successfully",
                secrets_path=str(self.secrets_path),
                secrets_sections=list(secrets_data_typed.keys()),
                action="secrets_validated",
                message=f"SecretsConfig loaded and validated successfully from {self.secrets_path}",
            )

        except ValidationError as e:
            logger.critical(
                "secrets_validation_failed",
                secrets_path=str(self.secrets_path),
                validation_errors=e.errors(),
                error_count=len(e.errors()),
                action="raising_configuration_error",
                message=f"Secrets validation failed for {self.secrets_path}: {e}",
                exc_info=True,
            )
            self.secrets_data = None
            self.secrets_loaded = False
            raise ConfigValidationError(str(self.secrets_path), e) from e

    def _get_secrets_path(self) -> Path:
        """Get the path to the secrets file from environment variable or default location.

        Returns:
            Path: The path to the secrets file

        """
        # Try environment variable first
        env_path_str = os.environ.get("CYBERDELTA_SECRETS_PATH")
        if env_path_str:
            env_path = Path(env_path_str).expanduser()
            logger.debug(
                "using_secrets_path_from_env",
                env_variable="CYBERDELTA_SECRETS_PATH",
                secrets_path=str(env_path),
                action="path_resolved",
                message=f"Using secrets path from CYBERDELTA_SECRETS_PATH: {env_path}",
            )
            return env_path

        # Use default location in user's home directory
        default_path = Path.home() / ".cyberdelta" / "secrets.yaml"
        logger.debug(
            "using_default_secrets_path",
            env_variable="CYBERDELTA_SECRETS_PATH",
            env_variable_set=False,
            default_path=str(default_path),
            action="using_default_path",
            message=f"CYBERDELTA_SECRETS_PATH not set, using default secrets path: {default_path}",
        )
        return default_path

    def reload(self) -> None:
        """Reload secrets from file."""
        self.secrets_data = None
        self.secrets_loaded = False
        self.load()
