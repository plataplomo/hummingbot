"""Configuration module for CyberDeltaEngine.

This module manages loading and accessing configuration and secrets.
All configuration access is now through validated Pydantic models.
"""

import os
from pathlib import Path

from cyberdelta.exceptions.base import (
    AppSettingsNotLoadedError,
    ConfigurationError,
    ConfigurationInitializationError,
    ConfigurationNotInitializedError,
    SecretsNotLoadedError,
)

from .config_manager import ConfigManager
from .models import AppSettings
from .secrets_manager import SecretsManager
from .secrets_models import SecretsConfig
from .structlog_config import get_logger


logger = get_logger(__name__)


def _get_config_file_path() -> Path:
    """Get the configuration file path from environment or default locations.

    Returns:
        Path to the configuration file, checking environment variable first,
        then standard locations, with fallback to default

    """
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


def _get_secrets_file_path() -> Path:
    """Get the secrets file path from environment or default location.

    Returns:
        Path to the secrets file, checking environment variable first,
        then defaulting to ~/.cyberdelta/secrets.yaml

    """
    env_path_str = os.environ.get("CYBERDELTA_SECRETS_PATH")
    if env_path_str:
        return Path(env_path_str).expanduser()

    # Use default location in user's home directory
    return Path.home() / ".cyberdelta" / "secrets.yaml"


class _ConfigurationState:
    """Internal configuration state management."""

    def __init__(self) -> None:
        self.config_manager: ConfigManager | None = None
        self.secrets_manager: SecretsManager | None = None
        self.app_settings: AppSettings | None = None
        self.secrets_config: SecretsConfig | None = None

    def is_initialized(self) -> bool:
        """Check if configuration is initialized.

        Returns:
            True if configuration is initialized, False otherwise.
        """
        return self.config_manager is not None

    def reset(self) -> None:
        """Reset configuration state for testing."""
        self.config_manager = None
        self.secrets_manager = None
        self.app_settings = None
        self.secrets_config = None


# Configuration state singleton
_config_state = _ConfigurationState()


def _initialize_config() -> None:
    """Initialize configuration managers lazily.

    Raises:
        ConfigurationInitializationError: If configuration system fails to initialize
        AppSettingsNotLoadedError: If app settings cannot be loaded
        SecretsNotLoadedError: If secrets configuration cannot be loaded

    """
    if _config_state.is_initialized():
        return  # Already initialized

    # Check if we're in a testing environment
    if os.environ.get("PYTEST_CURRENT_TEST") or "pytest" in os.environ.get("_", ""):
        logger.debug(
            "testing_environment_detected",
            pytest_test=os.environ.get("PYTEST_CURRENT_TEST") is not None,
            pytest_in_path="pytest" in os.environ.get("_", ""),
            action="skipping_config_initialization",
            message="Testing environment detected, skipping automatic config initialization",
        )
        return

    config_file_path = _get_config_file_path()
    secrets_file_path = _get_secrets_file_path()

    try:
        # Create manager instances (they load automatically and raise ConfigurationError on failure)
        _config_state.config_manager = ConfigManager(str(config_file_path))
        _config_state.secrets_manager = SecretsManager(str(secrets_file_path))
    except ConfigurationError as e:
        logger.critical(
            "configuration_system_initialization_failed",
            config_file_path=str(config_file_path),
            secrets_file_path=str(secrets_file_path),
            error=str(e),
            action="raising_runtime_error",
            message=f"CRITICAL: Configuration system initialization failed: {e}",
        )
        raise ConfigurationInitializationError(str(e), e) from e

    # Validate that configuration loaded successfully
    if _config_state.config_manager.settings is None:
        logger.critical(
            "app_settings_not_loaded",
            config_path=str(_config_state.config_manager.config_path),
            config_loaded=_config_state.config_manager.loaded,
            action="raising_runtime_error",
            message=(
                f"CRITICAL: AppSettings not loaded by ConfigManager from "
                f"{_config_state.config_manager.config_path}. Application cannot proceed safely "
                f"without configuration."
            ),
        )
        raise AppSettingsNotLoadedError(
            config_path=str(_config_state.config_manager.config_path),
            config_loaded=_config_state.config_manager.loaded,
        )

    # Validate that secrets loaded successfully
    if _config_state.secrets_manager.secrets_data is None:
        logger.critical(
            "secrets_config_not_loaded",
            secrets_path=str(_config_state.secrets_manager.secrets_path),
            secrets_loaded=_config_state.secrets_manager.secrets_loaded,
            env_path_set=os.environ.get("CYBERDELTA_SECRETS_PATH") is not None,
            action="raising_runtime_error",
            message=(
                "CRITICAL: SecretsConfig failed to load by SecretsManager "
                "(expected at ~/.cyberdelta/secrets.yaml or via CYBERDELTA_SECRETS_PATH). "
                "Application cannot proceed without secrets."
            ),
        )
        raise SecretsNotLoadedError(
            secrets_path=str(_config_state.secrets_manager.secrets_path),
            secrets_loaded=_config_state.secrets_manager.secrets_loaded,
            env_path_set=os.environ.get("CYBERDELTA_SECRETS_PATH") is not None,
        )

    _config_state.app_settings = _config_state.config_manager.settings
    _config_state.secrets_config = _config_state.secrets_manager.secrets_data


def get_app_settings() -> AppSettings:
    """Get the application settings, initializing if necessary.

    Returns:
        Validated application settings from configuration file

    Raises:
        ConfigurationNotInitializedError: If configuration system is not initialized

    """
    _initialize_config()
    if _config_state.app_settings is None:
        raise ConfigurationNotInitializedError("Configuration")
    return _config_state.app_settings


def get_secrets_config() -> SecretsConfig:
    """Get the secrets configuration, initializing if necessary.

    Returns:
        Validated secrets configuration from secrets file

    Raises:
        ConfigurationNotInitializedError: If secrets system is not initialized

    """
    _initialize_config()
    if _config_state.secrets_config is None:
        raise ConfigurationNotInitializedError("Secrets")
    return _config_state.secrets_config


def get_config_manager() -> ConfigManager:
    """Get the configuration manager, initializing if necessary.

    Returns:
        Configuration manager instance for advanced configuration operations

    Raises:
        ConfigurationNotInitializedError: If configuration system is not initialized

    """
    _initialize_config()
    if _config_state.config_manager is None:
        raise ConfigurationNotInitializedError("Configuration")
    return _config_state.config_manager


def get_secrets_manager() -> SecretsManager:
    """Get the secrets manager, initializing if necessary.

    Returns:
        Secrets manager instance for advanced secrets operations

    Raises:
        ConfigurationNotInitializedError: If secrets system is not initialized

    """
    _initialize_config()
    if _config_state.secrets_manager is None:
        raise ConfigurationNotInitializedError("Secrets")
    return _config_state.secrets_manager


def reset_configuration() -> None:
    """Reset configuration state for testing purposes.

    This function clears all cached configuration and forces reinitialization
    on the next access. Should only be used in testing contexts.
    """
    _config_state.reset()


# Export the primary interfaces for configuration access
__all__ = [
    "AppSettings",  # Export the Pydantic model class
    "ConfigManager",  # Export the manager class for advanced use cases
    "ConfigurationError",  # Export the exception class
    "SecretsConfig",  # Export the Pydantic model class
    "SecretsManager",  # Export the manager class for advanced use cases
    "get_app_settings",  # Primary way to access application settings
    "get_config_manager",  # Access to config manager
    "get_secrets_config",  # Primary way to access secrets
    "get_secrets_manager",  # Access to secrets manager
]
