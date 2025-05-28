"""
Configuration module for CyberDeltaEngine.

This module manages loading and accessing configuration and secrets.
All configuration access is now through validated Pydantic models.
"""

import logging
import os
from pathlib import Path

from .config_manager import ConfigManager, ConfigurationError
from .config_models import AppSettings
from .secrets_manager import SecretsManager
from .secrets_models import SecretsConfig

logger = logging.getLogger(__name__)


def _get_config_file_path() -> Path:
    """Get the configuration file path from environment or default locations."""
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
    """Get the secrets file path from environment or default location."""
    env_path_str = os.environ.get("CYBERDELTA_SECRETS_PATH")
    if env_path_str:
        return Path(env_path_str)

    # Use default location in user's home directory
    return Path.home() / ".cyberdelta" / "secrets.yaml"


# Initialize configuration and secrets managers
CONFIG_FILE_PATH = _get_config_file_path()
SECRETS_FILE_PATH = _get_secrets_file_path()

try:
    # Create manager instances (they load automatically and raise ConfigurationError on failure)
    config_manager = ConfigManager(str(CONFIG_FILE_PATH))
    secrets_manager = SecretsManager(str(SECRETS_FILE_PATH))
except ConfigurationError as e:
    logger.critical(f"CRITICAL: Configuration system initialization failed: {e}")
    raise RuntimeError(f"Configuration system initialization failed: {e}") from e

# Validate that configuration loaded successfully
if config_manager.settings is None:
    logger.critical(
        f"CRITICAL: AppSettings not loaded by ConfigManager from {config_manager.config_path}. "
        "Application cannot proceed safely without configuration."
    )
    raise RuntimeError("AppSettings failed to load. Check logs for details from ConfigManager.")

# Expose validated Pydantic models for type-safe access
APP_SETTINGS: AppSettings = config_manager.settings

# Validate that secrets loaded successfully
if secrets_manager.secrets_data is None:
    logger.critical(
        "CRITICAL: SecretsConfig failed to load by SecretsManager "
        "(expected at ~/.cyberdelta/secrets.yaml or via CYBERDELTA_SECRETS_PATH). "
        "Application cannot proceed without secrets."
    )
    raise RuntimeError(
        f"SecretsConfig failed to load. Check logs. "
        f"Path used by manager: {secrets_manager.secrets_path}"
    )

SECRETS_CONFIG: SecretsConfig = secrets_manager.secrets_data

# Export the primary interfaces for configuration access
__all__ = [
    "APP_SETTINGS",  # Primary way to access application settings
    "SECRETS_CONFIG",  # Primary way to access secrets
    "AppSettings",  # Export the Pydantic model class
    "SecretsConfig",  # Export the Pydantic model class
    "ConfigManager",  # Export the manager class for advanced use cases
    "SecretsManager",  # Export the manager class for advanced use cases
    "ConfigurationError",  # Export the exception class
]
