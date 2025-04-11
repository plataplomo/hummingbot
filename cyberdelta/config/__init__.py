"""
Configuration module for CyberDeltaEngine.

This module manages loading and accessing configuration and secrets.
"""

from .config_manager import ConfigManager
from .secrets_manager import SecretsManager

# Create singletons for application-wide use
config = ConfigManager()
secrets = SecretsManager()

__all__ = ["ConfigManager", "SecretsManager", "config", "secrets"]
