#!/usr/bin/env python

"""
Secrets Manager for securely loading API keys and other sensitive information.
This module ensures secrets are stored outside the source code repository.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class SecretsManager:
    """
    Manages loading of secrets from secure location outside source tree.

    This class ensures that sensitive information like API keys and credentials
    are loaded from a secure location outside the Git repository, reducing the
    risk of accidentally committing secrets.
    """

    def __init__(self) -> None:
        """Initialize the SecretsManager."""
        self.secrets: dict[str, Any] = {}
        self.secrets_loaded = False

    def load_secrets(self) -> bool:
        """
        Load secrets from the configured location outside the source tree.

        Returns:
            bool: True if secrets were loaded successfully, False otherwise
        """
        # Get secrets path from environment or use default fallbacks
        secrets_path = self._get_secrets_path()

        if not secrets_path.exists():
            logger.warning(f"Secrets file not found at {secrets_path}")
            return False

        try:
            with open(secrets_path) as f:
                self.secrets = yaml.safe_load(f)
            self.secrets_loaded = True
            logger.info(f"Secrets loaded successfully from {secrets_path}")
            return True
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
            return False

    def _get_secrets_path(self) -> Path:
        """
        Get the path to the secrets file from environment variable or default locations.

        Returns:
            Path: The path to the secrets file
        """
        # Try environment variable first
        env_path = os.environ.get("CYBERDELTA_SECRETS_PATH")
        if env_path:
            return Path(env_path)

        # Try default locations in order of preference
        home_dir = Path.home()
        default_paths = [
            home_dir / ".cyberdelta" / "secrets.yaml",
            Path("/etc/cyberdelta/secrets.yaml"),
            Path("/opt/cyberdelta/secrets.yaml"),
        ]

        for path in default_paths:
            if path.exists():
                return path

        # Return the first default path as fallback
        return default_paths[0]

    def get(self, key: str, default: object = None) -> object:
        """
        Get a secret value by key.

        Args:
            key: The secret key to retrieve
            default: Default value to return if key is not found

        Returns:
            The secret value or default
        """
        if not self.secrets_loaded:
            self.load_secrets()

        # Support nested keys with dot notation (e.g., "exchanges.hyperliquid.api_key")
        keys = key.split(".")
        value = self.secrets

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value
