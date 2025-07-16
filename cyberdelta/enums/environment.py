"""Environment type enumeration.

This module defines the EnvironmentType enum in a neutral location
to avoid circular imports between config and API modules.
"""

from enum import Enum


class EnvironmentType(Enum):
    """Environment type for configuration.

    Replaces the dangerous boolean `is_mainnet_environment` with explicit environment types.
    """

    MAINNET = "mainnet"
    """Production environment with real funds."""

    TESTNET = "testnet"
    """Test environment for development."""

    @property
    def is_production(self) -> bool:
        """Check if this is a production environment."""
        return self == EnvironmentType.MAINNET
