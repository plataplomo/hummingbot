"""Testing configuration models.

This module contains configuration ONLY for test environments.
NO mock or test settings should leak into production code.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class TestingSettings(BaseModel):
    """Testing configuration settings - ONLY for test environments."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Test execution settings
    deterministic_mode: bool = Field(
        default=True, description="Use deterministic random seeds for reproducible tests"
    )
    random_seed: int = Field(default=42, description="Random seed for deterministic mode")

    # Test safety settings - these should NEVER be used in production
    disable_external_apis: bool = Field(
        default=True, description="Disable external API calls in test mode"
    )
    disable_persistence: bool = Field(
        default=True, description="Disable state persistence in test mode"
    )
