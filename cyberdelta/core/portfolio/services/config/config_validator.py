"""Clean configuration validation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioConfig


class ConfigValidatorService(BaseModel):
    """Validates configuration data only."""

    strict_validation: bool = Field(default=True, description="Enable strict validation mode")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def validate_config(self, config: PortfolioConfig) -> bool:
        """Validate portfolio configuration."""
        # Validate base currency
        if not await self._is_valid_currency(config.base_currency):
            return False

        # All other validations pass for now
        return True

    async def validate_config_dict(self, config_data: dict[str, Any]) -> bool:
        """Validate configuration dictionary before creating PortfolioConfig."""
        # Check required fields
        if "base_currency" not in config_data:
            return False

        # Check field types
        if not isinstance(config_data["base_currency"], str):
            return False

        return True

    async def check_config_completeness(self, config: PortfolioConfig) -> bool:
        """Check if configuration is complete with all recommended settings."""
        # Basic completeness check
        return config.base_currency != ""

    async def _is_valid_currency(self, currency: str) -> bool:
        """Check if currency code is valid."""
        # Simplified validation - would check against real currency list
        valid_currencies = ["USDC", "USD", "BTC", "ETH", "USDT"]
        return currency.upper() in valid_currencies

    async def _strict_validation_checks(self, config: PortfolioConfig, warnings: list[str]) -> None:
        """Perform additional strict validation checks."""
        # Add strict validation logic here
        if config.base_currency.lower() != config.base_currency.upper():
            warnings.append("Base currency should be uppercase")

        # Add more strict checks as needed
        if hasattr(config, 'max_leverage') and config.max_leverage and config.max_leverage > Decimal(10):
            warnings.append(f"High leverage setting: {config.max_leverage}x may increase risk")