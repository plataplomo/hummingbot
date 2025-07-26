# cyberdelta/config/models/smart_symbol_models.py
"""Smart Symbol Configuration Models.

Implements a streamlined symbol configuration system that reduces configuration
complexity from 108 lines to 13 lines while maintaining full type safety and validation.

This module provides pattern-based symbol configuration that integrates seamlessly
with the existing Pydantic architecture.
"""

from __future__ import annotations

import builtins
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# Import enums directly - avoiding circular import issues
# These enums are leaf dependencies that don't import config models


class SymbolPatterns(BaseModel):
    """Exchange patterns for symbol generation.

    Provides pattern-based symbol mapping for each exchange, supporting
    extensible configuration for new exchanges without breaking changes.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Uses exchange names from existing enums
    hyperliquid: dict[str, str] = Field(..., description="Hyperliquid patterns")
    backpack: dict[str, str] = Field(..., description="Backpack patterns")

    @field_validator("hyperliquid", "backpack")
    @classmethod
    def validate_patterns(cls, v: dict[str, str]) -> dict[str, str]:
        """Validate exchange patterns contain required market types."""
        if "perp" not in v:
            msg = "Exchange patterns must include 'perp' market type"
            raise ValueError(msg)

        # Validate pattern format strings
        for market_type, pattern in v.items():
            # Pydantic already validates pattern is str, so isinstance check not needed
            # Basic validation - pattern should contain symbol placeholder
            if "{symbol}" not in pattern and "{base}" not in pattern:
                msg = (
                    f"Pattern for {market_type} '{pattern}' must contain "
                    "{{symbol}} or {{base}} placeholder"
                )
                raise ValueError(msg)

        return v


class SmartSymbolsConfig(BaseModel):
    """Smart symbol configuration model.

    Provides a concise configuration format that generates the same
    UnifiedSymbolConfig objects as the verbose format, maintaining
    full compatibility with the existing system.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    list: builtins.list[str] = Field(..., description="Symbol list", min_length=1)
    patterns: SymbolPatterns = Field(..., description="Exchange patterns")
    defaults: dict[str, Any] = Field(default_factory=dict, description="Default values")
    overrides: dict[str, dict[str, str]] = Field(
        default_factory=dict, description="Custom overrides"
    )

    @field_validator("list")
    @classmethod
    def validate_symbol_list(cls, v: builtins.list[str]) -> builtins.list[str]:
        """Validate symbol list with basic validation."""
        validated: builtins.list[str] = []
        for symbol in v:
            # Basic symbol validation - alphanumeric, 2-10 chars
            # Pydantic already validates symbol is str, so isinstance check not needed
            symbol_upper = symbol.strip().upper()
            if not symbol_upper:
                msg = "Symbol cannot be empty"
                raise ValueError(msg)

            if not symbol_upper.replace("_", "").isalnum():
                msg = f"Symbol '{symbol_upper}' must be alphanumeric (underscore allowed)"
                raise ValueError(msg)

            min_symbol_length = 2
            max_symbol_length = 10
            if len(symbol_upper) < min_symbol_length or len(symbol_upper) > max_symbol_length:
                msg = (
                    f"Symbol '{symbol_upper}' must be "
                    f"{min_symbol_length}-{max_symbol_length} characters long"
                )
                raise ValueError(msg)

            validated.append(symbol_upper)

        return validated

    @field_validator("defaults")
    @classmethod
    def validate_defaults(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Validate defaults dictionary contains valid values."""
        if "market_type" in v:
            # Basic validation - allow common market types
            market_type_str = v["market_type"]
            valid_types = {"SPOT", "PERP", "IPERP", "DATED", "PREDICTION", "RFQ"}
            if market_type_str not in valid_types:
                msg = (
                    f"Invalid market_type '{market_type_str}'. "
                    "Valid types: SPOT, PERP, IPERP, DATED, PREDICTION, RFQ"
                )
                raise ValueError(msg)

        return v

    @field_validator("overrides")
    @classmethod
    def validate_overrides(cls, v: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
        """Validate overrides structure and values."""
        for symbol, overrides in v.items():
            # Pydantic already validates symbol and overrides types
            if not symbol.strip():
                msg = "Override symbols must be non-empty strings"
                raise ValueError(msg)

            for exchange_name, exchange_value in overrides.items():
                # Basic validation - allow common exchange names
                if exchange_name not in {"hyperliquid", "backpack"}:
                    msg = (
                        f"Invalid exchange '{exchange_name}'. "
                        "Valid exchanges: hyperliquid, backpack"
                    )
                    raise ValueError(msg)

                # Validate exchange value is non-empty
                if not exchange_value.strip():
                    msg = f"Exchange value for {symbol}.{exchange_name} must be non-empty string"
                    raise ValueError(msg)

        return v
