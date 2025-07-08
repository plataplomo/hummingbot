"""Type-safe value objects for symbol mapping.

This module provides type aliases and value objects for working with symbols
in a type-safe manner across the trading system.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NewType, final

from pydantic import BaseModel, ConfigDict, Field


# Type aliases for better type safety and clarity
InternalSymbol = NewType("InternalSymbol", str)
ExchangeSymbol = NewType("ExchangeSymbol", str)
ExchangeId = NewType("ExchangeId", str)


@final
@dataclass(frozen=True, slots=True)
class SymbolPair:
    """Immutable representation of a symbol mapping.

    This class represents a validated mapping between an internal symbol
    and its exchange-specific representation.
    """

    internal: InternalSymbol
    exchange: ExchangeSymbol
    exchange_id: ExchangeId

    def __post_init__(self) -> None:
        """Validate fields are not empty."""
        if not self.internal or not self.exchange or not self.exchange_id:
            msg = "All fields must be non-empty"
            raise ValueError(msg)


@final
@dataclass(frozen=True, slots=True)
class SymbolLookupResult:
    """Result of a symbol lookup operation.

    This class encapsulates the result of looking up a symbol,
    including whether it was found and any error information.
    """

    found: bool
    symbol: str | None = None
    error: str | None = None

    @classmethod
    def success(cls, symbol: str) -> SymbolLookupResult:
        """Create successful result."""
        return cls(found=True, symbol=symbol)

    @classmethod
    def not_found(cls, error: str) -> SymbolLookupResult:
        """Create not found result."""
        return cls(found=False, error=error)


class SymbolValidationResult(BaseModel):
    """Result of symbol validation operations.

    This Pydantic model provides detailed information about
    symbol validation including errors and warnings.
    """

    model_config = ConfigDict(frozen=True)

    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    validated_mappings: dict[str, dict[str, str]] | None = None

    @property
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


class SymbolMapping(BaseModel):
    """Value object representing a validated symbol mapping.

    This Pydantic model ensures symbol mappings are properly
    validated at construction time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    internal_symbol: str = Field(
        ...,
        min_length=1,
        max_length=20,
        pattern=r"^[A-Z0-9]+$",
        description="Internal symbol representation (e.g., 'BTC', 'ETH')",
    )
    exchange_symbol: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Exchange-specific symbol (e.g., 'BTC_PERP', 'ETH-USD')",
    )
    exchange_id: str = Field(..., min_length=1, description="Exchange identifier")
