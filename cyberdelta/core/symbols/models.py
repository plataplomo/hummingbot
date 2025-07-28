"""Pydantic models for the unified symbol system.

This module provides type-safe models for representing symbols across
different contexts (internal, exchange-specific, unified) with comprehensive
validation and transformation capabilities.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator
from pydantic.config import ConfigDict

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.enums.exchange_names import ExchangeName


# Export MarketType for convenience
__all__ = [
    "BaseSymbol",
    "ExchangeSymbol",
    "ExchangeSymbolType",
    "InternalSymbol",
    "InternalSymbolType",
    "MarketType",
    "SymbolFormat",
    "SymbolType",
    "UnifiedSymbol",
    "UnifiedSymbolType",
    "create_exchange_symbol",
    "create_internal_symbol",
]


# ===========================
# Enums and Configuration
# ===========================


class SymbolType(StrEnum):
    """Types of symbols in the system."""

    INTERNAL = "internal"  # Internal canonical representation
    EXCHANGE = "exchange"  # Exchange-specific representation
    WEBSOCKET = "websocket"  # WebSocket stream representation
    CONFIGURATION = "configuration"  # Configuration file representation


class SymbolFormat:
    """Symbol format configuration and validation patterns."""

    # Unified validation patterns
    PATTERNS: ClassVar[dict[SymbolType, re.Pattern[str]]] = {
        SymbolType.INTERNAL: re.compile(r"^[A-Z0-9_]{2,20}$"),  # Allow underscore for pairs
        SymbolType.EXCHANGE: re.compile(r"^[A-Z0-9_\-@/]{1,20}$"),  # Added / for SPOT pairs
        SymbolType.WEBSOCKET: re.compile(r"^[A-Z0-9_\-]{1,20}$"),
        SymbolType.CONFIGURATION: re.compile(r"^[A-Z0-9_\-/]{1,30}$"),
    }

    # Maximum lengths
    MAX_LENGTHS: ClassVar[dict[SymbolType, int]] = {
        SymbolType.INTERNAL: 20,  # Increased to support pairs like BTC_USDC
        SymbolType.EXCHANGE: 20,
        SymbolType.WEBSOCKET: 20,
        SymbolType.CONFIGURATION: 30,
    }

    # Pattern definitions for backward compatibility
    INTERNAL_PATTERN: ClassVar[re.Pattern[str]] = PATTERNS[SymbolType.INTERNAL]
    EXCHANGE_PATTERN: ClassVar[re.Pattern[str]] = PATTERNS[SymbolType.EXCHANGE]
    ASSET_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[A-Z0-9]{2,10}$")

    # Length constraints
    MIN_SYMBOL_LENGTH: ClassVar[int] = 2
    MAX_SYMBOL_LENGTH: ClassVar[int] = 20
    MAX_ASSET_LENGTH: ClassVar[int] = 10

    # Special characters
    SEPARATOR_CHARS: ClassVar[set[str]] = {"_", "-"}
    HYPERLIQUID_INDEX_PREFIX: ClassVar[str] = "@"

    @classmethod
    def is_valid_internal(cls, symbol: str) -> bool:
        """Check if symbol matches internal format.
        
        Returns:
            True if symbol matches internal format pattern
        """
        return bool(cls.INTERNAL_PATTERN.match(symbol))

    @classmethod
    def is_valid_exchange(cls, symbol: str) -> bool:
        """Check if symbol matches exchange format.
        
        Returns:
            True if symbol matches exchange format pattern
        """
        return bool(cls.EXCHANGE_PATTERN.match(symbol))

    @classmethod
    def is_valid_asset(cls, asset: str) -> bool:
        """Check if asset code is valid.
        
        Returns:
            True if asset code matches valid pattern
        """
        return bool(cls.ASSET_PATTERN.match(asset))


# ===========================
# Base Symbol Model
# ===========================


class BaseSymbol(BaseModel):
    """Base model for all symbol types with common validation."""

    model_config = ConfigDict(
        frozen=True,  # Immutable
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    value: str = Field(..., min_length=2, max_length=20)
    symbol_type: SymbolType = Field(default=SymbolType.INTERNAL)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("value", mode="before")
    @classmethod
    def normalize_value(cls, v: object) -> str:
        """Normalize symbol value handling various input types.
        
        Accepts both string and integer inputs (for Backpack WebSocket compatibility).
        Strings are normalized to uppercase and whitespace is stripped.
        
        Args:
            v: Input value to normalize (string or int)
            
        Returns:
            Normalized symbol value as uppercase string
            
        Raises:
            SymbolValidationError: If input is neither string nor integer
        """
        if isinstance(v, int):
            # Handle integer symbols (Backpack WebSocket case)
            return str(v)
        if isinstance(v, str):
            # Strip whitespace and uppercase
            return v.strip().upper()
        raise SymbolValidationError(
            str(v), f"Invalid value type: {type(v).__name__}", expected_format="string or integer"
        )

    @model_validator(mode="after")
    def validate_format(self) -> BaseSymbol:
        """Validate symbol format against type-specific rules.
        
        Checks the symbol value against regex patterns and length constraints
        defined for each symbol type. Validation is skipped if no pattern is
        defined for the symbol type.
        
        Returns:
            Validated BaseSymbol instance
            
        Raises:
            SymbolValidationError: If symbol format or length is invalid
        """
        pattern = SymbolFormat.PATTERNS.get(self.symbol_type)
        max_length = SymbolFormat.MAX_LENGTHS.get(self.symbol_type)

        if not pattern:
            # If no pattern defined for this type, skip validation
            return self

        if max_length and len(self.value) > max_length:
            raise SymbolValidationError(
                self.value,
                f"Length {len(self.value)} exceeds maximum {max_length}",
                expected_format=f"max {max_length} characters",
            )

        if not pattern.match(self.value):
            raise SymbolValidationError(
                self.value,
                f"Invalid format for type {self.symbol_type.value}",
                expected_format=pattern.pattern,
            )

        return self

    def __str__(self) -> str:
        """String representation.
        
        Returns:
            Symbol value as string
        """
        return self.value

    def __hash__(self) -> int:
        """Hash based on value and type.
        
        Returns:
            Hash of the symbol value and type tuple
        """
        return hash((self.value, self.symbol_type))

    def __eq__(self, other: object) -> bool:
        """Equality comparison.
        
        Returns:
            True if symbols are equal, False otherwise
        """
        if isinstance(other, str):
            return self.value == other.upper()
        if isinstance(other, BaseSymbol):
            return self.value == other.value and self.symbol_type == other.symbol_type
        return False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary with essential fields.
        
        Returns:
            Dictionary containing value, symbol_type, and created_at
        """
        return {
            "value": self.value,
            "symbol_type": self.symbol_type.value,
            "created_at": self.created_at.isoformat(),
        }


# ===========================
# Internal Symbol Model
# ===========================


class InternalSymbol(BaseSymbol):
    """Internal canonical symbol representation.

    Used as the single source of truth for symbol identification
    across the system. Supports both single assets (BTC) and
    trading pairs (BTC_USDT).
    """

    symbol_type: SymbolType = Field(default=SymbolType.INTERNAL, frozen=True)
    base_asset: str = Field(..., pattern=r"^[A-Z0-9]{2,10}$")
    quote_asset: str | None = Field(default=None, pattern=r"^[A-Z0-9]{2,10}$")
    market_type: MarketType = Field(default=MarketType.PERP)

    @model_validator(mode="before")
    @classmethod
    def extract_assets(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Extract base and quote assets from value if not provided.
        
        Returns:
            Updated values dict with extracted base and quote assets
        """
        if "value" in values:
            value = values.get("value", "")

            # Only extract if base_asset not explicitly provided
            if "base_asset" not in values:
                if "_" in value:
                    parts = value.split("_", 1)
                    values["base_asset"] = parts[0]
                    if len(parts) > 1 and "quote_asset" not in values:
                        values["quote_asset"] = parts[1]
                else:
                    values["base_asset"] = value
        return values

    @property
    @computed_field
    def is_pair(self) -> bool:
        """Check if this is a trading pair.
        
        Returns:
            True if symbol has a quote asset (is a pair)
        """
        return self.quote_asset is not None

    @property
    @computed_field
    def canonical_name(self) -> str:
        """Get canonical symbol representation.
        
        Returns:
            Canonical symbol name in format BASE_QUOTE or BASE
        """
        if self.quote_asset:
            return f"{self.base_asset}_{self.quote_asset}"
        return self.base_asset

    def to_pair_format(self) -> str:
        """Convert to pair format for compatibility.
        
        Returns:
            Symbol in pair format (BASE_QUOTE) or single asset format
        """
        if self.quote_asset:
            return f"{self.base_asset}_{self.quote_asset}"
        return self.base_asset

    def matches_asset(self, asset: str) -> bool:
        """Check if symbol contains the specified asset.
        
        Returns:
            True if asset matches base or quote asset
        """
        asset_upper = asset.upper()
        return asset_upper in {self.base_asset, self.quote_asset}


# ===========================
# Exchange Symbol Model
# ===========================


class ExchangeSymbol(BaseSymbol):
    """Exchange-specific symbol representation.

    Handles exchange-specific formats and metadata including:
    - Hyperliquid asset indices (@N format)
    - Backpack integer symbol IDs
    - Exchange-specific naming conventions
    """

    symbol_type: SymbolType = Field(default=SymbolType.EXCHANGE, frozen=True)
    exchange_id: ExchangeName
    internal_symbol: InternalSymbol | None = Field(default=None)

    # Exchange-specific metadata
    asset_index: int | None = Field(default=None, ge=0)  # Hyperliquid
    symbol_id: int | None = Field(default=None, ge=0)  # Backpack

    @property
    @computed_field
    def is_indexed(self) -> bool:
        """Check if symbol uses index notation.
        
        Returns:
            True if symbol has asset_index, symbol_id, or starts with @
        """
        return (
            self.asset_index is not None or self.symbol_id is not None or self.value.startswith("@")
        )

    @property
    @computed_field
    def display_name(self) -> str:
        """Get human-readable display name.
        
        Returns:
            Human-readable symbol name based on internal symbol or value
        """
        if self.internal_symbol:
            if self.internal_symbol.quote_asset:
                return f"{self.internal_symbol.base_asset}_{self.internal_symbol.quote_asset}"
            return self.internal_symbol.base_asset
        return self.value

    def to_websocket_format(self) -> str | int:
        """Convert to WebSocket stream format.
        
        Returns:
            Symbol ID for Backpack or string value for other exchanges
        """
        if self.exchange_id == ExchangeName.BACKPACK and self.symbol_id is not None:
            return self.symbol_id
        return self.value


# ===========================
# Unified Symbol Model
# ===========================


class UnifiedSymbol(BaseModel):
    """Unified symbol with mappings across all exchanges.

    This model represents the complete symbol information including
    all exchange mappings, trading metadata, and validation rules.
    """

    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        extra="forbid",
    )

    # Core identification
    internal: InternalSymbol
    exchange_mappings: dict[str, ExchangeSymbol] = Field(default_factory=dict)

    # Trading metadata
    tick_size: Decimal | None = Field(default=None, gt=0)
    lot_size: Decimal | None = Field(default=None, gt=0)
    min_order_size: Decimal | None = Field(default=None, gt=0)
    max_order_size: Decimal | None = Field(default=None, gt=0)

    # Status
    is_tradeable: bool = Field(default=True)
    is_active: bool = Field(default=True)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_mappings(self) -> UnifiedSymbol:
        """Ensure all exchange mappings reference the same internal symbol.
        
        Validates that all exchange symbols in the mappings dictionary have the
        correct exchange ID and reference the same internal symbol to maintain
        data consistency.
        
        Returns:
            Validated UnifiedSymbol instance
            
        Raises:
            SymbolValidationError: If exchange ID mismatch or internal symbol mismatch
        """
        for exchange_id_str, exchange_symbol in self.exchange_mappings.items():
            exchange_id = ExchangeName(exchange_id_str)
            # Check exchange ID matches
            if exchange_symbol.exchange_id != exchange_id:
                raise SymbolValidationError(
                    exchange_symbol.value,
                    f"Exchange ID mismatch: mapping key is {exchange_id.value} "
                    f"but symbol has exchange_id {exchange_symbol.exchange_id.value}",
                )
            # Check internal symbol matches
            if exchange_symbol.internal_symbol and exchange_symbol.internal_symbol != self.internal:
                raise SymbolValidationError(
                    self.internal.value,
                    f"Internal symbol mismatch: unified has {self.internal.value} "
                    f"but exchange symbol references {exchange_symbol.internal_symbol.value}",
                )
        return self

    def get_exchange_symbol(self, exchange: ExchangeName) -> ExchangeSymbol | None:
        """Get exchange-specific symbol if available.
        
        Returns:
            ExchangeSymbol for the specified exchange or None if not mapped
        """
        return self.exchange_mappings.get(exchange.value)

    def supports_exchange(self, exchange: ExchangeName) -> bool:
        """Check if symbol is supported on exchange.
        
        Returns:
            True if symbol has mapping for the specified exchange
        """
        return exchange.value in self.exchange_mappings

    def get_all_exchange_values(self) -> dict[ExchangeName, str]:
        """Get all exchange symbol values.
        
        Returns:
            Dictionary mapping exchange names to symbol values
        """
        return {
            ExchangeName(exchange): symbol.value
            for exchange, symbol in self.exchange_mappings.items()
        }

    @property
    @computed_field
    def supported_exchanges(self) -> set[ExchangeName]:
        """Get set of supported exchanges.
        
        Returns:
            Set of exchange names that have symbol mappings
        """
        return {ExchangeName(key) for key in self.exchange_mappings}


# ===========================
# Factory Functions
# ===========================


def create_internal_symbol(
    value: str,
    market_type: MarketType = MarketType.PERP,
    base_asset: str | None = None,
    quote_asset: str | None = None,
) -> InternalSymbol:
    """Factory function to create internal symbols.
    
    Returns:
        New InternalSymbol instance with extracted or provided assets
    """
    # Extract base and quote assets if not provided
    if base_asset is None:
        if "_" in value:
            parts = value.split("_", 1)
            base_asset = parts[0]
            if quote_asset is None and len(parts) > 1:
                quote_asset = parts[1]
        else:
            base_asset = value

    return InternalSymbol(
        value=value,
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=market_type,
    )


def create_exchange_symbol(
    value: str | int,
    exchange_id: ExchangeName,
    internal_symbol: InternalSymbol | None = None,
    asset_index: int | None = None,
    symbol_id: int | None = None,
) -> ExchangeSymbol:
    """Factory function to create exchange symbols.
    
    Returns:
        New ExchangeSymbol instance with provided metadata
    """
    return ExchangeSymbol(
        value=str(value),
        exchange_id=exchange_id,
        internal_symbol=internal_symbol,
        asset_index=asset_index,
        symbol_id=symbol_id,
    )


# Type aliases for clarity
InternalSymbolType = InternalSymbol
ExchangeSymbolType = ExchangeSymbol
UnifiedSymbolType = UnifiedSymbol
