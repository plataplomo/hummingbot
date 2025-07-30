# Unified Symbol Model V2 - Context-Aware Design

## Overview

This document presents a revised unified Symbol model that consolidates all symbol functionality into a single context-aware model. Instead of having separate InternalSymbol, ExchangeSymbol, and UnifiedSymbol classes, we use one Symbol class with a `context` field that drives its behavior.

## Key Problems Being Solved

From analyzing the current multi-model architecture, we're solving:

1. **Format validation** - Different patterns for internal (BTC_USD) vs exchange (BTC-PERP, @0)
2. **Asset extraction** - Parsing "BTC_PERP" to get base_asset="BTC"
3. **Type differentiation** - Knowing if it's internal/exchange/websocket context
4. **Cross-reference integrity** - ExchangeSymbol can reference its InternalSymbol
5. **Multi-exchange mapping** - UnifiedSymbol maps one concept to multiple exchange representations
6. **Exchange-specific metadata** - Asset indices, symbol IDs

## Model Implementation

```python
from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator
from pydantic.config import ConfigDict

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.enums.exchange_names import ExchangeName


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


class Symbol(BaseModel):
    """Single symbol model with context-aware behavior."""
    
    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)
    
    # Core identity
    value: str = Field(..., min_length=1, max_length=30, description="Symbol value in current context")
    context: SymbolType = Field(..., description="Context determining behavior and validation")
    
    # Exchange context (required when context=EXCHANGE)
    exchange_id: ExchangeName | None = Field(default=None, description="Exchange when context=EXCHANGE")
    
    # Canonical representation
    internal_value: str = Field(..., description="Canonical internal representation (e.g. BTC_PERP)")
    base_asset: str = Field(..., pattern=r"^[A-Z0-9]{2,10}$", description="Base asset code")
    quote_asset: str | None = Field(default=None, pattern=r"^[A-Z0-9]{2,10}$", description="Quote asset code")
    market_type: MarketType = Field(default=MarketType.PERP, description="Market type")
    
    # Exchange metadata (sparse - only populated when needed)
    asset_index: int | None = Field(default=None, ge=0, description="Hyperliquid asset index")
    symbol_id: int | None = Field(default=None, ge=0, description="Backpack symbol ID")
    
    # Cross-exchange mapping
    exchange_variants: dict[ExchangeName, str] = Field(
        default_factory=dict,
        description="Mapping of exchange to their symbol format"
    )
    
    # Trading parameters
    tick_size: Decimal | None = Field(default=None, gt=0)
    lot_size: Decimal | None = Field(default=None, gt=0)
    min_order_size: Decimal | None = Field(default=None, gt=0)
    max_order_size: Decimal | None = Field(default=None, gt=0)
    
    # Status
    is_tradeable: bool = Field(default=True)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
    @field_validator("value", mode="before")
    @classmethod
    def normalize_value(cls, v: object) -> str:
        """Normalize symbol value handling various input types."""
        if isinstance(v, int):
            return str(v)
        if isinstance(v, str):
            return v.strip().upper()
        raise SymbolValidationError(
            str(v), f"Invalid value type: {type(v).__name__}"
        )
    
    @model_validator(mode="after")
    def validate_context_requirements(self) -> 'Symbol':
        """Ensure required fields for context."""
        # Exchange context requires exchange_id
        if self.context == SymbolType.EXCHANGE and not self.exchange_id:
            raise SymbolValidationError(
                self.value,
                "exchange_id required for EXCHANGE context"
            )
            
        # Validate format based on context
        pattern = SymbolFormat.PATTERNS.get(self.context)
        if pattern and not pattern.match(self.value):
            raise SymbolValidationError(
                self.value,
                f"Invalid format for {self.context.value} context",
                expected_format=pattern.pattern
            )
            
        # Validate exchange-specific metadata
        if self.asset_index is not None and self.exchange_id != ExchangeName.HYPERLIQUID:
            raise SymbolValidationError(
                self.value,
                "asset_index only valid for Hyperliquid"
            )
            
        if self.symbol_id is not None and self.exchange_id != ExchangeName.BACKPACK:
            raise SymbolValidationError(
                self.value,
                "symbol_id only valid for Backpack"
            )
            
        return self
    
    @model_validator(mode="before")
    @classmethod
    def populate_internal_value(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Auto-populate internal_value if not provided."""
        if "internal_value" not in values and "base_asset" in values:
            base = values["base_asset"]
            market = values.get("market_type", MarketType.PERP)
            
            if market == MarketType.PERP:
                values["internal_value"] = f"{base}_PERP"
            else:
                quote = values.get("quote_asset", "USDC")
                values["internal_value"] = f"{base}_{quote}"
                
        return values
    
    @model_validator(mode="before")
    @classmethod
    def extract_assets_from_value(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Extract base and quote assets from value if not provided."""
        if "base_asset" not in values and "value" in values:
            value = str(values["value"]).upper()
            
            # Handle exchange-specific patterns
            if value.endswith("_PERP") or value.endswith("-PERP"):
                base = value.replace("_PERP", "").replace("-PERP", "")
                values["base_asset"] = base.split("_")[0].split("-")[0]
                values["market_type"] = MarketType.PERP
            elif "_" in value:
                parts = value.split("_", 1)
                values["base_asset"] = parts[0]
                if len(parts) > 1 and "quote_asset" not in values:
                    values["quote_asset"] = parts[1]
            elif "-" in value and not value.startswith("@"):
                parts = value.split("-", 1)
                values["base_asset"] = parts[0]
            else:
                values["base_asset"] = value.replace("@", "")
                
        return values
    
    def for_exchange(self, exchange: ExchangeName) -> 'Symbol':
        """Get symbol representation for specific exchange."""
        if exchange == self.exchange_id:
            # Already in correct context
            return self
            
        if exchange in self.exchange_variants:
            # Create new symbol in exchange context
            return Symbol(
                value=self.exchange_variants[exchange],
                context=SymbolType.EXCHANGE,
                exchange_id=exchange,
                internal_value=self.internal_value,
                base_asset=self.base_asset,
                quote_asset=self.quote_asset,
                market_type=self.market_type,
                exchange_variants=self.exchange_variants,
                # Copy trading params
                tick_size=self.tick_size,
                lot_size=self.lot_size,
                min_order_size=self.min_order_size,
                max_order_size=self.max_order_size,
            )
            
        raise ValueError(f"No variant defined for {exchange.value}")
    
    def to_internal(self) -> 'Symbol':
        """Get internal representation of symbol."""
        if self.context == SymbolType.INTERNAL:
            return self
            
        return Symbol(
            value=self.internal_value,
            context=SymbolType.INTERNAL,
            internal_value=self.internal_value,
            base_asset=self.base_asset,
            quote_asset=self.quote_asset,
            market_type=self.market_type,
            exchange_variants=self.exchange_variants,
        )
    
    def to_websocket_format(self) -> str | int:
        """Convert to WebSocket stream format based on context and exchange."""
        if self.context != SymbolType.WEBSOCKET:
            # Convert to websocket context first
            ws_symbol = Symbol(
                value=self.value,
                context=SymbolType.WEBSOCKET,
                exchange_id=self.exchange_id,
                internal_value=self.internal_value,
                base_asset=self.base_asset,
                quote_asset=self.quote_asset,
                asset_index=self.asset_index,
                symbol_id=self.symbol_id,
            )
            return ws_symbol.to_websocket_format()
            
        # In websocket context, return appropriate format
        if self.exchange_id == ExchangeName.BACKPACK and self.symbol_id is not None:
            return self.symbol_id
        if self.exchange_id == ExchangeName.HYPERLIQUID and self.asset_index is not None:
            return f"@{self.asset_index}"
        return self.value
    
    def shares_underlying_with(self, other: 'Symbol') -> bool:
        """Check if two symbols share the same underlying asset."""
        return self.base_asset == other.base_asset
    
    def matches_internal(self, internal_value: str) -> bool:
        """Check if this symbol matches the given internal value."""
        return self.internal_value == internal_value.upper()
    
    @computed_field
    @property
    def display_name(self) -> str:
        """Human-readable display name."""
        if self.quote_asset:
            return f"{self.base_asset}/{self.quote_asset}"
        return self.base_asset
    
    @computed_field
    @property
    def is_pair(self) -> bool:
        """Check if this represents a trading pair."""
        return self.quote_asset is not None
    
    @computed_field
    @property
    def is_indexed(self) -> bool:
        """Check if symbol uses index notation."""
        return (
            self.asset_index is not None or 
            self.symbol_id is not None or 
            self.value.startswith("@")
        )
    
    def __str__(self) -> str:
        """String representation for API calls."""
        return self.value
    
    def __hash__(self) -> int:
        """Hash by internal value, context, and exchange."""
        return hash((self.internal_value, self.context, self.exchange_id))
    
    def __eq__(self, other: object) -> bool:
        """Equality based on context and value."""
        if isinstance(other, str):
            return self.value == other.upper()
        if isinstance(other, Symbol):
            return (
                self.internal_value == other.internal_value and
                self.context == other.context and
                self.exchange_id == other.exchange_id
            )
        return False


# Factory functions for common use cases
def create_internal_symbol(
    value: str,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    market_type: MarketType = MarketType.PERP,
) -> Symbol:
    """Create a symbol in internal context."""
    return Symbol(
        value=value,
        context=SymbolType.INTERNAL,
        internal_value=value,  # For internal context, value = internal_value
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=market_type,
    )


def create_exchange_symbol(
    value: str | int,
    exchange_id: ExchangeName,
    internal_value: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    market_type: MarketType = MarketType.PERP,
    asset_index: int | None = None,
    symbol_id: int | None = None,
    **trading_params
) -> Symbol:
    """Create a symbol in exchange context."""
    return Symbol(
        value=str(value),
        context=SymbolType.EXCHANGE,
        exchange_id=exchange_id,
        internal_value=internal_value,
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=market_type,
        asset_index=asset_index,
        symbol_id=symbol_id,
        **trading_params
    )


def create_unified_symbol(
    internal_value: str,
    base_asset: str,
    quote_asset: str | None = None,
    market_type: MarketType = MarketType.PERP,
    exchange_variants: dict[ExchangeName, str] | None = None,
    **trading_params
) -> Symbol:
    """Create a symbol with cross-exchange mappings."""
    return Symbol(
        value=internal_value,
        context=SymbolType.INTERNAL,
        internal_value=internal_value,
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=market_type,
        exchange_variants=exchange_variants or {},
        **trading_params
    )
```

## Key Design Benefits

### 1. Context-Aware Behavior
The `context` field determines:
- Which validation patterns apply
- Which fields are required (e.g., exchange_id for EXCHANGE context)
- How methods like `to_websocket_format()` behave

### 2. Unified Yet Flexible
- One model serves all purposes
- Fields are sparse - only populate what's needed
- No confusion about which model to use

### 3. Cross-Exchange Mapping Built-In
The `exchange_variants` field stores how this symbol appears on different exchanges:
```python
btc_perp = Symbol(
    value="BTC_PERP",
    context=SymbolType.INTERNAL,
    internal_value="BTC_PERP",
    base_asset="BTC",
    exchange_variants={
        ExchangeName.HYPERLIQUID: "BTC-PERP",
        ExchangeName.BACKPACK: "BTC_USDC_PERP",
    }
)

# Get Hyperliquid representation
hl_symbol = btc_perp.for_exchange(ExchangeName.HYPERLIQUID)
```

### 4. Type Safety Maintained
- All fields are properly typed
- No dict[str, Any] for exchange data
- Exchange-specific fields (asset_index, symbol_id) are validated

### 5. Migration Path
The factory functions (create_internal_symbol, create_exchange_symbol, create_unified_symbol) provide backward compatibility with existing code patterns.

## Usage Examples

```python
# Internal context - canonical representation
internal = create_internal_symbol(
    value="BTC_PERP",
    base_asset="BTC",
    market_type=MarketType.PERP
)

# Exchange context - Hyperliquid
hl_btc = create_exchange_symbol(
    value="BTC-PERP",
    exchange_id=ExchangeName.HYPERLIQUID,
    internal_value="BTC_PERP",
    base_asset="BTC",
    asset_index=0,
    tick_size=Decimal("0.1")
)

# Exchange context - Backpack  
bp_btc = create_exchange_symbol(
    value="BTC_USDC_PERP",
    exchange_id=ExchangeName.BACKPACK,
    internal_value="BTC_PERP",
    base_asset="BTC",
    quote_asset="USDC",
    symbol_id=12345
)

# Unified with variants
unified = create_unified_symbol(
    internal_value="BTC_PERP",
    base_asset="BTC",
    exchange_variants={
        ExchangeName.HYPERLIQUID: "BTC-PERP",
        ExchangeName.BACKPACK: "BTC_USDC_PERP",
    },
    tick_size=Decimal("0.1"),
    lot_size=Decimal("0.001")
)

# Convert between contexts
hl_from_unified = unified.for_exchange(ExchangeName.HYPERLIQUID)
internal_from_hl = hl_btc.to_internal()

# WebSocket formats
print(hl_btc.to_websocket_format())  # "@0"
print(bp_btc.to_websocket_format())  # 12345

# Arbitrage checking
assert hl_btc.shares_underlying_with(bp_btc)  # True - both BTC
```

## Conclusion

This single context-aware Symbol model provides all the functionality of the three-model architecture while eliminating the confusion about which model to use. The context field drives behavior, validation happens based on context, and all data is type-safe without excessive nullable fields.