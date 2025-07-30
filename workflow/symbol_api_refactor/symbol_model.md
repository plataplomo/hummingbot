# Unified Symbol Model Design

## Overview

This document defines the unified Symbol model that consolidates the existing 3-model architecture (InternalSymbol, ExchangeSymbol, UnifiedSymbol) into a single model using the typed extension slots pattern.

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


# Exchange-specific extension models
class HyperliquidSymbolData(BaseModel):
    """Hyperliquid-specific symbol metadata."""
    asset_index: int | None = Field(default=None, ge=0, description="Asset index for @N notation")
    
class BackpackSymbolData(BaseModel):
    """Backpack-specific symbol metadata."""
    symbol_id: int | None = Field(default=None, ge=0, description="Integer symbol ID for WebSocket")


# Main unified model
class Symbol(BaseModel):
    """Unified symbol model with typed extension slots.
    
    This model handles all symbol contexts through a core set of universal fields
    plus exchange-specific extension slots for type-safe metadata.
    """
    
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )
    
    # Core identification
    value: str = Field(..., min_length=1, max_length=20, description="Exchange-specific symbol format")
    exchange_id: ExchangeName = Field(..., description="Exchange this symbol belongs to")
    internal_name: str = Field(..., description="Canonical internal representation (e.g., BTC_PERP)")
    
    # Asset decomposition
    base_asset: str = Field(..., pattern=r"^[A-Z0-9]{2,10}$", description="Base asset (e.g., BTC)")
    quote_asset: str | None = Field(default=None, pattern=r"^[A-Z0-9]{2,10}$", description="Quote asset (e.g., USDC)")
    market_type: MarketType = Field(default=MarketType.PERP, description="Market type (PERP/SPOT)")
    
    # Trading parameters
    tick_size: Decimal | None = Field(default=None, gt=0, description="Minimum price increment")
    lot_size: Decimal | None = Field(default=None, gt=0, description="Minimum quantity increment")
    min_order_size: Decimal | None = Field(default=None, gt=0, description="Minimum order size")
    max_order_size: Decimal | None = Field(default=None, gt=0, description="Maximum order size")
    
    # Status
    is_tradeable: bool = Field(default=True, description="Whether symbol is currently tradeable")
    is_active: bool = Field(default=True, description="Whether symbol is active")
    
    # Extension slots
    hl_data: HyperliquidSymbolData | None = Field(default=None, description="Hyperliquid-specific data")
    bp_data: BackpackSymbolData | None = Field(default=None, description="Backpack-specific data")
    
    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
    @field_validator("value", mode="before")
    @classmethod
    def normalize_value(cls, v: object) -> str:
        """Normalize symbol value handling various input types."""
        if isinstance(v, int):
            return str(v)
        if isinstance(v, str):
            return v.strip().upper()
        raise SymbolValidationError(
            str(v), f"Invalid value type: {type(v).__name__}", expected_format="string or integer"
        )
    
    @model_validator(mode="before")
    @classmethod
    def auto_generate_internal_name(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Generate internal_name if not provided based on assets and market type."""
        if "internal_name" not in values and "base_asset" in values:
            base = values["base_asset"]
            market = values.get("market_type", MarketType.PERP)
            
            if market == MarketType.PERP:
                values["internal_name"] = f"{base}_PERP"
            else:  # SPOT
                quote = values.get("quote_asset", "USDC")
                values["internal_name"] = f"{base}_{quote}"
                
        return values
    
    @model_validator(mode="before")
    @classmethod
    def extract_assets_from_value(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Extract base and quote assets from value if not provided."""
        if "base_asset" not in values and "value" in values:
            value = str(values["value"]).upper()
            
            # Common patterns
            if value.endswith("_PERP") or value.endswith("-PERP"):
                base = value.replace("_PERP", "").replace("-PERP", "")
                values["base_asset"] = base.split("_")[0].split("-")[0]
                values["market_type"] = MarketType.PERP
            elif "_" in value:
                parts = value.split("_")
                values["base_asset"] = parts[0]
                if len(parts) > 1:
                    values["quote_asset"] = parts[1]
            else:
                values["base_asset"] = value
                
        return values
    
    @model_validator(mode="after")
    def validate_extension_slots(self) -> 'Symbol':
        """Ensure only the relevant extension slot is populated."""
        if self.exchange_id == ExchangeName.HYPERLIQUID and self.bp_data is not None:
            raise SymbolValidationError(
                self.value,
                "Backpack data provided for Hyperliquid symbol"
            )
        if self.exchange_id == ExchangeName.BACKPACK and self.hl_data is not None:
            raise SymbolValidationError(
                self.value, 
                "Hyperliquid data provided for Backpack symbol"
            )
        return self
    
    @computed_field
    @property
    def is_pair(self) -> bool:
        """Check if this represents a trading pair."""
        return self.quote_asset is not None
    
    @computed_field
    @property
    def display_name(self) -> str:
        """Human-readable display name."""
        if self.quote_asset:
            return f"{self.base_asset}/{self.quote_asset}"
        return self.base_asset
    
    def shares_underlying_with(self, other: 'Symbol') -> bool:
        """Check if two symbols share the same underlying asset."""
        return self.base_asset == other.base_asset
    
    def to_websocket_format(self) -> str | int:
        """Convert to WebSocket stream format based on exchange."""
        if self.exchange_id == ExchangeName.BACKPACK and self.bp_data and self.bp_data.symbol_id:
            return self.bp_data.symbol_id
        if self.exchange_id == ExchangeName.HYPERLIQUID and self.hl_data and self.hl_data.asset_index is not None:
            return f"@{self.hl_data.asset_index}"
        return self.value
    
    def get_asset_index(self) -> int | None:
        """Get Hyperliquid asset index if available."""
        return self.hl_data.asset_index if self.hl_data else None
    
    def get_symbol_id(self) -> int | None:
        """Get Backpack symbol ID if available."""
        return self.bp_data.symbol_id if self.bp_data else None
    
    def __str__(self) -> str:
        """String representation for API calls."""
        return self.value
    
    def __hash__(self) -> int:
        """Hash based on internal name and exchange."""
        return hash((self.internal_name, self.exchange_id))
    
    def __eq__(self, other: object) -> bool:
        """Equality based on internal name and exchange."""
        if isinstance(other, str):
            return self.value == other.upper()
        if isinstance(other, Symbol):
            return (
                self.internal_name == other.internal_name and 
                self.exchange_id == other.exchange_id
            )
        return False


# Factory function
def create_symbol(
    value: str | int,
    exchange_id: ExchangeName,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    market_type: MarketType = MarketType.PERP,
    internal_name: str | None = None,
    asset_index: int | None = None,  # Hyperliquid
    symbol_id: int | None = None,     # Backpack
    **trading_params
) -> Symbol:
    """Factory function to create symbols with proper extension data."""
    
    # Build extension data based on exchange
    hl_data = None
    bp_data = None
    
    if exchange_id == ExchangeName.HYPERLIQUID and asset_index is not None:
        hl_data = HyperliquidSymbolData(asset_index=asset_index)
    elif exchange_id == ExchangeName.BACKPACK and symbol_id is not None:
        bp_data = BackpackSymbolData(symbol_id=symbol_id)
    
    return Symbol(
        value=str(value),
        exchange_id=exchange_id,
        internal_name=internal_name,
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=market_type,
        hl_data=hl_data,
        bp_data=bp_data,
        **trading_params
    )
```

## Key Design Decisions

### 1. Typed Extension Slots Pattern
Following the successful pattern used in the Order model, exchange-specific data is handled through typed extension slots:
- `hl_data: HyperliquidSymbolData | None`
- `bp_data: BackpackSymbolData | None`

This provides type safety while allowing extensibility for new exchanges.

### 2. Core Universal Fields
All symbols share these core fields regardless of exchange:
- `value`: The exchange-specific string representation
- `exchange_id`: Which exchange this symbol belongs to
- `internal_name`: Canonical internal representation (e.g., "BTC_PERP")
- `base_asset` & `quote_asset`: Asset decomposition
- `market_type`: PERP or SPOT

### 3. Auto-generation and Validation
The model includes smart validators that:
- Auto-generate `internal_name` from assets and market type
- Extract assets from the value string using common patterns
- Validate that only appropriate extension slots are populated
- Normalize input values (handle integers, uppercase strings)

### 4. Arbitrage Support
The `shares_underlying_with()` method enables checking if two symbols (even across different markets) share the same underlying asset for arbitrage:

```python
btc_perp = Symbol(value="BTC-PERP", exchange_id=ExchangeName.HYPERLIQUID, ...)
btc_spot = Symbol(value="BTC", exchange_id=ExchangeName.HYPERLIQUID, ...)

if btc_perp.shares_underlying_with(btc_spot):
    # Can arbitrage between these
```

### 5. WebSocket Support
The `to_websocket_format()` method handles exchange-specific WebSocket requirements:
- Returns integer ID for Backpack
- Returns "@N" format for Hyperliquid asset indices
- Falls back to string value otherwise

## Migration Benefits

1. **Single Model**: No more confusion about which model to use
2. **Type Safety**: Exchange-specific data remains typed through extension slots
3. **Extensibility**: Add new exchanges by creating new extension data classes
4. **Backward Compatible**: Can maintain similar factory functions and method signatures
5. **Clear Responsibilities**: Each Symbol instance represents one symbol on one exchange

## Usage Examples

```python
# Create a Hyperliquid BTC perpetual
btc_perp_hl = create_symbol(
    value="BTC-PERP",
    exchange_id=ExchangeName.HYPERLIQUID,
    asset_index=0,  # Will populate hl_data
    tick_size=Decimal("0.1"),
    lot_size=Decimal("0.001")
)

# Create a Backpack BTC perpetual  
btc_perp_bp = create_symbol(
    value="BTC_USDC_PERP",
    exchange_id=ExchangeName.BACKPACK,
    symbol_id=12345,  # Will populate bp_data
    tick_size=Decimal("0.01"),
    lot_size=Decimal("0.0001")
)

# Check if they share underlying
assert btc_perp_hl.shares_underlying_with(btc_perp_bp)  # True - both BTC

# Get exchange-specific data
hl_index = btc_perp_hl.get_asset_index()  # 0
bp_id = btc_perp_bp.get_symbol_id()       # 12345

# WebSocket formats
hl_ws = btc_perp_hl.to_websocket_format()  # "@0"
bp_ws = btc_perp_bp.to_websocket_format()  # 12345
```