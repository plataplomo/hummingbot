# Step 1: Replace Current Models with New Architecture

## Overview
Replace the entire 3-model system (BaseSymbol, InternalSymbol, ExchangeSymbol, UnifiedSymbol) with a clean single Symbol model using generics. This is a complete replacement, not a compatibility layer.

## Current State
- **models.py**: Contains BaseSymbol, InternalSymbol, ExchangeSymbol, UnifiedSymbol (600+ lines)
- **Issues**: Complex inheritance, mixed behavior/data, no type-safe metadata
- **Usage**: Throughout service.py, store.py, config_loader.py, transformers.py

## Implementation

### 1.1 Replace models.py
**File**: `cyberdelta/core/symbols/models.py`

Replace entire file with:
```python
"""Symbol Models - Clean Architecture with Type Safety."""

from typing import Protocol, TypeVar, Generic
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

# Type variable for metadata
TMetadata = TypeVar('TMetadata', bound='SymbolMetadata')

# Base metadata model
class SymbolMetadata(BaseModel):
    """Base class for exchange metadata."""
    model_config = ConfigDict(frozen=True)

    @property
    def exchange_type(self) -> type[ExchangeName]:
        """Get the exchange type this metadata is for."""
        raise NotImplementedError

class HyperliquidMetadata(SymbolMetadata):
    """Hyperliquid-specific metadata."""
    asset_index: int | None = None

    @property
    def exchange_type(self) -> type[ExchangeName]:
        return ExchangeName.HYPERLIQUID

class BackpackMetadata(SymbolMetadata):
    """Backpack-specific metadata."""
    symbol_id: int

    @property
    def exchange_type(self) -> type[ExchangeName]:
        return ExchangeName.BACKPACK

class SymbolComponents(BaseModel):
    """Parsed symbol components - pure data."""
    model_config = ConfigDict(frozen=True)

    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP

class Symbol(BaseModel, Generic[TMetadata]):
    """Pure symbol data model with no behavior."""
    model_config = ConfigDict(frozen=True)

    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName
    metadata: TMetadata

    _components: SymbolComponents | None = PrivateAttr(default=None)

    @property
    def base_asset(self) -> str:
        if self._components:
            return self._components.base_asset
        # Fallback parsing
        if '_' in self.value:
            return self.value.split('_')[0]
        elif '-' in self.value:
            return self.value.split('-')[0]
        return self.value

    @property
    def quote_asset(self) -> str | None:
        if self._components:
            return self._components.quote_asset
        return None

    @property
    def market_type(self) -> MarketType:
        if self._components:
            return self._components.market_type
        return MarketType.PERP

    def __str__(self) -> str:
        return self.value

    def __hash__(self) -> int:
        return hash((self.value, self.exchange))
```

### 1.2 Replace protocols.py
**File**: `cyberdelta/core/symbols/protocols.py`

Replace entire file with:
```python
"""Symbol Protocols for Clean Architecture."""

from typing import Protocol, Any
from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol, SymbolComponents, TMetadata

class ExchangeHandler(Protocol[TMetadata]):
    """Protocol for exchange-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        """The exchange this handler is for."""
        ...

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse symbol value into components using exchange rules."""
        ...

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into exchange-specific symbol value."""
        ...

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format and return components."""
        ...

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical format to exchange format."""
        ...

    def create_metadata(self, **kwargs) -> TMetadata:
        """Create exchange-specific metadata."""
        ...

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[TMetadata]:
        """Create symbol with proper metadata."""
        ...
```

### 1.3 Remove Old Files
Delete these files completely:
- `exceptions.py` - Move necessary exceptions to models.py
- `operation_results.py` - Not needed in new architecture
- `validators.py` - Validation in handlers
- `logging_helpers.py` - Use standard logging
- `migration_utils.py` - Clean break, no migration

### 1.4 Simplify __init__.py
**File**: `cyberdelta/core/symbols/__init__.py`

Replace with minimal exports:
```python
"""Symbol System - Clean Architecture."""

from .models import (
    Symbol,
    SymbolMetadata,
    HyperliquidMetadata,
    BackpackMetadata,
    SymbolComponents,
)
from .protocols import ExchangeHandler

__all__ = [
    "Symbol",
    "SymbolMetadata",
    "HyperliquidMetadata",
    "BackpackMetadata",
    "SymbolComponents",
    "ExchangeHandler",
]
```

## Testing
1. Remove all existing tests (clean break)
2. Create new test file for models
3. Test Symbol creation with different metadata
4. Test property accessors
5. Test immutability

## Success Criteria
- [ ] Old models completely removed
- [ ] New models in place
- [ ] Protocols defined
- [ ] Minimal __init__.py
- [ ] Clean directory structure

## Next: Step 2
Implement exchange handlers that use these protocols.
