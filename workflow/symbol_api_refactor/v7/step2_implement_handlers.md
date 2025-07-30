# Step 2: Implement Exchange Handlers

## Overview
Replace the simple transformers with full exchange handlers that implement the ExchangeHandler protocol. Each handler encapsulates all exchange-specific logic.

## Current State
- **transformers.py**: Simple HyperliquidSymbolTransformer, BackpackSymbolTransformer
- **Issues**: No metadata handling, no canonical logic, limited functionality
- **Usage**: Used by SymbolService for conversions

## Implementation

### 2.1 Create handlers Directory
```bash
mkdir -p cyberdelta/core/symbols/handlers
```

### 2.2 Create Hyperliquid Handler
**File**: `cyberdelta/core/symbols/handlers/hyperliquid.py`

```python
"""Hyperliquid Exchange Handler - Clean Architecture."""

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType
from ..models import Symbol, SymbolComponents, HyperliquidMetadata

class HyperliquidHandler:
    """Hyperliquid-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.HYPERLIQUID

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Hyperliquid symbol format."""
        # Handle @N format
        if value.startswith('@'):
            return SymbolComponents(base_asset=value)

        # Handle PERP format
        if value.endswith('-PERP'):
            base = value[:-5]
            return SymbolComponents(
                base_asset=base,
                quote_asset='USD',
                market_type=MarketType.PERP
            )

        # Handle spot pairs
        if '-' in value:
            parts = value.split('-', 1)
            return SymbolComponents(
                base_asset=parts[0],
                quote_asset=parts[1],
                market_type=MarketType.SPOT
            )

        return SymbolComponents(base_asset=value)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Hyperliquid symbol."""
        if components.market_type == MarketType.PERP:
            return f"{components.base_asset}-PERP"
        elif components.quote_asset:
            return f"{components.base_asset}-{components.quote_asset}"
        return components.base_asset

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format."""
        components = self.parse_components(value)
        if components.quote_asset:
            canonical = f"{components.base_asset}_{components.quote_asset}"
        else:
            canonical = components.base_asset
        return canonical, components

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical to Hyperliquid format."""
        return self.format_symbol(components)

    def create_metadata(self, **kwargs) -> HyperliquidMetadata:
        """Create Hyperliquid metadata."""
        return HyperliquidMetadata(
            asset_index=kwargs.get('asset_index')
        )

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[HyperliquidMetadata]:
        """Create Hyperliquid symbol."""
        metadata = self.create_metadata(**metadata_kwargs)
        symbol = Symbol[HyperliquidMetadata](
            value=value,
            exchange=self.exchange,
            metadata=metadata
        )
        # Pre-compute components
        symbol._components = self.parse_components(value)
        return symbol
```

### 2.3 Create Backpack Handler
**File**: `cyberdelta/core/symbols/handlers/backpack.py`

```python
"""Backpack Exchange Handler - Clean Architecture."""

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType
from ..models import Symbol, SymbolComponents, BackpackMetadata

class BackpackHandler:
    """Backpack-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BACKPACK

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Backpack symbol format."""
        # Handle PERP format
        if value.endswith('_PERP'):
            base_part = value[:-5]
            if '_' in base_part:
                parts = base_part.split('_', 1)
                return SymbolComponents(
                    base_asset=parts[0],
                    quote_asset=parts[1],
                    market_type=MarketType.PERP
                )
            return SymbolComponents(
                base_asset=base_part,
                quote_asset='USD',
                market_type=MarketType.PERP
            )

        # Handle spot pairs
        if '_' in value:
            parts = value.split('_', 1)
            return SymbolComponents(
                base_asset=parts[0],
                quote_asset=parts[1],
                market_type=MarketType.SPOT
            )

        return SymbolComponents(base_asset=value)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Backpack symbol."""
        if components.market_type == MarketType.PERP:
            if components.quote_asset and components.quote_asset != 'USD':
                return f"{components.base_asset}_{components.quote_asset}_PERP"
            return f"{components.base_asset}_PERP"
        elif components.quote_asset:
            return f"{components.base_asset}_{components.quote_asset}"
        return components.base_asset

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format."""
        components = self.parse_components(value)
        if components.quote_asset:
            canonical = f"{components.base_asset}_{components.quote_asset}"
        else:
            canonical = components.base_asset
        return canonical, components

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical to Backpack format."""
        return self.format_symbol(components)

    def create_metadata(self, **kwargs) -> BackpackMetadata:
        """Create Backpack metadata."""
        symbol_id = kwargs.get('symbol_id')
        if symbol_id is None:
            raise ValueError("symbol_id is required for Backpack symbols")
        return BackpackMetadata(symbol_id=symbol_id)

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[BackpackMetadata]:
        """Create Backpack symbol."""
        metadata = self.create_metadata(**metadata_kwargs)
        symbol = Symbol[BackpackMetadata](
            value=value,
            exchange=self.exchange,
            metadata=metadata
        )
        # Pre-compute components
        symbol._components = self.parse_components(value)
        return symbol
```

### 2.4 Create Handler Registry
**File**: `cyberdelta/core/symbols/handlers/__init__.py`

```python
"""Exchange Handlers."""

from typing import Any
from cyberdelta.enums.exchange_names import ExchangeName
from ..protocols import ExchangeHandler
from .hyperliquid import HyperliquidHandler
from .backpack import BackpackHandler

# Default handler registry
DEFAULT_HANDLERS: dict[ExchangeName, ExchangeHandler[Any]] = {
    ExchangeName.HYPERLIQUID: HyperliquidHandler(),
    ExchangeName.BACKPACK: BackpackHandler(),
}

__all__ = [
    "HyperliquidHandler",
    "BackpackHandler",
    "DEFAULT_HANDLERS",
]
```

### 2.5 Remove transformers.py
Delete the old transformers.py file completely - handlers replace it.

## Testing
1. Test each handler's parse_components method
2. Test format_symbol for all market types
3. Test canonical conversions (round-trip)
4. Test metadata creation
5. Test symbol creation with cached components

## Success Criteria
- [ ] transformers.py deleted
- [ ] Both handlers implement protocol
- [ ] All symbol formats handled
- [ ] Metadata properly created
- [ ] Components cached in symbols

## Next: Step 3
Implement the new SymbolService that uses these handlers.
