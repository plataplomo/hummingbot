# Registry Pattern Implementation Guide

## Quick Summary

Transform the current global function approach (`hl_symbol()`, `bp_symbol()`) into a scalable registry pattern that supports 10+ exchanges cleanly.

## Implementation Steps

### Step 1: Create Registry Infrastructure

**File: `cyberdelta/core/symbols/registry.py`** (NEW)
```python
from typing import Protocol, Any
from functools import lru_cache
from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol
from .protocols import ExchangeHandler


class SymbolFactory(Protocol):
    def __call__(self, value: str, **metadata) -> Symbol[Any]: ...


class SymbolRegistry:
    def __init__(self):
        self._handlers: dict[ExchangeName, ExchangeHandler[Any]] = {}
        self._factories: dict[ExchangeName, SymbolFactory] = {}
    
    def register_handler(self, exchange: ExchangeName, handler: ExchangeHandler[Any]) -> None:
        self._handlers[exchange] = handler
        
        # Create cached factory
        @lru_cache(maxsize=1000)
        def factory(value: str, **metadata) -> Symbol[Any]:
            symbol = handler.create_symbol(value, **metadata)
            components = handler.parse_components(value)
            symbol.set_components(components)
            return symbol
        
        self._factories[exchange] = factory
    
    def create_symbol(self, value: str, exchange: ExchangeName, **metadata) -> Symbol[Any]:
        return self._factories[exchange](value, **metadata)
    
    def __getattr__(self, name: str) -> SymbolFactory:
        # Enable: registry.hyperliquid("BTC-PERP")
        exchange = ExchangeName(name.upper())
        return self._factories[exchange]


_registry = SymbolRegistry()

def get_registry() -> SymbolRegistry:
    return _registry
```

### Step 2: Update Handler Registration

**File: `cyberdelta/core/symbols/handlers/__init__.py`** (UPDATE)
```python
# Current code loads handlers into DEFAULT_HANDLERS dict
# Add auto-registration to registry

from ..registry import get_registry

# Register existing handlers
registry = get_registry()
for exchange, handler in DEFAULT_HANDLERS.items():
    registry.register_handler(exchange, handler)
```

### Step 3: Create Clean Public API

**File: `cyberdelta/core/symbols/api.py`** (NEW)
```python
from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol
from .registry import get_registry


def symbol(value: str, exchange: ExchangeName, **metadata) -> Symbol:
    """Create symbol for any exchange."""
    return get_registry().create_symbol(value, exchange, **metadata)


class exchanges:
    """Dynamic namespace for all exchanges."""
    def __getattr__(self, name: str):
        return getattr(get_registry(), name)

exchanges = exchanges()
```

### Step 4: Add Common Symbols

**File: `cyberdelta/core/symbols/common.py`** (NEW)
```python
from cyberdelta.enums.exchange_names import ExchangeName
from .api import symbol


class CommonSymbols:
    class BTC:
        @staticmethod
        def hyperliquid():
            return symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
        
        @staticmethod
        def backpack():
            return symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)
        
        @staticmethod
        def all():
            return [
                CommonSymbols.BTC.hyperliquid(),
                CommonSymbols.BTC.backpack(),
                # Add more as needed
            ]
    
    class ETH:
        # Similar pattern for ETH
        pass

symbols = CommonSymbols
```

### Step 5: Update Main __init__.py

**File: `cyberdelta/core/symbols/__init__.py`** (UPDATE)
```python
# Keep existing exports for backward compatibility
from .global_service import bp_symbol, get_symbol_service, hl_symbol

# Add new registry-based exports
from .api import symbol, exchanges  
from .common import symbols

# ... existing exports ...

__all__ = [
    # Existing
    "bp_symbol",  # Mark as deprecated later
    "hl_symbol",  # Mark as deprecated later
    
    # New registry pattern
    "symbol",     # Main creation function
    "exchanges",  # Exchange namespace
    "symbols",    # Common symbols
    
    # ... rest of existing exports
]
```

## Usage Examples

### Before (Current Approach)
```python
from cyberdelta.core.symbols.global_service import hl_symbol, bp_symbol

btc_hl = hl_symbol("BTC-PERP")
btc_bp = bp_symbol("BTC_USD_PERP", symbol_id=12345)
```

### After (Registry Pattern)
```python
# Option 1: Direct creation
from cyberdelta.core.symbols import symbol
from cyberdelta.enums.exchange_names import ExchangeName

btc_hl = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
btc_bp = symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)

# Option 2: Exchange namespace (cleaner)
from cyberdelta.core.symbols import exchanges

btc_hl = exchanges.hyperliquid("BTC-PERP")
btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)

# Option 3: Common symbols (cleanest)
from cyberdelta.core.symbols import symbols

btc_hl = symbols.BTC.hyperliquid()
btc_bp = symbols.BTC.backpack()
all_btc = symbols.BTC.all()
```

## Adding New Exchange (e.g., Binance)

### 1. Create Handler
```python
# File: cyberdelta/core/symbols/handlers/binance.py
class BinanceHandler:
    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BINANCE
    
    def create_symbol(self, value: str, **metadata) -> Symbol[BinanceMetadata]:
        return Symbol(
            value=value,
            exchange=self.exchange,
            metadata=BinanceMetadata(**metadata)
        )
    # ... implement other methods
```

### 2. Register Handler
```python
# In handlers/__init__.py
DEFAULT_HANDLERS[ExchangeName.BINANCE] = BinanceHandler()
```

### 3. Add Common Symbols
```python
# In common.py
class CommonSymbols:
    class BTC:
        @staticmethod
        def binance():
            return symbol("BTCUSDT", ExchangeName.BINANCE)
```

### 4. Use It
```python
# Automatically available!
btc_bn = exchanges.binance("BTCUSDT")
btc_bn = symbols.BTC.binance()
```

## Benefits

1. **Scales to N exchanges** without import explosion
2. **Clean API**: `exchanges.binance("BTCUSDT")`  
3. **Discoverable**: IDE shows all exchanges
4. **Type safe**: Full type information preserved
5. **Backward compatible**: Old code still works

## Migration Strategy

### Phase 1: Add Registry (Now)
- Implement registry pattern
- Keep old global functions working
- New code uses new pattern

### Phase 2: Deprecate (Later)
```python
def hl_symbol(value: str, asset_index: int | None = None) -> Symbol:
    """DEPRECATED: Use exchanges.hyperliquid() instead."""
    warnings.warn(
        "hl_symbol is deprecated, use exchanges.hyperliquid() instead",
        DeprecationWarning,
        stacklevel=2
    )
    return exchanges.hyperliquid(value, asset_index=asset_index)
```

### Phase 3: Remove (Much Later)
- Remove deprecated functions
- Clean up imports

## Testing

```python
def test_registry_pattern():
    # Test all ways to create symbols
    from cyberdelta.core.symbols import symbol, exchanges, symbols
    
    # All should create same symbol
    s1 = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
    s2 = exchanges.hyperliquid("BTC-PERP")
    s3 = symbols.BTC.hyperliquid()
    
    assert s1.value == s2.value == s3.value == "BTC-PERP"
    assert s1.exchange == s2.exchange == s3.exchange == ExchangeName.HYPERLIQUID

def test_new_exchange():
    # After adding Binance
    btc = exchanges.binance("BTCUSDT")
    assert btc.exchange == ExchangeName.BINANCE
```

## Summary

This registry pattern provides a clean, scalable solution that:
- Supports 10+ exchanges without code explosion
- Maintains exchange agnosticism in Symbol model
- Provides multiple usage patterns for different preferences
- Enables easy addition of new exchanges
- Preserves backward compatibility during migration