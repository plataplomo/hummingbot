# Scalable Symbol Registry Pattern for 10+ Exchanges

## Overview

As CyberDelta expands to support 10+ exchanges, the current approach of global functions (`hl_symbol()`, `bp_symbol()`) and direct handler references doesn't scale well. This document outlines a registry-based pattern that maintains exchange agnosticism while providing a clean, scalable API.

## Current Architecture Analysis

### What We Have Now
```python
# Current approach - doesn't scale well
from cyberdelta.core.symbols.global_service import hl_symbol, bp_symbol

# Problems with 10+ exchanges:
# 1. Need to add a new global function for each exchange
# 2. Import statements become unwieldy
# 3. No dynamic exchange support
# 4. Tight coupling between symbol creation and specific exchanges
```

### Existing Infrastructure We Can Leverage
1. **ExchangeHandler Protocol** - Already defines the interface
2. **DEFAULT_HANDLERS Registry** - Already has basic registry structure
3. **SymbolService** - Already manages handlers dynamically
4. **Symbol Model** - Clean, exchange-agnostic data model

## Proposed Registry Pattern

### 1. Enhanced Registry with Factory Pattern

```python
# File: cyberdelta/core/symbols/registry.py
"""Scalable symbol registry for N exchanges."""

from typing import Protocol, Any, TypeVar
from functools import lru_cache

from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol, SymbolMetadata
from .protocols import ExchangeHandler

TMetadata = TypeVar("TMetadata", bound=SymbolMetadata)


class SymbolFactory(Protocol):
    """Protocol for symbol factories."""
    
    def __call__(self, value: str, **metadata) -> Symbol[Any]:
        """Create a symbol with the given value and metadata."""
        ...


class SymbolRegistry:
    """Central registry for symbol creation - scalable to N exchanges."""
    
    def __init__(self):
        self._handlers: dict[ExchangeName, ExchangeHandler[Any]] = {}
        self._factories: dict[ExchangeName, SymbolFactory] = {}
        self._symbol_cache: dict[tuple[str, ExchangeName, tuple], Symbol[Any]] = {}
    
    def register_handler(
        self, 
        exchange: ExchangeName, 
        handler: ExchangeHandler[Any]
    ) -> None:
        """Register an exchange handler."""
        self._handlers[exchange] = handler
        
        # Create and cache factory for this exchange
        self._factories[exchange] = self._create_factory(exchange, handler)
    
    def _create_factory(
        self, 
        exchange: ExchangeName, 
        handler: ExchangeHandler[Any]
    ) -> SymbolFactory:
        """Create a factory function for an exchange."""
        @lru_cache(maxsize=1000)
        def factory(value: str, **metadata) -> Symbol[Any]:
            # Create symbol using handler
            symbol = handler.create_symbol(value, **metadata)
            
            # Pre-compute and cache components
            components = handler.parse_components(value)
            symbol.set_components(components)
            
            return symbol
        
        # Add exchange info to factory for debugging
        factory.__name__ = f"{exchange.value}_symbol_factory"
        factory.__doc__ = f"Create {exchange.value} symbol"
        
        return factory
    
    def get_factory(self, exchange: ExchangeName) -> SymbolFactory:
        """Get factory for an exchange."""
        if exchange not in self._factories:
            raise ValueError(f"No factory registered for {exchange}")
        return self._factories[exchange]
    
    def create_symbol(
        self, 
        value: str, 
        exchange: ExchangeName, 
        **metadata
    ) -> Symbol[Any]:
        """Create symbol for any registered exchange."""
        factory = self.get_factory(exchange)
        return factory(value, **metadata)
    
    def __getattr__(self, name: str) -> SymbolFactory:
        """Dynamic attribute access for exchange factories.
        
        Allows: registry.hyperliquid("BTC-PERP")
        """
        # Convert attribute name to exchange enum
        try:
            exchange = ExchangeName(name.upper())
            return self.get_factory(exchange)
        except (ValueError, KeyError):
            raise AttributeError(f"No factory for exchange: {name}")


# Global registry instance
_registry = SymbolRegistry()


def get_registry() -> SymbolRegistry:
    """Get the global symbol registry."""
    return _registry
```

### 2. Auto-Registration of Handlers

```python
# File: cyberdelta/core/symbols/handlers/registry_loader.py
"""Auto-load all exchange handlers into registry."""

import importlib
import pkgutil
from typing import Any

from cyberdelta.enums.exchange_names import ExchangeName
from ..registry import get_registry
from ..protocols import ExchangeHandler


def auto_register_handlers() -> None:
    """Automatically discover and register all exchange handlers."""
    registry = get_registry()
    
    # Import all handler modules
    import cyberdelta.core.symbols.handlers as handlers_pkg
    
    for _, module_name, _ in pkgutil.iter_modules(handlers_pkg.__path__):
        if module_name.startswith("_"):
            continue
            
        # Import the module
        module = importlib.import_module(
            f"cyberdelta.core.symbols.handlers.{module_name}"
        )
        
        # Find handler class (convention: ModuleNameHandler)
        handler_class_name = f"{module_name.title()}Handler"
        handler_class = getattr(module, handler_class_name, None)
        
        if handler_class and hasattr(handler_class, "exchange"):
            # Create instance and register
            handler_instance = handler_class()
            exchange = handler_instance.exchange
            registry.register_handler(exchange, handler_instance)


# Auto-register on import
auto_register_handlers()
```

### 3. Clean Public API

```python
# File: cyberdelta/core/symbols/api.py
"""Clean public API for symbol creation."""

from typing import Any
from functools import lru_cache

from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol
from .registry import get_registry
from .service import SymbolService


# Direct symbol creation
def symbol(value: str, exchange: ExchangeName, **metadata) -> Symbol[Any]:
    """Create symbol for any exchange.
    
    Examples:
        >>> symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
        >>> symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)
        >>> symbol("BTCUSDT", ExchangeName.BINANCE)
    """
    return get_registry().create_symbol(value, exchange, **metadata)


# Exchange namespaces for cleaner imports
class exchanges:
    """Namespace for exchange-specific symbol creation.
    
    Provides dynamic access to all registered exchanges:
        >>> exchanges.hyperliquid("BTC-PERP")
        >>> exchanges.backpack("BTC_USD_PERP", symbol_id=12345)
        >>> exchanges.binance("BTCUSDT")
    """
    def __getattr__(self, name: str):
        return getattr(get_registry(), name)


# Singleton instance
exchanges = exchanges()


# Get symbol service for advanced operations
@lru_cache(maxsize=1)
def get_symbol_service() -> SymbolService:
    """Get symbol service for advanced operations like equivalence."""
    from .factory import create_symbol_service
    return create_symbol_service(handlers=get_registry()._handlers)
```

### 4. Common Symbol Constants

```python
# File: cyberdelta/core/symbols/common.py
"""Common symbol constants for frequently used symbols."""

from dataclasses import dataclass
from typing import Callable

from cyberdelta.enums.exchange_names import ExchangeName
from .api import symbol
from .models import Symbol


@dataclass
class AssetSymbols:
    """Container for an asset's symbols across exchanges."""
    
    asset: str
    _creators: dict[ExchangeName, Callable[[], Symbol]]
    
    def __getattr__(self, exchange_name: str) -> Callable[[], Symbol]:
        """Get symbol creator for an exchange.
        
        Usage: btc.hyperliquid() or btc.binance()
        """
        try:
            exchange = ExchangeName(exchange_name.upper())
            return self._creators[exchange]
        except (ValueError, KeyError):
            raise AttributeError(f"No symbol defined for {exchange_name}")
    
    def all(self) -> list[Symbol]:
        """Get symbols for all configured exchanges."""
        return [creator() for creator in self._creators.values()]
    
    def for_exchanges(self, exchanges: list[ExchangeName]) -> list[Symbol]:
        """Get symbols for specific exchanges."""
        return [self._creators[ex]() for ex in exchanges if ex in self._creators]


class CommonSymbols:
    """Common symbols for major assets."""
    
    # BTC symbols across exchanges
    BTC = AssetSymbols(
        asset="BTC",
        _creators={
            ExchangeName.HYPERLIQUID: lambda: symbol("BTC-PERP", ExchangeName.HYPERLIQUID),
            ExchangeName.BACKPACK: lambda: symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345),
            ExchangeName.BINANCE: lambda: symbol("BTCUSDT", ExchangeName.BINANCE),
            ExchangeName.BYBIT: lambda: symbol("BTCUSD", ExchangeName.BYBIT),
            ExchangeName.OKX: lambda: symbol("BTC-USD-SWAP", ExchangeName.OKX),
            # Add more as exchanges are added
        }
    )
    
    # ETH symbols across exchanges  
    ETH = AssetSymbols(
        asset="ETH",
        _creators={
            ExchangeName.HYPERLIQUID: lambda: symbol("ETH-PERP", ExchangeName.HYPERLIQUID),
            ExchangeName.BACKPACK: lambda: symbol("ETH_USD_PERP", ExchangeName.BACKPACK, symbol_id=67890),
            ExchangeName.BINANCE: lambda: symbol("ETHUSDT", ExchangeName.BINANCE),
            ExchangeName.BYBIT: lambda: symbol("ETHUSD", ExchangeName.BYBIT),
            ExchangeName.OKX: lambda: symbol("ETH-USD-SWAP", ExchangeName.OKX),
        }
    )
    
    @classmethod
    def add_asset(cls, asset: str, symbols: dict[ExchangeName, dict]) -> None:
        """Dynamically add a new asset."""
        creators = {}
        for exchange, config in symbols.items():
            value = config.pop("value")
            creators[exchange] = lambda v=value, e=exchange, **c=config: symbol(v, e, **c)
        
        setattr(cls, asset.upper(), AssetSymbols(asset=asset, _creators=creators))


# Shorter alias
symbols = CommonSymbols
```

## Usage Examples

### Basic Symbol Creation

```python
from cyberdelta.core.symbols import symbol, exchanges
from cyberdelta.enums.exchange_names import ExchangeName

# Method 1: Direct function call
btc_hl = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
btc_bp = symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)

# Method 2: Exchange namespace (cleaner)
btc_hl = exchanges.hyperliquid("BTC-PERP")
btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)
btc_bn = exchanges.binance("BTCUSDT")

# Method 3: Common symbols (cleanest for standard assets)
from cyberdelta.core.symbols import symbols

btc_hl = symbols.BTC.hyperliquid()
btc_bp = symbols.BTC.backpack()
all_btc = symbols.BTC.all()  # Get BTC for all exchanges
```

### Dynamic Exchange Support

```python
# Works with any number of exchanges
def create_symbols_for_asset(asset: str, exchanges_list: list[ExchangeName]) -> list[Symbol]:
    """Create symbols dynamically for any set of exchanges."""
    symbols = []
    
    for exchange in exchanges_list:
        if exchange == ExchangeName.HYPERLIQUID:
            sym = exchanges.hyperliquid(f"{asset}-PERP")
        elif exchange == ExchangeName.BACKPACK:
            # Would need to lookup symbol_id dynamically
            sym = exchanges.backpack(f"{asset}_USD_PERP", symbol_id=lookup_id(asset))
        elif exchange == ExchangeName.BINANCE:
            sym = exchanges.binance(f"{asset}USDT")
        # ... handle all exchanges
        
        symbols.append(sym)
    
    return symbols
```

### Advanced Operations

```python
from cyberdelta.core.symbols import get_symbol_service

# Service only needed for advanced features
service = get_symbol_service()

# Register equivalence
btc_hl = exchanges.hyperliquid("BTC-PERP")
btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)
service.register_equivalent_symbols(btc_hl, btc_bp)

# Check equivalence
are_equal = service.are_equivalent(btc_hl, btc_bp)  # True

# Parse components
components = service.parse_components(btc_hl)
print(f"Base: {components.base_asset}")  # BTC
print(f"Market: {components.market_type}")  # PERP
```

## Adding a New Exchange

Adding support for a new exchange (e.g., Kraken) is straightforward:

### 1. Create Handler

```python
# File: cyberdelta/core/symbols/handlers/kraken.py
from cyberdelta.enums.exchange_names import ExchangeName
from ..models import Symbol, SymbolComponents, SymbolMetadata


class KrakenMetadata(SymbolMetadata):
    """Kraken-specific metadata."""
    
    pair_id: int | None = None
    
    @property
    def exchange_type(self) -> ExchangeName:
        return ExchangeName.KRAKEN


class KrakenHandler:
    """Kraken exchange handler."""
    
    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.KRAKEN
    
    def parse_components(self, value: str) -> SymbolComponents:
        # Implement Kraken-specific parsing
        # e.g., "XXBTZUSD" -> BTC/USD
        pass
    
    def create_symbol(self, value: str, **metadata) -> Symbol[KrakenMetadata]:
        return Symbol(
            value=value,
            exchange=self.exchange,
            metadata=KrakenMetadata(**metadata)
        )
    
    # ... implement other protocol methods
```

### 2. Add to Exchange Enum

```python
# File: cyberdelta/enums/exchange_names.py
class ExchangeName(str, Enum):
    HYPERLIQUID = "hyperliquid"
    BACKPACK = "backpack"
    BINANCE = "binance"
    KRAKEN = "kraken"  # Add new exchange
    # ...
```

### 3. Add Common Symbols

```python
# Update cyberdelta/core/symbols/common.py
CommonSymbols.BTC._creators[ExchangeName.KRAKEN] = lambda: symbol("XXBTZUSD", ExchangeName.KRAKEN)
CommonSymbols.ETH._creators[ExchangeName.KRAKEN] = lambda: symbol("XETHZUSD", ExchangeName.KRAKEN)
```

### 4. Use It

```python
# Automatically available!
kraken_btc = exchanges.kraken("XXBTZUSD")
kraken_btc = symbols.BTC.kraken()
```

## Benefits of This Approach

### 1. **Scalability**
- Adding exchange #11 is identical to adding exchange #3
- No import explosion
- No global function proliferation

### 2. **Discoverability**
```python
# IDE autocomplete shows all available exchanges
exchanges.<TAB>  # Shows: hyperliquid, backpack, binance, bybit, okx...

# Common symbols are discoverable
symbols.BTC.<TAB>  # Shows: hyperliquid(), backpack(), binance()...
```

### 3. **Type Safety**
- Full type information preserved
- Protocol ensures all handlers implement required methods
- Generic types flow through properly

### 4. **Performance**
- LRU caching at multiple levels
- Symbol creation is fast
- No repeated parsing

### 5. **Flexibility**
- Works with dynamic exchange lists
- Supports runtime exchange addition
- Clean separation of concerns

## Migration Path

### Phase 1: Add Registry (Backward Compatible)
```python
# Keep existing global functions
from cyberdelta.core.symbols.global_service import hl_symbol, bp_symbol

# Add new registry-based API
from cyberdelta.core.symbols import exchanges, symbols
```

### Phase 2: Update Documentation
- Show new patterns as preferred
- Keep old patterns as "legacy"

### Phase 3: Gradual Migration
- New code uses registry pattern
- Old code continues to work
- Migrate during regular maintenance

### Phase 4: Deprecate Old Pattern
- Add deprecation warnings to global functions
- Provide migration script

### Phase 5: Remove Legacy Code
- Remove global functions
- Clean up imports

## Testing Strategy

```python
# File: tests/unit/core/symbols/test_registry.py
import pytest
from cyberdelta.core.symbols import exchanges, symbols, get_registry


class TestSymbolRegistry:
    """Test scalable registry pattern."""
    
    def test_dynamic_exchange_access(self):
        """Test dynamic attribute access for exchanges."""
        btc = exchanges.hyperliquid("BTC-PERP")
        assert btc.value == "BTC-PERP"
        assert btc.exchange == ExchangeName.HYPERLIQUID
    
    def test_all_registered_exchanges(self):
        """Test all exchanges are accessible."""
        registry = get_registry()
        
        for exchange in ExchangeName:
            # Should not raise
            factory = registry.get_factory(exchange)
            assert factory is not None
    
    def test_common_symbols(self):
        """Test common symbol constants."""
        all_btc = symbols.BTC.all()
        
        # Should have BTC for each registered exchange
        assert len(all_btc) >= 2  # At least HL and BP
        
        # All should be Symbol objects
        for sym in all_btc:
            assert isinstance(sym, Symbol)
            assert "BTC" in sym.value.upper()
    
    def test_caching(self):
        """Test symbol creation is cached."""
        # Same calls should return same object
        btc1 = exchanges.hyperliquid("BTC-PERP")
        btc2 = exchanges.hyperliquid("BTC-PERP")
        
        assert btc1 is btc2  # Same object due to caching
```

## Conclusion

This registry pattern provides a clean, scalable solution for supporting 10+ exchanges while maintaining:
- Exchange agnosticism in the core Symbol model
- Clean, discoverable API
- Type safety throughout
- Excellent performance
- Easy extensibility

The pattern grows linearly with exchange count (not exponentially), making it suitable for CyberDelta's expansion to many exchanges.