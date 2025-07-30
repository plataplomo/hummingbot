# V7 Patterns to Avoid Service Injection

## Yes, you can use a cached/global SymbolService!

Here are the recommended patterns:

## 1. Global Singleton Pattern (Recommended)

```python
# In cyberdelta/core/symbols/service.py
_global_symbol_service: SymbolService | None = None

def get_symbol_service() -> SymbolService:
    """Get or create the global symbol service."""
    global _global_symbol_service
    if _global_symbol_service is None:
        _global_symbol_service = create_symbol_service()
    return _global_symbol_service

# Usage anywhere in codebase - no injection needed!
from cyberdelta.core.symbols import get_symbol_service

class BackpackMarketDataService:
    async def get_ticker(self, symbol: Symbol) -> Ticker:
        # Just use it directly
        return await self._fetch_ticker(symbol)

    async def get_ticker_by_string(self, symbol_str: str) -> Ticker:
        # Create symbols on demand
        service = get_symbol_service()
        symbol = service.create_symbol(symbol_str, ExchangeName.BACKPACK)
        return await self.get_ticker(symbol)
```

## 2. Factory Functions Pattern (Most Convenient)

```python
# In cyberdelta/core/symbols/factories.py
from functools import lru_cache

@lru_cache(maxsize=1000)
def bp_symbol(value: str, **kwargs) -> Symbol:
    """Create a Backpack symbol (cached)."""
    return get_symbol_service().create_symbol(
        value, ExchangeName.BACKPACK, **kwargs
    )

@lru_cache(maxsize=1000)
def hl_symbol(value: str, **kwargs) -> Symbol:
    """Create a Hyperliquid symbol (cached)."""
    return get_symbol_service().create_symbol(
        value, ExchangeName.HYPERLIQUID, **kwargs
    )

# Usage - super clean!
from cyberdelta.core.symbols import bp_symbol, hl_symbol

async def my_strategy():
    btc_perp = bp_symbol("BTC_PERP", symbol_id=1)
    btc_perp_hl = hl_symbol("BTC-PERP", asset_index=0)

    # Direct property access
    print(btc_perp.base_asset)  # "BTC"
    print(btc_perp.market_type)  # MarketType.PERP
```

## 3. Module-Level Cache Pattern

```python
# In cyberdelta/core/symbols/__init__.py

# Initialize once when module is imported
_service = create_symbol_service()

# Export convenience functions
def create_symbol(value: str, exchange: ExchangeName, **kwargs) -> Symbol:
    return _service.create_symbol(value, exchange, **kwargs)

def are_equivalent(symbol1: Symbol, symbol2: Symbol) -> bool:
    return _service.are_equivalent(symbol1, symbol2)

def convert_symbol(symbol: Symbol, target_exchange: ExchangeName) -> Symbol:
    return _service.convert_symbol(symbol, target_exchange)

# Usage - import functions directly
from cyberdelta.core.symbols import create_symbol, are_equivalent

btc_bp = create_symbol("BTC_PERP", ExchangeName.BACKPACK)
btc_hl = create_symbol("BTC-PERP", ExchangeName.HYPERLIQUID)

if are_equivalent(btc_bp, btc_hl):
    print("Same instrument!")
```

## 4. App Context Pattern (For larger applications)

```python
# In cyberdelta/app_context.py
class AppContext:
    """Global application context."""

    def __init__(self):
        self.symbol_service = create_symbol_service()
        # Other global services...

    @cached_property
    def symbols(self) -> SymbolRegistry:
        """Lazy-loaded symbol registry."""
        return SymbolRegistry(self.symbol_service)

# Global instance
app = AppContext()

# Usage
from cyberdelta.app_context import app

btc = app.symbols.bp("BTC_PERP")
```

## 5. Symbol Registry Pattern (Pre-loaded symbols)

```python
# In cyberdelta/core/symbols/registry.py
class SymbolRegistry:
    """Registry of all known symbols."""

    def __init__(self):
        self._symbols: dict[tuple[str, ExchangeName], Symbol] = {}
        self._service = get_symbol_service()
        self._load_from_config()

    def bp(self, value: str) -> Symbol:
        """Get Backpack symbol."""
        return self._get_or_create(value, ExchangeName.BACKPACK)

    def hl(self, value: str) -> Symbol:
        """Get Hyperliquid symbol."""
        return self._get_or_create(value, ExchangeName.HYPERLIQUID)

# Global registry
symbols = SymbolRegistry()

# Usage - no service needed!
from cyberdelta.core.symbols import symbols

btc_perp = symbols.bp("BTC_PERP")
eth_spot = symbols.bp("ETH_USDC")
```

## Which Pattern to Choose?

### For CyberDeltaEngine, I recommend:

1. **Use the Global Singleton Pattern** for the SymbolService
2. **Add Factory Functions** (bp_symbol, hl_symbol) for convenience
3. **Optional: Add a Symbol Registry** if you have a fixed set of symbols

This gives you:
- No dependency injection needed
- Clean, simple API
- Good performance (caching)
- Easy testing (can mock get_symbol_service)

### Implementation Plan:

```python
# 1. Create the global service (cyberdelta/core/symbols/service.py)
_service: SymbolService | None = None

def get_symbol_service() -> SymbolService:
    global _service
    if _service is None:
        _service = create_symbol_service()
    return _service

# 2. Add factory functions (cyberdelta/core/symbols/factories.py)
@lru_cache(maxsize=1000)
def bp_symbol(value: str, **kwargs) -> Symbol:
    return get_symbol_service().create_symbol(value, ExchangeName.BACKPACK, **kwargs)

# 3. Export from __init__.py
from .service import get_symbol_service
from .factories import bp_symbol, hl_symbol
from .models import Symbol

__all__ = ['Symbol', 'get_symbol_service', 'bp_symbol', 'hl_symbol']

# 4. Use everywhere without injection!
from cyberdelta.core.symbols import bp_symbol

btc = bp_symbol("BTC_PERP")
print(btc.base_asset)  # "BTC"
```

## Benefits:

1. **No injection required** - Just import and use
2. **Cached symbols** - Same string creates same Symbol instance
3. **Clean API** - `bp_symbol("BTC_PERP")` is very readable
4. **Testable** - Can mock `get_symbol_service()` in tests
5. **Gradual migration** - Can start using immediately

## Conclusion:

Yes, you absolutely can avoid injecting SymbolService everywhere! The global singleton pattern with factory functions provides the best balance of simplicity and functionality.
