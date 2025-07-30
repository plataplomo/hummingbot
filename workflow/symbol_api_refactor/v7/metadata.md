# Exchange-Agnostic Metadata Solution

## Problem

The current generic `Symbol[TMetadata]` approach forces `Symbol[Any]` everywhere in the codebase, revealing fundamental issues:

1. **Lost type safety** - `Any` defeats the purpose of typed metadata
2. **Ergonomic nightmare** - Developers must type `Symbol[Any]` constantly
3. **Generic complexity without benefit** - Most code doesn't need specific metadata types

## Failed Alternative Solutions

### Runtime Type Checking with Properties
```python
@property  
def hyperliquid_metadata(self) -> HyperliquidMetadata | None:
    return self.metadata if isinstance(self.metadata, HyperliquidMetadata) else None
```
**Problem**: Breaks exchange agnosticism by hardcoding exchange names in API.

### Union Types
```python
metadata: HyperliquidMetadata | BackpackMetadata
```
**Problem**: Unmaintainable with 20+ exchanges. Union would become massive.

### Type Aliases (Traditional)
```python
Symbol = _Symbol[SymbolMetadata]
```
**Problem**: Still requires generic complexity internally, doesn't solve the core issue.

## Best Solution: Python 3.12+ Type Statement

Use Python 3.13's enhanced type system with the `type` statement for clean public API while maintaining full internal type safety.

### Why This Is Optimal

1. **Zero imports needed** - `type` statement is built-in Python 3.12+
2. **Clean public API** - Users only see `Symbol`, never generics
3. **Full internal type safety** - Generics work internally for metadata
4. **Forward compatible** - Uses latest Python typing features
5. **No breaking changes** - Registry API stays identical

### Implementation

```python
# models.py
from typing import TypeVar

TMetadata = TypeVar("TMetadata", bound="SymbolMetadata")

class _Symbol[TMetadata: SymbolMetadata](BaseModel):
    """Internal generic symbol implementation with full type safety."""
    
    model_config = ConfigDict(frozen=True)
    
    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName
    metadata: TMetadata  # Fully typed internally
    _components: SymbolComponents | None = PrivateAttr(default=None)
    
    # All current methods unchanged...
    def set_components(self, components: SymbolComponents) -> None:
        self._components = components
    
    @property
    def base_asset(self) -> str:
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.base_asset
    
    # ... rest of implementation

# Public API type alias - clean and simple
type Symbol = _Symbol[SymbolMetadata]

# api.py  
def symbol(value: str, exchange: ExchangeName, ...) -> Symbol:  # Clean!
    return get_registry().create_symbol(value, exchange, ...)

# All registry methods return Symbol, not Symbol[Any]
class Exchanges:
    def hyperliquid(self, value: str, asset_index: int | None = None) -> Symbol:
        return get_registry().create_symbol(value, ExchangeName.HYPERLIQUID, asset_index=asset_index)
    
    def backpack(self, value: str, symbol_id: int | None = None) -> Symbol:
        return get_registry().create_symbol(value, ExchangeName.BACKPACK, symbol_id=symbol_id)

exchanges = Exchanges()
```

### Usage Examples

```python
# Clean everywhere - no Symbol[Any] noise
def process_order(symbol: Symbol) -> None:
    logger.info("Processing", symbol=symbol.value, exchange=symbol.exchange)

def calculate_arbitrage(symbol1: Symbol, symbol2: Symbol) -> Decimal:
    return symbol1.get_price() - symbol2.get_price()

def create_ticker(symbol: Symbol, price: Decimal) -> Ticker:
    return Ticker(symbol=symbol, price=price)

# Registry API stays clean
btc_hl = exchanges.hyperliquid("BTC-PERP", asset_index=0)
btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)

# Both are just Symbol type, no generics visible
assert isinstance(btc_hl, Symbol)
assert isinstance(btc_bp, Symbol)
```

### Benefits

1. **Clean Type System** - Just `Symbol` everywhere, no generic pollution
2. **Full Type Safety** - Internal generics provide complete type checking
3. **Zero Breaking Changes** - Public API identical, internal implementation enhanced
4. **Future Proof** - Uses Python 3.13's latest typing features
5. **Perfect Migration** - Simple find/replace `Symbol[Any]` → `Symbol`

## Alternative: Exchange-Agnostic Metadata Access

If you can't use Python 3.12+ features, make `Symbol` non-generic and provide exchange-agnostic access patterns through methods instead of types.

### Implementation

```python
class Symbol(BaseModel):
    """Non-generic symbol with exchange-agnostic metadata access."""
    
    model_config = ConfigDict(frozen=True)
    
    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName
    metadata: SymbolMetadata  # Base type only - no generics
    _components: SymbolComponents | None = PrivateAttr(default=None)
    
    # Exchange-agnostic metadata access
    def get_metadata(self, key: str) -> Any:
        """Get metadata field if it exists."""
        return getattr(self.metadata, key, None)
    
    def has_metadata(self, key: str) -> bool:
        """Check if metadata field exists."""
        return hasattr(self.metadata, key)
    
    # Common patterns as methods
    def get_index(self) -> int | None:
        """Get exchange index (asset_index, symbol_id, etc)."""
        return self.get_metadata('asset_index') or self.get_metadata('symbol_id')
    
    def get_exchange_id(self) -> str | int | None:
        """Get exchange-specific identifier."""
        return self.get_index()
    
    # Component access (unchanged)
    def set_components(self, components: SymbolComponents) -> None:
        """Set the cached components for this symbol."""
        self._components = components

    @property
    def base_asset(self) -> str:
        """Get the base asset of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.base_asset

    @property
    def quote_asset(self) -> str | None:
        """Get the quote asset of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.quote_asset

    @property
    def market_type(self) -> MarketType:
        """Get the market type of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.market_type
    
    def __str__(self) -> str:
        """String representation of the symbol."""
        return self.value
    
    def __hash__(self) -> int:
        """Hash based on value and exchange."""
        return hash((self.value, self.exchange))
```

### Metadata Classes (unchanged)

```python
class SymbolMetadata(BaseModel):
    """Base class for exchange metadata."""
    model_config = ConfigDict(frozen=True)

    @property
    def exchange_type(self) -> ExchangeName:
        """Get the exchange type this metadata is for."""
        raise NotImplementedError

class HyperliquidMetadata(SymbolMetadata):
    """Hyperliquid-specific metadata."""
    asset_index: int | None = None

    @property
    def exchange_type(self) -> ExchangeName:
        return ExchangeName.HYPERLIQUID

class BackpackMetadata(SymbolMetadata):
    """Backpack-specific metadata."""
    symbol_id: int

    @property
    def exchange_type(self) -> ExchangeName:
        return ExchangeName.BACKPACK
```

## Usage Examples

### Clean Types Everywhere

```python
# No more Symbol[Any] - just Symbol
def process_order(symbol: Symbol) -> None:
    logger.info("Processing", symbol=symbol.value, exchange=symbol.exchange)

def calculate_arbitrage(symbol1: Symbol, symbol2: Symbol) -> Decimal:
    return symbol1.get_price() - symbol2.get_price()

def create_ticker(symbol: Symbol, price: Decimal) -> Ticker:
    return Ticker(symbol=symbol, price=price)
```

### Exchange-Agnostic Metadata Access

```python
# Works with any exchange without hardcoding exchange names
def needs_special_handling(symbol: Symbol) -> bool:
    return symbol.has_metadata('asset_index') or symbol.has_metadata('symbol_id')

def get_exchange_identifier(symbol: Symbol) -> str | int | None:
    return symbol.get_index()  # Works for Hyperliquid, Backpack, any future exchange

def log_symbol_details(symbol: Symbol) -> None:
    logger.info(
        "Symbol details",
        value=symbol.value,
        exchange=symbol.exchange.value,
        has_index=symbol.has_metadata('asset_index'),
        has_id=symbol.has_metadata('symbol_id'),
        index_value=symbol.get_index()
    )
```

### Registry API (unchanged)

```python
# Factory functions still create appropriate metadata internally
# But return plain Symbol type
btc_hl = exchanges.hyperliquid("BTC-PERP", asset_index=0)
btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)

# Both are just Symbol type, no generics
assert isinstance(btc_hl, Symbol)
assert isinstance(btc_bp, Symbol)

# Exchange-agnostic usage
symbols = [btc_hl, btc_bp]
for symbol in symbols:
    if symbol.get_index():
        print(f"Symbol {symbol.value} has index: {symbol.get_index()}")
```

### Type Annotations in Services

```python
# Clean service signatures
class OrderService:
    async def place_order(self, symbol: Symbol, quantity: Decimal) -> Order:
        # String conversion only at API boundary
        api_symbol = symbol.value
        ...

# Clean mapper signatures  
class TickerMapper:
    def map_ticker(self, raw_data: dict, symbol: Symbol) -> Ticker:
        return Ticker(symbol=symbol, price=Decimal(raw_data['price']))

# Clean domain models
@dataclass
class Position:
    symbol: Symbol  # Not Symbol[Any]
    quantity: Decimal
    entry_price: Decimal
```

## Benefits

### 1. Clean Type System
- Just `Symbol` everywhere - no generics in user code
- No `Symbol[Any]` noise in type annotations
- Simple, readable function signatures

### 2. Exchange Agnosticism
- Metadata access through methods, not hardcoded properties
- Works with infinite exchanges without code changes
- No exchange names in generic business logic

### 3. Type Safety Maintained
- Metadata classes still properly typed
- Factory functions create correct metadata internally
- Runtime validation through `hasattr`/`getattr`

### 4. Scalability
- Adding new exchanges requires only new metadata class and handler
- No changes to existing code using Symbol
- No growing union types or generic parameter lists

### 5. Migration Path
- Change `Symbol[Any]` to `Symbol` throughout codebase
- Replace direct metadata access with method calls
- Registry API stays the same

## Migration Steps

1. **Update Symbol model** - Remove generic, add access methods
2. **Update type annotations** - Replace `Symbol[Any]` with `Symbol`
3. **Update metadata access** - Use methods instead of direct field access
4. **Update handlers** - Return plain Symbol type
5. **Test exchange agnosticism** - Verify code works with new exchanges without changes

The key insight: **Hide metadata complexity behind methods, not types**. This provides exchange agnosticism while maintaining clean, simple type annotations throughout the codebase.