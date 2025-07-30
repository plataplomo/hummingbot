# Symbol Model V4 - One Symbol, Multiple Contexts

## Deep Research Findings

After analyzing the codebase and the Week 2 refactor, here's what I discovered:

### Current State
1. **The refactor used ONLY ExchangeSymbol** - completely ignoring InternalSymbol and UnifiedSymbol
2. **Core domain models now expect ExchangeSymbol** - Ticker, OrderBook, Position all have `symbol: ExchangeSymbol`
3. **But core services still expect strings** - portfolio tracker uses `dict[str, Position]`, does `.split()` on symbols
4. **The transformers convert between formats** but only work with InternalSymbol

### The Real Problems
1. **Unclear boundaries** - When should we use InternalSymbol vs ExchangeSymbol?
2. **Impedance mismatch** - Domain models use objects, but business logic uses strings
3. **Missing the middle layer** - No clear way to go from ExchangeSymbol → business logic needs
4. **Systemic inconsistency** - Having 3 models requires documentation about which to use where

## The Solution: One Symbol, Multiple Contexts

Based on the research, we need ONE symbol model that can represent different contexts:

```python
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr
from cyberdelta.enums.exchange_names import ExchangeName

class Symbol(BaseModel):
    """Universal symbol that can represent any context"""
    
    model_config = ConfigDict(frozen=True)
    
    # The actual symbol value
    value: str = Field(..., min_length=1, max_length=30)
    
    # Which exchange (None = internal/canonical)
    exchange: ExchangeName | None = None
    
    # Parsed components (lazy computed)
    _base_asset: str | None = PrivateAttr(default=None)
    _quote_asset: str | None = PrivateAttr(default=None)
    
    # Exchange-specific metadata (sparse)
    asset_index: int | None = Field(default=None, ge=0)  # Hyperliquid
    symbol_id: int | None = Field(default=None, ge=0)    # Backpack
    
    @property
    def base_asset(self) -> str:
        """Get base asset, parsing if needed"""
        if self._base_asset is None:
            self._parse_assets()
        return self._base_asset
    
    @property 
    def quote_asset(self) -> str | None:
        """Get quote asset if this is a pair"""
        if self._quote_asset is None:
            self._parse_assets()
        return self._quote_asset
    
    @property
    def is_internal(self) -> bool:
        """Check if this is internal/canonical format"""
        return self.exchange is None
        
    @property
    def is_exchange(self) -> bool:
        """Check if this is exchange-specific"""
        return self.exchange is not None
    
    def _parse_assets(self) -> None:
        """Parse base and quote assets based on context"""
        if self.exchange:
            # Use exchange-specific transformer
            transformer = SYMBOL_TRANSFORMERS.get(self.exchange)
            if transformer:
                internal = transformer.exchange_to_internal(self.value)
                self._base_asset = internal.base_asset
                self._quote_asset = internal.quote_asset
                return
        
        # Internal format parsing
        if "_" in self.value:
            parts = self.value.split("_", 1)
            self._base_asset = parts[0]
            self._quote_asset = parts[1] if len(parts) > 1 else None
        else:
            self._base_asset = self.value
            self._quote_asset = None
    
    def to_internal(self) -> 'Symbol':
        """Convert to canonical format"""
        if self.is_internal:
            return self
            
        # Use transformer to convert
        transformer = SYMBOL_TRANSFORMERS.get(self.exchange)
        if transformer:
            internal = transformer.exchange_to_internal(self.value)
            return Symbol(
                value=internal.value,
                exchange=None,
                _base_asset=internal.base_asset,
                _quote_asset=internal.quote_asset
            )
        return self
    
    def for_exchange(self, exchange: ExchangeName) -> 'Symbol':
        """Get representation for specific exchange"""
        if self.exchange == exchange:
            return self
            
        # Convert via internal first
        internal = self.to_internal()
        transformer = SYMBOL_TRANSFORMERS.get(exchange)
        if transformer:
            # Create InternalSymbol for transformer
            from cyberdelta.core.symbols.models import create_internal_symbol
            internal_sym = create_internal_symbol(
                value=internal.value,
                base_asset=internal.base_asset,
                quote_asset=internal.quote_asset
            )
            exchange_value = transformer.internal_to_exchange(internal_sym)
            return Symbol(value=exchange_value, exchange=exchange)
        return Symbol(value=self.value, exchange=exchange)
    
    # Critical: Make it work with string-based code
    def __str__(self) -> str:
        """Allow use in string contexts"""
        return self.value
        
    def __hash__(self) -> int:
        """Allow use as dict key"""
        return hash((self.value, self.exchange))
        
    def __eq__(self, other: object) -> bool:
        """Equality comparison"""
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, Symbol):
            return self.value == other.value and self.exchange == other.exchange
        return False
    
    @classmethod
    def from_string(cls, value: str, exchange: ExchangeName | None = None) -> 'Symbol':
        """Create from string value"""
        return cls(value=value, exchange=exchange)
    
    def to_websocket_format(self) -> str | int:
        """Convert to WebSocket format based on exchange"""
        if self.exchange == ExchangeName.BACKPACK and self.symbol_id is not None:
            return self.symbol_id
        if self.exchange == ExchangeName.HYPERLIQUID and self.value.startswith("@"):
            return self.value
        return self.value
```

## Why This Works

1. **One model, multiple contexts** - No confusion about which type to use
2. **String compatible** - Works with legacy `dict[str, Position]` via `__str__`
3. **Preserves exchange context** - Know where the symbol came from
4. **Lazy parsing** - Only parse assets when needed
5. **Easy conversion** - `.to_internal()` and `.for_exchange()` handle transformations
6. **Type safe** - Still Pydantic with validation
7. **Exchange agnostic** - Metadata fields are optional/sparse

## Usage Examples

```python
# Creating symbols
btc_hl = Symbol(value="BTC-PERP", exchange=ExchangeName.HYPERLIQUID)
btc_bp = Symbol(value="BTC_USDC_PERP", exchange=ExchangeName.BACKPACK)
btc_internal = Symbol(value="BTC_PERP", exchange=None)  # Internal/canonical

# Converting between contexts
internal = btc_hl.to_internal()  # Symbol(value="BTC_PERP", exchange=None)
bp_version = internal.for_exchange(ExchangeName.BACKPACK)  # Symbol(value="BTC_USDC_PERP", exchange=BACKPACK)

# Working with legacy string code
positions: dict[str, Position] = {}
positions[str(btc_hl)] = position  # Works!
symbol_str = str(btc_hl)  # "BTC-PERP"

# Accessing parsed components
print(btc_hl.base_asset)  # "BTC" (lazy parsed)
print(btc_hl.quote_asset)  # "USD" (lazy parsed)

# Exchange-specific metadata
btc_hl_indexed = Symbol(
    value="@1", 
    exchange=ExchangeName.HYPERLIQUID,
    asset_index=1
)

# At RAW boundary
raw_request = {
    "symbol": str(btc_hl)  # Just the string value
}
```

## Migration Path

1. **Replace ExchangeSymbol with Symbol** - Mostly mechanical replacement
2. **Add exchange parameter** - Where creating symbols, specify the exchange
3. **Legacy string code works** - Via `__str__` method
4. **Update gradually** - Business logic can be updated to use Symbol directly over time

## Benefits Over Current Architecture

1. **No boundary confusion** - One type everywhere
2. **Backwards compatible** - Works with string-expecting code
3. **Clear context** - Exchange field shows where symbol came from
4. **No circular dependencies** - Symbol doesn't reference other symbol types
5. **Solves impedance mismatch** - Works as both object and string

## Comparison to Previous Approaches

- **V1 (Current)**: 3 models with unclear boundaries → confusion
- **V2**: God object with everything → too complex
- **V3**: Separate data/behavior → still multiple models
- **V4**: One model, context-aware → simple and clear

This solves the systemic inconsistency by having ONE type that works everywhere, while still preserving the exchange context and allowing transformations when needed.