# Symbol Model V6 - Enhanced ExchangeSymbol

## Deep Analysis Complete

After analyzing the entire codebase, here's what I found:

### Current State
1. **Week 1&2 refactor used ExchangeSymbol exclusively** - All domain models (Ticker, OrderBook, Position) now use ExchangeSymbol
2. **Business logic still uses strings** - Portfolio tracker does `symbol.split("_")[-1]` to extract quote assets
3. **InternalSymbol/UnifiedSymbol are barely used** - Only in symbol service/store, not in actual business logic
4. **Symbol transformers work with InternalSymbol** - But everything else uses ExchangeSymbol

### What Must Be Kept
1. **ExchangeSymbol everywhere** - Can't rollback Week 1&2 refactor
2. **Exchange-specific metadata** - Hyperliquid asset indices, Backpack symbol IDs
3. **Symbol transformers** - Handle format conversion between exchanges
4. **Exchange context** - Need to know which exchange a symbol belongs to

### Core Problems
1. **No asset extraction from ExchangeSymbol** - Must go through transformers/InternalSymbol
2. **Impedance mismatch** - Domain uses objects, business logic expects strings
3. **Unclear boundaries** - When to use which symbol type?
4. **Behavior mixed with data** - ExchangeSymbol can't parse itself

## Proposed Solution: Enhanced ExchangeSymbol

Instead of 3 models with unclear boundaries, enhance ExchangeSymbol to be self-sufficient:

```python
from pydantic import BaseModel, Field, PrivateAttr
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

class SymbolComponents(BaseModel):
    """Parsed symbol components."""
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP

# Enhanced ExchangeSymbol - the ONE model to rule them all
class ExchangeSymbol(BaseSymbol):
    """Exchange symbol that knows its components and can work everywhere."""
    
    # Existing fields
    value: str
    exchange_id: ExchangeName
    asset_index: int | None = None  # Hyperliquid
    symbol_id: int | None = None    # Backpack
    
    # NEW: Parsed components (computed on first access)
    _components: SymbolComponents | None = PrivateAttr(default=None)
    
    @property
    def base_asset(self) -> str:
        """Get base asset, parsing if needed."""
        if not self._components:
            self._parse_components()
        return self._components.base_asset
    
    @property
    def quote_asset(self) -> str | None:
        """Get quote asset if this is a pair."""
        if not self._components:
            self._parse_components()
        return self._components.quote_asset
    
    @property
    def market_type(self) -> MarketType:
        """Get market type."""
        if not self._components:
            self._parse_components()
        return self._components.market_type
    
    def _parse_components(self) -> None:
        """Parse components using the exchange's transformer."""
        transformer = SYMBOL_TRANSFORMERS.get(self.exchange_id.value)
        if transformer:
            internal = transformer.exchange_to_internal(self.value)
            self._components = SymbolComponents(
                base_asset=internal.base_asset,
                quote_asset=internal.quote_asset,
                market_type=internal.market_type
            )
        else:
            # Fallback parsing
            self._components = self._parse_fallback()
    
    def _parse_fallback(self) -> SymbolComponents:
        """Fallback parsing when no transformer available."""
        # Simple heuristic parsing
        if "-PERP" in self.value or "_PERP" in self.value:
            base = self.value.replace("-PERP", "").replace("_PERP", "")
            return SymbolComponents(
                base_asset=base,
                quote_asset="USD",
                market_type=MarketType.PERP
            )
        
        # Try common separators
        for sep in ["_", "-", "/"]:
            if sep in self.value:
                parts = self.value.split(sep, 1)
                return SymbolComponents(
                    base_asset=parts[0],
                    quote_asset=parts[1] if len(parts) > 1 else None,
                    market_type=MarketType.SPOT
                )
        
        # Single asset
        return SymbolComponents(
            base_asset=self.value,
            quote_asset=None,
            market_type=MarketType.SPOT
        )
    
    # Make it work with legacy string code
    def __str__(self) -> str:
        return self.value
    
    # Conversion methods
    def to_internal(self) -> str:
        """Get internal/canonical format."""
        if self.quote_asset:
            return f"{self.base_asset}_{self.quote_asset}"
        return self.base_asset
    
    def for_exchange(self, target: ExchangeName) -> 'ExchangeSymbol':
        """Convert to another exchange's format."""
        if target == self.exchange_id:
            return self
        
        # Use transformers for conversion
        transformer_from = SYMBOL_TRANSFORMERS.get(self.exchange_id.value)
        transformer_to = SYMBOL_TRANSFORMERS.get(target.value)
        
        if transformer_from and transformer_to:
            # Convert to internal first
            internal = transformer_from.exchange_to_internal(self.value)
            # Then to target exchange
            target_value = transformer_to.internal_to_exchange(internal)
            return ExchangeSymbol(
                value=target_value,
                exchange_id=target
            )
        
        # Fallback: assume same format
        return ExchangeSymbol(
            value=self.value,
            exchange_id=target
        )
```

### Benefits
1. **One model everywhere** - No confusion about which to use
2. **Self-contained** - Can extract assets without external dependencies
3. **Backward compatible** - Works as string via `__str__`
4. **Lazy parsing** - Components only parsed when needed
5. **Incremental change** - Keep existing ExchangeSymbol usage
6. **No god object** - Just adds missing asset extraction capability

### Usage Examples

```python
# Current problematic code in portfolio_tracker.py
pnl_quote_asset = position.symbol.split("_")[-1]  # String manipulation

# With enhanced ExchangeSymbol
pnl_quote_asset = position.symbol.quote_asset  # Clean property access

# String compatibility still works
positions_dict[str(symbol)] = position  # Works via __str__

# Asset checking
if symbol.base_asset == "BTC":  # Direct access
    ...

# Exchange conversion
btc_hl = ExchangeSymbol(value="BTC-PERP", exchange_id=ExchangeName.HYPERLIQUID)
btc_bp = btc_hl.for_exchange(ExchangeName.BACKPACK)  # "BTC_PERP"
```

### Migration Path
1. **Phase 1**: Add component properties to existing ExchangeSymbol class
2. **Phase 2**: Update business logic to use properties instead of string manipulation
3. **Phase 3**: Gradually reduce InternalSymbol usage to transformer internals only
4. **Phase 4**: Simplify UnifiedSymbol to just a registry/cache

### Why This Works
1. **Minimal change** - Just enhancing existing ExchangeSymbol
2. **Preserves Week 1&2 work** - All ExchangeSymbol usage remains valid
3. **Solves real problems** - Asset extraction, string compatibility
4. **Clear boundary** - ExchangeSymbol is THE domain object
5. **Type safe** - Pydantic validation still applies
6. **Exchange agnostic** - Components are parsed based on exchange rules

This is the "one step at a time" approach that enhances what's already working rather than introducing new complexity.