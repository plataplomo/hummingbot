# Symbol Model V5 - Data and Behavior Separation

## Core Insight

Looking at the current models:
- **ExchangeSymbol** has value + exchange context ✓
- **InternalSymbol** has asset parsing ✓
- **UnifiedSymbol** has cross-exchange mapping ✓

The problem isn't that we need to redesign - it's that behavior is mixed into data models.

## The Solution: Pure Data Models + Behavior Protocols

### Data Models (Pure Data)

```python
from pydantic import BaseModel, Field, ConfigDict
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

class Symbol(BaseModel):
    """Just the data - no behavior"""
    
    model_config = ConfigDict(frozen=True)
    
    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName | None = None  # None = canonical/internal
    
    # Metadata as structured data, not behavior
    components: SymbolComponents | None = None
    metadata: SymbolMetadata | None = None
    
    def __str__(self) -> str:
        """String representation for compatibility"""
        return self.value
    
    def __hash__(self) -> int:
        """Allow use as dict key"""
        return hash((self.value, self.exchange))

class SymbolComponents(BaseModel):
    """Parsed components - data, not behavior"""
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP

class SymbolMetadata(BaseModel):
    """Exchange-specific data"""
    asset_index: int | None = None  # Hyperliquid
    symbol_id: int | None = None    # Backpack
    # Can extend with more exchange-specific fields

class SymbolMapping(BaseModel):
    """Cross-exchange mapping data"""
    canonical_value: str
    exchange_values: dict[ExchangeName, str]
```

### Behavior Protocols (Pure Behavior)

```python
from typing import Protocol

class SymbolParser(Protocol):
    """Parse symbols into components"""
    def parse(self, symbol: Symbol) -> SymbolComponents:
        """Extract base/quote assets from symbol"""
        ...

class SymbolConverter(Protocol):
    """Convert symbols between exchanges"""
    def convert(self, symbol: Symbol, target: ExchangeName) -> Symbol:
        """Convert symbol to target exchange format"""
        ...

class SymbolResolver(Protocol):
    """Resolve symbol relationships"""
    def get_canonical(self, symbol: Symbol) -> Symbol:
        """Get canonical representation"""
        ...
    
    def find_equivalent(self, symbol: Symbol, exchange: ExchangeName) -> Symbol | None:
        """Find equivalent on another exchange"""
        ...

class SymbolValidator(Protocol):
    """Validate symbol formats"""
    def validate(self, symbol: Symbol) -> list[ValidationError]:
        """Validate symbol based on context"""
        ...
```

### Behavior Implementations

```python
class ExchangeSymbolParser:
    """Parser that uses exchange-specific rules"""
    
    def __init__(self, transformers: dict[ExchangeName, SymbolTransformer]):
        self.transformers = transformers
    
    def parse(self, symbol: Symbol) -> SymbolComponents:
        if symbol.exchange and symbol.exchange in self.transformers:
            # Use exchange-specific parsing
            transformer = self.transformers[symbol.exchange]
            internal = transformer.exchange_to_internal(symbol.value)
            return SymbolComponents(
                base_asset=internal.base_asset,
                quote_asset=internal.quote_asset,
                market_type=internal.market_type
            )
        
        # Default parsing for canonical format
        if "_" in symbol.value:
            parts = symbol.value.split("_", 1)
            return SymbolComponents(
                base_asset=parts[0],
                quote_asset=parts[1] if len(parts) > 1 else None
            )
        
        return SymbolComponents(base_asset=symbol.value)

class SymbolConverterService:
    """Convert symbols between exchanges"""
    
    def __init__(self, transformers: dict[ExchangeName, SymbolTransformer]):
        self.transformers = transformers
    
    def convert(self, symbol: Symbol, target: ExchangeName) -> Symbol:
        # First get canonical form
        canonical = self._to_canonical(symbol)
        
        # Then convert to target
        if target in self.transformers:
            transformer = self.transformers[target]
            target_value = transformer.internal_to_exchange(canonical.value)
            return Symbol(value=target_value, exchange=target)
        
        return Symbol(value=canonical.value, exchange=target)
```

## Usage Examples

```python
# Pure data creation
btc_hl = Symbol(
    value="BTC-PERP",
    exchange=ExchangeName.HYPERLIQUID,
    metadata=SymbolMetadata(asset_index=0)
)

# Behavior via services
parser = ExchangeSymbolParser(SYMBOL_TRANSFORMERS)
components = parser.parse(btc_hl)
# components.base_asset = "BTC"
# components.quote_asset = "USD"

converter = SymbolConverterService(SYMBOL_TRANSFORMERS)
btc_bp = converter.convert(btc_hl, ExchangeName.BACKPACK)
# btc_bp.value = "BTC_USDC_PERP"
# btc_bp.exchange = BACKPACK

# At API boundaries
raw_request = {
    "symbol": str(btc_hl)  # "BTC-PERP"
}
```

## Benefits

1. **Clear separation** - Data models have no behavior
2. **Type-safe behavior** - Protocols define contracts
3. **Testable** - Can mock protocols easily
4. **Extensible** - Add new behavior without changing models
5. **No god objects** - Each class has single responsibility

## Migration from Current Architecture

1. **Keep existing models** for backward compatibility
2. **Create Symbol from ExchangeSymbol**:
   ```python
   def from_exchange_symbol(es: ExchangeSymbol) -> Symbol:
       return Symbol(
           value=es.value,
           exchange=es.exchange_id,
           metadata=SymbolMetadata(
               asset_index=es.asset_index,
               symbol_id=es.symbol_id
           )
       )
   ```
3. **Gradually introduce services** for behavior
4. **Deprecate behavior methods** on models

## Key Differences from V4

- **V4**: One model with all behavior → god object
- **V5**: Pure data models + separate behavior protocols → clean separation

This approach:
- Keeps data and behavior separate
- Uses Pydantic for data validation
- Uses protocols for behavior contracts
- Is type-safe and exchange-agnostic