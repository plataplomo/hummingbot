# Symbol Model V3 - Incremental Simplification

## Overview

This document outlines an incremental approach to simplifying the symbol system while maintaining type safety and Pydantic models. The goal is to reduce complexity without radical changes.

## Current Problems

1. **Complex inheritance hierarchy**: BaseSymbol → InternalSymbol/ExchangeSymbol
2. **Behavior mixed with data**: Models contain parsing logic, validation, transformation
3. **Circular dependencies**: ExchangeSymbol references InternalSymbol
4. **God object tendencies**: Models trying to do too much
5. **Unused complexity**: UnifiedSymbol rarely used but adds mental overhead

## Proposed Solutions

### Step 1: Simplify the Model Hierarchy (Keep Pydantic)

Instead of BaseSymbol → InternalSymbol/ExchangeSymbol inheritance, we could have:

```python
from typing import Generic, TypeVar
from pydantic import BaseModel, Field

# Type-safe exchange metadata using generics
T = TypeVar('T', bound=BaseModel)

class ExchangeMetadata(BaseModel):
    """Base for exchange-specific metadata"""
    pass

class HyperliquidMetadata(ExchangeMetadata):
    asset_index: int | None = Field(default=None, ge=0)
    
class BackpackMetadata(ExchangeMetadata):
    symbol_id: int | None = Field(default=None, ge=0)

# Single Symbol model with generic metadata
class Symbol(BaseModel, Generic[T]):
    """A symbol as it exists in a specific context"""
    value: str
    exchange_id: ExchangeName
    metadata: T | None = None  # Type-safe, exchange-agnostic
    
    class Config:
        frozen = True
```

This gives us type safety without god objects.

### Step 2: Move Behavior Out of Models

Extract all parsing/transformation logic to dedicated services:

```python
# models.py - just data
class Symbol(BaseModel):
    value: str
    exchange_id: ExchangeName
    metadata: ExchangeMetadata | None = None

# asset_parser.py - behavior
class AssetParser:
    def parse_assets(self, symbol: Symbol) -> tuple[str, str | None]:
        """Parse base/quote assets based on exchange rules"""
        transformer = SYMBOL_TRANSFORMERS.get(symbol.exchange_id)
        if transformer:
            internal = transformer.exchange_to_internal(symbol.value)
            return internal.base_asset, internal.quote_asset
        return symbol.value, None
```

### Step 3: Simplify Cross-Exchange Mapping

Instead of UnifiedSymbol with embedded ExchangeSymbols, use a registry:

```python
class SymbolRegistry:
    """Manages symbol relationships without complex models"""
    
    def register_equivalence(self, canonical: str, symbols: list[Symbol]) -> None:
        """Register that these symbols represent the same instrument"""
        
    def find_equivalent(self, symbol: Symbol, target_exchange: ExchangeName) -> Symbol | None:
        """Find equivalent symbol on another exchange"""
```

### Step 4: Consolidate Validation

Move all validation to a single place instead of scattered across models:

```python
class SymbolValidator:
    def validate(self, symbol: Symbol) -> list[ValidationError]:
        """Single entry point for all validation"""
        errors = []
        
        # Format validation
        if not self._validate_format(symbol):
            errors.append(...)
            
        # Exchange-specific validation
        if not self._validate_exchange_rules(symbol):
            errors.append(...)
            
        # Metadata validation
        if symbol.metadata and not self._validate_metadata(symbol):
            errors.append(...)
            
        return errors
```

## Benefits of This Approach

1. **Simpler models** - Symbol is just data, no complex inheritance
2. **Type-safe metadata** - Using generics for exchange-specific data
3. **Clear separation** - Models vs behavior vs validation
4. **No circular deps** - Symbol doesn't reference other symbol types
5. **Extensible** - Easy to add new exchanges with their metadata types
6. **Backward compatible** - Can migrate gradually

## Next Concrete Step

Start with making ExchangeSymbol simpler and more focused:

```python
class ExchangeSymbol(BaseModel):
    """A symbol as it exists on a specific exchange"""
    
    model_config = ConfigDict(frozen=True)
    
    value: str = Field(..., min_length=1, max_length=30)
    exchange_id: ExchangeName
    
    # Exchange-agnostic metadata
    metadata: dict[str, Any] = Field(default_factory=dict)
    
    def __str__(self) -> str:
        return self.value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Type-safe metadata access"""
        return self.metadata.get(key, default)
```

This is a small step that:
- Keeps Pydantic
- Removes complex behavior  
- Makes metadata exchange-agnostic
- Maintains backward compatibility

## Migration Strategy

1. **Phase 1**: Simplify ExchangeSymbol (remove inheritance, complex behavior)
2. **Phase 2**: Extract behavior to services (AssetParser, SymbolValidator)
3. **Phase 3**: Replace UnifiedSymbol with SymbolRegistry
4. **Phase 4**: Consolidate remaining complexity

Each phase can be done independently without breaking existing code.

## Example Usage After Migration

```python
# Creating symbols
btc_hl = ExchangeSymbol(
    value="BTC-PERP",
    exchange_id=ExchangeName.HYPERLIQUID,
    metadata={"asset_index": 0}
)

btc_bp = ExchangeSymbol(
    value="BTC_USDC_PERP", 
    exchange_id=ExchangeName.BACKPACK,
    metadata={"symbol_id": 12345}
)

# Using services for behavior
parser = AssetParser()
base, quote = parser.parse_assets(btc_hl)  # Returns ("BTC", "USD")

# Registry for relationships
registry = SymbolRegistry()
registry.register_equivalence("BTC_PERP", [btc_hl, btc_bp])

# Find equivalent on another exchange
equivalent = registry.find_equivalent(btc_hl, ExchangeName.BACKPACK)
# Returns btc_bp
```

## Conclusion

This incremental approach maintains the benefits of the current system while reducing complexity. By separating data from behavior and using composition over inheritance, we get a cleaner, more maintainable architecture that's easier to understand and extend.