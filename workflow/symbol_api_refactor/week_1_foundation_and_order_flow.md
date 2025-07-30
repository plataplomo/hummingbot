# Week 1: Symbol API Clean Architecture Implementation
**Status: Foundation Complete | Focus: Verify and Document Clean Implementation**

## 🎯 Current Status Assessment

**ALREADY COMPLETED** ✅:
- Symbol model with type-safe metadata: `BaseSymbol[TMetadata]`
- Clean union type: `type Symbol = BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata]`
- Registry pattern implemented: `exchanges.hyperliquid()`, `exchanges.backpack()`, `symbol()`
- All mappers create Symbol objects at entry points
- Service args validate Symbol domain objects

**NO BACKWARD COMPATIBILITY** ⚡:
- No string symbol fields in models
- No migration helpers or converters
- Clean break from old architecture
- Symbol objects throughout the system

## 📊 Architecture Overview

### 🚨 API Boundary Rule: RAW Models = String Boundaries
**CRITICAL**: RAW models from exchanges are the ONLY place where symbols are strings. Everything else in the system uses Symbol objects.

```python
# RAW models (API boundary) - symbols are strings here
class RawBackpackOrder:
    symbol: str  # ✅ String at API boundary
    side: str
    price: str

# Domain models (business logic) - symbols are Symbol objects
class Order(BaseModel):
    symbol: Symbol  # ✅ Symbol object in business logic
    side: OrderSide
    price: Decimal
```

### Symbol Model Structure
```python
# Core generic model with metadata
class BaseSymbol[TMetadata: SymbolMetadata](BaseModel):
    model_config = ConfigDict(frozen=True)
    
    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName
    metadata: TMetadata
    
    _components: SymbolComponents | None = PrivateAttr(default=None)

# Clean union type alias
type Symbol = BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata]
```

### Registry Pattern APIs
```python
# Three clean ways to create symbols:

# 1. Direct function (when exchange is dynamic)
from cyberdelta.core.symbols import symbol
btc = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)

# 2. Exchange namespace (cleanest for specific exchanges)
from cyberdelta.core.symbols import exchanges
btc = exchanges.hyperliquid("BTC-PERP")
btc = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)

# 3. Common symbols (cleanest for standard assets)
from cyberdelta.core.symbols import symbols
btc = symbols.BTC.hyperliquid()
btc = symbols.BTC.backpack()
```

## 🔧 Week 1 Implementation Tasks

### **Day 1: Verify Foundation**
**Focus**: Ensure all core models use Symbol objects

#### Core Model Verification
```bash
# Verify Order model uses Symbol
grep -n "symbol: Symbol" cyberdelta/core/models/market/order.py

# Verify no string symbols remain
grep -n "symbol: str" cyberdelta/core/models/market/*.py

# Run type checking
mypy cyberdelta/core/models/market/
```

#### Service Args Verification
```python
# File: cyberdelta/apis/models/service_args/trading.py
class PlaceOrderArgs(BaseModel):
    symbol: Symbol  # ✅ Domain object only
    
    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_domain(cls, v: Symbol, info: ValidationInfo) -> Symbol:
        if not isinstance(v, BaseSymbol):
            raise ValueError(f"Symbol must be Symbol, got {type(v).__name__}")
        return v
```

### **Day 2: Verify Mappers**
**Focus**: Ensure all mappers create Symbol objects from RAW string boundaries

#### 🚨 Mapper Responsibility: Convert Strings to Symbols
**CRITICAL**: Mappers are the ONLY place where string-to-Symbol conversion happens. They sit at the API boundary and transform RAW models (with string symbols) into domain models (with Symbol objects).

#### Backpack Mapper Pattern
```python
# File: cyberdelta/apis/backpack/mappers/trading/bp_order_mapper.py
from cyberdelta.core.symbols import exchanges

@staticmethod
def transform_raw_order_to_internal(raw_order: BackpackRawOrderResponse) -> Order:
    """Convert RAW order (string symbol) to domain Order (Symbol object)."""
    
    # RAW model has string symbol - this is the API boundary
    # raw_order.symbol is a string like "BTC_USD_PERP"
    
    # Convert string to Symbol object at this boundary
    exchange_symbol = exchanges.backpack(
        raw_order.symbol,  # String from RAW model (first positional arg is value)
        symbol_id=getattr(raw_order, "symbol_id", None)
    )
    
    # Use secure_transform to create domain model with Symbol object
    order_data = {
        "exchange_order_id": raw_order.id,
        "symbol": exchange_symbol,  # Symbol object for business logic!
        "side": mapped_side.value,
        # ... rest of fields
    }
    
    return secure_transform(
        data=order_data,
        model_class=Order,
        context="backpack_raw_order_transform",
        source_exchange="backpack",
    )
```

#### Hyperliquid Mapper Pattern
```python
# File: cyberdelta/apis/hyperliquid/mappers/trading/hl_order_mapper.py
from cyberdelta.core.symbols import exchanges

@staticmethod
def transform_raw_order_to_internal(raw_order: HyperliquidRawOrder) -> Order:
    """Convert RAW order (string asset) to domain Order (Symbol object)."""
    
    # RAW model has string asset - this is the API boundary
    # raw_order.asset is a string like "BTC"
    
    # Convert string to Symbol object at this boundary
    exchange_symbol = exchanges.hyperliquid(
        raw_order.asset,  # String from RAW model (first positional arg is value)
        asset_index=getattr(raw_order, "asset_index", None)
    )
    
    # Use secure_transform to create domain model with Symbol object
    order_data = {
        "exchange_order_id": str(raw_order.oid),
        "symbol": exchange_symbol,  # Symbol object for business logic!
        "side": components["side"].value,
        # ... rest of fields
    }
    
    return secure_transform(
        data=order_data,
        model_class=Order,
        context="hyperliquid_order_transform",
        source_exchange="hyperliquid",
    )
```

### **Day 3: Document Type Patterns**
**Focus**: Document proper type handling for union types

#### isinstance Checks
```python
# CORRECT: Use BaseSymbol for runtime checks
from cyberdelta.core.symbols.models import BaseSymbol

if isinstance(obj.symbol, BaseSymbol):
    # Symbol is valid domain object
    logger.info("Symbol", value=obj.symbol.value, exchange=obj.symbol.exchange)
```

#### String Operations
```python
# CORRECT: Use .value for string operations
symbol_parts = order.symbol.value.split("-")  # Access string via .value
symbol_upper = order.symbol.value.upper()      # String methods on .value
```

#### Exchange Access
```python
# CORRECT: Use .exchange property
if order.symbol.exchange == ExchangeName.HYPERLIQUID:
    # Hyperliquid-specific logic
elif order.symbol.exchange == ExchangeName.BACKPACK:
    # Backpack-specific logic
```

### **Day 4: Test and Validate**
**Focus**: Comprehensive validation

#### Type Safety
```bash
# All should pass with 0 errors
mypy cyberdelta/core/symbols/
mypy cyberdelta/apis/models/service_args/
mypy cyberdelta/apis/*/mappers/
mypy cyberdelta/core/models/
```

#### Integration Tests
```python
# Test symbol creation patterns
def test_symbol_creation_patterns():
    # Registry patterns
    btc1 = exchanges.hyperliquid("BTC-PERP")
    btc2 = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
    btc3 = symbols.BTC.hyperliquid()
    
    # All create valid Symbol objects
    assert isinstance(btc1, BaseSymbol)
    assert isinstance(btc2, BaseSymbol)
    assert isinstance(btc3, BaseSymbol)
    
    # All have same value and exchange
    assert btc1.value == btc2.value == btc3.value == "BTC-PERP"
    assert btc1.exchange == btc2.exchange == btc3.exchange == ExchangeName.HYPERLIQUID
```

## 🎯 Week 1 Success Criteria

### Type Safety ✅
- [x] `mypy cyberdelta/core/symbols/` passes with 0 errors
- [x] `mypy cyberdelta/apis/models/service_args/` passes with 0 errors  
- [x] `mypy cyberdelta/apis/*/mappers/` passes with 0 errors
- [x] No string symbol fields in domain models

### Clean Architecture ✅
- [x] Registry pattern provides clean APIs
- [x] All mappers create Symbol objects at entry points
- [x] Service args validate Symbol domain objects
- [x] No backward compatibility code

### Runtime Correctness ✅
- [x] isinstance checks use BaseSymbol not Symbol type alias
- [x] String operations use `.value` accessor
- [x] Exchange access uses `.exchange` property
- [x] Metadata access type-safe (e.g., `.metadata.asset_index`)

## 🚨 Critical Notes

**API Boundary Rule**: RAW models are the ONLY place where symbols are strings. These RAW models represent the exact data from exchange APIs. Everything else in the system uses Symbol objects.

**Mapper Responsibility**: Mappers sit at the API boundary and are responsible for converting string symbols from RAW models into Symbol objects for the rest of the system.

**Clean Break Only**: This architecture has NO backward compatibility. All business logic uses Symbol objects.

**Union Type Handling**: The `Symbol` type alias is a union. Use `BaseSymbol` for isinstance checks and access common properties (value, exchange) directly.

**Registry Pattern**: Three clean APIs for symbol creation - choose based on use case.

**Entry Point Pattern**: Mappers create Symbol objects from raw exchange data at entry points. All downstream code operates with domain objects.

## 📊 Week 1 Metrics

- **Symbol Creation Points**: All in mappers at API boundaries ✅
- **String Symbol Usage**: 0 in domain models ✅
- **Type Safety**: 100% with proper union handling ✅
- **API Consistency**: Registry pattern throughout ✅

**Week 1 establishes the clean Symbol architecture foundation - ready for market data and business logic layers!** 🚀