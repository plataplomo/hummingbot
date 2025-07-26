# Gradual Symbol Domain Migration: Current State After Git Resets & Better Approach

## 📊 Current State Analysis After Git Resets (Deep Research Completed)

### ✅ What Infrastructure Exists (The Good)

#### 1. **Domain Symbol System (100% Complete)**
- `cyberdelta/core/symbols/` - Fully implemented with domain-driven design
- **Models**: `InternalSymbol`, `ExchangeSymbol`, `UnifiedSymbol` - Well-designed
- **Service**: `SymbolService` with proper validation and transformation
- **Test Factories**: Complete factories in `tests/factories/symbol_factories.py`

#### 2. **Main Application Integration**
- `main.py` correctly injects `SymbolService` into components
- Core handlers accept symbol service in constructors

### ❌ What's Actually Still Using Strings (The Reality - 95%+ of codebase)

#### 1. **Core Models - CRITICAL ISSUE**
```python
# Order model (cyberdelta/core/models/market/order.py:72)
class Order(BaseModel):
    symbol: str = Field(..., description="Trading symbol.")  # STILL STRING!
    # NO symbol_domain field exists
```

#### 2. **Service Arguments - All String-Based**
```python
# PlaceOrderArgs (cyberdelta/apis/models/service_args_models.py:95)
class PlaceOrderArgs(BaseModel):
    symbol: str  # STILL STRING!
    # No domain object usage
```

#### 3. **API Layer - 100% String-Based**
```python
# Request builders (hl_trading_request_builder.py:551)
def build_place_order_payload(symbol: str, ...) -> dict:
    # String parameters only

# Order mappers (hl_order_mapper.py:687)
order_data = {
    "symbol": raw_order.asset,  # STRING!
}
```

#### 4. **Tests - Hardcoded String Disaster**
- **297+ occurrences** of hardcoded `"BTC-PERP"`, `"ETH-PERP"` etc.
- Test factories exist but largely unused
- Every test file has different hardcoded formats

### 🎯 The Core Problem: No Domain Objects Actually Flow Through System

**Current Flow**:
```
User Input (string) → Services (string) → APIs (string) → External Exchange
```

**Desired Flow**:
```
User Input → Domain Validation → Services (domain) → APIs (domain→string) → External Exchange
```

## 🚨 Why Previous Migration Attempts Failed

### 1. **Attempted Parallel Systems (The "Backwards Madness")**
Previous attempts created dual systems with:
- Migration helpers accepting `Union[str, DomainSymbol]`
- Both string and domain methods existing side-by-side
- Services converting domain objects back to strings

### 2. **No Core Model Changes**
- Order model never got a `symbol_domain` field
- Service arguments stayed string-based
- No actual domain objects flowing through system

### 3. **Scale Overwhelm**
- 297+ hardcoded test strings
- 100+ API methods to change
- Attempted everything at once


## 🎯 New Approach: File-by-File Clean Migration

### Core Principle: **Transform One File Completely Before Moving to Next**

Based on your requirements:
1. **Breaking clean refactor** - No backwards compatibility
2. **File-by-file or layer-by-layer** - Complete one unit before moving on
3. **Fully Pydantic domain-driven** - Domain objects throughout
4. **No double-layers** - Single clean implementation

### The Strategy: **Surgical File Transformations**

## 📋 File-by-File Migration Plan

### **Phase 1: Core Model Foundation** (Start Here - Everything Depends on This)
**Files**: 2 files, ~50 lines to change

#### 1.1 Update Order Model
```python
# cyberdelta/core/models/market/order.py
class Order(BaseModel):
    # CHANGE THIS:
    symbol: str = Field(...)  # DELETE
    # TO THIS:
    symbol: ExchangeSymbol = Field(..., description="Exchange-specific symbol")
    # Note: We use ExchangeSymbol not UnifiedSymbol because orders are exchange-specific
```

#### 1.2 Update Service Arguments
```python
# cyberdelta/apis/models/service_args_models.py
class PlaceOrderArgs(BaseModel):
    # CHANGE THIS:
    symbol: str  # DELETE
    # TO THIS:
    symbol: ExchangeSymbol
```

**Why Start Here**: Every other component depends on these models. Changing them first forces all other code to adapt.

### **Phase 2: Order Creation Flow** (One Direction at a Time)
**Files**: ~10 mapper files

#### 2.1 Fix Order Mappers (Hyperliquid)
```python
# cyberdelta/apis/hyperliquid/mappers/trading/hl_order_mapper.py
def _create_order_from_components(self, raw_order, components) -> Order:
    # Parse symbol to domain object
    exchange_symbol = ExchangeSymbol(
        value=raw_order.asset,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    order_data = {
        "symbol": exchange_symbol,  # Now domain object!
        # ... rest of fields
    }
```

#### 2.2 Fix Order Mappers (Backpack)
Same pattern for Backpack mappers - parse string to ExchangeSymbol

**Why This Order**: Orders flow FROM exchanges TO our system. Fix the entry points first.

### **Phase 3: Order Placement Flow** (Other Direction)
**Files**: ~5 service files, ~5 request builder files

#### 3.1 Update Order Placement Services
```python
# cyberdelta/apis/hyperliquid/services/trading/hl_order_placement_service.py
async def place_order(self, args: PlaceOrderArgs) -> Order:
    # args.symbol is now ExchangeSymbol
    # No more string extraction needed!

    # Request builder converts to string at boundary:
    payload = self.request_builder.build_place_order_payload(
        symbol=str(args.symbol),  # Convert ONLY at API boundary
        # ... other args
    )
```

### **Phase 4: Market Data Flow**
**Files**: ~20 mapper/service files for tickers, order books, etc.

Same pattern - parse exchange strings to domain objects at entry points.

### **Phase 5: Test Migration** (The Big One)
**Files**: 100+ test files with 297+ hardcoded strings

#### 5.1 Migration Strategy
```python
# Before:
test_symbol = "BTC-PERP"

# After:
test_symbol = ExchangeSymbolFactory.create_hyperliquid("BTC-PERP")
# Or better:
test_symbol = ExchangeSymbolFactory.btc_perp_hyperliquid()
```

#### 5.2 Batch by Test Directory
- `tests/unit/apis/hyperliquid/` - Do all HL tests together
- `tests/unit/apis/backpack/` - Do all BP tests together
- `tests/integration/` - Update integration tests last

## 🔧 Implementation Guidelines

### 1. **One File at a Time**
- Complete ALL changes in a file before moving to next
- Run tests after each file change

### 2. **Domain Objects Throughout**
```python
# Input validation at system boundaries
def handle_user_request(symbol_str: str) -> Order:
    # Parse to domain object immediately
    symbol = ExchangeSymbol(value=symbol_str, exchange_id=ExchangeName.HYPERLIQUID)

    # Everything else uses domain object
    args = PlaceOrderArgs(symbol=symbol, ...)
    return await order_service.place_order(args)
```

### 3. **String Conversion Only at External Boundaries**
```python
# API request builder (external boundary)
def build_api_payload(symbol: ExchangeSymbol) -> dict:
    return {
        "coin": symbol.value,  # Convert to string ONLY here
        # ... other fields
    }
```

### 4. **No Backwards Compatibility**
- Delete old code, don't keep it around
- Force all callers to update
- Use type checker to find all usage points

## 📊 Concrete File Order (Based on Dependencies)

### Week 1: Foundation
1. `cyberdelta/core/models/market/order.py` - Add ExchangeSymbol field
2. `cyberdelta/apis/models/service_args_models.py` - Update all Args classes
3. `cyberdelta/apis/hyperliquid/mappers/trading/hl_order_mapper.py` - Parse symbols
4. `cyberdelta/apis/backpack/mappers/trading/bp_order_mapper.py` - Parse symbols
5. `cyberdelta/apis/hyperliquid/services/trading/hl_order_placement_service.py`
6. `cyberdelta/apis/backpack/services/trading/bp_order_placement_service.py`

### Week 2: Expand Coverage
7-15. All other order-related mappers and services
16-25. Market data mappers (tickers, order books)
26-35. Market data services

### Week 3-4: Test Migration
36-136. Test files in batches by directory

## ✅ Success Metrics

### Per-File Completion
- [ ] All string parameters replaced with domain objects
- [ ] No `Union[str, ExchangeSymbol]` types
- [ ] Tests pass for that file
- [ ] Type checker (mypy) passes

### Overall Success
- **0 string symbol operations** except at API boundaries
- **100% domain object usage** in business logic
- **All tests using factories** not hardcoded strings
- **Clean architecture** with no dual systems

## 🚀 Why This Approach Will Work

### 1. **Forced Migration**
- Changing core models first forces everything else to adapt
- Can't accidentally keep using strings
- Type checker finds all usage points

### 2. **Clear Progress**
- Each file is either done or not done
- No partial states or dual systems
- Visible progress with each file completion

### 3. **Manageable Scope**
- One file at a time is psychologically manageable
- Can complete 5-10 files per day
- See constant progress

### 4. **No "Backwards Madness"**
- No Union types or migration helpers
- Clean breaks at each step
- Single implementation path

## 📝 Example: First File Migration

### File: `cyberdelta/core/models/market/order.py`

```python
# Step 1: Add import
from cyberdelta.core.symbols.models import ExchangeSymbol

# Step 2: Change field
class Order(BaseModel):
    # OLD: symbol: str = Field(..., description="Trading symbol.")
    symbol: ExchangeSymbol = Field(..., description="Exchange-specific trading symbol")

# Step 3: Update validator if needed
@field_validator("symbol", mode="before")
@classmethod
def ensure_exchange_symbol(cls, v: Any) -> ExchangeSymbol:
    if isinstance(v, ExchangeSymbol):
        return v
    if isinstance(v, str):
        # For migration period only - parse string to ExchangeSymbol
        # This allows existing code to keep working during migration
        return ExchangeSymbol(value=v, exchange_id=ExchangeName.UNKNOWN)
    raise ValueError(f"Invalid symbol type: {type(v)}")
```

### Result:
- Order model now requires ExchangeSymbol
- All code creating Orders must provide domain objects
- Type checker will find all places needing updates

## 🎯 Summary

### Current State (After Git Resets)
- ✅ Symbol infrastructure complete
- ❌ 95%+ of codebase still uses strings
- ❌ No domain objects flowing through system
- ❌ 297+ hardcoded test strings

### Migration Strategy
1. **File-by-file** transformation
2. **Start with core models** (forces everything to adapt)
3. **No backwards compatibility** (clean breaks)
4. **Domain objects throughout** (except API boundaries)

### First Steps
1. Change `Order.symbol` to `ExchangeSymbol`
2. Change `PlaceOrderArgs.symbol` to `ExchangeSymbol`
3. Fix order mappers to create domain objects
4. Continue file by file...

### Timeline
- **Week 1**: Core models and order flow
- **Week 2**: Market data and remaining services
- **Week 3-4**: Test migration (the big effort)

### Key Success Factor
**No dual systems, no migration helpers, no Union types**. Just clean, breaking changes one file at a time.

---

*Remember: "If it has to be a slow step-by step progress file by file or layer by layer so be it!"* - This is exactly that approach.
