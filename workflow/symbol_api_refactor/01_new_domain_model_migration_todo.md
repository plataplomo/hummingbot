# Symbol Domain Model Migration: Post-Reset State & File-by-File Action Plan

## 🔍 Current State Analysis (After Git Resets)

### Migration Status: **INFRASTRUCTURE COMPLETE, IMPLEMENTATION 5%**

The symbol domain infrastructure is fully built and excellent. However, virtually the entire codebase still uses string-based symbols. This document provides the current state and a file-by-file migration plan as requested.

## 📊 What Actually Exists vs What Needs To Be Done

### ✅ Completed Infrastructure (Ready to Use)

#### 1. **Symbol Domain Models** (100% Complete)
```python
# cyberdelta/core/symbols/models.py
- InternalSymbol(base_asset, quote_asset, market_type)
- ExchangeSymbol(value, exchange_id)
- UnifiedSymbol(internal, exchanges)
```

#### 2. **Symbol Service** (100% Complete)
```python
# cyberdelta/core/symbols/service.py
- Full transformation logic between formats
- Validation and error handling
- Store and registry functionality
```

#### 3. **Test Factories** (100% Complete)
```python
# tests/factories/symbol_factories.py
- InternalSymbolFactory
- ExchangeSymbolFactory
- UnifiedSymbolFactory
```

#### 4. **Main App Integration**
- `main.py` correctly injects SymbolService into core handlers

### ❌ What Still Uses Strings (95%+ of Codebase)

#### 1. **Core Models - THE CRITICAL BLOCKER**
```python
# cyberdelta/core/models/market/order.py:72
class Order(BaseModel):
    symbol: str = Field(...)  # STILL STRING!
    # NO symbol_domain field

# cyberdelta/apis/models/service_args_models.py:95
class PlaceOrderArgs(BaseModel):
    symbol: str  # STILL STRING!
```

#### 2. **API Layer - 100% String-Based**
```python
# Request builders accept strings:
def build_place_order_payload(symbol: str, ...) -> dict

# Mappers create Orders with strings:
order_data = {"symbol": raw_order.asset}  # STRING!
```

#### 3. **Tests - 297+ Hardcoded Strings**
```python
# Found in every test file:
test_symbol = "BTC-PERP"
test_symbol = "ETH-PERP"
# Factories exist but unused!
```

#### 4. **All Other Components**
- Services, strategies, risk management - all use strings
- WebSocket handlers - strings
- Monitoring - strings

## 🔥 The Core Problem

### No Domain Objects Actually Flow Through The System

```
Current Reality:
String → String → String → String → External API (string)

What We Built Infrastructure For:
Domain Object → Domain Object → Domain Object → String (only at API boundary)
```

The infrastructure is there, but nothing uses it!

## 📋 File-by-File Migration Plan (As Requested)

### Migration Approach: **Complete One File Before Moving to Next**
- **No backwards compatibility**
- **Breaking changes are OK** (and required!)
- **Domain objects throughout**
- **File by file progress**

### Phase 1: Core Models (Start Here - Forces Everything Else)

#### File 1: `cyberdelta/core/models/market/order.py`
```python
# Change:
symbol: str = Field(...)
# To:
symbol: ExchangeSymbol = Field(...)
```
**Impact**: Every Order creation must now use domain objects

#### Files 2-8: Service Args Domain Modules (REORGANIZED STRUCTURE)
**NOTE: Service args have been reorganized into domain-specific modules**

```python
# cyberdelta/apis/models/service_args/trading.py
class PlaceOrderArgs(BaseModel):
    symbol: ExchangeSymbol  # Was str

class CancelOrderArgs(BaseModel):
    symbol: ExchangeSymbol  # Was str

# cyberdelta/apis/models/service_args/market_data.py
class GetTickerArgs(BaseModel):
    symbol: ExchangeSymbol  # Was str

class GetOrderBookArgs(BaseModel):
    symbol: ExchangeSymbol  # Was str

# cyberdelta/apis/models/service_args/account.py
# ... plus account-related args

# cyberdelta/apis/models/service_args/hyperliquid.py
# ... plus Hyperliquid-specific args

# cyberdelta/apis/models/service_args/backpack.py
# ... plus Backpack-specific args

# cyberdelta/apis/models/service_args/internal.py
# ... plus internal system args
```
**Impact**: All service calls must use domain objects across **6 modules** instead of 1

### Phase 2: Order Flow Files (Fix How Orders Enter System)

#### Files 3-6: Order Mappers
- `cyberdelta/apis/hyperliquid/mappers/trading/hl_order_mapper.py`
- `cyberdelta/apis/backpack/mappers/trading/bp_order_mapper.py`
- `cyberdelta/apis/hyperliquid/mappers/trading/hl_order_response_mapper.py`
- `cyberdelta/apis/backpack/mappers/trading/bp_order_response_mapper.py`

```python
# In _create_order_from_components:
exchange_symbol = ExchangeSymbol(
    value=raw_order.asset,
    exchange_id=ExchangeName.HYPERLIQUID
)
order_data = {
    "symbol": exchange_symbol,  # Domain object!
}
```

#### Files 7-10: Order Services
- `cyberdelta/apis/hyperliquid/services/trading/hl_order_placement_service.py`
- `cyberdelta/apis/backpack/services/trading/bp_order_placement_service.py`
- `cyberdelta/apis/hyperliquid/services/trading/hl_order_cancellation_service.py`
- `cyberdelta/apis/backpack/services/trading/bp_order_cancellation_service.py`

```python
async def place_order(self, args: PlaceOrderArgs) -> Order:
    # args.symbol is now ExchangeSymbol
    # Convert to string ONLY at API boundary:
    payload = self.request_builder.build_place_order_payload(
        symbol=str(args.symbol),  # String only here!
    )
```

### Phase 3: Market Data Flow (Similar Pattern)

#### Files 11-20: Market Data Mappers
- All ticker mappers (4 files)
- All order book mappers (4 files)
- All trade mappers (4 files)
- All funding rate mappers (4 files)

Same pattern: Parse exchange strings to domain objects at mapper level.

### Phase 4: Complete Service Layer

#### Files 21-40: All Remaining Services
- Market data services
- Account services
- Position services
- Balance services

Update all to accept and return domain objects.

### Phase 5: Core Application Components

#### Files 41-50: Core Handlers
- `cyberdelta/core/execution_handler.py`
- `cyberdelta/core/signal_generator.py`
- `cyberdelta/core/portfolio_tracker.py`
- `cyberdelta/core/risk_manager.py`
- etc.

These already accept SymbolService but need to use domain objects in operations.

### Phase 6: Strategy & Risk Components

#### Files 51-60: Strategy Files
- `cyberdelta/strategies/funding_rate_arbitrage.py`
- Strategy base classes
- Signal generation components

### Phase 7: Test Migration (The Big One)

#### Files 61-160+: Test Files (297+ hardcoded strings)

**Approach**: Batch by directory
```python
# Week 3:
tests/unit/apis/hyperliquid/  (20 files)
tests/unit/apis/backpack/     (20 files)
tests/unit/core/              (20 files)

# Week 4:
tests/integration/            (40+ files)
tests/unit/remaining/         (40+ files)
```

**Pattern for each test file**:
```python
# OLD:
test_symbol = "BTC-PERP"

# NEW:
test_symbol = ExchangeSymbolFactory.create_hyperliquid("BTC-PERP")
```

## 🎯 Migration Strategy: File-by-File Clean Breaks

### Core Principles:

1. **Complete One File at a Time**
   - All changes in a file before moving on
   - Test after each file

2. **No Backwards Compatibility**
   - Delete old code
   - Force callers to update
   - Use type checker to find usage

3. **Domain Objects Throughout**
   - Parse strings to domain objects at entry
   - Use domain objects everywhere internally
   - Convert to strings only at API boundaries

4. **Breaking Changes Are Good**
   - They force migration
   - Can't accidentally use old patterns
   - Type checker enforces correctness

## ✅ Success Metrics

### Per-File:
- [ ] No `symbol: str` parameters
- [ ] No string parsing/splitting
- [ ] All tests pass
- [ ] mypy passes

### Overall:
- **0 string operations** except at API boundaries
- **100% domain object usage**
- **All tests use factories**
- **Clean single implementation**

## 📅 Realistic Timeline

### Week 1: Foundation (Files 1-10)
- Day 1: Core models (2 files)
- Day 2-3: Order mappers (4 files)
- Day 4-5: Order services (4 files)

### Week 2: Expand Coverage (Files 11-50)
- Market data mappers and services
- Core application components

### Week 3-4: Test Migration (Files 61-160+)
- Batch by directory
- Use regex/tools to help with replacements
- ~20-40 files per day possible with good tooling

## 🔧 First Step: Start With File #1

### File 1: `cyberdelta/core/models/market/order.py`
```python
# 1. Add import:
from cyberdelta.core.symbols.models import ExchangeSymbol

# 2. Change field:
symbol: ExchangeSymbol = Field(..., description="Exchange-specific symbol")

# 3. Run tests, fix errors
```

This single change will break ~100 places that create Orders. Good! Now you must fix them all to use domain objects.

## 📝 Summary

### The Reality:
- ✅ Great symbol infrastructure built
- ❌ 95%+ of codebase ignores it
- 🚨 Strings everywhere

### The Solution:
- **File-by-file migration**
- **Breaking changes force adoption**
- **No backwards compatibility**
- **Domain objects throughout**

### Why It Will Work:
1. **Can't avoid it** - Changing core models forces everything to adapt
2. **Clear progress** - Each file is done or not done
3. **Type safety** - mypy finds all problems
4. **No dual systems** - Clean single implementation

---

*Remember: "I still want breaking clean break refactor but due to the scope of the problem it has to be broken down into many layers where we refactor break one layer at a time"*

**This is exactly that - one file at a time, each a clean break.**
