# Week 1: Foundation & Order Flow Implementation Guide
**Phases 1-6 | Duration: 6 days | Focus: Breaking changes to establish domain object foundation**

## 🎯 Week 1 Objectives

**PRIMARY GOAL**: Transform the core order processing pipeline from strings to domain objects

**CRITICAL SUCCESS FACTORS**:
- `Order.symbol` becomes `ExchangeSymbol` (breaks ~100 usage points)
- All service arguments use domain objects
- Order mappers create domain objects from exchange responses
- Order services operate with domain objects internally
- String conversion ONLY at HTTP request boundaries

## 📅 Daily Implementation Schedule

### **Day 1: PHASE 1 - Core Domain Models (FOUNDATION)**
**Impact**: Forces entire system to adapt to domain objects
**Expected Breakage**: ~100 Order creation sites will fail compilation

#### Morning Tasks (2-3 hours)

##### 1.1 Update Order Model
```bash
# File: cyberdelta/core/models/market/order.py
```

**CRITICAL CHANGE - Line 72**:
```python
# BEFORE:
symbol: str = Field(..., description="Trading symbol.")

# AFTER:
symbol: ExchangeSymbol = Field(..., description="Exchange-specific trading symbol")

# ADD IMPORT at top:
from cyberdelta.core.symbols.models import ExchangeSymbol
```

**SECONDARY CHANGE - Line 479 (CancelOrderResult)**:
```python
# BEFORE:
symbol: str | None = Field(default=None, description="Symbol of the order(s)...")

# AFTER:
symbol: ExchangeSymbol | None = Field(default=None, description="Symbol of the order(s)...")
```

#### Afternoon Tasks (3-4 hours)

##### 1.2 Run Tests & Identify Breakage
```bash
# Run type checker to find all breakage points
mypy cyberdelta/core/models/market/order.py

# Run tests to see compilation failures
pytest tests/unit/core/models/market/test_order.py -v
```

**Expected Errors**:
- Order factories in tests will fail
- All Order creation in mappers will fail
- Service argument validation will fail

##### 1.3 Fix Test Factories First
```python
# File: tests/factories/symbol_factories.py
# Update OrderFactory to use ExchangeSymbol

# BEFORE:
symbol=fake.random_element(["BTC-PERP", "ETH-PERP"])

# AFTER:
symbol=ExchangeSymbolFactory.create_hyperliquid("BTC-PERP")
```

#### End of Day 1 Deliverable
- [x] Order model uses ExchangeSymbol
- [x] Basic test factories updated
- [x] Compilation errors documented

---

### **Day 2: PHASE 2 - Service Arguments (ENFORCEMENT)**
**Impact**: Forces all service calls to provide domain objects
**Expected Breakage**: All API service calls will fail

#### Morning Tasks (3-4 hours)

##### 2.1 Update Service Arguments Models
```bash
# File: cyberdelta/apis/models/service_args_models.py
```

**Add Domain Import**:
```python
from cyberdelta.core.symbols.models import ExchangeSymbol
from cyberdelta.enums.exchange_names import ExchangeName
```

**Update All Symbol Fields**:
```python
# PlaceOrderArgs (Line 95):
symbol: ExchangeSymbol  # Was: str

# CancelOrderArgs (Line 509):
symbol: ExchangeSymbol | None = Field(default=None)  # Was: str | None

# GetAllOpenOrdersArgs (Line 638):
symbol: ExchangeSymbol | None = Field(default=None)  # Was: str | None

# GetOrderArgs (Line 665):
symbol: ExchangeSymbol | None = Field(default=None)  # Was: str | None

# Continue for ALL Args classes with symbol fields
```

##### 2.2 Add Migration Helper Validators
```python
@field_validator("symbol", mode="before")
@classmethod
def validate_symbol_domain(cls, v: ExchangeSymbol | str, info: ValidationInfo) -> ExchangeSymbol:
    """Accept ExchangeSymbol or parse string during migration.

    MIGRATION HELPER: Will be removed in Phase 18.
    """
    if isinstance(v, ExchangeSymbol):
        return v
    if isinstance(v, str):
        # Temporary bridge during migration
        return ExchangeSymbol(value=v, exchange_id=ExchangeName.UNKNOWN)
    raise ValueError(f"Invalid symbol type: {type(v)}")
```

#### Afternoon Tasks (3-4 hours)

##### 2.3 Test Service Argument Validation
```bash
# Test that new validation works
pytest tests/unit/apis/models/ -v -k "test_service_args"
```

##### 2.4 Update Remaining Args Models
Continue pattern for all remaining service argument models:
- `GetMarketDataArgs`
- `GetTickerArgs`
- `GetOrderBookArgs`
- `GetHistoricalFundingRatesArgs`
- `GetMarketArgs`
- All other Args classes with symbol fields

#### End of Day 2 Deliverable
- [x] All service argument models use ExchangeSymbol
- [x] Migration helper validators in place
- [x] Service args tests pass

---

### **Day 3: PHASE 3 - Hyperliquid Order Mappers (ENTRY POINT)**
**Impact**: Orders enter system as domain objects
**Focus**: Fix how raw exchange data becomes domain objects

#### Morning Tasks (3-4 hours)

##### 3.1 Update Hyperliquid Order Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/trading/hl_order_mapper.py
```

**Add Imports**:
```python
from cyberdelta.core.symbols.models import ExchangeSymbol, create_exchange_symbol
from cyberdelta.enums.exchange_names import ExchangeName
```

**Update `_create_order_from_components` method**:
```python
def _create_order_from_components(self, raw_order, components) -> Order:
    """Create Order with ExchangeSymbol domain object."""

    # Parse symbol to domain object at entry point
    exchange_symbol = create_exchange_symbol(
        value=raw_order.asset,  # e.g., "BTC"
        exchange_id=ExchangeName.HYPERLIQUID,
        asset_index=getattr(raw_order, 'asset_index', None)
    )

    order_data = {
        "symbol": exchange_symbol,  # Domain object!
        "exchange": "hyperliquid",
        "side": components["side"],
        "order_type": components["order_type"],
        # ... rest of fields
    }

    return Order(**order_data)
```

##### 3.2 Update All Order Creation Methods
Update these methods in same file:
- `transform_simple_open_order`
- `transform_historical_order`
- `transform_websocket_order_update`

#### Afternoon Tasks (3-4 hours)

##### 3.3 Update Order Response Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/trading/hl_order_response_mapper.py
```

**Update Response Mapping**:
```python
def map_raw_status_to_order(self, raw_status) -> Order:
    """Map raw order status response to Order with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_status.coin,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    return Order(
        symbol=exchange_symbol,  # Domain object
        # ... rest of mapping
    )
```

##### 3.4 Test Hyperliquid Order Mapping
```bash
pytest tests/unit/apis/hyperliquid/mappers/test_hl_trading_data_mapper_core.py -v
```

#### End of Day 3 Deliverable
- [x] Hyperliquid order mappers create ExchangeSymbol objects
- [x] All order entry points use domain objects
- [x] Hyperliquid mapping tests pass

---

### **Day 4: PHASE 4 - Backpack Order Mappers (PARITY)**
**Impact**: Both exchanges create domain objects consistently
**Focus**: Establish pattern across all exchanges

#### Morning Tasks (3-4 hours)

##### 4.1 Update Backpack Order Mapper
```bash
# File: cyberdelta/apis/backpack/mappers/trading/bp_order_mapper.py
```

**Add Imports**:
```python
from cyberdelta.core.symbols.models import ExchangeSymbol, create_exchange_symbol
from cyberdelta.enums.exchange_names import ExchangeName
```

**Update Order Transformation Methods**:
```python
def transform_order(self, raw_order: BackpackRawOrder) -> Order:
    """Transform raw Backpack order to Order with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_order.symbol,  # e.g., "BTC_USD_PERP"
        exchange_id=ExchangeName.BACKPACK,
        symbol_id=getattr(raw_order, 'symbol_id', None)
    )

    return Order(
        symbol=exchange_symbol,  # Domain object
        exchange="backpack",
        # ... rest of transformation
    )
```

##### 4.2 Update WebSocket Order Updates
```python
def transform_order_update(self, raw_update: BackpackRawOrderUpdate) -> Order:
    """Transform WebSocket order update with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_update.symbol,
        exchange_id=ExchangeName.BACKPACK
    )

    # Update existing order or create new with domain object
    # ...
```

#### Afternoon Tasks (3-4 hours)

##### 4.3 Test Backpack Order Mapping
```bash
pytest tests/unit/apis/backpack/mappers/test_bp_trading_data_mapper_core.py -v
```

##### 4.4 Integration Test Both Exchanges
```bash
# Test that both exchanges work consistently
pytest tests/integration/apis/ -v -k "order_mapper"
```

#### End of Day 4 Deliverable
- [x] Backpack order mappers create ExchangeSymbol objects
- [x] Consistent pattern across both exchanges
- [x] Order mapper tests pass for both exchanges

**MILESTONE**: All Order objects now contain ExchangeSymbol instead of strings

---

### **Day 5: PHASE 5 - Hyperliquid Order Services (FLOW)**
**Impact**: Service operations use domain objects throughout
**Focus**: Domain objects flow through business operations

#### Morning Tasks (3-4 hours)

##### 5.1 Update Order Placement Service
```bash
# File: cyberdelta/apis/hyperliquid/services/trading/hl_order_placement_service.py
```

**Update Place Order Method**:
```python
async def place_order(self, args: PlaceOrderArgs) -> Order:
    """Place order using domain objects throughout."""

    # args.symbol is now ExchangeSymbol
    logger.info("Placing order", symbol=args.symbol.value, exchange=args.symbol.exchange_id)

    # Convert to string ONLY at API boundary
    payload = self.request_builder.build_place_order_payload(
        symbol=str(args.symbol),  # String conversion ONLY here
        side=args.side.value,
        order_type=args.order_type.value,
        quantity=str(args.quantity),
        # ... other args
    )

    response = await self.http_client.post("/exchange", json=payload)

    # Response processing creates Order with ExchangeSymbol (via mapper)
    return self.order_mapper.map_response_to_order(response.json())
```

##### 5.2 Update Order Cancellation Service
```bash
# File: cyberdelta/apis/hyperliquid/services/trading/hl_order_cancellation_service.py
```

```python
async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
    """Cancel order using domain objects."""

    # args.symbol is now ExchangeSymbol | None
    payload = self.request_builder.build_cancel_payload(
        symbol=str(args.symbol) if args.symbol else None,  # String only at boundary
        order_id=args.order_id,
        # ...
    )

    response = await self.http_client.post("/exchange", json=payload)

    # Return result with domain object
    return CancelOrderResult(
        symbol=args.symbol,  # Keep as domain object
        success=response.get("status") == "ok",
        # ...
    )
```

#### Afternoon Tasks (3-4 hours)

##### 5.3 Update Order Query Service
```bash
# File: cyberdelta/apis/hyperliquid/services/trading/hl_order_query_service.py
```

##### 5.4 Update Batch Order Service
```bash
# File: cyberdelta/apis/hyperliquid/services/trading/hl_batch_order_service.py
```

##### 5.5 Test Hyperliquid Services
```bash
pytest tests/unit/apis/hyperliquid/services/test_hl_trading_service_management.py -v
```

#### End of Day 5 Deliverable
- [x] All Hyperliquid order services use ExchangeSymbol
- [x] String conversion only at HTTP boundaries
- [x] Domain objects flow through service operations

---

### **Day 6: PHASE 6 - Backpack Order Services (PARITY)**
**Impact**: Complete order flow uses domain objects across both exchanges
**Focus**: Achieve consistency and establish the pattern

#### Morning Tasks (3-4 hours)

##### 6.1 Update Backpack Order Placement Service
```bash
# File: cyberdelta/apis/backpack/services/trading/bp_order_placement_service.py
```

**Same pattern as Hyperliquid**:
```python
async def place_order(self, args: PlaceOrderArgs) -> Order:
    """Place order with Backpack using domain objects."""

    # Convert ExchangeSymbol to Backpack's expected format at boundary
    payload = self.request_builder.build_place_order_payload(
        symbol=str(args.symbol),  # String conversion at boundary
        # ... rest
    )

    # Domain object flows through response processing
```

##### 6.2 Update Remaining Backpack Services
- Order cancellation service
- Order query service
- Batch order service

#### Afternoon Tasks (3-4 hours)

##### 6.3 Integration Testing
```bash
# Test complete order flow with both exchanges
pytest tests/integration/apis/test_symbol_api_integration.py -v
```

##### 6.4 End-to-End Order Flow Validation
```bash
# Test the complete transformation:
# User Input → Service (domain) → Mapper (domain) → Order (domain) → API (string)

pytest tests/integration/core/test_execution_handler.py -v
```

#### End of Day 6 Deliverable
- [x] All order services use ExchangeSymbol across both exchanges
- [x] Complete order flow works with domain objects
- [x] Integration tests pass

**🎯 WEEK 1 MILESTONE ACHIEVED**:
- Order creation, processing, and management now uses domain objects throughout
- String conversion happens only at HTTP API boundaries
- Foundation established for remaining system transformation

---

## 🔍 Week 1 Success Criteria

### Must Pass Before Week 2
- [ ] **Zero `Order.symbol: str` anywhere in codebase**
- [ ] **All service arguments use `ExchangeSymbol`**
- [ ] **Order mappers create domain objects from exchange responses**
- [ ] **Order services work with domain objects internally**
- [ ] **String conversion ONLY at HTTP request boundaries**
- [ ] **mypy passes for all updated files**
- [ ] **Core order flow integration tests pass**

### Validation Commands
```bash
# Type checking
mypy cyberdelta/core/models/market/order.py
mypy cyberdelta/apis/models/service_args_models.py
mypy cyberdelta/apis/*/mappers/trading/
mypy cyberdelta/apis/*/services/trading/

# Test execution
pytest tests/unit/core/models/market/test_order.py -v
pytest tests/unit/apis/models/ -v
pytest tests/integration/apis/test_symbol_api_integration.py -v
pytest tests/integration/core/test_execution_handler.py -v

# String usage audit
grep -r "symbol.*str" cyberdelta/core/models/market/order.py  # Should return 0 results
grep -r "symbol.*str" cyberdelta/apis/models/service_args_models.py  # Should return 0 results
```

### Expected Metrics After Week 1
- **Order Pipeline**: 100% domain objects (0% strings)
- **Service Arguments**: 100% domain objects
- **Core Models**: 100% domain objects
- **Test Coverage**: All order-related tests passing
- **Breaking Changes**: ~150+ compilation errors resolved

---

## 🚨 Week 1 Critical Path

### Monday (Phase 1): **FOUNDATION BREAK**
- Change `Order.symbol` to `ExchangeSymbol`
- **EXPECT**: ~100 compilation failures
- **GOAL**: Force system-wide adaptation

### Tuesday (Phase 2): **ENFORCEMENT LAYER**
- Change service arguments to use `ExchangeSymbol`
- **EXPECT**: All API calls to fail compilation
- **GOAL**: Force callers to provide domain objects

### Wednesday-Thursday (Phases 3-4): **DATA ENTRY POINTS**
- Fix order mappers to create domain objects
- **EXPECT**: Order creation to work again
- **GOAL**: Domain objects enter system at boundaries

### Friday-Saturday (Phases 5-6): **BUSINESS LOGIC FLOW**
- Fix order services to use domain objects
- **EXPECT**: Complete order pipeline working
- **GOAL**: String conversion only at HTTP boundaries

### Sunday: **VALIDATION & DOCUMENTATION**
- Integration testing
- Document lessons learned
- Prepare for Week 2

---

## 🛠️ Daily Tools & Commands

### Start of Each Day
```bash
# Run baseline tests
pytest tests/unit/core/models/market/test_order.py
pytest tests/unit/apis/models/
```

### During Implementation
```bash
# Type checking after changes
mypy --no-error-summary <file>

# Quick test after changes
pytest <specific-test-file> -v

# Find string usage patterns
grep -r "symbol.*str" cyberdelta/apis/
```

### End of Each Day
```bash
# Final validation
mypy cyberdelta/core/models/market/order.py
pytest tests/integration/core/test_execution_handler.py
```

---

## 🔄 Recovery Procedures

### If Phase Fails
1. **Identify root cause** - compilation error vs logic error
2. **Rollback to last working state** - revert changes to known working state
3. **Smaller incremental changes** - break phase into sub-phases
4. **Update validation approach** - add more migration helpers if needed

### If Integration Tests Fail
1. **Isolate the failure** - which specific test case
2. **Check domain object creation** - are mappers working correctly
3. **Verify string conversion** - only at HTTP boundaries
4. **Test factory updates** - ensure factories create valid domain objects

### If Performance Issues
1. **Profile domain object creation** - vs string operations
2. **Check symbol registry performance** - caching effectiveness
3. **Optimize hot paths** - minimize object creation in loops

---

## 📋 Week 1 Deliverables

### Code Changes
- [ ] `cyberdelta/core/models/market/order.py` - ExchangeSymbol fields
- [ ] `cyberdelta/apis/models/service_args_models.py` - All Args classes updated
- [ ] `cyberdelta/apis/hyperliquid/mappers/trading/` - Domain object creation
- [ ] `cyberdelta/apis/backpack/mappers/trading/` - Domain object creation
- [ ] `cyberdelta/apis/hyperliquid/services/trading/` - Domain object operations
- [ ] `cyberdelta/apis/backpack/services/trading/` - Domain object operations

### Documentation
- [ ] Phase-by-phase implementation notes
- [ ] Breaking changes catalog
- [ ] Performance impact analysis
- [ ] Lessons learned for Week 2

### Validation
- [ ] All target tests passing
- [ ] mypy validation clean
- [ ] Integration tests working
- [ ] No string symbol usage in scope

**Week 1 transforms the foundation - everything else builds on this success.** 🚀
