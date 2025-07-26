# Week 2: Market & Account Data Implementation Guide
**Phases 7-14 | Duration: 8 days | Focus: Expand domain objects to all data flows**

## 🎯 Week 2 Objectives

**PRIMARY GOAL**: Transform all market data and account data processing from strings to domain objects

**BUILDING ON WEEK 1**: Order flow now uses domain objects - extend this pattern to all data types

**CRITICAL SUCCESS FACTORS**:
- Market data (tickers, order books, trades, funding rates) uses ExchangeSymbol
- Account data (balances, positions, transactions) uses ExchangeSymbol
- Both Hyperliquid and Backpack achieve parity
- All data mappers create domain objects from exchange responses
- All data services operate with domain objects internally

## 📅 Daily Implementation Schedule

### **Day 1: PHASE 7 - Hyperliquid Market Data Mappers**
**Impact**: Market data enters system as domain objects
**Focus**: Establish pattern for all market data types

#### Morning Tasks (3-4 hours)

##### 7.1 Update Price Ticker Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/market_data/hl_price_ticker_mapper.py
```

**Add Domain Imports**:
```python
from cyberdelta.core.symbols.models import ExchangeSymbol, create_exchange_symbol
from cyberdelta.enums.exchange_names import ExchangeName
```

**Update Ticker Transformation**:
```python
def map_raw_ticker(self, raw_ticker) -> Ticker:
    """Map raw Hyperliquid ticker to Ticker with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_ticker.coin,  # e.g., "BTC"
        exchange_id=ExchangeName.HYPERLIQUID,
        asset_index=getattr(raw_ticker, 'asset_index', None)
    )

    return Ticker(
        symbol=exchange_symbol,  # Domain object!
        last_price=self.parse_decimal_safely(raw_ticker.px),
        volume_24h=self.parse_decimal_safely(raw_ticker.volume),
        price_change_24h=self.parse_decimal_safely(raw_ticker.change),
        # ... rest of mapping
    )
```

##### 7.2 Update Order Book Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/market_data/hl_order_book_mapper.py
```

**Update Order Book Transformation**:
```python
def map_raw_orderbook(self, raw_orderbook, symbol: str) -> OrderBook:
    """Map raw order book with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=symbol,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    return OrderBook(
        symbol=exchange_symbol,  # Domain object
        bids=self._map_price_levels(raw_orderbook.get("bids", [])),
        asks=self._map_price_levels(raw_orderbook.get("asks", [])),
        timestamp=datetime.now(UTC),
    )
```

#### Afternoon Tasks (3-4 hours)

##### 7.3 Update Trade Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/market_data/hl_historical_data_mapper.py
```

**Update Trade Transformation**:
```python
def map_raw_trade(self, raw_trade) -> Trade:
    """Map raw trade data with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_trade.coin,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    return Trade(
        symbol=exchange_symbol,  # Domain object
        side=self._map_side(raw_trade.side),
        quantity=self.parse_decimal_safely(raw_trade.sz),
        price=self.parse_decimal_safely(raw_trade.px),
        timestamp=parse_datetime_utc(raw_trade.time),
        # ... rest
    )
```

##### 7.4 Update Funding Rate Mapper
**Add funding rate mapping if not exists**:
```python
def map_raw_funding_rate(self, raw_funding) -> FundingRate:
    """Map raw funding rate with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_funding.coin,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    return FundingRate(
        symbol=exchange_symbol,  # Domain object
        rate=self.parse_decimal_safely(raw_funding.funding),
        next_funding_time=parse_datetime_utc(raw_funding.next_time),
    )
```

#### End of Day 1 Deliverable
- [x] All Hyperliquid market data mappers create ExchangeSymbol objects
- [x] Ticker, OrderBook, Trade, FundingRate use domain objects
- [x] Consistent pattern established

---

### **Day 2: PHASE 8 - Backpack Market Data Mappers**
**Impact**: Both exchanges create consistent domain objects for market data
**Focus**: Achieve parity between exchanges

#### Morning Tasks (3-4 hours)

##### 8.1 Update Backpack Ticker Mapper
```bash
# File: cyberdelta/apis/backpack/mappers/market_data/bp_ticker_mapper.py
```

**Update Ticker Transformation**:
```python
def map_raw_ticker(self, raw_ticker) -> Ticker:
    """Map raw Backpack ticker to Ticker with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_ticker.symbol,  # e.g., "BTC_USD_PERP"
        exchange_id=ExchangeName.BACKPACK,
        symbol_id=getattr(raw_ticker, 'symbol_id', None)
    )

    return Ticker(
        symbol=exchange_symbol,  # Domain object
        last_price=self.parse_decimal_safely(raw_ticker.lastPrice),
        volume_24h=self.parse_decimal_safely(raw_ticker.volume),
        # ... rest of Backpack-specific mapping
    )
```

##### 8.2 Update Backpack Order Book Mapper
```bash
# File: cyberdelta/apis/backpack/mappers/market_data/bp_order_book_mapper.py
```

**Same pattern as Hyperliquid but with Backpack specifics**

#### Afternoon Tasks (3-4 hours)

##### 8.3 Update Backpack Trade and Funding Rate Mappers
- `bp_trade_mapper.py`
- `bp_funding_rate_mapper.py`

##### 8.4 Test Both Exchange Market Data Mappers
```bash
pytest tests/unit/apis/hyperliquid/mappers/test_hl_market_data_mapper_core.py -v
pytest tests/unit/apis/backpack/mappers/test_bp_market_data_mapper_core.py -v
```

#### End of Day 2 Deliverable
- [x] All Backpack market data mappers use ExchangeSymbol
- [x] Consistent pattern across both exchanges
- [x] Market data mapper tests pass

---

### **Day 3: PHASE 9 - Hyperliquid Market Data Services**
**Impact**: Market data services operate with domain objects
**Focus**: Business logic uses domain objects, strings only at boundaries

#### Morning Tasks (3-4 hours)

##### 9.1 Update Price Ticker Service
```bash
# File: cyberdelta/apis/hyperliquid/services/market_data/hl_price_ticker_service.py
```

**Update Service Methods**:
```python
async def get_ticker(self, args: GetTickerArgs) -> Ticker:
    """Get ticker using domain objects."""

    # args.symbol is now ExchangeSymbol
    logger.info("Fetching ticker", symbol=args.symbol.value, exchange=args.symbol.exchange_id)

    # Convert to string ONLY at HTTP boundary
    payload = self.request_builder.build_ticker_request(
        symbol=str(args.symbol)  # String conversion ONLY here
    )

    response = await self.http_client.get("/info", params=payload)

    # Mapper creates Ticker with ExchangeSymbol
    return self.ticker_mapper.map_raw_ticker(response.json())

async def get_all_tickers(self) -> list[Ticker]:
    """Get all tickers - each contains ExchangeSymbol."""
    response = await self.http_client.get("/info", params={"type": "allMids"})

    tickers = []
    for coin, data in response.json().items():
        # Each ticker contains domain object
        ticker = self.ticker_mapper.map_raw_ticker_data(coin, data)
        tickers.append(ticker)

    return tickers
```

##### 9.2 Update Order Book Service
```bash
# File: cyberdelta/apis/hyperliquid/services/market_data/hl_order_book_service.py
```

**Update Order Book Methods**:
```python
async def get_order_book(self, args: GetOrderBookArgs) -> OrderBook:
    """Get order book using domain objects."""

    # Convert ExchangeSymbol to string at boundary
    payload = self.request_builder.build_orderbook_request(
        symbol=str(args.symbol),  # String only at boundary
        depth=args.depth
    )

    response = await self.http_client.get("/info", params=payload)

    # Mapper creates OrderBook with ExchangeSymbol
    return self.orderbook_mapper.map_raw_orderbook(
        response.json(),
        symbol=args.symbol.value  # Pass string for internal mapping
    )
```

#### Afternoon Tasks (3-4 hours)

##### 9.3 Update Historical Data Service
```bash
# File: cyberdelta/apis/hyperliquid/services/market_data/hl_historical_data_service.py
```

##### 9.4 Update Market Metadata Service
```bash
# File: cyberdelta/apis/hyperliquid/services/market_data/hl_market_metadata_service.py
```

#### End of Day 3 Deliverable
- [x] All Hyperliquid market data services use ExchangeSymbol
- [x] Domain objects flow through service operations
- [x] String conversion only at HTTP boundaries

---

### **Day 4: PHASE 10 - Backpack Market Data Services**
**Impact**: Complete market data flow uses domain objects
**Focus**: Achieve service-level parity between exchanges

#### All Day Tasks (6-8 hours)

##### 10.1 Update All Backpack Market Data Services
Apply same pattern to:
- `bp_price_ticker_service.py`
- `bp_order_book_service.py`
- `bp_historical_data_service.py`
- `bp_market_metadata_service.py`

**Consistent Pattern**:
```python
async def service_method(self, args: DomainArgs) -> DomainResult:
    """Service method using domain objects."""

    # Business logic with domain objects
    logger.info("Operation", symbol=args.symbol.value)

    # String conversion ONLY at HTTP boundary
    payload = self.request_builder.build_request(
        symbol=str(args.symbol)  # Boundary conversion
    )

    response = await self.http_client.request(payload)

    # Mapper returns object with domain symbols
    return self.mapper.map_response(response.json())
```

#### End of Day 4 Deliverable
- [x] All market data services use ExchangeSymbol across both exchanges
- [x] Complete market data flow uses domain objects
- [x] Integration tests pass for market data

**MILESTONE**: Market data pipeline fully transformed (Orders + Market Data = 100% domain objects)

---

### **Day 5: PHASE 11 - Hyperliquid Account Mappers**
**Impact**: Account data enters system as domain objects
**Focus**: Positions, balances, transactions use domain objects

#### Morning Tasks (3-4 hours)

##### 11.1 Update Position Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/account/hl_position_mapper.py
```

**Update Position Transformation**:
```python
def map_raw_position(self, raw_position) -> Position:
    """Map raw position data with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_position.coin,
        exchange_id=ExchangeName.HYPERLIQUID,
        asset_index=getattr(raw_position, 'asset_index', None)
    )

    return Position(
        symbol=exchange_symbol,  # Domain object
        size=self.parse_decimal_safely(raw_position.szi),
        average_price=self.parse_decimal_safely(raw_position.entryPx),
        unrealized_pnl=self.parse_decimal_safely(raw_position.unrealizedPnl),
        # ... rest of mapping
    )
```

##### 11.2 Update Balance Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/account/hl_balance_mapper.py
```

**Update Balance Transformation**:
```python
def map_raw_balance(self, raw_balance) -> Balance:
    """Map raw balance with domain object."""

    # For balances, create internal symbol (asset-based)
    internal_symbol = create_internal_symbol(
        value=raw_balance.coin,  # e.g., "USDC"
        market_type=MarketType.SPOT
    )

    # Convert to exchange symbol for consistency
    exchange_symbol = create_exchange_symbol(
        value=raw_balance.coin,
        exchange_id=ExchangeName.HYPERLIQUID
    )

    return Balance(
        symbol=exchange_symbol,  # Domain object
        total=self.parse_decimal_safely(raw_balance.total),
        available=self.parse_decimal_safely(raw_balance.hold),
        # ... rest
    )
```

#### Afternoon Tasks (3-4 hours)

##### 11.3 Update Transaction Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/account/hl_transaction_mapper.py
```

##### 11.4 Update Account Summary Mapper
```bash
# File: cyberdelta/apis/hyperliquid/mappers/account/hl_account_summary_mapper.py
```

#### End of Day 5 Deliverable
- [x] All Hyperliquid account mappers use ExchangeSymbol
- [x] Positions, balances, transactions use domain objects
- [x] Account mapping tests pass

---

### **Day 6: PHASE 12 - Backpack Account Mappers**
**Impact**: Both exchanges have consistent account data with domain objects
**Focus**: Account data parity between exchanges

#### All Day Tasks (6-8 hours)

##### 12.1 Update All Backpack Account Mappers
Apply same pattern to:
- `bp_position_mapper.py`
- `bp_balance_mapper.py`
- `bp_transaction_mapper.py`
- `bp_account_summary_mapper.py`

**Consistent Pattern for Positions**:
```python
def map_raw_position(self, raw_position) -> Position:
    """Map Backpack position with domain object."""

    exchange_symbol = create_exchange_symbol(
        value=raw_position.symbol,  # Backpack format
        exchange_id=ExchangeName.BACKPACK,
        symbol_id=getattr(raw_position, 'symbol_id', None)
    )

    return Position(
        symbol=exchange_symbol,  # Domain object
        # ... Backpack-specific field mapping
    )
```

#### End of Day 6 Deliverable
- [x] All Backpack account mappers use ExchangeSymbol
- [x] Consistent account data pattern across exchanges
- [x] Account mapper tests pass for both exchanges

---

### **Day 7: PHASE 13 - Hyperliquid Account Services**
**Impact**: Account operations use domain objects throughout
**Focus**: Business logic for account management with domain objects

#### Morning Tasks (3-4 hours)

##### 13.1 Update Position Service
```bash
# File: cyberdelta/apis/hyperliquid/services/account/hl_position_service.py
```

**Update Position Methods**:
```python
async def get_positions(self, wallet_address: str) -> list[Position]:
    """Get positions with domain objects."""

    payload = self.request_builder.build_positions_request(
        user=wallet_address
    )

    response = await self.http_client.post("/info", json=payload)

    positions = []
    for raw_position in response.json():
        # Each position contains ExchangeSymbol
        position = self.position_mapper.map_raw_position(raw_position)
        positions.append(position)

    return positions

async def get_position_for_symbol(self, wallet_address: str, symbol: ExchangeSymbol) -> Position | None:
    """Get specific position using domain object."""

    positions = await self.get_positions(wallet_address)

    # Filter by domain object comparison
    for position in positions:
        if position.symbol == symbol:  # Domain object equality
            return position

    return None
```

##### 13.2 Update Balance Service
```bash
# File: cyberdelta/apis/hyperliquid/services/account/hl_balance_service.py
```

#### Afternoon Tasks (3-4 hours)

##### 13.3 Update Other Account Services
- Trade history service
- Order history service
- Account summary service

#### End of Day 7 Deliverable
- [x] All Hyperliquid account services use ExchangeSymbol
- [x] Account operations work with domain objects
- [x] String conversion only at HTTP boundaries

---

### **Day 8: PHASE 14 - Backpack Account Services**
**Impact**: Complete account data flow uses domain objects
**Focus**: Achieve full parity between exchanges for all data types

#### All Day Tasks (6-8 hours)

##### 14.1 Update All Backpack Account Services
Apply same pattern to:
- `bp_position_service.py`
- `bp_balance_service.py`
- `bp_transaction_history_service.py`
- `bp_account_summary_service.py`

##### 14.2 Integration Testing
```bash
# Test complete data flow for both exchanges
pytest tests/integration/apis/test_symbol_api_integration.py -v

# Test account operations
pytest tests/integration/apis/hyperliquid/account/ -v
pytest tests/integration/apis/backpack/account/ -v
```

#### End of Day 8 Deliverable
- [x] All account services use ExchangeSymbol across both exchanges
- [x] Complete data pipeline uses domain objects (Orders + Market + Account)
- [x] Integration tests pass for all data types

**🎯 WEEK 2 MILESTONE ACHIEVED**:
- All data flows (Orders, Market Data, Account Data) use domain objects
- Both exchanges achieve full parity
- String conversion only at HTTP API boundaries

---

## 🔍 Week 2 Success Criteria

### Must Pass Before Week 3
- [ ] **All market data types use ExchangeSymbol** (Ticker, OrderBook, Trade, FundingRate)
- [ ] **All account data types use ExchangeSymbol** (Position, Balance, Transaction)
- [ ] **Both exchanges have identical domain object patterns**
- [ ] **All data mappers create domain objects from exchange responses**
- [ ] **All data services operate with domain objects internally**
- [ ] **String conversion ONLY at HTTP request boundaries**
- [ ] **mypy passes for all updated files**
- [ ] **Integration tests pass for all data types**

### Validation Commands
```bash
# Type checking for market data
mypy cyberdelta/apis/*/mappers/market_data/
mypy cyberdelta/apis/*/services/market_data/

# Type checking for account data
mypy cyberdelta/apis/*/mappers/account/
mypy cyberdelta/apis/*/services/account/

# Test execution
pytest tests/unit/apis/hyperliquid/mappers/ -v
pytest tests/unit/apis/backpack/mappers/ -v
pytest tests/unit/apis/hyperliquid/services/ -v
pytest tests/unit/apis/backpack/services/ -v
pytest tests/integration/apis/ -v

# String usage audit (should return 0 results in scope)
grep -r "symbol.*str" cyberdelta/apis/*/mappers/
grep -r "symbol.*str" cyberdelta/apis/*/services/
```

### Expected Metrics After Week 2
- **Order Pipeline**: 100% domain objects ✅ (from Week 1)
- **Market Data Pipeline**: 100% domain objects ✅ (new)
- **Account Data Pipeline**: 100% domain objects ✅ (new)
- **Exchange Parity**: 100% consistent patterns ✅ (new)
- **Test Coverage**: All data-related tests passing
- **API Boundaries**: String conversion only at HTTP requests

---

## 🔄 Week 2 Pattern Establishment

### Mapper Pattern (Applied 16+ times)
```python
def map_raw_data(self, raw_data) -> DomainModel:
    """Map raw exchange data to domain model."""

    # Create domain symbol at entry point
    exchange_symbol = create_exchange_symbol(
        value=raw_data.symbol_field,
        exchange_id=ExchangeName.EXCHANGE_NAME,
        # Exchange-specific metadata
    )

    return DomainModel(
        symbol=exchange_symbol,  # Always domain object
        # ... other fields
    )
```

### Service Pattern (Applied 16+ times)
```python
async def service_method(self, args: DomainArgs) -> DomainResult:
    """Service operation with domain objects."""

    # Log with domain object
    logger.info("Operation", symbol=args.symbol.value, exchange=args.symbol.exchange_id)

    # Convert to string ONLY at HTTP boundary
    payload = self.request_builder.build_request(
        symbol=str(args.symbol)  # Single conversion point
    )

    response = await self.http_client.request(payload)

    # Mapper returns domain objects
    return self.mapper.map_response(response.json())
```

### Testing Pattern (Applied 32+ times)
```python
def test_mapper_creates_domain_objects():
    """Test that mapper creates ExchangeSymbol objects."""

    # Use factories for consistent domain object creation
    raw_data = create_raw_test_data()

    result = mapper.map_raw_data(raw_data)

    # Verify domain object creation
    assert isinstance(result.symbol, ExchangeSymbol)
    assert result.symbol.exchange_id == ExchangeName.EXPECTED
    assert result.symbol.value == "EXPECTED_VALUE"
```

---

## 🚨 Week 2 Critical Dependencies

### Day 1-2: **MAPPERS FOUNDATION**
- Market data mappers create domain objects
- **EXPECT**: Data entry points work with domain objects
- **GOAL**: Establish pattern for all subsequent data types

### Day 3-4: **SERVICES TRANSFORMATION**
- Market data services use domain objects
- **EXPECT**: Complete market data pipeline working
- **GOAL**: Business logic operations with domain objects

### Day 5-6: **ACCOUNT DATA EXPANSION**
- Account mappers and services follow same pattern
- **EXPECT**: All data types use consistent approach
- **GOAL**: Full data coverage with domain objects

### Day 7-8: **PARITY AND INTEGRATION**
- Both exchanges achieve identical patterns
- **EXPECT**: All integration tests passing
- **GOAL**: Ready for business logic layer (Week 3)

---

## 🛠️ Week 2 Specialized Tools

### Domain Object Creation Helper
```bash
# Quick domain object creation for testing
function create_test_symbol() {
    local value=$1
    local exchange=$2
    echo "create_exchange_symbol(value='$value', exchange_id=ExchangeName.$exchange)"
}

# Usage: create_test_symbol "BTC-PERP" "HYPERLIQUID"
```

### Pattern Validation Script
```python
# validate_domain_usage.py
def check_mapper_patterns(file_path):
    """Verify mapper follows domain object pattern."""
    with open(file_path) as f:
        content = f.read()

    # Check for domain imports
    assert "from cyberdelta.core.symbols.models import" in content
    assert "create_exchange_symbol" in content

    # Check for domain object creation
    assert "exchange_symbol = create_exchange_symbol" in content
    assert "symbol=exchange_symbol" in content
```

### Progress Tracking
```bash
# Count domain object usage vs string usage
grep -r "ExchangeSymbol" cyberdelta/apis/ | wc -l  # Should increase daily
grep -r "symbol.*str" cyberdelta/apis/ | wc -l     # Should decrease daily
```

---

## 📋 Week 2 Deliverables

### Code Changes (32+ files)
#### Hyperliquid Market Data
- [ ] `hl_price_ticker_mapper.py` + `hl_price_ticker_service.py`
- [ ] `hl_order_book_mapper.py` + `hl_order_book_service.py`
- [ ] `hl_historical_data_mapper.py` + `hl_historical_data_service.py`
- [ ] `hl_market_metadata_mapper.py` + `hl_market_metadata_service.py`

#### Backpack Market Data
- [ ] `bp_ticker_mapper.py` + `bp_price_ticker_service.py`
- [ ] `bp_order_book_mapper.py` + `bp_order_book_service.py`
- [ ] `bp_trade_mapper.py` + `bp_historical_data_service.py`
- [ ] `bp_funding_rate_mapper.py` + `bp_market_metadata_service.py`

#### Hyperliquid Account Data
- [ ] `hl_position_mapper.py` + `hl_position_service.py`
- [ ] `hl_balance_mapper.py` + `hl_balance_service.py`
- [ ] `hl_transaction_mapper.py` + `hl_transaction_history_service.py`
- [ ] `hl_account_summary_mapper.py` + `hl_account_summary_service.py`

#### Backpack Account Data
- [ ] `bp_position_mapper.py` + `bp_position_service.py`
- [ ] `bp_balance_mapper.py` + `bp_balance_service.py`
- [ ] `bp_transaction_mapper.py` + `bp_transaction_history_service.py`
- [ ] `bp_account_summary_mapper.py` + `bp_account_summary_service.py`

### Documentation
- [ ] Pattern documentation for mappers and services
- [ ] Exchange-specific implementation notes
- [ ] Domain object creation guidelines
- [ ] Testing pattern documentation

### Validation
- [ ] All data mapper tests passing
- [ ] All data service tests passing
- [ ] Integration tests for complete data flows
- [ ] mypy validation clean for all updated files

**Week 2 expands domain objects to all data types - foundation for business logic transformation in Week 3.** 🚀
