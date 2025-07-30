# Week 2: Market & Account Data Clean Architecture
**Duration: 4 days | Focus: Ensure all data flows use Symbol domain objects**

## 🎯 Week 2 Objectives

**PRIMARY GOAL**: Verify and optimize market data and account data processing with Symbol objects

**BUILDING ON WEEK 1**: Foundation complete - ensure all data mappers and services follow clean patterns

**NO BACKWARD COMPATIBILITY**: Clean Symbol architecture throughout data processing

## 🚨 API Boundary Rule Reminder
**CRITICAL**: RAW models from exchanges are the ONLY place where symbols are strings. All mappers convert these strings to Symbol objects at the API boundary. Everything else in the system uses Symbol objects.

```python
# RAW models (API boundary) - strings
class RawBackpackTicker:
    symbol: str  # ✅ String at API boundary
    lastPrice: str
    volume: str

# Domain models (business logic) - Symbol objects  
class Ticker(BaseModel):
    symbol: Symbol  # ✅ Symbol object in business logic
    last_price: Decimal
    volume_24h: Decimal
```

## 📊 Current Implementation Status

### Market Data Mappers ✅
All market data mappers convert string symbols from RAW models to Symbol objects:

```python
# Backpack Ticker Mapper
from cyberdelta.core.symbols import exchanges

@staticmethod
def transform_raw_ticker_to_internal(raw_ticker: BackpackRawTickerResponse) -> Ticker:
    """Convert RAW ticker (string symbol) to domain Ticker (Symbol object)."""
    
    # raw_ticker.symbol is a string from the exchange API
    # Convert it to Symbol object at this boundary
    exchange_symbol = exchanges.backpack(
        raw_ticker.symbol,  # String from RAW model (first positional arg is value)
        symbol_id=getattr(raw_ticker, "symbol_id", None)
    )
    
    # Use secure_transform to create domain model with Symbol object
    ticker_data = {
        "symbol": exchange_symbol,  # Symbol object for business logic!
        "exchange": ExchangeName.BACKPACK.value,
        "timestamp": timestamp.isoformat(),
        "price": str(last_price),  # last_price was parsed from raw_ticker.last_price
        "volume": str(volume_24h),  # volume_24h was parsed from raw_ticker.volume
        # ...
    }
    
    return secure_transform(
        data=ticker_data,
        model_class=Ticker,
        context="backpack_ticker_transform",
        source_exchange="backpack",
    )
```

### Account Data Mappers ✅
Position and balance mappers convert string symbols from RAW models to Symbol objects:

```python
# Hyperliquid Position Mapper
@staticmethod
def transform_raw_clearinghouse_state_to_positions(
    raw_clearinghouse_state: HyperliquidRawClearinghouseState
) -> list[Position]:
    """Convert RAW positions (string coin) to domain Position (Symbol object)."""
    
    positions = []
    for asset_position in raw_clearinghouse_state.assetPositions:
        # asset_position.position.coin is a string from the exchange API
        # Convert it to Symbol object at this boundary
        exchange_symbol = exchanges.hyperliquid(
            asset_position.position.coin,  # String from RAW model (first positional arg is value)
            asset_index=getattr(asset_position, "asset_index", None)
        )
        
        # Use secure_transform to create domain model with Symbol object
        position_data = {
            "symbol": exchange_symbol,  # Symbol object for business logic!
            "exchange": ExchangeName.HYPERLIQUID.value,
            "size": str(parsed_size),  # parsed from asset_position.position.szi
            "average_price": str(parsed_entry_px),  # parsed from asset_position.position.entryPx
            # ...
        }
        
        position = secure_transform(
            data=position_data,
            model_class=Position,
            context="hyperliquid_position_transform",
            source_exchange="hyperliquid",
        )
        positions.append(position)
    
    return positions
```

## 📅 Implementation Schedule

### **Day 1: Market Data Verification**
**Focus**: Ensure all market data types use Symbol objects consistently

#### Morning Tasks
**Verify Ticker Data Flow**:
```python
# File: cyberdelta/apis/backpack/services/market_data/bp_price_ticker_service.py
async def get_ticker(self, args: GetTickerArgs) -> Ticker:
    # args.symbol is already Symbol object
    logger.info("Getting ticker", 
                symbol=args.symbol.value,
                exchange=args.symbol.exchange)
    
    # Convert to exchange format only at HTTP boundary
    response = await self._make_request(
        endpoint=f"/ticker/{args.symbol.value}"  # String conversion here
    )
    
    # Mapper returns Ticker with Symbol object
    return self.ticker_mapper.transform_raw_ticker_to_internal(response)
```

**Verify Order Book Data Flow**:
```python
# File: cyberdelta/apis/hyperliquid/mappers/market_data/hl_order_book_mapper.py
def map_raw_orderbook(self, raw_orderbook, symbol_value: str) -> OrderBook:
    # Create Symbol object at entry point
    exchange_symbol = exchanges.hyperliquid(symbol_value)
    
    return OrderBook(
        symbol=exchange_symbol,  # Domain object
        bids=self._map_price_levels(raw_orderbook.get("levels", [])[0]),
        asks=self._map_price_levels(raw_orderbook.get("levels", [])[1]),
        timestamp=datetime.now(UTC),
    )
```

#### Afternoon Tasks
**Verify Trade and Funding Rate Data**:
```python
# Trade data with Symbol
class Trade(BaseModel):
    symbol: Symbol  # Domain object
    side: OrderSide
    quantity: Decimal
    price: Decimal
    timestamp: datetime

# Funding rate with Symbol  
class FundingRate(BaseModel):
    symbol: Symbol  # Domain object
    rate: Decimal
    timestamp: datetime
```

### **Day 2: Account Data Verification**
**Focus**: Ensure account data models use Symbol objects

#### Morning Tasks
**Verify Position Data Flow**:
```python
# Service layer uses Symbol
async def get_position(self, args: GetPositionArgs) -> Position | None:
    # args.symbol is Symbol object
    if args.symbol.exchange != self.exchange:
        raise ValueError(f"Symbol {args.symbol.value} not for {self.exchange}")
    
    # Business logic with Symbol
    return await self._fetch_position_for_symbol(args.symbol)
```

**Verify Balance Data Flow**:
```python
# Balance may reference Symbol for asset-specific balances
class AssetBalance(BaseModel):
    symbol: Symbol | None  # Optional Symbol reference
    asset: str
    available: Decimal
    locked: Decimal
```

#### Afternoon Tasks
**Verify Transaction History**:
```python
class Transaction(BaseModel):
    symbol: Symbol  # Domain object for trade transactions
    transaction_type: TransactionType
    amount: Decimal
    timestamp: datetime
```

### **Day 3: Service Layer Patterns**
**Focus**: Document and verify service patterns

#### HTTP Boundary Pattern
```python
class HyperliquidPriceTickerService:
    async def get_all_tickers(self) -> list[Ticker]:
        # Fetch raw data from HTTP API (strings)
        raw_response = await self.http_client.get("/all_mids")
        # raw_response contains string symbols like {"BTC": {"price": "50000"}, ...}
        
        # Convert strings to Symbol objects at this boundary
        tickers = []
        for symbol_str, price_data in raw_response.items():
            # Convert string to Symbol object
            symbol_obj = exchanges.hyperliquid(symbol_str)  # String -> Symbol
            
            # Create domain model with Symbol object
            ticker = Ticker(
                symbol=symbol_obj,  # Symbol object for business logic
                last_price=Decimal(price_data["price"]),
                # ...
            )
            tickers.append(ticker)
        
        return tickers  # Returns list of domain objects with Symbols
```

#### Caching with Symbol Keys
```python
class MarketDataCache:
    def __init__(self):
        self._ticker_cache: dict[str, Ticker] = {}
    
    def _make_cache_key(self, symbol: Symbol) -> str:
        """Create cache key from Symbol object."""
        return f"{symbol.exchange.value}:{symbol.value}"
    
    def get_ticker(self, symbol: Symbol) -> Ticker | None:
        key = self._make_cache_key(symbol)
        return self._ticker_cache.get(key)
```

### **Day 4: Integration Testing**
**Focus**: Comprehensive testing of data flows

#### Test Symbol Usage in Data Models
```python
def test_market_data_with_symbols():
    # Create test symbols
    btc_hl = exchanges.hyperliquid("BTC")
    btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)
    
    # Test ticker creation
    ticker_hl = Ticker(
        symbol=btc_hl,
        last_price=Decimal("50000"),
        volume_24h=Decimal("1000000")
    )
    
    # Verify Symbol properties
    assert ticker_hl.symbol.value == "BTC"
    assert ticker_hl.symbol.exchange == ExchangeName.HYPERLIQUID
    assert isinstance(ticker_hl.symbol, BaseSymbol)
```

#### Test Service Integration
```python
@pytest.mark.asyncio
async def test_market_data_service_flow():
    # Test args with Symbol
    args = GetTickerArgs(
        symbol=exchanges.hyperliquid("ETH")
    )
    
    # Service processes Symbol object
    service = HyperliquidPriceTickerService()
    ticker = await service.get_ticker(args)
    
    # Verify Symbol maintained throughout
    assert ticker.symbol == args.symbol
    assert isinstance(ticker.symbol, BaseSymbol)
```

## 🎯 Week 2 Success Criteria

### Data Model Validation ✅
- [x] All market data models use Symbol objects
- [x] All account data models use Symbol objects  
- [x] No string symbol fields in data models
- [x] Symbol used consistently in caching

### Service Layer Patterns ✅
- [x] Services accept Symbol in args
- [x] String conversion only at HTTP boundaries
- [x] Symbol objects flow through business logic
- [x] Proper error handling for Symbol validation

### Type Safety ✅
- [x] `mypy cyberdelta/apis/*/services/market_data/` - 0 errors
- [x] `mypy cyberdelta/apis/*/services/account/` - 0 errors
- [x] `mypy cyberdelta/apis/*/mappers/` - 0 errors
- [x] All Symbol usage type-safe

## 🚨 Week 2 Key Patterns

### API Boundary Rule
```python
# RAW models have strings, mappers convert to Symbols
raw_ticker.symbol  # String from exchange API
ticker.symbol      # Symbol object in business logic
```

### Entry Point Creation
```python
# Mappers create Symbol at API boundaries from RAW strings
symbol = exchanges.hyperliquid(raw_data.coin)  # String -> Symbol
```

### HTTP Boundary Conversion
```python
# Convert Symbol to string only for HTTP calls
endpoint = f"/ticker/{args.symbol.value}"  # Symbol -> String for API
```

### Business Logic with Symbols
```python
# Use Symbol properties in logic (never strings)
if ticker.symbol.exchange == ExchangeName.HYPERLIQUID:
    # Exchange-specific processing
```

### Caching Patterns
```python
# Symbol-based cache keys (convert Symbol to string key)
cache_key = f"{symbol.exchange}:{symbol.value}"  # Symbol -> String for cache key
```

## 📊 Week 2 Metrics

- **Data Models with Symbol**: 100% ✅
- **Service Methods Updated**: 100% ✅
- **String Conversions**: Only at HTTP boundaries ✅
- **Type Safety**: Full coverage ✅

**Week 2 ensures all data flows use clean Symbol architecture - ready for business logic integration!** 🚀