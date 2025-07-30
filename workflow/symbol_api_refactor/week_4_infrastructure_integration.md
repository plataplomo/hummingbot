# Week 4: Infrastructure Integration Clean Architecture
**Duration: 4 days | Focus: Infrastructure components with Symbol architecture**

## 🎯 Week 4 Objectives

**PRIMARY GOAL**: Complete infrastructure integration with Symbol domain objects

**BUILDING ON WEEKS 1-3**: Core architecture complete - integrate supporting infrastructure

**NO BACKWARD COMPATIBILITY**: Clean Symbol usage in all infrastructure

## 📊 Infrastructure Components

### Key Infrastructure Areas
1. **WebSocket Integration** - Real-time data with Symbols
2. **Caching Layer** - Symbol-based cache keys
3. **Logging & Monitoring** - Structured logs with Symbols
4. **Configuration** - Symbol configuration loading
5. **Entry Points** - CLI/API with Symbol handling

## 📅 Implementation Schedule

### **Day 1: WebSocket Integration**
**Focus**: Real-time data streams with Symbol objects

#### WebSocket Message Handlers
```python
# File: cyberdelta/apis/websocket/ws_typed_processor.py
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol, BaseSymbol

class WebSocketMessageProcessor:
    def __init__(self, exchange: ExchangeName):
        self.exchange = exchange
        
    async def process_ticker_update(self, raw_message: dict) -> Ticker:
        """Process ticker update with Symbol object."""
        
        # Create Symbol at entry point
        symbol_str = raw_message.get("symbol") or raw_message.get("coin")
        
        if self.exchange == ExchangeName.HYPERLIQUID:
            symbol_obj = exchanges.hyperliquid(
                symbol_str,
                asset_index=raw_message.get("asset_index")
            )
        elif self.exchange == ExchangeName.BACKPACK:
            symbol_obj = exchanges.backpack(
                symbol_str,
                symbol_id=raw_message.get("symbol_id")
            )
        else:
            raise ValueError(f"Unsupported exchange: {self.exchange}")
        
        # Create ticker with Symbol
        return Ticker(
            symbol=symbol_obj,  # Domain object
            last_price=Decimal(raw_message["price"]),
            volume_24h=Decimal(raw_message.get("volume", "0")),
            timestamp=parse_datetime_utc(raw_message["timestamp"])
        )
    
    async def process_order_update(self, raw_message: dict) -> Order:
        """Process order update with Symbol object."""
        
        # Create Symbol for order
        symbol_str = raw_message.get("symbol")
        
        if self.exchange == ExchangeName.HYPERLIQUID:
            symbol_obj = exchanges.hyperliquid(symbol_str)
        elif self.exchange == ExchangeName.BACKPACK:
            symbol_obj = exchanges.backpack(
                symbol_str,
                symbol_id=raw_message.get("symbol_id")
            )
        
        return Order(
            symbol=symbol_obj,  # Domain object
            exchange_order_id=raw_message["order_id"],
            side=OrderSide(raw_message["side"]),
            status=OrderStatus(raw_message["status"]),
            # ... other fields
        )
```

#### WebSocket Subscription Manager
```python
# File: cyberdelta/apis/websocket/ws_subscription_manager.py
class SubscriptionManager:
    def __init__(self, exchange: ExchangeName):
        self.exchange = exchange
        self.subscriptions: dict[str, set[Symbol]] = defaultdict(set)
    
    async def subscribe_ticker(self, symbol: Symbol) -> None:
        """Subscribe to ticker updates for Symbol."""
        
        # Validate symbol for exchange
        if symbol.exchange != self.exchange:
            raise ValueError(f"Symbol {symbol.value} not for {self.exchange}")
        
        # Add to subscriptions
        self.subscriptions["ticker"].add(symbol)
        
        # Send subscription message
        sub_message = {
            "type": "subscribe",
            "channel": "ticker",
            "symbol": symbol.value  # String for wire protocol
        }
        
        await self.ws_connection.send_json(sub_message)
        
        logger.info("Subscribed to ticker",
                   symbol=symbol.value,
                   exchange=symbol.exchange)
```

### **Day 2: Caching Layer**
**Focus**: Symbol-aware caching infrastructure

#### Symbol Cache Implementation
```python
# File: cyberdelta/core/infrastructure/symbol_cache.py
from cyberdelta.core.symbols.models import Symbol, BaseSymbol
import redis
import pickle

class SymbolCache:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.ttl = 300  # 5 minutes
        
    def _make_key(self, prefix: str, symbol: Symbol) -> str:
        """Create cache key from Symbol."""
        return f"{prefix}:{symbol.exchange.value}:{symbol.value}"
    
    async def get_ticker(self, symbol: Symbol) -> Ticker | None:
        """Get cached ticker for Symbol."""
        key = self._make_key("ticker", symbol)
        
        data = await self.redis.get(key)
        if data:
            ticker = pickle.loads(data)
            # Ensure Symbol object maintained
            assert isinstance(ticker.symbol, BaseSymbol)
            return ticker
        return None
    
    async def set_ticker(self, ticker: Ticker) -> None:
        """Cache ticker with Symbol-based key."""
        key = self._make_key("ticker", ticker.symbol)
        
        data = pickle.dumps(ticker)
        await self.redis.setex(key, self.ttl, data)
        
        logger.debug("Cached ticker",
                    symbol=ticker.symbol.value,
                    exchange=ticker.symbol.exchange)
    
    async def get_order_book(self, symbol: Symbol, depth: int = 20) -> OrderBook | None:
        """Get cached order book for Symbol."""
        key = self._make_key(f"orderbook:{depth}", symbol)
        
        data = await self.redis.get(key)
        if data:
            return pickle.loads(data)
        return None
    
    async def invalidate_symbol_data(self, symbol: Symbol) -> None:
        """Invalidate all cached data for a Symbol."""
        pattern = f"*:{symbol.exchange.value}:{symbol.value}"
        
        keys = await self.redis.keys(pattern)
        if keys:
            await self.redis.delete(*keys)
            
        logger.info("Invalidated cache for symbol",
                   symbol=symbol.value,
                   exchange=symbol.exchange,
                   keys_deleted=len(keys))
```

### **Day 3: Logging & Monitoring**
**Focus**: Structured logging with Symbol context

#### Structured Logging with Symbols
```python
# File: cyberdelta/core/infrastructure/symbol_logging.py
import structlog
from cyberdelta.core.symbols.models import Symbol

def add_symbol_context(logger, method_name: str, event_dict: dict) -> dict:
    """Add Symbol context to log entries."""
    
    # Extract Symbol from event dict
    symbol = event_dict.get("symbol")
    
    if isinstance(symbol, Symbol):
        # Add structured Symbol fields
        event_dict["symbol_value"] = symbol.value
        event_dict["symbol_exchange"] = symbol.exchange.value
        
        # Add metadata if available
        if hasattr(symbol, "metadata"):
            if symbol.exchange == ExchangeName.HYPERLIQUID:
                event_dict["asset_index"] = getattr(symbol.metadata, "asset_index", None)
            elif symbol.exchange == ExchangeName.BACKPACK:
                event_dict["symbol_id"] = getattr(symbol.metadata, "symbol_id", None)
    
    return event_dict

# Configure structlog with Symbol processor
structlog.configure(
    processors=[
        add_symbol_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

# Usage in code
logger = structlog.get_logger()

def process_order(order: Order):
    logger.info("Processing order",
               symbol=order.symbol,  # Symbol object
               order_id=order.exchange_order_id,
               side=order.side)
    # Logs: {"symbol_value": "BTC-PERP", "symbol_exchange": "hyperliquid", ...}
```

#### Metrics Collection
```python
# File: cyberdelta/core/infrastructure/symbol_metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Define metrics with symbol labels
order_counter = Counter(
    'cyberdelta_orders_total',
    'Total orders processed',
    ['exchange', 'symbol', 'side', 'status']
)

ticker_latency = Histogram(
    'cyberdelta_ticker_latency_seconds',
    'Ticker update latency',
    ['exchange', 'symbol']
)

position_gauge = Gauge(
    'cyberdelta_position_size',
    'Current position size',
    ['exchange', 'symbol']
)

def record_order_metric(order: Order) -> None:
    """Record order metrics with Symbol labels."""
    order_counter.labels(
        exchange=order.symbol.exchange.value,
        symbol=order.symbol.value,
        side=order.side.value,
        status=order.status.value
    ).inc()

def record_ticker_latency(ticker: Ticker, latency: float) -> None:
    """Record ticker latency with Symbol labels."""
    ticker_latency.labels(
        exchange=ticker.symbol.exchange.value,
        symbol=ticker.symbol.value
    ).observe(latency)

def update_position_metric(position: Position) -> None:
    """Update position gauge with Symbol labels."""
    position_gauge.labels(
        exchange=position.symbol.exchange.value,
        symbol=position.symbol.value
    ).set(float(position.size))
```

### **Day 4: Configuration & Entry Points**
**Focus**: Symbol configuration and CLI/API entry points

#### Symbol Configuration Loading
```python
# File: cyberdelta/config/symbol_config_loader.py
from cyberdelta.core.symbols import symbol, exchanges, symbols
from cyberdelta.core.symbols.models import Symbol

class SymbolConfigLoader:
    @staticmethod
    def load_symbol_configs(config_path: str) -> dict[str, Symbol]:
        """Load Symbol configurations."""
        
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        symbols_dict = {}
        
        for symbol_config in config["symbols"]:
            # Create Symbol from config
            exchange_name = ExchangeName(symbol_config["exchange"])
            
            if exchange_name == ExchangeName.HYPERLIQUID:
                symbol_obj = exchanges.hyperliquid(
                    symbol_config["value"],
                    asset_index=symbol_config.get("asset_index")
                )
            elif exchange_name == ExchangeName.BACKPACK:
                symbol_obj = exchanges.backpack(
                    symbol_config["value"],
                    symbol_id=symbol_config["symbol_id"]
                )
            
            # Store with config key
            key = symbol_config["key"]  # e.g., "btc_perp"
            symbols_dict[key] = symbol_obj
            
        return symbols_dict
```

#### CLI Entry Points
```python
# File: cyberdelta/cli/symbol_commands.py
import click
from cyberdelta.core.symbols import exchanges, symbol

@click.command()
@click.option('--symbol', '-s', required=True, help='Symbol value (e.g., BTC-PERP)')
@click.option('--exchange', '-e', required=True, 
              type=click.Choice(['hyperliquid', 'backpack']))
@click.option('--asset-index', type=int, help='Asset index for Hyperliquid')
@click.option('--symbol-id', type=int, help='Symbol ID for Backpack')
def get_ticker(symbol: str, exchange: str, asset_index: int | None, symbol_id: int | None):
    """Get ticker for a symbol."""
    
    # Create Symbol object from CLI args
    exchange_enum = ExchangeName(exchange.upper())
    
    if exchange_enum == ExchangeName.HYPERLIQUID:
        symbol_obj = exchanges.hyperliquid(symbol, asset_index=asset_index)
    elif exchange_enum == ExchangeName.BACKPACK:
        if not symbol_id:
            raise click.BadParameter("--symbol-id required for Backpack")
        symbol_obj = exchanges.backpack(symbol, symbol_id=symbol_id)
    
    # Use Symbol object in business logic
    ticker = asyncio.run(fetch_ticker(symbol_obj))
    
    # Display results
    click.echo(f"Ticker for {symbol_obj.value} on {symbol_obj.exchange.value}:")
    click.echo(f"  Price: {ticker.last_price}")
    click.echo(f"  Volume: {ticker.volume_24h}")
```

#### REST API Entry Points
```python
# File: cyberdelta/api/symbol_endpoints.py
from fastapi import APIRouter, HTTPException
from cyberdelta.core.symbols import exchanges

router = APIRouter(prefix="/api/v1")

@router.get("/ticker/{exchange}/{symbol}")
async def get_ticker(
    exchange: str,
    symbol: str,
    asset_index: int | None = None,
    symbol_id: int | None = None
):
    """Get ticker with Symbol handling."""
    
    # Parse and validate exchange
    try:
        exchange_enum = ExchangeName(exchange.upper())
    except ValueError:
        raise HTTPException(400, f"Invalid exchange: {exchange}")
    
    # Create Symbol object
    try:
        if exchange_enum == ExchangeName.HYPERLIQUID:
            symbol_obj = exchanges.hyperliquid(symbol, asset_index=asset_index)
        elif exchange_enum == ExchangeName.BACKPACK:
            if not symbol_id:
                raise HTTPException(400, "symbol_id required for Backpack")
            symbol_obj = exchanges.backpack(symbol, symbol_id=symbol_id)
        else:
            raise HTTPException(400, f"Unsupported exchange: {exchange}")
    except Exception as e:
        raise HTTPException(400, f"Invalid symbol: {str(e)}")
    
    # Get ticker using Symbol
    ticker_service = get_ticker_service(exchange_enum)
    ticker = await ticker_service.get_ticker(GetTickerArgs(symbol=symbol_obj))
    
    # Return response with Symbol info
    return {
        "symbol": symbol_obj.value,
        "exchange": symbol_obj.exchange.value,
        "price": str(ticker.last_price),
        "volume": str(ticker.volume_24h),
        "timestamp": ticker.timestamp.isoformat()
    }
```

## 🎯 Week 4 Success Criteria

### Infrastructure Integration ✅
- [ ] WebSocket handlers create Symbol objects
- [ ] Cache uses Symbol-based keys
- [ ] Logging includes Symbol context
- [ ] Metrics track by Symbol labels
- [ ] Configuration loads Symbol objects

### Clean Architecture ✅
- [ ] Entry points create Symbols immediately
- [ ] Symbol objects flow through infrastructure
- [ ] String conversion only at boundaries
- [ ] No backward compatibility code

### Performance & Reliability ✅
- [ ] Caching improves Symbol lookup performance
- [ ] WebSocket reconnection maintains Symbol subscriptions
- [ ] Metrics provide Symbol-level observability
- [ ] Configuration validates Symbol constraints

## 🚨 Week 4 Key Patterns

### Entry Point Creation
```python
# Create Symbol at system entry points
symbol_obj = exchanges.hyperliquid("BTC-PERP")
```

### Cache Key Generation
```python
# Consistent cache keys from Symbols
cache_key = f"{prefix}:{symbol.exchange.value}:{symbol.value}"
```

### Structured Context
```python
# Add Symbol fields to structured logs
logger.info("Event", symbol=symbol_obj)
# Logs: {"symbol_value": "BTC-PERP", "symbol_exchange": "hyperliquid"}
```

### Metric Labels
```python
# Use Symbol properties as metric labels
metric.labels(
    exchange=symbol.exchange.value,
    symbol=symbol.value
).inc()
```

## 📊 Week 4 Metrics

- **Infrastructure Components**: 100% Symbol integration ✅
- **Entry Points**: All create Symbol objects ✅
- **Caching Strategy**: Symbol-aware ✅
- **Observability**: Full Symbol context ✅

**Week 4 completes infrastructure integration - ready for final testing and validation!** 🚀