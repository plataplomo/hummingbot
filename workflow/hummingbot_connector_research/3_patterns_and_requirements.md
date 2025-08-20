# Hummingbot Connector Patterns and Requirements

## Core Design Patterns

### 1. Event-Driven Architecture
Hummingbot uses events extensively for communication between components:

```python
# Event types from hummingbot.core.event.events
MarketEvent.BuyOrderCreated
MarketEvent.SellOrderCreated
MarketEvent.OrderFilled
MarketEvent.OrderCancelled
MarketEvent.OrderFailure
MarketEvent.OrderExpired
MarketEvent.FundingPaymentCompleted
```

**Pattern Implementation:**
```python
# Emit events when things happen
self.trigger_event(
    MarketEvent.BuyOrderCreated,
    BuyOrderCreatedEvent(
        self.current_timestamp,
        order_type,
        trading_pair,
        amount,
        price,
        order_id,
        creation_timestamp
    )
)

# Listen to events
self.add_listener(MarketEvent.OrderFilled, self._on_order_filled)
```

### 2. Polling Pattern
Regular polling for updates with different intervals:

```python
SHORT_POLL_INTERVAL = 5.0    # Order status updates
LONG_POLL_INTERVAL = 120.0   # Balance updates
UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0  # Minimum between status checks
```

**Tasks Started in `start_network():`**
- `_status_polling_task` - Check exchange connectivity
- `_user_stream_tracker_task` - WebSocket management
- `_user_stream_event_listener_task` - Process WebSocket messages
- `_trading_rules_polling_task` - Update trading rules
- `_lost_orders_update_task` - Recover lost orders

### 3. Order Lifecycle Management

```python
class InFlightOrder:
    States: OrderState.PENDING_CREATE
           OrderState.OPEN
           OrderState.PARTIALLY_FILLED
           OrderState.PENDING_CANCEL
           OrderState.CANCELLED
           OrderState.FILLED
           OrderState.FAILED
```

**Required Order Tracking:**
- Client order ID → Exchange order ID mapping
- Order state transitions
- Fill tracking (partial fills)
- Fee tracking

### 4. Trading Rules Pattern

```python
class TradingRule:
    trading_pair: str
    min_order_size: Decimal
    max_order_size: Decimal
    min_price_increment: Decimal  # Tick size
    min_base_amount_increment: Decimal  # Step size
    min_quote_amount_increment: Decimal
    min_notional_size: Decimal
    max_price_significant_digits: Decimal
    supports_limit_orders: bool
    supports_market_orders: bool
```

## Required Methods Implementation

### 1. Abstract Properties (MUST implement)

```python
@property
def name(self) -> str:
    """Exchange name identifier"""
    return "backpack"

@property
def authenticator(self) -> AuthBase:
    """Authentication handler"""
    return self._auth

@property
def rate_limits_rules(self) -> List[RateLimit]:
    """Rate limiting configuration"""
    return CONSTANTS.RATE_LIMITS

@property
def domain(self) -> str:
    """Domain for multi-domain exchanges"""
    return self._domain

@property
def client_order_id_max_length(self) -> int:
    """Maximum length for client order IDs"""
    return CONSTANTS.MAX_ORDER_ID_LEN  # Usually 32-36

@property
def client_order_id_prefix(self) -> str:
    """Prefix for client-generated order IDs"""
    return CONSTANTS.BROKER_ID  # e.g., "HMB-"

@property
def trading_rules_request_path(self) -> str:
    """API endpoint for trading rules"""
    return CONSTANTS.EXCHANGE_INFO_URL

@property
def trading_pairs_request_path(self) -> str:
    """API endpoint for available pairs"""
    return CONSTANTS.EXCHANGE_INFO_URL

@property
def check_network_request_path(self) -> str:
    """API endpoint for connectivity check"""
    return CONSTANTS.PING_URL

@property
def trading_pairs(self) -> List[str]:
    """List of trading pairs to track"""
    return self._trading_pairs

@property
def is_cancel_request_in_exchange_synchronous(self) -> bool:
    """Whether cancel requests are synchronous"""
    return True  # Most exchanges

@property
def is_trading_required(self) -> bool:
    """Whether trading credentials are required"""
    return self._trading_required
```

### 2. Order Operations (MUST implement)

```python
async def _place_order(
    self,
    order_id: str,
    trading_pair: str,
    amount: Decimal,
    order_type: OrderType,
    is_buy: bool,
    price: Optional[Decimal] = None,
    **kwargs
) -> str:
    """Place order and return exchange order ID"""

async def _cancel(self, trading_pair: str, order_id: str) -> bool:
    """Cancel order by client order ID"""

async def _update_balances(self):
    """Update account balances"""

async def _update_order_status(self):
    """Update status of in-flight orders"""
```

### 3. Data Source Creation (MUST implement)

```python
def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
    """Create order book data source"""
    return BackpackAPIOrderBookDataSource(
        trading_pairs=self._trading_pairs,
        connector=self,
        api_factory=self._web_assistants_factory,
        domain=self.domain,
    )

def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
    """Create user stream data source"""
    return BackpackAPIUserStreamDataSource(
        auth=self._auth,
        connector=self,
        api_factory=self._web_assistants_factory,
        domain=self.domain,
    )
```

### 4. Trading Rules and Fees

```python
async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
    """Parse exchange info into TradingRule objects"""

async def _update_trading_fees(self):
    """Update trading fee structure"""
    self._trading_fees = {
        trading_pair: AddedToCostTradeFee(
            percent=Decimal("0.001"),  # 0.1% example
            flat_fees=[]
        )
        for trading_pair in self._trading_pairs
    }
```

## WebSocket Requirements

### User Stream Data Source Pattern

```python
class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Main loop to listen for user updates"""
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._authenticate(ws)
                await self._subscribe_channels(ws)

                async for msg in ws:
                    msg_type = msg.get("type")
                    if msg_type == "order_update":
                        output.put_nowait(msg)
                    elif msg_type == "balance_update":
                        output.put_nowait(msg)
                    elif msg_type == "fill":
                        output.put_nowait(msg)

            except Exception as e:
                self.logger().error(f"User stream error: {e}")
                await asyncio.sleep(5)  # Reconnect delay
```

### Order Book Data Source Pattern

```python
class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Listen for order book updates"""

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Periodic snapshots for synchronization"""

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """Public trade stream"""
```

## Error Handling Patterns

### 1. Order Not Found
```python
def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
    return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
    return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)
```

### 2. Time Synchronization
```python
def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
    # Check if error is due to time sync issues
    return "timestamp" in str(request_exception).lower()
```

### 3. Rate Limiting
```python
# Define rate limits
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=30, time_interval=1),
    RateLimit(limit_id=CANCEL_URL, limit=30, time_interval=1),
    RateLimit(limit_id=BALANCE_URL, limit=10, time_interval=1),
]

# Use throttler before requests
async with self._throttler.execute_task(CONSTANTS.ORDER_URL):
    response = await self._api_request(...)
```

## Type Conversions Required

### 1. Order Types
```python
def hummingbot_to_backpack_order_type(order_type: OrderType) -> str:
    mapping = {
        OrderType.LIMIT: "Limit",
        OrderType.MARKET: "Market",
        OrderType.LIMIT_MAKER: "PostOnly",
    }
    return mapping[order_type]
```

### 2. Trading Pairs
```python
def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # "BTC-USDC" -> "BTC_USDC" or whatever Backpack uses
    return hb_trading_pair.replace("-", "_")

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    # "BTC_USDC" -> "BTC-USDC"
    return exchange_trading_pair.replace("_", "-")
```

### 3. Decimal Handling
```python
# Always use Decimal for amounts and prices
amount = Decimal(str(amount))  # Ensure Decimal
price = Decimal(str(price)) if price else None

# Quantize according to trading rules
amount = amount.quantize(self._trading_rules[trading_pair].min_base_amount_increment)
```

## State Management Requirements

### 1. In-Flight Orders
```python
# Track all active orders
self._order_tracker: ClientOrderTracker

# Add new order
self._order_tracker.start_tracking_order(
    InFlightOrder(
        client_order_id=order_id,
        exchange_order_id=None,  # Set when exchange responds
        trading_pair=trading_pair,
        order_type=order_type,
        trade_type=trade_type,
        price=price,
        amount=amount,
        creation_timestamp=self.current_timestamp,
    )
)
```

### 2. Balance Tracking
```python
self._account_balances: Dict[str, Decimal] = {}
self._account_available_balances: Dict[str, Decimal] = {}

# Update from exchange
async def _update_balances(self):
    response = await self._api_get(CONSTANTS.BALANCE_URL)
    for balance in response["balances"]:
        asset = balance["asset"]
        self._account_balances[asset] = Decimal(balance["free"]) + Decimal(balance["locked"])
        self._account_available_balances[asset] = Decimal(balance["free"])
```

## Performance Requirements

### 1. Async Everything
- All I/O operations MUST be async
- No blocking calls in event loop
- Use `asyncio.gather()` for parallel operations

### 2. Memory Management
- Limit order book depth tracking
- Prune old completed orders
- Clear outdated market data

### 3. Reconnection Logic
- Automatic WebSocket reconnection
- Exponential backoff for failures
- State recovery after disconnect

## Testing Requirements

### 1. Unit Tests
- Mock all external API calls
- Test order lifecycle states
- Verify event generation
- Test error handling paths

### 2. Integration Tests
- Test against testnet
- Verify order placement/cancellation
- Test balance updates
- WebSocket message handling

### 3. Coverage Requirements
- Minimum 80% code coverage
- All critical paths tested
- Edge cases handled

## Minimal Implementation Checklist

For a working minimal connector:

- [ ] Main exchange class with required properties
- [ ] Order placement method
- [ ] Order cancellation method
- [ ] Balance update method
- [ ] Basic auth implementation
- [ ] Constants file with endpoints
- [ ] Order book data source (can be minimal)
- [ ] User stream data source (for account updates)
- [ ] Trading rules parsing
- [ ] Utils for pair conversion
- [ ] Web utils for API factory
- [ ] Basic error handling
- [ ] Event generation for orders

Everything else can raise `NotImplementedError` initially.
