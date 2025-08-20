# Required Classes and Methods for Backpack Connector

## Overview
This document details all required classes and methods that must be implemented for a functional Hummingbot exchange connector. Each method is marked as REQUIRED or OPTIONAL with implementation guidelines.

## 1. BackpackExchange Class

The main connector class inheriting from `ExchangePyBase`.

### Required Properties (Abstract from Base)

```python
@property
def name(self) -> str:
    """REQUIRED: Exchange name identifier"""
    return "backpack"

@property
def authenticator(self) -> AuthBase:
    """REQUIRED: Return authentication instance"""
    return BackpackAuth(self.backpack_api_key, self.backpack_api_secret)

@property
def rate_limits_rules(self) -> List[RateLimit]:
    """REQUIRED: Define rate limiting rules"""
    return CONSTANTS.RATE_LIMITS

@property
def domain(self) -> str:
    """REQUIRED: Exchange domain (mainnet/testnet)"""
    return self._domain

@property
def client_order_id_max_length(self) -> int:
    """REQUIRED: Maximum length for client order IDs"""
    return CONSTANTS.MAX_ORDER_ID_LEN  # e.g., 32

@property
def client_order_id_prefix(self) -> str:
    """REQUIRED: Prefix for client order IDs"""
    return CONSTANTS.BROKER_ID  # e.g., "HBOT"

@property
def trading_rules_request_path(self) -> str:
    """REQUIRED: API endpoint for trading rules"""
    return CONSTANTS.EXCHANGE_INFO_URL

@property
def trading_pairs_request_path(self) -> str:
    """REQUIRED: API endpoint for trading pairs"""
    return CONSTANTS.EXCHANGE_INFO_URL

@property
def check_network_request_path(self) -> str:
    """REQUIRED: API endpoint for network check"""
    return CONSTANTS.PING_URL

@property
def trading_pairs(self) -> List[str]:
    """REQUIRED: List of trading pairs"""
    return self._trading_pairs

@property
def is_cancel_request_in_exchange_synchronous(self) -> bool:
    """REQUIRED: Whether cancel is synchronous"""
    return True

@property
def is_trading_required(self) -> bool:
    """REQUIRED: Whether trading auth is required"""
    return self._trading_required
```

### Required Trading Methods

```python
async def _place_order(
    self,
    order_id: str,
    trading_pair: str,
    amount: Decimal,
    trade_type: TradeType,
    order_type: OrderType,
    price: Optional[Decimal] = None,
    **kwargs
) -> Tuple[str, float]:
    """
    REQUIRED: Place an order on the exchange
    Returns: (exchange_order_id, timestamp)
    """
    # Implementation required

async def _place_cancel(
    self,
    order_id: str,
    tracked_order: InFlightOrder
) -> bool:
    """
    REQUIRED: Cancel an order on the exchange
    Returns: True if successful
    """
    # Implementation required

async def _update_trading_rules(self) -> Dict[str, TradingRule]:
    """
    REQUIRED: Fetch and update trading rules
    Returns: Dictionary of trading rules by pair
    """
    # Implementation required

async def _update_balances(self) -> Dict[str, Decimal]:
    """
    REQUIRED: Update account balances
    Returns: Dictionary of balances by asset
    """
    # Implementation required

async def _all_trade_updates_for_order(
    self,
    order: InFlightOrder
) -> List[TradeUpdate]:
    """
    REQUIRED: Get all trades for an order
    Returns: List of TradeUpdate objects
    """
    # Implementation required

async def _request_order_status(
    self,
    tracked_order: InFlightOrder
) -> OrderUpdate:
    """
    REQUIRED: Request current order status
    Returns: OrderUpdate object
    """
    # Implementation required
```

### Required Factory Methods

```python
def _create_web_assistants_factory(self) -> WebAssistantsFactory:
    """
    REQUIRED: Create web assistants factory for API calls
    """
    return web_utils.build_api_factory(
        throttler=self._throttler,
        time_synchronizer=self._time_synchronizer,
        auth=self._auth,
    )

def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
    """
    REQUIRED: Create order book data source
    """
    return BackpackAPIOrderBookDataSource(
        trading_pairs=self._trading_pairs,
        connector=self,
        api_factory=self._web_assistants_factory,
    )

def _create_user_stream_tracker_data_source(self) -> UserStreamTrackerDataSource:
    """
    REQUIRED: Create user stream data source
    """
    return BackpackAPIUserStreamDataSource(
        auth=self._auth,
        trading_pairs=self._trading_pairs,
        connector=self,
        api_factory=self._web_assistants_factory,
    )

def _create_user_stream_tracker(self) -> UserStreamTracker:
    """
    REQUIRED: Create user stream tracker
    """
    return UserStreamTracker(
        data_source=self._create_user_stream_tracker_data_source()
    )
```

### Required Helper Methods

```python
async def _make_network_check_request(self):
    """
    REQUIRED: Perform network connectivity check
    """
    await self._api_get(path_url=self.check_network_request_path)

async def _make_trading_rules_request(self) -> Any:
    """
    REQUIRED: Request trading rules from exchange
    """
    return await self._api_get(path_url=self.trading_rules_request_path)

async def _make_trading_pairs_request(self) -> Any:
    """
    REQUIRED: Request available trading pairs
    """
    return await self._api_get(path_url=self.trading_pairs_request_path)

def _is_request_exception_related_to_time_synchronizer(
    self,
    request_exception: Exception
) -> bool:
    """
    REQUIRED: Check if exception is time-sync related
    """
    # Check for timestamp-related error messages
    error_str = str(request_exception).lower()
    return "timestamp" in error_str or "time" in error_str
```

### Required Event Processing Methods

```python
async def _user_stream_event_listener(self):
    """
    REQUIRED: Process user stream events
    """
    async for event_message in self._iter_user_event_queue():
        try:
            event_type = event_message.get("type")

            if event_type == "balanceUpdate":
                self._process_balance_message(event_message)
            elif event_type == "orderUpdate":
                self._process_order_message(event_message)
            elif event_type == "tradeUpdate":
                self._process_trade_message(event_message)

        except Exception:
            self.logger().error("Error processing user stream", exc_info=True)

def _process_balance_message(self, balance_msg: Dict[str, Any]):
    """
    REQUIRED: Process balance update messages
    """
    # Update self._account_balances
    # Update self._account_available_balances

def _process_order_message(self, order_msg: Dict[str, Any]):
    """
    REQUIRED: Process order update messages
    """
    # Update tracked orders in self._order_tracker

def _process_trade_message(self, trade_msg: Dict[str, Any]):
    """
    REQUIRED: Process trade execution messages
    """
    # Create TradeUpdate and update tracked order
```

## 2. BackpackAuth Class

Authentication handler implementing `AuthBase`.

### Required Methods

```python
class BackpackAuth(AuthBase):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        time_provider: Optional[Callable] = None
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self._time_provider = time_provider or (lambda: int(time.time() * 1000))

    async def rest_authenticate(
        self,
        request: RESTRequest,
    ) -> RESTRequest:
        """
        REQUIRED: Add authentication headers to REST request
        """
        headers = {}
        if request.method == RESTMethod.POST:
            headers.update(self._generate_auth_headers(
                method=request.method.value,
                path=request.url,
                body=request.data
            ))
        request.headers.update(headers)
        return request

    async def ws_authenticate(
        self,
        request: WSJSONRequest
    ) -> WSJSONRequest:
        """
        REQUIRED: Add authentication to WebSocket request
        """
        request.payload.update(self._generate_ws_auth_payload())
        return request

    def _generate_auth_headers(
        self,
        method: str,
        path: str,
        body: Optional[str] = None
    ) -> Dict[str, str]:
        """
        REQUIRED: Generate Ed25519 signature headers
        """
        timestamp = str(self._time_provider())
        signature_payload = f"{timestamp}{method}{path}"
        if body:
            signature_payload += body

        signature = self._sign_payload(signature_payload)

        return {
            "X-API-Key": self.api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
            "X-Window": "5000",  # 5 second window
        }

    def _sign_payload(self, payload: str) -> str:
        """
        REQUIRED: Sign payload with Ed25519
        """
        # Use nacl or cryptography library
        # Return base64 encoded signature
```

## 3. BackpackAPIOrderBookDataSource Class

Handles public market data streams.

### Required Methods

```python
class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None
    ) -> Dict[str, float]:
        """
        REQUIRED: Get last traded prices for pairs
        """
        # Fetch from REST API or cache

    async def fetch_trading_pairs(
        self,
        domain: Optional[str] = None
    ) -> List[str]:
        """
        REQUIRED: Fetch all available trading pairs
        """
        # Query exchange for available pairs

    async def get_order_book_data(
        self,
        trading_pair: str
    ) -> Dict[str, Any]:
        """
        REQUIRED: Get order book snapshot
        """
        # Fetch order book from REST API

    async def listen_for_order_book_diffs(
        self,
        ev_loop: asyncio.AbstractEventLoop,
        output: asyncio.Queue
    ):
        """
        REQUIRED: Stream order book updates via WebSocket
        """
        # Connect to WebSocket
        # Subscribe to order book channels
        # Put updates in output queue

    async def listen_for_order_book_snapshots(
        self,
        ev_loop: asyncio.AbstractEventLoop,
        output: asyncio.Queue
    ):
        """
        REQUIRED: Periodically fetch order book snapshots
        """
        # Fetch snapshots via REST at intervals
        # Put snapshots in output queue

    async def listen_for_trades(
        self,
        ev_loop: asyncio.AbstractEventLoop,
        output: asyncio.Queue
    ):
        """
        OPTIONAL: Stream trade data via WebSocket
        """
        # Connect to WebSocket
        # Subscribe to trade channels
        # Put trades in output queue
```

## 4. BackpackAPIUserStreamDataSource Class

Handles private account data streams.

### Required Methods

```python
class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):

    @property
    def last_recv_time(self) -> float:
        """
        REQUIRED: Timestamp of last received message
        """
        return self._last_recv_time

    async def listen_for_user_stream(
        self,
        output: asyncio.Queue
    ):
        """
        REQUIRED: Stream private account updates
        """
        # Connect to private WebSocket
        # Authenticate
        # Subscribe to private channels
        # Put updates in output queue

    async def _authenticate_websocket(
        self,
        ws: WSAssistant
    ) -> bool:
        """
        REQUIRED: Authenticate WebSocket connection
        """
        # Send authentication message
        # Wait for auth confirmation
        # Return success/failure
```

## 5. Utility Functions (backpack_utils.py)

### Required Functions

```python
def split_trading_pair(trading_pair: str) -> Tuple[str, str]:
    """
    REQUIRED: Split trading pair into base and quote
    Example: "BTC-USDC" -> ("BTC", "USDC")
    """
    return tuple(trading_pair.split("-"))

def convert_from_exchange_trading_pair(
    exchange_trading_pair: str
) -> str:
    """
    REQUIRED: Convert exchange format to Hummingbot format
    Example: "BTC_USDC" -> "BTC-USDC"
    """
    # Handle exchange-specific format

def convert_to_exchange_trading_pair(
    hb_trading_pair: str
) -> str:
    """
    REQUIRED: Convert Hummingbot format to exchange format
    Example: "BTC-USDC" -> "BTC_USDC"
    """
    # Handle exchange-specific format
```

## 6. Web Utils (backpack_web_utils.py)

### Required Functions

```python
def build_api_factory(
    throttler: AsyncThrottler,
    time_synchronizer: TimeSynchronizer,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    REQUIRED: Build WebAssistantsFactory instance
    """
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            time_synchronizer.time_provider,
        ]
    )
    return api_factory

async def get_current_server_time(
    throttler: AsyncThrottler,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    OPTIONAL: Get server time for synchronization
    """
    # Query server time endpoint
```

## 7. Constants (backpack_constants.py)

### Required Constants

```python
# Domain
DEFAULT_DOMAIN = "backpack"

# URLs
REST_URL = "https://api.backpack.exchange/"
WS_PUBLIC_URL = "wss://ws.backpack.exchange/stream"
WS_PRIVATE_URL = "wss://ws.backpack.exchange/stream"

# Endpoints
EXCHANGE_INFO_URL = "api/v1/capital"
ORDER_URL = "api/v1/order"
CANCEL_URL = "api/v1/order"
BALANCE_URL = "api/v1/capital"
OPEN_ORDERS_URL = "api/v1/orders"
ORDER_HISTORY_URL = "api/v1/orderHistory"
PING_URL = "api/v1/ping"
TIME_URL = "api/v1/time"

# Rate Limits
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_URL, limit=10, time_interval=1),
    RateLimit(limit_id=BALANCE_URL, limit=100, time_interval=60),
    RateLimit(limit_id=PUBLIC_ENDPOINTS, limit=1200, time_interval=60),
]

# Configuration
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

# Order States Mapping
ORDER_STATE_MAP = {
    "New": OrderState.OPEN,
    "PartiallyFilled": OrderState.PARTIALLY_FILLED,
    "Filled": OrderState.FILLED,
    "Cancelled": OrderState.CANCELED,
    "Expired": OrderState.CANCELED,
    "Rejected": OrderState.FAILED,
}
```

## Method Implementation Priority

### Phase 1: Core Trading (MUST HAVE)
1. `_place_order()` - Place orders
2. `_place_cancel()` - Cancel orders
3. `_update_balances()` - Track balances
4. `_update_trading_rules()` - Get trading constraints
5. Authentication methods

### Phase 2: Order Tracking (MUST HAVE)
1. `_user_stream_event_listener()` - Process account updates
2. `_process_order_message()` - Handle order updates
3. `_request_order_status()` - Poll order status
4. `_all_trade_updates_for_order()` - Get trade history

### Phase 3: Market Data (SHOULD HAVE)
1. `listen_for_order_book_diffs()` - Stream order books
2. `listen_for_order_book_snapshots()` - Periodic snapshots
3. `get_last_traded_prices()` - Price feeds

### Phase 4: Advanced Features (NICE TO HAVE)
1. `listen_for_trades()` - Trade stream
2. Position tracking for derivatives
3. Funding rate updates
4. Advanced order types

## Testing Requirements

Every required method needs corresponding tests:

```python
# test_backpack_exchange.py
class TestBackpackExchange(unittest.TestCase):
    def test_name_property(self):
        self.assertEqual(self.exchange.name, "backpack")

    async def test_place_order(self):
        # Test successful order placement
        # Test order placement failures
        # Test parameter validation
```

## Minimal Working Implementation

For a minimal working connector, implement:
1. All required properties
2. `_place_order()` and `_place_cancel()`
3. `_update_balances()`
4. `_update_trading_rules()`
5. Basic WebSocket listeners
6. Authentication

Everything else can raise `NotImplementedError` initially.
