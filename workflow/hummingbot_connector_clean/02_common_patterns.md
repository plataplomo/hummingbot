# Hummingbot Connector - Common Patterns

## Overview
This document describes the common design patterns used in Hummingbot connectors, focusing on how to implement them for the Backpack exchange while maintaining Python 3.10 compatibility.

## Core Architectural Patterns

### 1. Inheritance Pattern

All exchange connectors inherit from `ExchangePyBase`:

```python
class BackpackExchange(ExchangePyBase):
    """
    BackpackExchange connects to Backpack exchange and provides order management
    """

    # Class variables for configuration
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_api_key: str,
        backpack_api_secret: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.backpack_api_key = backpack_api_key
        self.backpack_api_secret = backpack_api_secret
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain

        super().__init__(client_config_map)
```

### 2. Property Pattern

Use properties for required abstract methods:

```python
@property
def name(self) -> str:
    return "backpack"

@property
def authenticator(self) -> AuthBase:
    return BackpackAuth(
        api_key=self.backpack_api_key,
        api_secret=self.backpack_api_secret,
        time_provider=self._time_synchronizer
    )

@property
def rate_limits_rules(self) -> List[RateLimit]:
    return CONSTANTS.RATE_LIMITS

@property
def domain(self) -> str:
    return self._domain

@property
def client_order_id_max_length(self) -> int:
    return CONSTANTS.MAX_ORDER_ID_LEN

@property
def client_order_id_prefix(self) -> str:
    return CONSTANTS.BROKER_ID
```

### 3. Data Source Pattern

#### OrderBook Data Source
```python
class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    def __init__(
        self,
        trading_pairs: List[str],
        connector: BackpackExchange,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen to orderbook diffs via WebSocket
        """
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=CONSTANTS.WS_PUBLIC_URL,
                    message_timeout=30
                )

                # Subscribe to orderbook updates
                for trading_pair in self._trading_pairs:
                    subscribe_msg = {
                        "method": "subscribe",
                        "params": [f"depth@{trading_pair}"]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)
                    await self._process_order_book_diff(data, output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error in orderbook WebSocket", exc_info=True)
                await asyncio.sleep(5.0)
```

#### User Stream Data Source
```python
class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listen to user account updates via WebSocket
        """
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=CONSTANTS.WS_PRIVATE_URL,
                    message_timeout=30
                )

                # Authenticate
                auth_msg = self._get_auth_message()
                await ws.send(json.dumps(auth_msg))

                # Subscribe to private channels
                subscribe_msg = {
                    "method": "subscribe",
                    "params": ["account.orders", "account.balances"]
                }
                await ws.send(json.dumps(subscribe_msg))

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)
                    await output.put(data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error in user stream WebSocket", exc_info=True)
                await asyncio.sleep(5.0)
```

### 4. Order Tracking Pattern

Track in-flight orders using `ClientOrderTracker`:

```python
# In BackpackExchange class
def _create_order_tracker(self) -> ClientOrderTracker:
    return ClientOrderTracker(connector=self)

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
    Place an order and track it
    """
    # Create tracking entry
    order = InFlightOrder(
        client_order_id=order_id,
        exchange_order_id=None,
        trading_pair=trading_pair,
        order_type=order_type,
        trade_type=trade_type,
        amount=amount,
        price=price,
        creation_timestamp=self.current_timestamp
    )
    self._order_tracker.start_tracking_order(order)

    # Send order to exchange
    try:
        response = await self._api_post(
            path_url=CONSTANTS.ORDER_URL,
            data={
                "symbol": convert_to_exchange_trading_pair(trading_pair),
                "side": trade_type.name,
                "orderType": order_type.name,
                "quantity": str(amount),
                "price": str(price) if price else None,
                "clientId": order_id,
            }
        )

        exchange_order_id = response["orderId"]
        order.update_exchange_order_id(exchange_order_id)

        return order_id, self.current_timestamp

    except Exception as e:
        self._order_tracker.stop_tracking_order(order_id)
        raise
```

### 5. Event Processing Pattern

Process events from WebSocket streams:

```python
async def _process_order_update(self, order_data: Dict[str, Any]):
    """
    Process order update from WebSocket
    """
    client_order_id = order_data.get("clientId")
    if not client_order_id:
        return

    tracked_order = self._order_tracker.fetch_order(client_order_id)
    if not tracked_order:
        return

    # Update order state
    new_state = self._parse_order_state(order_data["status"])
    if new_state == OrderState.FILLED:
        tracked_order.completely_filled = True
        self._order_tracker.process_order_filled(
            client_order_id,
            tracked_order
        )
    elif new_state == OrderState.CANCELED:
        tracked_order.cancelled_event.set()
        self._order_tracker.stop_tracking_order(client_order_id)
```

### 6. Error Handling Pattern

Consistent error handling across all operations:

```python
async def _api_request(
    self,
    method: str,
    path_url: str,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    is_auth_required: bool = False,
) -> Any:
    """
    Unified API request handling with error management
    """
    async with self._throttler.execute_task(limit_id=path_url):
        # Build request
        url = f"{CONSTANTS.REST_URL}{path_url}"
        headers = {"Content-Type": "application/json"}

        if is_auth_required:
            headers.update(self.authenticator.header_for_authentication())

        # Execute request
        client = await self._api_factory.get_rest_assistant()
        try:
            async with timeout(30):
                response = await client.execute_request(
                    url=url,
                    method=RESTMethod[method.upper()],
                    headers=headers,
                    params=params,
                    data=json.dumps(data) if data else None,
                )

            if response.status != 200:
                error_msg = await response.text()
                raise IOError(f"Error {response.status}: {error_msg}")

            return await response.json()

        except asyncio.TimeoutError:
            raise IOError("Request timed out")
```

### 7. Rate Limiting Pattern

Use AsyncThrottler for rate limiting:

```python
# In constants
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_URL, limit=10, time_interval=1),
    RateLimit(limit_id=BALANCE_URL, limit=100, time_interval=60),
]

# In exchange class
async def _update_balances(self):
    """
    Update account balances with rate limiting
    """
    async with self._throttler.execute_task(limit_id=CONSTANTS.BALANCE_URL):
        response = await self._api_get(
            path_url=CONSTANTS.BALANCE_URL,
            is_auth_required=True
        )
        self._process_balance_message(response)
```

### 8. Trading Rules Pattern

Manage and validate trading rules:

```python
async def _update_trading_rules(self):
    """
    Fetch and cache trading rules from exchange
    """
    response = await self._api_get(
        path_url=CONSTANTS.EXCHANGE_INFO_URL
    )

    trading_rules = {}
    for market_info in response["symbols"]:
        try:
            trading_pair = convert_from_exchange_trading_pair(market_info["symbol"])
            trading_rules[trading_pair] = TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal(market_info["filters"]["minQty"]),
                max_order_size=Decimal(market_info["filters"]["maxQty"]),
                min_price_increment=Decimal(market_info["filters"]["tickSize"]),
                min_base_amount_increment=Decimal(market_info["filters"]["stepSize"]),
                min_notional_size=Decimal(market_info["filters"]["minNotional"]),
            )
        except Exception:
            self.logger().error(f"Error parsing trading rule: {market_info}")

    self._trading_rules = trading_rules
```

### 9. Time Synchronization Pattern

Keep time synchronized with exchange:

```python
async def _update_time_synchronizer(self):
    """
    Synchronize local time with exchange server time
    """
    try:
        response = await self._api_get(
            path_url=CONSTANTS.TIME_URL
        )
        server_time = response["serverTime"]
        self._time_synchronizer.update_server_time(server_time)
    except Exception:
        self.logger().warning("Failed to update time synchronizer")
```

### 10. Balance and Position Update Pattern

Keep account state synchronized:

```python
def _process_balance_message(self, balance_data: Dict[str, Any]):
    """
    Process balance update message
    """
    for asset_balance in balance_data["balances"]:
        asset_name = asset_balance["asset"]
        free_balance = Decimal(asset_balance["free"])
        total_balance = Decimal(asset_balance["total"])

        self._account_available_balances[asset_name] = free_balance
        self._account_balances[asset_name] = total_balance
```

## WebSocket Patterns

### Connection Management
```python
async def _manage_websocket_connection(self):
    """
    Maintain WebSocket connection with auto-reconnect
    """
    while True:
        try:
            await self._inner_ws_messages_loop()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("WebSocket error, reconnecting...", exc_info=True)
            await asyncio.sleep(5.0)
```

### Message Router Pattern
```python
async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
    """
    Route WebSocket messages to appropriate handlers
    """
    async for ws_response in websocket_assistant.iter_messages():
        try:
            data = json.loads(ws_response.data)
            message_type = data.get("type")

            if message_type == "orderUpdate":
                await self._process_order_update(data)
            elif message_type == "balanceUpdate":
                self._process_balance_message(data)
            elif message_type == "trade":
                await self._process_trade_update(data)
            else:
                self.logger().debug(f"Unknown message type: {message_type}")

        except Exception:
            self.logger().error("Error processing WebSocket message", exc_info=True)
```

## Anti-Patterns to Avoid

### 1. Don't Use Python 3.11+ Features
```python
# DON'T use match/case (Python 3.10 doesn't support)
# match order_status:
#     case "NEW":
#         return OrderState.OPEN

# DO use if/elif
if order_status == "NEW":
    return OrderState.OPEN
elif order_status == "FILLED":
    return OrderState.FILLED
```

### 2. Don't Use Pydantic v2
```python
# DON'T use Pydantic models
# from pydantic import BaseModel
# class OrderRequest(BaseModel):
#     symbol: str

# DO use dictionaries or dataclasses
from dataclasses import dataclass
@dataclass
class OrderRequest:
    symbol: str
    side: str
    quantity: Decimal
```

### 3. Don't Block the Event Loop
```python
# DON'T use blocking calls
# response = requests.get(url)  # Blocks!

# DO use async
response = await self._api_get(url)
```

### 4. Don't Ignore Rate Limits
```python
# DON'T make unlimited requests
# for symbol in symbols:
#     await self._api_get(f"/ticker/{symbol}")

# DO use rate limiting
for symbol in symbols:
    async with self._throttler.execute_task(limit_id=TICKER_URL):
        await self._api_get(f"/ticker/{symbol}")
```

## Testing Patterns

### Mock WebSocket responses:
```python
@patch("aiohttp.ClientSession.ws_connect")
async def test_listen_for_user_stream(self, mock_ws):
    mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

    self.mocking_assistant.add_websocket_aiohttp_message(
        mock_ws.return_value,
        json.dumps({"type": "balanceUpdate", "asset": "BTC", "free": "1.5"})
    )

    await self.data_source.listen_for_user_stream(self.output_queue)

    received = await self.output_queue.get()
    self.assertEqual(received["asset"], "BTC")
```

These patterns form the foundation of a robust, maintainable connector that integrates smoothly with Hummingbot's architecture.
