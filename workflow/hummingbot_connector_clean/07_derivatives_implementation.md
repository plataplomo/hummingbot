# Backpack Derivatives (Perpetual) Connector Implementation Guide

## Overview
This document provides a comprehensive guide for implementing derivatives (perpetual futures) support for the Backpack exchange in Hummingbot. It leverages insights from CyberDelta's implementation and follows Hummingbot's established patterns for perpetual connectors.

## Current Status

### What Exists
- **Spot Connector**: Fully implemented in `hummingbot-dev` worktree (~/2,334 lines)
- **CyberDelta Derivatives**: Complete perpetual implementation in CyberDelta codebase
- **Backpack API Support**: Full perpetual futures API with positions, funding, leverage

### What's Missing
- **Hummingbot Perpetual Connector**: No derivatives implementation in Hummingbot yet
- **Position Tracking**: Not implemented for Backpack
- **Funding Rate Updates**: Not implemented
- **Leverage Management**: Not configured

## Architecture Overview

### Inheritance Hierarchy
```
PerpetualDerivativePyBase (base class)
    ↑
    |
BackpackPerpetualDerivative (our implementation)
    - Inherits perpetual-specific functionality
    - Implements Backpack-specific logic
    - Manages positions, funding, leverage
```

### Key Differences from Spot Connector

| Feature | Spot Connector | Perpetual Connector |
|---------|---------------|-------------------|
| Base Class | `ExchangePyBase` | `PerpetualDerivativePyBase` |
| Position Tracking | Balances only | Positions with P&L |
| Order Types | Buy/Sell | Open/Close positions |
| Funding | N/A | Funding rates & payments |
| Leverage | N/A | Configurable leverage |
| Margin | N/A | Initial/Maintenance margin |
| Liquidation | N/A | Liquidation price tracking |

## Implementation Structure

### File Structure
```
hummingbot/connector/derivative/backpack_perpetual/
├── __init__.py
├── backpack_perpetual_derivative.py          # Main connector class
├── backpack_perpetual_auth.py                # Ed25519 authentication (reuse spot)
├── backpack_perpetual_constants.py           # API endpoints, rate limits
├── backpack_perpetual_utils.py               # Helper functions
├── backpack_perpetual_web_utils.py           # HTTP/WebSocket builders
├── backpack_perpetual_api_order_book_data_source.py  # Market data
├── backpack_perpetual_user_stream_data_source.py     # Private streams
└── backpack_perpetual_position_tracker.py    # Position management
```

### Test Structure
```
test/hummingbot/connector/derivative/backpack_perpetual/
├── __init__.py
├── test_backpack_perpetual_derivative.py
├── test_backpack_perpetual_auth.py
├── test_backpack_perpetual_api_order_book_data_source.py
├── test_backpack_perpetual_user_stream_data_source.py
└── test_backpack_perpetual_position_tracker.py
```

## Core Implementation Details

### 1. Main Connector Class

```python
# backpack_perpetual_derivative.py

from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import PositionMode, PositionAction
from hummingbot.core.data_type.funding_info import FundingInfo

class BackpackPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Backpack perpetual futures connector implementing derivatives trading.
    """

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
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._position_mode = PositionMode.ONEWAY  # Backpack default
        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return "backpack_perpetual"

    @property
    def funding_fee_poll_interval(self) -> int:
        """Poll funding rates every 10 minutes"""
        return 600

    def supported_position_modes(self) -> List[PositionMode]:
        """Backpack supports ONE-WAY mode only (from CyberDelta analysis)"""
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for long positions"""
        # Backpack uses USDC as collateral for all perpetuals
        return "USDC"

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for short positions"""
        return "USDC"
```

### 2. Constants and Endpoints

```python
# backpack_perpetual_constants.py

from hummingbot.core.api_throttler.data_types import RateLimit

# Domain
DEFAULT_DOMAIN = "backpack_perpetual"

# Base URLs
REST_URL = "https://api.backpack.exchange/"
WS_PUBLIC_URL = "wss://ws.backpack.exchange/stream"
WS_PRIVATE_URL = "wss://ws.backpack.exchange/stream"

# Public Endpoints
EXCHANGE_INFO_URL = "api/v1/capital"
TICKER_URL = "api/v1/ticker"
ORDER_BOOK_URL = "api/v1/depth"
TRADES_URL = "api/v1/trades"
CANDLES_URL = "api/v1/klines"
FUNDING_RATE_URL = "api/v1/funding"
FUNDING_HISTORY_URL = "api/v1/fundingRates"
MARK_PRICE_URL = "api/v1/markPrice"

# Private Endpoints (Require Authentication)
ORDER_URL = "api/v1/order"
CANCEL_URL = "api/v1/order"
OPEN_ORDERS_URL = "api/v1/orders"
ORDER_HISTORY_URL = "api/v1/orderHistory"
FILLS_URL = "api/v1/fills"

# Account Endpoints
BALANCE_URL = "api/v1/capital"
POSITIONS_URL = "api/v1/positions"  # Key perpetual endpoint
LEVERAGE_URL = "api/v1/leverage"    # Set leverage
MARGIN_TYPE_URL = "api/v1/marginType"  # Isolated/Cross

# WebSocket Channels
WS_ACCOUNT_ORDERS_CHANNEL = "account.orders"
WS_ACCOUNT_BALANCES_CHANNEL = "account.balances"
WS_ACCOUNT_POSITIONS_CHANNEL = "account.positions"  # Perpetual-specific
WS_FUNDING_RATE_CHANNEL = "funding"  # Perpetual-specific

# Rate Limits
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_URL, limit=10, time_interval=1),
    RateLimit(limit_id=POSITIONS_URL, limit=100, time_interval=60),
    RateLimit(limit_id=LEVERAGE_URL, limit=10, time_interval=60),
    RateLimit(limit_id=FUNDING_RATE_URL, limit=100, time_interval=60),
]

# Configuration
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

# Order Type Mapping
ORDER_TYPE_MAP = {
    "LIMIT": "Limit",
    "MARKET": "Market",
    "LIMIT_MAKER": "PostOnly",
}

# Order Side Mapping (includes position actions)
ORDER_SIDE_MAP = {
    "BUY": "Buy",
    "SELL": "Sell",
}
```

### 3. Position Management

```python
# backpack_perpetual_position_tracker.py

from decimal import Decimal
from typing import Dict, Optional
from hummingbot.connector.derivative.position import Position
from cyberdelta.models import DerivativePosition

class BackpackPositionTracker:
    """
    Tracks and manages perpetual positions for Backpack.
    """

    def __init__(self):
        self._positions: Dict[str, Position] = {}

    def update_position_from_raw(self, raw_position: dict) -> Position:
        """
        Updates position from Backpack API response.

        From CyberDelta's bp_raw_position.py:
        - symbol: Trading pair (e.g., "BTC_USDC")
        - netQuantity: Position size (positive for long, negative for short)
        - entryPrice: Average entry price
        - markPrice: Current mark price
        - pnlUnrealized: Unrealized P&L
        - estLiquidationPrice: Estimated liquidation price
        - breakEvenPrice: Break-even price including fees
        """
        symbol = self._convert_symbol(raw_position["symbol"])

        # Parse position data
        net_quantity = Decimal(raw_position["netQuantity"])
        is_long = net_quantity > 0

        position = Position(
            trading_pair=symbol,
            position_side=PositionSide.LONG if is_long else PositionSide.SHORT,
            unrealized_pnl=Decimal(raw_position["pnlUnrealized"]),
            entry_price=Decimal(raw_position["entryPrice"]),
            amount=abs(net_quantity),
            leverage=self._calculate_leverage(raw_position),
        )

        # Store additional Backpack-specific data
        position.mark_price = Decimal(raw_position["markPrice"])
        position.liquidation_price = Decimal(raw_position["estLiquidationPrice"])
        position.break_even_price = Decimal(raw_position["breakEvenPrice"])
        position.realized_pnl = Decimal(raw_position["pnlRealized"])
        position.cumulative_funding = Decimal(raw_position["cumulativeFundingPayment"])

        self._positions[symbol] = position
        return position

    def _convert_symbol(self, backpack_symbol: str) -> str:
        """Convert Backpack format (BTC_USDC) to Hummingbot format (BTC-USDC)"""
        return backpack_symbol.replace("_", "-")
```

### 4. Funding Rate Management

```python
# In backpack_perpetual_derivative.py

async def _update_funding_info(self):
    """
    Updates funding rate information for all trading pairs.
    Called periodically based on funding_fee_poll_interval.
    """
    tasks = []
    for trading_pair in self._trading_pairs:
        tasks.append(self._fetch_funding_rate(trading_pair))

    funding_infos = await safe_gather(*tasks, return_exceptions=True)

    for trading_pair, funding_info in zip(self._trading_pairs, funding_infos):
        if isinstance(funding_info, Exception):
            self.logger().error(
                f"Error fetching funding rate for {trading_pair}: {funding_info}"
            )
        else:
            self._perpetual_trading.set_funding_info(trading_pair, funding_info)

async def _fetch_funding_rate(self, trading_pair: str) -> FundingInfo:
    """
    Fetches current funding rate for a trading pair.

    From Backpack API:
    - GET /api/v1/funding?symbol=BTC_USDC
    - Returns: nextFundingRate, nextFundingTime, currentFundingRate
    """
    symbol = utils.convert_to_exchange_trading_pair(trading_pair)

    response = await self._api_get(
        path_url=CONSTANTS.FUNDING_RATE_URL,
        params={"symbol": symbol}
    )

    return FundingInfo(
        trading_pair=trading_pair,
        index_price=Decimal(response.get("indexPrice", "0")),
        mark_price=Decimal(response.get("markPrice", "0")),
        next_funding_utc_timestamp=int(response["nextFundingTime"]),
        rate=Decimal(response["nextFundingRate"]),
    )

async def get_funding_payments(self, trading_pair: str) -> List[FundingPayment]:
    """
    Retrieves historical funding payments for a position.

    From Backpack API:
    - GET /api/v1/fundingRates?symbol=BTC_USDC&limit=100
    """
    symbol = utils.convert_to_exchange_trading_pair(trading_pair)

    response = await self._api_get(
        path_url=CONSTANTS.FUNDING_HISTORY_URL,
        params={"symbol": symbol, "limit": 100},
        is_auth_required=True
    )

    payments = []
    for payment_data in response:
        payments.append(FundingPayment(
            timestamp=payment_data["timestamp"],
            funding_rate=Decimal(payment_data["fundingRate"]),
            payment=Decimal(payment_data["payment"]),
        ))

    return payments
```

### 5. Leverage Configuration

```python
# In backpack_perpetual_derivative.py

async def _execute_set_leverage(self, trading_pair: str, leverage: int):
    """
    Sets leverage for a trading pair.

    From Backpack API:
    - POST /api/v1/leverage
    - Body: {"symbol": "BTC_USDC", "leverage": 10}
    """
    symbol = utils.convert_to_exchange_trading_pair(trading_pair)

    response = await self._api_post(
        path_url=CONSTANTS.LEVERAGE_URL,
        data={
            "symbol": symbol,
            "leverage": leverage,
        },
        is_auth_required=True
    )

    if response.get("success"):
        self._perpetual_trading.set_leverage(trading_pair, leverage)
        self.logger().info(
            f"Leverage set to {leverage}x for {trading_pair}"
        )
    else:
        raise ValueError(f"Failed to set leverage: {response}")

async def _get_current_leverage(self, trading_pair: str) -> int:
    """
    Gets current leverage setting for a trading pair.
    This is typically included in position data.
    """
    positions = await self._fetch_positions()

    symbol = utils.convert_to_exchange_trading_pair(trading_pair)
    for position in positions:
        if position.get("symbol") == symbol:
            # Extract leverage from position IMF function
            imf_function = position.get("imfFunction", {})
            # Calculate effective leverage from margin requirements
            return self._calculate_leverage_from_imf(imf_function)

    return 1  # Default leverage
```

### 6. Order Placement with Position Actions

```python
# In backpack_perpetual_derivative.py

async def _place_order(
    self,
    order_id: str,
    trading_pair: str,
    amount: Decimal,
    trade_type: TradeType,
    order_type: OrderType,
    price: Optional[Decimal] = None,
    position_action: PositionAction = PositionAction.OPEN,
    **kwargs
) -> Tuple[str, float]:
    """
    Places an order with position action support.

    For perpetuals, we need to handle:
    - Opening new positions (long/short)
    - Closing existing positions
    - Adding to positions
    - Reducing positions
    """
    symbol = utils.convert_to_exchange_trading_pair(trading_pair)

    # Determine if this is a reduce-only order
    reduce_only = position_action == PositionAction.CLOSE

    order_data = {
        "symbol": symbol,
        "side": self.backpack_order_side(trade_type),
        "orderType": self.backpack_order_type(order_type),
        "quantity": str(amount),
        "clientId": order_id,
        "reduceOnly": reduce_only,  # Important for position management
    }

    if order_type == OrderType.LIMIT:
        order_data["price"] = str(price)

    # Add time in force
    order_data["timeInForce"] = kwargs.get("time_in_force", "GTC")

    # Add post-only flag for maker orders
    if order_type == OrderType.LIMIT_MAKER:
        order_data["postOnly"] = True

    response = await self._api_post(
        path_url=CONSTANTS.ORDER_URL,
        data=order_data,
        is_auth_required=True
    )

    exchange_order_id = response["orderId"]

    # Track the order with position action
    self.start_tracking_order(
        order_id=order_id,
        exchange_order_id=exchange_order_id,
        trading_pair=trading_pair,
        trade_type=trade_type,
        price=price,
        amount=amount,
        order_type=order_type,
        position_action=position_action,
        leverage=self.get_leverage(trading_pair),
    )

    return exchange_order_id, self.current_timestamp
```

### 7. WebSocket Streams for Perpetuals

```python
# backpack_perpetual_user_stream_data_source.py

class BackpackPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Handles private WebSocket streams for perpetual markets.
    """

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listens to user stream including position updates and funding payments.
        """
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=CONSTANTS.WS_PRIVATE_URL,
                    message_timeout=30
                )

                # Authenticate
                auth_msg = self._auth.get_ws_auth_message()
                await ws.send(json.dumps(auth_msg))

                # Subscribe to perpetual-specific channels
                subscribe_msg = {
                    "method": "subscribe",
                    "params": {
                        "subscriptions": [
                            "account.orders",      # Order updates
                            "account.balances",    # Balance updates
                            "account.positions",   # Position updates (perpetual-specific)
                            "account.funding",     # Funding payments (perpetual-specific)
                            "account.liquidation", # Liquidation warnings
                        ]
                    }
                }
                await ws.send(json.dumps(subscribe_msg))

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)
                    await self._process_event(data, output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error in user stream", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_event(self, event: dict, output: asyncio.Queue):
        """
        Processes WebSocket events and routes them appropriately.
        """
        event_type = event.get("type")

        if event_type == "position":
            # Position update event
            await output.put({
                "type": "position_update",
                "data": event["data"]
            })
        elif event_type == "funding":
            # Funding payment event
            await output.put({
                "type": "funding_payment",
                "data": event["data"]
            })
        elif event_type == "liquidation":
            # Liquidation warning
            await output.put({
                "type": "liquidation_warning",
                "data": event["data"]
            })
        else:
            # Handle standard events (orders, balances)
            await output.put(event)
```

### 8. Event Processing for Perpetuals

```python
# In backpack_perpetual_derivative.py

def _process_position_message(self, position_msg: Dict[str, Any]):
    """
    Processes position update messages from WebSocket.
    """
    try:
        # Update position tracker
        position = self._position_tracker.update_position_from_raw(
            position_msg["data"]
        )

        # Update internal perpetual trading state
        self._perpetual_trading.update_position(
            position.trading_pair,
            position
        )

        # Trigger position update event
        self.trigger_event(
            MarketEvent.PositionUpdate,
            PositionUpdateEvent(
                timestamp=self.current_timestamp,
                trading_pair=position.trading_pair,
                position=position,
            )
        )
    except Exception:
        self.logger().error("Error processing position update", exc_info=True)

def _process_funding_payment_message(self, funding_msg: Dict[str, Any]):
    """
    Processes funding payment messages.
    """
    try:
        data = funding_msg["data"]

        self.trigger_event(
            MarketEvent.FundingPaymentCompleted,
            FundingPaymentCompletedEvent(
                timestamp=self.current_timestamp,
                trading_pair=self._convert_symbol(data["symbol"]),
                funding_rate=Decimal(data["fundingRate"]),
                payment=Decimal(data["payment"]),
            )
        )
    except Exception:
        self.logger().error("Error processing funding payment", exc_info=True)

def _process_liquidation_warning(self, liquidation_msg: Dict[str, Any]):
    """
    Processes liquidation warning messages.
    """
    try:
        data = liquidation_msg["data"]

        self.logger().warning(
            f"LIQUIDATION WARNING for {data['symbol']}: "
            f"Mark price {data['markPrice']} approaching "
            f"liquidation price {data['liquidationPrice']}"
        )

        # Could trigger custom event for strategy to handle
        self.trigger_event(
            MarketEvent.LiquidationWarning,
            LiquidationWarningEvent(
                timestamp=self.current_timestamp,
                trading_pair=self._convert_symbol(data["symbol"]),
                mark_price=Decimal(data["markPrice"]),
                liquidation_price=Decimal(data["liquidationPrice"]),
                margin_ratio=Decimal(data["marginRatio"]),
            )
        )
    except Exception:
        self.logger().error("Error processing liquidation warning", exc_info=True)
```

## Testing Strategy

### Unit Tests
1. **Authentication**: Test Ed25519 signing for perpetual endpoints
2. **Order Placement**: Test position actions (OPEN/CLOSE)
3. **Position Tracking**: Test position updates and P&L calculations
4. **Funding Rates**: Test funding rate updates and payment calculations
5. **Leverage**: Test leverage configuration and validation
6. **WebSocket**: Test position and funding event processing

### Integration Tests
1. **Live Position Management**: Open, modify, close positions
2. **Funding Rate Collection**: Verify funding rate accuracy
3. **Leverage Changes**: Test leverage modification on live positions
4. **Risk Management**: Test liquidation price calculations
5. **WebSocket Streams**: Verify real-time position updates

### Test Data from CyberDelta
Use CyberDelta's test fixtures for realistic position data:
- Position responses with IMF/MMF functions
- Funding rate structures
- Liquidation calculations
- Margin requirements

## Implementation Timeline

### Phase 1: Core Structure (Week 1)
- [ ] Create file structure and base classes
- [ ] Port authentication from spot connector
- [ ] Set up constants and endpoints
- [ ] Implement basic WebSocket connections

### Phase 2: Position Management (Week 2)
- [ ] Implement position tracker
- [ ] Add position update handlers
- [ ] Implement P&L calculations
- [ ] Add liquidation price tracking

### Phase 3: Funding & Leverage (Week 3)
- [ ] Implement funding rate polling
- [ ] Add funding payment tracking
- [ ] Implement leverage configuration
- [ ] Add margin calculations

### Phase 4: Order Management (Week 4)
- [ ] Adapt order placement for position actions
- [ ] Implement reduce-only orders
- [ ] Add position-aware order validation
- [ ] Test order lifecycle with positions

### Phase 5: Testing & Documentation (Week 5)
- [ ] Write comprehensive unit tests
- [ ] Perform integration testing
- [ ] Document API quirks and limitations
- [ ] Create usage examples

## Key Differences from Spot Implementation

### Order Management
- **Spot**: Simple buy/sell orders
- **Perpetual**: Orders affect positions (open/close/add/reduce)

### Account State
- **Spot**: Track balances per asset
- **Perpetual**: Track positions with entry price, P&L, liquidation

### Risk Management
- **Spot**: Limited to available balance
- **Perpetual**: Leverage, margin requirements, liquidation risk

### Fee Structure
- **Spot**: Trading fees only
- **Perpetual**: Trading fees + funding rates

### Market Data
- **Spot**: Price, volume, order book
- **Perpetual**: Price, volume, order book + funding rate, mark price, index price

## Configuration Requirements

### Exchange Configuration
```python
# In backpack_perpetual_constants.py

# Default configuration
DEFAULT_LEVERAGE = 1
MAX_LEVERAGE = 20
DEFAULT_POSITION_MODE = PositionMode.ONEWAY

# Margin requirements (from CyberDelta)
INITIAL_MARGIN_RATE = Decimal("0.05")  # 5% for 20x leverage
MAINTENANCE_MARGIN_RATE = Decimal("0.025")  # 2.5%

# Funding rate intervals
FUNDING_INTERVAL_HOURS = 8  # Every 8 hours
FUNDING_SETTLEMENT_TIMES = ["00:00", "08:00", "16:00"]  # UTC
```

### User Configuration
```yaml
# conf/connectors/backpack_perpetual.yml

backpack_perpetual_api_key: ""
backpack_perpetual_api_secret: ""

# Optional settings
default_leverage: 1
max_position_size_usd: 10000
enable_funding_payments: true
enable_liquidation_warnings: true
```

## Common Pitfalls and Solutions

### 1. Symbol Format
- **Issue**: Backpack uses underscore format (BTC_USDC)
- **Solution**: Consistent conversion in utils

### 2. Position Side vs Trade Side
- **Issue**: Position side (LONG/SHORT) differs from trade side (BUY/SELL)
- **Solution**: Proper mapping based on position action

### 3. Reduce-Only Orders
- **Issue**: Must flag orders that close positions
- **Solution**: Set `reduceOnly: true` for CLOSE actions

### 4. Funding Rate Timing
- **Issue**: Funding payments occur at specific times
- **Solution**: Track funding timestamps and calculate accurately

### 5. Leverage Limits
- **Issue**: Different symbols may have different max leverage
- **Solution**: Query and cache per-symbol leverage limits

## Resources and References

### Backpack API Documentation
- OpenAPI Spec: Available in CyberDelta codebase
- WebSocket Events: Documented in bp_ws_payloads.py
- Position Structure: bp_raw_position.py

### Hummingbot Examples
- Binance Perpetual: Reference implementation
- Bybit Perpetual: Alternative patterns
- OKX Perpetual: WebSocket handling

### CyberDelta Implementation
- Position Service: `bp_position_service.py`
- Funding Models: `bp_raw_funding.py`
- Margin Functions: `bp_raw_margin_functions.py`
- Position Mapper: `bp_position_mapper.py`

## Conclusion

Implementing the Backpack perpetual connector requires extending the existing spot connector with position management, funding rate tracking, and leverage configuration. The architecture follows Hummingbot's established patterns while incorporating Backpack's specific requirements.

Key success factors:
1. Leverage CyberDelta's proven implementation
2. Follow Hummingbot's perpetual patterns
3. Comprehensive testing with real positions
4. Clear documentation of Backpack-specific behaviors

The implementation can reuse significant portions of the spot connector (auth, web utils, basic order management) while adding perpetual-specific features in a modular way.
