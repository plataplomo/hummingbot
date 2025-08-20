# CyberDelta to Hummingbot Wrapper Strategy

## Executive Summary

This document outlines the strategy for creating a thin wrapper around CyberDelta's Backpack API to make it compatible with Hummingbot. The wrapper acts as a translation layer, converting between CyberDelta's modern architecture and Hummingbot's requirements.

## Wrapper Architecture

```
Hummingbot Strategy
        ↓
BackpackExchange (Wrapper)
        ↓
CyberDelta BackpackAPI
        ↓
Backpack Exchange
```

## Core Wrapper Components

### 1. Main Exchange Wrapper

```python
# backpack_exchange.py
from decimal import Decimal
from typing import Dict, List, Optional, Any

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.event.events import (
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
)

# Import CyberDelta implementation
from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import BackpackSecrets
from cyberdelta.enums import OrderSide, OrderType as CDOrderType
from cyberdelta.symbols.models import Symbol

class BackpackExchange(ExchangePyBase):
    """
    Thin wrapper around CyberDelta's BackpackAPI for Hummingbot compatibility.
    """

    def __init__(
        self,
        client_config_map,
        api_key: str,
        api_secret: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = "backpack",
    ):
        super().__init__(client_config_map)

        # Initialize CyberDelta API
        self._init_cyberdelta_api(api_key, api_secret)

        # Hummingbot specific
        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required
        self._domain = domain

        # Mappings
        self._client_to_exchange_order_id: Dict[str, str] = {}
        self._exchange_to_client_order_id: Dict[str, str] = {}

    def _init_cyberdelta_api(self, api_key: str, api_secret: str):
        """Initialize CyberDelta BackpackAPI with minimal config."""
        # Create minimal config for CyberDelta
        exchange_config = ExchangeSpecificConfig(
            name="backpack",
            api_base_url="https://api.backpack.exchange",
            ws_url="wss://ws.backpack.exchange",
            rate_limit_per_minute=1200,
        )

        secrets = BackpackSecrets(
            api_key=api_key,
            api_secret=api_secret,
        )

        # Initialize CyberDelta API
        self._cd_api = BackpackAPI(
            exchange_config=exchange_config,
            exchange_secrets=secrets,
        )

        # Store authenticator for Hummingbot auth wrapper
        self._cd_auth = self._cd_api.authenticator
```

### 2. Type Conversion Layer

```python
# backpack_type_converter.py
from decimal import Decimal
from typing import Optional

from hummingbot.core.data_type.common import OrderType, TradeType
from cyberdelta.enums import OrderSide, OrderType as CDOrderType

class BackpackTypeConverter:
    """Converts between Hummingbot and CyberDelta types."""

    @staticmethod
    def hb_to_cd_order_type(order_type: OrderType) -> CDOrderType:
        """Convert Hummingbot OrderType to CyberDelta OrderType."""
        mapping = {
            OrderType.LIMIT: CDOrderType.LIMIT,
            OrderType.MARKET: CDOrderType.MARKET,
            OrderType.LIMIT_MAKER: CDOrderType.POST_ONLY,
        }
        return mapping.get(order_type, CDOrderType.LIMIT)

    @staticmethod
    def hb_to_cd_order_side(is_buy: bool) -> OrderSide:
        """Convert Hummingbot buy/sell to CyberDelta OrderSide."""
        return OrderSide.BUY if is_buy else OrderSide.SELL

    @staticmethod
    def cd_to_hb_order_type(order_type: CDOrderType) -> OrderType:
        """Convert CyberDelta OrderType to Hummingbot OrderType."""
        mapping = {
            CDOrderType.LIMIT: OrderType.LIMIT,
            CDOrderType.MARKET: OrderType.MARKET,
            CDOrderType.POST_ONLY: OrderType.LIMIT_MAKER,
        }
        return mapping.get(order_type, OrderType.LIMIT)

    @staticmethod
    def hb_trading_pair_to_cd_symbol(trading_pair: str) -> Symbol:
        """Convert Hummingbot trading pair to CyberDelta Symbol."""
        # "BTC-USDC" -> Symbol("BTC", "USDC")
        base, quote = trading_pair.split("-")
        return Symbol(base=base, quote=quote)

    @staticmethod
    def cd_symbol_to_hb_trading_pair(symbol: Symbol) -> str:
        """Convert CyberDelta Symbol to Hummingbot trading pair."""
        return f"{symbol.base}-{symbol.quote}"
```

### 3. Order Operations Wrapper

```python
# In backpack_exchange.py
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
    """
    Place order using CyberDelta API and return exchange order ID.
    """
    try:
        # Convert types
        symbol = BackpackTypeConverter.hb_trading_pair_to_cd_symbol(trading_pair)
        cd_order_type = BackpackTypeConverter.hb_to_cd_order_type(order_type)
        side = BackpackTypeConverter.hb_to_cd_order_side(is_buy)

        # Call CyberDelta API
        cd_order = await self._cd_api.place_order(
            PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=cd_order_type,
                quantity=amount,
                price=price,
                client_order_id=order_id,  # Pass through client ID
            )
        )

        # Store mapping
        exchange_order_id = cd_order.order_id
        self._client_to_exchange_order_id[order_id] = exchange_order_id
        self._exchange_to_client_order_id[exchange_order_id] = order_id

        # Track order
        self._order_tracker.start_tracking_order(
            InFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.BUY if is_buy else TradeType.SELL,
                price=price,
                amount=amount,
                creation_timestamp=self.current_timestamp,
            )
        )

        # Emit event
        event_type = MarketEvent.BuyOrderCreated if is_buy else MarketEvent.SellOrderCreated
        event_class = BuyOrderCreatedEvent if is_buy else SellOrderCreatedEvent
        self.trigger_event(
            event_type,
            event_class(
                self.current_timestamp,
                order_type,
                trading_pair,
                amount,
                price,
                order_id,
                self.current_timestamp
            )
        )

        return exchange_order_id

    except Exception as e:
        self.logger().error(f"Failed to place order: {e}")

        # Emit failure event
        self.trigger_event(
            MarketEvent.OrderFailure,
            MarketOrderFailureEvent(
                self.current_timestamp,
                order_id,
                order_type
            )
        )

        # Re-raise for Hummingbot to handle
        raise

async def _cancel(self, trading_pair: str, order_id: str) -> bool:
    """
    Cancel order using CyberDelta API.
    """
    try:
        # Get exchange order ID
        exchange_order_id = self._client_to_exchange_order_id.get(order_id)
        if not exchange_order_id:
            self.logger().warning(f"No exchange order ID for {order_id}")
            return False

        # Convert symbol
        symbol = BackpackTypeConverter.hb_trading_pair_to_cd_symbol(trading_pair)

        # Call CyberDelta API
        result = await self._cd_api.cancel_order(
            CancelOrderArgs(
                symbol=symbol,
                order_id=exchange_order_id,
            )
        )

        if result.success:
            # Update order state
            tracked_order = self._order_tracker.fetch_tracked_order(order_id)
            if tracked_order:
                tracked_order.current_state = OrderState.PENDING_CANCEL

            # Emit event
            self.trigger_event(
                MarketEvent.OrderCancelled,
                OrderCancelledEvent(
                    self.current_timestamp,
                    order_id
                )
            )

            return True

        return False

    except Exception as e:
        self.logger().error(f"Failed to cancel order {order_id}: {e}")
        return False
```

### 4. Balance Updates Wrapper

```python
async def _update_balances(self):
    """Update balances using CyberDelta API."""
    try:
        # Get account summary from CyberDelta
        account_summary = await self._cd_api.get_account_summary()

        # Update Hummingbot balance tracking
        self._account_balances.clear()
        self._account_available_balances.clear()

        # Process spot balances
        for balance in account_summary.spot_balances:
            asset = balance.currency
            total = balance.total
            available = balance.available

            self._account_balances[asset] = total
            self._account_available_balances[asset] = available

        # Log balance update
        self.logger().info(f"Updated balances: {self._account_balances}")

    except Exception as e:
        self.logger().error(f"Failed to update balances: {e}")
```

### 5. WebSocket Wrapper

```python
# backpack_api_user_stream_data_source.py
import asyncio
from typing import Optional, Any

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from cyberdelta.apis.backpack import BackpackAPI

class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Wrapper around CyberDelta WebSocket for Hummingbot user stream.
    """

    def __init__(self, cd_api: BackpackAPI, connector):
        super().__init__()
        self._cd_api = cd_api
        self._connector = connector

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listen to CyberDelta WebSocket and convert messages for Hummingbot.
        """
        while True:
            try:
                # Subscribe to CyberDelta WebSocket channels
                await self._cd_api.subscribe_to_account_updates(
                    on_order_update=lambda msg: self._handle_order_update(msg, output),
                    on_balance_update=lambda msg: self._handle_balance_update(msg, output),
                    on_fill=lambda msg: self._handle_fill(msg, output),
                )

                # Keep connection alive
                await asyncio.sleep(float("inf"))

            except Exception as e:
                self.logger().error(f"WebSocket error: {e}")
                await asyncio.sleep(5)  # Reconnect delay

    def _handle_order_update(self, cd_msg: Any, output: asyncio.Queue):
        """Convert CyberDelta order update to Hummingbot format."""
        hb_msg = {
            "type": "order_update",
            "order_id": cd_msg.client_order_id,
            "exchange_order_id": cd_msg.order_id,
            "status": self._convert_order_status(cd_msg.status),
            "filled_amount": str(cd_msg.filled_quantity),
            "remaining_amount": str(cd_msg.remaining_quantity),
            "timestamp": cd_msg.timestamp,
        }
        output.put_nowait(hb_msg)

    def _handle_balance_update(self, cd_msg: Any, output: asyncio.Queue):
        """Convert CyberDelta balance update to Hummingbot format."""
        hb_msg = {
            "type": "balance_update",
            "asset": cd_msg.currency,
            "total": str(cd_msg.total),
            "available": str(cd_msg.available),
            "timestamp": cd_msg.timestamp,
        }
        output.put_nowait(hb_msg)

    def _handle_fill(self, cd_msg: Any, output: asyncio.Queue):
        """Convert CyberDelta fill to Hummingbot format."""
        hb_msg = {
            "type": "fill",
            "order_id": cd_msg.client_order_id,
            "trade_id": cd_msg.trade_id,
            "price": str(cd_msg.price),
            "amount": str(cd_msg.quantity),
            "fee": str(cd_msg.fee),
            "fee_asset": cd_msg.fee_currency,
            "timestamp": cd_msg.timestamp,
        }
        output.put_nowait(hb_msg)
```

### 6. Authentication Wrapper

```python
# backpack_auth.py
from typing import Dict, Any, Optional

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator

class BackpackAuth(AuthBase):
    """
    Wrapper around CyberDelta's BackpackEd25519Authenticator for Hummingbot.
    """

    def __init__(self, cd_auth: BackpackEd25519Authenticator):
        self._cd_auth = cd_auth

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Apply authentication to REST request using CyberDelta auth.
        """
        # Get auth headers from CyberDelta
        auth_headers = self._cd_auth.get_auth_headers(
            method=request.method.value,
            url=request.url,
            body=request.data,
        )

        # Apply to request
        if request.headers is None:
            request.headers = {}
        request.headers.update(auth_headers)

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Apply authentication to WebSocket request.
        """
        # Get WebSocket auth payload from CyberDelta
        auth_payload = self._cd_auth.get_ws_auth_payload()

        # Add to request
        if request.payload is None:
            request.payload = {}
        request.payload.update(auth_payload)

        return request
```

## Parts That Can Be Wrapped

### ✅ Fully Wrappable
1. **Authentication** - Direct wrapper around CyberDelta auth
2. **Order Placement** - Simple type conversion + API call
3. **Order Cancellation** - Simple type conversion + API call
4. **Balance Queries** - Direct mapping of response
5. **Trading Rules** - Parse CyberDelta market info
6. **Error Messages** - Map CyberDelta APIError

### ⚠️ Partially Wrappable
1. **WebSocket Handling** - Need message format conversion
2. **Order Status Updates** - Need state mapping
3. **Rate Limiting** - Different strategies need bridging
4. **Decimal/Float** - Need conversion at boundaries

## Parts That Need Reimplementation

### ❌ Must Reimplement
1. **Order Book Tracking** - Hummingbot specific format
2. **Event Generation** - Hummingbot event system
3. **In-Flight Order Tracking** - Hummingbot state machine
4. **Client Order ID Management** - Hummingbot prefix system

### Reimplementation Strategy

```python
# Order Book Data Source - Must follow Hummingbot format
class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Cannot wrap CyberDelta's order book - must implement Hummingbot format.
    """

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """Get order book snapshot from CyberDelta and convert."""
        # Get from CyberDelta
        cd_orderbook = await self._cd_api.get_order_book(
            symbol=BackpackTypeConverter.hb_trading_pair_to_cd_symbol(trading_pair)
        )

        # Convert to Hummingbot OrderBook
        order_book = OrderBook()

        # Add bids
        for bid in cd_orderbook.bids:
            order_book.apply_trade(
                TradeType.BUY,
                Decimal(str(bid.price)),
                Decimal(str(bid.quantity)),
                update_id=cd_orderbook.sequence
            )

        # Add asks
        for ask in cd_orderbook.asks:
            order_book.apply_trade(
                TradeType.SELL,
                Decimal(str(ask.price)),
                Decimal(str(ask.quantity)),
                update_id=cd_orderbook.sequence
            )

        return order_book
```

## Code Style Adaptations

### Python Version Differences

```python
# CyberDelta (Python 3.13)
from typing import override  # Not available in 3.10

# Hummingbot (Python 3.10)
# Must use older patterns

# CyberDelta
match error_code:
    case "INSUFFICIENT_BALANCE":
        return InsufficientBalance()
    case _:
        return UnknownError()

# Hummingbot - must use if/elif
if error_code == "INSUFFICIENT_BALANCE":
    return InsufficientBalance()
else:
    return UnknownError()
```

### Type Hints
```python
# CyberDelta - uses modern type hints
def process(data: list[dict[str, Any]]) -> None: ...

# Hummingbot - must use typing module
from typing import List, Dict, Any
def process(data: List[Dict[str, Any]]) -> None: ...
```

## Benefits of Wrapper Approach

1. **Leverage Existing Code** - 80% of logic already implemented
2. **Maintain Quality** - CyberDelta's robust error handling preserved
3. **Quick Implementation** - Days instead of weeks
4. **Easy Updates** - Update CyberDelta API independently
5. **Testing** - Can use CyberDelta's test infrastructure

## Risks and Mitigations

### Risk: Type Conversion Errors
**Mitigation**: Comprehensive unit tests for all conversions

### Risk: Event Timing Issues
**Mitigation**: Careful state management and logging

### Risk: WebSocket Message Format Mismatch
**Mitigation**: Message validation and conversion layer

### Risk: Python Version Incompatibility
**Mitigation**: Test in Python 3.10 environment early

## Conclusion

The wrapper strategy is optimal for creating a Hummingbot-compatible Backpack connector while maintaining CyberDelta's code quality. Key success factors:

1. Thin wrapper where possible
2. Reimplement only Hummingbot-specific patterns
3. Comprehensive type conversion
4. Robust error mapping
5. Maintain CyberDelta's architectural advantages

This approach delivers a working connector in minimal time while preserving the investment in CyberDelta's robust implementation.
