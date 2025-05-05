import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Callable, Coroutine, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

# Added import for ValidationError
from pydantic import ValidationError

# Import Fill type
from cyberdelta.apis.base_api import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.market.candle import Candle

# Correct the import to use the new typing module
# REMOVED INCORRECT IMPORT: from cyberdelta.core.symbol_mapper import Symbol
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

# Type alias for WebSocket message handlers from base.py
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


# Define a custom exception for mock API errors
class MockAPIError(Exception):
    pass


class MockExchangeAPI(ExchangeAPI):
    """
    Mock implementation of the ExchangeAPI for integration testing.
    Simulates basic exchange behavior, including order management, data fetching,
    and WebSocket interactions. Allows simulating errors and latency.
    """

    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any],
        secrets: dict[str, str | None],
        config_obj: Config | None = None,
    ) -> None:
        super().__init__(exchange_name, config, secrets)
        self.full_config = config_obj  # Store the full config object if provided
        self._order_id_counter = 1
        self._orders: dict[str, Order] = {}  # Store orders by ID
        self._positions: dict[str, DerivativePosition] = {}  # Store positions by symbol
        self._balances: dict[str, SpotBalance] = {}  # Store balances by asset
        self._mock_tickers: dict[str, Ticker] = {}
        self._mock_funding_rates: dict[str, FundingRate] = {}
        self._trades: list[Trade] = []  # Added to store trades

        # --- NEW: Fee attributes initialized from config as Decimal ---
        default_fee = "0.001"  # Default fee rate as string for Decimal
        default_asset = "USD"  # Default fee asset if not in config
        if self.full_config:
            # Convert fee rates fetched from config (potentially float) to Decimal via string
            self.maker_fee = Decimal(
                str(self.full_config.get(f"exchanges.{exchange_name}.maker_fee", default_fee))
            )
            self.taker_fee = Decimal(
                str(self.full_config.get(f"exchanges.{exchange_name}.taker_fee", default_fee))
            )
            # Use 'collateral_asset' as the primary indicator for fee asset
            self.fee_asset = self.full_config.get(
                f"exchanges.{exchange_name}.collateral_asset", default_asset
            )
        else:
            # Fallback if full_config not provided (less ideal)
            self.maker_fee = Decimal(str(config.get("maker_fee", default_fee)))
            self.taker_fee = Decimal(str(config.get("taker_fee", default_fee)))
            self.fee_asset = config.get("collateral_asset", default_asset)
        # ---------------------------------------------------

        # Simulation parameters
        self._latency_ms: float = 10.0  # Default latency in milliseconds
        self._error_simulation: dict[str, Any] | None = None  # Config to simulate errors
        self._fail_on_method: str | None = None  # Method name to fail on
        self._failure_exception: Exception = MockAPIError(
            "Simulated API failure"
        )  # Exception to raise
        self._open_orders_behavior: str = (
            "default"  # Options: default, fill_immediately, partial_fill
        )

        # Initialize balances and positions
        self.balances: dict[str, SpotBalance] = {}
        self.positions: dict[str, DerivativePosition] = {}
        self.open_orders: dict[str, Order] = {}
        self.trade_history: list[Trade] = []
        self.api_errors: list[dict[str, Any]] = []

        # Error simulation
        self._error_config: dict[str, tuple[Exception, int | None]] = {}
        self._call_counts: dict[str, int] = defaultdict(int)

        # Behavior settings
        self._open_orders_behavior = "keep_open"

        logger.info(
            f"Initialized MockExchangeAPI for {exchange_name} (Maker Fee: {self.maker_fee}, "
            f"Taker Fee: {self.taker_fee}, Fee Asset: {self.fee_asset})"
        )

    # --- Internal Helper: Create Mock Order ---
    def _create_internal_mock_order(
        self,
        client_order_id: str,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        status: OrderStatus,
        qty_req: Decimal,
        qty_fill: Decimal,
        avg_price: Decimal | None,
        price: Decimal | None,
        time_in_force: TimeInForce,
        ts: datetime,
        strategy: str | None = "mock_strategy",
        signal: str | None = "mock_signal",
    ) -> Order:
        # Helper to create Order instances with all required fields
        # Use model_validate to handle potential extra fields gracefully if needed
        order_data = {
            "client_order_id": client_order_id,
            "exchange": self.exchange_name,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "status": status,
            "quantity_requested": qty_req,
            "quantity_filled": qty_fill,
            "price": price,
            "average_fill_price": avg_price,
            "time_in_force": time_in_force,
            "created_at": ts,
            "updated_at": ts,  # Sensible default
            "triggered_at": None,  # Sensible default
            "strategy_name": strategy,
            "signal_id": signal,
            # Add defaults for other optional base fields if needed
            "exchange_order_id": f"mock-ex-{uuid.uuid4()}",  # Mock exchange ID
            "trades": [],
        }
        # Refine type hint if specific structure is known, otherwise Any is acceptable for internal helper
        # order_data: dict[str, Any] = { ... } # Example if refining
        return Order.model_validate(order_data)

    async def _simulate_latency(self) -> None:
        """Simulate network latency."""
        if self._latency_ms > 0:
            await asyncio.sleep(self._latency_ms / 1000.0)

    def configure_error(
        self,
        method_name: str,
        error_code: APIErrorCode,
        message: str,
        trigger_after_n_calls: int | None = 0,
    ) -> None:
        """Configure an APIError to be raised by a specific method."""
        # DEFENSIVE CHECK: Convert enum to str for APIError
        error = APIError(
            message=message, code=str(error_code.value), exchange_code=self.exchange_name
        )
        self._error_config[method_name] = (error, trigger_after_n_calls)
        self._call_counts[method_name] = 0  # Reset count when configuring

    def set_error_simulation(
        self,
        error: Exception,
        method_name: str,
        trigger_after_n_calls: int | None = 0,
    ) -> None:
        """Configure a specific exception to be raised by a method."""
        self._error_config[method_name] = (error, trigger_after_n_calls)
        self._call_counts[method_name] = 0

    def clear_error(self, method_name: str | None = None) -> None:
        """Clear error simulation for a specific method or all methods."""
        if method_name:
            if method_name in self._error_config:
                del self._error_config[method_name]
            if method_name in self._call_counts:
                del self._call_counts[method_name]
        else:
            self._error_config.clear()
            self._call_counts.clear()
        logger.info(
            f"MockExchange {self.exchange_name} error simulation cleared{f' for {method_name}' if method_name else ''}"
        )

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """Helper to split a symbol like 'BTC-USDC' into base and quote."""
        parts = symbol.split("-")
        if len(parts) == 2:
            return parts[0], parts[1]
        # Handle cases like BTC/USDT or simple BTC if needed
        parts = symbol.split("/")
        if len(parts) == 2:
            return parts[0], parts[1]
        # Default assumption if no separator
        logger.warning(
            f"Could not determine base/quote for symbol '{symbol}', assuming '{symbol}' and 'USD'"
        )
        return symbol, "USD"

    def _check_error(self, method_name: str) -> None:
        """Check if an error should be raised for the method call."""
        if method_name in self._error_config:
            error, trigger_after = self._error_config[method_name]
            current_count = self._call_counts[method_name]
            self._call_counts[method_name] += 1
            if trigger_after is None or current_count >= trigger_after:
                logger.warning(
                    f"MockExchange {self.exchange_name} raising simulated error for {method_name}: {error}"
                )
                raise error

    def configure_failure(
        self, method_name: str | None = None, exception: Exception | None = None
    ) -> None:
        """Configure the mock to fail on a specific method call."""
        self._fail_on_method = method_name
        if exception:
            self._failure_exception = exception
        else:
            self._failure_exception = MockAPIError(f"Simulated API failure on {method_name}")

    def reset_failure(self) -> None:
        """Reset the failure configuration."""
        self._fail_on_method = None

    # --- Implement abstract methods ---

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mock authentication - returns empty dict."""
        self._check_error("_authenticate")
        await self._simulate_latency()
        return {}

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Mock WebSocket message routing."""
        self._check_error("_route_ws_message")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Received WS message (not routed): {message}")

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Mock WebSocket subscription."""
        self._check_error("subscribe")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to topic '{topic}' (simulated).")

    async def _resubscribe(self) -> None:
        """Mock WebSocket resubscription."""
        self._check_error("_resubscribe")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Resubscribed to topics (simulated).")

    async def get_ticker(self, symbol: str) -> Ticker:
        """Return mock ticker data or raise KeyError if not found."""
        self._check_error("get_ticker")
        await self._simulate_latency()
        ticker = self._mock_tickers.get(symbol)
        if ticker is None:
            raise KeyError(f"Mock ticker not found for symbol: {symbol}")
        return ticker

    # Corrected override signature and implementation
    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Return mock order book data."""
        self._check_error("get_order_book")
        await self._simulate_latency()
        # Return a basic OrderBook structure, ensuring timestamp is datetime
        # DEFENSIVE CHECK: Convert int timestamp to datetime
        now_ts_ms = int(datetime.now(UTC).timestamp() * 1000)
        return OrderBook(
            symbol=symbol,
            bids=[],
            asks=[],
            timestamp=datetime.fromtimestamp(now_ts_ms / 1000, tz=UTC),
        )

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Return mock recent trades."""
        self._check_error("get_recent_trades")
        await self._simulate_latency()
        # Return empty list for simplicity
        return []

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Return mock funding rate."""
        self._check_error("get_funding_rate")
        await self._simulate_latency()
        return self._mock_funding_rates.get(symbol)

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Return mock balances."""
        self._check_error("get_balances")
        await self._simulate_latency()
        return self._balances.copy()

    # Corrected override signature
    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Return mock positions."""
        self._check_error("get_positions")
        await self._simulate_latency()
        if symbol:
            pos = self._positions.get(symbol)
            return [pos] if pos else []
        return list(self._positions.values())

    # --- Order Management ---

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """Simulate placing an order."""
        self._check_error("place_order")
        await self._simulate_latency()

        if order_type == OrderType.LIMIT and price is None:
            raise APIError(
                "Price must be specified for LIMIT orders", code=APIErrorCode.INVALID_PARAMS.value
            )
        if order_type == OrderType.MARKET and price is not None:
            logger.warning("Price is ignored for MARKET orders")

        order_id: str = str(client_order_id) if client_order_id else str(uuid.uuid4())
        self._order_id_counter += 1
        now = datetime.now(UTC)

        # Simulate order status based on behavior config
        order_status = OrderStatus.NEW  # Default
        qty_filled = Decimal("0.0")
        avg_fill_price = None

        # Basic balance check (improve this based on actual needs)
        base_asset, quote_asset = self._split_symbol(symbol)
        required_balance = Decimal("0")  # Initialize
        asset_to_check = ""

        if side == OrderSide.BUY:
            asset_to_check = quote_asset
            # Approximate quote needed (can be refined)
            required_balance = quantity * (
                price
                if price
                else self._mock_tickers.get(
                    symbol, Ticker(symbol=symbol, price=Decimal("0"), timestamp=now)
                ).price
                or Decimal("0")
            )
        else:  # SELL
            asset_to_check = base_asset
            required_balance = quantity

        current_balance = self._balances.get(
            asset_to_check,
            SpotBalance(
                asset=asset_to_check,
                total_quantity=Decimal("0"),
                available_quantity=Decimal("0"),
                exchange=self.exchange_name,
                timestamp=now,  # Add missing timestamp
            ),
        ).available_quantity

        if current_balance < required_balance:
            logger.warning(f"Mock {self.exchange_name}: Insufficient balance for order {order_id}")
            # Raise error or return rejected order
            raise APIError("Insufficient balance", code=APIErrorCode.INSUFFICIENT_FUNDS.value)
            # Or create a REJECTED order:
            # order = self._create_internal_mock_order(... status=OrderStatus.REJECTED ...)
            # self._orders[order_id] = order
            # return order

        if self._open_orders_behavior == "fill_immediately":
            order_status = OrderStatus.FILLED
            qty_filled = quantity
            # Use provided price for LIMIT, or mock ticker price for MARKET
            avg_fill_price = (
                price
                if order_type == OrderType.LIMIT
                else (
                    self._mock_tickers.get(
                        symbol, Ticker(symbol=symbol, price=Decimal("0"), timestamp=now)
                    ).price
                    or Decimal("0")
                )
            )
        elif self._open_orders_behavior == "partial_fill":
            order_status = OrderStatus.PARTIALLY_FILLED
            qty_filled = quantity / 2  # Example partial fill
            avg_fill_price = (
                price
                if order_type == OrderType.LIMIT
                else (
                    self._mock_tickers.get(
                        symbol, Ticker(symbol=symbol, price=Decimal("0"), timestamp=now)
                    ).price
                    or Decimal("0")
                )
            )
        else:  # default or keep_open
            order_status = OrderStatus.OPEN  # Or NEW?

        # Create the order using the helper method
        order = self._create_internal_mock_order(
            client_order_id=order_id,  # Use the generated/provided ID
            symbol=symbol,
            side=side,
            order_type=order_type,
            status=order_status,
            qty_req=quantity,
            qty_fill=qty_filled,
            avg_price=avg_fill_price,
            price=price,
            time_in_force=time_in_force,
            ts=now,
            strategy="mock_strategy",  # Example
            signal="mock_signal",  # Example
            # Pass reduce_only, post_only if needed by helper or add here
        )
        # Add reduce_only and post_only after creation if not in helper
        order.reduce_only = reduce_only
        order.post_only = post_only

        # Store the order
        self._orders[order.client_order_id] = order
        if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            self.open_orders[order.client_order_id] = order

        # Simulate fills/trades if filled
        if order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
            trade_fee_rate = self.taker_fee  # Assume taker for market/aggressive limit
            if order_type == OrderType.LIMIT and post_only:
                trade_fee_rate = self.maker_fee

            # Use avg_fill_price if available, otherwise fallback
            fill_price_for_trade = avg_fill_price if avg_fill_price is not None else order.price
            if fill_price_for_trade is None:
                logger.warning(
                    f"Cannot determine fill price for trade simulation for order {order.client_order_id}. Using 0."
                )
                fill_price_for_trade = Decimal("0")

            trade_cost = qty_filled * fill_price_for_trade
            trade_fee = trade_cost * trade_fee_rate

            trade = Trade(
                id=f"mock_trade_{order.client_order_id}",
                order_id=order.client_order_id,
                exchange=self.exchange_name,
                symbol=symbol,
                price=fill_price_for_trade,
                quantity=qty_filled,
                side=side,
                fee=trade_fee,
                fee_asset=self.fee_asset,
                timestamp=now,
                is_maker=(trade_fee_rate == self.maker_fee),
                executed_at=now,  # Add missing executed_at
                # Add optional detail slots if needed
            )
            self.trade_history.append(trade)
            self._trades.append(trade)  # Also add to internal list if used elsewhere
            # Update timestamp on order when trade occurs
            order.updated_at = now
            # Update balances and positions based on the trade
            self._update_balance_and_position(trade)

        logger.debug(
            f"Mock {self.exchange_name}: Placed order {order.client_order_id}: {order.status}"
        )
        return order

    async def cancel_order(
        self,
        order_id: str,
        symbol: str | None = None,  # client_order_id: str | None = None <-- Removed unused param
    ) -> dict[str, Any]:
        """
        Cancel an existing order by its ID or client_order_id.

        Args:
            order_id: The exchange order ID or client order ID to cancel.
            symbol: The symbol (unused in this mock, but kept for signature).
            # Removed client_order_id parameter

        Returns:
            A dictionary indicating success or failure.
        """
        logger.debug(f"Mock {self.exchange_name}: Attempting to cancel order: ID={order_id}")
        self._check_error("cancel_order")
        await self._simulate_latency()

        # Find the order by either exchange_order_id or client_order_id
        order_to_cancel: Order | None = None
        order_key_found: str | None = None

        # Try finding by client_order_id first if it matches format, then exchange_order_id
        # This assumes client_order_ids are distinct enough or exchange_order_ids have a prefix
        if (
            order_id in self._orders
        ):  # Treat order_id as the primary key (could be client or exchange ID)
            order_to_cancel = self._orders[order_id]
            order_key_found = order_id
        else:
            # Search by client_order_id if not found as primary key
            for key, o in self._orders.items():
                if o.client_order_id == order_id:
                    order_to_cancel = o
                    order_key_found = key  # Store the key used to find it
                    break
            # If still not found, try by exchange_order_id field (less likely)
            if order_to_cancel is None:
                for key, o in self._orders.items():
                    if o.exchange_order_id == order_id:
                        order_to_cancel = o
                        order_key_found = key
                        break

        if order_to_cancel and order_key_found:
            # Replace is_terminal() check
            if order_to_cancel.status in [
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            ]:
                logger.warning(
                    f"Mock {self.exchange_name}: Order {order_key_found} is already in terminal state: {order_to_cancel.status.name}"
                )
                return {"success": False, "message": "Order already terminated"}

            # Use correct Enum member access
            order_to_cancel.status = OrderStatus.CANCELLED
            order_to_cancel.updated_at = datetime.now(UTC)
            logger.info(f"Mock {self.exchange_name}: Marked order {order_key_found} as CANCELLED.")
            # Optionally remove from _orders if needed for test logic
            # del self._orders[order_key_found]
            return {"success": True}
        else:
            logger.warning(f"Mock {self.exchange_name}: Order with ID '{order_id}' not found.")
            # Simulate exchange error for not found
            raise APIError(
                message=f"Order not found: {order_id}",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
                exchange_code=self.exchange_name,
            )

    async def get_order(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Get order details by exchange ID or client ID."""
        logger.debug(
            f"Mock {self.exchange_name}: Getting order: ID={order_id}, ClientID={client_order_id}"
        )
        self._check_error("get_order")
        await self._simulate_latency()

        # Prioritize finding by exchange order ID (assuming order_id param is exchange ID)
        order = self._orders.get(order_id)

        # If not found by exchange ID and client_order_id is provided, try that
        if not order and client_order_id:
            target_client_order_id = str(client_order_id)
            for o in self._orders.values():
                if o.client_order_id == target_client_order_id:
                    order = o
                    break

        # Optionally check symbol match if provided
        if order and symbol and order.symbol != symbol:
            logger.warning(
                f"Order found by ID ({order_id} or {client_order_id}) but symbol mismatch: requested '{symbol}', found '{order.symbol}'"
            )
            return None  # Or raise an error, depending on desired mock behavior

        if not order:
            logger.warning(
                f"Mock {self.exchange_name}: Order not found for ID={order_id}, ClientID={client_order_id}"
            )
            # Simulate exchange error for not found
            # raise APIError(message=f"Order not found: {order_id}/{client_order_id}", code=APIErrorCode.ORDER_NOT_FOUND, exchange_code=self.exchange_name)
            return None  # Return None if not found

        logger.debug(f"Mock {self.exchange_name}: Found order: {order}")
        return order

    async def get_order_status(
        self,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """
        Get a specific order by ID or raise KeyError if not found (params ignored).
        """
        # Mark params as unused if necessary for linters
        _ = symbol
        _ = client_order_id
        self._check_error("get_order_status")
        await self._simulate_latency()
        order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"Mock order not found for order_id: {order_id}")
        # Note: Base class expects Order, not Order | None. Mock now raises if not found.
        return order

    # Corrected override signature
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error("get_open_orders")
        await self._simulate_latency()
        try:
            orders_to_return: list[Order] = []
            for order in self._orders.values():
                # Filter by symbol if provided
                order_symbol: str | None = getattr(order, "symbol", None)
                if symbol is None or order_symbol == symbol:
                    orders_to_return.append(order)
            return orders_to_return
        except Exception as e:
            logger.exception(f"Error in mock get_open_orders: {e}")
            return []

    # --- Mock Data Setup ---

    # --- ADD MISSING ABSTRACT METHOD IMPLEMENTATION ---
    async def get_recent_fills(
        self,
        symbol: str | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        start_time: int | None = None,
        # Ensure Fill type is available from TYPE_CHECKING import in base class or models
        # from ..core.models import Fill # <-- Might need explicit import if not via TYPE_CHECKING
    ) -> list["Trade"]:  # Use forward reference for Trade (was Fill)
        """Return mock recent fills (empty list for basic mock)."""
        self._check_error("get_recent_fills")
        await self._simulate_latency()
        # TODO: Optionally implement filtering based on args if needed for specific tests
        # For now, return empty list. Needs 'Fill' type defined/imported.
        return []

    # ------------------------------------------------

    # --- ADD MISSING ABSTRACT METHOD IMPLEMENTATION ---
    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle incoming websocket messages (mock implementation)."""
        self._check_error("_handle_websocket_message")
        await self._simulate_latency()
        # In a real mock, you might parse and route based on message type
        logger.debug(f"Mock {self.exchange_name} received WS message: {message}")
        # Placeholder: Does nothing with the message
        pass

    # ------------------------------------------------

    def set_mock_ticker(self, ticker: Ticker) -> None:
        """Set a predefined ticker for a symbol."""
        self._check_error("set_mock_ticker")
        self._mock_tickers[ticker.symbol] = ticker
        logger.debug(f"Mock {self.exchange_name}: Set mock ticker for {ticker.symbol}")

    def set_mock_funding_rate(self, funding_rate: FundingRate) -> None:
        """Set a predefined funding rate for a symbol."""
        self._check_error("set_mock_funding_rate")
        self._mock_funding_rates[funding_rate.symbol] = funding_rate
        logger.debug(f"Mock {self.exchange_name}: Set mock funding rate for {funding_rate.symbol}")

    def set_mock_balance(self, balance_data: SpotBalance | dict[str, Any]) -> None:
        """
        Set or update a mock balance for a specific asset.
        Accepts either a SpotBalance object or a dictionary representation.
        """
        asset = None
        if isinstance(balance_data, SpotBalance):
            asset = balance_data.asset
            # Ensure the mock knows the exchange name
            if not balance_data.exchange:
                balance_data = balance_data.model_copy(update={"exchange": self.exchange_name})
            self._balances[asset] = balance_data
            # Check if balances attribute exists and is a dictionary
            # Ensure asset is a string before using as key
            if isinstance(asset, str):
                self.balances[asset] = balance_data  # Keep public attribute consistent
            else:
                logger.error(f"Invalid asset type '{type(asset)}' for balance update.")
        elif isinstance(balance_data, dict):
            asset = balance_data.get("asset")
            if asset:
                # Ensure exchange field is present
                if "exchange" not in balance_data:
                    balance_data["exchange"] = self.exchange_name
                # Add timestamp if missing
                if "timestamp" not in balance_data:
                    balance_data["timestamp"] = datetime.now(UTC)
                try:
                    balance_obj = SpotBalance(**balance_data)
                    self._balances[asset] = balance_obj
                    # Check if balances attribute exists and is a dictionary
                    # Ensure asset is a string before using as key
                    if isinstance(asset, str):
                        self.balances[asset] = balance_obj  # Keep public attribute consistent
                    else:
                        logger.error(f"Invalid asset type '{type(asset)}' for balance update.")
                except Exception as e:
                    logger.error(
                        f"Failed to create SpotBalance from dict: {e}, data={balance_data}"
                    )
            else:
                logger.error("Dictionary provided to set_mock_balance must contain 'asset' key.")
        else:
            logger.error("Invalid data type provided to set_mock_balance.")

        if asset:
            logger.info(f"Mock balance set for {asset} on {self.exchange_name}")

    def set_mock_position(self, position: DerivativePosition) -> None:
        """Set a predefined position for a symbol."""
        self._check_error("set_mock_position")
        self._positions[position.symbol] = position
        logger.debug(f"Mock {self.exchange_name}: Set mock position for {position.symbol}")

    def set_open_orders_behavior(self, behavior: str) -> None:
        """Set how open orders are handled (keep_open, fill_immediately, partial_fill)."""
        self._check_error("set_open_orders_behavior")
        allowed_behaviors = ["keep_open", "fill_immediately", "partial_fill"]
        if behavior not in allowed_behaviors:
            raise ValueError(f"Invalid behavior. Must be one of: {allowed_behaviors}")
        self._open_orders_behavior = behavior
        logger.info(f"Mock {self.exchange_name}: Set open orders behavior to '{behavior}'")

    # --- Utility/Reset ---

    def reset(self) -> None:
        """Reset the mock exchange state."""
        self._check_error("reset")
        self._order_id_counter = 1
        self._orders.clear()
        self._positions.clear()
        self._balances.clear()
        self._mock_tickers.clear()
        self._mock_funding_rates.clear()
        self._trades.clear()
        self.clear_error()  # Clear error simulation config
        self.reset_failure()  # Clear failure simulation config
        logger.info(f"Mock {self.exchange_name}: State reset.")

    # --- Additional Fetch Methods (if needed by tests) ---

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Fetch ticker (simulated)."""
        self._check_error("fetch_ticker")
        await self._simulate_latency()
        ticker = self._mock_tickers.get(symbol)
        if not ticker:
            raise APIError(
                f"Ticker for {symbol} not found", code=APIErrorCode.SYMBOL_NOT_FOUND.value
            )
        return ticker

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """Fetch funding rate (simulated)."""
        self._check_error("fetch_funding_rate")
        await self._simulate_latency()
        rate = self._mock_funding_rates.get(symbol)
        if not rate:
            raise APIError(
                f"Funding rate for {symbol} not found",
                code=APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
            )
        return rate

    async def fetch_balances(self) -> dict[str, SpotBalance]:
        """Fetch balances (simulated)."""
        self._check_error("fetch_balances")
        await self._simulate_latency()
        return self._balances.copy()

    async def fetch_positions(self) -> dict[str, DerivativePosition]:
        """Fetch positions (simulated)."""
        self._check_error("fetch_positions")
        await self._simulate_latency()
        return self._positions.copy()

    def _update_balance_and_position(self, trade: Trade) -> None:
        """
        Update internal balances and positions based on a filled trade.
        Assumes the trade represents a fill.
        (Fix 41: Added position update logic)
        """
        if trade.exchange != self.exchange_name:
            logger.error(
                f"Trade exchange '{trade.exchange}' does not match mock exchange '{self.exchange_name}'"
            )
            return

        logger.debug(
            f"Updating balance and position for trade: {trade.id} ({trade.side} {trade.quantity} {trade.symbol} @ {trade.price})"
        )

        symbol = trade.symbol
        base_asset, quote_asset = self._split_symbol(symbol)
        quantity = trade.quantity
        price = trade.price
        fee = trade.fee
        fee_asset = trade.fee_asset or self.fee_asset  # Use trade fee asset or default
        side = trade.side

        # Ensure base and quote assets exist in balances, initialize if not
        if base_asset not in self._balances:
            self._balances[base_asset] = SpotBalance(
                asset=base_asset,
                exchange=self.exchange_name,
                total_quantity=Decimal(0),
                available_quantity=Decimal(0),
                timestamp=trade.executed_at,  # Use trade timestamp
            )
        # Ensure asset key is string before access
        if isinstance(base_asset, str):
            if quote_asset not in self._balances:
                self._balances[quote_asset] = SpotBalance(
                    asset=quote_asset,
                    exchange=self.exchange_name,
                    total_quantity=Decimal(0),
                    available_quantity=Decimal(0),
                    timestamp=trade.executed_at,  # Use trade timestamp
                )
            if isinstance(quote_asset, str):
                if fee_asset not in self._balances:
                    self._balances[fee_asset] = SpotBalance(
                        asset=fee_asset,
                        exchange=self.exchange_name,
                        total_quantity=Decimal(0),
                        available_quantity=Decimal(0),
                        timestamp=trade.executed_at,  # Use trade timestamp
                    )
                # Update Balances
                cost = quantity * price
                if side == OrderSide.BUY:
                    # Increase base asset, decrease quote asset
                    self._balances[base_asset].total_quantity += quantity
                    self._balances[base_asset].available_quantity += quantity
                    self._balances[quote_asset].total_quantity -= cost
                    self._balances[quote_asset].available_quantity -= cost
                else:  # SELL
                    # Decrease base asset, increase quote asset
                    self._balances[base_asset].total_quantity -= quantity
                    self._balances[base_asset].available_quantity -= quantity
                    self._balances[quote_asset].total_quantity += cost
                    self._balances[quote_asset].available_quantity += cost

                # Deduct Fee
                if fee > 0:
                    if fee_asset not in self._balances:
                        logger.error(
                            f"Fee asset {fee_asset} not found in balances for trade {trade.id}"
                        )
                    else:
                        self._balances[fee_asset].total_quantity -= fee
                        self._balances[fee_asset].available_quantity -= fee

                # Update Timestamps for affected balances (use trade.executed_at)
                self._balances[base_asset].timestamp = trade.executed_at
                self._balances[quote_asset].timestamp = trade.executed_at
                if fee > 0:
                    self._balances[fee_asset].timestamp = trade.executed_at
            else:
                logger.error(f"Invalid type for quote_asset: {type(quote_asset)}")
        else:
            logger.error(f"Invalid type for base_asset: {type(base_asset)}")

        # Update Position (Simplified: assumes perpetuals use same base/quote logic)
        current_position = self._positions.get(symbol)
        if current_position:
            if side == OrderSide.BUY:
                new_size = current_position.size + quantity
            else:  # SELL
                new_size = current_position.size - quantity

            # Calculate new average entry price (Weighted average)
            new_entry_price = Decimal(0)
            if new_size.is_zero():
                new_entry_price = Decimal(0)
            # Handle case where previous size was zero
            elif current_position.size.is_zero():
                new_entry_price = price
            else:
                # Determine if trade is in the same direction as the existing position
                is_same_direction = (current_position.size > 0 and side == OrderSide.BUY) or (
                    current_position.size < 0 and side == OrderSide.SELL
                )

                if is_same_direction:
                    # Increase position: Calculate weighted average entry price
                    new_entry_price = (
                        (current_position.size * current_position.entry_price) + (quantity * price)
                    ) / new_size
                else:
                    # Reduce/flip position: Entry price remains the same for the reduction part
                    # More complex logic needed for accurate PNL/entry tracking on partial closes/flips
                    new_entry_price = current_position.entry_price  # Simplification for now

            # Update the position object in place (if mutable) or replace it (if immutable)
            # Assuming DerivativePosition is mutable for simplicity here
            current_position.size = new_size
            # Update entry price only if size is non-zero
            if not new_size.is_zero():
                current_position.entry_price = new_entry_price
            else:
                current_position.entry_price = Decimal(0)  # Reset entry price if position is closed

            # Update timestamp or other fields if needed
            ticker = self._mock_tickers.get(trade.symbol)
            if ticker and ticker.price is not None:
                current_position.mark_price = ticker.price  # Update mark price
                if not current_position.size.is_zero():
                    if current_position.size > 0:  # Long
                        current_position.unrealized_pnl = (
                            ticker.price - current_position.entry_price
                        ) * current_position.size
                    else:  # Short
                        current_position.unrealized_pnl = (
                            current_position.entry_price - ticker.price
                        ) * abs(current_position.size)
                else:
                    current_position.unrealized_pnl = Decimal(0)  # Reset PNL if size is zero
            else:
                # Keep existing mark price or set PNL to zero if no ticker
                current_position.unrealized_pnl = Decimal(0)

        else:
            # New position
            entry_price = price
            size = quantity if side == OrderSide.BUY else -quantity
            # Initial PNL is zero
            unrealized_pnl = Decimal(0)
            pass

            new_position = DerivativePosition(
                exchange=self.exchange_name,
                symbol=symbol,
                side=side,  # Side of the *initial* trade creating the position
                size=size,
                entry_price=entry_price,
                timestamp=trade.executed_at,  # Add required timestamp
                unrealized_pnl=unrealized_pnl,
                # Add other required fields with default values if needed
                mark_price=entry_price,  # Initial mark price can be entry price
                liquidation_price=None,  # Cannot calculate easily
                margin=None,  # Cannot calculate easily
                leverage=None,  # Not applicable directly here
            )
            self._positions[symbol] = new_position

        logger.debug(
            f"Updated state for {symbol}: Balance={self._balances.get(base_asset)}, Position={self._positions.get(symbol)}"
        )

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """Cancel all open orders, optionally filtered by symbol."""
        self._check_error("cancel_all_orders")
        await self._simulate_latency()
        orders_to_remove = []
        cancelled_count = 0
        for order_id, order in self._orders.items():
            if symbol is None or order.symbol == symbol:
                if not order.status.is_terminal():
                    order.status = OrderStatus.CANCELLED
                    order.updated_at = datetime.now(UTC)
                    cancelled_count += 1
                    logger.debug(
                        f"Mock {self.exchange_name}: Marked order {order_id} as cancelled."
                    )

        # If the test needs orders removed from the dict, do it here, but usually just marking is enough
        # for order_id in orders_to_remove:
        #     del self._orders[order_id]

        logger.info(
            f"Mock {self.exchange_name}: Cancelled {cancelled_count} orders for symbol {symbol or 'all'}."
        )
        return {"success": True, "cancelled_count": cancelled_count}

    async def connect_websocket(self) -> None:
        """Simulate connecting to the WebSocket."""
        logger.info(f"Mock {self.exchange_name}: Simulating WebSocket connect.")
        self._check_error("connect_websocket")
        await self._simulate_latency()
        # No actual connection needed for mock

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Get funding rates for specified symbols or all tracked."""
        self._check_error("get_funding_rates")
        await self._simulate_latency()
        if symbols:
            return [rate for sym, rate in self._mock_funding_rates.items() if sym in symbols]
        return list(self._mock_funding_rates.values())

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """Simulate fetching market data (candles). Returns empty list for now."""
        logger.debug(
            f"Mock {self.exchange_name}: get_market_data called for {symbol} {timeframe} (limit {limit}) - returning empty list."
        )
        self._check_error("get_market_data")
        await self._simulate_latency()
        return []  # TODO: Implement mock candle generation if needed

    def get_message_type(self, message: dict[str, Any]) -> str:
        """Determine the type of a simulated WebSocket message."""
        # Simplified logic based on expected keys
        if "e" in message:  # Assuming Binance-like structure
            return message["e"]
        if "channel" in message and "data" in message:  # Assuming Hyperliquid-like
            return message["channel"]
        return "unknown"

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        """
        Simulate fetching order history. Returns current orders, optionally filtered.
        NOTE: A real implementation would fetch historical, not just current.
        """
        logger.debug(
            f"Mock {self.exchange_name}: get_order_history called for {symbol or 'all'} (limit {limit}). Returning current orders."
        )
        self._check_error("get_order_history")
        await self._simulate_latency()
        filtered_orders = [
            order for order in self._orders.values() if symbol is None or order.symbol == symbol
        ]
        return filtered_orders[-limit:]  # Apply limit

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """
        Simulate fetching trade history.
        """
        logger.debug(
            f"Mock {self.exchange_name}: get_trade_history called for {symbol or 'all'} (limit {limit}). Returning simulated trades."
        )
        self._check_error("get_trade_history")
        await self._simulate_latency()
        filtered_trades = [
            trade for trade in self._trades if symbol is None or trade.symbol == symbol
        ]
        return filtered_trades[-limit:]

    def parse_account_update_message(
        self, message: dict[str, Any]
    ) -> tuple[dict[str, SpotBalance] | None, dict[str, DerivativePosition] | None]:
        """Parse a simulated account update message (balances/positions)."""
        # This needs a specific format based on what tests will simulate
        # Example: Assuming separate keys for balances and positions
        balances = None
        positions = None
        if "balances" in message and isinstance(message["balances"], list):
            balances = {}
            for bal_data in message["balances"]:
                try:
                    balance = self.parse_balance(bal_data)
                    balances[balance.asset] = balance
                except Exception as e:
                    logger.error(f"Failed to parse balance update: {bal_data}. Error: {e}")

        if "positions" in message and isinstance(message["positions"], list):
            positions = {}
            for pos_data in message["positions"]:
                try:
                    position = self.parse_position(pos_data)
                    positions[position.symbol] = position
                except Exception as e:
                    logger.error(f"Failed to parse position update: {pos_data}. Error: {e}")

        return balances, positions

    def parse_balance(
        self, data: dict[str, Any]
    ) -> SpotBalance:  # Updated return type, Ensure implementation raises on failure
        """Parse raw balance data into a SpotBalance model."""
        # Add basic validation and conversion
        required_fields = ["asset", "total", "available"]
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields for SpotBalance: {data}")
        try:
            # Convert numeric strings to Decimal
            data["total_quantity"] = Decimal(str(data["total"]))
            data["available_quantity"] = Decimal(str(data["available"]))
            data["exchange"] = self.exchange_name  # Inject exchange name
            # Ensure timestamp exists, default if necessary
            if "timestamp" not in data:
                data["timestamp"] = datetime.now(UTC)
            # Remove original fields if they conflict with model
            del data["total"]
            del data["available"]

            return SpotBalance(**data)
        except (ValidationError, KeyError, TypeError, InvalidOperation) as e:
            logger.error(f"Failed to parse balance data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse balance data: {e}") from e

    def parse_funding_rate(
        self, data: dict[str, Any]
    ) -> FundingRate:  # Ensure implementation raises on failure
        """Parse raw funding rate data into a FundingRate model."""
        # Add validation and conversion
        # TODO: Implement proper parsing for funding rate data
        # return FundingRate(**data)
        raise NotImplementedError  # Keep as not implemented

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        """Parse a funding rate update message from WebSocket."""
        # Assuming a structure like {'type': 'funding', 'data': {...}}
        if message.get("type") == "funding" and "data" in message:
            try:
                return self.parse_funding_rate(message["data"])
            except Exception as e:
                logger.error(f"Failed to parse funding rate message: {message}. Error: {e}")
        return None

    def parse_order(self, data: dict[str, Any]) -> Order:  # Ensure implementation raises on failure
        """Parse raw order data into an Order model."""
        try:
            # Inject exchange name if missing
            if "exchange" not in data:
                data["exchange"] = self.exchange_name

            # Convert numeric strings/floats to Decimal
            for field in ["price", "quantity_requested", "quantity_filled", "average_fill_price"]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert order field '{field}' value '{data[field]}' to Decimal."
                        )
                        # Raise error instead of returning None
                        raise ValueError(f"Invalid Decimal value for field '{field}'") from None

            # Convert timestamps
            for field in ["created_at"]:  # Removed last_update_time
                if field in data and data[field]:
                    # Assuming timestamp is in ms or seconds epoch, or ISO string
                    # This needs robust parsing logic based on actual API format
                    try:
                        # Example: handle ms epoch int/float or ISO string
                        ts_val = data[field]
                        if isinstance(ts_val, (int, float)):
                            data[field] = datetime.fromtimestamp(ts_val / 1000, UTC)
                        elif isinstance(ts_val, str):
                            data[field] = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                        # Add other format checks if needed
                    except (ValueError, TypeError):
                        logger.error(
                            f"Could not parse timestamp for order field '{field}': {data[field]}"
                        )
                        data[field] = datetime.now(UTC)  # Fallback or raise

            # Convert enums (assuming string values from API)
            if "side" in data:
                data["side"] = OrderSide(data["side"])
            if "order_type" in data:
                data["order_type"] = OrderType(data["order_type"])
            if "status" in data:
                data["status"] = OrderStatus(data["status"])
            if "time_in_force" in data:
                data["time_in_force"] = TimeInForce(data["time_in_force"])

            # Add defaults for missing required fields if applicable
            # These should match the Order model defaults or be handled explicitly
            data.setdefault("updated_at", data.get("created_at", datetime.now(UTC)))
            data.setdefault("triggered_at", None)
            data.setdefault("strategy_name", None)
            data.setdefault("signal_id", None)

            # Validate using Pydantic
            # return Order(**data) # Direct init requires exact fields
            return Order.model_validate(data)
        except (ValidationError, KeyError, TypeError, ValueError, InvalidOperation) as e:
            logger.error(f"Failed to parse order data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse order data: {e}") from e

    def parse_order_book(
        self, data: dict[str, Any], symbol: str
    ) -> OrderBook:  # Ensure implementation raises on failure
        """Parse raw order book data into an OrderBook model."""
        try:
            # Convert bid/ask lists to list of tuples of Decimals
            data["bids"] = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("bids", [])]
            data["asks"] = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("asks", [])]
            data["symbol"] = symbol  # Inject symbol
            # Parse timestamp (similar logic as parse_order)
            if "timestamp" in data and data["timestamp"]:
                ts_val = data["timestamp"]
                if isinstance(ts_val, (int, float)):
                    data["timestamp"] = datetime.fromtimestamp(ts_val / 1000, UTC)
                elif isinstance(ts_val, str):
                    data["timestamp"] = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                else:
                    data["timestamp"] = datetime.now(UTC)  # Fallback
            else:
                data["timestamp"] = datetime.now(UTC)

            # return OrderBook(**data)
            return OrderBook.model_validate(data)
        except (ValidationError, KeyError, TypeError, ValueError, InvalidOperation) as e:
            logger.error(f"Failed to parse order book data for {symbol}: {data}. Error: {e}")
            raise ValueError(f"Failed to parse order book data: {e}") from e

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """Parse an order update message from WebSocket."""
        # Assuming structure like {'type': 'orderUpdate', 'data': {...}}
        # Or based on specific exchange format (e.g., Binance 'e': 'executionReport')
        msg_type = self.get_message_type(message)

        if msg_type in ["orderUpdate", "executionReport"] and "data" in message:
            try:
                # The 'data' might be the order itself or contain order info
                order_data = message["data"]
                # Adapt if the structure is different (e.g., Binance puts fields at top level)
                if msg_type == "executionReport":
                    order_data = message  # Use the whole message for Binance-like

                return self.parse_order(order_data)
            except Exception as e:
                logger.error(f"Failed to parse order update message: {message}. Error: {e}")
        return None

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        """Parse an order book update message from WebSocket."""
        # Assuming structure like {'type': 'depthUpdate', 'symbol': 'BTC-PERP', 'data': {...}}
        msg_type = self.get_message_type(message)
        if msg_type == "depthUpdate" and "data" in message and "symbol" in message:
            try:
                symbol = message["symbol"]
                # The 'data' usually contains bids/asks updates
                # Mock parsing might just return a full OrderBook snapshot based on the update
                # For simplicity, let's assume 'data' IS the full book structure here
                return self.parse_order_book(message["data"], symbol)
            except Exception as e:
                logger.error(f"Failed to parse order book message: {message}. Error: {e}")
        return None

    def parse_position(
        self, data: dict[str, Any]
    ) -> DerivativePosition:  # Ensure implementation raises on failure
        """Parse raw position data into a DerivativePosition model."""
        try:
            data["exchange"] = self.exchange_name  # Inject exchange name

            # Convert numeric fields to Decimal
            for field in [
                "size",
                "entry_price",
                "mark_price",
                "liquidation_price",
                "unrealized_pnl",
                "margin",
                "leverage",
            ]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert position field '{field}' value '{data[field]}' to Decimal."
                        )
                        raise ValueError(
                            f"Invalid Decimal value for position field '{field}'"
                        ) from None

            # Parse timestamp
            if "timestamp" in data and data["timestamp"]:
                ts_val = data["timestamp"]
                if isinstance(ts_val, (int, float)):
                    data["timestamp"] = datetime.fromtimestamp(ts_val / 1000, UTC)
                elif isinstance(ts_val, str):
                    data["timestamp"] = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                else:
                    data["timestamp"] = datetime.now(UTC)  # Fallback
            else:
                data["timestamp"] = datetime.now(UTC)

            # Handle 'side' based on 'size'
            if "size" in data:
                size_dec = data["size"]
                if size_dec > 0:
                    data["side"] = OrderSide.BUY
                elif size_dec < 0:
                    data["side"] = OrderSide.SELL
                else:
                    data["side"] = OrderSide.BUY  # Or None? Needs clarification for zero size

            # return DerivativePosition(**data)
            return DerivativePosition.model_validate(data)
        except (ValidationError, KeyError, TypeError, InvalidOperation, ValueError) as e:
            logger.error(f"Failed to parse position data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse position data: {e}") from e

    def parse_ticker(
        self, data: dict[str, Any], symbol: str
    ) -> Ticker:  # Ensure implementation raises on failure
        """Parse raw ticker data into a Ticker model."""
        try:
            data["symbol"] = symbol  # Inject symbol

            # Convert numeric fields to Decimal
            for field in ["bid", "ask", "price", "volume"]:  # Added volume
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert ticker field '{field}' value '{data[field]}' to Decimal."
                        )
                        # Raise error instead of returning None
                        raise ValueError(
                            f"Could not convert ticker field '{field}' value '{data[field]}' to Decimal."
                        ) from None
            # Parse timestamp
            if "timestamp" in data and data["timestamp"]:
                ts_val = data["timestamp"]
                if isinstance(ts_val, (int, float)):
                    data["timestamp"] = datetime.fromtimestamp(ts_val / 1000, UTC)
                elif isinstance(ts_val, str):
                    data["timestamp"] = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                else:
                    data["timestamp"] = datetime.now(UTC)  # Fallback
            else:
                data["timestamp"] = datetime.now(UTC)

            # return Ticker(**data)
            return Ticker.model_validate(data)
        except Exception as err:
            logger.error(f"Failed to parse ticker data for {symbol}: {data}. Error: {err}")
            raise ValueError(f"Failed to parse ticker data: {err}") from err

    def parse_ticker_message(self, message: dict[str, Any]) -> tuple[str, Ticker] | Ticker | None:
        """Parse a ticker update message from WebSocket."""
        # Example: {'type': 'ticker', 'symbol': 'BTC-PERP', 'data': {...}}
        # Or Binance: {'e': '24hrTicker', 's': 'BTCUSDT', 'c': '50000', ...}
        msg_type = self.get_message_type(message)
        if msg_type in ["ticker", "24hrTicker"]:
            try:
                if msg_type == "24hrTicker":  # Binance style
                    symbol = message["s"]
                    # Map Binance fields to Ticker model fields
                    data = {
                        "bid": message.get("b"),
                        "ask": message.get("a"),
                        "price": message.get("c"),  # Last price
                        "volume": message.get("v"),  # Total traded base asset volume
                        "timestamp": message.get("E"),  # Event time
                    }
                    return symbol, self.parse_ticker(data, symbol)
                elif "data" in message and "symbol" in message:  # Generic/Hyperliquid style
                    symbol = message["symbol"]
                    return symbol, self.parse_ticker(message["data"], symbol)
            except Exception as e:
                logger.error(f"Failed to parse ticker message: {message}. Error: {e}")
        return None

    def parse_trade(
        self, data: dict[str, Any], symbol: str
    ) -> Trade:  # Ensure implementation raises on failure
        """Parse raw trade data into a Trade model."""
        try:
            data["symbol"] = symbol  # Inject symbol
            data["exchange"] = self.exchange_name  # Inject exchange

            # Convert numeric fields to Decimal
            for field in ["price", "quantity", "fee"]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert trade field '{field}' value '{data[field]}' to Decimal."
                        )
                        raise ValueError(
                            f"Invalid Decimal value for trade field '{field}'"
                        ) from None

            # Parse timestamp
            if "timestamp" in data and data["timestamp"]:
                ts_val = data["timestamp"]
                if isinstance(ts_val, (int, float)):
                    data["timestamp"] = datetime.fromtimestamp(ts_val / 1000, UTC)
                elif isinstance(ts_val, str):
                    data["timestamp"] = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                else:
                    data["timestamp"] = datetime.now(UTC)  # Fallback
            else:
                data["timestamp"] = datetime.now(UTC)

            # Convert side enum
            if "side" in data:
                data["side"] = OrderSide(data["side"])

            # Ensure required fields like 'id' exist or generate mock ones
            data.setdefault("id", f"mock-trade-{uuid.uuid4()}")
            data.setdefault("order_id", f"mock-order-{uuid.uuid4()}")
            data.setdefault("fee_asset", self.fee_asset)  # Use exchange default fee asset
            # Add 'executed_at' if missing, use timestamp
            data.setdefault("executed_at", data.get("timestamp", datetime.now(UTC)))
            # Add 'is_maker' if missing, default to False
            data.setdefault("is_maker", False)

            # return Trade(**data)
            return Trade.model_validate(data)
        except (ValidationError, KeyError, TypeError, ValueError, InvalidOperation) as e:
            logger.error(
                f"Failed to parse trade data for {symbol}: {data}. Error: {e}", exc_info=True
            )
            raise ValueError(f"Failed to parse trade data: {e}") from e

    def parse_trade_message(
        self, message: dict[str, Any]
    ) -> Trade | None:  # Return type was already correct
        """Parse a trade update message from WebSocket."""
        # Example: {'type': 'trade', 'symbol': 'BTC-PERP', 'data': {...}}
        # Binance: {'e': 'trade', 's': 'BTCUSDT', 'p': '50001', 'q': '0.01', ...}
        msg_type = self.get_message_type(message)
        if msg_type == "trade":
            try:
                if msg_type == "trade" and "s" in message:  # Binance style
                    symbol = message["s"]
                    # Map Binance fields to Trade model fields
                    data = {
                        "id": message.get("t"),  # Trade ID
                        "order_id": message.get(
                            "a"
                        ),  # Aggregated trade ID? Or use order IDs? Needs check. Assume order ID for now.
                        "price": message.get("p"),
                        "quantity": message.get("q"),
                        "side": "buy"
                        if message.get("m") is False
                        else "sell",  # m=False is buyer maker
                        "timestamp": message.get("T"),  # Trade time
                        "fee": None,  # Binance WS doesn't typically include fee in trade message
                        "fee_asset": None,
                    }
                    return self.parse_trade(data, symbol)
                elif "data" in message and "symbol" in message:  # Generic style
                    symbol = message["symbol"]
                    return self.parse_trade(message["data"], symbol)
            except Exception as e:
                logger.error(f"Failed to parse trade message: {message}. Error: {e}")
        return None

    async def ping_websocket(self) -> None:
        """Simulate sending a ping and receiving a pong."""
        logger.debug(f"Mock {self.exchange_name}: Simulating WebSocket ping/pong.")
        self._check_error("ping_websocket")
        await self._simulate_latency()
        # No actual action needed

    async def subscribe_to_account_updates(self) -> None:
        """Simulate subscribing to account updates."""
        logger.info(f"Mock {self.exchange_name}: Simulating subscribe to account updates.")
        self._check_error("subscribe_to_account_updates")
        await self._simulate_latency()
        # Add handler or topic tracking if needed

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Simulate subscribing to order book updates."""
        # Add symbol validation if needed
        logger.info(f"Mock {self.exchange_name}: Simulating subscribe to order book for {symbol}.")
        self._check_error("subscribe_to_order_book")
        await self._simulate_latency()

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Simulate subscribing to ticker updates."""
        logger.info(f"Mock {self.exchange_name}: Simulating subscribe to ticker for {symbol}.")
        self._check_error("subscribe_to_ticker")
        await self._simulate_latency()

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Simulate subscribing to trade updates."""
        logger.info(f"Mock {self.exchange_name}: Simulating subscribe to trades for {symbol}.")
        self._check_error("subscribe_to_trades")
        await self._simulate_latency()

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """Simulate updating rate limit info (no-op for mock)."""
        _ = method
        _ = path
        logger.debug(
            f"Mock {self.exchange_name}: _update_rate_limit_from_headers called (no-op) with headers: {headers}"
        )
        # No actual rate limiting logic needed for the mock
