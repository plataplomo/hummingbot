import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Callable, Coroutine, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

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
        # return Order(**order_data) # Direct init requires exact fields
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
                timestamp=now,
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

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        """Simulate cancelling an order."""
        self._check_error("cancel_order")
        await self._simulate_latency()

        order = self._orders.get(str(order_id))
        if not order:
            # If client_order_id provided, try finding by that
            found_by_client_id = False
            if client_order_id:
                for o in self._orders.values():
                    if o.client_order_id == str(client_order_id):
                        order = o
                        found_by_client_id = True
                        break
            if not found_by_client_id:
                raise APIError("Order not found", code=APIErrorCode.ORDER_NOT_FOUND.value)

        # Assert order is not None after potentially finding it by client_order_id
        assert order is not None

        # Simulate updates if needed (e.g., status change)
        # For simplicity, just return the found order
        order.status = OrderStatus.CANCELED
        order.updated_at = datetime.now(UTC)  # Update timestamp on cancel
        logger.info(f"Mock {self.exchange_name}: Cancelled order {order_id}")
        # Return the cancelled order details, common practice - use model_dump()
        return {"status": "success", "order": order.model_dump()}

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Retrieve a specific order by its ID."""
        self._check_error("get_order")
        await self._simulate_latency()
        target_order_id = str(order_id)
        # Try finding by exchange order ID first
        order = self._orders.get(target_order_id)

        # If not found by exchange ID and client_order_id is provided, try that
        if not order and client_order_id:
            target_client_order_id = str(client_order_id)
            for o in self._orders.values():
                if o.client_order_id == target_client_order_id:
                    order = o
                    break

        if not order:
            raise APIError("Order not found", code=APIErrorCode.ORDER_NOT_FOUND.value)

        # Simulate updates if needed (e.g., status change)
        # For simplicity, just return the found order
        return order

    async def get_order_status(
        self,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """Get a specific order by ID or raise KeyError if not found (params ignored)."""
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
            self.balances[asset] = balance_data  # Keep public attribute consistent
        elif isinstance(balance_data, dict):
            asset = balance_data.get("asset")
            if asset:
                # Ensure exchange field is present
                if "exchange" not in balance_data:
                    balance_data["exchange"] = self.exchange_name
                try:
                    balance_obj = SpotBalance(**balance_data)
                    self._balances[asset] = balance_obj
                    self.balances[asset] = balance_obj  # Keep public attribute consistent
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

        # --- Balance Update --- (Existing logic, ensure robustness)
        # Assume trade.price and trade.quantity are non-None based on Mypy unreachable error
        cost = trade.cost
        fee = trade.fee
        fee_asset = trade.fee_asset

        # Determine the asset involved (base or quote)
        base_asset, quote_asset = self._split_symbol(trade.symbol)

        # Update quote currency balance (e.g., USD, USDC)
        quote_balance = self._balances.get(
            quote_asset, SpotBalance(asset=quote_asset, total=Decimal("0"), available=Decimal("0"))
        )
        quote_balance.total = Decimal(quote_balance.total or 0)
        quote_balance.available = Decimal(quote_balance.available or 0)
        if trade.side == OrderSide.BUY:
            quote_balance.total -= cost
            quote_balance.available -= cost  # Assuming cost reflects available reduction
        elif trade.side == OrderSide.SELL:
            quote_balance.total += cost
            quote_balance.available += cost

        # Update fee asset balance
        if fee > 0:
            fee_balance = self._balances.get(
                fee_asset, SpotBalance(asset=fee_asset, total=Decimal("0"), available=Decimal("0"))
            )
            fee_balance.total = Decimal(fee_balance.total or 0)
            fee_balance.available = Decimal(fee_balance.available or 0)
            fee_balance.total -= fee
            fee_balance.available -= fee
            self._balances[fee_asset] = fee_balance
            logger.debug(f"Applied fee: {fee} {fee_asset}")

        # Update base currency balance
        base_balance = self._balances.get(
            base_asset, SpotBalance(asset=base_asset, total=Decimal("0"), available=Decimal("0"))
        )
        base_balance.total = Decimal(base_balance.total or 0)
        base_balance.available = Decimal(base_balance.available or 0)
        if trade.side == OrderSide.BUY:
            base_balance.total += trade.quantity
            base_balance.available += trade.quantity  # Simplified
        elif trade.side == OrderSide.SELL:
            base_balance.total -= trade.quantity
            base_balance.available -= trade.quantity  # Simplified

        self._balances[quote_asset] = quote_balance
        self._balances[base_asset] = base_balance

        logger.debug(f"Updated balances: {self._balances}")

        # --- Position Update ---
        existing_position = self._positions.get(trade.symbol)

        if existing_position:
            logger.debug(f"Updating existing position for {trade.symbol}: {existing_position}")
            # Assume existing_position.size, entry_price and trade.quantity, price are non-None
            # based on Mypy unreachable error and logic creating these objects.
            assert existing_position.size is not None
            assert existing_position.entry_price is not None
            assert trade.quantity is not None
            assert trade.price is not None

            if existing_position.side == trade.side:
                # Increasing position size
                new_size = existing_position.size + trade.quantity
                trade_fee_adj = fee  # Fee affects cost basis when increasing
                trade_cost_basis_adjustment = trade.price * trade.quantity
                if trade.side == OrderSide.BUY:
                    trade_cost_basis_adjustment += trade_fee_adj
                else:  # SELL
                    trade_cost_basis_adjustment -= trade_fee_adj

                if new_size.copy_abs() < Decimal("1e-12"):  # Use tolerance for zero check
                    new_avg_entry = Decimal("0.0")
                else:
                    original_cost = existing_position.size * existing_position.entry_price
                    # New average entry = (Original Cost + New Trade Cost Adjustment) / New Size
                    new_avg_entry = (original_cost + trade_cost_basis_adjustment) / new_size

                existing_position.entry_price = new_avg_entry
                existing_position.size = new_size
                logger.debug(
                    f"Increased position size. New Avg Entry: {new_avg_entry}, New Size: {new_size}"
                )
            else:
                # Reducing or flipping position
                trade_fee_realized = fee  # Fee is realized on close/reduce
                if (
                    trade.quantity >= existing_position.size.copy_abs()
                ):  # Use absolute size for comparison
                    # Closing or flipping position
                    close_quantity = existing_position.size.copy_abs()  # Use absolute size
                    # Calculate PNL considering fee
                    if existing_position.side == OrderSide.BUY:  # Selling to close LONG
                        pnl = (trade.price * close_quantity - trade_fee_realized) - (
                            existing_position.entry_price * close_quantity
                        )
                    else:  # Buying to close SHORT
                        pnl = (existing_position.entry_price * close_quantity) - (
                            trade.price * close_quantity + trade_fee_realized
                        )

                    logger.debug(
                        f"Position closed/flipped. Realized PNL (approx, incl. fee): {pnl}"
                    )
                    # TODO: Track realized PNL

                    remaining_trade_qty = trade.quantity - close_quantity
                    if remaining_trade_qty > Decimal("1e-12"):  # Flipped
                        assert trade.side is not None, (
                            "Trade side cannot be None when flipping position"
                        )
                        existing_position.side = trade.side
                        existing_position.size = remaining_trade_qty
                        existing_position.entry_price = (
                            trade.price
                        )  # New entry price is the flip price
                        logger.debug(f"Position flipped to {existing_position.side}")
                    else:
                        # Position closed exactly or dust remaining
                        logger.debug(f"Position closed for {trade.symbol}")
                        del self._positions[trade.symbol]
                        existing_position = None  # Mark as deleted
                else:
                    # Reducing position size
                    reduce_quantity = trade.quantity
                    # Calculate realized PNL for the reduced portion
                    if existing_position.side == OrderSide.BUY:  # Selling to reduce LONG
                        pnl = (trade.price * reduce_quantity - trade_fee_realized) - (
                            existing_position.entry_price * reduce_quantity
                        )
                    else:  # Buying to reduce SHORT
                        pnl = (existing_position.entry_price * reduce_quantity) - (
                            trade.price * reduce_quantity + trade_fee_realized
                        )

                    logger.debug(f"Position size reduced. Realized PNL (approx, incl. fee): {pnl}")
                    # TODO: Track realized PNL
                    existing_position.size -= reduce_quantity  # Reduce size (maintains sign)
                    logger.debug(f"Reduced position size. New Size: {existing_position.size}")

            # Update timestamp or other fields if needed
            if existing_position:  # Check if not deleted
                # existing_position.timestamp = trade.timestamp # Position model doesn't have timestamp directly? Check model.
                # Update unrealized PNL if mark price is available
                ticker = self._mock_tickers.get(trade.symbol)
                if ticker and ticker.price is not None:  # Check if ticker and its price exist
                    existing_position.mark_price = ticker.price
                    existing_position.calculate_unrealized_pnl(ticker.price)
                logger.debug(f"Updated position: {existing_position}")

        else:
            # Creating a new position
            # Assume trade.side, quantity, price are non-None based on Mypy unreachable error
            assert trade.side is not None, "Trade side cannot be None when creating position"
            assert trade.quantity is not None, (
                "Trade quantity cannot be None when creating position"
            )
            assert trade.price is not None, "Trade price cannot be None when creating position"

            # Restore missing fields based on Position definition
            new_position = DerivativePosition(
                symbol=trade.symbol,
                side=trade.side,
                size=trade.quantity,
                entry_price=trade.price,
                leverage=Decimal("1"),  # Default leverage
                # timestamp=trade.timestamp, # Position model doesn't have timestamp directly?
                margin_type="cross",  # Default margin type
                unrealized_pnl=Decimal("0.0"),  # Initial PNL is zero
                # Add other optional fields as needed, defaulting to None
                id=None,
                status=None,  # Status might be 'OPEN' implicitly
                mark_price=trade.price,  # Initial mark price can be entry price
                liquidation_price=None,
                realized_pnl=None,
                margin_used=None,
                strategy_name=None,
                close_price=None,
                close_time=None,
                pnl=None,
            )
            self._positions[trade.symbol] = new_position
            logger.debug(f"Created new position: {new_position}")

    # --- WebSocket Simulation ---

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """Simulate cancelling all orders."""
        self._check_error("cancel_all_orders")
        await self._simulate_latency()
        cancelled_count = 0
        final_statuses = (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        )
        for order_id, order in self._orders.items():
            if order.status not in final_statuses and (symbol is None or order.symbol == symbol):
                order.status = OrderStatus.CANCELED
                # order.last_update_time = datetime.now(UTC) # Order model doesn't have this
                cancelled_count += 1
                # Don't remove immediately, just mark as cancelled
                logger.debug(f"Mock {self.exchange_name}: Marked order {order_id} as cancelled.")

        # If the test needs orders removed from the dict, do it here, but usually just marking is enough
        # for order_id in orders_to_remove:
        #     del self._orders[order_id]

        logger.info(f"Mock {self.exchange_name}: Cancelled {cancelled_count} orders.")
        return {"status": "success", "cancelled_count": cancelled_count}

    async def connect_websocket(self) -> None:
        """Simulate connecting to WebSocket."""
        self._check_error("connect_websocket")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: WebSocket connected (simulated).")
        self._is_connected = True  # Assuming _is_connected is tracked

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Return mock funding rates."""
        self._check_error("get_funding_rates")
        await self._simulate_latency()
        if symbols:
            return [self._mock_funding_rates[s] for s in symbols if s in self._mock_funding_rates]
        return list(self._mock_funding_rates.values())

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """Return mock market data as Candle objects."""
        self._check_error("get_market_data")
        await self._simulate_latency()
        # Placeholder: Return empty list matching list[Candle]
        # Real implementation would fetch OHLCV data.
        return []

    def get_message_type(self, message: dict[str, Any]) -> str:
        """Determine message type (placeholder)."""
        # Ensure a string is always returned, raise if type cannot be determined
        msg_type = message.get("type") or message.get("e")
        if not isinstance(msg_type, str):
            raise ValueError(f"Could not determine message type from message: {message}")
        return msg_type

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        """Return mock order history."""
        self._check_error("get_order_history")
        await self._simulate_latency()
        history = sorted(
            self._orders.values(),
            key=lambda o: o.created_at or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        if symbol:
            history = [o for o in history if o.symbol == symbol]
        return history[:limit]

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Return mock trade history."""
        self._check_error("get_trade_history")
        await self._simulate_latency()
        history = sorted(
            self._trades, key=lambda t: t.timestamp if t.timestamp is not None else 0, reverse=True
        )
        if symbol:
            history = [t for t in history if t.symbol == symbol]
        return history[:limit]

    def parse_account_update_message(
        self, message: dict[str, Any]
    ) -> tuple[dict[str, SpotBalance] | None, dict[str, DerivativePosition] | None]:
        """
        Parse account update message to extract balances and positions.
        Returns a tuple: (balances_dict, positions_dict). Dictionaries are None if not present.
        """
        # Placeholder implementation - assumes message contains 'balances' and/or 'positions' keys
        # Needs adaptation based on actual WebSocket message format
        updated_balances = None
        updated_positions = None

        if "balances" in message:
            try:
                # Assume message["balances"] is dict[str, dict]
                parsed_balances = {}
                for asset, data in message["balances"].items():
                    # Add exchange for parsing consistency
                    data["exchange"] = self.exchange_name
                    parsed_balances[asset] = self.parse_balance(data)
                updated_balances = parsed_balances
            except Exception as e:
                logger.error(f"Failed to parse balances from WS message: {e}", exc_info=True)

        if "positions" in message:
            try:
                # Assume message["positions"] is list[dict] or dict[str, dict]
                parsed_positions = {}
                pos_data = message["positions"]
                # Use isinstance checks that work with generics
                if isinstance(pos_data, list):
                    for data in pos_data:
                        # Add exchange for parsing consistency
                        if isinstance(data, dict):
                            data["exchange"] = self.exchange_name
                            pos = self.parse_position(data)
                            parsed_positions[pos.symbol] = pos  # Use symbol as key
                        parsed_positions[pos.symbol] = pos  # Use symbol as key
                elif isinstance(pos_data, dict):  # Handle dict of positions (symbol -> data)
                    for symbol, data in pos_data.items():
                        # Add exchange for parsing consistency
                        data["exchange"] = self.exchange_name
                        parsed_positions[symbol] = self.parse_position(data)
                updated_positions = parsed_positions
            except Exception as e:
                logger.error(f"Failed to parse positions from WS message: {e}", exc_info=True)

        return updated_balances, updated_positions

    def parse_balance(
        self, data: dict[str, Any]
    ) -> SpotBalance:  # Updated return type, Ensure implementation raises on failure
        """Parse balance data, raising ValueError on failure."""
        try:
            # Ensure required fields are present before passing to SpotBalance
            required_fields = ["asset", "total_quantity", "available_quantity", "timestamp"]
            if "exchange" not in data:
                data["exchange"] = self.exchange_name  # Add if missing

            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required balance field: {field}")

            # Perform necessary type conversions (e.g., string to Decimal)
            data["total_quantity"] = Decimal(str(data["total_quantity"]))
            data["available_quantity"] = Decimal(str(data["available_quantity"]))
            # Assuming timestamp is correctly formatted or handled by Pydantic

            return SpotBalance(**data)
        except (ValidationError, KeyError, TypeError, InvalidOperation) as e:
            logger.error(f"Failed to parse balance data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse balance data: {e}") from e

    def parse_funding_rate(
        self, data: dict[str, Any]
    ) -> FundingRate:  # Ensure implementation raises on failure
        """Parse funding rate data (placeholder)."""
        try:
            # Assuming data is dict-like
            return FundingRate(**data)
        except Exception as err:
            logger.error(f"Mock parse_funding_rate failed for data: {data}. Error: {err}")
            raise ValueError(f"Mock parse_funding_rate failed: {err}") from err

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        """Parse funding rate message (placeholder)."""
        # Depends heavily on exchange message format
        return None

    def parse_order(self, data: dict[str, Any]) -> Order:  # Ensure implementation raises on failure
        """Parse order data (placeholder)."""
        try:
            # Assuming data is dict-like
            # Convert fields as needed
            if "side" in data and isinstance(data["side"], str):
                data["side"] = OrderSide(data["side"])
            if "type" in data and isinstance(data["type"], str):
                data["type"] = OrderType(data["type"])
            if "status" in data and isinstance(data["status"], str):
                data["status"] = OrderStatus(data["status"])
            if "time_in_force" in data and isinstance(data["time_in_force"], str):
                data["time_in_force"] = TimeInForce(data["time_in_force"])
            # Convert numeric fields
            for field in [
                "price",
                "quantity",
                "filled_quantity",
                "average_fill_price",
            ]:  # Removed fee, cost
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert order field '{field}' value '{data[field]}' to Decimal."
                        )
                        # Raise error instead of returning None
                        raise ValueError(f"Invalid Decimal value for field '{field}'")
            # Convert timestamps
            for field in ["created_at"]:  # Removed last_update_time
                if field in data and data[field] is not None:
                    # Assuming datetime object or compatible string/int
                    try:
                        if isinstance(data[field], int | float):
                            ts_sec = int(data[field]) / 1000
                            data[field] = datetime.fromtimestamp(ts_sec, tz=UTC)
                        elif isinstance(data[field], str):
                            data[field] = datetime.fromisoformat(data[field].replace("Z", "+00:00"))
                        # If already datetime, do nothing
                    except (ValueError, TypeError, OSError):
                        logger.error(
                            f"Could not convert order timestamp '{data[field]}' for field '{field}'"
                        )
                        data[field] = None  # Or raise error if timestamp is mandatory

            return Order(**data)
        except Exception as err:
            logger.error(f"Mock parse_order failed for data: {data}. Error: {err}")
            raise ValueError(f"Mock parse_order failed: {err}") from err

    def parse_order_book(
        self, data: dict[str, Any], symbol: str
    ) -> OrderBook:  # Ensure implementation raises on failure
        """Parse order book data (placeholder)."""
        try:
            # Assuming data has 'bids' and 'asks' lists
            # Convert price/quantity pairs to Decimal
            bids = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("bids", [])]
            asks = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("asks", [])]
            ts_raw = data.get("timestamp", datetime.now(UTC).timestamp() * 1000)
            ts = int(ts_raw) if ts_raw is not None else None  # Convert to int
            return OrderBook(symbol=symbol, bids=bids, asks=asks, timestamp=ts)
        except Exception as err:
            logger.error(f"Mock parse_order_book failed for data: {data}. Error: {err}")
            raise ValueError(f"Mock parse_order_book failed: {err}") from err

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """Parse order update message (placeholder)."""
        # Depends heavily on exchange message format
        # Should ideally call self.parse_order if data structure matches
        try:
            # Example: Assuming message contains the order data directly
            if "order_data" in message:
                return self.parse_order(message["order_data"])
            return None  # Or raise if format is unknown/invalid
        except Exception as err:
            logger.error(f"Failed to parse order update message: {message}, Error: {err}")
            return None  # Keep returning None for WS messages if parsing fails

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        """Parse order book message (placeholder)."""
        # Depends heavily on exchange message format
        # Should ideally call self.parse_order_book if data structure matches
        try:
            if "symbol" in message and ("bids" in message or "asks" in message):
                return self.parse_order_book(message, message["symbol"])
            return None
        except Exception as err:
            logger.error(f"Failed to parse orderbook message: {message}, Error: {err}")
            return None  # Keep returning None for WS messages

    def parse_position(
        self, data: dict[str, Any]
    ) -> DerivativePosition:  # Ensure implementation raises on failure
        """Parse position data, raising ValueError on failure."""
        try:
            # Ensure required fields
            required_fields = ["symbol", "side", "size", "entry_price"]
            if "exchange" not in data:
                data["exchange"] = self.exchange_name

            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required position field: {field}")

            # Convert types
            data["side"] = OrderSide(data["side"])
            data["size"] = Decimal(str(data["size"]))
            data["entry_price"] = Decimal(str(data["entry_price"]))
            if "mark_price" in data and data["mark_price"] is not None:
                data["mark_price"] = Decimal(str(data["mark_price"]))
            if "unrealized_pnl" in data and data["unrealized_pnl"] is not None:
                data["unrealized_pnl"] = Decimal(str(data["unrealized_pnl"]))
            if "realized_pnl" in data and data["realized_pnl"] is not None:
                data["realized_pnl"] = Decimal(str(data["realized_pnl"]))
            if "liquidation_price" in data and data["liquidation_price"] is not None:
                data["liquidation_price"] = Decimal(str(data["liquidation_price"]))
            if "initial_margin" in data and data["initial_margin"] is not None:
                data["initial_margin"] = Decimal(str(data["initial_margin"]))
            if "maintenance_margin" in data and data["maintenance_margin"] is not None:
                data["maintenance_margin"] = Decimal(str(data["maintenance_margin"]))
            if "leverage" in data and data["leverage"] is not None:
                data["leverage"] = Decimal(str(data["leverage"]))
            # Assuming timestamp is handled by Pydantic if present

            return DerivativePosition(**data)
        except (ValidationError, KeyError, TypeError, InvalidOperation, ValueError) as e:
            logger.error(f"Failed to parse position data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse position data: {e}") from e

    def parse_ticker(
        self, data: dict[str, Any], symbol: str
    ) -> Ticker:  # Ensure implementation raises on failure
        """Parse ticker data (placeholder)."""
        try:
            # Assuming data is dict-like
            # Convert numeric fields
            for field in ["price", "bid", "ask", "volume"]:
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
                        )
            return Ticker(**data)
        except Exception as err:
            logger.error(f"Mock parse_ticker failed for data: {data}. Error: {err}")
            raise ValueError(f"Mock parse_ticker failed: {err}") from err

    def parse_ticker_message(self, message: dict[str, Any]) -> tuple[str, Ticker] | Ticker | None:
        """Parse ticker message (placeholder)."""
        # Depends heavily on exchange message format
        # Might return Ticker directly or (symbol, Ticker) tuple
        return None

    def parse_trade(
        self, data: dict[str, Any], symbol: str
    ) -> Trade:  # Ensure implementation raises on failure
        """Parse trade data, raising ValueError on failure."""
        try:
            # Ensure required fields
            required_fields = [
                "id",
                "order_id",
                "price",
                "quantity",
                "side",
                "timestamp",
                "is_maker",
            ]
            if "exchange" not in data:
                data["exchange"] = self.exchange_name

            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required trade field: {field}")

            # Convert types
            data["symbol"] = symbol  # Add symbol if missing
            data["price"] = Decimal(str(data["price"]))
            data["quantity"] = Decimal(str(data["quantity"]))
            data["side"] = OrderSide(data["side"])
            if "fee" in data and data["fee"] is not None:
                data["fee"] = Decimal(str(data["fee"]))
            # Assuming timestamp is handled by Pydantic
            # Ensure is_maker is bool
            data["is_maker"] = bool(data["is_maker"])

            return Trade(**data)
        except (ValidationError, KeyError, TypeError, InvalidOperation, ValueError) as e:
            logger.error(f"Failed to parse trade data: {data}. Error: {e}", exc_info=True)
            raise ValueError(f"Failed to parse trade data: {e}") from e

    def parse_trade_message(
        self, message: dict[str, Any]
    ) -> Trade | None:  # Return type was already correct
        """Parse trade message (placeholder)."""
        # Depends heavily on exchange message format
        # Might contain single trade or list of trades
        return None

    async def ping_websocket(self) -> None:
        """Simulate sending a WebSocket ping."""
        self._check_error("ping_websocket")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Sent WebSocket ping (simulated).")

    async def subscribe_to_account_updates(self) -> None:
        """Simulate subscribing to account updates."""
        self._check_error("subscribe_to_account_updates")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to account updates (simulated).")

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Simulate subscribing to order book."""
        self._check_error("subscribe_to_order_book")
        await self._simulate_latency()
        logger.info(
            f"Mock {self.exchange_name}: Subscribed to order book for {symbol} (simulated)."
        )

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Simulate subscribing to ticker."""
        self._check_error("subscribe_to_ticker")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to ticker for {symbol} (simulated).")

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Simulate subscribing to trades."""
        self._check_error("subscribe_to_trades")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to trades for {symbol} (simulated).")

    # Corrected signature to match base class
    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """Mock implementation - does nothing (ignores method/path)."""
        # Mark params as unused if necessary for linters
        _ = method
        _ = path
        logger.debug(
            f"Mock {self.exchange_name}: _update_rate_limit_from_headers called (no-op) with headers: {headers}"
        )
        # No actual rate limiting logic needed for the mock
