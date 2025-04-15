import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

# Import Fill type
from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Balance,
    Fill,  # Added Fill
    FundingRate,
    MarketData,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    TimeInForce,
    Trade,
)

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
    ):  # Add optional config_obj parameter
        super().__init__(exchange_name, config, secrets)
        self.full_config = config_obj  # Store the full config object if provided
        self._order_id_counter = 1
        self._orders: dict[str, Order] = {}  # Store orders by ID
        self._positions: dict[str, Position] = {}  # Store positions by symbol
        self._balances: dict[str, Balance] = {}  # Store balances by asset
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
        self.balances: dict[str, Balance] = {}
        self.positions: dict[str, Position] = {}
        self.open_orders: dict[str, Order] = {}
        self.trade_history: list[Trade] = []
        self.api_errors: list[dict[str, Any]] = []

        # Error simulation
        self._error_config: dict[str, tuple[Exception, int | None]] = {}
        self._call_counts: dict[str, int] = defaultdict(int)

        # Behavior settings
        self._open_orders_behavior = "keep_open"

        logger.info(
            f"Initialized MockExchangeAPI for {exchange_name} (Maker Fee: {self.maker_fee}, Taker Fee: {self.taker_fee}, Fee Asset: {self.fee_asset})"
        )

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
        error = APIError(message=message, code=error_code, exchange_code=self.exchange_name)
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

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Return mock ticker data."""
        self._check_error("get_ticker")
        await self._simulate_latency()
        return self._mock_tickers.get(symbol)

    # Corrected override signature and implementation
    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Return mock order book data."""
        self._check_error("get_order_book")
        await self._simulate_latency()
        # Return a basic OrderBook structure, ensuring timestamp is int
        return OrderBook(
            symbol=symbol, bids=[], asks=[], timestamp=int(datetime.now(UTC).timestamp() * 1000)
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

    async def get_balances(self) -> dict[str, Balance]:
        """Return mock balances."""
        self._check_error("get_balances")
        await self._simulate_latency()
        return self._balances.copy()

    # Corrected override signature
    async def get_positions(self, symbol: str | None = None) -> list[Position]:
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
                "Price must be specified for LIMIT orders", code=APIErrorCode.INVALID_PARAMS
            )
        if order_type == OrderType.MARKET and price is not None:
            logger.warning("Price is ignored for MARKET orders")

        order_id = f"{self.exchange_name.lower()}-{self._order_id_counter}"
        self._order_id_counter += 1
        order_time = datetime.now(UTC)  # Use a consistent time for the order

        # Determine fill price for market orders or use limit price
        fill_price = price
        if order_type == OrderType.MARKET:
            # Simulate market price based on mock ticker or a default
            ticker = self._mock_tickers.get(symbol)
            if ticker and ticker.price:
                # Simulate some slippage for market orders
                slippage_factor = Decimal("0.001")  # 0.1% slippage
                if side == OrderSide.BUY:
                    fill_price = ticker.price * (Decimal("1") + slippage_factor)
                else:
                    fill_price = ticker.price * (Decimal("1") - slippage_factor)
            else:
                # Fallback if no ticker price
                fill_price = price if price else Decimal("100.0")  # Arbitrary fallback
                logger.warning(
                    f"No mock ticker price for {symbol}, using fallback fill price: {fill_price}"
                )

        # Basic validation (can be expanded)
        if quantity <= Decimal("0"):
            raise APIError("Order quantity must be positive", code=APIErrorCode.INVALID_PARAMS)

        # Create the order object - using only fields defined in models.Order
        order = Order(
            id=order_id,
            client_order_id=client_order_id or f"mock-{order_id}",
            symbol=symbol,
            side=side,
            type=order_type,
            status=OrderStatus.OPEN,  # Initial status
            price=price,  # Limit price
            quantity=quantity,
            filled_quantity=Decimal("0.0"),
            avg_fill_price=None,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            time=order_time,
        )

        self._orders[order_id] = order
        logger.info(f"Mock {self.exchange_name}: Placed order {order_id}: {order}")

        # Simulate immediate fill for market orders or based on behavior
        trade_cost: Decimal | None = None
        trade_fee: Decimal | None = None
        trade_fee_asset: str | None = None
        trade_timestamp: datetime | None = None

        if order_type == OrderType.MARKET or self._open_orders_behavior == "fill_immediately":
            if fill_price is None:
                # This should ideally not happen for market orders if logic is correct
                raise ValueError("Cannot fill market order without a fill price")

            order.status = OrderStatus.FILLED
            order.filled_quantity = quantity
            order.avg_fill_price = fill_price
            trade_timestamp = datetime.now(UTC)  # Use a consistent time for the fill/trade

            # Calculate cost and fee
            trade_cost = order.filled_quantity * order.avg_fill_price
            # Determine fee rate (simplified: assume taker for market orders)
            trade_fee_rate = self.taker_fee
            trade_fee = trade_cost * trade_fee_rate
            trade_fee_asset = self.fee_asset

            logger.info(f"Mock {self.exchange_name}: Order {order_id} filled immediately.")

            # Create and record the trade
            assert order.avg_fill_price is not None  # Ensure fill price is set
            assert trade_timestamp is not None  # Ensure timestamp is set
            trade_to_record = Trade(
                id=f"trade-{order_id}",
                order_id=order_id,
                exchange=self.exchange_name,  # Trade model has exchange
                symbol=symbol,
                side=side,
                price=order.avg_fill_price,
                quantity=order.filled_quantity,
                fee=trade_fee,
                fee_asset=trade_fee_asset,
                cost=trade_cost,
                timestamp=int(trade_timestamp.timestamp() * 1000),  # Convert to int ms
                is_maker=False,  # Assume taker for market fills
            )
            self._trades.append(trade_to_record)
            # Update balances and positions based on the trade
            self._update_balance_and_position(trade_to_record)

        elif self._open_orders_behavior == "partial_fill":
            if fill_price is None:
                raise ValueError("Cannot partially fill order without a fill price")

            # Simulate partial fill (e.g., 50%)
            partial_fill_qty = quantity / Decimal("2")
            order.status = OrderStatus.PARTIALLY_FILLED
            order.filled_quantity = partial_fill_qty
            order.avg_fill_price = fill_price  # Use simulated fill price
            trade_timestamp = datetime.now(UTC)  # Use a consistent time for the fill/trade

            # Calculate cost and fee for partial fill
            trade_cost = order.filled_quantity * order.avg_fill_price
            trade_fee_rate = self.taker_fee  # Assume taker for simplicity
            trade_fee = trade_cost * trade_fee_rate
            trade_fee_asset = self.fee_asset

            logger.info(f"Mock {self.exchange_name}: Order {order_id} partially filled.")

            # Create and record the partial trade
            assert order.avg_fill_price is not None  # Ensure fill price is set
            assert trade_timestamp is not None  # Ensure timestamp is set
            trade_to_record = Trade(
                id=f"trade-{order_id}-p1",
                order_id=order_id,
                exchange=self.exchange_name,  # Trade model has exchange
                symbol=symbol,
                side=side,
                price=order.avg_fill_price,
                quantity=order.filled_quantity,
                fee=trade_fee,
                fee_asset=trade_fee_asset,
                cost=trade_cost,
                timestamp=int(trade_timestamp.timestamp() * 1000),  # Convert to int ms
                is_maker=False,
            )
            self._trades.append(trade_to_record)
            self._update_balance_and_position(trade_to_record)

        # If behavior is 'keep_open', the order remains in self._orders with status OPEN

        return order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        """Simulate cancelling an order."""
        self._check_error("cancel_order")
        await self._simulate_latency()

        order = self._orders.get(order_id)
        if not order:
            raise APIError(f"Order {order_id} not found", code=APIErrorCode.ORDER_NOT_FOUND)

        # Check against final statuses explicitly
        final_statuses = (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        )
        if order.status in final_statuses:
            logger.warning(f"Order {order_id} is already in final state: {order.status}")
            # Return success indication even if already final, mimicking some exchanges
            return {"status": "success", "order": order.to_dict()}

        order.status = OrderStatus.CANCELED
        # order.last_update_time = datetime.now(UTC) # Order model doesn't have this
        logger.info(f"Mock {self.exchange_name}: Cancelled order {order_id}")
        # Return the cancelled order details, common practice
        return {"status": "success", "order": order.to_dict()}

    async def get_order(self, order_id: str) -> Order | None:
        """Get a specific order by ID."""
        self._check_error("get_order")
        await self._simulate_latency()
        return self._orders.get(order_id)

    async def get_order_status(self, order_id: str, **kwargs: Any) -> Order | None:
        """Get the status of a specific order."""
        self._check_error("get_order_status")
        await self._simulate_latency()
        return self._orders.get(order_id)

    # Corrected override signature
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error("get_open_orders")
        await self._simulate_latency()
        final_statuses = (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        )
        open_orders = [
            o
            for o in self._orders.values()
            if o.status not in final_statuses and (symbol is None or o.symbol == symbol)
        ]
        return open_orders

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
    ) -> list["Fill"]:  # Use forward reference if Fill not imported directly
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

    def set_mock_balance(self, balance_data: Balance | dict[str, Any]) -> None:
        """Set a predefined balance for an asset."""
        self._check_error("set_mock_balance")
        balance_obj: Balance | None = None
        asset: str | None = None

        if isinstance(balance_data, Balance):
            balance_obj = balance_data
            asset = balance_obj.asset
            logger.debug(
                f"Mock {self.exchange_name}: Setting mock balance for {asset} using Balance object."
            )
        elif isinstance(balance_data, dict):
            # Try to construct Balance from dict
            try:
                # Extract required fields first
                asset_val = balance_data.get("asset")
                total_val = balance_data.get("total")
                if asset_val is None or total_val is None:
                    raise ValueError("Missing 'asset' or 'total' in balance dict")

                # Extract optional fields with defaults
                available_val = balance_data.get(
                    "available", total_val
                )  # Default available to total
                free_val = balance_data.get("free", available_val)  # Default free to available
                locked_val = balance_data.get(
                    "locked", Decimal(str(total_val)) - Decimal(str(available_val))
                )  # Default locked

                # Create Balance object, ensuring Decimal conversion
                balance_obj = Balance(
                    asset=str(asset_val),
                    total=Decimal(str(total_val)),
                    available=Decimal(str(available_val)),
                    free=Decimal(str(free_val)),
                    locked=Decimal(str(locked_val)),
                )
                asset = balance_obj.asset
                logger.debug(
                    f"Mock {self.exchange_name}: Setting mock balance for {asset} using direct Balance dict."
                )
            except (TypeError, KeyError, ValueError, InvalidOperation) as e:
                logger.error(f"Failed to parse direct balance dict: {balance_data}. Error: {e}")
                return  # Don't proceed if parsing fails

        if asset and balance_obj:
            self._balances[asset] = balance_obj
        else:
            logger.error("Could not determine asset or create Balance object.")

    def set_mock_position(self, position: Position) -> None:
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
            raise APIError(f"Ticker for {symbol} not found", code=APIErrorCode.SYMBOL_NOT_FOUND)
        return ticker

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """Fetch funding rate (simulated)."""
        self._check_error("fetch_funding_rate")
        await self._simulate_latency()
        rate = self._mock_funding_rates.get(symbol)
        if not rate:
            raise APIError(
                f"Funding rate for {symbol} not found",
                code=APIErrorCode.FUNDING_RATE_UNAVAILABLE,
            )
        return rate

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch balances (simulated)."""
        self._check_error("fetch_balances")
        await self._simulate_latency()
        return self._balances.copy()

    async def fetch_positions(self) -> dict[str, Position]:
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
        cost = trade.cost if trade.cost is not None else trade.price * trade.quantity
        fee = trade.fee if trade.fee is not None else Decimal("0")
        fee_asset = trade.fee_asset if trade.fee_asset is not None else self.fee_asset

        # Determine the asset involved (base or quote)
        base_asset, quote_asset = self._split_symbol(trade.symbol)

        # Update quote currency balance (e.g., USD, USDC)
        quote_balance = self._balances.get(
            quote_asset, Balance(asset=quote_asset, total=Decimal("0"), available=Decimal("0"))
        )
        if trade.side == OrderSide.BUY:
            if cost is not None and quote_balance.total is not None:
                quote_balance.total -= cost
            if cost is not None and quote_balance.available is not None:
                quote_balance.available -= cost  # Assuming cost reflects available reduction
        elif trade.side == OrderSide.SELL:
            # Ensure cost and balances are not None before arithmetic
            if cost is not None and quote_balance.total is not None:
                quote_balance.total += cost
            if cost is not None and quote_balance.available is not None:
                quote_balance.available += cost

        # Update fee asset balance
        if fee > 0:
            fee_balance = self._balances.get(
                fee_asset, Balance(asset=fee_asset, total=Decimal("0"), available=Decimal("0"))
            )
            # Ensure fee and balances are not None
            if fee is not None and fee_balance.total is not None:
                fee_balance.total -= fee
            if fee is not None and fee_balance.available is not None:
                fee_balance.available -= fee
            self._balances[fee_asset] = fee_balance
            logger.debug(f"Applied fee: {fee} {fee_asset}")

        # Update base currency balance
        base_balance = self._balances.get(
            base_asset, Balance(asset=base_asset, total=Decimal("0"), available=Decimal("0"))
        )
        if trade.side == OrderSide.BUY:
            # Ensure quantity and balances are not None
            if trade.quantity is not None and base_balance.total is not None:
                base_balance.total += trade.quantity
            if trade.quantity is not None and base_balance.available is not None:
                base_balance.available += trade.quantity  # Simplified
        elif trade.side == OrderSide.SELL:
            # Ensure quantity and balances are not None
            if trade.quantity is not None and base_balance.total is not None:
                base_balance.total -= trade.quantity
            if trade.quantity is not None and base_balance.available is not None:
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
                    # Ensure calculate_unrealized_pnl handles potential None entry_price defensively
                    if existing_position.entry_price is not None:
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
            new_position = Position(
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
        orders_to_remove: list[str] = []
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

    async def get_market_data(  # Corrected signature and return type
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[MarketData]:
        """Return mock market data."""
        self._check_error("get_market_data")
        await self._simulate_latency()
        # Placeholder: Return empty list matching list[MarketData]
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
            key=lambda o: o.time or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        if symbol:
            history = [o for o in history if o.symbol == symbol]
        return history[:limit]

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Return mock trade history."""
        self._check_error("get_trade_history")
        await self._simulate_latency()
        history = sorted(self._trades, key=lambda t: t.timestamp, reverse=True)
        if symbol:
            history = [t for t in history if t.symbol == symbol]
        return history[:limit]

    def parse_account_update_message(  # Corrected return type again based on base class
        self, message: dict[str, Any]
    ) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
        """Parse account update message (placeholder)."""
        # Placeholder: Return None, None as per signature. Real parsing needed if used.
        # If parsing were implemented, raise ValueError on failure.
        return None, None

    def parse_balance(
        self, data: Any
    ) -> Balance:  # Ensure implementation raises on failure, not returns None
        """Parse balance data (placeholder)."""
        try:
            # Attempt to create Balance, assuming data is a dict-like structure
            return Balance(**data)
        except Exception as e:
            logger.error(f"Mock parse_balance failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_balance failed: {e}") from e

    def parse_funding_rate(
        self, data: Any
    ) -> FundingRate:  # Ensure implementation raises on failure
        """Parse funding rate data (placeholder)."""
        try:
            # Assuming data is dict-like
            return FundingRate(**data)
        except Exception as e:
            logger.error(f"Mock parse_funding_rate failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_funding_rate failed: {e}") from e

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        """Parse funding rate message (placeholder)."""
        # Depends heavily on exchange message format
        return None

    def parse_order(self, data: Any) -> Order:  # Ensure implementation raises on failure
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
                "avg_fill_price",
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
            for field in ["time"]:  # Removed last_update_time
                if field in data and data[field] is not None:
                    # Assuming datetime object or compatible string/int
                    try:
                        if isinstance(data[field], (int, float)):
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
        except Exception as e:
            logger.error(f"Mock parse_order failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_order failed: {e}") from e

    def parse_order_book(
        self, data: Any, symbol: str
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
        except Exception as e:
            logger.error(f"Mock parse_order_book failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_order_book failed: {e}") from e

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """Parse order update message (placeholder)."""
        # Depends heavily on exchange message format
        # Should ideally call self.parse_order if data structure matches
        try:
            # Example: Assuming message contains the order data directly
            if "order_data" in message:
                return self.parse_order(message["order_data"])
            return None  # Or raise if format is unknown/invalid
        except Exception as e:
            logger.error(f"Failed to parse order update message: {message}, Error: {e}")
            return None  # Keep returning None for WS messages if parsing fails

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        """Parse order book message (placeholder)."""
        # Depends heavily on exchange message format
        # Should ideally call self.parse_order_book if data structure matches
        try:
            if "symbol" in message and ("bids" in message or "asks" in message):
                return self.parse_order_book(message, message["symbol"])
            return None
        except Exception as e:
            logger.error(f"Failed to parse orderbook message: {message}, Error: {e}")
            return None  # Keep returning None for WS messages

    def parse_position(self, data: Any) -> Position:  # Ensure implementation raises on failure
        """Parse position data (placeholder)."""
        try:
            # Assuming data is dict-like
            if "side" in data and isinstance(data["side"], str):
                data["side"] = OrderSide(data["side"])
            # Convert numeric fields
            for field in [
                "size",
                "entry_price",
                "mark_price",
                "liquidation_price",
                "leverage",
                "unrealized_pnl",
                "realized_pnl",
                "margin_used",
                "pnl",
                "close_price",
            ]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert position field '{field}' value '{data[field]}' to Decimal."
                        )
                        raise ValueError(f"Invalid Decimal value for field '{field}'")
            # Convert timestamps (Position model doesn't have timestamp directly)
            # if "timestamp" in data and data["timestamp"] is not None:
            #      try:
            #          ts_sec = int(data["timestamp"]) / 1000
            #          data["timestamp"] = datetime.fromtimestamp(ts_sec, tz=UTC)
            #      except (ValueError, TypeError, OSError):
            #          logger.error(f"Could not convert position timestamp '{data['timestamp']}'")
            #          data["timestamp"] = None
            if "close_time" in data and data["close_time"] is not None:
                try:
                    # Attempt parsing assuming ISO format string
                    data["close_time"] = datetime.fromisoformat(
                        data["close_time"].replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    logger.error(f"Could not parse close_time '{data['close_time']}'")
                    data["close_time"] = None

            return Position(**data)
        except Exception as e:  # Outer exception block
            logger.error(f"Mock parse_position failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_position failed: {e}") from e

    def parse_ticker(
        self, data: Any, symbol: str
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
        except Exception as e:
            logger.error(f"Mock parse_ticker failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_ticker failed: {e}") from e

    def parse_ticker_message(self, message: dict[str, Any]) -> tuple[str, Ticker] | Ticker | None:
        """Parse ticker message (placeholder)."""
        # Depends heavily on exchange message format
        # Might return Ticker directly or (symbol, Ticker) tuple
        return None

    def parse_trade(
        self, data: Any, symbol: str
    ) -> Trade:  # Ensure implementation raises on failure
        """Parse trade data (placeholder)."""
        try:
            # Assuming data is dict-like
            if "side" in data and isinstance(data["side"], str):
                data["side"] = OrderSide(data["side"])
            # Convert numeric fields
            for field in ["price", "quantity", "fee", "cost"]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert trade field '{field}' value '{data[field]}' to Decimal."
                        )
                        # Raise error instead of returning None
                        raise ValueError(
                            f"Could not convert trade field '{field}' value '{data[field]}' to Decimal."
                        )
            # Convert timestamp
            if "timestamp" in data and data["timestamp"] is not None:
                # Assuming int ms timestamp
                try:
                    # Ensure it's converted to int
                    data["timestamp"] = int(data["timestamp"])
                except (ValueError, TypeError):
                    logger.error(
                        f"Could not convert trade timestamp '{data['timestamp']}' to int ms"
                    )
                    # Raise error if timestamp is mandatory and invalid
                    raise ValueError(f"Invalid timestamp format for trade: {data['timestamp']}")

            # Remove 'datetime' if it exists from previous attempts, Trade expects 'timestamp' (int)
            data.pop("datetime", None)

            return Trade(**data)
        except Exception as e:  # Outer exception block
            logger.error(f"Mock parse_trade failed for data: {data}. Error: {e}")
            raise ValueError(f"Mock parse_trade failed: {e}") from e

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
    def _update_rate_limit_from_headers(self, headers: Any) -> None:
        """Mock implementation - does nothing."""
        logger.debug(
            f"Mock {self.exchange_name}: _update_rate_limit_from_headers called (no-op) with headers: {headers}"
        )
        # No actual rate limiting logic needed for the mock
