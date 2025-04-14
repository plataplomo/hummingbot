import asyncio
import copy
import logging
import time
from abc import abstractmethod  # Added import
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    TimeInForce,  # Added import
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
        """Simulate authentication - return empty dict."""
        self._check_error("_authenticate")
        await self._simulate_latency()
        return {}

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Simulate routing WebSocket messages."""
        self._check_error("_route_ws_message")
        await self._simulate_latency()
        # In a real mock, parse message type and call handlers
        pass

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Simulate subscribing to a topic."""
        self._check_error("subscribe")
        await self._simulate_latency()
        # Store handler if needed for simulated pushes
        pass

    async def _resubscribe(self) -> None:
        """Simulate resubscribing after reconnect."""
        self._check_error("_resubscribe")
        await self._simulate_latency()
        pass

    async def get_ticker(self, symbol: str) -> Ticker | None:  # type: ignore[override] # Mock allows None return
        """Return a predefined mock ticker."""
        self._check_error("get_ticker")
        await self._simulate_latency()
        logger.debug(f"[DEBUG] Mock {self.exchange_name}: get_ticker called for symbol '{symbol}'.")
        logger.debug(
            f"[DEBUG] Mock {self.exchange_name}: Current _mock_tickers keys: {list(self._mock_tickers.keys())}"
        )
        logger.debug(f"Mock {self.exchange_name}: Getting ticker for {symbol}")
        return self._mock_tickers.get(symbol)

    async def get_order_book(
        self, symbol: str, depth: int | None = None
    ) -> OrderBook:  # Removed | None
        """Return a predefined mock order book (basic implementation)."""
        self._check_error("get_order_book")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order book for {symbol}")
        # Return a dummy OrderBook or None
        return OrderBook(
            symbol=symbol, bids=[], asks=[], timestamp=int(datetime.now(UTC).timestamp() * 1000)
        )

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Return mock recent trades."""
        self._check_error("get_recent_trades")
        await self._simulate_latency()
        # Basic implementation, return empty list
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
        return copy.deepcopy(self._balances)

    # Aligned signature with base class, ignoring stricter base return type for mock
    async def get_positions(self, symbol: str | None = None) -> list[Position] | None:  # type: ignore[override]
        """
        Return mock positions, optionally filtered by symbol.
        Handles mapping internal symbols if full_config is available.
        Returns None if symbol is requested but not found (for testing).
        """
        self._check_error("get_positions")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting positions. Requested symbol: {symbol}")

        # Return a deep copy to prevent external modification
        all_positions = copy.deepcopy(list(self._positions.values()))

        if not symbol:
            logger.debug(f"Returning all {len(all_positions)} positions.")
            return all_positions  # Return list[Position]

        # If a symbol is provided, filter the positions
        # Handle potential symbol mapping (e.g., BTC -> BTC-PERP)
        target_exchange_symbol: str = symbol
        if self.full_config:
            # Assume input 'symbol' is the internal symbol
            target_exchange_symbol = self.full_config.get(
                f"exchanges.{self.exchange_name}.symbols.{symbol}",
                symbol,  # Default to the symbol itself if no mapping
            )

        logger.debug(f"Filtering positions for exchange symbol: {target_exchange_symbol}")

        filtered_positions = [pos for pos in all_positions if pos.symbol == target_exchange_symbol]

        if not filtered_positions:
            logger.debug(f"No position found for symbol {target_exchange_symbol}. Returning None.")
            return None  # Return None if specific symbol not found

        logger.debug(f"Returning {len(filtered_positions)} filtered positions.")
        return filtered_positions  # Return list[Position]

    # Aligned signature with base class
    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,  # Changed from float
        time_in_force: TimeInForce,  # Changed from str | None
        price: Decimal | None = None,  # Changed from float | None
        client_order_id: str | None = None,
        reduce_only: bool = False,  # Changed default, removed None
        post_only: bool = False,  # Added missing argument
        # Removed **kwargs
    ) -> Order:
        """Simulate placing an order."""
        self._check_error("place_order")
        await self._simulate_latency()

        # Basic validation
        if quantity <= Decimal("0"):
            raise APIError("Quantity must be positive", APIErrorCode.INVALID_PARAMS)
        if order_type == OrderType.LIMIT and price is None:
            raise APIError("Price is required for limit orders", APIErrorCode.INVALID_PARAMS)
        if price is not None and price <= Decimal("0"):
            raise APIError("Price must be positive", APIErrorCode.INVALID_PARAMS)

        order_id = f"{self.exchange_name}-order-{self._order_id_counter}"
        self._order_id_counter += 1

        # --- Determine Fill Price and Status ---
        fill_price: Decimal | None = None
        status: OrderStatus = OrderStatus.NEW  # Default

        # --- MODIFIED: Determine fill price based on order type ---
        if order_type == OrderType.MARKET:
            # Get the exchange-specific symbol
            # Assuming a simple mapping for mock: internal symbol maps directly
            exchange_symbol = symbol  # In real API, might need lookup config.get(f'exchanges.{self.exchange_name}.symbols.{symbol}')
            mock_ticker = self._mock_tickers.get(exchange_symbol)
            if mock_ticker:
                # Ensure prices are Decimal before comparison
                mock_ask = mock_ticker.ask if mock_ticker.ask is not None else Decimal("inf")
                mock_bid = mock_ticker.bid if mock_ticker.bid is not None else Decimal("-inf")
                mock_price = mock_ticker.price if mock_ticker.price is not None else None

                if side == OrderSide.BUY:
                    # Use ask if available and positive, otherwise fallback to price
                    fill_price = mock_ask if mock_ask > 0 else mock_price
                else:  # SELL
                    # Use bid if available and positive, otherwise fallback to price
                    fill_price = mock_bid if mock_bid > 0 else mock_price

                if fill_price is None:
                    logger.warning(
                        f"Could not determine fill price for MARKET order {symbol}, using limit price if available."
                    )
                    fill_price = price  # Fallback to limit price if provided, else None
            else:
                logger.warning(
                    f"No mock ticker found for {exchange_symbol}, cannot determine market fill price."
                )
                fill_price = price  # Fallback to limit price if provided

            # Market orders are typically filled immediately in this mock
            status = OrderStatus.FILLED
        elif order_type == OrderType.LIMIT:
            fill_price = price  # Use provided limit price
            status = OrderStatus.NEW  # Correct: Limit orders start as NEW
        # ------------------------------------------------------

        # Create the initial order object
        order = Order(
            id=order_id,
            client_order_id=client_order_id or "",  # Provide default empty string
            symbol=symbol,
            price=price,  # Use the original limit price (or None for market)
            quantity=quantity,
            filled_quantity=Decimal("0.0"),  # Start with zero filled
            side=side,
            type=order_type,
            status=status,  # Initial status (NEW or FILLED for market)
            time=datetime.now(UTC),  # Use current time
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            leverage=None,  # Leverage might be set later or is position-specific
        )

        # --- Apply Fill Behavior ---
        final_filled_quantity = Decimal("0.0")  # Use Decimal
        final_status = status  # Default to initial status (NEW for limit, FILLED for market)
        trade_to_record = None  # Only create a trade if filled
        avg_fill_price = fill_price if fill_price is not None else None  # Use determined fill_price

        # Determine fee rate (use taker fee for market/immediate fills, maker otherwise - simplified)
        # Ensure fee rate is Decimal
        trade_fee_rate = self.taker_fee if final_status == OrderStatus.FILLED else self.maker_fee
        # trade_fee_rate is already Decimal due to assignment on line 406

        # --- MODIFIED: Only apply special behavior if NOT reduce_only ---
        if not reduce_only:
            if self._open_orders_behavior == "fill_immediately":
                # Simulate immediate fill for all orders if requested
                final_status = OrderStatus.FILLED
                final_filled_quantity = quantity  # Use Decimal quantity
                # Update order object directly
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.avg_fill_price = avg_fill_price  # Ensure fill price is set
                logger.debug(f"Mock {self.exchange_name}: Immediately filling order {order_id}")

            elif self._open_orders_behavior == "partial_fill" and order_type == OrderType.MARKET:
                # Simulate partial fill for market orders if requested
                final_filled_quantity = quantity / Decimal("2")  # Fill half, ensure Decimal
                if final_filled_quantity > Decimal("0"):
                    final_status = OrderStatus.PARTIALLY_FILLED
                else:
                    final_status = OrderStatus.NEW  # If half is zero, remains new
                # Update order object directly
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.avg_fill_price = avg_fill_price  # Ensure fill price is set
                logger.debug(
                    f"Mock {self.exchange_name}: Partially filling order {order_id} ({final_filled_quantity}/{quantity})"
                )

            elif order_type == OrderType.MARKET:
                # Default market order fill (full)
                final_status = OrderStatus.FILLED
                final_filled_quantity = quantity  # Use Decimal quantity
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.avg_fill_price = avg_fill_price
                logger.debug(f"Mock {self.exchange_name}: Fully filling market order {order_id}")

            # else: Limit orders remain NEW unless fill_immediately is set

        # --- Handle reduce_only orders (assume they fill immediately) ---
        elif reduce_only:
            final_status = OrderStatus.FILLED
            final_filled_quantity = quantity
            order.status = final_status
            order.filled_quantity = final_filled_quantity
            order.avg_fill_price = avg_fill_price  # Use the determined fill price
            logger.debug(
                f"Mock {self.exchange_name}: Immediately filling reduce_only order {order_id}"
            )
            # Typically reduce_only uses TAKER fee
            trade_fee_rate = self.taker_fee  # Already Decimal

        # --- Update balances and positions IF the order was filled/partially filled ---
        if final_filled_quantity > Decimal("0") and avg_fill_price is not None:
            trade_fee = final_filled_quantity * avg_fill_price * trade_fee_rate
            trade_cost = (
                final_filled_quantity * avg_fill_price
            )  # Cost before fee adjustment based on side

            # Create Trade object
            trade_to_record = Trade(
                id=f"trade-{order_id}-{int(time.time() * 1e6)}",  # Unique trade ID
                symbol=symbol,
                timestamp=int(datetime.now(UTC).timestamp() * 1000),
                price=avg_fill_price,
                quantity=final_filled_quantity,
                side=side,
                order_id=order_id,
                exchange=self.exchange_name,
                fee=trade_fee,
                fee_asset=self.fee_asset,
                is_maker=(trade_fee_rate == self.maker_fee),  # Simplified maker check
                client_order_id=client_order_id,
                cost=trade_cost,  # Store cost before fee adjustment
            )
            self._trades.append(trade_to_record)
            self._update_balance_and_position(trade_to_record)  # Update internal state

        # Store the order
        self._orders[order_id] = order
        logger.debug(f"Mock {self.exchange_name}: Placed order: {order}")
        return order

    async def cancel_order(
        self,
        order_id: str,
        symbol: str | None = None,  # Added symbol for consistency
    ) -> dict[str, Any]:
        """Simulate cancelling an order."""
        self._check_error("cancel_order")
        await self._simulate_latency()
        order = self._orders.get(order_id)
        if order:
            if order.status in [OrderStatus.NEW, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                order.status = OrderStatus.CANCELED
                logger.info(f"Mock {self.exchange_name}: Cancelled order {order_id}")
                # Return structure similar to Backpack
                return {
                    "status": "success",
                    "cancelledOrderId": order_id,
                    "cancelledOrders": [order.to_dict()],  # Return cancelled order details
                }
            else:
                msg = f"Order {order_id} cannot be cancelled in status {order.status}"
                logger.warning(msg)
                raise APIError(msg, APIErrorCode.ORDER_REJECTED)
        else:
            msg = f"Order {order_id} not found for cancellation"
            logger.warning(msg)
            raise APIError(msg, APIErrorCode.ORDER_NOT_FOUND)

    async def get_order(self, order_id: str) -> Order | None:
        """Return a specific order by ID."""
        self._check_error("get_order")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order {order_id}")
        return self._orders.get(order_id)

    async def get_order_status(self, order_id: str, **kwargs: Any) -> Order | None:  # type: ignore[override] # Mock allows None return
        """Get the status of a specific order by ID."""
        self._check_error("get_order_status")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order status for {order_id}")
        return self._orders.get(order_id)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:  # type: ignore[override] # Mock returns simpler type
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error("get_open_orders")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting open orders (symbol: {symbol})")
        open_orders = [
            o
            for o in self._orders.values()
            if o.status in [OrderStatus.NEW, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
        ]
        if symbol:
            open_orders = [o for o in open_orders if o.symbol == symbol]
        return open_orders

    # --- Mock Data Setup Methods ---

    def set_mock_ticker(self, ticker: Ticker) -> None:
        """Set a predefined ticker for a symbol."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock ticker for {ticker.symbol}: {ticker}"
        )
        self._mock_tickers[ticker.symbol] = ticker

    def set_mock_funding_rate(self, funding_rate: FundingRate) -> None:
        """Set a predefined funding rate for a symbol."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock funding rate for {funding_rate.symbol}: {funding_rate}"
        )
        self._mock_funding_rates[funding_rate.symbol] = funding_rate

    def set_mock_balance(self, balance_data: Balance | dict[str, Any]) -> None:
        """Set a predefined balance for an asset."""
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
        else:
            logger.error(f"Invalid type for balance_data: {type(balance_data)}")
            return

        if asset and balance_obj:
            self._balances[asset] = balance_obj
        else:
            logger.error("Could not determine asset or create Balance object.")

    def set_mock_position(self, position: Position) -> None:
        """Set a predefined position for a symbol."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock position for {position.symbol}: {position}"
        )
        self._positions[position.symbol] = position

    def set_open_orders_behavior(self, behavior: str) -> None:
        """Set how open orders are handled ('keep_open', 'fill_immediately', 'partial_fill')."""
        if behavior in ["keep_open", "fill_immediately", "partial_fill"]:
            self._open_orders_behavior = behavior
        else:
            logger.warning(f"Invalid open orders behavior: {behavior}. Using 'keep_open'.")
            self._open_orders_behavior = "keep_open"

    def set_latency(self, latency_ms: float) -> None:
        """Set simulated network latency."""
        self._latency_ms = max(0, latency_ms)

    def reset(self) -> None:
        """Reset the mock state."""
        self._order_id_counter = 1
        self._orders.clear()
        self._positions.clear()
        self._balances.clear()
        self._mock_tickers.clear()
        self._mock_funding_rates.clear()
        self._trades.clear()
        self.clear_error()  # Clear error simulations
        self._fail_on_method = None
        self._open_orders_behavior = "default"
        logger.info(f"MockExchangeAPI {self.exchange_name} reset.")
        self._positions = {}

    async def fetch_ticker(self, symbol: str) -> Ticker:
        if self._fail_on_method == "fetch_ticker":
            raise self._failure_exception
        if self._mock_tickers and self._mock_tickers.get(symbol):
            return self._mock_tickers[symbol]
        # Simulate fetching if no specific mock is set
        return Ticker(
            symbol=symbol,
            bid=Decimal("9990.0"),  # Convert to Decimal
            ask=Decimal("10010.0"),  # Convert to Decimal
            # Removed last_price argument
            timestamp=int(time.time() * 1000),
        )

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        if self._fail_on_method == "fetch_funding_rate":
            raise self._failure_exception
        if self._mock_funding_rates and self._mock_funding_rates.get(symbol):
            return self._mock_funding_rates[symbol]
        # Simulate fetching if no specific mock is set
        return FundingRate(
            symbol=symbol,
            funding_rate=Decimal("0.0001"),
            next_funding_time=int((time.time() + 3600) * 1000),  # Example: 1 hour later
            timestamp=int(time.time() * 1000),
        )

    async def fetch_balances(self) -> dict[str, Balance]:
        if self._fail_on_method == "fetch_balances":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate latency
        return self._balances.copy()

    async def fetch_positions(self) -> dict[str, Position]:
        if self._fail_on_method == "fetch_positions":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate network latency
        return self._positions.copy()

    def _update_balance_and_position(self, trade: Trade):
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
        # Ensure price and quantity are Decimal before calculating cost
        if trade.price is None or trade.quantity is None:
            logger.error(
                f"Trade {trade.id} is missing price or quantity, cannot update balance/position."
            )
            return
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
            # Ensure required fields are not None
            if (
                existing_position.size is None
                or existing_position.entry_price is None
                or trade.quantity is None
                or trade.price is None
            ):
                logger.error(
                    f"Cannot update position for trade {trade.id} due to missing critical data."
                )
                return

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
                existing_position.timestamp = trade.timestamp  # Update timestamp from trade
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
            # Ensure required fields for Position are not None
            if trade.side is None or trade.quantity is None or trade.price is None:
                logger.error(
                    f"Cannot create new position from trade {trade.id} due to missing side, quantity, or price."
                )
                return

            # Restore missing fields based on Position definition
            new_position = Position(
                symbol=trade.symbol,
                side=trade.side,  # Asserted not None above
                size=trade.quantity,  # Asserted not None above
                entry_price=trade.price,  # Asserted not None above
                leverage=Decimal("1"),  # Default leverage
                timestamp=trade.timestamp,  # Use trade timestamp
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

    def get_trades(self) -> list[Trade]:
        """Return the recorded trade history."""
        return copy.deepcopy(self._trades)

    # --- WebSocket Simulation ---
    # Basic stubs, can be expanded for more complex WS testing

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        # In a real mock, this would parse and route messages
        pass

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """Simulate cancelling all orders."""
        self._check_error("cancel_all_orders")
        await self._simulate_latency()
        cancelled_ids = []
        orders_to_remove = []
        for order_id, order in self._orders.items():
            if symbol is None or order.symbol == symbol:
                if order.status in [
                    OrderStatus.NEW,
                    OrderStatus.OPEN,
                    OrderStatus.PARTIALLY_FILLED,
                ]:
                    order.status = OrderStatus.CANCELED
                    cancelled_ids.append(order_id)
                # Keep completed/failed orders in history unless explicitly cleared
                # orders_to_remove.append(order_id) # Decide if cancelled orders are removed

        # for order_id in orders_to_remove:
        #     del self._orders[order_id]

        logger.info(f"Mock {self.exchange_name}: Cancelled orders: {cancelled_ids}")
        return {"status": "success", "cancelled_orders": cancelled_ids}

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

    async def get_market_data(self, symbol: str) -> dict[str, Any]:
        """Return mock market data (placeholder)."""
        self._check_error("get_market_data")
        await self._simulate_latency()
        # Return a basic dict, real implementation would fetch OHLCV
        ticker = self._mock_tickers.get(symbol)
        return {
            "symbol": symbol,
            "last_price": str(ticker.price) if ticker and ticker.price is not None else "N/A",
        }

    def get_message_type(self, message: dict[str, Any]) -> str | None:
        """Determine message type (placeholder)."""
        return message.get("type") or message.get("e")  # Common patterns

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

    def parse_account_update_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """Parse account update message (placeholder)."""
        # Expects specific structure based on exchange
        return None

    def parse_balance(self, data: Any) -> Balance | None:
        """Parse balance data (placeholder)."""
        try:
            # Attempt to create Balance, assuming data is a dict-like structure
            return Balance(**data)
        except Exception:
            return None

    def parse_funding_rate(self, data: Any) -> FundingRate | None:
        """Parse funding rate data (placeholder)."""
        try:
            # Assuming data is dict-like
            return FundingRate(**data)
        except Exception:
            return None

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        """Parse funding rate message (placeholder)."""
        # Depends heavily on exchange message format
        return None

    def parse_order(self, data: Any) -> Order | None:
        """Parse order data (placeholder)."""
        try:
            # Assuming data is dict-like
            # Need to handle enums correctly if they are strings in data
            if "side" in data and isinstance(data["side"], str):
                data["side"] = OrderSide(data["side"])
            if "type" in data and isinstance(data["type"], str):
                data["type"] = OrderType(data["type"])
            if "status" in data and isinstance(data["status"], str):
                data["status"] = OrderStatus(data["status"])
            if "time_in_force" in data and isinstance(data["time_in_force"], str):
                data["time_in_force"] = TimeInForce(data["time_in_force"])
            return Order(**data)
        except Exception as e:
            logger.error(f"Failed to parse order data: {data}, Error: {e}")
            return None

    def parse_order_book(self, data: Any) -> OrderBook | None:
        """Parse order book data (placeholder)."""
        try:
            # Assuming data is dict-like
            return OrderBook(**data)
        except Exception:
            return None

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """Parse order update message (placeholder)."""
        # Depends heavily on exchange message format
        # Often contains data similar to what parse_order expects
        order_data = message.get("data")  # Example structure
        if order_data:
            return self.parse_order(order_data)
        return None

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        """Parse order book message (placeholder)."""
        # Depends heavily on exchange message format
        return None

    def parse_position(self, data: Any) -> Position | None:
        """Parse position data (placeholder)."""
        try:
            # Assuming data is dict-like
            if "side" in data and isinstance(data["side"], str):
                data["side"] = OrderSide(data["side"])
            # Convert numeric fields from string if necessary
            for field in [
                "size",
                "entry_price",
                "leverage",
                "mark_price",
                "liquidation_price",
                "unrealized_pnl",
                "realized_pnl",
                "margin_used",
                "close_price",
                "pnl",
            ]:
                if field in data and data[field] is not None:
                    try:
                        data[field] = Decimal(str(data[field]))
                    except InvalidOperation:
                        logger.error(
                            f"Could not convert position field '{field}' value '{data[field]}' to Decimal."
                        )
                        return None  # Or handle error differently
            # Convert timestamp if needed
            if "timestamp" in data and data["timestamp"] is not None:
                # Assuming timestamp is int (ms or s) - needs clarification
                pass  # Conversion logic depends on source format
            if "close_time" in data and isinstance(data["close_time"], str):
                try:
                    data["close_time"] = datetime.fromisoformat(data["close_time"])
                except ValueError:
                    logger.error(f"Could not parse close_time '{data['close_time']}'")
                    data["close_time"] = None

            return Position(**data)
        except Exception as e:
            logger.error(f"Failed to parse position data: {data}, Error: {e}")
            return None

    def parse_ticker(self, data: Any) -> Ticker | None:
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
                        return None
            return Ticker(**data)
        except Exception:
            return None

    def parse_ticker_message(self, message: dict[str, Any]) -> tuple[str, Ticker] | Ticker | None:
        """Parse ticker message (placeholder)."""
        # Depends heavily on exchange message format
        # Might return Ticker directly or (symbol, Ticker) tuple
        return None

    def parse_trade(self, data: Any) -> Trade | None:
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
                        return None
            # Convert timestamp
            if "timestamp" in data and data["timestamp"] is not None:
                # Assuming int ms timestamp
                try:
                    ts_sec = int(data["timestamp"]) / 1000
                    data["datetime"] = datetime.fromtimestamp(ts_sec, tz=UTC)
                except (ValueError, TypeError, OSError):
                    logger.error(f"Could not convert trade timestamp '{data['timestamp']}'")
                    data["datetime"] = None

            return Trade(**data)
        except Exception as e:
            logger.error(f"Failed to parse trade data: {data}, Error: {e}")
            return None

    def parse_trade_message(self, message: dict[str, Any]) -> list[Trade] | Trade | None:
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
        """Simulate subscribing to order book updates."""
        self._check_error("subscribe_to_order_book")
        await self._simulate_latency()
        logger.info(
            f"Mock {self.exchange_name}: Subscribed to order book for {symbol} (simulated)."
        )

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Simulate subscribing to ticker updates."""
        self._check_error("subscribe_to_ticker")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to ticker for {symbol} (simulated).")

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Simulate subscribing to public trade updates."""
        self._check_error("subscribe_to_trades")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Subscribed to trades for {symbol} (simulated).")

    # Add dummy implementation for the new abstract method
    @abstractmethod  # Keep abstract as mock doesn't need specific logic
    def _update_rate_limit_from_headers(self, headers: Any, method: str, path: str) -> None:
        pass  # Mock does not need to implement this
