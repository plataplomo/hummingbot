import asyncio
import copy
import logging
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,  # ExchangeID, # REMOVE ExchangeID from here
    # Symbol      # REMOVE Symbol from here
    OrderType,
    Position,
    Ticker,
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

    async def _simulate_latency(self):
        """Simulate network latency."""
        if self._latency_ms > 0:
            await asyncio.sleep(self._latency_ms / 1000.0)

    def configure_error(
        self,
        method_name: str,
        error_code: APIErrorCode,
        message: str,
        trigger_after_n_calls: int | None = 0,
    ):
        """Configure an APIError to be raised by a specific method."""
        error = APIError(message=message, code=error_code, exchange_code=self.exchange_name)
        self._error_config[method_name] = (error, trigger_after_n_calls)
        self._call_counts[method_name] = 0  # Reset count when configuring

    def set_error_simulation(
        self,
        error: Exception,
        method_name: str,
        trigger_after_n_calls: int | None = 0,
    ):
        """Configure a specific exception to be raised by a method."""
        self._error_config[method_name] = (error, trigger_after_n_calls)
        self._call_counts[method_name] = 0

    def clear_error(self, method_name: str | None = None):
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

    def _check_error(self, method_name: str):
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

    def configure_failure(self, method_name: str | None = None, exception: Exception | None = None):
        """Configure the mock to fail on a specific method call."""
        self._fail_on_method = method_name
        if exception:
            self._failure_exception = exception
        else:
            self._failure_exception = MockAPIError(f"Simulated API failure on {method_name}")

    def reset_failure(self):
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
        """Mock authentication - always succeeds."""
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Authenticating request {method} {path}")
        # Return dummy headers or signature info if needed by _request implementation
        return {"mock_auth_header": "valid"}

    async def _route_ws_message(self, message: dict[str, Any]):
        """Route mock WebSocket messages (implementation needed)."""
        logger.debug(f"Mock {self.exchange_name}: Received WS message: {message}")
        # Basic routing logic can be added here based on message type
        pass

    async def subscribe(self, topic: str, handler: MessageHandler):
        """Mock subscription - stores handler."""
        logger.info(f"Mock {self.exchange_name}: Subscribing to {topic}")
        self._ws_handlers[topic] = handler
        # In a real mock, you might start pushing simulated data for this topic

    async def _resubscribe(self):
        """Mock resubscription after reconnect."""
        logger.info(f"Mock {self.exchange_name}: Re-subscribing to {len(self._ws_handlers)} topics")
        # Simulate re-sending subscription requests if needed

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Return a predefined mock ticker."""
        self._check_error("get_ticker")
        await self._simulate_latency()
        logger.debug(f"[DEBUG] Mock {self.exchange_name}: get_ticker called for symbol '{symbol}'.")
        logger.debug(
            f"[DEBUG] Mock {self.exchange_name}: Current _mock_tickers keys: {list(self._mock_tickers.keys())}"
        )
        logger.debug(f"Mock {self.exchange_name}: Getting ticker for {symbol}")
        return self._mock_tickers.get(symbol)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Return a predefined mock order book (basic implementation)."""
        self._check_error("get_order_book")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order book for {symbol}")
        # Return a dummy OrderBook or None
        return OrderBook(
            symbol=symbol, bids=[], asks=[], timestamp=int(datetime.now(UTC).timestamp() * 1000)
        )

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Return an empty list of recent trades."""
        self._check_error("get_recent_trades")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting recent trades for {symbol}")
        return []

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Return a predefined mock funding rate."""
        self._check_error("get_funding_rate")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting funding rate for {symbol}")
        return self._mock_funding_rates.get(symbol)

    async def get_balances(self) -> dict[str, Balance]:
        """Return predefined mock balances."""
        self._check_error("get_balances")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting balances")
        return self._balances

    async def get_positions(self, symbols: list[str] | None = None) -> list[Position]:
        """
        Return mock positions, optionally filtered by symbol.
        Handles mapping internal symbols if full_config is available.
        (Fix 42)
        """
        self._check_error("get_positions")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting positions. Requested symbols: {symbols}")

        # Return a deep copy to prevent external modification
        all_positions = copy.deepcopy(list(self._positions.values()))

        if not symbols:
            logger.debug(f"Returning all {len(all_positions)} positions.")
            return all_positions

        # If symbols are provided, filter the positions
        # We need to handle potential symbol mapping (e.g., BTC -> BTC-PERP)
        requested_symbols_set: set[str] = set()
        if self.full_config:
            for internal_symbol in symbols:
                exchange_symbol = self.full_config.get(
                    f"exchanges.{self.exchange_name}.symbols.{internal_symbol}",
                    internal_symbol,  # Default to the symbol itself if no mapping
                )
                requested_symbols_set.add(exchange_symbol)
        else:
            requested_symbols_set = set(symbols)  # No mapping possible

        logger.debug(f"Filtering positions for exchange symbols: {requested_symbols_set}")

        filtered_positions = [pos for pos in all_positions if pos.symbol in requested_symbols_set]
        logger.debug(f"Returning {len(filtered_positions)} filtered positions.")
        return filtered_positions

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: float,
        price: float | None = None,
        client_order_id: str | None = None,
        time_in_force: str | None = None,  # Argument exists but not used by Order model
        reduce_only: bool | None = None,  # Argument exists but not used by Order model
        **kwargs: Any,  # Added type hint for kwargs
    ) -> Order:
        """Simulate placing an order."""
        self._check_error("place_order")
        await self._simulate_latency()
        logger.info(
            f"Mock {self.exchange_name}: Placing order: {symbol} {side.value} {quantity} @ {price if price else order_type.value}"
        )

        order_id = f"mock_{self.exchange_name}_{self._order_id_counter}"
        self._order_id_counter += 1

        # --- Simulate more realistic fill for market orders ---
        fill_price = price  # Default to limit price if provided
        status = OrderStatus.FILLED if order_type == OrderType.MARKET else OrderStatus.NEW

        if order_type == OrderType.MARKET:
            # Get the exchange-specific symbol
            # Assuming a simple mapping for mock: internal symbol maps directly
            exchange_symbol = symbol  # In real API, might need lookup config.get(f'exchanges.{self.exchange_name}.symbols.{symbol}')
            mock_ticker = self._mock_tickers.get(exchange_symbol)
            if mock_ticker:
                if side == OrderSide.BUY:
                    fill_price = mock_ticker.ask if mock_ticker.ask > 0 else mock_ticker.price
                else:  # SELL
                    fill_price = mock_ticker.bid if mock_ticker.bid > 0 else mock_ticker.price
            else:
                logger.warning(
                    f"Mock {self.exchange_name}: No ticker found for {exchange_symbol} to determine market fill price. Using default 40000.0"
                )
                fill_price = 40000.0  # Fallback if no ticker set
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Limit price must be provided for LIMIT orders")
            fill_price = price  # Use provided limit price
            status = OrderStatus.NEW  # Correct: Limit orders start as NEW
        # ------------------------------------------------------

        filled_quantity = quantity if status == OrderStatus.FILLED else 0.0

        order = Order(
            id=order_id,
            client_order_id=client_order_id or "",  # Provide default empty string
            symbol=symbol,
            price=Decimal(str(price)) if price is not None else None,  # Convert float to Decimal
            quantity=Decimal(str(quantity)),  # Convert float to Decimal
            filled_quantity=Decimal("0.0"),  # Use Decimal
            side=side,
            type=order_type,
            status=OrderStatus.NEW,
            time=int(time.time() * 1000),
        )
        # Store initial order state before potentially modifying for fill behavior
        self._orders[order_id] = copy.deepcopy(order)

        # --- Apply Fill Behavior ---
        final_filled_quantity = Decimal("0.0")  # Use Decimal
        final_status = status  # Default to initial status (NEW for limit, FILLED for market)
        trade_to_record = None  # Only create a trade if filled
        avg_fill_price = (
            Decimal(str(fill_price)) if fill_price is not None else None
        )  # Convert fill_price to Decimal

        # Determine fee rate (use taker fee for market/immediate fills, maker otherwise - simplified)
        # Ensure fee rate is Decimal
        trade_fee_rate = self.taker_fee if final_status == OrderStatus.FILLED else self.maker_fee
        if not isinstance(trade_fee_rate, Decimal):
            trade_fee_rate = Decimal(str(trade_fee_rate))

        # --- MODIFIED: Only apply special behavior if NOT reduce_only ---
        if not reduce_only:
            if self._open_orders_behavior == "fill_immediately":
                # Simulate immediate fill for all orders if requested
                final_status = OrderStatus.FILLED
                final_filled_quantity = Decimal(str(quantity))  # Use Decimal quantity
                # Update order object directly
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.price = avg_fill_price  # Ensure fill price is set
                logger.debug(f"Mock {self.exchange_name}: Immediately filling order {order_id}")

            elif self._open_orders_behavior == "partial_fill" and order_type == OrderType.MARKET:
                # Simulate partial fill for market orders if requested
                final_filled_quantity = Decimal(str(quantity)) / Decimal(
                    "2"
                )  # Fill half, ensure Decimal
                if final_filled_quantity > Decimal("0"):
                    final_status = OrderStatus.PARTIALLY_FILLED
                else:
                    final_status = OrderStatus.NEW  # If half is zero, remains new
                # Update order object directly
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.price = avg_fill_price  # Ensure fill price is set
                logger.debug(
                    f"Mock {self.exchange_name}: Partially filling order {order_id} ({final_filled_quantity}/{quantity})"
                )

            elif order_type == OrderType.MARKET:
                # Default market order fill (full)
                final_status = OrderStatus.FILLED
                final_filled_quantity = Decimal(str(quantity))  # Use Decimal quantity
                order.status = final_status
                order.filled_quantity = final_filled_quantity
                order.price = avg_fill_price
                logger.debug(f"Mock {self.exchange_name}: Fully filling market order {order_id}")

            # else: Limit orders remain NEW unless fill_immediately is set

        # --- Handle reduce_only orders (assume they fill immediately) ---
        elif reduce_only:
            final_status = OrderStatus.FILLED
            final_filled_quantity = Decimal(str(quantity))
            order.status = final_status
            order.filled_quantity = final_filled_quantity
            order.price = avg_fill_price  # Use the determined fill price
            logger.debug(
                f"Mock {self.exchange_name}: Immediately filling reduce_only order {order_id}"
            )
            # Typically reduce_only uses TAKER fee
            trade_fee_rate = self.taker_fee
            if not isinstance(trade_fee_rate, Decimal):
                trade_fee_rate = Decimal(str(trade_fee_rate))

        # --- Update balances and positions IF the order was filled/partially filled ---
        if final_filled_quantity > Decimal("0") and avg_fill_price is not None:
            try:
                # Create Trade object for balance update
                trade = Trade(
                    id=f"trade_{order_id}_{int(time.time() * 1000)}",
                    order_id=order_id,
                    exchange=self.exchange_name,  # Use exchange_name
                    symbol=symbol,
                    side=side,
                    quantity=final_filled_quantity,
                    price=avg_fill_price,
                    fee=abs(final_filled_quantity * avg_fill_price * trade_fee_rate),
                    fee_asset=self.fee_asset,
                    timestamp=int(datetime.now(UTC).timestamp() * 1000),
                )
                self._trades.append(trade)  # Store the trade
                trade_to_record = trade  # Assign for potential return/logging

                # Update internal state
                self._update_balance_and_position(trade)
                logger.debug(
                    f"Updated balance/position for trade {trade.id} on {self.exchange_name}"
                )

            except Exception as e:
                logger.error(
                    f"Error processing trade update for order {order_id} on {self.exchange_name}: {e}",
                    exc_info=True,
                )
                # Decide how to handle this - should the order placement fail?
                # For mock, maybe log and continue, but return original order state?
                # Revert order status if update failed?
                order.status = status  # Revert to original status before fill attempt
                order.filled_quantity = Decimal("0.0")
                # Consider raising a specific error here?
                raise MockAPIError(
                    f"Failed to update internal state after fill for order {order_id}: {e}"
                ) from e

        # Update the order in the store with its final state
        self._orders[order_id] = order

        logger.info(
            f"Mock {self.exchange_name}: Order {order_id} placed/updated. Final Status: {order.status.name}, Filled: {order.filled_quantity}"
        )
        # Return a copy to prevent external modification
        return copy.deepcopy(order)

    async def cancel_order(
        self, order_id: str, symbol: str | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Cancel an order by ID."""
        self._check_error("cancel_order")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Cancelling order {order_id}")

        if order_id in self._orders:
            order = self._orders[order_id]
            if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.NEW]:
                order.status = OrderStatus.CANCELED
                logger.debug(f"Mock {self.exchange_name}: Order {order_id} cancelled.")
                return {"success": True, "orderId": order_id}
            else:
                logger.warning(
                    f"Mock {self.exchange_name}: Order {order_id} cannot be cancelled (status: {order.status})"
                )
                raise APIError(
                    f"Order already {order.status.value}",
                    code=APIErrorCode.ORDER_NOT_FOUND,
                )
        else:
            logger.error(f"Mock {self.exchange_name}: Order {order_id} not found for cancellation.")
            raise APIError("Order not found", code=APIErrorCode.ORDER_NOT_FOUND)

    async def get_order(self, order_id: str) -> Order | None:
        """Return a specific order by ID."""
        self._check_error("get_order")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order {order_id}")
        return self._orders.get(order_id)

    async def get_order_status(self, order_id: str, **kwargs: Any) -> Order | None:
        """Get the status of a specific order by ID."""
        self._check_error("get_order_status")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order status for {order_id}")
        return self._orders.get(order_id)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error("get_open_orders")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting open orders (symbol: {symbol})")
        open_orders = [
            o
            for o in self._orders.values()
            if o.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
        ]
        if symbol:
            open_orders = [o for o in open_orders if o.symbol == symbol]
        return open_orders

    # --- Mock-specific methods for test setup ---

    def set_mock_ticker(self, ticker: Ticker):
        """Set a ticker value for the mock to return."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock ticker for {ticker.symbol}")
        self._mock_tickers[ticker.symbol] = ticker

    def set_mock_funding_rate(self, funding_rate: FundingRate):
        """Set a specific funding rate for testing."""
        if funding_rate.symbol:
            self._mock_funding_rates[funding_rate.symbol] = funding_rate

    def set_mock_balance(self, balance_data: Balance | dict[str, Any]):
        """Set or update mock balances."""
        balance_obj: Balance | None = None
        asset: str | None = None

        if isinstance(balance_data, Balance):
            balance_obj = copy.deepcopy(balance_data)
            asset = balance_obj.asset
            logger.debug(
                f"Mock {self.exchange_name}: Setting mock balance for {asset} using Balance object."
            )
        elif isinstance(balance_data, dict):
            # Handle dictionary input
            if (
                len(balance_data) == 1 and list(balance_data.keys())[0].isupper()
            ):  # Check if it's {'ASSET': {...}} format
                asset = list(balance_data.keys())[0]
                data = balance_data[asset]
                try:
                    balance_obj = Balance(
                        asset=asset,
                        total=Decimal(str(data.get("total", "0"))),
                        available=Decimal(str(data.get("available", data.get("total", "0")))),
                        free=Decimal(
                            str(data.get("free", data.get("available", data.get("total", "0"))))
                        ),
                        locked=Decimal(str(data.get("locked", "0"))),
                    )
                    logger.debug(
                        f"Mock {self.exchange_name}: Setting mock balance for {asset} using dict input."
                    )
                except (TypeError, KeyError, InvalidOperation) as e:
                    logger.error(f"Failed to parse nested balance dict: {balance_data}. Error: {e}")
                    return
            else:  # Assume it's a single Balance represented as a dict {'asset': 'USD', ...}
                try:
                    # Ensure all values needed by Balance are present and convertible
                    asset_val = balance_data.get("asset")
                    total_val = balance_data.get("total", "0")
                    available_val = balance_data.get("available", total_val)
                    free_val = balance_data.get("free", available_val)
                    locked_val = balance_data.get("locked", "0")

                    if asset_val is None:
                        raise ValueError("'asset' key missing from balance dict")

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
            logger.error(f"Could not determine asset or balance object from input: {balance_data}")

    def set_mock_position(self, position: Position):
        """Set a specific position for testing."""
        logger.debug(f"Mock {self.exchange_name}: Setting mock position for {position.symbol}")
        self._positions[position.symbol] = position

    def set_open_orders_behavior(self, behavior: str):
        """Set how open orders should behave (default, fill_immediately, partial_fill)."""
        if behavior not in ["default", "fill_immediately", "partial_fill"]:
            raise ValueError("Invalid open_orders_behavior")
        self._open_orders_behavior = behavior

    def set_latency(self, latency_ms: float):
        """Set the simulated latency for API calls."""
        self._latency_ms = max(0, latency_ms)
        logger.info(f"Mock {self.exchange_name} latency set to {self._latency_ms} ms")

    def reset(self):
        """Reset the mock state (orders, positions, etc.)."""
        self._order_id_counter = 1
        self._orders = {}
        self._positions = {}
        self._balances = {}
        self._mock_tickers = {}
        self._mock_funding_rates = {}
        self._latency_ms = 10.0
        self._error_simulation = None
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
            bid=9990.0,
            ask=10010.0,
            last_price=10000.0,
            timestamp=int(datetime.now(UTC).timestamp() * 1000),
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
            next_funding_time=int((datetime.now(UTC) + timedelta(hours=1)).timestamp() * 1000),
        )

    async def fetch_balances(self) -> dict[str, Balance]:
        if self._fail_on_method == "fetch_balances":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate network latency
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
        cost = trade.cost if trade.cost is not None else trade.price * trade.quantity
        fee = trade.fee if trade.fee is not None else Decimal("0")
        fee_asset = trade.fee_asset if trade.fee_asset is not None else self.fee_asset

        # Determine the asset involved (base or quote)
        # This is simplified; real perp/margin calculations are complex
        # Assuming simple spot-like balance update for now
        base_asset, quote_asset = self._split_symbol(trade.symbol)

        # Update quote currency balance (e.g., USD, USDC)
        quote_balance = self._balances.get(
            quote_asset, Balance(asset=quote_asset, total=Decimal("0"), available=Decimal("0"))
        )
        if trade.side == OrderSide.BUY:
            quote_balance.total -= cost
            quote_balance.available -= cost  # Assuming cost reflects available reduction
        elif trade.side == OrderSide.SELL:
            quote_balance.total += cost
            quote_balance.available += cost

        # Update fee asset balance
        if fee > 0:
            fee_balance = self._balances.get(
                fee_asset, Balance(asset=fee_asset, total=Decimal("0"), available=Decimal("0"))
            )
            fee_balance.total -= fee
            fee_balance.available -= fee
            self._balances[fee_asset] = fee_balance
            logger.debug(f"Applied fee: {fee} {fee_asset}")

        # Update base currency balance (crude for perps, more for spot)
        # This part might be less relevant for perps where position matters more
        base_balance = self._balances.get(
            base_asset, Balance(asset=base_asset, total=Decimal("0"), available=Decimal("0"))
        )
        if trade.side == OrderSide.BUY:
            base_balance.total += trade.quantity
            base_balance.available += trade.quantity  # Simplified
        elif trade.side == OrderSide.SELL:
            base_balance.total -= trade.quantity
            base_balance.available -= trade.quantity  # Simplified

        self._balances[quote_asset] = quote_balance
        self._balances[base_asset] = base_balance

        logger.debug(f"Updated balances: {self._balances}")

        # --- Position Update (Fix 41) ---
        existing_position = self._positions.get(trade.symbol)

        if existing_position:
            logger.debug(f"Updating existing position for {trade.symbol}: {existing_position}")
            # Calculate new average entry price and size
            if existing_position.side == trade.side:
                # Increasing position size
                new_size = existing_position.size + trade.quantity
                trade_fee = trade.fee if trade.fee is not None else Decimal("0")
                trade_cost_basis_adjustment = trade.price * trade.quantity
                if trade.side == OrderSide.BUY:
                    trade_cost_basis_adjustment += trade_fee
                else:  # SELL
                    trade_cost_basis_adjustment -= trade_fee

                if new_size == Decimal("0"):  # Avoid division by zero if size becomes exactly 0
                    new_avg_entry = Decimal("0.0")
                else:
                    # Calculate cost of existing position
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
                trade_fee = (
                    trade.fee if trade.fee is not None else Decimal("0")
                )  # Fee is realized on close/reduce
                if trade.quantity >= existing_position.size:
                    # Closing or flipping position
                    close_quantity = existing_position.size
                    # Calculate PNL considering fee
                    if existing_position.side == OrderSide.BUY:  # Selling to close LONG
                        pnl = (trade.price * close_quantity - trade_fee) - (
                            existing_position.entry_price * close_quantity
                        )
                    else:  # Buying to close SHORT
                        pnl = (existing_position.entry_price * close_quantity) - (
                            trade.price * close_quantity + trade_fee
                        )

                    # TODO: Add realized PNL tracking if needed (using self.portfolio_tracker._update_realized_pnl?)
                    logger.debug(
                        f"Position closed/flipped. Realized PNL (approx, incl. fee): {pnl}"
                    )

                    remaining_trade_qty = trade.quantity - existing_position.size
                    if remaining_trade_qty > Decimal(
                        "1e-12"
                    ):  # Flipped, tolerance for float issues
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
                    trade_fee = (
                        trade.fee if trade.fee is not None else Decimal("0")
                    )  # Fee is realized on reduce
                    # Calculate realized PNL for the reduced portion
                    if existing_position.side == OrderSide.BUY:  # Selling to reduce LONG
                        pnl = (trade.price * reduce_quantity - trade_fee) - (
                            existing_position.entry_price * reduce_quantity
                        )
                    else:  # Buying to reduce SHORT
                        pnl = (existing_position.entry_price * reduce_quantity) - (
                            trade.price * reduce_quantity + trade_fee
                        )

                    # TODO: Add realized PNL tracking if needed
                    logger.debug(f"Position size reduced. Realized PNL (approx, incl. fee): {pnl}")
                    existing_position.size -= reduce_quantity
                    # Entry price remains the same when reducing
                    logger.debug(f"Reduced position size. New Size: {existing_position.size}")

            # Update timestamp or other fields if needed (e.g., mark price from ticker)
            if existing_position:  # Check if not deleted
                existing_position.timestamp = trade.timestamp  # Update timestamp from trade
                # Update unrealized PNL if mark price is available
                ticker = self._mock_tickers.get(trade.symbol)
                if ticker:
                    existing_position.mark_price = ticker.price  # Use last trade price as mark
                    existing_position.calculate_unrealized_pnl(ticker.price)
                logger.debug(f"Updated position: {existing_position}")

        else:
            # Creating a new position
            new_position = Position(
                symbol=trade.symbol,
                side=trade.side,
                size=trade.quantity,
                entry_price=trade.price,
                leverage=Decimal("1"),
                timestamp=trade.timestamp,
                margin_type="cross",
                unrealized_pnl=Decimal("0.0"),
            )
            # Update unrealized PNL if mark price is available
            ticker = self._mock_tickers.get(trade.symbol)
            if ticker:
                new_position.mark_price = ticker.price  # Use last trade price as mark
                # PNL is zero at creation based on entry price, but mark might differ slightly
                new_position.calculate_unrealized_pnl(ticker.price)

            self._positions[trade.symbol] = new_position
            logger.debug(f"Created new position: {new_position}")

    def get_trades(self) -> list[Trade]:
        # This logic was incorrect, need to store trades separately
        # return [trade for trade in self._orders.values() if trade.status == OrderStatus.FILLED]
        # Assuming trades are stored elsewhere or this method needs removal/rework
        # For now, return empty to avoid crashing tests if called
        return []

    def get_orders(self) -> dict[str, Order]:
        return self._orders.copy()

    # Add placeholder implementations for all abstract methods from ExchangeAPI
    # to allow instantiation of MockExchangeAPI

    async def _handle_websocket_message(self, message: dict[str, Any]):
        logger.debug(f"Mock {self.exchange_name}: _handle_websocket_message called with {message}")
        # Add basic handling or pass
        pass

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        logger.debug(f"Mock {self.exchange_name}: cancel_all_orders called for {symbol}")
        # Simulate cancelling some orders if needed for tests
        cancelled_count = 0
        orders_to_remove = []
        for order_id, order in list(self._orders.items()):  # Iterate over a copy
            if symbol is None or order.symbol == symbol:
                if order.status not in [
                    OrderStatus.FILLED,
                    OrderStatus.CANCELED,
                    OrderStatus.REJECTED,
                ]:
                    order.status = OrderStatus.CANCELED
                    cancelled_count += 1
                    # Optionally remove from active orders or just update status
                    # If removing: orders_to_remove.append(order_id)
        # for order_id in orders_to_remove: del self._orders[order_id]
        return {"status": "success", "cancelled_count": cancelled_count}

    async def connect_websocket(self):
        logger.debug(f"Mock {self.exchange_name}: connect_websocket called")
        # Return a mock connection object or identifier if needed
        return {"connection_id": f"mock_ws_{self.exchange_name}"}

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        logger.debug(f"Mock {self.exchange_name}: get_funding_rates called for {symbols}")
        if symbols is None:
            return list(self._mock_funding_rates.values())
        else:
            return [self._mock_funding_rates[s] for s in symbols if s in self._mock_funding_rates]

    async def get_market_data(self, symbol: str) -> dict[str, Any]:
        logger.debug(f"Mock {self.exchange_name}: get_market_data called for {symbol}")
        # Return a combined dict of ticker, orderbook, etc. or None
        ticker = self._mock_tickers.get(symbol)
        # Add orderbook, etc. if needed
        return {"ticker": ticker.to_dict() if ticker else None} if ticker else {}

    def get_message_type(self, message: dict[str, Any]) -> str | None:
        logger.debug(f"Mock {self.exchange_name}: get_message_type called with {message}")
        # Implement basic logic based on mock message structure
        return message.get("type")  # Example

    async def get_order_history(
        self, symbol: str | None = None, limit: int | None = None
    ) -> list[Order]:
        logger.debug(f"Mock {self.exchange_name}: get_order_history called for {symbol}")
        # Return a filtered list of historical orders (could be same as self._orders for simplicity)
        history = list(self._orders.values())
        if symbol:
            history = [o for o in history if o.symbol == symbol]
        if limit:
            history = history[-limit:]
        return history

    async def get_trade_history(
        self, symbol: str | None = None, limit: int | None = None
    ) -> list[Trade]:
        logger.debug(f"Mock {self.exchange_name}: get_trade_history called for {symbol}")
        history = list(self._trades)
        if symbol:
            history = [t for t in history if t.symbol == symbol]
        if limit:
            history = history[-limit:]
        return history

    def parse_account_update_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        logger.debug(f"Mock {self.exchange_name}: parse_account_update_message called")
        # Return parsed balance/position update or None
        return None  # Placeholder

    def parse_balance(self, data: Any) -> Balance | None:
        logger.debug(f"Mock {self.exchange_name}: parse_balance called")
        # Assume data is already a Balance object or dict usable by Balance constructor
        if isinstance(data, Balance):
            return data
        elif isinstance(data, dict):
            try:
                return Balance(**data)
            except Exception:
                return None
        return None

    def parse_funding_rate(self, data: Any) -> FundingRate | None:
        logger.debug(f"Mock {self.exchange_name}: parse_funding_rate called")
        if isinstance(data, FundingRate):
            return data
        elif isinstance(data, dict):
            try:
                return FundingRate(**data)
            except Exception:
                return None
        return None

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        logger.debug(f"Mock {self.exchange_name}: parse_funding_rate_message called")
        # Extract data and parse
        data = message.get("data")
        return self.parse_funding_rate(data)

    def parse_order(self, data: Any) -> Order | None:
        logger.debug(f"Mock {self.exchange_name}: parse_order called")
        if isinstance(data, Order):
            return data
        elif isinstance(data, dict):
            try:
                # Basic conversion, might need more sophisticated parsing for real APIs
                # Convert side/status/type strings to enums if present
                if isinstance(data.get("side"), str):
                    data["side"] = OrderSide(data["side"].lower())
                if isinstance(data.get("status"), str):
                    data["status"] = OrderStatus(data["status"].upper())
                if isinstance(data.get("type"), str):
                    data["type"] = OrderType(data["type"].lower())
                # Convert numeric strings/floats to Decimal
                for key in ["price", "quantity", "filled_quantity", "avg_fill_price"]:
                    if key in data and data[key] is not None:
                        data[key] = Decimal(str(data[key]))

                return Order(**data)
            except Exception as e:
                logger.error(f"Mock Error parsing order dict: {e} - Data: {data}")
                return None
        return None

    def parse_order_book(self, data: Any) -> OrderBook | None:
        logger.debug(f"Mock {self.exchange_name}: parse_order_book called")
        if isinstance(data, OrderBook):
            return data
        elif isinstance(data, dict):
            try:
                # Ensure bids/asks are lists of tuples with Decimals
                data["bids"] = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("bids", [])]
                data["asks"] = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get("asks", [])]
                return OrderBook(**data)
            except Exception:
                return None
        return None

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        logger.debug(f"Mock {self.exchange_name}: parse_order_update_message called")
        data = message.get("data")
        return self.parse_order(data)

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        logger.debug(f"Mock {self.exchange_name}: parse_orderbook_message called")
        data = message.get("data")
        return self.parse_order_book(data)

    def parse_position(self, data: Any) -> Position | None:
        logger.debug(f"Mock {self.exchange_name}: parse_position called")
        if isinstance(data, Position):
            return data
        elif isinstance(data, dict):
            try:
                # Handle enum/Decimal conversion similar to parse_order
                if isinstance(data.get("side"), str):
                    pos_side = OrderSide(data["side"].lower())
                    if pos_side is not None:
                        data["side"] = pos_side
                    else:
                        # Handle case where side conversion fails, maybe default or raise
                        logger.warning(f"Could not parse position side: {data.get('side')}")
                        # Decide on handling: raise, default, or skip setting side
                        data.pop("side", None)  # Example: remove if unparseable
                elif isinstance(data.get("side"), OrderSide):
                    pass  # Already correct type
                else:
                    data.pop("side", None)  # Remove if not string or OrderSide

                for key in [
                    "size",
                    "entry_price",
                    "mark_price",
                    "liquidation_price",
                    "leverage",
                    "unrealized_pnl",
                ]:
                    if key in data and data[key] is not None:
                        data[key] = Decimal(str(data[key]))
                return Position(**data)
            except Exception:
                return None
        return None

    def parse_ticker(self, data: Any) -> Ticker | None:
        logger.debug(f"Mock {self.exchange_name}: parse_ticker called")
        if isinstance(data, Ticker):
            return data
        elif isinstance(data, dict):
            try:
                # Handle Decimal conversion
                for key in ["price", "bid", "ask", "volume"]:
                    if key in data and data[key] is not None:
                        data[key] = Decimal(str(data[key]))
                # Use 'price' key for Ticker, not 'last_price'
                if "last_price" in data:
                    data["price"] = data.pop("last_price")
                return Ticker(**data)
            except Exception as e:
                logger.error(f"Failed to parse ticker data: {e}, Data: {data}")
                return None
        return None

    def parse_ticker_message(self, message: dict[str, Any]) -> tuple[str, Ticker] | Ticker | None:
        logger.debug(f"Mock {self.exchange_name}: parse_ticker_message called")
        # Assume structure like {'type': 'ticker', 'symbol': 'BTC-PERP', 'data': {...}}
        data = message.get("data")
        ticker = self.parse_ticker(data)
        # Return tuple or just Ticker based on expected format
        # Example: return (ticker.symbol, ticker) if ticker else None
        return ticker  # Simpler return for now

    def parse_trade(self, data: Any) -> Trade | None:
        logger.debug(f"Mock {self.exchange_name}: parse_trade called")
        if isinstance(data, Trade):
            return data
        elif isinstance(data, dict):
            try:
                # Handle enum/Decimal conversion similar to parse_order
                if isinstance(data.get("side"), str):
                    data["side"] = OrderSide(data["side"].lower())
                for key in ["price", "quantity", "fee", "cost"]:
                    if key in data and data[key] is not None:
                        data[key] = Decimal(str(data[key]))
                return Trade(**data)
            except Exception:
                return None
        return None

    def parse_trade_message(self, message: dict[str, Any]) -> list[Trade] | Trade | None:
        logger.debug(f"Mock {self.exchange_name}: parse_trade_message called")
        # Assume structure like {'type': 'trades', 'data': [{...}, {...}]} or single trade
        data = message.get("data")
        if isinstance(data, list):
            return [self.parse_trade(t) for t in data if self.parse_trade(t)]
        else:
            return self.parse_trade(data)

    async def ping_websocket(self) -> None:
        logger.debug(f"Mock {self.exchange_name}: ping_websocket called")
        pass

    async def subscribe_to_account_updates(self, **kwargs: Any) -> None:
        logger.debug(f"Mock {self.exchange_name}: subscribe_to_account_updates called")
        pass

    async def subscribe_to_order_book(self, symbols: list[str], **kwargs: Any) -> None:
        logger.debug(f"Mock {self.exchange_name}: subscribe_to_order_book called for {symbols}")
        pass

    async def subscribe_to_ticker(self, symbols: list[str], **kwargs: Any) -> None:
        logger.debug(f"Mock {self.exchange_name}: subscribe_to_ticker called for {symbols}")
        pass

    async def subscribe_to_trades(self, symbols: list[str], **kwargs: Any) -> None:
        logger.debug(f"Mock {self.exchange_name}: subscribe_to_trades called for {symbols}")
        pass

    # --- End Added Placeholders ---
