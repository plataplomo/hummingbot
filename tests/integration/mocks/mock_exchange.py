import asyncio
import copy
import logging
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
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
from cyberdelta.core.typing import Symbol
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
                str(
                    self.full_config.get(
                        f"exchanges.{exchange_name}.maker_fee", default_fee
                    )
                )
            )
            self.taker_fee = Decimal(
                str(
                    self.full_config.get(
                        f"exchanges.{exchange_name}.taker_fee", default_fee
                    )
                )
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
        self._error_simulation: dict[str, Any] | None = (
            None  # Config to simulate errors
        )
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
        self.api_errors: list[dict] = []

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
        error = APIError(
            message=message, code=error_code, exchange_code=self.exchange_name
        )
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

    def configure_failure(
        self, method_name: str | None = None, exception: Exception | None = None
    ):
        """Configure the mock to fail on a specific method call."""
        self._fail_on_method = method_name
        if exception:
            self._failure_exception = exception
        else:
            self._failure_exception = MockAPIError(
                f"Simulated API failure on {method_name}"
            )

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
        logger.debug(
            f"Mock {self.exchange_name}: Authenticating request {method} {path}"
        )
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
        logger.info(
            f"Mock {self.exchange_name}: Re-subscribing to {len(self._ws_handlers)} topics"
        )
        # Simulate re-sending subscription requests if needed

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Return a predefined mock ticker."""
        self._check_error("get_ticker")
        await self._simulate_latency()
        logger.debug(
            f"[DEBUG] Mock {self.exchange_name}: get_ticker called for symbol '{symbol}'."
        )
        logger.debug(
            f"[DEBUG] Mock {self.exchange_name}: Current _mock_tickers keys: {list(self._mock_tickers.keys())}"
        )
        logger.debug(f"Mock {self.exchange_name}: Getting ticker for {symbol}")
        return self._mock_tickers.get(symbol)

    async def get_order_book(
        self, symbol: str, depth: int | None = None
    ) -> OrderBook | None:
        """Return a predefined mock order book (basic implementation)."""
        self._check_error("get_order_book")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting order book for {symbol}")
        # Return a dummy OrderBook or None
        return OrderBook(
            symbol=symbol, bids=[], asks=[], timestamp=datetime.now(UTC)
        )

    async def get_recent_trades(
        self, symbol: str, limit: int | None = None
    ) -> list[Trade]:
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
        """Return the mock balances."""
        if self._fail_on_method == "get_balances":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate latency
        # Return a deep copy to prevent external modification
        return copy.deepcopy(self._balances)

    async def get_positions(
        self, symbols: list[Symbol] | None = None
    ) -> list[Position]:
        """Return mock positions, optionally filtered by symbols."""
        self._check_error("get_positions")
        await self._simulate_latency()
        logger.debug(f"Mock {self.exchange_name}: Getting positions")
        positions = [
            p
            for p in self._positions.values()
            if symbols is None or p.symbol in symbols
        ]
        return positions

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
        **kwargs,
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
        status = (
            OrderStatus.FILLED if order_type == OrderType.MARKET else OrderStatus.NEW
        )

        if order_type == OrderType.MARKET:
            # Get the exchange-specific symbol
            # Assuming a simple mapping for mock: internal symbol maps directly
            exchange_symbol = symbol  # In real API, might need lookup config.get(f'exchanges.{self.exchange_name}.symbols.{symbol}')
            mock_ticker = self._mock_tickers.get(exchange_symbol)
            if mock_ticker:
                if side == OrderSide.BUY:
                    fill_price = (
                        mock_ticker.ask if mock_ticker.ask > 0 else mock_ticker.price
                    )
                else:  # SELL
                    fill_price = (
                        mock_ticker.bid if mock_ticker.bid > 0 else mock_ticker.price
                    )
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
            client_order_id=client_order_id,
            symbol=symbol,
            price=fill_price,  # Use the determined fill_price
            quantity=quantity,
            filled_quantity=filled_quantity,
            side=side,
            type=order_type,
            status=status,
            time=int(datetime.now(UTC).timestamp() * 1000),
        )
        # Store initial order state before potentially modifying for fill behavior
        self._orders[order_id] = copy.deepcopy(order)

        # --- Apply Fill Behavior ---
        final_filled_quantity = Decimal("0.0")  # Use Decimal
        final_status = (
            status  # Default to initial status (NEW for limit, FILLED for market)
        )
        trade_to_record = None  # Only create a trade if filled
        avg_fill_price = (
            Decimal(str(fill_price)) if fill_price is not None else None
        )  # Convert fill_price to Decimal

        # Determine fee rate (use taker fee for market/immediate fills, maker otherwise - simplified)
        # Ensure fee rate is Decimal
        trade_fee_rate = (
            self.taker_fee if final_status == OrderStatus.FILLED else self.maker_fee
        )
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
                logger.debug(
                    f"Mock {self.exchange_name}: Immediately filling order {order_id}"
                )

            elif (
                self._open_orders_behavior == "partial_fill"
                and order_type == OrderType.MARKET
            ):
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
                logger.debug(
                    f"Mock {self.exchange_name}: Fully filling market order {order_id}"
                )

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
        self, order_id: str, symbol: str | None = None
    ) -> dict[str, Any]:
        """Simulate cancelling an order."""
        self._check_error("cancel_order")
        await self._simulate_latency()
        logger.info(f"Mock {self.exchange_name}: Cancelling order {order_id}")

        if order_id in self._orders:
            order = self._orders[order_id]
            if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                order.status = OrderStatus.CANCELLED
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
            logger.error(
                f"Mock {self.exchange_name}: Order {order_id} not found for cancellation."
            )
            raise APIError("Order not found", code=APIErrorCode.ORDER_NOT_FOUND)

    async def get_order(self, order_id: str) -> Order | None:
        """Return a specific order by its ID from the mock store."""
        if self._fail_on_method == "get_order":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate latency
        return copy.deepcopy(self._orders.get(order_id))

    async def get_open_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Return mock open orders, optionally filtered by symbol."""
        self._check_error("get_open_orders")
        await self._simulate_latency()
        logger.debug(
            f"Mock {self.exchange_name}: Getting open orders (symbol: {symbol})"
        )
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
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock ticker for {ticker.symbol}"
        )
        self._mock_tickers[ticker.symbol] = ticker

    def set_mock_funding_rate(self, funding_rate: FundingRate):
        """Set a funding rate value for the mock to return."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock funding rate for {funding_rate.symbol}"
        )
        self._mock_funding_rates[funding_rate.symbol] = funding_rate

    def set_mock_balance(self, balance: Balance):
        """Set a balance value for the mock to return. Ensures values are stored as Decimal."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock balance for {balance.asset}"
        )
        try:
            # Create a new Balance object with Decimal values
            decimal_balance = Balance(
                asset=balance.asset,
                # Convert potential float/int/str to Decimal via string
                total=Decimal(str(balance.total))
                if balance.total is not None
                else Decimal("0.0"),
                free=Decimal(str(balance.free))
                if balance.free is not None
                else Decimal("0.0"),
                locked=Decimal(str(balance.locked))
                if balance.locked is not None
                else Decimal("0.0"),
            )
            self._balances[balance.asset] = decimal_balance
        except Exception as e:
            logger.error(
                f"Error converting/setting mock balance for {balance.asset}: {e}. Original: {balance}"
            )
            # Fallback: store original if conversion fails, though this might lead to later TypeErrors
            self._balances[balance.asset] = balance

    def set_mock_position(self, position: Position):
        """Set a position value for the mock to return."""
        logger.debug(
            f"Mock {self.exchange_name}: Setting mock position for {position.symbol}"
        )
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

    async def fetch_ticker(self, symbol: Symbol) -> Ticker:
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

    async def fetch_funding_rate(self, symbol: Symbol) -> FundingRate:
        if self._fail_on_method == "fetch_funding_rate":
            raise self._failure_exception
        if self._mock_funding_rates and self._mock_funding_rates.get(symbol):
            return self._mock_funding_rates[symbol]
        # Simulate fetching if no specific mock is set
        return FundingRate(
            symbol=symbol,
            funding_rate=0.0001,
            next_funding_time=int(
                (datetime.now(UTC) + timedelta(hours=1)).timestamp() * 1000
            ),
        )

    async def fetch_balances(self) -> dict[str, Balance]:
        if self._fail_on_method == "fetch_balances":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate network latency
        return self._balances.copy()

    async def fetch_positions(self) -> dict[Symbol, Position]:
        if self._fail_on_method == "fetch_positions":
            raise self._failure_exception
        await asyncio.sleep(0.01)  # Simulate network latency
        return self._positions.copy()

    def _update_balance_and_position(self, trade: Trade):
        """Helper to update internal balances and positions based on a trade."""
        # Determine base asset from the symbol
        base_asset = trade.symbol.split("-")[0] if "-" in trade.symbol else trade.symbol

        # Determine the quote/margin asset for cost calculation
        # Prioritize USDC or USD if they exist as primary collateral
        if "USDC" in self._balances:
            quote_asset = "USDC"
        elif "USD" in self._balances:
            quote_asset = "USD"
        # Fallback: infer from symbol (less reliable for complex collateral)
        elif "-" in trade.symbol:
            quote_asset = trade.symbol.split("-")[1]
        else:
            quote_asset = (
                "USD"  # Default assumption if no hyphen and no primary collateral found
            )

        logger.debug(f"Determined Quote Asset for cost calculation: {quote_asset}")

        # Ensure balances exist for involved assets
        if quote_asset not in self._balances:
            self.set_mock_balance(
                Balance(asset=quote_asset, total=10000.0, free=10000.0)
            )  # Default initial balance
        if base_asset not in self._balances:
            self.set_mock_balance(Balance(asset=base_asset, total=0.0, free=0.0))

        cost = trade.quantity * trade.price
        # Fee deduction asset comes directly from the Trade object now
        fee_deduction_asset = trade.fee_asset
        fee = trade.fee  # Fee amount is already calculated in Trade object

        logger.debug(
            f"Updating balance for {self.exchange_name}: Symbol={trade.symbol}, Side={trade.side.value}, Qty={trade.quantity}, Price={trade.price}, Cost={cost}, Fee={fee}, FeeAsset={fee_deduction_asset}, CalculatedQuoteAsset={quote_asset}"
        )
        balance_before = self._balances.get(quote_asset)
        logger.debug(f"Balance BEFORE update ({quote_asset}): {balance_before}")
        # Ensure fee deduction asset balance exists
        if fee_deduction_asset not in self._balances:
            self.set_mock_balance(
                Balance(asset=fee_deduction_asset, total=10000.0, free=10000.0)
            )  # Add if missing

        fee_balance_before = self._balances.get(fee_deduction_asset)
        logger.debug(
            f"Fee Balance BEFORE update ({fee_deduction_asset}): {fee_balance_before}"
        )

        # --- Add Logging before adjustment ---
        logger.debug(
            f"Updating [{base_asset}] balance for trade side [{trade.side.value}] with quantity [{trade.quantity}]"
        )
        # --- End Logging ---

        # Apply cost and fee updates (using trade.fee and trade.fee_asset)
        if trade.side == OrderSide.BUY:
            # Increase base asset
            self._balances[base_asset].total += trade.quantity
            self._balances[base_asset].free += trade.quantity
            # Decrease quote asset by cost
            self._balances[quote_asset].total -= cost
            self._balances[quote_asset].free -= cost
            # Decrease fee asset by fee
            self._balances[fee_deduction_asset].total -= fee
            self._balances[fee_deduction_asset].free -= fee
        else:  # SELL
            # Decrease base asset
            self._balances[base_asset].total -= trade.quantity
            self._balances[base_asset].free -= trade.quantity
            # Increase quote asset by cost
            self._balances[quote_asset].total += cost
            self._balances[quote_asset].free += cost
            # Decrease fee asset by fee
            self._balances[fee_deduction_asset].total -= fee
            self._balances[fee_deduction_asset].free -= fee

        balance_after = self._balances.get(quote_asset)
        logger.debug(f"Balance AFTER update ({quote_asset}): {balance_after}")
        fee_balance_after = self._balances.get(fee_deduction_asset)
        logger.debug(
            f"Fee Balance AFTER update ({fee_deduction_asset}): {fee_balance_after}"
        )

        # Update position (very basic) - Ensure Decimal usage
        if trade.symbol not in self._positions:
            # Initialize position with Decimal values
            self._positions[trade.symbol] = Position(
                symbol=trade.symbol,
                size=Decimal("0.0"),  # Initialize with Decimal zero
                entry_price=Decimal("0.0"),  # Initialize with Decimal zero
                mark_price=trade.price,  # Already Decimal from Trade
                side=trade.side,  # side is enum
                liquidation_price=Decimal("0.0"),  # Initialize
                unrealized_pnl=Decimal("0.0"),  # Initialize
            )

        pos = self._positions[trade.symbol]
        # Ensure calculations use Decimal
        if trade.side == OrderSide.BUY:
            new_size = pos.size + trade.quantity  # Decimal + Decimal
        else:
            new_size = pos.size - trade.quantity  # Decimal - Decimal

        # Simple average entry price update (ensure Decimal math)
        if new_size != Decimal("0"):
            if pos.size == Decimal("0"):
                new_entry_price = trade.price  # trade.price is Decimal
            elif (pos.size > Decimal("0") and trade.side == OrderSide.BUY) or (
                pos.size < Decimal("0") and trade.side == OrderSide.SELL
            ):
                # Averaging up/down - All operands are Decimal
                new_entry_price = (
                    (pos.size * pos.entry_price) + (trade.quantity * trade.price)
                ) / new_size
            else:
                # Reducing position size - entry price doesn't change (usually PnL is realized)
                new_entry_price = pos.entry_price  # Already Decimal
        else:
            new_entry_price = Decimal("0.0")  # Position closed

        pos.size = new_size  # Assign Decimal
        pos.entry_price = new_entry_price  # Assign Decimal
        pos.mark_price = trade.price  # Assign Decimal

        # Ensure side is correct, especially when opening/flipping
        # Store size as signed Decimal: positive for BUY, negative for SELL
        # The 'side' attribute reflects the side of the *last* trade affecting the position,
        # but the sign of 'size' definitively indicates long/short.
        if new_size > Decimal("0"):
            pos.side = OrderSide.BUY
        elif new_size < Decimal("0"):
            pos.side = OrderSide.SELL
        else:
            # Position closed, side can remain as is or be reset (e.g., to BUY by default)
            pass

        pos.size = new_size  # Assign the potentially signed Decimal size
        pos.entry_price = new_entry_price  # Assign Decimal
        pos.mark_price = trade.price  # Assign Decimal

        # Recalculate PnL (optional, could be done elsewhere)
        # Note: calculate_unrealized_pnl needs to handle signed size correctly
        pos.unrealized_pnl = pos.calculate_unrealized_pnl(pos.mark_price)

        balance_after_pos = self._balances.get(quote_asset)
        logger.debug(
            f"Balance AFTER position update ({quote_asset}): {balance_after_pos}"
        )

    def get_trades(self) -> list[Trade]:
        # This logic was incorrect, need to store trades separately
        # return [trade for trade in self._orders.values() if trade.status == OrderStatus.FILLED]
        # Assuming trades are stored elsewhere or this method needs removal/rework
        # For now, return empty to avoid crashing tests if called
        return []

    def get_orders(self) -> dict[str, Order]:
        return self._orders.copy()
