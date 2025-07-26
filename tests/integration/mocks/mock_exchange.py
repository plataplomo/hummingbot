"""Mock exchange API implementation for integration testing.

Provides a complete mock implementation of ExchangeAPI that simulates
exchange behavior including order management, market data, WebSocket
interactions, and error conditions for comprehensive testing.
"""

import asyncio
import uuid
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from pydantic import BaseModel

# Added import for ValidationError
# Import Fill type
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    PositionIntent,
)
from cyberdelta.apis.common import APIError, APIErrorCode, IErrorMapper, MessageHandler
from cyberdelta.apis.models.service_args import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)

# Correct the import to use the new typing module
# REMOVED INCORRECT IMPORT: from cyberdelta.core.symbol_mapper import Symbol
from cyberdelta.config import AppSettings
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    CancelOrderResultStatus,
    InternalTransferStatus,
    InternalWithdrawalStatus,
)
from cyberdelta.core.models import (
    AccountSettings,
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
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
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.market import Market
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.operations import Transfer, Withdrawal


logger = get_logger(__name__)

# Type alias for WebSocket message handlers from base.py


# Define a custom exception for mock API errors
class MockAPIError(Exception):
    """Custom exception for mock API errors in integration testing."""


# Minimal placeholder ErrorMapper to resolve import issues for this mock file
class MockErrorMapper(IErrorMapper):
    """Minimal placeholder ErrorMapper to resolve import issues for mock testing."""

    def map_exchange_error(
        self,
        status_code: int,
        error_body: str | None,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """Map exchange error details to APIError instance.

        Returns:
            APIError instance with mapped error details.
        """
        exchange_code = getattr(self, "exchange_name", "MockExchange")
        return APIError(
            message=error_body or "Mock API Error",
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            exchange_code=exchange_code,
        )

    def map_string_error(self, error_message: str, http_status: int | None = None) -> APIError:
        """Map string error message to APIError instance.

        Returns:
            APIError instance with mapped error message.
        """
        exchange_code = getattr(self, "exchange_name", "MockExchange")
        api_error_code_val = APIErrorCode.EXCHANGE_SPECIFIC.value  # Default
        if http_status == 404:  # MODIFIED: Simplified condition
            # For a 404, we might use UNKNOWN or keep EXCHANGE_SPECIFIC if no better fit
            api_error_code_val = (
                APIErrorCode.UNKNOWN.value
            )  # MODIFIED: Using UNKNOWN for 404 as a general "not found"

        return APIError(
            message=error_message,
            code=api_error_code_val,  # MODIFIED
            exchange_code=exchange_code,
        )


class MockExchangeAPI(ExchangeAPI):
    """Mock implementation of the ExchangeAPI for integration testing.

    Simulates basic exchange behavior, including order management, data fetching,
    and WebSocket interactions. Allows simulating errors and latency.
    """

    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any] | ExchangeSpecificConfig,
        secrets: dict[str, str | None],
        config_obj: AppSettings | None = None,
    ) -> None:
        """Initialize MockExchangeAPI with exchange configuration and secrets."""
        # Handle both dict and ExchangeSpecificConfig objects
        if isinstance(config, dict):
            # Original dict-based approach
            config_copy = config.copy()  # Modify a copy
            current_api_base_url = config_copy.get("api_base_url")
        else:
            # ExchangeSpecificConfig object - need to convert to dict for compatibility
            config_copy = {"api_base_url": "http://fixedmock.exchange"}  # Simplified for mock
            current_api_base_url = str(config.api_base_url_mainnet)
        is_valid_url = False
        if isinstance(current_api_base_url, str) and current_api_base_url.startswith((
            "http://",
            "https://",
        )):
            # Simple check for protocol, can be enhanced if needed
            is_valid_url = True

        if not is_valid_url:
            logger.warning(
                "mock_exchange_api_url_override",
                exchange_name=exchange_name,
                original_url=current_api_base_url,
                new_url="http://fixedmock.exchange",
                message="MockExchangeAPI overriding api_base_url for HttpClientConfig stability",
            )
            config_copy["api_base_url"] = "http://fixedmock.exchange"  # Force a valid one

        mock_error_mapper = MockErrorMapper()  # Use the placeholder ErrorMapper

        # Pass the original config object to the base class if it's ExchangeSpecificConfig
        if isinstance(config, ExchangeSpecificConfig):
            super().__init__(
                exchange_name,
                config,
                cast("AnyExchangeSecrets", secrets),
                error_mapper=mock_error_mapper,
            )
        else:
            # For dict configs, cast to ExchangeSpecificConfig (legacy behavior)
            super().__init__(
                exchange_name,
                cast("ExchangeSpecificConfig", config_copy),
                cast("AnyExchangeSecrets", secrets),
                error_mapper=mock_error_mapper,
            )
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
            # Access exchange config from AppSettings
            exchange_config = self.full_config.exchanges.get(exchange_name)
            if exchange_config:
                # Use default fees since these are not in the exchange config model
                self.maker_fee = Decimal(default_fee)
                self.taker_fee = Decimal(default_fee)
                self.fee_asset = default_asset  # Use default since not in config model
            else:
                self.maker_fee = Decimal(default_fee)
                self.taker_fee = Decimal(default_fee)
                self.fee_asset = default_asset
        # Fallback if full_config not provided (less ideal)
        elif isinstance(config, dict):
            self.maker_fee = Decimal(str(config.get("maker_fee", default_fee)))
            self.taker_fee = Decimal(str(config.get("taker_fee", default_fee)))
            self.fee_asset = config.get("collateral_asset", default_asset)
        else:
            # config is ExchangeSpecificConfig, use defaults
            self.maker_fee = Decimal(default_fee)
            self.taker_fee = Decimal(default_fee)
            self.fee_asset = default_asset
        # ---------------------------------------------------

        # Simulation parameters
        self._latency_ms: float = 10.0  # Default latency in milliseconds
        self._error_simulation: dict[str, Any] | None = None  # Config to simulate errors
        self._fail_on_method: str | None = None  # Method name to fail on
        self._failure_exception: Exception = MockAPIError(
            "Simulated API failure",
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

        # Added for WebSocket handlers
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._ws_subscriptions: dict[str, MessageHandler] = {}

        logger.info(
            "mock_exchange_initialized",
            exchange_name=exchange_name,
            maker_fee=float(self.maker_fee),
            taker_fee=float(self.taker_fee),
            fee_asset=self.fee_asset,
            message="Initialized MockExchangeAPI",
        )

    # --- Test Control Methods ADDED ---
    def reset(self) -> None:
        """Reset the mock exchange to a clean state for a new test."""
        self._order_id_counter = 1
        self._orders.clear()
        self._positions.clear()
        self._balances.clear()
        self._mock_tickers.clear()
        self._mock_funding_rates.clear()
        self._trades.clear()
        self.open_orders.clear()
        self.trade_history.clear()
        self.api_errors.clear()
        self._error_config.clear()
        self._call_counts.clear()
        self._open_orders_behavior = "keep_open"
        logger.debug(
            "mock_exchange_reset",
            exchange_name=self.exchange_name,
            message="MockExchangeAPI has been reset",
        )

    def set_mock_balance(self, balance: SpotBalance) -> None:
        """Set a mock balance for a specific asset."""
        if balance.exchange != self.exchange_name:
            logger.warning(
                "balance_exchange_mismatch",
                balance_exchange=balance.exchange,
                mock_exchange=self.exchange_name,
                message="Attempted to set balance for different exchange on mock. Ignoring.",
            )
            return
        self._balances[balance.asset] = balance
        logger.debug(
            "mock_balance_set",
            exchange_name=self.exchange_name,
            asset=balance.asset,
            balance=str(balance),
            message="Mock balance set for exchange",
        )

    def set_mock_ticker(self, ticker: Ticker) -> None:
        """Set a mock ticker for a specific symbol."""
        self._mock_tickers[ticker.symbol] = ticker
        logger.debug(
            "mock_ticker_set",
            exchange_name=self.exchange_name,
            symbol=ticker.symbol,
            ticker=str(ticker),
            message="Mock ticker set for exchange",
        )

    def set_mock_funding_rate(self, funding_rate: FundingRate) -> None:
        """Set a mock funding rate for a specific symbol."""
        self._mock_funding_rates[funding_rate.symbol] = funding_rate
        logger.debug(
            "mock_funding_rate_set",
            exchange_name=self.exchange_name,
            symbol=funding_rate.symbol,
            funding_rate=str(funding_rate),
            message="Mock funding rate set for exchange",
        )

    def set_mock_position(self, position: DerivativePosition) -> None:
        """Set a mock derivative position for a specific symbol."""
        if position.exchange != self.exchange_name:
            logger.warning(
                "position_exchange_mismatch",
                position_exchange=position.exchange,
                mock_exchange=self.exchange_name,
                message=(
                    f"Attempted to set position for {position.exchange} on "
                    f"{self.exchange_name} mock. Ignoring."
                ),
            )
            return
        self._positions[position.symbol] = (
            position  # Store in the _positions dict used by get_positions
        )
        logger.debug(
            "mock_position_set",
            exchange_name=self.exchange_name,
            symbol=position.symbol,
            position=str(position),
            message="Mock position set for exchange",
        )

    def set_open_orders_behavior(self, behavior: str) -> None:
        """Set the behavior for handling open orders.

        Args:
            behavior: One of "keep_open", "fill_immediately", or "partial_fill"
        """
        if behavior not in ["keep_open", "fill_immediately", "partial_fill"]:
            raise ValueError(f"Invalid open orders behavior: {behavior}")
        self._open_orders_behavior = behavior
        logger.debug(
            "open_orders_behavior_set",
            exchange_name=self.exchange_name,
            behavior=behavior,
            message="Open orders behavior set for exchange",
        )

    async def get_account_summary(self) -> MarginAccountSummary:
        """Return a mock account summary."""
        # Simulate potential API error for this method if configured
        self._check_error("get_account_summary")
        await self._simulate_latency()

        # For simplicity, return a generic summary. Tests can override by mocking this
        # method further if needed.
        # Or, could store a self._mock_account_summary and allow tests to set it.
        now = datetime.now(UTC)
        # Basic mock summary, can be expanded or made configurable
        return MarginAccountSummary(
            exchange=self.exchange_name,
            timestamp=now,
            total_equity=self._balances.get(
                "USDC",
                SpotBalance(
                    exchange=self.exchange_name,
                    asset="USDC",
                    total_quantity=Decimal(10000),
                    available_quantity=Decimal(10000),
                    timestamp=now,
                ),
            ).total_quantity,  # Example logic
            available_equity=self._balances.get(
                "USDC",
                SpotBalance(
                    exchange=self.exchange_name,
                    asset="USDC",
                    total_quantity=Decimal(9000),
                    available_quantity=Decimal(9000),
                    timestamp=now,
                ),
            ).available_quantity,  # Example logic
            total_initial_margin_required=Decimal(1000),
            total_maintenance_margin_required=Decimal(500),
            total_unrealized_pnl=Decimal(
                sum((pos.unrealized_pnl or Decimal(0)) for pos in self._positions.values()),
            ),
        )

    # --- END Test Control Methods ---

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
        order_data: dict[str, Any] = {
            "client_order_id": client_order_id,
            "exchange_order_id": client_order_id,  # Use the passed client_order_id
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
            "updated_at": ts,
            "triggered_at": None,
            "strategy_name": strategy,
            "signal_id": signal,
            "reduce_only": False,
            "post_only": False,
            "trades": [],
        }
        # Refine type hint if specific structure is known, otherwise Any
        # is acceptable for internal helper
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
            message=message,
            code=str(error_code.value),
            exchange_code=self.exchange_name,
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
            "error_simulation_cleared",
            exchange_name=self.exchange_name,
            method_name=method_name,
            message="MockExchange error simulation cleared",
        )

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """Split a symbol like 'BTC-USDC' into base and quote.

        Returns:
            tuple[str, str]: Base and quote assets from the symbol.
        """
        parts = symbol.split("-")
        if len(parts) == 2:
            return parts[0], parts[1]
        # Handle cases like BTC/USDT or simple BTC if needed
        parts = symbol.split("/")
        if len(parts) == 2:
            return parts[0], parts[1]
        # Default assumption if no separator
        logger.warning(
            "symbol_parse_warning",
            symbol=symbol,
            assumed_base=symbol,
            assumed_quote="USD",
            message="Could not determine base/quote for symbol, making assumptions",
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
                    "simulated_error_raised",
                    exchange_name=self.exchange_name,
                    method_name=method_name,
                    error=str(error),
                    message="MockExchange raising simulated error",
                )
                raise error

    def configure_failure(
        self,
        method_name: str | None = None,
        exception: Exception | None = None,
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
        """Mock authentication - returns empty dict.

        Returns:
            dict[str, Any]: Empty authentication dictionary.
        """
        self._check_error("_authenticate")
        await self._simulate_latency()
        return {}

    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """No-op for mock. Exchanges might use this to update internal rate limit states."""
        logger.debug(
            "rate_limit_headers_update",
            headers=dict(headers),
            method=method,
            path=path,
            message="_update_rate_limit_from_headers called (no-op)",
        )

    async def ping_websocket(self) -> None:
        """Mock implementation for WebSocket ping."""
        logger.debug(
            "websocket_ping_simulated",
            exchange_name=self.exchange_name,
            message="MockExchange simulating WebSocket ping",
        )
        self._check_error("ping_websocket")
        await self._simulate_latency()
        # No actual action needed

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WebSocket messages to registered handlers based on topic/type."""
        topic = message.get("topic") or message.get("channel") or message.get("type")
        data_payload = message.get("data", {})  # Extract data payload

        handler_to_call: MessageHandler | None = None
        if topic:
            if topic in self._ws_handlers:
                handler_to_call = self._ws_handlers[topic]
            elif topic in self._ws_subscriptions:  # Check subscriptions dict
                handler_to_call = self._ws_subscriptions[topic]

        if handler_to_call:
            try:
                # Call handler with enhanced context
                await handler_to_call(data_payload)
            except (APIError, ValueError, TypeError, KeyError) as e:
                logger.exception(
                    "ws_handler_error: Error in WS handler for topic",
                    topic=topic,
                    error=str(e),
                )
        else:
            logger.warning(
                "no_ws_handler: No handler for WS message topic/type",
                topic=topic,
                message=message,
            )

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
    ) -> None:  # Ensure handler type is correct
        """Subscribe to a WebSocket topic."""
        self._ws_subscriptions[topic] = handler  # Storing in _ws_subscriptions
        logger.info(
            "mock_subscription_added",
            topic=topic,
            message="Mock subscribed to topic",
        )

    async def _resubscribe(self) -> None:
        """Mock implementation for resubscribing to WebSocket topics."""
        logger.info(
            "websocket_resubscribing",
            exchange_name=self.exchange_name,
            topic_count=len(self._ws_subscriptions),
            message="MockExchange resubscribing to all known topics",
        )
        for topic, handler in self._ws_subscriptions.items():
            await self.subscribe(topic, handler)  # Re-use subscribe logic
        await self._simulate_latency()

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Mock implementation for generic WebSocket message handling."""
        logger.debug(
            "websocket_message_received",
            exchange_name=self.exchange_name,
            message_type=message.get("type"),
            message="MockExchange received generic WebSocket message",
        )
        # This could parse common message types if not routed by _route_ws_message

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

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update mock account settings.

        Returns:
            AccountSettings: Updated mock account settings.
        """
        self._check_error("update_account_settings")
        await self._simulate_latency()

        # Return a mock AccountSettings object with the requested values
        return AccountSettings(
            exchange=self.exchange_name,
            timestamp=datetime.now(UTC),
            leverage_limit=args.leverage_limit,
            auto_borrow_settlements=args.auto_borrow_settlements,
            auto_lend=args.auto_lend,
            auto_realize_pnl=args.auto_realize_pnl,
            auto_repay_borrows=args.auto_repay_borrows,
            bp_details=None,
            hl_details=None,
        )

    # --- Order Management ---

    def _validate_order_params(self, args: PlaceOrderArgs) -> None:
        """Validate order parameters."""
        if args.order_type == OrderType.LIMIT and args.price is None:
            raise APIError(
                "Price must be specified for LIMIT orders",
                code=APIErrorCode.INVALID_PARAMS.value,
            )
        if args.order_type == OrderType.MARKET and args.price is not None:
            logger.warning("Price is ignored for MARKET orders")

    def _check_balance_for_order(self, args: PlaceOrderArgs, order_id: str) -> None:
        """Check if sufficient balance exists for the order."""
        now = datetime.now(UTC)
        base_asset, quote_asset = self._split_symbol(args.symbol)

        if args.side == OrderSide.BUY:
            asset_to_check = quote_asset
            ticker_price = self._mock_tickers.get(
                args.symbol,
                Ticker(
                    symbol=args.symbol,
                    exchange="mock_exchange",
                    price=Decimal(0),
                    timestamp=now,
                ),
            ).price or Decimal(0)
            required_balance = args.quantity * (args.price or ticker_price)
        else:  # SELL
            asset_to_check = base_asset
            required_balance = args.quantity

        current_balance = self._balances.get(
            asset_to_check,
            SpotBalance(
                asset=asset_to_check,
                total_quantity=Decimal(0),
                available_quantity=Decimal(0),
                exchange=self.exchange_name,
                timestamp=now,
            ),
        ).available_quantity

        if current_balance < required_balance:
            logger.warning(
                "insufficient_balance_order",
                exchange_name=self.exchange_name,
                order_id=order_id,
                required_balance=str(required_balance),
                available_balance=str(current_balance),
                asset=asset_to_check,
                message="Insufficient balance for order",
            )
            raise APIError("Insufficient balance", code=APIErrorCode.INSUFFICIENT_FUNDS.value)

    def _determine_order_fill_params(
        self,
        args: PlaceOrderArgs,
    ) -> tuple[OrderStatus, Decimal, Decimal | None]:
        """Determine order status, fill quantity, and average fill price based on behavior.

        Returns:
            tuple[OrderStatus, Decimal, Decimal | None]: Order status, fill quantity, and
                average fill price.
        """
        now = datetime.now(UTC)

        if self._open_orders_behavior == "fill_immediately":
            order_status = OrderStatus.FILLED
            qty_filled = args.quantity
            avg_fill_price = self._get_fill_price(args, now)
        elif self._open_orders_behavior == "partial_fill":
            order_status = OrderStatus.PARTIALLY_FILLED
            qty_filled = args.quantity / 2
            avg_fill_price = self._get_fill_price(args, now)
        else:  # default or keep_open
            order_status = OrderStatus.OPEN
            qty_filled = Decimal("0.0")
            avg_fill_price = None

        return order_status, qty_filled, avg_fill_price

    def _get_fill_price(self, args: PlaceOrderArgs, timestamp: datetime) -> Decimal:
        """Get fill price for an order.

        Returns:
            Decimal: Fill price for the order based on order type and market conditions.
        """
        if args.order_type == OrderType.LIMIT:
            return args.price or Decimal(0)

        ticker = self._mock_tickers.get(
            args.symbol,
            Ticker(
                symbol=args.symbol,
                exchange="mock_exchange",
                price=Decimal(0),
                timestamp=timestamp,
            ),
        )
        return ticker.price or Decimal(0)

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place an order. Mock implementation.

        Returns:
            Order: Created order with mock execution status and pricing.
        """
        self._check_error("place_order")
        await self._simulate_latency()

        self._validate_order_params(args)

        order_id: str = str(args.client_order_id) if args.client_order_id else str(uuid.uuid4())
        self._order_id_counter += 1

        self._check_balance_for_order(args, order_id)

        order_status, qty_filled, avg_fill_price = self._determine_order_fill_params(args)

        now = datetime.now(UTC)
        # Create the order using the helper method
        order = self._create_internal_mock_order(
            client_order_id=order_id,  # Use the generated/provided ID
            symbol=args.symbol,
            side=args.side,
            order_type=args.order_type,
            status=order_status,
            qty_req=args.quantity,
            qty_fill=qty_filled,
            avg_price=avg_fill_price,
            price=args.price,
            time_in_force=args.time_in_force,
            ts=now,
            strategy="mock_strategy",  # Example
            signal="mock_signal",  # Example
            # Pass reduce_only, post_only if needed by helper or add here
        )
        # Add reduce_only and post_only after creation based on execution domain
        order.reduce_only = args.execution.position_intent == PositionIntent.REDUCE_ONLY
        order.post_only = args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY

        # Store the order
        self._orders[order.client_order_id] = order
        if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            self.open_orders[order.client_order_id] = order

        # Simulate fills/trades if filled
        if order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
            trade_fee_rate = self.taker_fee  # Assume taker for market/aggressive limit
            if (
                args.order_type == OrderType.LIMIT
                and args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY
            ):
                trade_fee_rate = self.maker_fee

            # Use avg_fill_price if available, otherwise fallback
            fill_price_for_trade = avg_fill_price if avg_fill_price is not None else order.price
            if fill_price_for_trade is None:
                logger.warning(
                    "missing_fill_price",
                    order_id=order.client_order_id,
                    fallback_price="0",
                    message="Cannot determine fill price for trade simulation, using fallback",
                )
                fill_price_for_trade = Decimal(0)

            trade_cost = qty_filled * fill_price_for_trade
            trade_fee = trade_cost * trade_fee_rate

            trade = Trade(
                id=f"mock_trade_{order.client_order_id}",
                order_id=order.client_order_id,
                exchange=self.exchange_name,
                symbol=args.symbol,
                price=fill_price_for_trade,
                quantity=qty_filled,
                side=args.side,
                fee=trade_fee,
                fee_asset=self.fee_asset,
                is_maker=(trade_fee_rate == self.maker_fee),
                executed_at=now,  # Ensure executed_at is present
                # Add optional detail slots if needed
            )
            self.trade_history.append(trade)
            self._trades.append(trade)  # Also add to internal list if used elsewhere
            # Update timestamp on order when trade occurs
            order.updated_at = now
            # Update balances and positions based on the trade
            self._update_balance_and_position(trade)

        logger.debug(
            "order_placed",
            exchange_name=self.exchange_name,
            order_id=order.client_order_id,
            order_status=order.status.value,
            message="Mock order placed",
        )
        return order

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Mock implementation for cancelling an order.

        Returns:
            CancelOrderResult: Result of the order cancellation operation.
        """
        self._check_error("cancel_order")
        await self._simulate_latency()

        # Extract validated fields from Pydantic model
        order_id = args.order_id
        symbol = args.symbol

        if order_id not in self._orders:
            logger.warning(
                "cancel_order_not_found",
                order_id=order_id,
                message="Mock order not found for cancellation",
            )
            return CancelOrderResult(
                symbol=symbol,
                order_id=order_id,
                client_order_id=args.client_order_id,
                success=False,
                message="Order not found",
                status=CancelOrderResultStatus.NOT_FOUND,
                raw_response=None,
            )

        order = self._orders[order_id]

        # Optionally validate symbol if provided
        if symbol is not None and order.symbol != symbol:
            logger.warning(
                "cancel_order_symbol_mismatch",
                order_id=order_id,
                expected_symbol=symbol,
                found_symbol=order.symbol,
                message="Mock order symbol mismatch during cancellation",
            )
            return CancelOrderResult(
                symbol=symbol,
                order_id=order_id,
                client_order_id=args.client_order_id,
                success=False,
                message=f"Symbol mismatch: expected {symbol}, found {order.symbol}",
                status=CancelOrderResultStatus.FAILED,
                raw_response=None,
            )

        if order.status not in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            logger.warning(
                "cancel_order_invalid_state",
                order_id=order_id,
                order_status=order.status.value,
                message="Mock order is not in a cancellable state",
            )
            return CancelOrderResult(
                symbol=symbol,
                order_id=order_id,
                client_order_id=args.client_order_id,
                success=False,
                message=f"Order not in cancellable state: {order.status}",
                status=CancelOrderResultStatus.ALREADY_CANCELLED_OR_CLOSED,
                raw_response=None,
            )

        # Simulate cancellation
        self._orders[order_id].status = OrderStatus.CANCELED
        self._orders[order_id].updated_at = datetime.now(UTC)
        if order_id in self.open_orders:
            del self.open_orders[order_id]

        logger.info(
            "order_cancelled",
            exchange_name=self.exchange_name,
            order_id=order_id,
            message="MockExchange cancelled order",
        )
        return CancelOrderResult(
            symbol=symbol,
            order_id=order_id,
            client_order_id=args.client_order_id,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Get order details by exchange ID or client ID.

        Returns:
            Order | None: Order details if found, None otherwise.
        """
        logger.debug(
            "get_order_request",
            exchange_name=self.exchange_name,
            order_id=args.order_id,
            symbol=args.symbol,
            client_order_id=args.client_order_id,
            message="Mock getting order details",
        )
        self._check_error("get_order")
        await self._simulate_latency()

        # Prioritize finding by exchange order ID
        order = self._orders.get(args.order_id)

        # If not found by exchange ID and client_order_id is provided, try that
        if not order and args.client_order_id:
            target_client_order_id = str(args.client_order_id)
            for o in self._orders.values():
                if o.client_order_id == target_client_order_id:
                    order = o
                    break

        # Optionally check symbol match if provided
        if args.symbol and order and order.symbol != args.symbol:
            logger.warning(
                "get_order_symbol_mismatch",
                order_id=args.order_id or args.client_order_id,
                requested_symbol=args.symbol,
                found_symbol=order.symbol,
                message="Order found but symbol mismatch",
            )
            return None  # Behavior for symbol mismatch can be refined.
        return order

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Get a specific order by ID, returning None if not found.

        Returns:
            Order | None: Order if found, None otherwise.
        """
        self._check_error("get_order_status")
        await self._simulate_latency()
        order = self._orders.get(args.order_id)
        if order is None:
            logger.warning(
                "get_order_status_not_found",
                order_id=args.order_id,
                message="Mock order not found for status query",
            )
            return None
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
        except (APIError, ValueError, TypeError, KeyError) as e:
            logger.exception(
                "get_open_orders_error",
                error=str(e),
                symbol=symbol,
                message="Error in mock get_open_orders",
            )
            return []
        else:
            return orders_to_return

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Return all mock open orders, optionally filtered by symbol."""
        self._check_error("get_all_open_orders")
        await self._simulate_latency()
        try:
            orders_to_return: list[Order] = []
            for order in self._orders.values():
                # Filter by symbol if provided
                order_symbol: str | None = getattr(order, "symbol", None)
                if args.symbol is None or order_symbol == args.symbol:
                    orders_to_return.append(order)
        except (APIError, ValueError, TypeError, KeyError) as e:
            logger.exception(
                "get_all_open_orders_error",
                error=str(e),
                symbol=args.symbol,
                message="Error in mock get_all_open_orders",
            )
            return []
        else:
            return orders_to_return

    # --- Placeholder for balance and position update --- #
    def _update_balance_and_position(self, trade: Trade) -> None:
        """Simulate updating balances and positions after a trade."""
        logger.info(
            "balance_position_update_simulated",
            exchange_name=self.exchange_name,
            trade_id=trade.id,
            trade_side=trade.side.value,
            trade_quantity=str(trade.quantity),
            trade_symbol=trade.symbol,
            trade_price=str(trade.price),
            message="Mock simulating balance/position update for trade",
        )
        # TODO: Implement actual balance and position update logic if needed for tests.
        # This would involve:
        # 1. Identifying base and quote assets from trade.symbol.
        # 2. Adjusting balances for base and quote assets based on trade side, quantity, price, fee.
        # 3. Updating or creating a position for the symbol.

    async def _on_ws_connected(self) -> None:  # Added concrete implementation
        # This method is not provided in the original file or the code block
        # It's assumed to exist as it's called in the _route_ws_message method
        pass

    # --- ADDED PLACEHOLDERS FOR MISSING ExchangeAPI ABSTRACT METHODS ---

    def _construct_subscription_payload(self, topic: str) -> BaseModel:
        """Mock implementation for constructing subscription payload.

        Returns:
            BaseModel: Generic subscription payload as BaseModel.
        """
        logger.debug(
            "subscription_payload_constructed",
            exchange_name=self.exchange_name,
            topic=topic,
            message="MockExchange constructing subscription payload",
        )
        # Return a generic payload as a BaseModel

        class MockSubscriptionPayload(BaseModel):
            op: str
            args: list[str]

        if "orderbook" in topic.lower():
            return MockSubscriptionPayload(op="subscribe", args=[topic])
        if "trades" in topic.lower():
            return MockSubscriptionPayload(op="subscribe", args=[topic])
        return MockSubscriptionPayload(op="subscribe", args=[topic])

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Return mock funding rates for multiple symbols."""
        self._check_error("get_funding_rates")
        await self._simulate_latency()

        # Extract validated fields from Pydantic model
        symbols = args.symbols

        rates: list[FundingRate] = []
        if symbols:
            for symbol in symbols:
                rate = self._mock_funding_rates.get(symbol)
                if rate:
                    rates.append(rate)
        else:  # Return all mock rates if no specific symbols requested
            rates.extend(list(self._mock_funding_rates.values()))
        return rates

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Return mock historical funding rates."""
        self._check_error("get_historical_funding_rates")
        await self._simulate_latency()

        # Return empty list for simplicity, or a predefined set of historical rates
        logger.debug(
            "get_historical_funding_rates_request",
            exchange_name=self.exchange_name,
            symbol=args.symbol,
            message="MockExchange get_historical_funding_rates request",
        )
        return []

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Return mock market data (candles)."""
        self._check_error("get_market_data")
        await self._simulate_latency()

        # Extract validated fields from Pydantic model
        symbol = args.symbol
        timeframe = args.timeframe
        limit = args.limit or 100

        # Return empty list for simplicity, or a predefined set of candles
        logger.debug(
            "get_market_data_request",
            exchange_name=self.exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            message="MockExchange get_market_data request",
        )
        return []

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Mock implementation for cancelling all orders.

        Returns:
            list[CancelOrderResult]: List of cancellation results for all orders.
        """
        self._check_error("cancel_all_orders")
        await self._simulate_latency()
        orders_to_cancel_ids: list[str] = []
        for order_id, order in self._orders.items():
            if (order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]) and (
                symbol is None or order.symbol == symbol
            ):
                orders_to_cancel_ids.append(order_id)

        results: list[CancelOrderResult] = []
        for order_id in orders_to_cancel_ids:
            order = self._orders[order_id]
            self._orders[order_id].status = OrderStatus.CANCELED
            self._orders[order_id].updated_at = datetime.now(UTC)
            if order_id in self.open_orders:
                del self.open_orders[order_id]

            # Create a CancelOrderResult for each cancelled order
            results.append(
                CancelOrderResult(
                    symbol=order.symbol,
                    order_id=order_id,
                    client_order_id=order.client_order_id,
                    success=True,
                    message="Successfully cancelled.",
                    status=CancelOrderResultStatus.SUCCESS,
                ),
            )

        logger.info(
            "all_orders_cancelled",
            exchange_name=self.exchange_name,
            cancelled_count=len(orders_to_cancel_ids),
            symbol=symbol,
            message="MockExchange cancelled all orders",
        )
        return results

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Return mock order history."""
        self._check_error("get_order_history")
        await self._simulate_latency()

        # Extract validated fields from Pydantic model
        symbol = args.symbol
        start_time = args.start_time
        end_time = args.end_time
        limit = args.limit
        order_id = args.order_id
        client_order_id = args.client_order_id

        # Basic filtering, can be enhanced
        results = list(self._orders.values())
        if symbol:
            results = [o for o in results if o.symbol == symbol]
        if start_time:
            results = [o for o in results if o.created_at >= start_time]
        if end_time:
            results = [o for o in results if o.created_at <= end_time]
        if order_id:  # Filter by exchange order id
            results = [o for o in results if o.exchange_order_id == order_id]
        if client_order_id:  # Filter by client order id
            results = [o for o in results if o.client_order_id == client_order_id]

        if limit:
            results = results[:limit]
        return results

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Return mock trade history."""
        self._check_error("get_trade_history")
        await self._simulate_latency()
        results = self._trades  # Use the internal _trades list
        if args.symbol:
            results = [t for t in results if t.symbol == args.symbol]
        if args.limit:
            results = results[: args.limit]
        return results

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Mock implementation for internal transfer.

        Returns:
            Transfer: Mock transfer record with transaction details.
        """
        self._check_error("transfer")
        await self._simulate_latency()

        # Return a mock transfer result
        return Transfer(
            id=f"mock_transfer_{args.client_transfer_id or 'auto'}",
            exchange=self.exchange_name,
            asset=args.asset,
            quantity=args.amount,
            status=InternalTransferStatus.COMPLETED,
            timestamp=datetime.now(UTC),
        )

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Mock implementation for withdrawal.

        Returns:
            Withdrawal: Mock withdrawal record with transaction details.
        """
        self._check_error("withdraw")
        await self._simulate_latency()

        # Return a mock withdrawal result
        return Withdrawal(
            id=f"mock_withdrawal_{args.client_withdrawal_id or 'auto'}",
            exchange=self.exchange_name,
            asset=args.asset,
            quantity=args.amount,
            address=args.address,
            status=InternalWithdrawalStatus.PENDING,
            timestamp=datetime.now(UTC),
            fee=Decimal("0.001"),  # Mock fee
        )

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Return mock market data for a specific symbol."""
        self._check_error("get_market")
        await self._simulate_latency()

        # Return a mock market for the requested symbol
        return Market(
            symbol=args.symbol,
            base_symbol=args.symbol.split("_")[0] if "_" in args.symbol else args.symbol,
            quote_symbol=args.symbol.split("_")[1] if "_" in args.symbol else "USD",
            market_type="Spot",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            status="ACTIVE",
        )

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Return mock markets data."""
        self._check_error("get_markets")
        await self._simulate_latency()

        # Return a few mock markets
        mock_symbols = ["BTC_USD", "ETH_USD", "SOL_USD"]
        markets: list[Market] = []
        for symbol in mock_symbols:
            base, quote = symbol.split("_")
            market = Market(
                symbol=symbol,
                base_symbol=base,
                quote_symbol=quote,
                market_type="Spot",
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                status="ACTIVE",
            )
            markets.append(market)
        return markets

    # --- END OF ADDED PLACEHOLDERS ---

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Mock implementation of place_batch_orders.

        Returns:
            list[Order]: List of created orders from the batch operation.
        """
        self._check_error("place_batch_orders")
        await self._simulate_latency()

        results: list[Order] = []
        for order_args in orders:
            try:
                # Place each order individually
                order = await self.place_order(order_args)
                results.append(order)
            except (APIError, ValueError, TypeError, KeyError) as e:
                # In batch operations, we continue even if some orders fail
                logger.exception(
                    "batch_order_placement_failed",
                    exchange_name=self.exchange_name,
                    error=str(e),
                    message="MockExchange failed to place batch order",
                )
                # Create a failed order object
                failed_order = Order(
                    client_order_id=order_args.client_order_id
                    or f"failed_{self._order_id_counter}",
                    exchange_order_id="",
                    exchange=self.exchange_name,
                    symbol=order_args.symbol,
                    side=order_args.side,
                    order_type=order_args.order_type,
                    status=OrderStatus.REJECTED,
                    quantity_requested=order_args.quantity,
                    quantity_filled=Decimal(0),
                    price=order_args.price,
                    average_fill_price=None,
                    time_in_force=order_args.time_in_force,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                    triggered_at=None,
                    strategy_name=None,  # PlaceOrderArgs doesn't have strategy_name
                    signal_id=None,  # PlaceOrderArgs doesn't have signal_id
                    reduce_only=order_args.execution.position_intent == PositionIntent.REDUCE_ONLY,
                    post_only=(
                        order_args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY
                    ),
                )
                results.append(failed_order)

        logger.info(
            "batch_orders_placed",
            exchange_name=self.exchange_name,
            total_orders=len(results),
            successful_orders=sum(1 for o in results if o.status != OrderStatus.REJECTED),
            message="MockExchange placed batch orders",
        )
        return results

    async def cancel_batch_orders(
        self,
        cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Mock implementation of cancel_batch_orders.

        Returns:
            list[CancelOrderResult]: List of cancellation results for the batch operation.
        """
        self._check_error("cancel_batch_orders")
        await self._simulate_latency()

        results: list[CancelOrderResult] = []
        for cancel_arg in cancel_args:
            try:
                # Cancel each order individually
                cancel_result = await self.cancel_order(cancel_arg)
                results.append(cancel_result)
            except (APIError, ValueError, TypeError, KeyError) as e:
                # In batch operations, we continue even if some cancellations fail
                logger.exception(
                    "batch_order_cancellation_failed",
                    exchange_name=self.exchange_name,
                    error=str(e),
                    message="MockExchange failed to cancel batch order",
                )
                results.append(
                    CancelOrderResult(
                        symbol=cancel_arg.symbol or "",
                        order_id=cancel_arg.order_id,
                        client_order_id=cancel_arg.client_order_id,
                        success=False,
                        message=str(e),
                        status=CancelOrderResultStatus.FAILED,
                    ),
                )

        logger.info(
            "batch_orders_cancelled",
            exchange_name=self.exchange_name,
            total_orders=len(results),
            successful_cancellations=sum(1 for r in results if r.success),
            message="MockExchange cancelled batch orders",
        )
        return results

    async def close(self) -> None:
        """Close any resources held by the mock API (e.g., WebSocket connection)."""
        logger.info(
            "mock_exchange_api_closing",
            exchange_name=self.exchange_name,
            message="MockExchangeAPI is being closed",
        )
        # In a real scenario, you might close mock WebSocket connections or clean up resources.
        # For this mock, we primarily log. If specific mock resources were created (e.g.,
        # a mock WebSocket server task), they would be cleaned up here.
        # Since _http_client and _ws_manager are managed by the parent ExchangeAPI,
        # their closure is handled there if they were internally created.
        # If they were patched out (as in the conftest.py fixtures), then this close
        # might not do much for them directly, but it's good practice to call super().close()
        # if the parent has a meaningful close.

        # Call super().close() if ExchangeAPI.close() does something meaningful.
        # Handle cases where http_client/ws_manager were mocked out
        try:
            await super().close()
        except (AttributeError, TypeError) as e:
            # Expected when http_client/ws_manager are mocked - just log and continue
            logger.debug(
                "mock_exchange_close_error",
                error=str(e),
                message="MockExchangeAPI close caught expected error from mocked components",
            )

        # For now, this mock's close is mostly a placeholder for testability.

    def set_order_book_behavior(
        self,
        behavior: str,
        data: dict[str, str | int | float] | None = None,
    ) -> None:
        """Configure the behavior of get_order_book."""
        # ... existing code ...
