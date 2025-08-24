"""Backpack Exchange connector for Hummingbot.
Main exchange class implementing all required trading functionality.
"""

import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from bidict import bidict

from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_utils as utils,
    backpack_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import BackpackAPIUserStreamDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import (
    AddedToCostTradeFee,
    DeductedFromReturnsTradeFee,
    TokenAmount,
    TradeFeeBase,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BackpackExchange(ExchangePyBase):
    """Backpack exchange connector implementing all required Hummingbot functionality.

    Features:
    - Order placement and cancellation
    - Balance and position tracking
    - Trading rules management
    - Real-time data via WebSocket
    - Ed25519 authentication
    """

    # These should be moved to constants file if needed
    # UPDATE_ORDER_STATUS_MIN_INTERVAL is handled by base class
    # HEARTBEAT_TIME_INTERVAL should be in constants

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_api_key: str,
        backpack_api_secret: str,
        trading_pairs: list[str] | None = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize Backpack exchange connector.

        Args:
            client_config_map: Client configuration
            backpack_api_key: API key for authentication
            backpack_api_secret: API secret for authentication
            trading_pairs: List of trading pairs to track
            trading_required: Whether trading functionality is required
            domain: Exchange domain
        """
        self.backpack_api_key = backpack_api_key
        self.backpack_api_secret = backpack_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        # Removed unused hardcoded value _last_trades_poll_timestamp
        super().__init__(client_config_map)

    # Static helper methods
    @staticmethod
    def backpack_order_type(order_type: OrderType) -> str:
        """Convert Hummingbot order type to Backpack format."""
        return CONSTANTS.ORDER_TYPE_MAP.get(order_type.name, order_type.name)

    @staticmethod
    def to_hb_order_type(backpack_type: str) -> OrderType:
        """Convert Backpack order type to Hummingbot format."""
        type_map = {v: k for k, v in CONSTANTS.ORDER_TYPE_MAP.items()}
        return OrderType[type_map.get(backpack_type, backpack_type)]

    @staticmethod
    def backpack_order_side(trade_type: TradeType) -> str:
        """Convert Hummingbot trade type to Backpack format."""
        return CONSTANTS.ORDER_SIDE_MAP.get(trade_type.name, trade_type.name)

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert Hummingbot trading pair format to Backpack exchange format.

        Args:
            trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

        Returns:
            Exchange symbol format (e.g., "BTC_USDC")
        """
        # For spot, Backpack uses underscore format like "BTC_USDC"
        return trading_pair.replace("-", "_")

    # Required properties
    @property
    def authenticator(self) -> BackpackAuth:
        """Get authenticator instance."""
        return BackpackAuth(
            api_key=self.backpack_api_key,
            api_secret=self.backpack_api_secret,
            time_provider=lambda: int(self._time_synchronizer.time() * 1000),
        )

    @property
    def name(self) -> str:
        """Get exchange name."""
        return "backpack"

    @property
    def rate_limits_rules(self):
        """Get rate limiting rules."""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        """Get exchange domain."""
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        """Get maximum client order ID length."""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        """Get client order ID prefix."""
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        """Get trading rules request path."""
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def trading_pairs_request_path(self) -> str:
        """Get trading pairs request path."""
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        """Get network check request path."""
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self) -> list[str]:
        """Get list of trading pairs."""
        return self._trading_pairs or []

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Whether cancel requests are synchronous."""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Whether trading functionality is required."""
        return self._trading_required

    @property
    def supported_order_types(self) -> list[OrderType]:
        """Get supported order types.
        Note: LIMIT_MAKER is supported by converting to LIMIT with PostOnly timeInForce.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    # Factory methods
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory."""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Create order book data source."""
        return BackpackAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs or [],
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Create user stream data source."""
        # Cast _auth to BackpackAuth since we know it's the correct type
        auth = self._auth
        if not isinstance(auth, BackpackAuth):
            raise ValueError("Invalid auth type")
        return BackpackAPIUserStreamDataSource(
            auth=auth,
            trading_pairs=self._trading_pairs or [],
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # Error handling
    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """Check if exception is related to time synchronization."""
        error_description = str(request_exception).lower()
        return (
            "timestamp" in error_description
            or "expired" in error_description
            or CONSTANTS.ERROR_CODE_EXPIRED_TIMESTAMP.lower() in error_description
        )

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if exception indicates order not found during status update."""
        # Try to parse JSON response properly
        if hasattr(status_update_exception, "response"):
            try:
                response = status_update_exception.response
                if hasattr(response, "json"):
                    error_data = response.json()
                    error_code = error_data.get("code", "")
                    error_message = error_data.get("message", "").lower()
                    return (
                        error_code == CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND
                        or "order not found" in error_message
                    )
            except (AttributeError, ValueError, TypeError):
                # Fall back to string checking if JSON parsing fails
                pass

        # Fallback to string checking for non-JSON responses
        error_str = str(status_update_exception).lower()
        return CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str or "order not found" in error_str

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if exception indicates order not found during cancellation."""
        # Try to parse JSON response properly
        if hasattr(cancelation_exception, "response"):
            try:
                response = cancelation_exception.response
                if hasattr(response, "json"):
                    error_data = response.json()
                    error_code = error_data.get("code", "")
                    error_message = error_data.get("message", "").lower()
                    return (
                        error_code == CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND
                        or "order not found" in error_message
                    )
            except (AttributeError, ValueError, TypeError):
                # Fall back to string checking if JSON parsing fails
                pass

        # Fallback to string checking for non-JSON responses
        error_str = str(cancelation_exception).lower()
        return CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str or "order not found" in error_str

    # API helper methods
    async def _api_get(
        self,
        path_url: str,
        params: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute GET request to Backpack API."""
        return await self._api_request(
            method=RESTMethod.GET,
            path_url=path_url,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
        )

    async def _api_post(
        self,
        path_url: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute POST request to Backpack API."""
        return await self._api_request(
            path_url=path_url,
            method=RESTMethod.POST,
            data=data,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
        )

    async def _api_delete(
        self,
        path_url: str,
        params: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute DELETE request to Backpack API."""
        return await self._api_request(
            method=RESTMethod.DELETE,
            path_url=path_url,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
        )

    async def _api_request(
        self,
        path_url: Any,
        overwrite_url: str | None = None,
        method: RESTMethod = RESTMethod.GET,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        return_err: bool = False,
        limit_id: str | None = None,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute API request with proper error handling and rate limiting.

        Args:
            path_url: API endpoint path
            overwrite_url: Optional URL override
            method: HTTP method
            params: Query parameters
            data: Request body data
            is_auth_required: Whether authentication is required
            return_err: Whether to return errors instead of raising
            limit_id: Rate limit identifier
            headers: Additional headers
            **kwargs: Additional arguments

        Returns:
            API response data
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        if limit_id is None:
            limit_id = str(path_url)

        # Use overwrite_url if provided, otherwise construct from path_url
        url = overwrite_url or web_utils.get_rest_url_for_endpoint(str(path_url), self._domain)

        async with self._throttler.execute_task(limit_id=limit_id):
            response = await rest_assistant.execute_request(
                url=url,
                method=method,
                params=params,
                data=data,
                is_auth_required=is_auth_required,
                headers=headers,
                throttler_limit_id=limit_id,
            )

            # Response from execute_request could be dict, str, list, or other types
            # Since Backpack API can return both dict and list, handle all cases
            if isinstance(response, dict):
                return response
            if isinstance(response, str):
                try:
                    return json.loads(response)
                except json.JSONDecodeError:
                    return {"error": "Invalid response format", "raw": response}
            elif isinstance(response, list):
                # For compatibility, wrap list responses in a dict
                return {"data": response}
            else:
                # Fallback - this handles None and other unexpected types
                return {"error": f"Unexpected response type: {type(response).__name__}"}

    # Network check
    async def check_network(self) -> bool:
        """Check network connectivity to Backpack."""
        try:
            await self._api_get(path_url=self.check_network_request_path)
            return True
        except Exception:
            return False

    # Trading rules and pairs
    async def _update_trading_rules(self):
        """Fetch and update trading rules from the exchange."""
        try:
            exchange_info = await self._api_get(
                path_url=self.trading_rules_request_path,
                limit_id=CONSTANTS.EXCHANGE_INFO_URL,
            )

            trading_rules = {}

            if "symbols" not in exchange_info:
                self.logger().error("Invalid exchange info response")
                return

            for symbol_info in exchange_info["symbols"]:
                try:
                    if symbol_info.get("status") != "TRADING":
                        continue

                    exchange_symbol = symbol_info["symbol"]
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)

                    filters = symbol_info.get("filters", {})

                    trading_rules[trading_pair] = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=Decimal(filters.get("minQty", "0")),
                        max_order_size=Decimal(filters.get("maxQty", "9999999")),
                        min_price_increment=Decimal(filters.get("tickSize", "0.01")),
                        min_base_amount_increment=Decimal(filters.get("stepSize", "0.01")),
                        min_notional_size=Decimal(filters.get("minNotional", "0")),
                    )

                except Exception as e:
                    self.logger().error(f"Error parsing trading rule for {symbol_info}: {e}")
                    continue

            self._trading_rules = trading_rules

        except Exception as e:
            self.logger().error(f"Error updating trading rules: {e}")
            self._trading_rules = {}

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get the last traded price for a trading pair.

        Args:
            trading_pair: Trading pair in Hummingbot format

        Returns:
            Last traded price as float
        """
        prices = await self._get_last_traded_prices([trading_pair])
        price = prices[trading_pair]  # Trust that price exists for requested pair
        return float(price)

    async def _get_last_traded_prices(self, trading_pairs: list[str]) -> dict[str, Decimal]:
        """Get last traded prices for specified trading pairs."""
        try:
            ticker_data = await self._api_get(
                path_url=CONSTANTS.TICKER_URL,
                limit_id=CONSTANTS.TICKER_URL,
            )

            prices = {}

            # Normalize response to list format
            tickers = web_utils.normalize_response_to_list(ticker_data)

            for ticker in tickers:
                if isinstance(ticker, dict):
                    exchange_symbol = ticker.get("symbol")
                    if exchange_symbol:
                        trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)
                        if trading_pair in trading_pairs:
                            prices[trading_pair] = Decimal(ticker.get("price", "0"))

            return prices

        except Exception as e:
            self.logger().error(f"Error fetching last traded prices: {e}")
            return {}

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = Decimal("NaN"),
        is_maker: bool | None = None,
    ) -> AddedToCostTradeFee:
        """Get fee for an order.

        Args:
            base_currency: Base currency of the trading pair
            quote_currency: Quote currency of the trading pair
            order_type: Order type
            order_side: Order side (buy/sell)
            amount: Order amount
            price: Order price
            is_maker: Whether the order is a maker order

        Returns:
            AddedToCostTradeFee object with fee information
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _status_polling_loop_fetch_updates(self):
        """Fetch updates for the status polling loop.
        This method is called periodically to update order status.
        """
        # Update order fills from trades if needed
        await self._update_order_fills_from_trades()
        # Call parent implementation
        await super()._status_polling_loop_fetch_updates()

    async def _update_order_fills_from_trades(self):
        """Update order fills from recent trades.
        This is used to ensure we capture all fills even if WebSocket messages are missed.
        """
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick and len(self._order_tracker.active_orders) > 0:
            # Group orders by trading pair for efficient querying
            trading_pairs_to_order_map: dict[str, dict[str, Any]] = {}
            for order in self._order_tracker.active_orders.values():
                if order.trading_pair not in trading_pairs_to_order_map:
                    trading_pairs_to_order_map[order.trading_pair] = {}
                trading_pairs_to_order_map[order.trading_pair][order.exchange_order_id] = order

            trading_pairs = list(trading_pairs_to_order_map.keys())

            # Fetch fills for each trading pair
            tasks = []
            for trading_pair in trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                tasks.append(
                    self._api_get(
                        path_url=CONSTANTS.FILLS_URL,
                        params={"symbol": exchange_symbol},
                        is_auth_required=True,
                        limit_id=CONSTANTS.FILLS_URL,
                    ),
                )

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)

            for result, trading_pair in zip(results, trading_pairs, strict=False):
                order_map = trading_pairs_to_order_map.get(trading_pair)

                if isinstance(result, Exception):
                    self.logger().network(
                        f"Error fetching fills update for {trading_pair}: {result}.",
                        app_warning_msg=f"Failed to fetch fill updates for {trading_pair}.",
                    )
                    continue

                fills_data = result.get("fills", []) if isinstance(result, dict) else []

                for fill in fills_data:
                    order_id = str(fill.get("orderId", ""))
                    if order_map and order_id in order_map:
                        tracked_order: InFlightOrder = order_map[order_id]

                        # Create trade update from fill data
                        trade_update = TradeUpdate(
                            trade_id=str(fill.get("tradeId", "")),
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=order_id,
                            trading_pair=tracked_order.trading_pair,
                            fee=self._get_fee_from_fill(fill),
                            fill_base_amount=Decimal(str(fill.get("quantity", 0))),
                            fill_quote_amount=(
                                Decimal(str(fill.get("quantity", 0))) * Decimal(str(fill.get("price", 0)))
                            ),
                            fill_price=Decimal(str(fill.get("price", 0))),
                            fill_timestamp=fill.get("timestamp", 0) * 1e-3,  # Convert ms to seconds
                        )
                        self._order_tracker.process_trade_update(trade_update)

    def _get_fee_from_fill(self, fill_data: dict) -> TradeFeeBase:
        """Extract fee information from fill data."""
        fee_amount = Decimal(str(fill_data.get("fee", 0)))
        fee_asset = fill_data.get("feeSymbol", "")

        return TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=TradeType.BUY if fill_data.get("side") == "Buy" else TradeType.SELL,
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)] if fee_amount > 0 else [],
        )

    # Balance management
    async def _update_balances(self):
        """Update account balances."""
        try:
            balances_data = await self._api_get(
                path_url=CONSTANTS.BALANCES_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.BALANCES_URL,
            )

            for balance_info in balances_data.get("balances", []):
                asset = balance_info["asset"]
                free_balance = Decimal(balance_info["free"])
                total_balance = Decimal(balance_info["total"])

                self._account_available_balances[asset] = free_balance
                self._account_balances[asset] = total_balance

        except Exception as e:
            self.logger().error(f"Error updating balances: {e}")

    # Order management
    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal | None = None,
        **kwargs,
    ) -> tuple[str, float]:
        """Place an order on Backpack exchange.

        Args:
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            trade_type: Buy or sell
            order_type: Limit or market
            price: Order price (required for limit orders)

        Returns:
            Tuple of (exchange_order_id, timestamp)
        """
        exchange_symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        order_data = {
            "symbol": exchange_symbol,
            "side": self.backpack_order_side(trade_type),
            "quantity": str(amount),
            "clientId": order_id,
        }

        # Handle LIMIT_MAKER by converting to LIMIT with PostOnly
        if order_type == OrderType.LIMIT_MAKER:
            order_data["orderType"] = "Limit"
            order_data["price"] = str(price)
            order_data["timeInForce"] = "PostOnly"
        elif order_type == OrderType.LIMIT:
            order_data["orderType"] = "Limit"
            order_data["price"] = str(price)  # Price is required for limit orders
            # Add time in force if specified
            time_in_force = kwargs.get("time_in_force")
            if time_in_force:
                order_data["timeInForce"] = CONSTANTS.TIME_IN_FORCE_MAP.get(time_in_force, "GTC")
        else:  # MARKET order
            order_data["orderType"] = "Market"

        try:
            response = await self._api_post(
                path_url=CONSTANTS.ORDER_URL,
                data=order_data,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_URL,
            )

            exchange_order_id = response["orderId"]
            timestamp = self.current_timestamp

            return exchange_order_id, timestamp

        except Exception as e:
            self.logger().error(f"Error placing order {order_id}: {e}")
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """Cancel an order on Backpack exchange.

        Args:
            order_id: Client order ID
            tracked_order: InFlightOrder being cancelled

        Returns:
            True if cancellation successful
        """
        exchange_symbol = utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)

        cancel_data = {
            "symbol": exchange_symbol,
            "orderId": tracked_order.exchange_order_id,
        }

        try:
            await self._api_delete(
                path_url=CONSTANTS.CANCEL_ORDER_URL,
                params=cancel_data,
                is_auth_required=True,
                limit_id=CONSTANTS.CANCEL_ORDER_URL,
            )

            return True

        except Exception as e:
            if self._is_order_not_found_during_cancelation_error(e):
                # Order already cancelled or filled
                return True
            self.logger().error(f"Error canceling order {order_id}: {e}")
            return False

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> list[TradeUpdate]:
        """Get all trade updates for a specific order."""
        try:
            exchange_symbol = utils.convert_to_exchange_trading_pair(order.trading_pair)

            fills_data = await self._api_get(
                path_url=CONSTANTS.FILLS_URL,
                params={
                    "symbol": exchange_symbol,
                    "orderId": order.exchange_order_id,
                },
                is_auth_required=True,
                limit_id=CONSTANTS.FILLS_URL,
            )

            trade_updates = []

            for fill in fills_data.get("fills", []):
                trade_update = TradeUpdate(
                    trade_id=fill["tradeId"],
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id or "",  # Handle None case
                    trading_pair=order.trading_pair,
                    # Backpack WebSocket uses microseconds, convert to seconds
                    fill_timestamp=fill["timestamp"] / 1_000_000 if "timestamp" in fill else self.current_timestamp,
                    fill_price=Decimal(fill["price"]),
                    fill_base_amount=Decimal(fill["quantity"]),
                    fill_quote_amount=Decimal(fill["quantity"]) * Decimal(fill["price"]),  # Calculate quote amount
                    fee=self._get_trade_fee(
                        base_currency=order.base_asset,
                        quote_currency=order.quote_asset,
                        order_type=order.order_type,
                        order_side=order.trade_type,
                        amount=Decimal(fill["quantity"]),
                        price=Decimal(fill["price"]),
                        fee_asset=fill.get("feeAsset", order.quote_asset),
                        fee_amount=Decimal(fill.get("fee", "0")),
                    ),
                )
                trade_updates.append(trade_update)

            return trade_updates

        except Exception as e:
            self.logger().error(f"Error getting trade updates for order {order.client_order_id}: {e}")
            return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """Request current status of an order."""
        try:
            exchange_symbol = utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)

            order_data = await self._api_get(
                path_url=CONSTANTS.ORDER_URL,
                params={
                    "symbol": exchange_symbol,
                    "orderId": tracked_order.exchange_order_id,
                },
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_URL,
            )

            order_state_str = CONSTANTS.ORDER_STATE_MAP.get(order_data["status"], "OPEN")

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState[order_state_str],
                misc_updates={
                    "fill_price": Decimal(order_data.get("price", "0")),
                    "executed_amount_base": Decimal(order_data.get("executedQuantity", "0")),
                    "executed_amount_quote": (
                        Decimal(order_data.get("executedQuantity", "0")) * Decimal(order_data.get("price", "0"))
                    ),
                },
            )

            return order_update

        except Exception as e:
            if self._is_order_not_found_during_status_update_error(e):
                # Order not found, mark as cancelled
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.CANCELED,
                )
            self.logger().error(f"Error requesting order status for {tracked_order.client_order_id}: {e}")
            raise

    # Fee calculation
    def _get_trade_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal,
        fee_asset: str | None = None,
        fee_amount: Decimal | None = None,
    ) -> TradeFeeBase:
        """Calculate trade fee for an order."""
        if fee_amount is not None and fee_asset is not None:
            # Use actual fee from trade
            return DeductedFromReturnsTradeFee(
                percent_token=fee_asset,
                flat_fees=[TokenAmount(token=fee_asset, amount=fee_amount)],
            )

        # Get fee rate from trading fees configuration
        # This should be fetched from exchange API or configuration
        # For now, use estimate_fee_pct which should be set from exchange data
        is_maker = order_type is OrderType.LIMIT
        fee_rate = self.estimate_fee_pct(is_maker)

        if order_side == TradeType.BUY:
            # Fee paid in base currency
            fee_amount = amount * fee_rate
            fee_asset = base_currency
        else:
            # Fee paid in quote currency
            fee_amount = amount * price * fee_rate
            fee_asset = quote_currency

        return DeductedFromReturnsTradeFee(
            percent_token=fee_asset,
            flat_fees=[TokenAmount(token=fee_asset, amount=fee_amount)],
        )

    async def _update_trading_fees(self):
        """Update trading fees from exchange.

        Backpack may provide fee information in the account endpoint
        or use a fixed fee structure. This should be fetched from the API
        when available.
        """
        try:
            # Try to fetch account info which may contain fee rates
            account_info = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_URL if hasattr(CONSTANTS, "ACCOUNT_URL") else "api/v1/account",
                is_auth_required=True,
                limit_id=CONSTANTS.BALANCES_URL,  # Use balance rate limit
            )

            # Look for fee information in the response
            # The actual structure depends on Backpack's API response
            # This is a placeholder - adjust based on actual API response
            maker_fee = account_info.get("makerFeeRate")
            taker_fee = account_info.get("takerFeeRate")

            if maker_fee is not None and taker_fee is not None:
                self._maker_fee_percentage = Decimal(str(maker_fee))
                self._taker_fee_percentage = Decimal(str(taker_fee))
            else:
                # If fee rates not available from API, log warning
                # The base class should have default fees set
                self.logger().warning(
                    "Fee rates not available from Backpack API. Using default rates from configuration.",
                )

        except Exception as e:
            self.logger().error(f"Error updating trading fees: {e}")
            # Continue with existing fee configuration if update fails

    async def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: dict[str, Any]):
        """Initialize trading pair symbol mappings from exchange info.

        This method is required by ExchangePyBase to map between Hummingbot's
        trading pair format and the exchange's native symbol format.

        Args:
            exchange_info: Exchange market information containing symbol details
        """
        mapping: bidict[str, str] = bidict()

        # Get symbols from exchange info - Backpack format
        symbols = exchange_info.get("data", exchange_info.get("symbols", []))

        for symbol_info in symbols:
            if isinstance(symbol_info, dict):
                exchange_symbol = symbol_info.get("symbol")
                if exchange_symbol:
                    # Convert exchange symbol to Hummingbot format
                    trading_pair = await self.trading_pair_associated_to_exchange_symbol(exchange_symbol)
                    if trading_pair:
                        mapping[exchange_symbol] = trading_pair

        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict: dict[str, Any]) -> list[TradingRule]:
        """Format trading rules from exchange info.

        Args:
            exchange_info_dict: Exchange market information

        Returns:
            List of TradingRule objects
        """
        trading_rules = []

        for symbol_info in exchange_info_dict.get("data", []):
            try:
                symbol = symbol_info.get("symbol")
                if not symbol:
                    continue

                # Convert exchange format to Hummingbot format
                trading_pair = symbol.replace("_", "-")

                # Extract trading rule parameters - trust exchange data
                min_base_size = Decimal(str(symbol_info["minQuantity"]))
                min_quote_size = Decimal(str(symbol_info["minQuoteQuantity"]))
                tick_size = Decimal(str(symbol_info["tickSize"]))
                step_size = Decimal(str(symbol_info["stepSize"]))

                # Get max order size from API or use None if not specified
                max_order_size = None
                if "maxQuantity" in symbol_info:
                    max_order_size = Decimal(str(symbol_info["maxQuantity"]))

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_base_size,
                    max_order_size=max_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    min_quote_amount_increment=min_quote_size,
                    min_notional_size=min_quote_size,
                    min_order_value=min_quote_size,
                    max_price_significant_digits=Decimal(str(tick_size)).as_tuple().exponent * -1,
                    supports_limit_orders=True,
                    supports_market_orders=True,
                    buy_order_collateral_token=trading_pair.split("-")[1],
                    sell_order_collateral_token=trading_pair.split("-")[0],
                )

                trading_rules.append(trading_rule)

            except Exception as e:
                self.logger().error(f"Error parsing trading rule for {symbol}: {e}")
                continue

        return trading_rules

    # User stream event processing
    async def _user_stream_event_listener(self):
        """Listen for user stream events and process them."""
        async for event_message in self._iter_user_event_queue():
            try:
                message_type = event_message.get("message_type")

                if message_type == "order_update":
                    self._process_order_message(event_message)
                elif message_type == "balance_update":
                    self._process_balance_message(event_message)
                elif message_type == "trade_update":
                    self._process_trade_message(event_message)

            except Exception:
                self.logger().error("Error processing user stream event", exc_info=True)

    def _process_order_message(self, order_msg: dict[str, Any]):
        """Process order update message from user stream."""
        try:
            data = order_msg.get("data", {})
            client_order_id = data.get("clientId")

            if not client_order_id:
                return

            tracked_order = self._order_tracker.fetch_order(client_order_id)
            if not tracked_order:
                return

            order_state = CONSTANTS.ORDER_STATE_MAP.get(data["status"], "OPEN")
            new_state = getattr(OrderState, order_state)

            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=data.get("orderId"),
                trading_pair=tracked_order.trading_pair,
                # WebSocket timestamps are in microseconds, convert to seconds
                update_timestamp=data.get("timestamp", self.current_timestamp * 1_000_000) / 1_000_000,
                new_state=new_state,
                misc_updates={
                    "fill_price": Decimal(data.get("price", "0")),
                    "executed_amount_base": Decimal(data.get("executedQuantity", "0")),
                },
            )

            self._order_tracker.process_order_update(order_update)

        except Exception:
            self.logger().error(f"Error processing order message: {order_msg}", exc_info=True)

    def _process_balance_message(self, balance_msg: dict[str, Any]):
        """Process balance update message from user stream."""
        try:
            data = balance_msg.get("data", {})
            asset = data.get("asset")

            if asset:
                free_balance = Decimal(data.get("free", "0"))
                total_balance = Decimal(data.get("total", "0"))

                self._account_available_balances[asset] = free_balance
                self._account_balances[asset] = total_balance

        except Exception:
            self.logger().error(f"Error processing balance message: {balance_msg}", exc_info=True)

    def _process_trade_message(self, trade_msg: dict[str, Any]):
        """Process trade update message from user stream."""
        try:
            data = trade_msg.get("data", {})
            client_order_id = data.get("clientId")

            if not client_order_id:
                return

            tracked_order = self._order_tracker.fetch_order(client_order_id)
            if not tracked_order:
                return

            fill_price = Decimal(data["price"])
            fill_base_amount = Decimal(data["quantity"])
            fill_quote_amount = fill_price * fill_base_amount

            trade_update = TradeUpdate(
                trade_id=data["tradeId"],
                client_order_id=client_order_id,
                exchange_order_id=data.get("orderId"),
                trading_pair=tracked_order.trading_pair,
                # WebSocket timestamps are in microseconds, convert to seconds
                fill_timestamp=data.get("timestamp", self.current_timestamp * 1_000_000) / 1_000_000,
                fill_price=fill_price,
                fill_base_amount=fill_base_amount,
                fill_quote_amount=fill_quote_amount,
                fee=self._get_trade_fee(
                    base_currency=tracked_order.base_asset,
                    quote_currency=tracked_order.quote_asset,
                    order_type=tracked_order.order_type,
                    order_side=tracked_order.trade_type,
                    amount=fill_base_amount,
                    price=fill_price,
                    fee_asset=data.get("feeAsset"),
                    fee_amount=Decimal(data.get("fee", "0")),
                ),
            )

            self._order_tracker.process_trade_update(trade_update)

        except Exception:
            self.logger().error(f"Error processing trade message: {trade_msg}", exc_info=True)
