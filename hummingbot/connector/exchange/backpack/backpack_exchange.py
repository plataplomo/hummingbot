"""Backpack Exchange connector for Hummingbot.
Main exchange class implementing all required trading functionality.
"""

import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional, cast

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
from hummingbot.connector.utils import combine_to_hb_trading_pair
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
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackExchange(ExchangePyBase):
    """Backpack exchange connector implementing all required Hummingbot functionality.

    Features:
    - Order placement and cancellation
    - Balance and position tracking
    - Trading rules management
    - Real-time data via WebSocket
    - Ed25519 authentication
    """

    # Increase interval to respect fills endpoint rate limit (60 requests per 2 minutes)
    # Setting to 120 seconds ensures we don't hit the rate limit
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 120.0
    web_utils = cast(Any, web_utils)  # Module assignment for base class

    def __init__(
        self,
        backpack_api_key: str,
        backpack_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize Backpack exchange connector.

        Args:
            backpack_api_key: API key for authentication
            backpack_api_secret: API secret for authentication
            balance_asset_limit: Per-asset balance limits for budget checks
            rate_limits_share_pct: Rate limits allocation percent
            trading_pairs: List of trading pairs to track
            trading_required: Whether trading functionality is required
            domain: Exchange domain
        """
        self.backpack_api_key = backpack_api_key
        self.backpack_api_secret = backpack_api_secret
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._trading_pair_symbol_map: dict[str, str] | None = None

        # ID mapper for Backpack's numeric client ID requirement
        self._id_mapper = utils.BackpackIDMapper()

        super().__init__(balance_asset_limit, rate_limits_share_pct)

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
    def ready(self) -> bool:
        """Check if the connector is ready to operate."""
        return all(self.status_dict.values())

    def _is_user_stream_initialized(self) -> bool:
        """Check if user stream is initialized.

        For Backpack, we consider the stream initialized if we have a data source,
        regardless of whether messages have been received yet, since the private
        stream may not send messages until there are actual order updates.
        """
        return (
            self._user_stream_tracker is not None
            and self._user_stream_tracker.data_source is not None
        ) or not self.is_trading_required

    @property
    def authenticator(self) -> BackpackAuth:
        """Get authenticator instance."""
        return BackpackAuth(
            api_key=self.backpack_api_key,
            api_secret=self.backpack_api_secret,
            time_provider=self._time_synchronizer,
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
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

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
        return CONSTANTS.TIME_URL

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
        # Check the string representation first
        error_str = str(status_update_exception).lower()

        # Check for order not found patterns in the error message
        if "order not found" in error_str or "order does not exist" in error_str:
            return True

        # Check for specific error codes
        if CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str:
            return True

        # Try to extract error code from JSON-like string patterns
        # Look for patterns like {"code": "ORDER_NOT_FOUND"} in the error string
        code_match = re.search(r'"code"\s*:\s*"([^"]+)"', error_str)
        return bool(code_match and code_match.group(1).upper() == CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if exception indicates order not found during cancellation."""
        # Check the string representation first
        error_str = str(cancelation_exception).lower()

        # Check for order not found patterns in the error message
        if "order not found" in error_str or "order does not exist" in error_str:
            return True

        # Check for specific error codes
        if CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str:
            return True

        # Try to extract error code from JSON-like string patterns
        # Look for patterns like {"code": "ORDER_NOT_FOUND"} in the error string
        code_match = re.search(r'"code"\s*:\s*"([^"]+)"', error_str)
        return bool(code_match and code_match.group(1).upper() == CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND)

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
        data: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute DELETE request to Backpack API."""
        # For DELETE, Backpack expects data in body, not params
        return await self._api_request(
            method=RESTMethod.DELETE,
            path_url=path_url,
            params=params,
            data=data,
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
            # Cast response to Any because execute_request can actually return list
            # despite its type hint only showing Union[str, Dict[str, Any]]
            response = cast(
                Any,
                await rest_assistant.execute_request(
                    url=url,
                    method=method,
                    params=params,
                    data=data,
                    is_auth_required=is_auth_required,
                    headers=headers,
                    throttler_limit_id=limit_id,
                ),
            )

            # Response from execute_request can be dict, str, or list
            # The REST assistant's execute_request can return list when response.json() is a list
            if isinstance(response, dict):
                return response
            elif isinstance(response, str):
                # Handle string responses by parsing JSON
                try:
                    parsed = json.loads(response)
                    if isinstance(parsed, dict):
                        return parsed
                    # Wrap non-dict JSON results (list, etc.)
                    return {"data": parsed}
                except json.JSONDecodeError as e:
                    return {"error": f"JSON decode failed: {e!s}", "raw": response}
            elif isinstance(response, list):
                # Response is a list (when API returns JSON array directly)
                # Wrap it in a dict to maintain consistent return type
                return {"data": response}
            else:
                # Fallback for any unexpected type - should not happen in practice
                return {"error": "Unexpected response type", "data": response}

    # Network check
    async def check_network(self) -> NetworkStatus:
        """Check network connectivity to Backpack."""
        try:
            await self._api_get(path_url=self.check_network_request_path)
            return NetworkStatus.CONNECTED
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    # Trading rules are now handled by _format_trading_rules which is called by the base class

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
        try:
            # Update order fills from trades if needed
            await self._update_order_fills_from_trades()
        except Exception as e:
            self.logger().error(f"Error in _update_order_fills_from_trades: {e}", exc_info=True)

        # Call parent implementation
        await super()._status_polling_loop_fetch_updates()

    async def _update_order_fills_from_trades(self):
        """Update order fills from recent trades.
        This is used to ensure we capture all fills even if WebSocket messages are missed.
        """
        # Skip if no active orders or if UPDATE_ORDER_STATUS_MIN_INTERVAL is not set
        if not self._order_tracker.active_orders or self.UPDATE_ORDER_STATUS_MIN_INTERVAL <= 0:
            return

        last_tick = (
            int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
            if self._last_poll_timestamp > 0
            else 0
        )
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick:
            # Group orders by trading pair for efficient querying
            trading_pairs_to_order_map: dict[str, dict[str, Any]] = {}
            for order in self._order_tracker.active_orders.values():
                if order.exchange_order_id is not None:
                    if order.trading_pair not in trading_pairs_to_order_map:
                        trading_pairs_to_order_map[order.trading_pair] = {}
                    trading_pairs_to_order_map[order.trading_pair][order.exchange_order_id] = order

            trading_pairs = list(trading_pairs_to_order_map.keys())

            # Fetch fills for each trading pair - the throttler will handle rate limiting
            tasks = []
            for trading_pair in trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                tasks.append(
                    self._api_get(
                        path_url=CONSTANTS.FILLS_URL,
                        params={"symbol": exchange_symbol},
                        is_auth_required=True,
                        limit_id=CONSTANTS.FILLS_URL,  # Throttler enforces 1 req per 2 sec
                    ),
                )

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            # safe_gather will run async but throttler will serialize based on rate limits
            results = await safe_gather(*tasks, return_exceptions=True)

            for result, trading_pair in zip(results, trading_pairs, strict=False):
                order_map = trading_pairs_to_order_map.get(trading_pair)

                if isinstance(result, Exception):
                    self.logger().network(
                        f"Error fetching fills update for {trading_pair}: {result}.",
                        app_warning_msg=f"Failed to fetch fill updates for {trading_pair}.",
                    )
                    continue

                if not isinstance(result, dict) or "fills" not in result:
                    continue
                fills_data = result["fills"]

                for fill in fills_data:
                    if "orderId" not in fill:
                        self.logger().error(f"Fill missing orderId: {fill}")
                        continue
                    order_id = str(fill["orderId"])
                    if order_map and order_id in order_map:
                        tracked_order: InFlightOrder = order_map[order_id]

                        # Validate required fields exist (no fallbacks for critical data)
                        trade_id = fill.get("tradeId")
                        if not trade_id:
                            self.logger().error(f"Fill missing tradeId for order {order_id}: {fill}")
                            continue

                        quantity = fill.get("quantity")
                        price = fill.get("price")
                        timestamp = fill.get("timestamp")
                        side = fill.get("side")

                        if not all([quantity, price, timestamp, side]):
                            self.logger().error(
                                f"Fill missing required fields for order {order_id}. "
                                f"quantity={quantity}, price={price}, timestamp={timestamp}, side={side}",
                            )
                            continue

                        # Validate side value
                        if side not in ["Buy", "Sell"]:
                            self.logger().error(f"Invalid side value '{side}' for order {order_id}")
                            continue

                        # Extract fee information inline (following Binance/Bybit pattern)
                        # Note: fee might be 0 for maker orders, so we don't require it
                        # Fee data is required for proper accounting
                        if "fee" not in fill or "feeSymbol" not in fill:
                            self.logger().error(f"Fill missing fee data for order {order_id}: {fill}")
                            continue
                        fee_amount = Decimal(str(fill["fee"]))
                        fee_asset = fill["feeSymbol"]

                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=TradeType.BUY if side == "Buy" else TradeType.SELL,
                            percent_token=fee_asset,
                            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)] if fee_amount > 0 else [],
                        )

                        # Create trade update from fill data
                        fill_base_amount = Decimal(str(quantity))
                        fill_price = Decimal(str(price))

                        trade_update = TradeUpdate(
                            trade_id=str(trade_id),
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=order_id,
                            trading_pair=tracked_order.trading_pair,
                            fee=fee,
                            fill_base_amount=fill_base_amount,
                            fill_quote_amount=fill_base_amount * fill_price,
                            fill_price=fill_price,
                            fill_timestamp=timestamp * 1e-3,  # Backpack uses milliseconds
                        )
                        self._order_tracker.process_trade_update(trade_update)

    # Balance management
    async def _update_balances(self):
        """Update account balances using collateral endpoint as single source of truth."""
        try:
            # Get collateral balances - this includes everything (spot + auto-lent)
            collateral_data = await self._api_get(
                path_url=CONSTANTS.COLLATERAL_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.BALANCES_URL,
            )

            # Check if we got valid data
            if not collateral_data or "collateral" not in collateral_data:
                self.logger().error(f"Invalid collateral data received: {collateral_data}")
                return

            # Initialize balance dictionaries
            self._account_balances.clear()
            self._account_available_balances.clear()

            # Use collateral endpoint as the single source of truth
            # It already includes both spot and auto-lent balances
            for asset_info in collateral_data["collateral"]:
                symbol = asset_info["symbol"]
                total_quantity = Decimal(asset_info["totalQuantity"])
                available_quantity = Decimal(asset_info["availableQuantity"])
                lend_quantity = Decimal(asset_info.get("lendQuantity", "0"))

                # Total balance is totalQuantity
                self._account_balances[symbol] = total_quantity

                # Lent funds are ALWAYS available for trading on Backpack
                # When you place an order, Backpack automatically unlends what's needed
                # So we add lendQuantity to availableQuantity to get true available balance
                self._account_available_balances[symbol] = available_quantity + lend_quantity
        except Exception as e:
            self.logger().error(f"Failed to update balances: {e}", exc_info=True)

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

        # Convert Hummingbot string ID to Backpack numeric ID
        numeric_client_id = self._id_mapper.get_numeric_id(order_id)

        order_data = {
            "symbol": exchange_symbol,
            "side": utils.backpack_order_side(trade_type),
            "quantity": str(amount),
            "clientId": numeric_client_id,  # Backpack expects integer clientId
        }

        # Handle LIMIT_MAKER by converting to LIMIT with postOnly flag
        if order_type == OrderType.LIMIT_MAKER:
            order_data["orderType"] = "Limit"
            order_data["price"] = str(price)
            order_data["postOnly"] = True
        elif order_type == OrderType.LIMIT:
            order_data["orderType"] = "Limit"
            order_data["price"] = str(price)  # Price is required for limit orders
            # Add time in force if specified
            time_in_force = kwargs.get("time_in_force")
            if time_in_force:
                # Time in force must be in the map
                if time_in_force not in CONSTANTS.TIME_IN_FORCE_MAP:
                    raise ValueError(f"Invalid time in force: {time_in_force}")
                order_data["timeInForce"] = CONSTANTS.TIME_IN_FORCE_MAP[time_in_force]
        else:  # MARKET order
            order_data["orderType"] = "Market"

        try:
            response = await self._api_post(
                path_url=CONSTANTS.ORDER_URL,
                data=order_data,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_URL,
            )

            # Backpack returns 'id' field, not 'orderId'
            if "id" in response:
                exchange_order_id = response["id"]
            elif "orderId" in response:
                exchange_order_id = response["orderId"]
            else:
                self.logger().error(f"Unexpected order response format: {response}")
                if "error" in response:
                    raise Exception(f"Order placement failed: {response['error']}")
                raise Exception(f"No order ID in response: {response}")

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
                data=cancel_data,  # Pass as body data, not params
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
            # Skip if order doesn't have an exchange ID (failed to submit)
            if not order.exchange_order_id:
                return []

            exchange_symbol = utils.convert_to_exchange_trading_pair(order.trading_pair)

            response = await self._api_get(
                path_url=CONSTANTS.FILLS_URL,
                params={
                    "symbol": exchange_symbol,
                    "orderId": order.exchange_order_id,
                },
                is_auth_required=True,
                limit_id=CONSTANTS.FILLS_URL,
            )

            trade_updates: list[TradeUpdate] = []

            # Since _api_get wraps list responses in {"data": [...]}
            # we need to extract the actual fills list
            if "data" in response and isinstance(response["data"], list):
                fills_data = response["data"]
            else:
                self.logger().error(f"Unexpected fills response format: {response}")
                return []

            # If empty list, no fills for this order
            if not fills_data:
                return []

            for fill in fills_data:
                trade_update = TradeUpdate(
                    trade_id=fill["tradeId"],
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id or "",  # Handle None case
                    trading_pair=order.trading_pair,
                    # Backpack fills API returns ISO format timestamps or microseconds
                    fill_timestamp=utils.parse_fill_timestamp(fill.get("timestamp")),
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
                        fee_amount=Decimal(fill["fee"]) if "fee" in fill else Decimal(0),  # Fee might be 0 for maker
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
            # If order doesn't have an exchange ID, it failed to submit
            if not tracked_order.exchange_order_id:
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=None,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.FAILED,
                )

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

            # Status must be in our mapping
            if order_data["status"] not in CONSTANTS.ORDER_STATE_MAP:
                self.logger().error(f"Unknown order status: {order_data['status']}")
                # Return a failed order update to indicate error
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.FAILED,
                )
            order_state_str = CONSTANTS.ORDER_STATE_MAP[order_data["status"]]

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState[order_state_str],
                misc_updates={
                    "fill_price": Decimal(order_data["price"]),
                    "executed_amount_base": Decimal(order_data["executedQuantity"]),
                    "executed_amount_quote": (
                        Decimal(order_data["executedQuantity"]) * Decimal(order_data["price"])
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

            # Extract fee information from actual Backpack API response
            # Fee information is critical for proper order execution
            if "spotMakerFee" not in account_info or "spotTakerFee" not in account_info:
                self.logger().error(f"Missing fee information in account data: {account_info}")
                return
            spot_maker_fee = account_info["spotMakerFee"]  # Returns as integer (e.g., 8 = 0.08%)
            spot_taker_fee = account_info["spotTakerFee"]  # Returns as integer (e.g., 10 = 0.10%)

            if spot_maker_fee is not None and spot_taker_fee is not None:
                # Convert from integer percentage to decimal (8 -> 0.0008)
                self._maker_fee_percentage = Decimal(str(spot_maker_fee)) / Decimal(10000)
                self._taker_fee_percentage = Decimal(str(spot_taker_fee)) / Decimal(10000)
            else:
                # If fee rates not available from API, log warning
                # The base class should have default fees set
                self.logger().warning(
                    "Fee rates not available from Backpack API. Using default rates from configuration.",
                )

        except Exception as e:
            self.logger().error(f"Error updating trading fees: {e}")
            # Continue with existing fee configuration if update fails

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: dict[str, Any]):
        """Initialize trading pair symbol mappings from exchange info."""
        mapping: bidict[str, str] = bidict()

        # Handle wrapped response from _api_request
        if "data" not in exchange_info:
            self.logger().error(f"Missing data in exchange info: {exchange_info}")
            return
        symbols_list = exchange_info["data"]

        for symbol_info in symbols_list:
            if symbol_info["marketType"] == "SPOT":
                exchange_symbol = symbol_info["symbol"]
                base_asset = symbol_info["baseSymbol"]
                quote_asset = symbol_info["quoteSymbol"]

                trading_pair = combine_to_hb_trading_pair(base=base_asset, quote=quote_asset)
                mapping[exchange_symbol] = trading_pair

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get the last traded price for a trading pair."""
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        resp_json = await self._api_get(
            path_url=CONSTANTS.TICKER_URL,
            params={"symbol": exchange_symbol},
        )

        # Backpack ticker response has 'lastPrice' field
        return float(resp_json["lastPrice"])

    async def get_last_traded_prices(self, trading_pairs: list[str]) -> dict[str, float]:
        """Get last traded prices for multiple trading pairs."""
        async def fetch_price(trading_pair: str) -> tuple[str, float | None]:
            try:
                price = await self._get_last_traded_price(trading_pair)
                return trading_pair, price
            except Exception as e:
                self.logger().error(f"Error fetching last price for {trading_pair}: {e}")
                return trading_pair, None

        results = await asyncio.gather(*[fetch_price(tp) for tp in trading_pairs])
        return {pair: price for pair, price in results if price is not None}

    async def _format_trading_rules(self, exchange_info_dict: dict[str, Any]) -> list[TradingRule]:
        """Format trading rules from exchange info.

        Args:
            exchange_info_dict: Exchange market information

        Returns:
            List of TradingRule objects
        """
        # Update trading fees from account endpoint
        await self._update_trading_fees()
        trading_rules = []

        # Handle response which comes as {"data": list} after our wrapping
        if not isinstance(exchange_info_dict, dict) or "data" not in exchange_info_dict:
            self.logger().error(f"Invalid exchange info format: {exchange_info_dict}")
            return []
        market_list = exchange_info_dict["data"]
        for symbol_info in market_list:
            try:
                # Only process SPOT markets in the spot connector
                # Market type is required to filter spot vs perpetual
                if "marketType" not in symbol_info:
                    self.logger().error(f"Missing marketType in symbol info: {symbol_info}")
                    continue
                market_type = symbol_info["marketType"]
                if market_type != "SPOT":
                    continue

                symbol = symbol_info.get("symbol")
                if not symbol:
                    continue

                # Convert exchange format to Hummingbot format
                trading_pair = symbol.replace("_", "-")

                # Extract filters
                # Filters are required for trading rules
                if "filters" not in symbol_info:
                    self.logger().error(f"Missing filters in symbol info: {symbol_info}")
                    continue
                filters = symbol_info["filters"]

                if "price" not in filters or "quantity" not in filters:
                    self.logger().error(f"Missing price or quantity filters: {filters}")
                    continue
                price_filter = filters["price"]
                quantity_filter = filters["quantity"]

                # Extract trading rule parameters from filters - all are required
                if "minQuantity" not in quantity_filter or "stepSize" not in quantity_filter:
                    self.logger().error(f"Missing quantity filter fields: {quantity_filter}")
                    continue
                if "tickSize" not in price_filter or "minPrice" not in price_filter:
                    self.logger().error(f"Missing price filter fields: {price_filter}")
                    continue

                min_base_size = Decimal(str(quantity_filter["minQuantity"]))
                tick_size = Decimal(str(price_filter["tickSize"]))
                step_size = Decimal(str(quantity_filter["stepSize"]))
                min_price = Decimal(str(price_filter["minPrice"]))
                min_quote_size = min_base_size * min_price

                # Get max order size from API or use None if not specified
                max_order_size = None
                if quantity_filter.get("maxQuantity"):
                    max_order_size = Decimal(str(quantity_filter["maxQuantity"]))

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

        self.logger().info(f"Formatted {len(trading_rules)} trading rules")
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
            if "data" not in order_msg:
                self.logger().error(f"Missing data in order message: {order_msg}")
                return
            data = order_msg["data"]
            # WebSocket uses abbreviated field names: 'c' for clientId
            # Extract numeric client ID from WebSocket
            numeric_client_id = None
            if "c" in data:
                numeric_client_id = int(data["c"])
            elif "clientId" in data:
                numeric_client_id = int(data["clientId"])
            else:
                self.logger().error(f"Missing clientId (c) in data: {data}")
                return

            # Convert numeric ID back to Hummingbot string ID
            client_order_id = self._id_mapper.get_hb_id(numeric_client_id)
            if not client_order_id:
                # This might be an order from a different session or placed outside of Hummingbot
                self.logger().debug(f"Unknown numeric client ID: {numeric_client_id}")
                return

            tracked_order = self._order_tracker.fetch_order(client_order_id)
            if not tracked_order:
                return

            # WebSocket uses abbreviated fields: X=status, i=orderId, T=timestamp
            status = data.get("X") or data.get("status")
            if not status or status not in CONSTANTS.ORDER_STATE_MAP:
                self.logger().error(f"Unknown order status in WebSocket: {status}")
                return
            order_state = CONSTANTS.ORDER_STATE_MAP[status]
            new_state = getattr(OrderState, order_state)

            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=data.get("i") or data.get("orderId"),
                trading_pair=tracked_order.trading_pair,
                # WebSocket timestamps are in microseconds, convert to seconds
                update_timestamp=(
                    data.get("T") or data.get("timestamp", self.current_timestamp * 1_000_000)
                ) / 1_000_000,
                new_state=new_state,
                misc_updates={
                    "fill_price": Decimal(data.get("p") or data.get("price", 0)),
                    "executed_amount_base": Decimal(data.get("z") or data.get("executedQuantity", 0)),
                },
            )

            self._order_tracker.process_order_update(order_update)

        except Exception:
            self.logger().error(f"Error processing order message: {order_msg}", exc_info=True)

    def _process_balance_message(self, balance_msg: dict[str, Any]):
        """Process balance update message from user stream."""
        try:
            if "data" not in balance_msg:
                self.logger().error(f"Missing data in balance message: {balance_msg}")
                return
            data = balance_msg["data"]

            if "asset" not in data:
                self.logger().error(f"Missing asset in balance data: {data}")
                return
            asset = data["asset"]

            # Balance fields are required
            if "free" not in data or "total" not in data:
                self.logger().error(f"Missing balance fields for {asset}: {data}")
                return
            free_balance = Decimal(data["free"])
            total_balance = Decimal(data["total"])

            self._account_available_balances[asset] = free_balance
            self._account_balances[asset] = total_balance

        except Exception:
            self.logger().error(f"Error processing balance message: {balance_msg}", exc_info=True)

    def _process_trade_message(self, trade_msg: dict[str, Any]):
        """Process trade update message from user stream."""
        try:
            if "data" not in trade_msg:
                self.logger().error(f"Missing data in trade message: {trade_msg}")
                return
            data = trade_msg["data"]
            # WebSocket uses abbreviated field names: 'c' for clientId
            # Extract numeric client ID from WebSocket
            numeric_client_id = None
            if "c" in data:
                numeric_client_id = int(data["c"])
            elif "clientId" in data:
                numeric_client_id = int(data["clientId"])
            else:
                self.logger().error(f"Missing clientId (c) in data: {data}")
                return

            # Convert numeric ID back to Hummingbot string ID
            client_order_id = self._id_mapper.get_hb_id(numeric_client_id)
            if not client_order_id:
                # This might be an order from a different session or placed outside of Hummingbot
                self.logger().debug(f"Unknown numeric client ID: {numeric_client_id}")
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
