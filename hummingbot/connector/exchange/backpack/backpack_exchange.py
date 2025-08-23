"""
Backpack Exchange connector for Hummingbot.
Main exchange class implementing all required trading functionality.
"""

import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_utils as utils,
    backpack_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import BackpackAPIUserStreamDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from bidict import bidict

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BackpackExchange(ExchangePyBase):
    """
    Backpack exchange connector implementing all required Hummingbot functionality.

    Features:
    - Order placement and cancellation
    - Balance and position tracking
    - Trading rules management
    - Real-time data via WebSocket
    - Ed25519 authentication
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    HEARTBEAT_TIME_INTERVAL = 30.0

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_api_key: str,
        backpack_api_secret: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """
        Initialize Backpack exchange connector.

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
        self._last_trades_poll_timestamp = 1.0
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
        """
        Convert Hummingbot trading pair format to Backpack exchange format.
        
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
            time_provider=self._time_synchronizer.time
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
    def trading_pairs(self) -> List[str]:
        """Get list of trading pairs."""
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Whether cancel requests are synchronous."""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Whether trading functionality is required."""
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        """Get supported order types."""
        return [OrderType.LIMIT, OrderType.MARKET]

    # Factory methods
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory."""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Create order book data source."""
        return BackpackAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Create user stream data source."""
        return BackpackAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain
        )

    # Error handling
    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """Check if exception is related to time synchronization."""
        error_description = str(request_exception).lower()
        return (
            "timestamp" in error_description or
            "expired" in error_description or
            CONSTANTS.ERROR_CODE_EXPIRED_TIMESTAMP.lower() in error_description
        )

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if exception indicates order not found during status update."""
        error_str = str(status_update_exception).lower()
        return (
            CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str or
            "order not found" in error_str
        )

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if exception indicates order not found during cancellation."""
        error_str = str(cancelation_exception).lower()
        return (
            CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND.lower() in error_str or
            "order not found" in error_str
        )

    # API helper methods
    async def _api_get(
        self,
        path_url: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute GET request to Backpack API."""
        return await self._api_request(
            method=RESTMethod.GET,
            path_url=path_url,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id
        )

    async def _api_post(
        self,
        path_url: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute POST request to Backpack API."""
        return await self._api_request(
            method=RESTMethod.POST,
            path_url=path_url,
            data=data,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id
        )

    async def _api_delete(
        self,
        path_url: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute DELETE request to Backpack API."""
        return await self._api_request(
            method=RESTMethod.DELETE,
            path_url=path_url,
            params=params,
            is_auth_required=is_auth_required,
            limit_id=limit_id
        )

    async def _api_request(
        self,
        method: RESTMethod,
        path_url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute API request with proper error handling and rate limiting.

        Args:
            method: HTTP method
            path_url: API endpoint path
            params: Query parameters
            data: Request body data
            is_auth_required: Whether authentication is required
            limit_id: Rate limit identifier

        Returns:
            API response data
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        if limit_id is None:
            limit_id = path_url

        url = web_utils.get_rest_url_for_endpoint(path_url, self._domain)

        async with self._throttler.execute_task(limit_id=limit_id):
            response = await rest_assistant.execute_request(
                url=url,
                method=method,
                params=params,
                data=json.dumps(data) if data else None,
                is_auth_required=is_auth_required
            )

            if response.status != 200:
                error_data = await response.json()
                error_msg = error_data.get("msg", f"HTTP {response.status}")
                raise IOError(f"API request failed: {error_msg}")

            return await response.json()

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
                limit_id=CONSTANTS.EXCHANGE_INFO_URL
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
        """
        Get the last traded price for a trading pair.
        
        Args:
            trading_pair: Trading pair in Hummingbot format
            
        Returns:
            Last traded price as float
        """
        prices = await self._get_last_traded_prices([trading_pair])
        return float(prices.get(trading_pair, Decimal("0")))
    
    async def _get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, Decimal]:
        """Get last traded prices for specified trading pairs."""
        try:
            ticker_data = await self._api_get(
                path_url=CONSTANTS.TICKER_URL,
                limit_id=CONSTANTS.TICKER_URL
            )

            prices = {}

            if isinstance(ticker_data, list):
                for ticker in ticker_data:
                    exchange_symbol = ticker.get("symbol")
                    if exchange_symbol:
                        trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)
                        if trading_pair in trading_pairs:
                            prices[trading_pair] = Decimal(ticker.get("price", "0"))
            else:
                # Single ticker response
                exchange_symbol = ticker_data.get("symbol")
                if exchange_symbol:
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)
                    if trading_pair in trading_pairs:
                        prices[trading_pair] = Decimal(ticker_data.get("price", "0"))

            return prices

        except Exception as e:
            self.logger().error(f"Error fetching last traded prices: {e}")
            return {}

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = Decimal("NaN"),
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Get fee for an order.
        
        Args:
            base_currency: Base currency of the trading pair
            quote_currency: Quote currency of the trading pair
            order_type: Order type
            order_side: Order side (buy/sell)
            amount: Order amount
            price: Order price
            is_maker: Whether the order is a maker order
            
        Returns:
            TradeFeeBase object with fee information
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))
    
    async def _status_polling_loop_fetch_updates(self):
        """
        Fetch updates for the status polling loop.
        This method is called periodically to update order status.
        """
        # Update order fills from trades if needed
        await self._update_order_fills_from_trades()
        # Call parent implementation
        await super()._status_polling_loop_fetch_updates()
    
    async def _update_order_fills_from_trades(self):
        """
        Update order fills from recent trades.
        This is used to ensure we capture all fills even if WebSocket messages are missed.
        """
        # This method can be implemented if needed for reliability
        # For now, we rely on WebSocket updates and REST status queries
        pass
    
    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbols from exchange info.
        
        Args:
            exchange_info: Exchange market information
        """
        mapping = bidict()
        
        for symbol_data in exchange_info.get("data", []):
            if symbol_data.get("status") == "ACTIVE":
                # Backpack uses underscore format: BTC_USDC
                exchange_symbol = symbol_data["symbol"]
                # Convert to Hummingbot format: BTC-USDC
                if "_" in exchange_symbol:
                    parts = exchange_symbol.split("_")
                    if len(parts) == 2:
                        base_asset = parts[0]
                        quote_asset = parts[1]
                        hb_trading_pair = combine_to_hb_trading_pair(base=base_asset, quote=quote_asset)
                        mapping[exchange_symbol] = hb_trading_pair
        
        self._set_trading_pair_symbol_map(mapping)

    # Balance management
    async def _update_balances(self):
        """Update account balances."""
        try:
            balances_data = await self._api_get(
                path_url=CONSTANTS.BALANCES_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.BALANCES_URL
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
        price: Optional[Decimal] = None,
        **kwargs
    ) -> Tuple[str, float]:
        """
        Place an order on Backpack exchange.

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
            "orderType": self.backpack_order_type(order_type),
            "quantity": str(amount),
            "clientId": order_id,
        }

        if order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for limit orders")
            order_data["price"] = str(price)

        # Add time in force if specified
        time_in_force = kwargs.get("time_in_force")
        if time_in_force:
            order_data["timeInForce"] = CONSTANTS.TIME_IN_FORCE_MAP.get(time_in_force, "GTC")

        try:
            response = await self._api_post(
                path_url=CONSTANTS.ORDER_URL,
                data=order_data,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_URL
            )

            exchange_order_id = response["orderId"]
            timestamp = self.current_timestamp

            return exchange_order_id, timestamp

        except Exception as e:
            self.logger().error(f"Error placing order {order_id}: {e}")
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Cancel an order on Backpack exchange.

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
                data=cancel_data,
                is_auth_required=True,
                limit_id=CONSTANTS.CANCEL_ORDER_URL
            )

            return True

        except Exception as e:
            if self._is_order_not_found_during_cancelation_error(e):
                # Order already cancelled or filled
                return True
            else:
                self.logger().error(f"Error canceling order {order_id}: {e}")
                return False

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """Get all trade updates for a specific order."""
        try:
            exchange_symbol = utils.convert_to_exchange_trading_pair(order.trading_pair)

            fills_data = await self._api_get(
                path_url=CONSTANTS.FILLS_URL,
                params={
                    "symbol": exchange_symbol,
                    "orderId": order.exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.FILLS_URL
            )

            trade_updates = []

            for fill in fills_data.get("fills", []):
                trade_update = TradeUpdate(
                    trade_id=fill["tradeId"],
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    trading_pair=order.trading_pair,
                    fill_timestamp=fill["timestamp"] / 1000,
                    fill_price=Decimal(fill["price"]),
                    fill_base_amount=Decimal(fill["quantity"]),
                    fee=self._get_trade_fee(
                        base_currency=order.base_asset,
                        quote_currency=order.quote_asset,
                        order_type=order.order_type,
                        order_side=order.trade_type,
                        amount=Decimal(fill["quantity"]),
                        price=Decimal(fill["price"]),
                        fee_asset=fill.get("feeAsset", order.quote_asset),
                        fee_amount=Decimal(fill.get("fee", "0"))
                    )
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
                    "orderId": tracked_order.exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_URL
            )

            order_state = CONSTANTS.ORDER_STATE_MAP.get(order_data["status"], OrderState.OPEN)

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=getattr(OrderState, order_state),
                fill_price=Decimal(order_data.get("price", "0")),
                executed_amount_base=Decimal(order_data.get("executedQuantity", "0")),
                executed_amount_quote=Decimal(order_data.get("executedQuantity", "0")) * Decimal(order_data.get("price", "0")),
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
                    new_state=OrderState.CANCELED
                )
            else:
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
        fee_asset: str = None,
        fee_amount: Decimal = None
    ) -> TradeFeeBase:
        """Calculate trade fee for an order."""
        if fee_amount is not None and fee_asset is not None:
            # Use actual fee from trade
            return DeductedFromReturnsTradeFee(
                fee_asset=fee_asset,
                fee_amount=fee_amount
            )

        # Calculate estimated fee (typically 0.1% for Backpack)
        fee_rate = Decimal("0.001")  # 0.1%

        if order_side == TradeType.BUY:
            # Fee paid in base currency
            fee_amount = amount * fee_rate
            fee_asset = base_currency
        else:
            # Fee paid in quote currency
            fee_amount = amount * price * fee_rate
            fee_asset = quote_currency

        return DeductedFromReturnsTradeFee(
            fee_asset=fee_asset,
            fee_amount=fee_amount
        )

    async def _update_trading_fees(self):
        """
        Update trading fees from exchange.
        Note: Backpack typically uses a fixed fee structure
        """
        # Backpack uses fixed fees for all trading pairs
        # Maker: 0.1%, Taker: 0.1%
        pass

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info.
        
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
                
                # Extract trading rule parameters
                min_base_size = Decimal(str(symbol_info.get("minQuantity", "0.001")))
                min_quote_size = Decimal(str(symbol_info.get("minQuoteQuantity", "1")))
                tick_size = Decimal(str(symbol_info.get("tickSize", "0.01")))
                step_size = Decimal(str(symbol_info.get("stepSize", "0.001")))
                
                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_base_size,
                    max_order_size=Decimal("1000000"),  # No max specified by Backpack
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    min_quote_amount_increment=min_quote_size,
                    min_notional_size=min_quote_size,
                    min_order_value=min_quote_size,
                    max_price_significant_digits=Decimal(str(tick_size)).as_tuple().exponent * -1,
                    supports_limit_orders=True,
                    supports_market_orders=True,
                    buy_order_collateral_token=trading_pair.split("-")[1],
                    sell_order_collateral_token=trading_pair.split("-")[0]
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

    def _process_order_message(self, order_msg: Dict[str, Any]):
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
                update_timestamp=data.get("timestamp", self.current_timestamp) / 1000,
                new_state=new_state,
                fill_price=Decimal(data.get("price", "0")),
                executed_amount_base=Decimal(data.get("executedQuantity", "0")),
            )

            self._order_tracker.process_order_update(order_update)

        except Exception:
            self.logger().error(f"Error processing order message: {order_msg}", exc_info=True)

    def _process_balance_message(self, balance_msg: Dict[str, Any]):
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

    def _process_trade_message(self, trade_msg: Dict[str, Any]):
        """Process trade update message from user stream."""
        try:
            data = trade_msg.get("data", {})
            client_order_id = data.get("clientId")

            if not client_order_id:
                return

            tracked_order = self._order_tracker.fetch_order(client_order_id)
            if not tracked_order:
                return

            trade_update = TradeUpdate(
                trade_id=data["tradeId"],
                client_order_id=client_order_id,
                exchange_order_id=data.get("orderId"),
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=data.get("timestamp", self.current_timestamp) / 1000,
                fill_price=Decimal(data["price"]),
                fill_base_amount=Decimal(data["quantity"]),
                fee=self._get_trade_fee(
                    base_currency=tracked_order.base_asset,
                    quote_currency=tracked_order.quote_asset,
                    order_type=tracked_order.order_type,
                    order_side=tracked_order.trade_type,
                    amount=Decimal(data["quantity"]),
                    price=Decimal(data["price"]),
                    fee_asset=data.get("feeAsset"),
                    fee_amount=Decimal(data.get("fee", "0"))
                )
            )

            self._order_tracker.process_trade_update(trade_update)

        except Exception:
            self.logger().error(f"Error processing trade message: {trade_msg}", exc_info=True)
