"""
Backpack Perpetual Exchange connector for Hummingbot.
Main derivative class implementing perpetual futures trading functionality.
"""

import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.client.config.trade_fee_schema_loader import TradeFeeSchemaLoader
from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_utils as utils,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source import (
    BackpackPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_user_stream_data_source import (
    BackpackPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import (
    AccountEvent,
    FundingPaymentCompletedEvent,
    MarketEvent,
    PositionModeChangeEvent,
)
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_perpetual_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BackpackPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Backpack perpetual futures exchange connector implementing derivatives trading.

    Features:
    - Perpetual futures trading with leverage
    - Position tracking and management
    - Funding rate updates
    - Real-time data via WebSocket
    - Ed25519 authentication
    """

    web_utils = web_utils
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_perpetual_api_key: str = None,
        backpack_perpetual_api_secret: str = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """
        Initialize Backpack perpetual derivative connector.

        Args:
            client_config_map: Client configuration
            backpack_perpetual_api_key: API key for authentication
            backpack_perpetual_api_secret: API secret for authentication
            trading_pairs: List of trading pairs to track
            trading_required: Whether trading functionality is required
            domain: Exchange domain
        """
        self.backpack_perpetual_api_key = backpack_perpetual_api_key
        self.backpack_perpetual_api_secret = backpack_perpetual_api_secret
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None  # Initialize as None, set during connection
        self._last_trade_history_timestamp = None
        self._leverage_map: Dict[str, int] = {}  # Store leverage per trading pair
        super().__init__(client_config_map)

    # Required properties from PerpetualDerivativePyBase
    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BackpackPerpetualAuth:
        return BackpackPerpetualAuth(
            self.backpack_perpetual_api_key,
            self.backpack_perpetual_api_secret,
            self._time_synchronizer
        )

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.TIME_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs
    
    def set_leverage(self, trading_pair: str, leverage: int = 1):
        """
        Set leverage for a trading pair (stored locally).
        
        Note: Backpack uses account-wide leverage limits rather than per-position leverage.
        This method stores the leverage locally for use in calculations.
        
        Args:
            trading_pair: The trading pair
            leverage: The leverage value (default 1)
        
        Returns:
            The leverage value that was set
        """
        self._leverage_map[trading_pair] = leverage
        self._perpetual_trading.set_leverage(trading_pair, leverage)
        
        self.logger().info(
            f"Leverage {leverage}x stored locally for {trading_pair}. "
            f"Note: Backpack uses account-wide leverage limits."
        )
        
        return leverage

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        """Poll funding rates every 10 minutes"""
        return CONSTANTS.FUNDING_INFO_UPDATE_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    @property
    def supported_position_modes(self) -> List[PositionMode]:
        """
        Backpack supports ONE-WAY mode only.
        """
        return CONSTANTS.SUPPORTED_POSITION_MODES

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for long positions"""
        return CONSTANTS.COLLATERAL_TOKEN

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for short positions"""
        return CONSTANTS.COLLATERAL_TOKEN

    def trade_fee_schema(self) -> TradeFeeSchema:
        """Returns the trade fee schema for Backpack exchange."""
        return TradeFeeSchemaLoader.configured_schema_for_exchange(exchange_name=self.name)

    # Helper methods for order and trade type conversion
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

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = (
            "timestamp" in error_description.lower()
            or "time" in error_description.lower() and "sync" in error_description.lower()
        )
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return (
            str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(status_update_exception)
            and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)
        )

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return (
            str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(cancelation_exception)
            and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BackpackPerpetualUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type == OrderType.LIMIT_MAKER)
        fee = build_perpetual_trade_fee(
            self.name,
            is_maker,
            position_action=position_action,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    # API request methods
    async def _api_get(
        self,
        path_url: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute GET request."""
        return await web_utils.api_request(
            path=path_url,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            params=params,
            method=RESTMethod.GET,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
        )

    async def _api_post(
        self,
        path_url: str,
        data: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = True,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute POST request."""
        return await web_utils.api_request(
            path=path_url,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            data=json.dumps(data) if data else None,
            method=RESTMethod.POST,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
            headers={"Content-Type": "application/json"},
        )

    async def _api_delete(
        self,
        path_url: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = True,
        limit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute DELETE request."""
        return await web_utils.api_request(
            path=path_url,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            params=params,
            method=RESTMethod.DELETE,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
        )

    # Trading rules and exchange info
    async def _update_trading_rules(self):
        """Fetch and update trading rules from exchange."""
        response = await self._api_get(
            path_url=self.trading_rules_request_path,
            is_auth_required=False
        )

        trading_rules = {}

        for market_info in response.get("symbols", []):
            try:
                # Only process perpetual markets
                if not utils.is_perpetual_symbol(market_info["symbol"]):
                    continue

                trading_pair = utils.convert_from_exchange_trading_pair(market_info["symbol"])
                if trading_pair is None:
                    continue

                # Parse filters according to Backpack API structure
                filters = market_info.get("filters", {})
                price_filter = filters.get("price", {})
                quantity_filter = filters.get("quantity", {})
                notional_filter = filters.get("notional", {})
                
                # Extract required fields - will raise exception if missing
                min_order_size = Decimal(str(quantity_filter["minQuantity"]))
                step_size = Decimal(str(quantity_filter["stepSize"]))
                tick_size = Decimal(str(price_filter["tickSize"]))
                
                # Optional fields
                max_order_size = quantity_filter.get("maxQuantity")
                if max_order_size:
                    max_order_size = Decimal(str(max_order_size))
                
                min_notional = notional_filter.get("minNotional")
                if min_notional:
                    min_notional = Decimal(str(min_notional))
                
                # Create trading rule
                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    buy_order_collateral_token=CONSTANTS.COLLATERAL_TOKEN,
                    sell_order_collateral_token=CONSTANTS.COLLATERAL_TOKEN,
                )
                
                # Add optional fields if present
                if max_order_size:
                    trading_rule.max_order_size = max_order_size
                if min_notional:
                    trading_rule.min_notional_size = min_notional
                
                trading_rules[trading_pair] = trading_rule

            except Exception as e:
                self.logger().error(
                    f"Error parsing trading rule for {market_info.get('symbol', 'unknown')}: {e}. Skipping...",
                    exc_info=True
                )

        self._trading_rules = trading_rules

    # Balance and position management
    async def _update_balances(self):
        """Update account balances.
        
        According to OpenAPI spec, the response format is:
        {"USDC": {"available": "0", "locked": "0", "staked": "0"}, ...}
        """
        response = await self._api_get(
            path_url=CONSTANTS.BALANCE_URL,
            is_auth_required=True
        )

        self._account_available_balances.clear()
        self._account_balances.clear()

        # Response format per OpenAPI spec: {"ASSET": {"available": "0", "locked": "0", "staked": "0"}}
        for asset, balance_info in response.items():
            if isinstance(balance_info, dict):
                available = Decimal(str(balance_info.get("available", "0")))
                locked = Decimal(str(balance_info.get("locked", "0")))
                # Note: staked is in the response but not used for trading
                total = available + locked
                
                self._account_available_balances[asset] = available
                self._account_balances[asset] = total

    async def _update_positions(self):
        """Update open positions."""
        response = await self._api_get(
            path_url=CONSTANTS.POSITIONS_URL,
            is_auth_required=True
        )

        # Handle both list format (direct response) and object format (wrapped in "positions" key)
        positions = response if isinstance(response, list) else response.get("positions", [])
        
        # Track which trading pairs have positions in the response
        trading_pairs_in_response = set()
        
        for position_data in positions:
            symbol = position_data.get("symbol")
            if symbol:
                trading_pair = utils.convert_from_exchange_trading_pair(symbol)
                # Only process positions for trading pairs we're actually tracking
                if trading_pair and trading_pair in self._trading_pairs:
                    trading_pairs_in_response.add(trading_pair)
                    self._process_position_update(position_data)
        
        # Remove positions that are no longer in the response
        current_positions = list(self._perpetual_trading.account_positions.keys())
        for trading_pair in current_positions:
            if trading_pair not in trading_pairs_in_response:
                del self._perpetual_trading.account_positions[trading_pair]

    def _process_position_update(self, position_data: Dict[str, Any]):
        """Process a position update from API or WebSocket."""
        try:
            symbol = position_data.get("symbol") or position_data.get("s")
            if not symbol:
                return
                
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)

            if trading_pair is None:
                return

            # Parse position data - handle both REST API and WebSocket formats
            # REST API uses "netQuantity", WebSocket uses "q", test data uses "size"
            net_quantity = None
            for key in ["netQuantity", "q", "size"]:
                if key in position_data:
                    net_quantity = Decimal(str(position_data[key]))
                    break
            
            if net_quantity is None:
                self.logger().warning(f"No quantity field found in position data: {position_data}")
                return

            # Determine position side - could be from 'side' field or calculated from quantity
            if "side" in position_data:
                is_long = position_data["side"] in ["LONG", "Long"]
            else:
                is_long = net_quantity > 0

            # Skip if no position
            if abs(net_quantity) == Decimal("0"):
                if trading_pair in self._perpetual_trading.account_positions:
                    del self._perpetual_trading.account_positions[trading_pair]
                return

            # Get PnL - handle different field names
            unrealized_pnl = Decimal(str(position_data.get("pnlUnrealized", 
                                                           position_data.get("unrealizedPnl", 
                                                           position_data.get("P", "0")))))
            
            # Get entry price - handle different field names
            entry_price = Decimal(str(position_data.get("entryPrice", 
                                                       position_data.get("B", "0"))))

            position = Position(
                trading_pair=trading_pair,
                position_side=PositionSide.LONG if is_long else PositionSide.SHORT,
                unrealized_pnl=unrealized_pnl,
                entry_price=entry_price,
                amount=abs(net_quantity),
                leverage=self._leverage_map.get(trading_pair, position_data.get("leverage", 1)),
            )

            # Store position
            self._perpetual_trading.account_positions[trading_pair] = position

        except Exception:
            self.logger().exception(f"Error processing position update: {position_data}")

    # Order placement and cancellation
    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        position_action: PositionAction = PositionAction.OPEN,
        **kwargs
    ) -> Tuple[str, float]:
        """
        Place an order on Backpack perpetual.
        """
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        # Determine if this is a reduce-only order
        reduce_only = position_action == PositionAction.CLOSE

        order_data = {
            "symbol": symbol,
            "side": self.backpack_order_side(trade_type),
            "orderType": self.backpack_order_type(order_type),
            "quantity": str(amount),
            "clientId": order_id,
        }

        # Add reduce-only flag for closing positions
        if reduce_only:
            order_data["reduceOnly"] = True

        # Add price for limit orders
        if order_type == OrderType.LIMIT or order_type == OrderType.LIMIT_MAKER:
            order_data["price"] = str(price)

        # Add time in force
        order_data["timeInForce"] = kwargs.get("time_in_force", "GTC")

        # Add post-only flag for maker orders
        if order_type == OrderType.LIMIT_MAKER:
            order_data["postOnly"] = True

        response = await self._api_post(
            path_url=CONSTANTS.ORDER_URL,
            data=order_data,
            is_auth_required=True
        )

        exchange_order_id = response.get("orderId", response.get("id"))
        timestamp = self.current_timestamp

        return exchange_order_id, timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Cancel an order on the exchange.
        """
        symbol = utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)

        try:
            # Try to cancel by exchange order ID if available
            if tracked_order.exchange_order_id:
                params = {
                    "symbol": symbol,
                    "orderId": tracked_order.exchange_order_id,
                }
            else:
                # Fall back to client order ID
                params = {
                    "symbol": symbol,
                    "clientId": order_id,
                }

            await self._api_delete(
                path_url=CONSTANTS.CANCEL_URL,
                params=params,
                is_auth_required=True
            )

            return True

        except Exception:
            # Re-raise all exceptions to let the base class handle them properly
            # The base class will call _is_order_not_found_during_cancelation_error
            raise

    # Order status and trade updates
    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request current order status from exchange.
        """
        symbol = utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)

        # Query open orders
        response = await self._api_get(
            path_url=CONSTANTS.OPEN_ORDERS_URL,
            params={"symbol": symbol},
            is_auth_required=True
        )

        # Look for our order
        for order_data in response:
            if (order_data.get("clientId") == tracked_order.client_order_id or
                    order_data.get("orderId") == tracked_order.exchange_order_id):

                return self._parse_order_update(order_data, tracked_order)

        # If not in open orders, check order history
        history_response = await self._api_get(
            path_url=CONSTANTS.ORDER_HISTORY_URL,
            params={"symbol": symbol, "limit": 50},
            is_auth_required=True
        )

        for order_data in history_response:
            if (order_data.get("clientId") == tracked_order.client_order_id or
                    order_data.get("orderId") == tracked_order.exchange_order_id):

                return self._parse_order_update(order_data, tracked_order)

        # Order not found - assume cancelled
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.CANCELED,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    def _parse_order_update(self, order_data: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        """Parse order data into OrderUpdate."""
        status = order_data.get("status", "")
        state = OrderState[CONSTANTS.ORDER_STATE_MAP.get(status, "FAILED")]

        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=state,
            client_order_id=order_data.get("clientId", tracked_order.client_order_id),
            exchange_order_id=order_data.get("orderId", tracked_order.exchange_order_id),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade fills for an order.
        """
        symbol = utils.convert_to_exchange_trading_pair(order.trading_pair)

        response = await self._api_get(
            path_url=CONSTANTS.FILLS_URL,
            params={
                "symbol": symbol,
                "orderId": order.exchange_order_id,
            },
            is_auth_required=True
        )

        trade_updates = []

        for fill_data in response:
            trade_updates.append(
                TradeUpdate(
                    trade_id=str(fill_data.get("tradeId", fill_data.get("id"))),
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    trading_pair=order.trading_pair,
                    fill_timestamp=fill_data.get("timestamp", self.current_timestamp),
                    fill_price=Decimal(str(fill_data.get("price", "0"))),
                    fill_base_amount=Decimal(str(fill_data.get("quantity", "0"))),
                    fill_quote_amount=Decimal(str(fill_data.get("price", "0"))) * Decimal(str(fill_data.get("quantity", "0"))),
                    fee=self._get_trade_fee_from_fill(fill_data),
                )
            )

        return trade_updates

    def _get_trade_fee_from_fill(self, fill_data: Dict[str, Any]) -> TradeFeeBase:
        """Extract trade fee from fill data."""
        fee_amount = Decimal(str(fill_data.get("fee", "0")))
        fee_asset = fill_data.get("feeAsset", CONSTANTS.COLLATERAL_TOKEN)

        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=PositionAction.OPEN,  # Determine from context if needed
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)]
        )

    # Funding rate management
    async def _update_funding_info(self):
        """Update funding rate information for all trading pairs."""
        tasks = []
        for trading_pair in self._trading_pairs:
            tasks.append(self._fetch_funding_rate(trading_pair))

        funding_infos = await safe_gather(*tasks, return_exceptions=True)

        for trading_pair, funding_info in zip(self._trading_pairs, funding_infos):
            if isinstance(funding_info, Exception):
                self.logger().error(
                    f"Error fetching funding rate for {trading_pair}: {funding_info}"
                )
            else:
                self._perpetual_trading.set_funding_info(trading_pair, funding_info)

    async def _fetch_funding_rate(self, trading_pair: str) -> FundingInfo:
        """Fetch current funding rate for a trading pair."""
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        response = await self._api_get(
            path_url=CONSTANTS.FUNDING_RATE_URL,
            params={"symbol": symbol},
            is_auth_required=False
        )

        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(response.get("indexPrice", "0"))),
            mark_price=Decimal(str(response.get("markPrice", "0"))),
            next_funding_utc_timestamp=int(response.get("nextFundingTime", 0)),
            rate=Decimal(str(response.get("nextFundingRate", "0"))),
        )

    # Leverage management
    async def _execute_set_leverage(self, trading_pair: str, leverage: int):
        """
        Set leverage for a trading pair.
        
        Note: Backpack uses account-wide leverage limits rather than per-position leverage.
        This method updates the account-wide leverage limit.
        """
        # Backpack doesn't support per-position leverage setting
        # Leverage is an account-wide setting (leverageLimit)
        # For now, we'll store the desired leverage locally and log a warning
        
        self._leverage_map[trading_pair] = leverage
        self._perpetual_trading.set_leverage(trading_pair, leverage)
        
        self.logger().warning(
            f"Backpack uses account-wide leverage limits. "
            f"Storing leverage {leverage}x for {trading_pair} locally. "
            f"To change actual leverage, update account settings via the exchange interface."
        )

    async def _execute_set_position_mode(self, mode: PositionMode):
        """
        Set position mode on the exchange.
        Backpack only supports ONE-WAY mode, so this is mostly a no-op.
        """
        if mode != PositionMode.ONEWAY:
            raise ValueError(f"Backpack only supports ONE-WAY position mode, not {mode}")

        self._position_mode = mode
        self.trigger_event(
            AccountEvent.PositionModeChangeSucceeded,
            PositionModeChangeEvent(
                timestamp=self.current_timestamp,
                trading_pair=None,
                position_mode=mode,
            )
        )

    def _set_position_mode(self, mode: PositionMode):
        """
        Synchronously set position mode (for testing).
        Backpack only supports ONE-WAY mode.
        """
        if mode != PositionMode.ONEWAY:
            raise ValueError(f"Backpack only supports ONE-WAY position mode, not {mode}")
        self._position_mode = mode

    # WebSocket event processing
    async def _user_stream_event_listener(self):
        """Process user stream events."""
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")

                if event_type == "order":
                    self._process_order_message(event_message)
                elif event_type == "balance":
                    self._process_balance_message(event_message)
                elif event_type == "position":
                    self._process_position_message(event_message)
                elif event_type == "funding":
                    self._process_funding_payment_message(event_message)
                elif event_type == "liquidation":
                    self._process_liquidation_warning(event_message)

            except Exception:
                self.logger().exception("Error processing user stream event")

    def _process_order_message(self, order_msg: dict[str, Any]):
        """Process order update messages."""
        try:
            # Handle both order updates and trade fills
            data = order_msg.get("data", order_msg)
            
            # Check if this is a trade/fill event
            if "tradeId" in data:
                self._process_trade_fill(data)
            else:
                # Regular order status update
                self._process_order_status_update(data)
                
        except Exception:
            self.logger().exception(f"Error processing order message: {order_msg}")

    def _process_trade_fill(self, fill_data: dict[str, Any]):
        """Process trade fill event."""
        try:
            # Get order by exchange order ID
            exchange_order_id = fill_data.get("orderId")
            if not exchange_order_id:
                return
                
            # Find the tracked order
            tracked_order = None
            for order in self._order_tracker.active_orders.values():
                if order.exchange_order_id == exchange_order_id:
                    tracked_order = order
                    break
                    
            if not tracked_order:
                return
            
            # Determine position action based on order side
            # For one-way mode: BUY opens long/closes short, SELL opens short/closes long
            position_action = tracked_order.position if hasattr(tracked_order, 'position') else PositionAction.NIL
            
            # Get fee details
            fee_amount = Decimal(str(fill_data.get("fee", "0")))
            fee_asset = fill_data.get("feeAsset", tracked_order.quote_asset)
            
            # Create flat_fees list - always include even if fee is zero
            flat_fees = [TokenAmount(token=fee_asset, amount=fee_amount)]
            
            # Create fee object
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=flat_fees
            )
                
            # Create trade update
            trade_update = TradeUpdate(
                trade_id=str(fill_data["tradeId"]),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=fill_data.get("timestamp", self.current_timestamp) / 1000,
                fill_price=Decimal(str(fill_data.get("price", "0"))),
                fill_base_amount=Decimal(str(fill_data.get("quantity", "0"))),
                fill_quote_amount=Decimal(str(fill_data.get("price", "0"))) * Decimal(str(fill_data.get("quantity", "0"))),
                fee=fee
            )
            
            self._order_tracker.process_trade_update(trade_update)
            
        except Exception:
            self.logger().exception(f"Error processing trade fill: {fill_data}")
    
    def _process_order_status_update(self, order_data: dict[str, Any]):
        """Process order status update."""
        try:
            # Handle both REST (clientId) and WebSocket (c) field names
            client_order_id = order_data.get("clientId") or order_data.get("c")
            if not client_order_id:
                # If no client order ID, try to use exchange order ID to find order
                exchange_order_id = order_data.get("orderId") or order_data.get("id") or order_data.get("i")
                if exchange_order_id:
                    # Find order by exchange ID
                    for order in self._order_tracker.active_orders.values():
                        if order.exchange_order_id == exchange_order_id:
                            client_order_id = order.client_order_id
                            break
                
                if not client_order_id:
                    return
                
            tracked_order = self._order_tracker.fetch_order(client_order_id)
            if not tracked_order:
                # For new orders from WebSocket, create an InFlightOrder
                exchange_order_id = order_data.get("orderId") or order_data.get("id") or order_data.get("i")
                if exchange_order_id and order_data.get("status") in ["NEW", "New"]:
                    # Extract order details
                    symbol = order_data.get("symbol") or order_data.get("s")
                    if not symbol:
                        return
                        
                    # Convert symbol to trading pair
                    trading_pair = symbol.replace("_", "-")
                    
                    # Map side
                    side = order_data.get("side") or order_data.get("S")
                    if side in ["Buy", "Bid"]:
                        trade_type = TradeType.BUY
                    elif side in ["Sell", "Ask"]:
                        trade_type = TradeType.SELL
                    else:
                        return
                    
                    # Map order type
                    order_type_str = order_data.get("orderType") or order_data.get("o") or "LIMIT"
                    if "MARKET" in order_type_str.upper():
                        order_type = OrderType.MARKET
                    else:
                        order_type = OrderType.LIMIT
                    
                    # Get price and quantity
                    price = Decimal(str(order_data.get("price") or order_data.get("p") or "0"))
                    quantity = Decimal(str(order_data.get("quantity") or order_data.get("q") or "0"))
                    
                    # Create InFlightOrder
                    order = InFlightOrder(
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=trading_pair,
                        order_type=order_type,
                        trade_type=trade_type,
                        price=price,
                        amount=quantity,
                        creation_timestamp=order_data.get("timestamp", self.current_timestamp)
                    )
                    self._order_tracker.start_tracking_order(order)
                    tracked_order = order
                else:
                    return
                
            # Map order status
            order_state = CONSTANTS.ORDER_STATE_MAP.get(order_data.get("status"), "OPEN")
            new_state = getattr(OrderState, order_state)
            
            # Create order update
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=order_data.get("orderId"),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=order_data.get("timestamp", self.current_timestamp) / 1000,
                new_state=new_state,
            )
            
            self._order_tracker.process_order_update(order_update)
            
        except Exception:
            self.logger().exception(f"Error processing order status: {order_data}")
    
    def _process_balance_message(self, balance_msg: dict[str, Any]):
        """Process balance update messages."""
        try:
            data = balance_msg.get("data", balance_msg)
            
            # Handle different balance message formats
            if "balances" in data:
                # Multiple balance updates
                for balance in data["balances"]:
                    self._update_single_balance(balance)
            else:
                # Single balance update
                self._update_single_balance(data)
                
        except Exception:
            self.logger().exception(f"Error processing balance message: {balance_msg}")
    
    def _update_single_balance(self, balance_data: dict[str, Any]):
        """Update a single asset balance."""
        try:
            asset = balance_data.get("asset") or balance_data.get("symbol")
            if not asset:
                return
                
            # Get balance values
            free_balance = Decimal(str(balance_data.get("free", "0")))
            locked_balance = Decimal(str(balance_data.get("locked", "0")))
            total_balance = free_balance + locked_balance
            
            # Update internal balance tracking
            self._account_available_balances[asset] = free_balance
            self._account_balances[asset] = total_balance
            
        except Exception:
            self.logger().exception(f"Error updating balance: {balance_data}")

    def _process_position_message(self, position_msg: Dict[str, Any]):
        """Process position update messages from WebSocket."""
        try:
            self._process_position_update(position_msg.get("data", position_msg))
        except Exception:
            self.logger().exception("Error processing position update")

    def _process_funding_payment_message(self, funding_msg: Dict[str, Any]):
        """Process funding payment messages."""
        try:
            data = funding_msg.get("data", funding_msg)

            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                self.trigger_event(
                    MarketEvent.FundingPaymentCompleted,
                    FundingPaymentCompletedEvent(
                        timestamp=self.current_timestamp,
                        market=self.name,
                        trading_pair=trading_pair,
                        funding_rate=Decimal(str(data.get("fundingRate", "0"))),
                        payment=Decimal(str(data.get("payment", "0"))),
                    )
                )
        except Exception:
            self.logger().exception("Error processing funding payment")

    def _process_liquidation_warning(self, liquidation_msg: Dict[str, Any]):
        """Process liquidation warning messages."""
        try:
            data = liquidation_msg.get("data", liquidation_msg)

            self.logger().warning(
                f"LIQUIDATION WARNING for {data['symbol']}: "
                f"Mark price {data['markPrice']} approaching "
                f"liquidation price {data['liquidationPrice']}"
            )
        except Exception:
            self.logger().exception("Error processing liquidation warning")

    async def _update_trading_fees(self):
        """Update trading fees from the exchange."""
        # Backpack fees are typically fixed and configured in constants
        # For perpetuals: 0.05% maker, 0.10% taker typically
        pass

    async def _make_trading_pairs_request(self) -> Any:
        """
        Request trading pairs information from the exchange.
        
        Returns:
            Exchange trading pairs info
        """
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        return exchange_info

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info.

        Args:
            exchange_info_dict: Exchange trading pair information

        Returns:
            List of TradingRule objects
        """
        trading_rules = []

        for symbol_info in exchange_info_dict.get("symbols", []):
            try:
                symbol = symbol_info["symbol"]
                # Check for perpetual markets
                if symbol_info.get("contractType") != "PERPETUAL":
                    continue

                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)

                # Extract from filters structure matching Backpack API
                filters = symbol_info["filters"]
                price_filter = filters["price"]
                quantity_filter = filters["quantity"]
                notional_filter = filters.get("notional", {})
                
                # Extract required values - no fallbacks
                min_order_size = Decimal(str(quantity_filter["minQuantity"]))
                tick_size = Decimal(str(price_filter["tickSize"]))
                step_size = Decimal(str(quantity_filter["stepSize"]))
                
                # Optional values
                max_order_size = quantity_filter.get("maxQuantity")
                if max_order_size:
                    max_order_size = Decimal(str(max_order_size))
                
                min_notional = notional_filter.get("minNotional")
                if min_notional:
                    min_notional = Decimal(str(min_notional))

                # Get collateral token from symbol info
                collateral_token = symbol_info.get("quoteCurrency", CONSTANTS.COLLATERAL_TOKEN)

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    buy_order_collateral_token=collateral_token,
                    sell_order_collateral_token=collateral_token,
                )
                
                # Add optional fields if present
                if max_order_size:
                    trading_rule.max_order_size = max_order_size
                if min_notional:
                    trading_rule.min_notional_size = min_notional
                
                trading_rules.append(trading_rule)
            except Exception as e:
                self.logger().error(
                    f"Error parsing trading rule for {symbol_info}. Error: {e}",
                    exc_info=True
                )

        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize the trading pair symbol mapping from exchange info.

        Args:
            exchange_info: Exchange information containing symbol mappings
        """
        mapping = {}

        for symbol_info in exchange_info.get("symbols", []):
            try:
                symbol = symbol_info["symbol"]
                if "_PERP" not in symbol:
                    continue

                # Extract base and quote from symbol (e.g., BTC_PERP -> BTC-USDC)
                base = symbol.replace("_PERP", "")
                quote = symbol_info.get("quoteCurrency", "USDC")

                trading_pair = f"{base}-{quote}"
                mapping[symbol] = trading_pair

            except Exception as e:
                self.logger().error(
                    f"Error parsing symbol mapping for {symbol_info}. Error: {e}",
                    exc_info=True
                )

        self._set_trading_pair_symbol_map(mapping)

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """
        Set the position mode for a trading pair.

        Args:
            mode: The position mode to set
            trading_pair: The trading pair

        Returns:
            Tuple of (success, message)
        """
        # Backpack may not support position mode changes via API
        # Check if the exchange supports this feature

        if mode == PositionMode.HEDGE:
            return False, "Backpack perpetuals only support ONE-WAY position mode"

        # If already in ONEWAY mode, no action needed
        if self.position_mode == PositionMode.ONEWAY:
            return True, "Position mode already set to ONE-WAY"

        # For Backpack, position mode might be fixed
        self._position_mode = PositionMode.ONEWAY
        return True, "Position mode set to ONE-WAY"

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Set leverage for a trading pair.
        
        Note: Backpack uses account-wide leverage limits rather than per-position leverage.
        This method stores the leverage locally for use in calculations.

        Args:
            trading_pair: The trading pair
            leverage: The leverage value

        Returns:
            Tuple of (success, message)
        """
        # Backpack doesn't support per-position leverage setting
        # Leverage is an account-wide setting (leverageLimit)
        # Store the desired leverage locally for calculations
        
        self._leverage_map[trading_pair] = leverage
        self._perpetual_trading.set_leverage(trading_pair, leverage)
        
        msg = (
            f"Leverage {leverage}x stored locally for {trading_pair}. "
            f"Note: Backpack uses account-wide leverage limits."
        )
        self.logger().info(msg)
        
        return True, msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        """
        Fetch the last funding fee payment for a trading pair.

        Args:
            trading_pair: The trading pair

        Returns:
            Tuple of (timestamp, funding_rate, payment_amount)
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        try:
            response = await self._api_get(
                path_url=CONSTANTS.FUNDING_HISTORY_URL,
                params={"symbol": symbol, "limit": 1},
                is_auth_required=True,
            )

            if response and len(response) > 0:
                last_payment = response[0]
                # Convert timestamp from ISO format or milliseconds
                if "intervalEndTimestamp" in last_payment:
                    # Parse ISO timestamp
                    from datetime import datetime
                    timestamp_str = last_payment["intervalEndTimestamp"]
                    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    timestamp = int(dt.timestamp() * 1000)
                else:
                    timestamp = int(last_payment.get("timestamp", 0))
                    
                funding_rate = Decimal(str(last_payment["fundingRate"]))
                # The 'quantity' field represents the payment amount (positive if received, negative if paid)
                payment = Decimal(str(last_payment.get("quantity", last_payment.get("fundingFee", "0"))))
                return timestamp, funding_rate, payment
            else:
                return 0, s_decimal_NaN, s_decimal_NaN

        except Exception as e:
            self.logger().error(
                f"Error fetching last fee payment for {trading_pair}: {e}",
                exc_info=True
            )
            return 0, s_decimal_NaN, s_decimal_NaN

    async def _make_network_check_request(self):
        """
        Make a network check request to verify connectivity.
        This is called by check_network() from the parent class.
        
        Uses TIME endpoint since Backpack's ping returns plain text "pong", not JSON,
        which is incompatible with the standard REST assistant used by Hummingbot.
        """
        await self._api_get(
            path_url=CONSTANTS.TIME_URL,
            is_auth_required=False
        )
