"""Backpack Perpetual Exchange connector for Hummingbot.
Main derivative class implementing perpetual futures trading functionality.
"""

import time
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
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
    """Backpack perpetual futures exchange connector implementing derivatives trading.

    Features:
    - Perpetual futures trading with leverage
    - Position tracking and management
    - Funding rate updates
    - Real-time data via WebSocket
    - Ed25519 authentication
    """

    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_perpetual_api_key: str | None = None,
        backpack_perpetual_api_secret: str | None = None,
        trading_pairs: list[str] | None = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize Backpack perpetual derivative connector.

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
        self._position_mode: PositionMode | None = None  # Initialize as None, set during connection
        self._last_trade_history_timestamp = None
        self._leverage_map: dict[str, int] = {}  # Store leverage per trading pair
        self._market_margin_requirements: dict[str, dict[str, Decimal]] = {}  # Store margin requirements per market
        self._funding_info_cache: dict[str, tuple[FundingInfo, float]] = {}  # Cache funding info with timestamp
        super().__init__(client_config_map)

    # Required properties from PerpetualDerivativePyBase
    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BackpackPerpetualAuth:
        if self.backpack_perpetual_api_key is None or self.backpack_perpetual_api_secret is None:
            raise ValueError("API key and secret are required for authentication")

        # Convert TimeSynchronizer to a callable that returns int (milliseconds)

        def time_provider() -> int:
            if self._time_synchronizer:
                return int(self._time_synchronizer.time() * 1000)
            return int(time.time() * 1000)  # Fallback to system time
        return BackpackPerpetualAuth(
            self.backpack_perpetual_api_key,
            self.backpack_perpetual_api_secret,
            time_provider,
        )

    @property
    def rate_limits_rules(self) -> list[RateLimit]:
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
    def trading_pairs(self) -> list[str]:
        return self._trading_pairs or []

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        """Set leverage for a trading pair (stored locally).

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
            f"Note: Backpack uses account-wide leverage limits.",
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

    def supported_order_types(self) -> list[OrderType]:
        """:return a list of OrderType supported by this connector
        Note: Backpack only supports LIMIT and MARKET order types.
        PostOnly orders are handled via timeInForce parameter with LIMIT orders.
        """
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> list[PositionMode]:
        """Backpack supports ONE-WAY mode only.
        """
        return CONSTANTS.SUPPORTED_POSITION_MODES

    @property
    def position_mode(self) -> PositionMode:
        """Get the current position mode.
        
        Returns:
            Current position mode (always ONEWAY for Backpack)
        """
        if self._position_mode is None:
            self._position_mode = PositionMode.ONEWAY
        return self._position_mode  # Type is guaranteed to be PositionMode after assignment

    def validate_position_mode(self, mode: PositionMode) -> bool:
        """Validate if the given position mode is supported.
        
        Args:
            mode: Position mode to validate
            
        Returns:
            True if mode is supported, False otherwise
        """
        return mode in CONSTANTS.SUPPORTED_POSITION_MODES
    
    def set_position_mode(self, position_mode: PositionMode):
        """Set the position mode for the connector.
        
        Note: Backpack only supports ONEWAY position mode, so this method
        validates the mode and raises an exception if an unsupported mode is requested.
        
        Args:
            position_mode: The position mode to set
            
        Raises:
            ValueError: If the requested position mode is not supported
        """
        if position_mode != PositionMode.ONEWAY:
            raise ValueError(
                f"Backpack perpetual only supports ONEWAY position mode. "
                f"Requested mode: {position_mode}",
            )
        self._position_mode = position_mode
        self.logger().info(f"Position mode set to {position_mode} (only mode supported by Backpack)")

    def start(self, clock: Clock, timestamp: float):
        """Start the connector and initialize position mode."""
        super().start(clock, timestamp)
        if self._domain == CONSTANTS.DEFAULT_DOMAIN and self.is_trading_required:
            # Backpack only supports ONE-WAY mode
            self.set_position_mode(PositionMode.ONEWAY)

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for long positions"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Returns collateral token for short positions"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def get_max_leverage(self, trading_pair: str) -> Decimal:
        """Get the maximum leverage for a trading pair."""
        if trading_pair not in self._market_margin_requirements:
            raise ValueError(
                f"Market margin requirements not available for {trading_pair}. "
                "Please wait for market data to load.",
            )
        return self._market_margin_requirements[trading_pair]["max_leverage"]

    def get_initial_margin_ratio(self, trading_pair: str) -> Decimal:
        """Get the initial margin ratio for a trading pair."""
        if trading_pair not in self._market_margin_requirements:
            raise ValueError(
                f"Market margin requirements not available for {trading_pair}. "
                "Please wait for market data to load.",
            )
        return self._market_margin_requirements[trading_pair]["initial_margin"]

    def get_maintenance_margin_ratio(self, trading_pair: str) -> Decimal:
        """Get the maintenance margin ratio for a trading pair."""
        if trading_pair not in self._market_margin_requirements:
            raise ValueError(
                f"Market margin requirements not available for {trading_pair}. "
                "Please wait for market data to load.",
            )
        return self._market_margin_requirements[trading_pair]["maintenance_margin"]

    def trade_fee_schema(self) -> TradeFeeSchema:
        """Returns the trade fee schema for Backpack exchange."""
        return TradeFeeSchemaLoader.configured_schema_for_exchange(exchange_name=self.name)

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert Hummingbot trading pair format to Backpack exchange format.
        
        Args:
            trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")
            
        Returns:
            Exchange symbol format (e.g., "BTC_PERP")
        """
        # For perpetuals, Backpack uses format like "BTC_PERP"
        # Extract base asset from trading pair (remove quote asset)
        base_asset = trading_pair.split("-", maxsplit=1)[0]
        return f"{base_asset}_PERP"

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
            or ("time" in error_description.lower() and "sync" in error_description.lower())
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
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> BackpackPerpetualAPIOrderBookDataSource:
        return BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs or [],
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BackpackPerpetualUserStreamDataSource(
            auth=self.authenticator,  # Use the property that returns BackpackPerpetualAuth
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        """Fetch updates for the status polling loop.
        This method is called periodically to update order status and positions.
        """
        # Update positions for perpetual trading
        await self._update_positions()
        # Update order fills from trades if needed
        await self._update_order_fills_from_trades()
        # Call parent implementation
        await super()._status_polling_loop_fetch_updates()
    
    async def _update_order_fills_from_trades(self):
        """Update order fills from recent trades.
        This is used to ensure we capture all fills even if WebSocket messages are missed.
        """
        # This method can be implemented if needed for reliability
        # For now, we rely on WebSocket updates and REST status queries

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: bool | None = None,
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
        params: dict[str, Any] | None = None,
        is_auth_required: bool = False,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
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
        data: dict[str, Any] | None = None,
        is_auth_required: bool = True,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute POST request."""
        return await web_utils.api_request(
            path=path_url,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            data=data,
            method=RESTMethod.POST,
            is_auth_required=is_auth_required,
            limit_id=limit_id,
            headers={"Content-Type": "application/json"},
        )

    async def _api_delete(
        self,
        path_url: str,
        params: dict[str, Any] | None = None,
        is_auth_required: bool = True,
        limit_id: str | None = None,
    ) -> dict[str, Any]:
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
            is_auth_required=False,
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
                    exc_info=True,
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
            is_auth_required=True,
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
            is_auth_required=True,
        )

        # Handle both list format (direct response) and object format (wrapped in "positions" key)
        # API response can be list or dict with "positions" key
        positions: list[Any]
        if isinstance(response, list):
            positions = response
        elif isinstance(response, dict):
            positions = response.get("positions", [])
        else:
            # Handle unexpected response type
            positions = []

        # Track which trading pairs have positions in the response
        trading_pairs_in_response = set()

        for position_data in positions:
            symbol = position_data.get("symbol")
            if symbol:
                trading_pair = utils.convert_from_exchange_trading_pair(symbol)
                # Only process positions for trading pairs we're actually tracking
                if trading_pair and self._trading_pairs and trading_pair in self._trading_pairs:
                    trading_pairs_in_response.add(trading_pair)
                    self._process_position_update(position_data)

        # Remove positions that are no longer in the response
        current_positions = list(self._perpetual_trading.account_positions.keys())
        for trading_pair in current_positions:
            if trading_pair not in trading_pairs_in_response:
                del self._perpetual_trading.account_positions[trading_pair]

    def _process_position_update(self, position_data: dict[str, Any]):
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
            is_long = position_data["side"] in ["LONG", "Long"] if "side" in position_data else net_quantity > 0

            # Skip if no position
            if abs(net_quantity) == Decimal(0):
                if trading_pair in self._perpetual_trading.account_positions:
                    del self._perpetual_trading.account_positions[trading_pair]
                return

            # Get PnL - handle different field names
            unrealized_pnl_raw = position_data.get("pnlUnrealized",
                                                   position_data.get("unrealizedPnl",
                                                                     position_data.get("P")))
            if unrealized_pnl_raw is None:
                self.logger().warning(f"No PnL field found in position data for {trading_pair}, using 0")
                unrealized_pnl = Decimal(0)  # PnL can legitimately be 0 for new positions
            else:
                unrealized_pnl = Decimal(str(unrealized_pnl_raw))

            # Get entry price - handle different field names
            # Trust exchange data structure
            entry_price = Decimal(str(position_data.get("entryPrice", position_data.get("B"))))

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

            # Check for margin call conditions based on maintenance margin and liquidation price
            # Similar to Binance implementation
            liquidation_price = position_data.get("l")
            mark_price = position_data.get("M")
            maint_margin_fraction = position_data.get("m")

            if liquidation_price and mark_price:
                mark_price_decimal = Decimal(str(mark_price))
                liquidation_price_decimal = Decimal(str(liquidation_price))

                # Check if position is at risk using maintenance margin rate from constants
                at_risk = False

                # Get actual maintenance margin ratio for the trading pair
                try:
                    maint_margin_ratio = self.get_maintenance_margin_ratio(trading_pair)
                except ValueError:
                    # If market data not loaded yet, cannot determine risk
                    self.logger().warning(f"Cannot determine risk for {trading_pair}: margin data not loaded")
                    maint_margin_ratio = None

                if maint_margin_ratio is not None:
                    if is_long:
                        # For long positions, risk when mark price approaches liquidation price from above
                        risk_threshold = liquidation_price_decimal * (Decimal(1) + maint_margin_ratio)
                        at_risk = (mark_price_decimal <= risk_threshold)
                    else:
                        # For short positions, risk when mark price approaches liquidation price from below
                        risk_threshold = liquidation_price_decimal * (Decimal(1) - maint_margin_ratio)
                        at_risk = (mark_price_decimal >= risk_threshold)
                else:
                    at_risk = False

                if at_risk:
                    # Issue margin call warning similar to Binance connector
                    self.logger().warning(
                        "Margin Call: Your position risk is too high, and you are at risk of "
                        "liquidation. Close your positions or add additional margin to your wallet.",
                    )

                    # Log additional info similar to Binance
                    negative_pnl_msg = ""
                    if unrealized_pnl < Decimal(0):
                        negative_pnl_msg = f"{trading_pair}: {unrealized_pnl}, "

                    maint_margin_msg = ""
                    if maint_margin_fraction:
                        maint_margin_msg = f"Maintenance Margin: {maint_margin_fraction}. "

                    self.logger().info(
                        f"{maint_margin_msg}Negative PnL assets: {negative_pnl_msg}.",
                    )

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
        price: Decimal | None = None,
        position_action: PositionAction = PositionAction.OPEN,
        **kwargs,
    ) -> tuple[str, float]:
        """Place an order on Backpack perpetual.
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
            order_data["reduceOnly"] = "true"

        # Add price for limit orders
        if order_type == OrderType.LIMIT or order_type == OrderType.LIMIT_MAKER:
            order_data["price"] = str(price)

        # Add time in force
        order_data["timeInForce"] = kwargs.get("time_in_force", "GTC")

        # Add post-only flag for maker orders
        if order_type == OrderType.LIMIT_MAKER:
            order_data["postOnly"] = "true"

        response = await self._api_post(
            path_url=CONSTANTS.ORDER_URL,
            data=order_data,
            is_auth_required=True,
        )

        exchange_order_id = response["id"]  # Backpack returns 'id' field
        # Get timestamp from response - Backpack returns createdAt in milliseconds
        timestamp = response["createdAt"] / 1000.0  # Convert to seconds

        return exchange_order_id, timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """Cancel an order on the exchange.
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
                is_auth_required=True,
            )

            return True

        except Exception:
            # Re-raise all exceptions to let the base class handle them properly
            # The base class will call _is_order_not_found_during_cancelation_error
            raise

    # Order status and trade updates
    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """Request current order status from exchange.
        """
        symbol = utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)

        # Query open orders
        response = await self._api_get(
            path_url=CONSTANTS.OPEN_ORDERS_URL,
            params={"symbol": symbol},
            is_auth_required=True,
        )

        # Look for our order - Backpack uses 'id' not 'orderId'
        # API response is list of orders
        orders: list[dict[str, Any]]
        if isinstance(response, list):
            orders = response
        else:
            # Handle unexpected response type or dict wrapper
            orders = response.get("orders", []) if isinstance(response, dict) else []
        for order_data in orders:
            if isinstance(order_data, dict) and (order_data.get("clientId") == tracked_order.client_order_id or
                    order_data.get("id") == tracked_order.exchange_order_id):

                return self._parse_order_update(order_data, tracked_order)

        # If not in open orders, check order history
        history_response = await self._api_get(
            path_url=CONSTANTS.ORDER_HISTORY_URL,
            params={"symbol": symbol, "limit": 50},
            is_auth_required=True,
        )

        # API response is list of historical orders
        history_orders: list[dict[str, Any]]
        if isinstance(history_response, list):
            history_orders = history_response
        else:
            # Handle unexpected response type or dict wrapper
            history_orders = history_response.get("orders", []) if isinstance(history_response, dict) else []
        for order_data in history_orders:
            if isinstance(order_data, dict) and (order_data.get("clientId") == tracked_order.client_order_id or
                    order_data.get("id") == tracked_order.exchange_order_id):

                return self._parse_order_update(order_data, tracked_order)

        # Order not found - assume cancelled
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.CANCELED,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    def _parse_order_update(self, order_data: dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        """Parse order data into OrderUpdate."""
        status = order_data.get("status", "")
        state = OrderState[CONSTANTS.ORDER_STATE_MAP.get(status, "FAILED")]

        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=state,
            client_order_id=order_data.get("clientId", tracked_order.client_order_id),
            exchange_order_id=order_data.get("id", tracked_order.exchange_order_id),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> list[TradeUpdate]:
        """Get all trade fills for an order.
        """
        symbol = utils.convert_to_exchange_trading_pair(order.trading_pair)

        response = await self._api_get(
            path_url=CONSTANTS.FILLS_URL,
            params={
                "symbol": symbol,
                "orderId": order.exchange_order_id,  # Fills endpoint uses orderId parameter
            },
            is_auth_required=True,
        )

        # API response is list of fills
        fills_list: list[dict[str, Any]]
        if isinstance(response, list):
            fills_list = response
        else:
            # Handle unexpected response type or dict wrapper
            fills_list = response.get("fills", []) if isinstance(response, dict) else []
        trade_updates = [TradeUpdate(
                    trade_id=str(fill_data.get("tradeId", fill_data.get("id"))),
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id or "",  # Handle None case
                    trading_pair=order.trading_pair,
                    fill_timestamp=float(fill_data.get("timestamp") or self.current_timestamp),
                    fill_price=Decimal(str(fill_data["price"])),  # Required field
                    fill_base_amount=Decimal(str(fill_data["quantity"])),  # Required field
                    fill_quote_amount=Decimal(str(fill_data["price"])) * Decimal(str(fill_data["quantity"])),
                    fee=self._get_trade_fee_from_fill(fill_data),
                ) for fill_data in fills_list if isinstance(fill_data, dict)]

        return trade_updates

    def _get_trade_fee_from_fill(self, fill_data: dict[str, Any]) -> TradeFeeBase:
        """Extract trade fee from fill data."""
        fee_amount = Decimal(str(fill_data.get("fee", "0")))
        fee_asset = fill_data.get("feeAsset", CONSTANTS.COLLATERAL_TOKEN)

        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=PositionAction.OPEN,  # Determine from context if needed
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
        )

    # Funding rate management
    def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Get the stored funding information for a trading pair.
        
        This method is required by PerpetualDerivativePyBase.
        
        Args:
            trading_pair: The trading pair to get funding info for
            
        Returns:
            FundingInfo object containing current funding rate data
        """
        return self._perpetual_trading.get_funding_info(trading_pair)

    async def _update_funding_info(self):
        """Update funding rate information for all trading pairs."""
        trading_pairs = self._trading_pairs or []
        tasks = [self._fetch_funding_rate(trading_pair) for trading_pair in trading_pairs]

        funding_infos: list[FundingInfo | Exception] = await safe_gather(*tasks, return_exceptions=True)

        for trading_pair, funding_info in zip(trading_pairs, funding_infos, strict=False):
            if isinstance(funding_info, Exception):
                self.logger().error(
                    f"Error fetching funding rate for {trading_pair}: {funding_info}",
                )
            else:
                self._perpetual_trading._funding_info[trading_pair] = funding_info

    async def _fetch_funding_rate(self, trading_pair: str) -> FundingInfo:
        """Fetch current funding rate for a trading pair with caching.
        
        Cache is valid for the duration specified in CONSTANTS.FUNDING_INFO_UPDATE_INTERVAL.
        """
        # Check cache first
        current_time = time.time()
        if trading_pair in self._funding_info_cache:
            cached_info, cache_timestamp = self._funding_info_cache[trading_pair]
            # Use cached value if it's still fresh (within update interval)
            if current_time - cache_timestamp < CONSTANTS.FUNDING_INFO_UPDATE_INTERVAL:
                return cached_info
        
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        # Use markPrices endpoint which provides current funding rate
        response = await self._api_get(
            path_url=CONSTANTS.MARK_PRICE_URL,
            params={"symbol": symbol},
            is_auth_required=False,
        )

        # Response is a list, find the matching symbol
        mark_data: dict[str, Any] | None = None
        if isinstance(response, list):
            for item in response:
                if isinstance(item, dict) and item.get("symbol") == symbol:
                    mark_data = item
                    break
        elif isinstance(response, dict):
            mark_data = response
        
        if mark_data is None:
            raise ValueError(f"No mark price data found for {symbol}")

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(mark_data["indexPrice"])),
            mark_price=Decimal(str(mark_data["markPrice"])),
            next_funding_utc_timestamp=int(mark_data["nextFundingTime"]),
            rate=Decimal(str(mark_data["fundingRate"])),
        )
        
        # Update cache
        self._funding_info_cache[trading_pair] = (funding_info, current_time)
        
        return funding_info

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get the last traded price for a trading pair.

        Args:
            trading_pair: The trading pair to get price for

        Returns:
            The last traded price as a float
        """
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        response = await self._api_get(
            path_url=CONSTANTS.TICKER_URL,
            params={"symbol": symbol},
            is_auth_required=False,
        )

        return float(response["lastPrice"])

    # Leverage management
    async def _execute_set_leverage(self, trading_pair: str, leverage: int):
        """Set leverage for a trading pair.

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
            f"To change actual leverage, update account settings via the exchange interface.",
        )

    async def _execute_set_position_mode(self, mode: PositionMode):
        """Set position mode on the exchange.
        Backpack only supports ONE-WAY mode, so this validates and sets it.
        """
        if not self.validate_position_mode(mode):
            error_msg = (
                f"Invalid position mode {mode}. "
                f"Backpack only supports: {', '.join(str(m) for m in CONSTANTS.SUPPORTED_POSITION_MODES)}"
            )
            self.logger().error(error_msg)
            self.trigger_event(
                AccountEvent.PositionModeChangeFailed,
                PositionModeChangeEvent(
                    timestamp=self.current_timestamp,
                    trading_pair="",  # Empty string when no specific pair
                    position_mode=mode,
                    message=error_msg,
                ),
            )
            raise ValueError(error_msg)

        # Backpack doesn't have an API to set position mode - it's always ONEWAY
        # So we just store it locally and emit success event
        self._position_mode = mode
        self.trigger_event(
            AccountEvent.PositionModeChangeSucceeded,
            PositionModeChangeEvent(
                timestamp=self.current_timestamp,
                trading_pair="",  # Empty string when no specific pair
                position_mode=mode,
                message="Position mode confirmed as ONE-WAY (Backpack default)",
            ),
        )

    def _set_position_mode(self, mode: PositionMode):
        """Synchronously set position mode (for testing).
        Backpack only supports ONE-WAY mode.
        """
        if not self.validate_position_mode(mode):
            raise ValueError(
                f"Invalid position mode {mode}. "
                f"Backpack only supports: {', '.join(str(m) for m in CONSTANTS.SUPPORTED_POSITION_MODES)}",
            )
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
            position_action = tracked_order.position if hasattr(tracked_order, "position") else PositionAction.NIL

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
                flat_fees=flat_fees,
            )

            # Create trade update
            trade_update = TradeUpdate(
                trade_id=str(fill_data["tradeId"]),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=(fill_data.get("timestamp") or self.current_timestamp * 1000) / 1000,
                fill_price=Decimal(str(fill_data["price"])),  # Required field
                fill_base_amount=Decimal(str(fill_data["quantity"])),  # Required field
                fill_quote_amount=Decimal(str(fill_data["price"])) * Decimal(str(fill_data["quantity"])),
                fee=fee,
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
                    order_type = OrderType.MARKET if "MARKET" in order_type_str.upper() else OrderType.LIMIT

                    # Trust exchange data - try alternate field names for compatibility
                    price = Decimal(str(order_data.get("price", order_data.get("p"))))
                    quantity = Decimal(str(order_data.get("quantity", order_data.get("q"))))

                    # Create InFlightOrder
                    order = InFlightOrder(
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=trading_pair,
                        order_type=order_type,
                        trade_type=trade_type,
                        price=price,
                        amount=quantity,
                        creation_timestamp=float(order_data.get("timestamp", self.current_timestamp * 1000)) / 1000,
                    )
                    self._order_tracker.start_tracking_order(order)
                    tracked_order = order
                else:
                    return

            # Map order status
            order_state = CONSTANTS.ORDER_STATE_MAP.get(order_data.get("status") or "", "OPEN")
            new_state = OrderState[order_state]

            # Create order update - Backpack uses 'id' field for order ID
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=order_data.get("id") or order_data.get("orderId"),  # Handle both formats
                trading_pair=tracked_order.trading_pair,
                update_timestamp=(order_data.get("timestamp") or self.current_timestamp * 1000) / 1000,
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

    def _process_position_message(self, position_msg: dict[str, Any]):
        """Process position update messages from WebSocket."""
        try:
            self._process_position_update(position_msg.get("data", position_msg))
        except Exception:
            self.logger().exception("Error processing position update")

    def _process_funding_payment_message(self, funding_msg: dict[str, Any]):
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
                        funding_rate=Decimal(str(data["fundingRate"])),
                        amount=Decimal(str(data["payment"])),
                    ),
                )
        except Exception:
            self.logger().exception("Error processing funding payment")

    def _process_liquidation_warning(self, liquidation_msg: dict[str, Any]):
        """Process liquidation warning messages."""
        try:
            data = liquidation_msg.get("data", liquidation_msg)

            self.logger().warning(
                f"LIQUIDATION WARNING for {data['symbol']}: "
                f"Mark price {data['markPrice']} approaching "
                f"liquidation price {data['liquidationPrice']}",
            )
        except Exception:
            self.logger().exception("Error processing liquidation warning")

    async def _update_trading_fees(self):
        """Update trading fees from the exchange."""
        # Try to fetch fees from account endpoint if available
        try:
            account_info = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.BALANCE_URL,  # Use balance rate limit
            )
            
            # Check if fee information is available in account response
            if "makerFeeRate" in account_info or "takerFeeRate" in account_info:
                # Update fee configuration if data is available
                # This would need to be integrated with TradeFeeSchemaLoader
                self.logger().info(
                    f"Account fee rates - Maker: {account_info.get('makerFeeRate')}, "
                    f"Taker: {account_info.get('takerFeeRate')}",
                )
            else:
                # If not available, the TradeFeeSchemaLoader will use configured defaults
                self.logger().debug("No fee rates in account info, using configured defaults")
                
        except Exception:
            # If the endpoint doesn't exist or fails, fees will come from configuration
            self.logger().debug("Could not fetch account fee rates, using configured defaults")

    async def _make_trading_pairs_request(self) -> Any:
        """Request trading pairs information from the exchange.

        Returns:
            Exchange trading pairs info
        """
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        return exchange_info

    async def _format_trading_rules(self, exchange_info_dict: dict[str, Any]) -> list[TradingRule]:
        """Format trading rules from exchange info.

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
                
                # Extract margin and leverage data from API
                max_leverage = symbol_info.get("maxLeverage")
                initial_margin_ratio = symbol_info.get("initialMarginRatio")
                maintenance_margin_ratio = symbol_info.get("maintenanceMarginRatio")
                
                # Fail fast if critical margin parameters are missing
                if max_leverage is None:
                    raise ValueError(
                        f"Market {symbol} missing required maxLeverage in API response. "
                        "Cannot trade without proper leverage limits.",
                    )
                    
                if initial_margin_ratio is None:
                    raise ValueError(
                        f"Market {symbol} missing required initialMarginRatio in API response. "
                        "Cannot calculate positions without margin requirements.",
                    )
                    
                if maintenance_margin_ratio is None:
                    raise ValueError(
                        f"Market {symbol} missing required maintenanceMarginRatio in API response. "
                        "Cannot manage risk without maintenance margin data.",
                    )
                
                # Store market-specific margin requirements
                self._market_margin_requirements[trading_pair] = {
                    "max_leverage": Decimal(str(max_leverage)),
                    "initial_margin": Decimal(str(initial_margin_ratio)),
                    "maintenance_margin": Decimal(str(maintenance_margin_ratio)),
                }
                
                # Also store max leverage in the leverage map for compatibility
                self._leverage_map[trading_pair] = int(max_leverage)

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
                    exc_info=True,
                )

        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: dict[str, Any]):
        """Initialize the trading pair symbol mapping from exchange info.

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
                    exc_info=True,
                )

        self._set_trading_pair_symbol_map(mapping)

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> tuple[bool, str]:
        """Set the position mode for a trading pair.

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

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> tuple[bool, str]:
        """Set leverage for a trading pair.

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

    async def _fetch_last_fee_payment(self, trading_pair: str) -> tuple[int, Decimal, Decimal]:
        """Fetch the last funding fee payment for a trading pair.

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

            # API response is list of funding payments
            payments: list[dict[str, Any]]
            if isinstance(response, list):
                payments = response
            else:
                # Handle unexpected response type or dict wrapper
                payments = response.get("payments", []) if isinstance(response, dict) else []
            if payments and len(payments) > 0:
                last_payment = payments[0]
                # Convert timestamp from ISO format or milliseconds
                if "intervalEndTimestamp" in last_payment:
                    # Parse ISO timestamp
                    timestamp_str = last_payment["intervalEndTimestamp"]
                    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    timestamp = int(dt.timestamp() * 1000)
                else:
                    timestamp_raw = last_payment.get("timestamp")
                    if timestamp_raw is None:
                        self.logger().warning(f"No timestamp in funding payment data for {trading_pair}")
                        return 0, s_decimal_NaN, s_decimal_NaN
                    timestamp = int(timestamp_raw)

                funding_rate_raw = last_payment.get("fundingRate", last_payment.get("rate"))
                if funding_rate_raw is None:
                    self.logger().warning(f"No funding rate in payment data for {trading_pair}")
                    return timestamp, s_decimal_NaN, s_decimal_NaN
                funding_rate = Decimal(str(funding_rate_raw))
                
                # The 'quantity' field represents the payment amount (positive if received, negative if paid)
                payment_raw = last_payment.get("quantity", last_payment.get("fundingFee"))
                if payment_raw is None:
                    self.logger().warning(f"No payment amount in funding data for {trading_pair}")
                    payment = s_decimal_NaN
                else:
                    payment = Decimal(str(payment_raw))
                return timestamp, funding_rate, payment
            return 0, s_decimal_NaN, s_decimal_NaN

        except Exception as e:
            self.logger().error(
                f"Error fetching last fee payment for {trading_pair}: {e}",
                exc_info=True,
            )
            return 0, s_decimal_NaN, s_decimal_NaN

    async def _make_network_check_request(self):
        """Make a network check request to verify connectivity.
        This is called by check_network() from the parent class.

        Uses TIME endpoint since Backpack's ping returns plain text "pong", not JSON,
        which is incompatible with the standard REST assistant used by Hummingbot.
        """
        await self._api_get(
            path_url=CONSTANTS.TIME_URL,
            is_auth_required=False,
        )
