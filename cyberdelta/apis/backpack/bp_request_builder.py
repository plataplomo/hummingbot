"""CyberDeltaEngine: Backpack API Request Builder.

This module provides the BackpackRequestBuilder class for constructing
request parameters and payloads for all Backpack API endpoints.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountConvertDustRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawInternalTransferRequest,
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
    BackpackRawQuoteAcceptRequest,
    BackpackRawQuoteSubmitRequest,
    BackpackRawRequestForQuoteCancelRequest,
    BackpackRawRequestForQuoteRefreshRequest,
    BackpackRawRequestForQuoteRequest,
    BackpackRawUpdateAccountSettingsRequest,
)
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralQueryParams
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
    BackpackRawGetPositionsParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
    BackpackRawGetTradeHistoryParams,
    BackpackRawMaxBorrowQuantityParams,
    BackpackRawMaxOrderQuantityParams,
    BackpackRawMaxWithdrawalQuantityParams,
)
from cyberdelta.apis.models.service_args_models import (
    GetMaxBorrowQuantityArgs,
    GetMaxOrderQuantityArgs,
    GetMaxWithdrawalQuantityArgs,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


logger = get_logger(__name__)


class BackpackRequestBuilder:
    """Build request parameters and payloads for Backpack API endpoints.

    This class centralizes the logic for constructing the dictionaries
    needed for various Backpack API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    def __init__(self, exchange_config: ExchangeSpecificConfig) -> None:
        """Initialize the BackpackRequestBuilder.

        Args:
            exchange_config: Exchange-specific configuration model.

        Raises:
            ValueError: If testnet environment is requested but testnet API URL is not configured.

        """
        self._exchange_config = exchange_config
        # Get base URL from the configuration based on environment
        if self._exchange_config.is_mainnet_environment:
            self.base_url = str(self._exchange_config.api_base_url_mainnet)
        else:
            if self._exchange_config.api_base_url_testnet is None:
                raise ValueError("Testnet API URL not configured but testnet environment requested")
            self.base_url = str(self._exchange_config.api_base_url_testnet)

    def _get_endpoint_url(self, path: str) -> str:
        """Construct the full URL for an API endpoint path.

        Returns:
            Full URL string for the API endpoint.
        """
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    @staticmethod
    def format_symbol(symbol: str) -> str:
        """Ensure symbol is in the format X_Y (e.g., SOL_USDC).

        Returns:
            Symbol formatted with underscores and uppercase.
        """
        return symbol.replace("-", "_").upper()

    @staticmethod
    def build_get_ticker_params(symbol: str) -> BackpackRawGetTickerParams:
        """Build parameters for the get_ticker endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetTickerParams: The validated request parameters model.

        """
        return BackpackRawGetTickerParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_get_order_book_params(
        symbol: str,
        limit: int | None,
    ) -> BackpackRawGetOrderBookParams:
        """Build parameters for the get_order_book endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of bids and asks to retrieve (optional).

        Returns:
            BackpackRawGetOrderBookParams: The validated request parameters model.

        """
        return BackpackRawGetOrderBookParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            limit=limit,
        )

    @staticmethod
    def build_get_recent_trades_params(
        symbol: str,
        limit: int | None,
    ) -> BackpackRawGetRecentTradesParams:
        """Build parameters for the get_recent_trades endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve (optional).

        Returns:
            BackpackRawGetRecentTradesParams: The validated request parameters model.

        """
        return BackpackRawGetRecentTradesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            limit=limit,
        )

    @staticmethod
    def build_get_balances_params() -> BackpackRawGetBalancesParams:
        """Build parameters for the get_balances endpoint.

        Returns:
            BackpackRawGetBalancesParams: The validated request parameters model.
            This endpoint requires no query parameters, but returns a model for consistency.

        """
        return BackpackRawGetBalancesParams()

    @staticmethod
    def build_get_positions_params(symbol: str | None) -> BackpackRawGetPositionsParams:
        """Build parameters for the get_positions endpoint.

        Args:
            symbol: The trading symbol to filter by (optional). Note that the symbol
                   is typically used in the URL path (/positions/{symbol}) rather than
                   as a query parameter.

        Returns:
            BackpackRawGetPositionsParams: The validated request parameters model.
            This endpoint requires no query parameters when fetching all positions
            or a specific position (symbol goes in URL path).

        """
        # GET /api/v1/positions
        # Symbol is used in the URL path, not as a query parameter,
        # so we return an empty params model for consistency.
        return BackpackRawGetPositionsParams()

    @staticmethod
    def _map_order_enums_to_api_strings(
        side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        self_trade_prevention: str | None,
    ) -> tuple[str, str, str | None, str | None]:
        """Map internal enums to API string values.

        Returns:
            Tuple of (api_side, api_order_type, api_time_in_force, api_self_trade_prevention).
        """
        api_side = "Bid" if side == OrderSide.BUY else "Ask"

        api_order_type = {
            OrderType.LIMIT: "Limit",
            OrderType.MARKET: "Market",
            OrderType.STOP_MARKET: "Market",  # With triggerPrice it becomes a stop
            OrderType.STOP_LIMIT: "Limit",  # With triggerPrice it becomes a stop limit
            OrderType.TAKE_PROFIT_MARKET: "Market",  # With triggerPrice it becomes a take profit
            OrderType.TAKE_PROFIT_LIMIT: "Limit",  # With triggerPrice becomes take profit limit
        }.get(order_type, "Limit")  # Default to "Limit" if not found

        # Map time in force (only for limit orders, service validates this)
        api_time_in_force = None
        if order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}:
            api_time_in_force = {
                TimeInForce.GTC: "GTC",
                TimeInForce.IOC: "IOC",
                TimeInForce.FOK: "FOK",
            }.get(time_in_force)

        # Map self trade prevention
        api_self_trade_prevention = None
        if self_trade_prevention:
            api_self_trade_prevention = {
                "RejectTaker": "RejectTaker",
                "RejectMaker": "RejectMaker",
                "RejectBoth": "RejectBoth",
            }.get(self_trade_prevention)

        return api_side, api_order_type, api_time_in_force, api_self_trade_prevention

    @staticmethod
    def _add_basic_order_fields(
        request_data: dict[str, Any],
        symbol: str,
        quantity: Decimal,
        price: Decimal | None,
        client_order_id: str | None,
        post_only: bool,
        order_type: OrderType,
        reduce_only: bool,
        api_time_in_force: str | None,
        api_self_trade_prevention: str | None,
    ) -> None:
        """Add basic order fields to request data."""
        # For stop limit and take profit limit orders, Backpack API requires
        # NOT to specify quantity, only triggerQuantity
        if order_type not in {OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}:
            request_data["quantity"] = str(quantity)
        if price is not None:
            request_data["price"] = str(price)

        # Convert client_order_id string to int if provided (service validates convertibility)
        if client_order_id:
            request_data["clientId"] = int(client_order_id)

        if post_only and order_type == OrderType.LIMIT:
            request_data["postOnly"] = post_only
        if api_time_in_force is not None:
            request_data["timeInForce"] = api_time_in_force
        if reduce_only:
            request_data["reduceOnly"] = reduce_only
        if api_self_trade_prevention is not None:
            request_data["selfTradePrevention"] = api_self_trade_prevention

    @staticmethod
    def _add_stop_loss_fields(
        request_data: dict[str, Any],
        stop_loss_trigger_price: Decimal | None,
        stop_loss_trigger_by: str | None,
        stop_loss_limit_price: Decimal | None,
    ) -> None:
        """Add stop loss fields to request data."""
        if stop_loss_trigger_price is not None:
            request_data["stopLossTriggerPrice"] = str(stop_loss_trigger_price)
        if stop_loss_trigger_by is not None:
            request_data["stopLossTriggerBy"] = stop_loss_trigger_by
        if stop_loss_limit_price is not None:
            request_data["stopLossLimitPrice"] = str(stop_loss_limit_price)

    @staticmethod
    def _add_take_profit_fields(
        request_data: dict[str, Any],
        take_profit_trigger_price: Decimal | None,
        take_profit_trigger_by: str | None,
        take_profit_limit_price: Decimal | None,
    ) -> None:
        """Add take profit fields to request data."""
        if take_profit_trigger_price is not None:
            request_data["takeProfitTriggerPrice"] = str(take_profit_trigger_price)
        if take_profit_trigger_by is not None:
            request_data["takeProfitTriggerBy"] = take_profit_trigger_by
        if take_profit_limit_price is not None:
            request_data["takeProfitLimitPrice"] = str(take_profit_limit_price)

    @staticmethod
    def build_place_order_payload(
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        post_only: bool = False,
        trigger_price: Decimal | None = None,  # For STOP_LOSS, TAKE_PROFIT etc.
        stop_loss_trigger_price: Decimal | None = None,
        stop_loss_trigger_by: str | None = None,
        stop_loss_limit_price: Decimal | None = None,
        take_profit_trigger_price: Decimal | None = None,
        take_profit_trigger_by: str | None = None,
        take_profit_limit_price: Decimal | None = None,
        reduce_only: bool = False,
        self_trade_prevention: str | None = None,
    ) -> BackpackRawOrderExecuteRequest:
        """Build the payload for placing an order.

        Args:
            symbol: The trading symbol.
            side: The order side (BUY or SELL).
            order_type: The type of order (MARKET, LIMIT, etc.).
            quantity: The quantity of the order.
            time_in_force: The time in force for the order.
            price: The limit price for LIMIT orders.
            client_order_id: Optional client-specified order ID.
            post_only: If true, the order will only be accepted if it is a maker order.
            trigger_price: The price at which a stop or take profit order is triggered.
            stop_loss_trigger_price: Stop loss trigger price.
            stop_loss_trigger_by: Reference price for stop loss (LastPrice/MarkPrice/IndexPrice).
            stop_loss_limit_price: Stop loss limit price.
            take_profit_trigger_price: Take profit trigger price.
            take_profit_trigger_by: Reference price for take profit trigger.
            take_profit_limit_price: Take profit limit price.
            reduce_only: If true, order can only reduce position (futures only).
            self_trade_prevention: Self trade prevention mode.

        Returns:
            BackpackRawOrderExecuteRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        # Map internal enums to API strings using helper method
        api_side, api_order_type, api_time_in_force, api_self_trade_prevention = (
            BackpackRequestBuilder._map_order_enums_to_api_strings(
                side,
                order_type,
                time_in_force,
                self_trade_prevention,
            )
        )

        # Build the raw request model with core fields
        request_data: dict[str, Any] = {
            "orderType": api_order_type,
            "side": api_side,
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        # Add basic order fields using helper method
        BackpackRequestBuilder._add_basic_order_fields(
            request_data,
            symbol,
            quantity,
            price,
            client_order_id,
            post_only,
            order_type,
            reduce_only,
            api_time_in_force,
            api_self_trade_prevention,
        )

        # Add trigger price for stop orders
        if trigger_price is not None:
            request_data["triggerPrice"] = str(trigger_price)
            # Backpack requires triggerQuantity to be the same as quantity for stop orders
            request_data["triggerQuantity"] = str(quantity)

        # Add stop loss and take profit fields using helper methods
        BackpackRequestBuilder._add_stop_loss_fields(
            request_data,
            stop_loss_trigger_price,
            stop_loss_trigger_by,
            stop_loss_limit_price,
        )
        BackpackRequestBuilder._add_take_profit_fields(
            request_data,
            take_profit_trigger_price,
            take_profit_trigger_by,
            take_profit_limit_price,
        )

        # Create and return the Pydantic model
        return BackpackRawOrderExecuteRequest(**request_data)

    @staticmethod
    def build_cancel_order_payload(
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> BackpackRawOrderCancelRequest:
        """Build the JSON payload for cancelling an order.

        Corresponds to DELETE /api/v1/order request body.

        Args:
            symbol: The trading symbol.
            order_id: The order ID (optional if client_order_id is provided).
            client_order_id: The client order ID (optional if order_id is provided).

        Returns:
            BackpackRawOrderCancelRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        if order_id:
            request_data["orderId"] = order_id

        if client_order_id:
            # Convert to int for the API (service validates convertibility)
            client_id = int(client_order_id)
            request_data["clientId"] = client_id

        # Create and return the Pydantic model
        return BackpackRawOrderCancelRequest(**request_data)

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str,
        order_type_filter: str | None = None,
    ) -> BackpackRawOrderCancelAllRequest:
        """Build the payload for cancelling all orders for a symbol.

        Args:
            symbol: The trading symbol.
            order_type_filter: Optional filter for order type.

        Returns:
            BackpackRawOrderCancelAllRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        if order_type_filter:
            request_data["orderType"] = order_type_filter

        # Create and return the Pydantic model
        return BackpackRawOrderCancelAllRequest(**request_data)

    @staticmethod
    def build_get_open_orders_params(symbol: str | None) -> BackpackRawGetOpenOrdersParams:
        """Build parameters for the get_open_orders endpoint.

        Args:
            symbol: The trading symbol to filter by (optional).

        Returns:
            BackpackRawGetOpenOrdersParams: The validated request parameters model.

        """
        formatted_symbol = None
        if symbol:
            formatted_symbol = BackpackRequestBuilder.format_symbol(symbol)
        return BackpackRawGetOpenOrdersParams(symbol=formatted_symbol)

    @staticmethod
    def build_get_funding_rate_params(symbol: str) -> BackpackRawGetFundingRateParams:
        """Build parameters for fetching the current funding rate for a single symbol.

        Args:
            symbol: The trading symbol to get funding rate for (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetFundingRateParams: The validated request parameters model.

        Note:
            For historical funding rates, use build_get_historical_funding_rates_params.

        """
        return BackpackRawGetFundingRateParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_get_historical_funding_rates_params(
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
    ) -> BackpackRawGetHistoricalFundingRatesParams:
        """Build parameters for fetching historical funding rates (/api/v1/fundingRates).

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            start_time_ms: Optional start time in milliseconds since Unix epoch.
            end_time_ms: Optional end time in milliseconds since Unix epoch.
            limit: Optional limit on the number of results.

        Returns:
            BackpackRawGetHistoricalFundingRatesParams: The validated request parameters model.

        """
        # Convert millisecond timestamps to seconds as required by Backpack API
        start_time_seconds = None if start_time_ms is None else start_time_ms // 1000
        end_time_seconds = None if end_time_ms is None else end_time_ms // 1000

        return BackpackRawGetHistoricalFundingRatesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            startTime=start_time_seconds,
            endTime=end_time_seconds,
            limit=limit,
        )

    @staticmethod
    def build_get_account_info_params() -> BackpackRawGetAccountInfoParams:
        """Build parameters for the get_account_info endpoint (GET /api/v1/account).

        Returns:
            BackpackRawGetAccountInfoParams: The validated request parameters model.
            This endpoint requires no query parameters, but returns a model for consistency.

        """
        return BackpackRawGetAccountInfoParams()

    @staticmethod
    def build_get_markets_params() -> BackpackRawGetMarketsParams:
        """Build parameters for the get_markets endpoint (GET /api/v1/markets).

        Returns:
            BackpackRawGetMarketsParams: The validated request parameters model.
            This endpoint requires no query parameters, but returns a model for consistency.

        """
        return BackpackRawGetMarketsParams()

    @staticmethod
    def build_get_market_params(symbol: str) -> BackpackRawGetMarketParams:
        """Build parameters for the get_market endpoint (GET /api/v1/market).

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetMarketParams: The validated request parameters model.

        """
        return BackpackRawGetMarketParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_withdraw_payload(
        asset: str,
        amount: Decimal,
        address: str,
        network: str,
        tag: str | None = None,
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        auto_borrow: bool = False,
        auto_lend_redeem: bool = False,
    ) -> BackpackRawAccountWithdrawalRequest:
        """Build the payload for a withdrawal request.

        Args:
            asset: The asset symbol (e.g., "USDC").
            amount: The amount to withdraw.
            address: The destination address.
            network: The blockchain network (e.g., "Solana", "Ethereum").
            tag: Destination tag or memo, if required.
            client_withdrawal_id: Optional client-provided ID for the withdrawal.
            two_factor_token: Optional 2FA token if required by user settings.
            auto_borrow: Auto borrow if required.
            auto_lend_redeem: Auto redeem lend if required.

        Returns:
            BackpackRawAccountWithdrawalRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        # Map network names to blockchain values (mapping/translation only)
        blockchain_mapping = {
            "Arbitrum": "Arbitrum",
            "Base": "Base",
            "Bitcoin": "Bitcoin",
            "BitcoinCash": "BitcoinCash",
            "BNBSmartChain": "BNBSmartChain",
            "Cardano": "Cardano",
            "Dogecoin": "Dogecoin",
            "Ethereum": "Ethereum",
            "Litecoin": "Litecoin",
            "Polygon": "Polygon",
            "Solana": "Solana",
            "Story": "Story",
            "Sui": "Sui",
            "XRP": "XRP",
        }

        blockchain = blockchain_mapping[network]  # No validation, just mapping

        request_data: dict[str, Any] = {
            "address": address,
            "blockchain": blockchain,
            "quantity": str(amount),
            "symbol": asset.upper(),
        }

        # Add optional fields
        if client_withdrawal_id:
            request_data["clientId"] = client_withdrawal_id
        if two_factor_token:
            request_data["twoFactorToken"] = two_factor_token
        if tag:
            request_data["addressTag"] = tag
        if auto_borrow:
            request_data["autoBorrow"] = auto_borrow
        if auto_lend_redeem:
            request_data["autoLendRedeem"] = auto_lend_redeem

        # Create and return the Pydantic model
        return BackpackRawAccountWithdrawalRequest(**request_data)

    @staticmethod
    def build_get_order_history_params(
        symbol: str | None,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> BackpackRawGetOrderHistoryParams:
        """Build parameters for the get_order_history endpoint.

        Args:
            symbol: The trading symbol (optional).
            start_time_ms: Start time in milliseconds (optional).
            end_time_ms: End time in milliseconds (optional).
            limit: Maximum number of orders to retrieve (optional).
            order_id: Filter by specific order ID (optional).
            client_order_id: Filter by specific client order ID (optional).

        Returns:
            BackpackRawGetOrderHistoryParams: The validated request parameters model.

        """
        formatted_symbol = None
        if symbol:
            formatted_symbol = BackpackRequestBuilder.format_symbol(symbol)

        # Only one of orderId or clientId should be used per API docs
        formatted_order_id = None
        formatted_client_id = None
        if order_id:
            formatted_order_id = order_id
        elif client_order_id:
            formatted_client_id = client_order_id

        return BackpackRawGetOrderHistoryParams(
            symbol=formatted_symbol,
            orderId=formatted_order_id,
            clientId=formatted_client_id,
            limit=limit,
            **{"from": start_time_ms} if start_time_ms is not None else {},
            **{"to": end_time_ms} if end_time_ms is not None else {},
        )

    @staticmethod
    def build_get_trade_history_params(
        symbol: str | None,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
        from_id: str | None = None,  # For pagination based on trade ID
    ) -> BackpackRawGetTradeHistoryParams:
        """Build parameters for the get_trade_history (fills) endpoint.

        Args:
            symbol: The trading symbol (optional).
            start_time_ms: Start time in milliseconds (optional).
            end_time_ms: End time in milliseconds (optional).
            limit: Maximum number of trades to retrieve (optional).
            from_id: Get trades from this ID onwards (optional, for pagination).

        Returns:
            BackpackRawGetTradeHistoryParams: The validated request parameters model.

        """
        formatted_symbol = None
        if symbol:
            formatted_symbol = BackpackRequestBuilder.format_symbol(symbol)

        return BackpackRawGetTradeHistoryParams(
            symbol=formatted_symbol,
            limit=limit,
            **{"from": start_time_ms} if start_time_ms is not None else {},
            **{"to": end_time_ms} if end_time_ms is not None else {},
            fromId=from_id,
        )

    @staticmethod
    def build_get_market_data_params(
        symbol: str,
        timeframe_str: Literal[
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
        ],
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
    ) -> BackpackRawGetMarketDataParams:
        """Build parameters for fetching market data (candlesticks).

        GET /api/v1/klines

        Args:
            symbol: The trading symbol.
            timeframe_str: The candlestick interval string (e.g., "1m", "1h").
            start_time_ms: Start time in milliseconds (optional).
            end_time_ms: End time in milliseconds (optional).
            limit: Maximum number of candles to retrieve (optional).

        Returns:
            BackpackRawGetMarketDataParams: The validated request parameters model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        # Map to raw API format - business logic validation is done by service layer
        formatted_symbol = BackpackRequestBuilder.format_symbol(symbol)

        # Convert millisecond timestamps to seconds as required by Backpack API
        start_time_seconds = None if start_time_ms is None else start_time_ms // 1000
        end_time_seconds = None if end_time_ms is None else end_time_ms // 1000

        return BackpackRawGetMarketDataParams(
            symbol=formatted_symbol,
            interval=timeframe_str,
            **{"startTime": start_time_seconds} if start_time_seconds is not None else {},
            **{"endTime": end_time_seconds} if end_time_seconds is not None else {},
            limit=limit,
        )

    @staticmethod
    def build_get_historical_trades_params(
        symbol: str,
        limit: int | None,
        from_id: str | None,
    ) -> BackpackRawGetHistoricalTradesParams:
        """Build parameters for fetching historical public trades.

        GET /api/v1/trades/history

        Args:
            symbol: The trading symbol.
            limit: Maximum number of trades to retrieve (optional).
            from_id: Get trades from this ID onwards (optional, for pagination).

        Returns:
            BackpackRawGetHistoricalTradesParams: The validated request parameters model.

        """
        return BackpackRawGetHistoricalTradesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            limit=limit,
            fromId=from_id,
        )

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: str,
        amount: Decimal,
        from_account: str,
        to_account: str,
        client_transfer_id: str | None = None,
    ) -> BackpackRawInternalTransferRequest:
        """Build the payload for an internal capital transfer.

        Args:
            asset_symbol: The symbol of the asset to transfer (e.g., "USDC").
            amount: The quantity of the asset to transfer, as a Decimal.
            from_account: The source account type.
            to_account: The destination account type.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            BackpackRawInternalTransferRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(asset_symbol),
            "quantity": str(amount),
            "fromAccount": from_account,
            "toAccount": to_account,
        }
        if client_transfer_id:
            request_data["clientId"] = client_transfer_id

        # Create and return the Pydantic model
        return BackpackRawInternalTransferRequest(**request_data)

    @staticmethod
    def build_get_order_params(symbol: str) -> BackpackRawGetOrderParams:
        """Build parameters for GET /api/v1/orders/{orderIdOrClientId}.

        This endpoint requires 'symbol' as a query parameter.

        Args:
            symbol: The trading symbol for the order (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetOrderParams: The validated request parameters model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        return BackpackRawGetOrderParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_convert_dust_payload(asset_symbol: str) -> BackpackRawAccountConvertDustRequest:
        """Build the payload for converting dust balances to USDC.

        Args:
            asset_symbol: The asset symbol to convert dust for (e.g., "BTC").

        Returns:
            BackpackRawAccountConvertDustRequest: The validated request payload model.

        """
        request_data: dict[str, Any] = {
            "symbol": asset_symbol.upper(),
        }
        return BackpackRawAccountConvertDustRequest(**request_data)

    @staticmethod
    def build_borrow_lend_payload(
        asset_symbol: str,
        quantity: Decimal,
        side: str,
    ) -> BackpackRawBorrowLendExecuteRequest:
        """Build the payload for borrowing or lending operations.

        Args:
            asset_symbol: The asset symbol (e.g., "USDC").
            quantity: The quantity to borrow/lend/repay/redeem.
            side: The operation type ("Borrow", "Lend", "Repay", "Redeem").

        Returns:
            BackpackRawBorrowLendExecuteRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        request_data: dict[str, Any] = {
            "quantity": str(quantity),
            "side": side,
            "symbol": asset_symbol.upper(),
        }
        return BackpackRawBorrowLendExecuteRequest(**request_data)

    @staticmethod
    def build_update_account_settings_payload(
        auto_borrow_settlements: bool | None = None,
        auto_lend: bool | None = None,
        auto_realize_pnl: bool | None = None,
        auto_repay_borrows: bool | None = None,
        leverage_limit: Decimal | None = None,
    ) -> BackpackRawUpdateAccountSettingsRequest:
        """Build the payload for updating account settings.

        Args:
            auto_borrow_settlements: Enable/disable auto borrow settlements.
            auto_lend: Enable/disable auto lending.
            auto_realize_pnl: Enable/disable auto PnL realization.
            auto_repay_borrows: Enable/disable auto repay borrows.
            leverage_limit: Maximum leverage limit for the account.

        Returns:
            BackpackRawUpdateAccountSettingsRequest: The validated request payload model.

        """
        request_data: dict[str, Any] = {}

        if auto_borrow_settlements is not None:
            request_data["autoBorrowSettlements"] = auto_borrow_settlements
        if auto_lend is not None:
            request_data["autoLend"] = auto_lend
        if auto_realize_pnl is not None:
            request_data["autoRealizePnl"] = auto_realize_pnl
        if auto_repay_borrows is not None:
            request_data["autoRepayBorrows"] = auto_repay_borrows
        if leverage_limit is not None:
            request_data["leverageLimit"] = str(leverage_limit)

        return BackpackRawUpdateAccountSettingsRequest(**request_data)

    @staticmethod
    def build_request_for_quote_payload(
        symbol: str,
        quantity: Decimal | None = None,
        quote_quantity: Decimal | None = None,
        auto_accept_threshold: Decimal | None = None,
        submission_time_ms: int | None = None,
        expiry_time_ms: int | None = None,
        client_id: str | None = None,
    ) -> BackpackRawRequestForQuoteRequest:
        """Build the payload for submitting a Request For Quote (RFQ).

        Args:
            symbol: The trading symbol.
            quantity: Base quantity for the RFQ.
            quote_quantity: Quote quantity for the RFQ.
            auto_accept_threshold: Auto-accept threshold.
            submission_time_ms: Submission time in milliseconds.
            expiry_time_ms: Expiry time in milliseconds.
            client_id: Optional client-provided ID.

        Returns:
            BackpackRawRequestForQuoteRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        if quantity is not None:
            request_data["quantity"] = str(quantity)
        if quote_quantity is not None:
            request_data["quoteQuantity"] = str(quote_quantity)
        if auto_accept_threshold is not None:
            request_data["autoAcceptThreshold"] = str(auto_accept_threshold)
        if submission_time_ms is not None:
            request_data["submissionTimeMs"] = submission_time_ms
        if expiry_time_ms is not None:
            request_data["expiryTimeMs"] = expiry_time_ms
        if client_id is not None:
            request_data["clientId"] = client_id

        return BackpackRawRequestForQuoteRequest(**request_data)

    @staticmethod
    def build_submit_quote_payload(
        rfq_id: str,
        side: OrderSide,
        price: Decimal,
        client_quote_id: str | None = None,
    ) -> BackpackRawQuoteSubmitRequest:
        """Build the payload for submitting a quote in response to an RFQ.

        Args:
            rfq_id: The RFQ ID to respond to.
            side: The side of the quote (BUY or SELL).
            price: The quoted price.
            client_quote_id: Optional client-provided quote ID.

        Returns:
            BackpackRawQuoteSubmitRequest: The validated request payload model.

        Note:
            Inputs are assumed to be business-validated by the service layer.
            This method only performs mapping/translation to raw API values.

        """
        # Map OrderSide to API string
        api_side = "Bid" if side == OrderSide.BUY else "Ask"

        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
            "side": api_side,
            "price": str(price),
        }

        if client_quote_id is not None:
            request_data["clientQuoteId"] = client_quote_id

        return BackpackRawQuoteSubmitRequest(**request_data)

    @staticmethod
    def build_accept_quote_payload(rfq_id: str, quote_id: str) -> BackpackRawQuoteAcceptRequest:
        """Build the payload for accepting a quote.

        Args:
            rfq_id: The RFQ ID.
            quote_id: The quote ID to accept.

        Returns:
            BackpackRawQuoteAcceptRequest: The validated request payload model.

        """
        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
            "quoteId": quote_id,
        }
        return BackpackRawQuoteAcceptRequest(**request_data)

    @staticmethod
    def build_cancel_rfq_payload(rfq_id: str) -> BackpackRawRequestForQuoteCancelRequest:
        """Build the payload for cancelling an RFQ.

        Args:
            rfq_id: The RFQ ID to cancel.

        Returns:
            BackpackRawRequestForQuoteCancelRequest: The validated request payload model.

        """
        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
        }
        return BackpackRawRequestForQuoteCancelRequest(**request_data)

    @staticmethod
    def build_refresh_rfq_payload(
        rfq_id: str,
        submission_time_ms: int | None = None,
        expiry_time_ms: int | None = None,
    ) -> BackpackRawRequestForQuoteRefreshRequest:
        """Build the payload for refreshing an RFQ.

        Args:
            rfq_id: The RFQ ID to refresh.
            submission_time_ms: New submission time in milliseconds.
            expiry_time_ms: New expiry time in milliseconds.

        Returns:
            BackpackRawRequestForQuoteRefreshRequest: The validated request payload model.

        """
        request_data: dict[str, Any] = {
            "rfqId": rfq_id,
        }

        if submission_time_ms is not None:
            request_data["submissionTimeMs"] = submission_time_ms
        if expiry_time_ms is not None:
            request_data["expiryTimeMs"] = expiry_time_ms

        return BackpackRawRequestForQuoteRefreshRequest(**request_data)

    @staticmethod
    def build_collateral_query_params(
        subaccount_id: int | None = None,
    ) -> BackpackRawCollateralQueryParams:
        """Build query parameters for collateral endpoint.

        OpenAPI Spec: Only subaccountId is supported as optional parameter.

        Args:
            subaccount_id: Optional subaccount ID (uint16, 0-65535)

        Returns:
            Validated query parameters
        """
        return BackpackRawCollateralQueryParams(subaccountId=subaccount_id)

    # --- Account Limits Request Builders (INTERNAL USE ONLY) ---

    @staticmethod
    def build_max_borrow_quantity_params(
        args: GetMaxBorrowQuantityArgs,
    ) -> BackpackRawMaxBorrowQuantityParams:
        """Build query parameters for max borrow quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        Args:
            args: Validated arguments for max borrow quantity request

        Returns:
            Validated query parameters for max borrow quantity endpoint
        """
        return BackpackRawMaxBorrowQuantityParams(symbol=args.symbol)

    @staticmethod
    def build_max_order_quantity_params(
        args: GetMaxOrderQuantityArgs,
    ) -> BackpackRawMaxOrderQuantityParams:
        """Build query parameters for max order quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        Args:
            args: Validated arguments for max order quantity request

        Returns:
            Validated query parameters for max order quantity endpoint
        """
        # Convert OrderSide to string for Backpack API
        side_str = "Bid" if args.side == OrderSide.BUY else "Ask"
        price_str = str(args.price) if args.price is not None else None

        return BackpackRawMaxOrderQuantityParams(
            symbol=args.symbol,
            side=side_str,
            price=price_str,
            reduceOnly=args.reduce_only,
            autoBorrow=args.auto_borrow,
            autoBorrowRepay=args.auto_borrow_repay,
            autoLendRedeem=args.auto_lend_redeem,
        )

    @staticmethod
    def build_max_withdrawal_quantity_params(
        args: GetMaxWithdrawalQuantityArgs,
    ) -> BackpackRawMaxWithdrawalQuantityParams:
        """Build query parameters for max withdrawal quantity endpoint.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.

        Args:
            args: Validated arguments for max withdrawal quantity request

        Returns:
            Validated query parameters for max withdrawal quantity endpoint
        """
        return BackpackRawMaxWithdrawalQuantityParams(
            symbol=args.symbol,
            autoBorrow=args.auto_borrow,
            autoLendRedeem=args.auto_lend_redeem,
        )
