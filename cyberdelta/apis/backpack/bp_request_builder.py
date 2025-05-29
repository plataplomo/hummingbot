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
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
    BackpackRawGetPositionsParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
    BackpackRawGetTradeHistoryParams,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

logger = get_logger(__name__)


class BackpackRequestBuilder:
    """
    Builds request parameters and payloads for Backpack API endpoints.

    This class centralizes the logic for constructing the dictionaries
    needed for various Backpack API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    def __init__(self, exchange_config: ExchangeSpecificConfig) -> None:
        """
        Initializes the BackpackRequestBuilder.

        Args:
            exchange_config: Exchange-specific configuration model.
        """
        self._exchange_config = exchange_config
        # Get base URL from the configuration
        self.base_url = str(self._exchange_config.api_base_url)

    def _get_endpoint_url(self, path: str) -> str:
        """Constructs the full URL for an API endpoint path."""
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    @staticmethod
    def format_symbol(symbol: str) -> str:
        """Ensure symbol is in the format X_Y (e.g., SOL_USDC)."""
        return symbol.replace("-", "_").upper()

    @staticmethod
    def build_get_ticker_params(symbol: str) -> BackpackRawGetTickerParams:
        """
        Builds parameters for the get_ticker endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetTickerParams: The validated request parameters model.
        """
        return BackpackRawGetTickerParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_get_order_book_params(
        symbol: str, limit: int | None
    ) -> BackpackRawGetOrderBookParams:
        """
        Builds parameters for the get_order_book endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of bids and asks to retrieve (optional).

        Returns:
            BackpackRawGetOrderBookParams: The validated request parameters model.
        """
        return BackpackRawGetOrderBookParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol), limit=limit
        )

    @staticmethod
    def build_get_recent_trades_params(
        symbol: str, limit: int | None
    ) -> BackpackRawGetRecentTradesParams:
        """
        Builds parameters for the get_recent_trades endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve (optional).

        Returns:
            BackpackRawGetRecentTradesParams: The validated request parameters model.
        """
        return BackpackRawGetRecentTradesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol), limit=limit
        )

    @staticmethod
    def build_get_balances_params() -> BackpackRawGetBalancesParams:
        """Builds parameters for the get_balances endpoint.

        Returns:
            BackpackRawGetBalancesParams: The validated request parameters model.
            This endpoint requires no query parameters, but returns a model for consistency.
        """
        return BackpackRawGetBalancesParams()

    @staticmethod
    def build_get_positions_params(symbol: str | None) -> BackpackRawGetPositionsParams:
        """
        Builds parameters for the get_positions endpoint.

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
        # GET /api/v1/positions/{symbol}
        # Symbol is used in the URL path, not as a query parameter,
        # so we return an empty params model for consistency.
        return BackpackRawGetPositionsParams()

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
        """
        Builds the payload for placing an order.

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

        Raises:
            ValueError: If required parameters for an order type are missing or invalid.
        """
        # Business logic validation first
        if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and price is None:
            raise ValueError(f"Price is required for {order_type.value} orders.")

        if order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT] and trigger_price is None:
            raise ValueError(f"Trigger price is required for {order_type.value} orders.")

        # Map internal enums to API strings
        api_side = "Bid" if side == OrderSide.BUY else "Ask"

        api_order_type = {
            OrderType.LIMIT: "Limit",
            OrderType.MARKET: "Market",
            OrderType.STOP_MARKET: "Market",  # With triggerPrice it becomes a stop
            OrderType.STOP_LIMIT: "Limit",  # With triggerPrice it becomes a stop limit
        }.get(order_type)

        if api_order_type is None:
            raise ValueError(f"Unsupported order type for Backpack: {order_type}")

        # Map time in force
        api_time_in_force = None
        if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
            api_time_in_force = {
                TimeInForce.GTC: "GTC",
                TimeInForce.IOC: "IOC",
                TimeInForce.FOK: "FOK",
            }.get(time_in_force)
            if api_time_in_force is None:
                raise ValueError(f"Unsupported time in force: {time_in_force}")

        # Convert client_order_id string to int if provided
        client_id = None
        if client_order_id:
            try:
                client_id = int(client_order_id)
            except ValueError as e:
                raise ValueError(
                    f"client_order_id must be convertible to integer, got: {client_order_id}"
                ) from e

        # Map self trade prevention
        api_self_trade_prevention = None
        if self_trade_prevention:
            api_self_trade_prevention = {
                "RejectTaker": "RejectTaker",
                "RejectMaker": "RejectMaker",
                "RejectBoth": "RejectBoth",
            }.get(self_trade_prevention)
            if api_self_trade_prevention is None:
                raise ValueError(f"Invalid self trade prevention: {self_trade_prevention}")

        # Build the raw request model
        request_data: dict[str, Any] = {
            "orderType": api_order_type,
            "side": api_side,
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        # Add required quantity field
        request_data["quantity"] = str(quantity)
        if price is not None:
            request_data["price"] = str(price)
        if client_id is not None:
            request_data["clientId"] = client_id
        if post_only and order_type == OrderType.LIMIT:
            request_data["postOnly"] = post_only
        if api_time_in_force is not None:
            request_data["timeInForce"] = api_time_in_force
        if reduce_only:
            request_data["reduceOnly"] = reduce_only
        if api_self_trade_prevention is not None:
            request_data["selfTradePrevention"] = api_self_trade_prevention

        # Add stop loss fields if provided
        if stop_loss_trigger_price is not None:
            request_data["stopLossTriggerPrice"] = str(stop_loss_trigger_price)
        if stop_loss_trigger_by is not None:
            request_data["stopLossTriggerBy"] = stop_loss_trigger_by
        if stop_loss_limit_price is not None:
            request_data["stopLossLimitPrice"] = str(stop_loss_limit_price)

        # Add take profit fields if provided
        if take_profit_trigger_price is not None:
            request_data["takeProfitTriggerPrice"] = str(take_profit_trigger_price)
        if take_profit_trigger_by is not None:
            request_data["takeProfitTriggerBy"] = take_profit_trigger_by
        if take_profit_limit_price is not None:
            request_data["takeProfitLimitPrice"] = str(take_profit_limit_price)

        # Create and return the Pydantic model
        return BackpackRawOrderExecuteRequest(**request_data)

    @staticmethod
    def build_cancel_order_payload(
        symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> BackpackRawOrderCancelRequest:
        """
        Builds the JSON payload for cancelling an order.
        Corresponds to DELETE /api/v1/order request body.

        Args:
            symbol: The trading symbol.
            order_id: The order ID (optional if client_order_id is provided).
            client_order_id: The client order ID (optional if order_id is provided).

        Returns:
            BackpackRawOrderCancelRequest: The validated request payload model.

        Raises:
            ValueError: If neither order_id nor client_order_id is provided.
        """
        # Business logic validation
        if not order_id and not client_order_id:
            raise ValueError("Either orderId or clientId must be provided to cancel an order.")

        if order_id and client_order_id:
            raise ValueError("Only one of orderId or clientId should be provided, not both.")

        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        if order_id:
            request_data["orderId"] = order_id

        if client_order_id:
            # Convert to int for the API
            try:
                client_id = int(client_order_id)
                request_data["clientId"] = client_id
            except ValueError as e:
                raise ValueError(
                    f"client_order_id must be convertible to integer, got: {client_order_id}"
                ) from e

        # Create and return the Pydantic model
        return BackpackRawOrderCancelRequest(**request_data)

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str | None, order_type_filter: str | None = None
    ) -> BackpackRawOrderCancelAllRequest:
        """
        Builds the payload for cancelling all orders for a symbol.

        Args:
            symbol: The trading symbol.
            order_type_filter: Optional filter for order type.

        Returns:
            BackpackRawOrderCancelAllRequest: The validated request payload model.
        """
        # Validate required symbol parameter
        if symbol is None:
            raise ValueError("invalid symbol")

        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
        }

        if order_type_filter:
            if order_type_filter not in ["RestingLimitOrder", "ConditionalOrder"]:
                raise ValueError(f"Invalid order type filter: {order_type_filter}")
            request_data["orderType"] = order_type_filter

        # Create and return the Pydantic model
        return BackpackRawOrderCancelAllRequest(**request_data)

    @staticmethod
    def build_get_open_orders_params(symbol: str | None) -> BackpackRawGetOpenOrdersParams:
        """
        Builds parameters for the get_open_orders endpoint.

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
        """
        Builds parameters for fetching the current funding rate for a single symbol.

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
        """
        Builds parameters for fetching historical funding rates (/api/v1/fundingRates).

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            start_time_ms: Optional start time in milliseconds since Unix epoch.
            end_time_ms: Optional end time in milliseconds since Unix epoch.
            limit: Optional limit on the number of results.

        Returns:
            BackpackRawGetHistoricalFundingRatesParams: The validated request parameters model.
        """
        return BackpackRawGetHistoricalFundingRatesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            startTime=start_time_ms,
            endTime=end_time_ms,
            limit=limit,
        )

    @staticmethod
    def build_get_account_info_params() -> BackpackRawGetAccountInfoParams:
        """
        Builds parameters for the get_account_info endpoint (GET /api/v1/account).

        Returns:
            BackpackRawGetAccountInfoParams: The validated request parameters model.
            This endpoint requires no query parameters, but returns a model for consistency.
        """
        return BackpackRawGetAccountInfoParams()

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
        """
        Builds the payload for a withdrawal request.

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
        """
        # Business logic validation
        if amount <= 0:
            raise ValueError("Amount must be positive")

        # Map network names to blockchain values
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

        blockchain = blockchain_mapping.get(network)
        if blockchain is None:
            raise ValueError(
                f"Unsupported network: {network}. "
                f"Supported networks: {list(blockchain_mapping.keys())}"
            )

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
        """
        Builds parameters for the get_order_history endpoint.

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
        """
        Builds parameters for the get_trade_history (fills) endpoint.

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
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
        ],
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
    ) -> BackpackRawGetMarketDataParams:
        """
        Builds parameters for fetching market data (candlesticks).
        GET /api/v1/klines

        Args:
            symbol: The trading symbol.
            timeframe_str: The candlestick interval string (e.g., "1m", "1h").
            start_time_ms: Start time in milliseconds (optional).
            end_time_ms: End time in milliseconds (optional).
            limit: Maximum number of candles to retrieve (optional).

        Returns:
            BackpackRawGetMarketDataParams: The validated request parameters model.

        Raises:
            ValueError: If timeframe_str is not a supported interval.
        """
        # Business logic validation: timeframe_str will be validated by the Literal type
        # in the Pydantic model, but we can provide a more user-friendly error here
        supported_intervals = {
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
        }
        if timeframe_str not in supported_intervals:
            raise ValueError(
                f"Unsupported interval '{timeframe_str}'. "
                f"Supported intervals: {sorted(supported_intervals)}"
            )

        return BackpackRawGetMarketDataParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol),
            interval=timeframe_str,  # This will be validated by Literal in the model
            startTime=start_time_ms,
            endTime=end_time_ms,
            limit=limit,
        )

    @staticmethod
    def build_get_historical_trades_params(
        symbol: str, limit: int | None, from_id: str | None
    ) -> BackpackRawGetHistoricalTradesParams:
        """
        Builds parameters for fetching historical public trades.
        GET /api/v1/trades/history

        Args:
            symbol: The trading symbol.
            limit: Maximum number of trades to retrieve (optional).
            from_id: Get trades from this ID onwards (optional, for pagination).

        Returns:
            BackpackRawGetHistoricalTradesParams: The validated request parameters model.
        """
        return BackpackRawGetHistoricalTradesParams(
            symbol=BackpackRequestBuilder.format_symbol(symbol), limit=limit, fromId=from_id
        )

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: str,
        amount_str: str,  # Quantity as string
        from_account: str,  # e.g., "SPOT", "MARGIN", "FUTURES"
        to_account: str,  # e.g., "SPOT", "MARGIN", "FUTURES"
        client_transfer_id: str | None = None,
    ) -> BackpackRawInternalTransferRequest:
        """
        Builds the payload for an internal capital transfer.

        Args:
            asset_symbol: The symbol of the asset to transfer (e.g., "USDC").
            amount_str: The quantity of the asset to transfer, as a string.
            from_account: The source account type.
            to_account: The destination account type.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            BackpackRawInternalTransferRequest: The validated request payload model.

        Raises:
            ValueError: If account types are invalid.
        """
        # Business logic validation
        valid_accounts = {"SPOT", "MARGIN", "FUTURES"}
        if from_account not in valid_accounts:
            raise ValueError(
                f"Invalid from_account: {from_account}. Must be one of {valid_accounts}"
            )
        if to_account not in valid_accounts:
            raise ValueError(f"Invalid to_account: {to_account}. Must be one of {valid_accounts}")
        if from_account == to_account:
            raise ValueError("from_account and to_account cannot be the same")

        request_data: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(asset_symbol),
            "quantity": amount_str,
            "fromAccount": from_account,
            "toAccount": to_account,
        }
        if client_transfer_id:
            request_data["clientId"] = client_transfer_id

        # Create and return the Pydantic model
        return BackpackRawInternalTransferRequest(**request_data)

    @staticmethod
    def build_get_order_params(symbol: str) -> BackpackRawGetOrderParams:
        """
        Builds parameters for GET /api/v1/orders/{orderIdOrClientId}.
        This endpoint requires 'symbol' as a query parameter.

        Args:
            symbol: The trading symbol for the order (e.g., "SOL_USDC").

        Returns:
            BackpackRawGetOrderParams: The validated request parameters model.

        Raises:
            ValueError: If symbol is empty or None.
        """
        if not symbol:
            raise ValueError("Symbol is required for build_get_order_params.")
        return BackpackRawGetOrderParams(symbol=BackpackRequestBuilder.format_symbol(symbol))

    @staticmethod
    def build_convert_dust_payload(asset_symbol: str) -> BackpackRawAccountConvertDustRequest:
        """
        Builds the payload for converting dust balances to USDC.

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
        asset_symbol: str, quantity: Decimal, side: str
    ) -> BackpackRawBorrowLendExecuteRequest:
        """
        Builds the payload for borrowing or lending operations.

        Args:
            asset_symbol: The asset symbol (e.g., "USDC").
            quantity: The quantity to borrow/lend/repay/redeem.
            side: The operation type ("Borrow", "Lend", "Repay", "Redeem").

        Returns:
            BackpackRawBorrowLendExecuteRequest: The validated request payload model.

        Raises:
            ValueError: If side is invalid or quantity is not positive.
        """
        # Business logic validation
        valid_sides = {"Borrow", "Lend", "Repay", "Redeem"}
        if side not in valid_sides:
            raise ValueError(f"Invalid side: {side}. Must be one of {valid_sides}")

        if quantity <= 0:
            raise ValueError("Quantity must be positive")

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
    ) -> BackpackRawUpdateAccountSettingsRequest:
        """
        Builds the payload for updating account settings.

        Args:
            auto_borrow_settlements: Enable/disable auto borrow settlements.
            auto_lend: Enable/disable auto lending.
            auto_realize_pnl: Enable/disable auto PnL realization.
            auto_repay_borrows: Enable/disable auto repay borrows.

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
        """
        Builds the payload for submitting a Request For Quote (RFQ).

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

        Raises:
            ValueError: If neither quantity nor quote_quantity is provided.
        """
        # Business logic validation
        if quantity is None and quote_quantity is None:
            raise ValueError("Either quantity or quote_quantity must be provided")

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
        rfq_id: str, side: OrderSide, price: Decimal, client_quote_id: str | None = None
    ) -> BackpackRawQuoteSubmitRequest:
        """
        Builds the payload for submitting a quote in response to an RFQ.

        Args:
            rfq_id: The RFQ ID to respond to.
            side: The side of the quote (BUY or SELL).
            price: The quoted price.
            client_quote_id: Optional client-provided quote ID.

        Returns:
            BackpackRawQuoteSubmitRequest: The validated request payload model.

        Raises:
            ValueError: If price is not positive.
        """
        # Business logic validation
        if price <= 0:
            raise ValueError("Price must be positive")

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
        """
        Builds the payload for accepting a quote.

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
        """
        Builds the payload for cancelling an RFQ.

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
        """
        Builds the payload for refreshing an RFQ.

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
