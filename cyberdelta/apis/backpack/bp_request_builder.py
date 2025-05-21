from __future__ import annotations

from decimal import Decimal
from typing import Any

from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackRequestBuilder:
    """
    Builds request parameters and payloads for Backpack API endpoints.

    This class centralizes the logic for constructing the dictionaries
    needed for various Backpack API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """
        Initializes the BackpackRequestBuilder.

        Args:
            config: A dictionary containing API configuration,
                    expected to have a 'base_url'.
        """
        self._api_config = config
        # Ensure base_url is available or handle its absence appropriately
        self.base_url = str(self._api_config.get("base_url", ""))  # Ensure string
        if not self.base_url:
            logger.error(
                "base_url not found or empty in API configuration for BackpackRequestBuilder."
            )
            # Consider raising ConfigurationError or similar custom exception
            raise ValueError(
                "base_url not found or empty in API configuration for BackpackRequestBuilder"
            )

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
    def build_get_ticker_params(symbol: str) -> dict[str, str]:
        """
        Builds parameters for the get_ticker endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").

        Returns:
            dict[str, str]: The request parameters dictionary.
        """
        return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}

    @staticmethod
    def build_get_order_book_params(symbol: str, limit: int | None) -> dict[str, Any]:
        """
        Builds parameters for the get_order_book endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of bids and asks to retrieve (optional).

        Returns:
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if limit is not None:
            params["limit"] = limit
        return params

    @staticmethod
    def build_get_recent_trades_params(symbol: str, limit: int | None) -> dict[str, Any]:
        """
        Builds parameters for the get_recent_trades endpoint.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve (optional).

        Returns:
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if limit is not None:
            params["limit"] = limit
        return params

    @staticmethod
    def build_get_balances_params() -> dict[str, Any] | None:
        """Builds parameters for the get_balances endpoint (no specific params needed)."""
        return None  # GET /api/v1/capital

    @staticmethod
    def build_get_positions_params(symbol: str | None) -> dict[str, Any] | None:
        """
        Builds parameters for the get_positions endpoint.

        Args:
            symbol: The trading symbol to filter by (optional).

        Returns:
            dict[str, Any] | None: The request parameters dictionary or None.
        """
        # GET /api/v1/positions
        # GET /api/v1/positions/{symbol}
        # This builder assumes the path will be constructed with symbol if provided,
        # so no explicit "symbol" param here for the generic /api/v1/positions call.
        # If symbol is used in path, params should be None. If symbol is a query param,
        # then it should be included here.
        # Based on typical REST, if it's /positions/{symbol}, params is None.
        # If it's /positions?symbol=..., then params would include it.
        # The original bp_api.py for get_positions(symbol=None) calls /api/v1/positions
        # and for get_positions(symbol="SOL_USDC") calls /api/v1/positions/SOL_USDC.
        # So, no query parameters are needed from the builder.
        return None

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
    ) -> dict[str, Any]:
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

        Returns:
            dict[str, Any]: The request payload dictionary.

        Raises:
            ValueError: If required parameters for an order type are missing or invalid.
        """
        # Map internal OrderSide to Backpack API's side values
        api_side: str
        if side == OrderSide.BUY:
            api_side = "Bid"
        elif side == OrderSide.SELL:
            api_side = "Ask"
        else:
            # Should not happen if OrderSide enum is used correctly
            raise ValueError(f"Unsupported order side: {side}")

        # Map internal OrderType to Backpack API's orderType values (case-sensitive)
        api_order_type: str
        if order_type == OrderType.LIMIT:
            api_order_type = "Limit"
        elif order_type == OrderType.MARKET:
            api_order_type = "Market"
        elif order_type == OrderType.STOP_MARKET:
            api_order_type = "Stop"  # Backpack uses "Stop" for Stop Market
        elif order_type == OrderType.STOP_LIMIT:
            api_order_type = "Limit"  # Backpack uses "Limit" for Stop Limit, with triggerPrice
        else:
            raise ValueError(f"Unsupported or unmapped order type for Backpack: {order_type}")

        payload: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
            "side": api_side,
            "orderType": api_order_type,
            "quantity": str(quantity),
        }

        # Only include timeInForce for Limit and StopLimit orders.
        if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
            if time_in_force == TimeInForce.GTC:
                payload["timeInForce"] = "GTC"
            elif time_in_force == TimeInForce.IOC:
                payload["timeInForce"] = "IOC"
            elif time_in_force == TimeInForce.FOK:
                payload["timeInForce"] = "FOK"
            else:
                raise ValueError(f"Unsupported or unmapped time in force: {time_in_force}")
        elif (
            time_in_force != TimeInForce.GTC
        ):  # For MARKET and STOP_MARKET, GTC is implicit or not applicable
            logger.warning(
                f"time_in_force {time_in_force.value} ignored for order type {order_type.value}"
            )

        # Price validation and assignment
        if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
            if price is None:
                raise ValueError(f"Price is required for {order_type.value} orders.")
            payload["price"] = str(price)

        # Trigger price validation and assignment for stop orders
        if order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]:
            if trigger_price is None:
                raise ValueError(f"Trigger price is required for {order_type.value} orders.")
            payload["triggerPrice"] = str(trigger_price)

        if client_order_id:
            payload["clientId"] = client_order_id

        if post_only:
            if order_type == OrderType.LIMIT:
                payload["postOnly"] = True
            else:
                logger.warning(f"postOnly=True ignored for non-LIMIT order type {order_type.value}")

        return payload

    @staticmethod
    def build_cancel_order_payload(
        symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> dict[str, Any]:
        """
        Builds the JSON payload for cancelling an order.
        Corresponds to DELETE /api/v1/order request body.

        Args:
            symbol: The trading symbol.
            order_id: The order ID (optional if client_order_id is provided).
            client_order_id: The client order ID (optional if order_id is provided).

        Returns:
            dict[str, Any]: The request payload dictionary.

        Raises:
            ValueError: If neither order_id nor client_order_id is provided.
        """
        payload: dict[str, Any] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if order_id:
            payload["orderId"] = order_id
        elif client_order_id:
            # Assuming Backpack uses 'clientId' as a string in the payload, similar to other fields.
            # If it's an int, ensure proper conversion if client_order_id is passed as int.
            # For consistency with OrderExecutePayload clientId, let's assume it can be int or str.
            # However, OrderCancelPayload in OpenAPI spec shows clientId as uint32.
            # For now, will keep as string if passed as string, matching orderId.
            # If strict uint32 is required by API and causes issues, this may need adjustment.
            payload["clientId"] = client_order_id  # Keeping as string to match orderId type in dict
        else:
            raise ValueError("Either orderId or clientId must be provided to cancel an order.")
        return payload

    @staticmethod
    def build_get_open_orders_params(symbol: str | None) -> dict[str, str] | None:
        """
        Builds parameters for the get_open_orders endpoint.

        Args:
            symbol: The trading symbol to filter by (optional).

        Returns:
            dict[str, str] | None: The request parameters dictionary or None.
        """
        if symbol:
            return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        return None  # No params if fetching for all symbols

    @staticmethod
    def build_get_funding_rate_params(symbol: str) -> dict[str, str]:
        """
        Builds parameters for fetching the current funding rate for a single symbol.
        This typically corresponds to an endpoint like /api/v1/funding or derived from mark prices.
        Note: For historical funding rates, use build_get_historical_funding_rates_params.
        """
        # This existing method might be for a different endpoint (e.g., current funding)
        # or needs adjustment if it was intended for /api/v1/fundingRates
        # For now, assuming it's for a single current rate, if such an endpoint exists.
        # If Backpack only has /api/v1/fundingRates, this method might be redundant or
        # should call build_get_historical_funding_rates_params with limit=1.
        # Let's assume it's for a different purpose or a simplified current rate fetch.
        return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}

    @staticmethod
    def build_get_historical_funding_rates_params(
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Builds parameters for fetching historical funding rates (/api/v1/fundingRates).

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            start_time_ms: Optional start time in milliseconds since Unix epoch.
            end_time_ms: Optional end time in milliseconds since Unix epoch.
            limit: Optional limit on the number of results.

        Returns:
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms  # API expects startTime
        if end_time_ms is not None:
            params["endTime"] = end_time_ms  # API expects endTime
        if limit is not None:
            params["limit"] = limit
        return params

    @staticmethod
    def build_get_account_info_params() -> dict[str, Any] | None:
        """
        Builds parameters for the get_account_info endpoint (GET /api/v1/account).
        No specific query parameters are typically needed.
        """
        return None

    @staticmethod
    def build_withdraw_payload(
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None,
        tag: str | None = None,
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
    ) -> dict[str, Any]:
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

        Returns:
            dict[str, Any]: The request payload dictionary.
        """
        if not network:
            raise ValueError("Network is required for withdrawals.")

        payload: dict[str, Any] = {
            "blockchain": network,  # Backpack uses 'blockchain' for network
            "coin": asset.upper(),
            "quantity": str(amount),
            "address": address,
        }
        if tag:
            payload["addressTag"] = tag
        if client_withdrawal_id:
            payload["clientId"] = client_withdrawal_id
        if two_factor_token:
            payload["twoFactorToken"] = two_factor_token
        return payload

    @staticmethod
    def build_get_order_history_params(
        symbol: str | None,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
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
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = BackpackRequestBuilder.format_symbol(symbol)
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:  # Only one of orderId or clientId should be used per API docs
            params["clientId"] = client_order_id
        if limit is not None:
            params["limit"] = limit
        if start_time_ms is not None:
            params["from"] = start_time_ms  # Backpack uses 'from' for start time
        if end_time_ms is not None:
            params["to"] = end_time_ms  # Backpack uses 'to' for end time
        return params

    @staticmethod
    def build_get_trade_history_params(
        symbol: str | None,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
        from_id: str | None = None,  # For pagination based on trade ID
    ) -> dict[str, Any]:
        """
        Builds parameters for the get_trade_history (fills) endpoint.

        Args:
            symbol: The trading symbol (optional).
            start_time_ms: Start time in milliseconds (optional).
            end_time_ms: End time in milliseconds (optional).
            limit: Maximum number of trades to retrieve (optional).
            from_id: Get trades from this ID onwards (optional, for pagination).

        Returns:
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = BackpackRequestBuilder.format_symbol(symbol)
        if limit is not None:
            params["limit"] = limit
        if start_time_ms is not None:
            params["from"] = start_time_ms
        if end_time_ms is not None:
            params["to"] = end_time_ms
        if from_id is not None:
            # Check Backpack docs for exact param name for trade ID pagination
            # (e.g., fromId, startId)
            # Assuming "fromId" as a placeholder if it's different from timestamp "from"
            params["fromId"] = from_id  # Placeholder: verify actual param name
        return params

    @staticmethod
    def build_get_market_data_params(
        symbol: str,
        timeframe_str: str,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int | None,
    ) -> dict[str, Any]:
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
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(symbol),
            "interval": timeframe_str,
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        if limit is not None:
            params["limit"] = limit
        return params

    @staticmethod
    def build_get_historical_trades_params(
        symbol: str, limit: int | None, from_id: str | None
    ) -> dict[str, Any]:
        """
        Builds parameters for fetching historical public trades.
        GET /api/v1/trades/history

        Args:
            symbol: The trading symbol.
            limit: Maximum number of trades to retrieve (optional).
            from_id: Get trades from this ID onwards (optional, for pagination).

        Returns:
            dict[str, Any]: The request parameters dictionary.
        """
        params: dict[str, Any] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if limit is not None:
            params["limit"] = limit
        if from_id is not None:
            params["fromId"] = from_id  # Assuming fromId, check docs
        return params

    @staticmethod
    def build_cancel_all_orders_payload(symbol: str | None) -> dict[str, Any] | None:
        """
        Builds the payload for cancelling all orders for a specific symbol or all symbols.
        DELETE /api/v1/orders

        Args:
            symbol: The trading symbol. If None, cancels orders for all symbols.
                    Backpack's DELETE /api/v1/orders might require a symbol in the body
                    or as a query param. This needs to be verified.
                    If it's a query param, this method should return params dict.
                    If it's in body for DELETE, this returns a payload dict.
                    If symbol is optional and not providing it means all, behavior varies.
                    The original bp_api.py cancelled one by one; this is for a bulk
                    endpoint if available.
                    Assuming for now that if a symbol is provided, it's a query parameter.
                    If DELETE to /api/v1/orders with a body is supported:
                    payload = {
                        "symbol": BackpackRequestBuilder.format_symbol(symbol)
                    } if symbol else {}
                    return payload
                    If it's a query param and symbol is optional:
                    return {
                        "symbol": BackpackRequestBuilder.format_symbol(symbol)
                    } if symbol else None

                    Current bp_api.py does not use a bulk cancel endpoint, it gets open
                    orders and cancels them one by one. So this builder method might be
                    for a future enhancement if Backpack adds a bulk cancel with specific
                    payload/params.
                    For now, returning None as no specific payload is defined for a bulk
                    cancel in current usage.
        """
        # If Backpack requires a symbol in the body for DELETE /api/v1/orders for all
        # orders of that symbol:
        # if symbol:
        #     return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        # return {} # Empty body for all symbols if API supports it

        # If it's a query parameter:
        # if symbol:
        #     return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        # return None

        # As current BackpackAPI doesn't use a single call for cancel_all_orders with a payload,
        # this builder method is speculative. Assuming it would take query params for now.
        if symbol:
            return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        return None  # No params means all, if API supports that for DELETE /orders

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: str,
        amount_str: str,  # Quantity as string
        from_account: str,  # e.g., "SPOT", "MARGIN", "FUTURES"
        to_account: str,  # e.g., "SPOT", "MARGIN", "FUTURES"
        client_transfer_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Builds the payload for an internal capital transfer.

        Args:
            asset_symbol: The symbol of the asset to transfer (e.g., "USDC").
            amount_str: The quantity of the asset to transfer, as a string.
            from_account: The source account type.
            to_account: The destination account type.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            dict[str, Any]: The request payload dictionary.
        """
        payload: dict[str, Any] = {
            "symbol": BackpackRequestBuilder.format_symbol(asset_symbol),
            "quantity": amount_str,
            "fromAccount": from_account,
            "toAccount": to_account,
        }
        if client_transfer_id:
            payload["clientId"] = client_transfer_id
        return payload

    @staticmethod
    def build_get_order_params(symbol: str) -> dict[str, Any]:
        """
        Builds parameters for GET /api/v1/orders/{orderIdOrClientId}.
        This endpoint requires 'symbol' as a query parameter.
        """
        if not symbol:
            raise ValueError("Symbol is required for build_get_order_params.")
        return {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
