from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

# Specific model imports for type hints and construction
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
    HyperliquidRawTriggerDetails,  # Ensure this is imported if used separately
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce


class HyperliquidRequestBuilder:
    """
    Builds request payloads for Hyperliquid API endpoints.

    This class centralizes the logic for constructing the dictionaries
    needed for various Hyperliquid API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    @staticmethod
    def build_info_request_payload() -> dict[str, Any] | None:
        """
        Builds the payload for general Hyperliquid INFO requests.

        Many Hyperliquid INFO endpoints accept a POST request with an empty
        body or no specific payload, returning a comprehensive state object.

        Returns:
            dict[str, Any] | None: An empty dictionary or None,
                                    representing no specific payload.
        """
        return None  # Or {} depending on how _request handles None data

    @staticmethod
    def build_l2_usd_transfer_payload(destination_address: str, amount: Decimal) -> dict[str, Any]:
        """
        Builds the payload for an L2 USD transfer.

        Args:
            destination_address: The recipient wallet address.
            amount: The amount of USDC to transfer.

        Returns:
            dict[str, Any]: The request payload dictionary.

        Raises:
            ValueError: If destination_address is empty.
        """
        if not destination_address:
            raise ValueError(
                "Destination address (to_account) is required for Hyperliquid L2 transfer."
            )

        transfer_payload_model = HyperliquidRawL2UsdTransferPayload(
            destination=destination_address, token="USDC", amount=str(amount)
        )
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2", payload=transfer_payload_model
        )
        return {"type": "usdTransfer", "action": action_details_model.model_dump(by_alias=True)}

    @staticmethod
    def build_withdrawal_payload(
        asset: str, amount: Decimal, destination_address: str
    ) -> dict[str, Any]:
        """
        Builds the payload for a withdrawal to L1.

        Args:
            asset: The asset to withdraw (e.g., "ETH", "USDC").
            amount: The amount to withdraw.
            destination_address: The L1 destination address.

        Returns:
            dict[str, Any]: The request payload dictionary.

        Raises:
            ValueError: If destination_address is empty.
        """
        if not destination_address:
            raise ValueError("Destination address is required for withdrawal.")

        action_type: str
        action_payload_dict: dict[str, Any]

        if asset.upper() == "ETH":
            action_type = "withdrawEth"
            eth_withdrawal_model = HyperliquidRawEthWithdrawalActionPayload(
                amount=str(amount), destination=destination_address
            )
            action_payload_dict = eth_withdrawal_model.model_dump(by_alias=True)
        else:
            # Assuming other assets use the generic 'withdraw' type
            withdrawal_payload_model = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(), amount=str(amount), destination=destination_address
            )
            action_payload_dict = withdrawal_payload_model.model_dump(by_alias=True)
            action_type = "withdraw"

        return {"type": action_type, "action": action_payload_dict}

    @staticmethod
    def build_order_history_payload(
        wallet_address: str, start_time_ms: int, end_time_ms: int
    ) -> dict[str, Any]:
        """
        Builds the payload for querying order history.
        This is a request payload, not an exchange action, and should use its own model if needed.
        For now, returning a dict as per existing structure, assuming it's validated by the caller.
        Alternatively, this could return a HyperliquidRawQueryOrderHistoryRequestPayload model instance.
        """
        return {
            "type": "queryOrderHistory",
            "user": wallet_address,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }

    @staticmethod
    def build_candle_snapshot_payload(
        symbol: str, timeframe: str, start_time_ms: int, end_time_ms: int
    ) -> dict[str, Any]:
        """
        Builds the payload for fetching candle snapshots.
        This is a request payload, not an exchange action.
        """
        return {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol.upper(),
                "interval": timeframe,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }

    @staticmethod
    def build_place_order_payload(
        asset_index: int,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict[str, Any]:
        """
        Builds the payload for placing an order using HyperliquidRawPlaceOrderAction.
        """
        is_buy = side == OrderSide.BUY
        sz_str = str(quantity)
        hl_order_type: HyperliquidRawOrderType
        limit_px_str: str
        hl_trigger_details: HyperliquidRawTriggerDetails | None = None

        tif_map: dict[TimeInForce, Literal["Gtc", "Ioc", "Alo"]] = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",
        }
        raw_tif_str_candidate = tif_map.get(time_in_force)

        if post_only and order_type in [
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ]:
            raw_tif_str_candidate = "Alo"

        if raw_tif_str_candidate is None:
            if time_in_force not in tif_map:
                raise ValueError(f"Unsupported TimeInForce: {time_in_force}")
            raw_tif_str_candidate = "Gtc"  # Defaulting as per original logic

        effective_tif: Literal["Gtc", "Ioc", "Alo"]
        if raw_tif_str_candidate == "Gtc":
            effective_tif = "Gtc"
        elif raw_tif_str_candidate == "Ioc":
            effective_tif = "Ioc"
        elif raw_tif_str_candidate == "Alo":
            effective_tif = "Alo"
        else:
            raise ValueError(f"Internal TIF logic error, unexpected: {raw_tif_str_candidate}")

        if order_type == OrderType.MARKET:
            hl_order_type = HyperliquidRawOrderType(market=HyperliquidRawMarketOrderTypeDetails())
            limit_px_str = "0"
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for LIMIT orders.")
            limit_px_str = str(price)
            hl_order_type = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif=effective_tif)
            )
        elif order_type in [OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET]:
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            limit_px_str = "0"
            hl_trigger_details = HyperliquidRawTriggerDetails(
                triggerPx=str(stop_price),
                isMarket=True,
                tpsl="sl" if order_type == OrderType.STOP_MARKET else "tp",
            )
            # For pure market triggers, HL might expect underlying order type to be a basic limit/GTC.
            hl_order_type = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
            )
        elif order_type in [OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]:
            if price is None:
                raise ValueError(f"price (for triggered limit) is required for {order_type.value}.")
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            limit_px_str = str(price)
            hl_order_type = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif=effective_tif)
            )
            hl_trigger_details = HyperliquidRawTriggerDetails(
                triggerPx=str(stop_price),
                isMarket=False,
                tpsl="sl" if order_type == OrderType.STOP_LIMIT else "tp",
            )
        else:
            raise NotImplementedError(
                f"Order type {order_type.value} is not supported by HyperliquidRequestBuilder."
            )

        place_order_action = HyperliquidRawPlaceOrderAction(
            asset=asset_index,
            isBuy=is_buy,
            sz=sz_str,
            limitPx=limit_px_str,
            orderType=hl_order_type,
            reduceOnly=reduce_only,
            cloid=client_order_id if client_order_id else None,  # Ensure None if empty
            trigger=hl_trigger_details,
        )

        return {"type": "order", "actions": [place_order_action.model_dump(by_alias=True)]}

    @staticmethod
    def build_cancel_order_payload(asset_index: int, order_id: int) -> dict[str, Any]:
        """
        Builds the payload for cancelling an order.

        Args:
            asset_index: The numerical index of the asset for the order.
            order_id: The exchange-assigned ID of the order to cancel.

        Returns:
            dict[str, Any]: The request payload dictionary.
        """
        action_model = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id)
        return {"type": "cancel", "action": action_model.model_dump(by_alias=True)}

    @staticmethod
    def build_order_status_payload(wallet_address: str, order_id: int) -> dict[str, Any]:
        """
        Builds the payload for fetching the status of a specific order.
        This is a request payload, not an exchange action.
        """
        return {"type": "orderStatus", "user": wallet_address, "oid": order_id}

    # For methods like get_balances, get_positions, get_open_orders, get_ticker,
    # get_order_book, get_recent_trades, get_funding_rate, get_trade_history,
    # get_funding_rates, _get_asset_index (info call part) which POST to /info
    # without a specific request body (or an empty one), they will all use
    # build_info_request_payload().
    # No separate builder methods are needed if the payload is consistently None or {}.
    # The calling methods in HyperliquidAPI will use build_info_request_payload().
    # This simplifies the builder significantly.
    # The differentiation happens in the response parsing and Pydantic model used.

    # Note: If any /info endpoints start requiring specific "type" in their POST body,
    # then dedicated builder methods would be needed. For now, current hl_api.py
    # suggests these are general POSTs to /info.
    # Example: get_order_book POSTs to /info and validates with HyperliquidRawL2Book,
    # implying /info returns L2 book data without a specific {type: l2Book} in request.
    # If this assumption is wrong, more builder methods for /info calls are needed.
    # Based on current hl_api.py, the generic build_info_request_payload should cover these.

    # Method for _get_asset_index's POST to /info
    # This also seems to be a general POST to /info expecting MetaAndAssetCtxs
    # So, build_info_request_payload() would apply.
    # The method _get_asset_index itself is not a public API endpoint method but an internal helper.
    # The request it makes will be refactored to use the builder.
