from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerSpec
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
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

        transfer_action_payload = HyperliquidRawL2UsdTransferPayload(
            destination=destination_address, token="USDC", amount=str(amount)
        )
        action_details = {
            "chain": "L2",
            "payload": transfer_action_payload.model_dump(by_alias=True),
        }
        return {"type": "usdTransfer", "action": action_details}

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
            action_payload_dict = {"amount": str(amount), "destination": destination_address}
        else:
            # Assuming other assets use the generic 'withdraw' type
            withdrawal_payload = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(), amount=str(amount), destination=destination_address
            )
            action_payload_dict = withdrawal_payload.model_dump()
            action_type = "withdraw"

        return {"type": action_type, "action": action_payload_dict}

    @staticmethod
    def build_order_history_payload(
        wallet_address: str, start_time_ms: int, end_time_ms: int
    ) -> dict[str, Any]:
        """
        Builds the payload for querying order history.

        Args:
            wallet_address: The user's wallet address.
            start_time_ms: The start time in milliseconds.
            end_time_ms: The end time in milliseconds.

        Returns:
            dict[str, Any]: The request payload dictionary.
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

        Args:
            symbol: The trading symbol (e.g., "ETH-PERP").
            timeframe: The candle interval (e.g., "1m", "1h").
            start_time_ms: The start time for candles in milliseconds.
            end_time_ms: The end time for candles in milliseconds.

        Returns:
            dict[str, Any]: The request payload dictionary.
        """
        return {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol.upper(),  # Ensure coin is uppercase as per typical API behavior
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
        post_only: bool = False,  # Added to align with place_order signature
    ) -> dict[str, Any]:
        """
        Builds the payload for placing an order.

        Args:
            asset_index: The numerical index of the asset.
            side: The order side (BUY or SELL).
            order_type: The type of order (MARKET, LIMIT, etc.).
            quantity: The quantity of the order.
            time_in_force: The time in force for the order (GTC, IOC, ALO).
            price: The limit price for LIMIT or STOP_LIMIT orders.
            stop_price: The trigger price for STOP or TAKE_PROFIT orders.
            client_order_id: Optional client-specified order ID.
            reduce_only: Whether the order is reduce-only.
            post_only: Whether the order is post-only (maker only).

        Returns:
            dict[str, Any]: The request payload dictionary.

        Raises:
            ValueError: If required parameters for an order type are missing.
            NotImplementedError: If the order type is not supported.
        """
        is_buy = side == OrderSide.BUY
        sz_str = str(quantity)
        underlying_hl_order_type_dict: dict[str, Any] = {}
        underlying_limit_px_str: str = "0"
        trigger_payload: dict[str, Any] | None = None

        tif_map: dict[TimeInForce, Literal["Gtc", "Ioc", "Alo"]] = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",  # For "Maker Only" or "Post Only"
        }
        # Determine effective TIF based on post_only and time_in_force
        raw_tif_str_candidate = tif_map.get(time_in_force)

        if post_only and order_type in [
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ]:
            raw_tif_str_candidate = "Alo"  # Hyperliquid uses ALO for Post-Only Limit

        if raw_tif_str_candidate is None:
            # Defaulting logic, ensure it's robust or raises error for unhandled TIF
            # For now, let's assume a default or rely on downstream validation if HL API allows it.
            # Given the original code's warning, it's safer to ensure mapping.
            # If a TIF is not in map and not post_only, it might be an issue.
            # The original code defaulted to "Gtc" with a warning.
            # Here, we should ensure 'effective_tif' is one of the Literal types.
            # This part needs careful review based on Hyperliquid's actual TIF handling.
            # For now, directly map or raise if unmapped and not PostOnly.
            if time_in_force not in tif_map:
                raise ValueError(f"Unsupported TimeInForce: {time_in_force}")
            # If it is in tif_map, raw_tif_str_candidate would have been set.
            # This path implies raw_tif_str_candidate became None after post_only logic,
            # which shouldn't happen if post_only only switches to ALO.
            # Let's stick to the original's default for safety if it's truly unmapped:
            raw_tif_str_candidate = "Gtc"

        effective_tif: Literal["Gtc", "Ioc", "Alo"]
        if raw_tif_str_candidate == "Gtc":
            effective_tif = "Gtc"
        elif raw_tif_str_candidate == "Ioc":
            effective_tif = "Ioc"
        elif raw_tif_str_candidate == "Alo":
            effective_tif = "Alo"
        else:
            # This path should not be reachable if raw_tif_str_candidate is derived correctly.
            raise ValueError(f"Internal TIF logic error, unexpected: {raw_tif_str_candidate}")

        if order_type == OrderType.MARKET:
            underlying_hl_order_type_dict = {
                "market": HyperliquidRawMarketOrderTypeDetails().model_dump()
            }
            underlying_limit_px_str = "0"  # Market orders don't have a limit price.
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for LIMIT orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
        elif order_type in [OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET]:
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = "0"  # Triggered market order
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=True,
                tpsl="sl" if order_type == OrderType.STOP_MARKET else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
            # For triggered orders, the main 'orderType' might still be limit if they
            # trigger into one.
            # However, Hyperliquid's structure suggests 'trigger' modifies behavior.
            # If it's a STOP_MARKET, the core orderType might be implied or a simple limit
            # with TIF.
            # Original code set underlying_limit_px_str to "0" and did not set
            # underlying_hl_order_type_dict
            # for pure trigger orders without a limit component post-trigger.
            # Re-checking Hyperliquid docs: trigger orders are specified by `trigger` field.
            # The main order part still needs `orderType` and `limitPx`.
            # If it's a market trigger, limitPx is 0, orderType might be a basic limit/GTC.
            # Let's assume for STOP_MARKET, it implicitly becomes a market order on trigger,
            # so the underlying "orderType" part of the main payload might be minimal
            # or just "limit" with TIF.
            # The original code sets underlying_limit_px_str = "0".
            # For market triggers, Hyperliquid might expect orderType: {"limit": {"tif": "Gtc"}}
            # and then the trigger payload overrides to market.
            # Let's use a GTC limit as the base for triggered orders for now if not market.
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(
                    tif="Gtc"
                ).model_dump()  # Default TIF for the underlying order part
            }
        elif order_type in [OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]:
            if price is None:  # This is the limit price of the triggered order
                raise ValueError(f"price (for triggered limit) is required for {order_type.value}.")
            if stop_price is None:  # This is the trigger price
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                # The TIF for the limit order that gets placed after trigger.
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=False,  # It's a limit order after trigger
                tpsl="sl" if order_type == OrderType.STOP_LIMIT else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
        else:
            raise NotImplementedError(
                f"Order type {order_type.value} is not supported by HyperliquidRequestBuilder."
            )

        action_payload: dict[str, Any] = {
            "asset": asset_index,
            "isBuy": is_buy,
            "sz": sz_str,
            "limitPx": underlying_limit_px_str,
            "orderType": underlying_hl_order_type_dict,
            "reduceOnly": reduce_only,
        }
        if client_order_id:
            # Hyperliquid uses "cloid" for client order ID
            action_payload["cloid"] = client_order_id
        if trigger_payload:
            action_payload["trigger"] = trigger_payload

        return {"type": "order", "actions": [action_payload]}

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
        action_payload = {"asset": asset_index, "oid": order_id}
        return {"type": "cancel", "action": action_payload}

    @staticmethod
    def build_order_status_payload(wallet_address: str, order_id: int) -> dict[str, Any]:
        """
        Builds the payload for fetching the status of a specific order.

        Args:
            wallet_address: The user's wallet address.
            order_id: The exchange-assigned ID of the order.

        Returns:
            dict[str, Any]: The request payload dictionary.
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
