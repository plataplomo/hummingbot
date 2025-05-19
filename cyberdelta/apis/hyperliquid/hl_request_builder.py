from __future__ import annotations

from decimal import Decimal
from typing import Literal

# Specific model imports for type hints and construction
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
    HyperliquidApiPlaceOrderRequest,
    HyperliquidApiTokenWithdrawalRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
    HyperliquidRawQueryOrderHistoryRequestPayload,
    HyperliquidRawTriggerDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2BookRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawRecentTradesRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawUserStateRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFillsRequestPayload
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce


class HyperliquidRequestBuilder:
    """
    Builds request payloads for Hyperliquid API endpoints.

    This class centralizes the logic for constructing Pydantic models
    representing requests for Hyperliquid API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    @staticmethod
    def build_info_request_payload() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """
        Builds the Pydantic model for fetching meta and asset contexts via /info.
        Payload: {"type": "metaAndAssetCtxs"}
        """
        return HyperliquidRawMetaAndAssetCtxsRequestPayload(type="metaAndAssetCtxs")

    @staticmethod
    def build_l2_book_request_payload(symbol: str) -> HyperliquidRawL2BookRequestPayload:
        """
        Builds the Pydantic model for fetching L2 order book data.
        Payload: {"type": "l2Book", "coin": "SYMBOL"}
        """
        return HyperliquidRawL2BookRequestPayload(type="l2Book", coin=symbol.upper())

    @staticmethod
    def build_recent_trades_request_payload(
        symbol: str,
    ) -> HyperliquidRawRecentTradesRequestPayload:
        """
        Builds the Pydantic model for fetching recent public trades.
        Payload: {"type": "recentTrades", "coin": "SYMBOL"}
        """
        return HyperliquidRawRecentTradesRequestPayload(type="recentTrades", coin=symbol.upper())

    @staticmethod
    def build_l2_usd_transfer_payload(
        destination_address: str, amount: Decimal
    ) -> HyperliquidApiL2UsdTransferRequest:
        """
        Builds the Pydantic model for an L2 USD transfer request.
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
        return HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    @staticmethod
    def build_withdrawal_payload(
        asset: str, amount: Decimal, destination_address: str
    ) -> HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
        """
        Builds the Pydantic model for a withdrawal to L1 request.
        Returns a specific model based on whether the asset is ETH or another token.
        """
        if not destination_address:
            raise ValueError("Destination address is required for withdrawal.")

        if asset.upper() == "ETH":
            eth_withdrawal_model = HyperliquidRawEthWithdrawalActionPayload(
                amount=str(amount), destination=destination_address
            )
            return HyperliquidApiEthWithdrawalRequest(
                type="withdrawEth", action=eth_withdrawal_model
            )
        else:
            withdrawal_payload_model = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(), amount=str(amount), destination=destination_address
            )
            return HyperliquidApiTokenWithdrawalRequest(
                type="withdraw", action=withdrawal_payload_model
            )

    @staticmethod
    def build_order_history_payload(
        wallet_address: str, start_time_ms: int, end_time_ms: int
    ) -> HyperliquidRawQueryOrderHistoryRequestPayload:
        """
        Builds the Pydantic model for querying order history.
        """
        return HyperliquidRawQueryOrderHistoryRequestPayload(
            type="queryOrderHistory",
            user=wallet_address,
            startTime=start_time_ms,
            endTime=end_time_ms,
        )

    @staticmethod
    def build_candle_snapshot_payload(
        symbol: str, timeframe: str, start_time_ms: int, end_time_ms: int
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """
        Builds the Pydantic model for fetching candle snapshots.
        """
        req_details = HyperliquidRawCandleRequestDetails(
            coin=symbol.upper(),
            interval=timeframe,
            startTime=start_time_ms,
            endTime=end_time_ms,
        )
        return HyperliquidRawCandleSnapshotRequestPayload(type="candleSnapshot", req=req_details)

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
    ) -> HyperliquidApiPlaceOrderRequest:
        """
        Builds the Pydantic model for placing an order.
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
            cloid=client_order_id if client_order_id else None,
            trigger=hl_trigger_details,
        )
        return HyperliquidApiPlaceOrderRequest(type="order", actions=[place_order_action])

    @staticmethod
    def build_cancel_order_payload(
        asset_index: int, order_id: int
    ) -> HyperliquidApiCancelOrderRequest:
        """
        Builds the Pydantic model for cancelling an order.
        """
        action_model = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id)
        return HyperliquidApiCancelOrderRequest(type="cancel", action=action_model)

    @staticmethod
    def build_order_status_payload(
        wallet_address: str, order_id: int
    ) -> HyperliquidRawOrderStatusRequestPayload:
        """
        Builds the payload for querying the status of a specific order.
        """
        return HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus", user=wallet_address, oid=order_id
        )

    @staticmethod
    def build_user_state_payload(
        wallet_address: str,
    ) -> HyperliquidRawUserStateRequestPayload:
        """
        Builds the Pydantic model for fetching user state information.
        """
        if not wallet_address:
            # The RawLaxEthereumAddressStrHL in the model will handle more specific validation
            raise ValueError("Wallet address cannot be empty for user_state request.")
        # Explicitly provide 'type' to satisfy Pydantic, even if model has a default Field value.
        return HyperliquidRawUserStateRequestPayload(type="clearinghouseState", user=wallet_address)

    @staticmethod
    def build_user_fills_request_payload(
        wallet_address: str,
    ) -> HyperliquidRawUserFillsRequestPayload:
        """
        Builds the Pydantic model for fetching user fills information.
        """
        return HyperliquidRawUserFillsRequestPayload(type="userFills", user=wallet_address)

    # No changes needed for comments about /info endpoints and build_info_request_payload
    # as those are already handled or determined to not need specific Pydantic models for the
    # request body.
