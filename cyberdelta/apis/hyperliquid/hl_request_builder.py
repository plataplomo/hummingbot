from __future__ import annotations

from decimal import Decimal

from cyberdelta.apis.hyperliquid.models.common_raw_types import RawHlCoinName

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
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
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
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawUserStateRequestPayload,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce


class HyperliquidRequestBuilder:
    """Builds request payloads for Hyperliquid API endpoints.

    This class centralizes the logic for constructing Pydantic models
    representing requests for Hyperliquid API calls, ensuring consistency and
    separating request formatting from API call execution.
    """

    @staticmethod
    def _map_time_in_force_to_hyperliquid(tif: TimeInForce) -> str:
        """Maps internal TimeInForce enum values to Hyperliquid-specific format.

        Our internal enum uses all uppercase (GTC, IOC, ALO, FOK),
        but Hyperliquid expects specific capitalization (Gtc, Ioc, Alo).

        Args:
            tif: Internal TimeInForce enum value

        Returns:
            Hyperliquid-formatted time-in-force string

        Raises:
            ValueError: If the time-in-force value is not supported by Hyperliquid

        """
        mapping = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",
            # FOK is not supported by Hyperliquid according to the RAW model validation
        }

        if tif not in mapping:
            raise ValueError(
                f"TimeInForce {tif.value} is not supported by Hyperliquid. "
                f"Supported values: {list(mapping.keys())}",
            )

        return mapping[tif]

    @staticmethod
    def build_info_request_payload() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Builds the Pydantic model for fetching meta and asset contexts via /info.
        Payload: {"type": "metaAndAssetCtxs"}
        """
        return HyperliquidRawMetaAndAssetCtxsRequestPayload(type="metaAndAssetCtxs")

    @staticmethod
    def build_l2_book_request_payload(symbol: str) -> HyperliquidRawL2BookRequestPayload:
        """Builds the Pydantic model for fetching L2 order book data.
        Payload: {"type": "l2Book", "coin": "SYMBOL"}
        """
        return HyperliquidRawL2BookRequestPayload(type="l2Book", coin=symbol.upper())

    @staticmethod
    def build_recent_trades_request_payload(
        symbol: str,
    ) -> HyperliquidRawRecentTradesRequestPayload:
        """Builds the Pydantic model for fetching recent public trades.
        Payload: {"type": "recentTrades", "coin": "SYMBOL"}
        """
        return HyperliquidRawRecentTradesRequestPayload(type="recentTrades", coin=symbol.upper())

    @staticmethod
    def build_l2_usd_transfer_payload(
        destination_address: str, amount: Decimal,
    ) -> HyperliquidApiL2UsdTransferRequest:
        """Builds the Pydantic model for an L2 USD transfer request.

        Assumes all business validation has been done by the service layer.
        """
        transfer_payload_model = HyperliquidRawL2UsdTransferPayload(
            destination=destination_address, token="USDC", amount=str(amount),
        )
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2", payload=transfer_payload_model,
        )
        return HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    @staticmethod
    def build_withdrawal_payload(
        asset: str, amount: Decimal, destination_address: str,
    ) -> HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
        """Builds the Pydantic model for a withdrawal to L1 request.
        Returns a specific model based on whether the asset is ETH or another token.

        Assumes all business validation has been done by the service layer.
        """
        if asset.upper() == "ETH":
            eth_withdrawal_model = HyperliquidRawEthWithdrawalActionPayload(
                amount=str(amount), destination=destination_address,
            )
            return HyperliquidApiEthWithdrawalRequest(
                type="withdrawEth", action=eth_withdrawal_model,
            )
        else:
            withdrawal_payload_model = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(), amount=str(amount), destination=destination_address,
            )
            return HyperliquidApiTokenWithdrawalRequest(
                type="withdraw", action=withdrawal_payload_model,
            )

    @staticmethod
    def build_order_history_payload(
        wallet_address: str, start_time_ms: int, end_time_ms: int,
    ) -> HyperliquidRawQueryOrderHistoryRequestPayload:
        """Builds the Pydantic model for querying order history.
        """
        return HyperliquidRawQueryOrderHistoryRequestPayload(
            type="queryOrderHistory",
            user=wallet_address,
            startTime=start_time_ms,
            endTime=end_time_ms,
        )

    @staticmethod
    def build_candle_snapshot_payload(
        symbol: str, timeframe: str, start_time_ms: int, end_time_ms: int,
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Builds the Pydantic model for fetching candle snapshots.
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
        """Builds the Pydantic model for placing orders.
        Returns HyperliquidApiPlaceOrderRequest with full field structure and trigger support.

        Assumes all business validation has been done by the service layer.
        """
        is_buy = side == OrderSide.BUY
        sz_str = str(quantity)

        # Set limit price appropriately for order types
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            # Service layer ensures price is not None for these order types
            limit_px_str = str(price) if price is not None else "0"
        elif order_type in (OrderType.MARKET, OrderType.STOP_MARKET):
            limit_px_str = "0"
        else:
            # This is more of a mapping issue than business logic
            raise ValueError(f"Order type {order_type.value} is not supported by Hyperliquid")

        # Handle post_only mapping to ALO time-in-force
        if post_only and time_in_force != TimeInForce.ALO:
            time_in_force = TimeInForce.ALO

        # Construct order type details
        # Note: Both STOP_MARKET and STOP_LIMIT orders use limit order type details
        # The trigger.is_market field determines whether it executes as market or limit
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.STOP_MARKET):
            hl_tif_details = HyperliquidRawLimitOrderTypeDetails(
                tif=HyperliquidRequestBuilder._map_time_in_force_to_hyperliquid(time_in_force),
            )
            # Ensure only 'limit' is set, not 'market'
            hl_order_type = HyperliquidRawOrderType(limit=hl_tif_details, market=None)
        elif order_type == OrderType.MARKET:
            hl_market_details = HyperliquidRawMarketOrderTypeDetails()
            # Ensure only 'market' is set, not 'limit'
            hl_order_type = HyperliquidRawOrderType(limit=None, market=hl_market_details)

        # Handle trigger logic for stop orders
        trigger_details = None
        if order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT):
            # Service layer ensures stop_price is not None for stop orders
            if stop_price is not None:
                is_market = order_type == OrderType.STOP_MARKET
                trigger_details = HyperliquidRawTriggerDetails(
                    triggerPx=str(stop_price),
                    isMarket=is_market,
                    tpsl="sl",  # Stop-loss trigger type
                )

        # Create the order action using full field names
        order_action = HyperliquidRawPlaceOrderAction(
            asset=asset_index,
            isBuy=is_buy,
            limitPx=limit_px_str,
            sz=sz_str,
            reduceOnly=reduce_only,
            orderType=hl_order_type,
            trigger=trigger_details,
            cloid=client_order_id,
        )

        return HyperliquidApiPlaceOrderRequest(type="order", actions=[order_action])

    @staticmethod
    def build_cancel_order_payload(
        asset_index: int, order_id: int,
    ) -> HyperliquidApiCancelOrderRequest:
        """Builds the Pydantic model for cancelling an order.
        """
        action_model = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id)
        return HyperliquidApiCancelOrderRequest(type="cancel", action=action_model)

    @staticmethod
    def build_order_status_payload(
        wallet_address: str, order_id: int,
    ) -> HyperliquidRawOrderStatusRequestPayload:
        """Builds the payload for querying the status of a specific order.
        """
        return HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus", user=wallet_address, oid=order_id,
        )

    @staticmethod
    def build_user_state_payload(
        wallet_address: str,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Builds the Pydantic model for fetching user state information.

        Assumes all business validation has been done by the service layer.
        """
        # The RawLaxEthereumAddressStrHL in the model will handle format validation
        # Explicitly provide 'type' to satisfy Pydantic, even if model has a default Field value.
        return HyperliquidRawUserStateRequestPayload(type="clearinghouseState", user=wallet_address)

    @staticmethod
    def build_user_fills_request_payload(
        wallet_address: str,
    ) -> HyperliquidRawUserFillsRequestPayload:
        """Builds the Pydantic model for fetching user fills (trade history).
        Payload: {"type": "userFills", "user": "WALLET_ADDRESS"}
        """
        return HyperliquidRawUserFillsRequestPayload(type="userFills", user=wallet_address)

    @staticmethod
    def build_open_orders_payload(
        wallet_address: str,
    ) -> HyperliquidRawOpenOrdersRequestPayload:
        """Builds the Pydantic model for fetching open orders.
        Payload: {"type": "openOrders", "user": "WALLET_ADDRESS"}
        """
        return HyperliquidRawOpenOrdersRequestPayload(type="openOrders", user=wallet_address)

    @staticmethod
    def build_historical_funding_rates_payload(
        symbol: str,
        start_time_ms: int,
        end_time_ms: int | None,
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Builds the Pydantic model for fetching historical funding rates for a specific coin.

        Args:
            symbol: The coin symbol (e.g., "ETH").
            start_time_ms: The start time for the query in milliseconds (inclusive).
            end_time_ms: The end time for the query in milliseconds (inclusive).
                         If None, API defaults to current time.

        Returns:
            HyperliquidRawFundingHistoryRequestPayload: The validated request payload model.

        """
        return HyperliquidRawFundingHistoryRequestPayload(
            coin=RawHlCoinName(symbol),
            startTime=start_time_ms,
            endTime=end_time_ms,
        )

    # No changes needed for comments about /info endpoints and build_info_request_payload
    # as those are already handled or determined to not need specific Pydantic models for the
    # request body.
