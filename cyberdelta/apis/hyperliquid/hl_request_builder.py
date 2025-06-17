"""CyberDeltaEngine: Hyperliquid API Request Builder.

This module provides the HyperliquidRequestBuilder class for constructing
request payloads and parameters for all Hyperliquid API endpoints.
"""

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
    HyperliquidApiUpdateLeverageRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
    HyperliquidRawOrderItemSpec,
    HyperliquidRawUpdateLeverageAction,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawQueryOrderHistoryRequestPayload,
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
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetCandleSnapshotArgs,
    GetHistoricalFundingRatesArgs,
    GetL2BookArgs,
    GetOpenOrdersArgs,
    GetOrderHistoryArgsHL,
    GetOrderStatusArgs,
    GetRecentTradesArgs,
    GetUserFillsArgs,
    GetUserStateArgs,
    PlaceOrderArgs,
    TransferL2UsdArgs,
    UpdateLeverageArgs,
    WithdrawL1Args,
)
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce


class HyperliquidRequestBuilder:
    """Builds request payloads for Hyperliquid API endpoints.

    This class centralizes the logic for constructing Pydantic models
    representing requests for Hyperliquid API calls, ensuring consistency and
    separating request formatting from API call execution.

    Architecture Compliance: Follows proper Request Builder Pattern:
    - Takes internal Args models as input (PlaceOrderArgs, etc.)
    - Returns validated Raw API models (HyperliquidApiPlaceOrderRequest, etc.)
    - Handles INTERNAL → RAW transformation directly
    - Uses Pydantic validation at all boundaries
    """


    @staticmethod
    def _decimal_to_wire_format(value: Decimal | None) -> str:
        """Convert decimal to Hyperliquid wire format with comprehensive validation.

        Implements the exact SDK's float_to_wire function behavior with enhanced
        safety checks and Pydantic-compliant error handling.
        
        Args:
            value: Decimal value to convert (None returns "0")
            
        Returns:
            String representation in Hyperliquid wire format
            
        Raises:
            ValueError: If conversion causes precision loss or value is invalid
        """
        if value is None:
            return "0"

        # Type validation at boundary
        if not isinstance(value, Decimal):
            raise TypeError(f"Expected Decimal, got {type(value).__name__}: {value}")

        # Validate input is finite
        if not value.is_finite():
            raise ValueError(f"Value must be finite, got {value}")
        
        # Check for extreme values that could cause issues
        if abs(value) > Decimal("1e18"):
            raise ValueError(f"Value too large for wire format: {value}")
        
        # Check for too small values that would round to zero
        if value != 0 and abs(value) < Decimal("1e-8"):
            raise ValueError(f"Value too small for wire format precision: {value}")

        # Convert to float for rounding (maintaining SDK compatibility)
        try:
            x = float(value)
        except (ValueError, OverflowError) as e:
            raise ValueError(f"Cannot convert {value} to float: {e}") from e
            
        # Format with 8 decimal places
        rounded = f"{x:.8f}"

        # Check for rounding errors with tighter tolerance
        precision_loss = abs(float(rounded) - x)
        if precision_loss >= 1e-12:
            raise ValueError(
                f"Wire format conversion causes precision loss for {value}. "
                f"Loss: {precision_loss:.2e}"
            )

        # Handle negative zero
        if rounded == "-0.00000000":
            rounded = "0.00000000"

        # Normalize to remove trailing zeros
        try:
            normalized = Decimal(rounded).normalize()
            # Ensure we don't return scientific notation
            result = f"{normalized:f}"
            
            # Final validation - ensure result is parseable
            _ = Decimal(result)
            
            return result
        except Exception as e:
            raise ValueError(f"Failed to normalize wire format for {value}: {e}") from e

    @staticmethod
    def build_info_request_payload() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Build the Pydantic model for fetching meta and asset contexts via /info.

        Architecture Compliance: Pure factory method returning validated Pydantic model.
        No args needed as this is a static request type.

        Returns:
            HyperliquidRawMetaAndAssetCtxsRequestPayload: Validated request payload
            
        Payload: {"type": "metaAndAssetCtxs"}
        """
        return HyperliquidRawMetaAndAssetCtxsRequestPayload(type="metaAndAssetCtxs")

    @staticmethod
    def build_l2_book_request_payload(args: GetL2BookArgs) -> HyperliquidRawL2BookRequestPayload:
        """Build the Pydantic model for fetching L2 order book data.

        Pure factory method returning validated Pydantic model.

        Args:
            args: Validated GetL2BookArgs containing symbol

        Returns:
            HyperliquidRawL2BookRequestPayload: Validated Raw API model
        """
        return HyperliquidRawL2BookRequestPayload(type="l2Book", coin=args.symbol)

    @staticmethod
    def build_recent_trades_request_payload(
        args: GetRecentTradesArgs,
    ) -> HyperliquidRawRecentTradesRequestPayload:
        """Build the Pydantic model for fetching recent public trades.

        Pure factory method returning validated Pydantic model.

        Args:
            args: Validated GetRecentTradesArgs containing symbol

        Returns:
            HyperliquidRawRecentTradesRequestPayload: Validated Raw API model
        """
        return HyperliquidRawRecentTradesRequestPayload(
            type="recentTrades", coin=args.symbol
        )

    @staticmethod
    def build_l2_usd_transfer_payload(
        args: TransferL2UsdArgs,
    ) -> HyperliquidApiL2UsdTransferRequest:
        """Build the Pydantic model for an L2 USD transfer request.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated TransferL2UsdArgs containing transfer parameters

        Returns:
            HyperliquidApiL2UsdTransferRequest: Validated Raw API model

        Assumes all business validation has been done by the service layer.
        """
        # Convert amount to wire format for precision
        amount_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.amount)
        
        transfer_payload_model = HyperliquidRawL2UsdTransferPayload(
            destination=args.destination_address,
            token="USDC",  # noqa: S106
            amount=amount_wire,
        )
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=transfer_payload_model,
        )
        return HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    @staticmethod
    def build_withdrawal_payload(
        args: WithdrawL1Args,
    ) -> HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
        """Build the Pydantic model for a withdrawal to L1 request.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated WithdrawL1Args containing withdrawal parameters

        Returns:
            HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest: Validated Raw API model

        Returns a specific model based on whether the asset is ETH or another token.
        Assumes all business validation has been done by the service layer.
        """
        # Convert amount to wire format for precision
        amount_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.amount)
        
        if args.asset.upper() == "ETH":
            eth_withdrawal_model = HyperliquidRawEthWithdrawalActionPayload(
                amount=amount_wire,
                destination=args.destination_address,
            )
            return HyperliquidApiEthWithdrawalRequest(
                type="withdrawEth",
                action=eth_withdrawal_model,
            )
        else:
            withdrawal_payload_model = HyperliquidRawWithdrawalToL1ActionPayload(
                token=args.asset,
                amount=amount_wire,
                destination=args.destination_address,
            )
            return HyperliquidApiTokenWithdrawalRequest(
                type="withdraw",
                action=withdrawal_payload_model,
            )

    @staticmethod
    def build_order_history_payload(
        args: GetOrderHistoryArgsHL,
    ) -> HyperliquidRawQueryOrderHistoryRequestPayload:
        """Build the Pydantic model for querying order history.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetOrderHistoryArgsHL containing history query parameters

        Returns:
            HyperliquidRawQueryOrderHistoryRequestPayload: Validated Raw API model
        """
        return HyperliquidRawQueryOrderHistoryRequestPayload(
            type="queryOrderHistory",
            user=args.wallet_address,
            startTime=args.start_time_ms,
            endTime=args.end_time_ms,
        )

    @staticmethod
    def build_candle_snapshot_payload(
        args: GetCandleSnapshotArgs,
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Build the Pydantic model for fetching candle snapshots.

        Pure factory method returning validated Pydantic model.

        Args:
            args: Validated GetCandleSnapshotArgs containing candle query parameters

        Returns:
            HyperliquidRawCandleSnapshotRequestPayload: Validated Raw API model
        """
        req_details = HyperliquidRawCandleRequestDetails(
            coin=args.symbol,
            interval=args.timeframe,
            startTime=args.start_time_ms,
            endTime=args.end_time_ms,
        )
        return HyperliquidRawCandleSnapshotRequestPayload(type="candleSnapshot", req=req_details)

    @staticmethod
    def build_place_order_payload(
        args: PlaceOrderArgs,
        asset_index: int,
        tif_str: str | None = None,  # Pass mapped TIF from service
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build the Pydantic model for placing orders.

        Pure factory method creating the exact wire format structure expected by Hyperliquid API.

        Args:
            args: Validated PlaceOrderArgs containing order parameters
            asset_index: Hyperliquid-specific asset index for the symbol

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated Raw API model
        """
        is_buy = args.side == OrderSide.BUY

        # Convert prices to wire format - handle None for market orders
        price_for_wire = args.price if args.price is not None else Decimal("0")
        limit_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(price_for_wire)

        # Convert quantity to wire format
        sz_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.quantity)

        # Construct order type model
        if args.order_type == OrderType.LIMIT:
            # Use provided tif_str or default to "Gtc"
            order_type_model = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif=tif_str or "Gtc")
            )
        elif args.order_type == OrderType.MARKET:
            order_type_model = HyperliquidRawOrderType(
                market=HyperliquidRawMarketOrderTypeDetails()
            )
        elif args.order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT):
            # For stop orders, create trigger order type
            if args.stop_price is not None:
                trigger_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.stop_price)
                is_market = args.order_type == OrderType.STOP_MARKET
                order_type_model = HyperliquidRawOrderType(
                    trigger=HyperliquidRawTriggerInfo(
                        triggerPx=trigger_px_wire, isMarket=is_market, tpsl="sl"
                    )
                )
            else:
                # Fallback to limit order if no stop price
                order_type_model = HyperliquidRawOrderType(
                    limit=HyperliquidRawLimitOrderTypeDetails(tif=tif_str or "Gtc")
                )

        # Create validated Pydantic model - INTERNAL → RAW transformation
        # Architecture Compliance: All fields validated by Pydantic at boundary
        # Build the raw order specification with full validation
        wire_order = HyperliquidRawOrderItemSpec(
            a=asset_index,
            b=is_buy,
            p=limit_px_wire,  # Wire format string validated by RawFiniteDecimalStr
            s=sz_wire,  # Wire format string validated by RawFiniteDecimalStr
            r=args.reduce_only,
            t=order_type_model,  # Pydantic model with proper validation
            c=args.client_order_id,
        )

        # Return the final request payload with Pydantic validation
        return HyperliquidApiPlaceOrderRequest(
            type="order", 
            orders=[wire_order], 
            grouping="na"  # Default grouping per Hyperliquid API
        )

    @staticmethod
    def build_cancel_order_payload(
        args: CancelOrderArgs,
        asset_index: int,
        order_id: int,
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the Pydantic model for cancelling an order.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated CancelOrderArgs containing cancellation parameters
            asset_index: Hyperliquid-specific asset index for the symbol
            order_id: Numeric order ID to cancel

        Returns:
            HyperliquidApiCancelOrderRequest: Validated Raw API model
        """
        action_model = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id)
        return HyperliquidApiCancelOrderRequest(type="cancel", action=action_model)

    @staticmethod
    def build_order_status_payload(
        args: GetOrderStatusArgs,
    ) -> HyperliquidRawOrderStatusRequestPayload:
        """Build the payload for querying the status of a specific order.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetOrderStatusArgs containing wallet address and order ID

        Returns:
            HyperliquidRawOrderStatusRequestPayload: Validated Raw API model
        """
        return HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus",
            user=args.wallet_address,
            oid=args.order_id,
        )

    @staticmethod
    def build_user_state_payload(
        args: GetUserStateArgs,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Build the Pydantic model for fetching user state information.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetUserStateArgs containing wallet address

        Returns:
            HyperliquidRawUserStateRequestPayload: Validated Raw API model

        Assumes all business validation has been done by the service layer.
        """
        # The RawLaxEthereumAddressStrHL in the model will handle format validation
        # Explicitly provide 'type' to satisfy Pydantic, even if model has a default Field value.
        return HyperliquidRawUserStateRequestPayload(
            type="clearinghouseState", user=args.wallet_address
        )

    @staticmethod
    def build_user_fills_request_payload(
        args: GetUserFillsArgs,
    ) -> HyperliquidRawUserFillsRequestPayload:
        """Build the Pydantic model for fetching user fills (trade history).

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetUserFillsArgs containing wallet address

        Returns:
            HyperliquidRawUserFillsRequestPayload: Validated Raw API model

        Payload: {"type": "userFills", "user": "WALLET_ADDRESS"}
        """
        return HyperliquidRawUserFillsRequestPayload(type="userFills", user=args.wallet_address)

    @staticmethod
    def build_open_orders_payload(
        args: GetOpenOrdersArgs,
    ) -> HyperliquidRawOpenOrdersRequestPayload:
        """Build the Pydantic model for fetching open orders.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetOpenOrdersArgs containing wallet address

        Returns:
            HyperliquidRawOpenOrdersRequestPayload: Validated Raw API model

        Payload: {"type": "openOrders", "user": "WALLET_ADDRESS"}
        """
        return HyperliquidRawOpenOrdersRequestPayload(type="openOrders", user=args.wallet_address)

    @staticmethod
    def build_historical_funding_rates_payload(
        args: GetHistoricalFundingRatesArgs,
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Build the Pydantic model for fetching historical funding rates for a specific coin.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated GetHistoricalFundingRatesArgs containing funding rate query parameters

        Returns:
            HyperliquidRawFundingHistoryRequestPayload: The validated request payload model.

        """
        # Convert datetime to milliseconds
        start_time_ms = int(args.start_time.timestamp() * 1000) if args.start_time else 0
        end_time_ms = int(args.end_time.timestamp() * 1000) if args.end_time else None

        # Create request with Pydantic validation at boundary
        return HyperliquidRawFundingHistoryRequestPayload(
            coin=RawHlCoinName(args.symbol),
            startTime=start_time_ms,
            endTime=end_time_ms,
        )

    # No changes needed for comments about /info endpoints and build_info_request_payload
    # as those are already handled or determined to not need specific Pydantic models for the
    # request body.

    @staticmethod
    def build_update_leverage_request(
        args: UpdateLeverageArgs,
    ) -> HyperliquidApiUpdateLeverageRequest:
        """Build the request payload for updating leverage on a specific asset.

        Following proper Request Builder Pattern: Takes internal Args model → Returns Raw Pydantic models.

        Args:
            args: Validated UpdateLeverageArgs containing leverage parameters

        Returns:
            HyperliquidApiUpdateLeverageRequest: The validated request payload model.

        """
        return HyperliquidApiUpdateLeverageRequest(
            type="updateLeverage",
            action=HyperliquidRawUpdateLeverageAction(
                asset=args.asset_index,
                isCross=args.is_cross,
                leverage=args.leverage,
            ),
        )
