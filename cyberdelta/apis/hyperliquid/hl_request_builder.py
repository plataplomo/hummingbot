"""CyberDeltaEngine: Hyperliquid API Request Builder.

This module provides the HyperliquidRequestBuilder class for constructing
request payloads and parameters for all Hyperliquid API endpoints.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import RawHlCoinName
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import (
    HyperliquidRawAllMidsRequestPayload,
)
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
    HyperliquidRawCancelItem,
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
    HyperliquidRawHistoricalOrdersRequestPayload,
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawOrderType,
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
    GetRecentTradesArgs,
    GetUserFillsArgs,
    GetUserStateArgs,
    HyperliquidGetOrderStatusArgs,
    PlaceOrderArgs,
    TransferL2UsdArgs,
    UpdateLeverageArgs,
    WithdrawL1Args,
)
from cyberdelta.core.models import OrderSide, OrderType


# Precision and batch size constants
PRECISION_TOLERANCE = 1e-12  # Tolerance for floating point precision checks
MAX_BATCH_SIZE = 50  # Maximum number of orders/cancellations per batch request


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
    def _validate_decimal_input(value: Decimal) -> None:
        """Validate decimal input for wire format conversion."""
        if not value.is_finite():
            raise ValueError(f"Value must be finite, got {value}")

        if abs(value) > Decimal("1e18"):
            raise ValueError(f"Value too large for wire format: {value}")

        if value != 0 and abs(value) < Decimal("1e-8"):
            raise ValueError(f"Value too small for wire format precision: {value}")

    @staticmethod
    def _format_and_validate_precision(value: Decimal) -> str:
        """Format decimal with precision validation."""
        try:
            x = float(value)
        except (ValueError, OverflowError) as e:
            raise ValueError(f"Cannot convert {value} to float: {e}") from e

        rounded = f"{x:.8f}"

        # Check for rounding errors
        precision_loss = abs(float(rounded) - x)
        if precision_loss >= PRECISION_TOLERANCE:
            raise ValueError(
                f"Wire format conversion causes precision loss for {value}. "
                f"Loss: {precision_loss:.2e}",
            )

        return rounded

    @staticmethod
    def _normalize_wire_format(rounded: str, original_value: Decimal) -> str:
        """Normalize wire format string with proper decimal handling."""
        # Handle negative zero
        if rounded == "-0.00000000":
            rounded = "0.00000000"

        try:
            normalized = Decimal(rounded).normalize()
            result = f"{normalized:f}"

            # Ensure at least one decimal place for Hyperliquid API compatibility
            if "." not in result:
                result += ".0"

            # Final validation - ensure result is parseable
            _ = Decimal(result)
        except Exception as e:
            raise ValueError(f"Failed to normalize wire format for {original_value}: {e}") from e
        else:
            return result

    @staticmethod
    def _decimal_to_wire_format(value: Decimal | None) -> str:
        """Convert a Decimal to the string wire format with validation.

        This is the authoritative conversion method that ensures all decimal values
        sent to Hyperliquid API are properly formatted. The wire format must be
        decimal-represented for financial precision.

        Args:
            value: Decimal value to convert to wire format, or None for market orders

        Returns:
            str: String representation ready for API transmission

        Raises:
            ValueError: If the decimal value is not finite or if formatting fails
            TypeError: If the value is not a Decimal type
        """
        if value is None:
            return "0"

        HyperliquidRequestBuilder._validate_decimal_input(value)
        rounded = HyperliquidRequestBuilder._format_and_validate_precision(value)
        return HyperliquidRequestBuilder._normalize_wire_format(rounded, value)

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
    def build_all_mids_request_payload() -> HyperliquidRawAllMidsRequestPayload:
        """Build the Pydantic model for fetching all mid prices via /info.

        Architecture Compliance: Pure factory method returning validated Pydantic model.
        No args needed as this is a static request type.

        Returns:
            HyperliquidRawAllMidsRequestPayload: Validated request payload

        Payload: {"type": "allMids"}
        """
        return HyperliquidRawAllMidsRequestPayload(type="allMids")

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
        return HyperliquidRawRecentTradesRequestPayload(type="recentTrades", coin=args.symbol)

    @staticmethod
    def build_l2_usd_transfer_payload(
        args: TransferL2UsdArgs,
    ) -> HyperliquidApiL2UsdTransferRequest:
        """Build the Pydantic model for an L2 USD transfer request.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

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

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated WithdrawL1Args containing withdrawal parameters

        Returns:
            HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
                Validated Raw API model

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
    def build_historical_orders_payload(
        wallet_address: str,
    ) -> HyperliquidRawHistoricalOrdersRequestPayload:
        """Build the Pydantic model for fetching historical orders.

        Uses 'historicalOrders' endpoint which returns all historical orders.
        Time filtering must be done after fetching the results.

        Args:
            wallet_address: User's wallet address

        Returns:
            HyperliquidRawHistoricalOrdersRequestPayload: Validated Raw API model
        """
        return HyperliquidRawHistoricalOrdersRequestPayload(
            type="historicalOrders",
            user=wallet_address,
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
    def _build_order_item_spec_static(
        args: PlaceOrderArgs,
        asset_index: int,
        tif_str: str | None = None,
    ) -> HyperliquidRawOrderItemSpec:
        """Build a single order item specification for use in batch or single order requests.

        Static helper method for creating order specs with proper wire format conversion.
        This is the core logic shared between single and batch order placement.

        Args:
            args: Validated PlaceOrderArgs containing order parameters
            asset_index: Hyperliquid-specific asset index for the symbol
            tif_str: Optional time-in-force string override

        Returns:
            HyperliquidRawOrderItemSpec: Validated order specification for Hyperliquid API
        """
        is_buy = args.side == OrderSide.BUY

        # Convert prices to wire format - handle None for market orders
        if args.order_type == OrderType.MARKET:
            # Market orders require a price to be passed from the service layer
            # The service layer should calculate aggressive pricing based on current market data
            if args.price is None:
                raise ValueError(
                    "Market orders require a calculated aggressive price. "
                    "The service layer must provide the price based on current market data.",
                )
            limit_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.price)
        else:
            price_for_wire = args.price if args.price is not None else Decimal(0)
            limit_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(price_for_wire)

        # Convert quantity to wire format
        sz_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.quantity)

        # Construct order type model
        if args.order_type == OrderType.LIMIT:
            # Use provided tif_str or default to "Gtc"
            order_type_model = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif=tif_str or "Gtc"),
            )
        elif args.order_type == OrderType.MARKET:
            # Hyperliquid market orders are implemented as aggressive IoC limit orders
            # Based on official SDK: "Market Order is an aggressive Limit Order IoC"
            order_type_model = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Ioc"),
            )
        elif args.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
            # Construct trigger information for stop orders
            if args.stop_price is None:
                raise ValueError(f"Stop orders require stop_price, got None for {args.order_type}")

            trigger_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(args.stop_price)
            is_market = args.order_type == OrderType.STOP_MARKET

            trigger_info = HyperliquidRawTriggerInfo(
                triggerPx=trigger_px_wire,
                isMarket=is_market,
                tpsl="sl",  # All stop orders are stop-loss ("sl")
            )

            order_type_model = HyperliquidRawOrderType(trigger=trigger_info)
        else:
            raise ValueError(f"Unsupported order type: {args.order_type}")

        # Build and return the order specification
        return HyperliquidRawOrderItemSpec(
            asset_index=asset_index,
            is_buy=is_buy,
            limit_px=limit_px_wire,
            size=sz_wire,
            reduce_only=args.reduce_only,
            order_type_details=order_type_model,  # Pydantic model with proper validation
            client_order_id=args.client_order_id,
        )

    def _build_order_item_spec(
        self,
        args: PlaceOrderArgs,
        asset_index: int,
        tif_str: str | None = None,
    ) -> HyperliquidRawOrderItemSpec:
        """Build a single order item specification for use in batch or single order requests.

        Instance wrapper around static method for backward compatibility.
        This method delegates to the static implementation.

        Args:
            args: Validated PlaceOrderArgs containing order parameters
            asset_index: Hyperliquid-specific asset index for the symbol
            tif_str: Optional time-in-force string mapped from service layer

        Returns:
            HyperliquidRawOrderItemSpec: Validated Raw order specification
        """
        return HyperliquidRequestBuilder._build_order_item_spec_static(args, asset_index, tif_str)

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
            tif_str: Optional time-in-force string mapped from service layer

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated Raw API model
        """
        # Use the static helper method to build the order spec
        wire_order = HyperliquidRequestBuilder._build_order_item_spec_static(
            args,
            asset_index,
            tif_str,
        )

        # Return the final request payload with Pydantic validation
        return HyperliquidApiPlaceOrderRequest(
            type="order",
            orders=[wire_order],
            grouping="na",  # Default grouping per Hyperliquid API
        )

    @staticmethod
    def build_cancel_order_payload(
        args: CancelOrderArgs,
        asset_index: int,
        order_id: int,
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the Pydantic model for cancelling an order.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated CancelOrderArgs containing cancellation parameters
            asset_index: Hyperliquid-specific asset index for the symbol
            order_id: Numeric order ID to cancel

        Returns:
            HyperliquidApiCancelOrderRequest: Validated Raw API model
        """
        # Create cancel item with short field names as per official SDK

        cancel_item = HyperliquidRawCancelItem(a=asset_index, o=order_id)
        return HyperliquidApiCancelOrderRequest(type="cancel", cancels=[cancel_item])

    @staticmethod
    def build_order_status_payload(
        args: HyperliquidGetOrderStatusArgs,
    ) -> HyperliquidRawOrderStatusRequestPayload:
        """Build the payload for querying the status of a specific order.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidGetOrderStatusArgs containing wallet address and order ID

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

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated GetUserStateArgs containing wallet address

        Returns:
            HyperliquidRawUserStateRequestPayload: Validated Raw API model

        Assumes all business validation has been done by the service layer.
        """
        # The RawLaxEthereumAddressStrHL in the model will handle format validation
        # Explicitly provide 'type' to satisfy Pydantic, even if model has a default Field value.
        return HyperliquidRawUserStateRequestPayload(
            type="clearinghouseState",
            user=args.wallet_address,
        )

    @staticmethod
    def build_user_fills_request_payload(
        args: GetUserFillsArgs,
    ) -> HyperliquidRawUserFillsRequestPayload:
        """Build the Pydantic model for fetching user fills (trade history).

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

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

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

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

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

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

    def build_batch_place_order_payload(
        self,
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None] | None = None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build the Pydantic model for placing multiple orders in a single batch request.

        Enables massive performance improvements by batching multiple orders into one API call,
        reducing from N HTTP requests to 1, and from N EIP-712 signatures to 1.

        Args:
            orders_with_indices: List of (PlaceOrderArgs, asset_index) tuples for batch placement
            tif_mapping: Optional mapping of symbol->tif_str for custom time-in-force per order

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated Raw API model with multiple orders

        Raises:
            ValueError: If any order has invalid parameters or batch is empty
        """
        if not orders_with_indices:
            raise ValueError("Cannot create batch order payload with empty order list")

        if len(orders_with_indices) > MAX_BATCH_SIZE:  # Conservative batch size limit
            raise ValueError(
                f"Batch size {len(orders_with_indices)} exceeds maximum of {MAX_BATCH_SIZE} "
                "orders. Consider splitting into smaller batches."
            )

        # Build order specs for all orders in the batch
        order_specs: list[HyperliquidRawOrderItemSpec] = []
        for args, asset_index in orders_with_indices:
            # Get TIF for this specific order if provided
            tif_str = None
            if tif_mapping:
                tif_str = tif_mapping.get(args.symbol)

            # Use the extracted helper method to build each order spec
            order_spec = self._build_order_item_spec(args, asset_index, tif_str)
            order_specs.append(order_spec)

        # Return the final batch request payload with Pydantic validation
        return HyperliquidApiPlaceOrderRequest(
            type="order",
            orders=order_specs,  # Multiple orders in one request!
            grouping="na",  # Default grouping per Hyperliquid API
        )

    def build_batch_cancel_order_payload(
        self,
        cancel_items: list[tuple[int, int]],  # (asset_index, order_id) pairs
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the Pydantic model for cancelling multiple orders in a single batch request.

        Enables performance improvements by batching multiple cancellations into one API call.

        Args:
            cancel_items: List of (asset_index, order_id) tuples for batch cancellation

        Returns:
            HyperliquidApiCancelOrderRequest: Validated Raw API model with multiple cancels

        Raises:
            ValueError: If cancel list is empty or exceeds batch limits
        """
        if not cancel_items:
            raise ValueError("Cannot create batch cancel payload with empty cancel list")

        if len(cancel_items) > MAX_BATCH_SIZE:  # Conservative batch size limit
            raise ValueError(
                f"Batch size {len(cancel_items)} exceeds maximum of {MAX_BATCH_SIZE} "
                "cancellations. Consider splitting into smaller batches."
            )

        # Build cancel item specs for all cancellations in the batch
        cancel_specs: list[HyperliquidRawCancelItem] = []
        for asset_index, order_id in cancel_items:
            cancel_spec = HyperliquidRawCancelItem(a=asset_index, o=order_id)
            cancel_specs.append(cancel_spec)

        # Return the final batch cancel request payload with Pydantic validation
        return HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=cancel_specs,  # Multiple cancels in one request!
        )

    # No changes needed for comments about /info endpoints and build_info_request_payload
    # as those are already handled or determined to not need specific Pydantic models for the
    # request body.

    @staticmethod
    def build_update_leverage_request(
        args: UpdateLeverageArgs,
    ) -> HyperliquidApiUpdateLeverageRequest:
        """Build the request payload for updating leverage on a specific asset.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

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
