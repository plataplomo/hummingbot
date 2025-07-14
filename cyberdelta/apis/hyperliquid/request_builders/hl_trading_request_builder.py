"""Hyperliquid Trading Request Builder.

This module handles the construction of request payloads for trading operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Order placement requests (single and batch)
- Order cancellation requests (single and batch)
- Order status and query requests
- Open orders and order history requests
- Trading-related validation and formatting
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from eth_typing import ChecksumAddress

from cyberdelta.apis.exceptions import (
    InvalidEnumValueError,
    MissingRequiredParameterError,
)
from cyberdelta.apis.exceptions.request_validation import DecimalRangeError
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelItem,
    HyperliquidRawOrderItemSpec,
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
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsRequestPayload,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.request_builders.hl_request_builder_base import (
    HyperliquidRequestBuilderBase,
)
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOpenOrdersArgs,
    GetOrderHistoryArgsHL,
    GetUserFillsArgs,
    HyperliquidGetOrderStatusArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Constants
PRECISION_TOLERANCE = 1e-12  # Tolerance for floating point precision checks
MAX_BATCH_SIZE = 100  # Conservative batch size limit


class HyperliquidTradingRequestBuilder(
    HyperliquidRequestBuilderBase, TradingRequestBuilderProtocol
):
    """Focused request builder for Hyperliquid trading operations.

    This class contains methods for constructing validated request payloads
    for all trading related API endpoints. Implements TradingRequestBuilderProtocol
    for type safety and consistency.
    """

    def __init__(self) -> None:
        """Initialize the trading request builder."""

    # Base protocol method implementation
    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build request payload per base protocol.

        Args:
            *args: Positional arguments for request building
            **kwargs: Keyword arguments for request building

        Returns:
            Dictionary containing the request payload
        """
        # This is a generic method required by the protocol
        # In practice, specific builder methods are used
        request_type = kwargs.get("request_type", "")

        if request_type == "place_order":
            order_args = kwargs.get("order_args")
            asset_index = kwargs.get("asset_index", 0)
            if (
                order_args
                and hasattr(order_args, "model_dump")
                and isinstance(order_args, PlaceOrderArgs)
                and isinstance(asset_index, (int, str))
            ):
                return self.build_place_order_payload_with_args(
                    order_args, int(asset_index)
                ).model_dump(mode="json", by_alias=True)
        elif request_type == "cancel_order":
            cancel_args = kwargs.get("cancel_args")
            asset_index = kwargs.get("asset_index", 0)
            if (
                cancel_args
                and hasattr(cancel_args, "model_dump")
                and isinstance(cancel_args, CancelOrderArgs)
                and isinstance(asset_index, (int, str))
            ):
                return self.build_cancel_order_payload_with_args(
                    cancel_args, int(asset_index), 0
                ).model_dump(mode="json", by_alias=True)

        # Default to empty request
        return {}

    def _build_order_item_spec(
        self,
        args: PlaceOrderArgs,
        asset_index: int,
        tif_str: str | None = None,
    ) -> HyperliquidRawOrderItemSpec:
        """Build a single order item specification for use in batch or single order requests.

        Static helper method that can be used by both instance and static methods.

        Args:
            args: Validated PlaceOrderArgs containing order parameters
            asset_index: Hyperliquid-specific asset index for the symbol
            tif_str: Optional time-in-force string mapped from service layer

        Returns:
            HyperliquidRawOrderItemSpec: Validated Raw order specification
        """
        logger.debug(
            "building_order_item_spec",
            symbol=args.symbol,
            order_type=args.order_type.value,
            side=args.side.value,
            quantity=str(args.quantity),
            price=str(args.price) if args.price else None,
            stop_price=str(args.stop_price) if args.stop_price else None,
            asset_index=asset_index,
            tif=tif_str,
            message="Building order item specification",
        )

        # Convert order side to wire format
        is_buy = args.side == OrderSide.BUY

        # Convert price to wire format based on order type
        if args.order_type == OrderType.MARKET:
            # Market orders in Hyperliquid are implemented as aggressive IoC limit orders
            # If no price is provided, the order placement service will calculate
            # an aggressive price
            # using the thin market order implementation
            if args.price is None:
                raise MissingRequiredParameterError(
                    parameter_name="price",
                    operation="market order placement",
                )
            limit_px_wire = self._decimal_to_wire_format(args.price)
        else:
            price_for_wire = args.price if args.price is not None else Decimal(0)
            limit_px_wire = self._decimal_to_wire_format(price_for_wire)

        # Convert quantity to wire format
        sz_wire = self._decimal_to_wire_format(args.quantity)

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
                raise MissingRequiredParameterError(
                    parameter_name="stop_price",
                    operation=f"{args.order_type.value} order placement",
                )

            trigger_px_wire = self._decimal_to_wire_format(args.stop_price)
            is_market = args.order_type == OrderType.STOP_MARKET

            trigger_info = HyperliquidRawTriggerInfo(
                triggerPx=trigger_px_wire,
                isMarket=is_market,
                tpsl="sl",  # All stop orders are stop-loss ("sl")
            )

            order_type_model = HyperliquidRawOrderType(trigger=trigger_info)
        else:
            raise InvalidEnumValueError(
                parameter_name="order_type",
                value=args.order_type.value,
                valid_values=[ot.value for ot in OrderType],
                enum_type="OrderType",
            )

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

    def build_place_order_payload_with_args(
        self,
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
        logger.debug(
            "building_place_order_payload",
            symbol=args.symbol,
            order_type=args.order_type.value,
            asset_index=asset_index,
            message="Building place order request payload",
        )

        # Use the static helper method to build the order spec
        wire_order = self._build_order_item_spec(
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
    def build_cancel_order_payload_with_args(
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
        logger.debug(
            "building_cancel_order_payload",
            symbol=args.symbol,
            order_id=order_id,
            asset_index=asset_index,
            message="Building cancel order request payload",
        )

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
        logger.debug(
            "building_order_status_payload",
            wallet_address=args.wallet_address,
            order_id=args.order_id,
            message="Building order status request payload",
        )

        return HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus",
            user=args.wallet_address,
            oid=args.order_id,
        )

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
        logger.debug(
            "building_open_orders_payload",
            wallet_address=args.wallet_address,
            message="Building open orders request payload",
        )

        return HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders", user=args.wallet_address or ""
        )

    @staticmethod
    def build_historical_orders_payload(
        args: GetOrderHistoryArgsHL,
    ) -> HyperliquidRawHistoricalOrdersRequestPayload:
        """Build the Pydantic model for fetching historical orders.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated GetOrderHistoryArgsHL containing order history parameters

        Returns:
            HyperliquidRawHistoricalOrdersRequestPayload: Validated Raw API model
        """
        logger.debug(
            "building_historical_orders_payload",
            wallet_address=args.wallet_address,
            message="Building historical orders request payload",
        )

        # Currently just uses wallet address, but could be extended with time filters
        return HyperliquidRawHistoricalOrdersRequestPayload(
            type="historicalOrders",
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
        logger.debug(
            "building_user_fills_request_payload",
            wallet_address=args.wallet_address,
            message="Building user fills request payload",
        )

        return HyperliquidRawUserFillsRequestPayload(type="userFills", user=args.wallet_address)

    def build_batch_place_order_payload(
        self,
        orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None] | None = None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build the Pydantic model for placing multiple orders in a single batch request.

        Enables massive performance improvements by batching multiple orders into one API call,
        reducing from N HTTP requests to 1, and from N EIP-712 signatures to 1.

        Args:
            orders: List of order placement arguments (compatibility parameter)
            orders_with_indices: List of (PlaceOrderArgs, asset_index) tuples for batch placement
            tif_mapping: Optional mapping of symbol->tif_str for custom time-in-force per order

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated Raw API model with multiple orders

        Raises:
            ValueError: If any order has invalid parameters or batch is empty
        """
        if not orders_with_indices:
            raise MissingRequiredParameterError(
                parameter_name="orders",
                operation="batch order placement",
            )

        if len(orders_with_indices) > MAX_BATCH_SIZE:  # Conservative batch size limit
            raise DecimalRangeError(
                value=Decimal(len(orders_with_indices)),
                constraint="batch size exceeds maximum",
                max_value=Decimal(MAX_BATCH_SIZE),
                parameter_name="batch_size",
            )

        logger.debug(
            "building_batch_place_order_payload",
            batch_size=len(orders_with_indices),
            has_tif_mapping=tif_mapping is not None,
            message="Building batch place order request payload",
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

    def build_place_order_request(
        self,
        _orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None] | None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build place order request payload.

        Wrapper around build_batch_place_order_payload for compatibility with
        the decomposed trading services.

        Args:
            _orders: List of order placement arguments (unused, for compatibility)
            orders_with_indices: List of (PlaceOrderArgs, asset_index) tuples
            tif_mapping: Time-in-force mapping

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated request payload model
        """
        return self.build_batch_place_order_payload(
            orders=_orders,
            orders_with_indices=orders_with_indices,
            tif_mapping=tif_mapping,
        )

    def build_batch_cancel_order_payload(
        self,
        cancel_requests: list[tuple[str, int, str]],  # (order_id, asset_index, symbol) tuples
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the Pydantic model for cancelling multiple orders in a single batch request.

        Enables performance improvements by batching multiple cancellations into one API call.

        Args:
            cancel_requests: List of (order_id, asset_index, symbol) tuples for batch cancellation

        Returns:
            HyperliquidApiCancelOrderRequest: Validated Raw API model with multiple cancels

        Raises:
            ValueError: If cancel list is empty or exceeds batch limits
        """
        if not cancel_requests:
            raise MissingRequiredParameterError(
                parameter_name="cancel_requests",
                operation="batch order cancellation",
            )

        if len(cancel_requests) > MAX_BATCH_SIZE:  # Conservative batch size limit
            raise DecimalRangeError(
                value=Decimal(len(cancel_requests)),
                constraint="batch size exceeds maximum",
                max_value=Decimal(MAX_BATCH_SIZE),
                parameter_name="batch_size",
            )

        logger.debug(
            "building_batch_cancel_order_payload",
            batch_size=len(cancel_requests),
            message="Building batch cancel order request payload",
        )

        # Build cancel item specs for all cancellations in the batch
        cancel_specs: list[HyperliquidRawCancelItem] = []
        for order_id, asset_index, _symbol in cancel_requests:
            # Note: symbol is not used in the raw API request, only order_id and asset_index
            cancel_spec = HyperliquidRawCancelItem(a=asset_index, o=int(order_id))
            cancel_specs.append(cancel_spec)

        # Return the final batch cancel request payload with Pydantic validation
        return HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=cancel_specs,  # Multiple cancels in one request!
        )

    # Protocol implementation methods
    @staticmethod
    def build_place_order_payload(
        symbol: str,
        order_type: OrderType,
        order_side: OrderSide,
        quantity: Decimal,
        price: Decimal | None = None,
        reduce_only: bool = False,
        vault_address: ChecksumAddress | None = None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build order placement payload using protocol interface.

        Args:
            symbol: The trading symbol
            order_type: Type of order (limit, market, etc.)
            order_side: Side of order (buy/sell)
            quantity: Order quantity
            price: Order price (None for market orders)
            reduce_only: Whether this is a reduce-only order
            vault_address: Optional vault address for vault trading

        Returns:
            Validated Pydantic model containing order placement payload
        """
        logger.debug(
            "building_place_order_payload_protocol",
            symbol=symbol,
            order_type=order_type.value,
            order_side=order_side.value,
            quantity=str(quantity),
            price=str(price) if price else None,
            reduce_only=reduce_only,
            vault_address=vault_address,
            message="Building place order payload via protocol",
        )

        # Convert order side to wire format
        is_buy = order_side == OrderSide.BUY

        # Convert quantity to wire format
        sz_wire = HyperliquidTradingRequestBuilder._decimal_to_wire_format(quantity)

        # Handle price based on order type
        if order_type == OrderType.MARKET:
            if price is None:
                raise MissingRequiredParameterError(
                    parameter_name="price",
                    operation="market order placement",
                )
            limit_px_wire = HyperliquidTradingRequestBuilder._decimal_to_wire_format(price)
            order_type_model = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Ioc"),
            )
        else:
            price_for_wire = price if price is not None else Decimal(0)
            limit_px_wire = HyperliquidTradingRequestBuilder._decimal_to_wire_format(price_for_wire)
            order_type_model = HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc"),
            )

        # Build order spec (asset_index would need to be resolved by caller)
        # For protocol compliance, we'll use asset_index=0 as placeholder
        wire_order = HyperliquidRawOrderItemSpec(
            asset_index=0,  # Placeholder - caller must resolve symbol to asset_index
            is_buy=is_buy,
            limit_px=limit_px_wire,
            size=sz_wire,
            reduce_only=reduce_only,
            order_type_details=order_type_model,
            client_order_id=None,
        )

        return HyperliquidApiPlaceOrderRequest(
            type="order",
            orders=[wire_order],
            grouping="na",
        )

    @staticmethod
    def build_cancel_order_payload(order_id: str, symbol: str) -> HyperliquidApiCancelOrderRequest:
        """Build order cancellation payload using protocol interface.

        Args:
            order_id: The order ID to cancel
            symbol: The trading symbol

        Returns:
            Validated Pydantic model containing order cancellation payload
        """
        logger.debug(
            "building_cancel_order_payload_protocol",
            order_id=order_id,
            symbol=symbol,
            message="Building cancel order payload via protocol",
        )

        # Convert order_id to integer (Hyperliquid uses numeric order IDs)
        try:
            numeric_order_id = int(order_id)
        except ValueError as e:
            raise InvalidEnumValueError(
                parameter_name="order_id",
                value=order_id,
                valid_values=["numeric_string"],
                enum_type="OrderID",
            ) from e

        # For protocol compliance, we'll use asset_index=0 as placeholder
        cancel_item = HyperliquidRawCancelItem(a=0, o=numeric_order_id)
        return HyperliquidApiCancelOrderRequest(type="cancel", cancels=[cancel_item])

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str | None = None,
    ) -> HyperliquidApiCancelOrderRequest:
        """Build cancel all orders payload using protocol interface.

        Args:
            symbol: Optional symbol to cancel orders for (None for all symbols)

        Returns:
            Validated Pydantic model containing cancel all orders payload
        """
        logger.debug(
            "building_cancel_all_orders_payload_protocol",
            symbol=symbol,
            message="Building cancel all orders payload via protocol",
        )

        # Hyperliquid doesn't have a direct "cancel all" endpoint
        # This would need to be implemented by fetching open orders first
        # For now, return empty cancellation list
        return HyperliquidApiCancelOrderRequest(type="cancel", cancels=[])

    @staticmethod
    def build_modify_order_payload(
        order_id: str, symbol: str, quantity: Decimal | None = None, price: Decimal | None = None
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build order modification payload using protocol interface.

        Note: Hyperliquid doesn't have direct order modification - this is implemented
        as cancel + place operation.

        Args:
            order_id: The order ID to modify
            symbol: The trading symbol
            quantity: New quantity (None to keep current)
            price: New price (None to keep current)

        Returns:
            Validated Pydantic model containing order modification payload
        """
        logger.debug(
            "building_modify_order_payload_protocol",
            order_id=order_id,
            symbol=symbol,
            quantity=str(quantity) if quantity else None,
            price=str(price) if price else None,
            message="Building modify order payload via protocol",
        )

        # Since Hyperliquid doesn't support direct modification,
        # we'll create a placeholder order that would replace the original
        if quantity is None:
            quantity = Decimal(1)  # Placeholder
        if price is None:
            price = Decimal(1)  # Placeholder

        # Convert to wire format
        sz_wire = HyperliquidTradingRequestBuilder._decimal_to_wire_format(quantity)
        limit_px_wire = HyperliquidTradingRequestBuilder._decimal_to_wire_format(price)

        # Build replacement order spec
        wire_order = HyperliquidRawOrderItemSpec(
            asset_index=0,  # Placeholder - caller must resolve
            is_buy=True,  # Placeholder - caller must determine
            limit_px=limit_px_wire,
            size=sz_wire,
            reduce_only=False,
            order_type_details=HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc"),
            ),
            client_order_id=None,
        )

        return HyperliquidApiPlaceOrderRequest(
            type="order",
            orders=[wire_order],
            grouping="na",
        )
