"""CyberDeltaEngine: Hyperliquid API Raw Models (Historical Order Group).

--------------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of
Hyperliquid Exchange API responses related to historical or any-status orders,
such as those from 'queryOrderHistory' or 'orderStatus' (when the order might not be open).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, RootModel, field_validator, model_validator

# Assuming common_raw_types and other necessary components are accessible
# For simplicity, copying relevant parts of HyperliquidRawOrder here
# and modifying the status field.
from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawCloidString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawHistoricalOrderStatusHL,  # Use the new historical status type
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.typing import is_dict_str_any, is_list_any


# Logger removed - no longer needed after refactoring to Pydantic validators

# Assuming HyperliquidRawTriggerInfo is defined elsewhere (e.g., hl_raw_open_orders.py)
# If not, it would need to be defined or imported here.
# For now, let's assume it might not be part of a simple historical order view,
# or would be handled if needed. The main change is the status.


class HyperliquidRawHistoricalOrderData(BaseModel):
    """Order details from historicalOrders endpoint - the 'order' object only."""

    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawCloidString64HL | None = Field(None, alias="cloid")
    coin: RawAssetString64HL = Field(..., alias="coin")  # The symbol/asset
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: RawDefaultString = Field(..., alias="orderType")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    tif: RawDefaultString = Field(..., alias="tif")
    # Additional fields that may be present
    trigger_condition: RawDefaultString | None = Field(None, alias="triggerCondition")
    is_trigger: RawStrictBool | None = Field(None, alias="isTrigger")
    trigger_px: RawFiniteDecimalStr | None = Field(None, alias="triggerPx")
    children: list[object] | None = Field(None, alias="children")
    is_position_tpsl: RawStrictBool | None = Field(None, alias="isPositionTpsl")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawHistoricalOrder(BaseModel):
    """Complete historical order model combining order data with status.

    This model is used by the mapper and includes all fields needed for
    transformation to internal Order model.
    """

    # All fields from HyperliquidRawHistoricalOrderData
    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawCloidString64HL | None = Field(None, alias="cloid")
    coin: RawAssetString64HL = Field(..., alias="coin")
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: RawDefaultString = Field(..., alias="orderType")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    tif: RawDefaultString = Field(..., alias="tif")
    trigger_condition: RawDefaultString | None = Field(None, alias="triggerCondition")
    is_trigger: RawStrictBool | None = Field(None, alias="isTrigger")
    trigger_px: RawFiniteDecimalStr | None = Field(None, alias="triggerPx")
    children: list[object] | None = Field(None, alias="children")
    is_position_tpsl: RawStrictBool | None = Field(None, alias="isPositionTpsl")

    # Status fields from the parent level
    status: RawHistoricalOrderStatusHL = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")

    # For compatibility with mapper - provide asset field from coin
    @property
    def asset(self) -> str:
        """Get asset name from coin field for compatibility with mappers."""
        return self.coin

    # For compatibility - remaining_sz is always the current sz for historical orders
    @property
    def remaining_sz(self) -> str:
        """Get remaining size which equals current size for historical orders."""
        return self.sz

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawHistoricalOrderResponse(BaseModel):
    """Response structure for each item in the historicalOrders endpoint response.

    This represents each item in the array returned by historicalOrders, with
    status and statusTimestamp at the top level alongside the order object.
    """

    order: HyperliquidRawHistoricalOrderData = Field(
        ...,
        description="The order details.",
    )
    status: RawHistoricalOrderStatusHL = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def preprocess_order_status_response(cls, data: object) -> dict[str, Any]:
        """Handle various response formats from the orderStatus API endpoint.

        This validator replaces the preprocessing mapper logic by handling:
        - List responses (including empty lists and string items)
        - String responses (e.g., "Order not found")
        - None responses
        - Nested order structures
        - Flat order structures
        """
        # Handle list format responses
        if is_list_any(data):
            # data is now properly typed as list[Any] due to TypeGuard
            data = cls._handle_list_response(data)

        # Handle string or None responses
        elif isinstance(data, str):
            cls._handle_string_response(data)
        elif data is None:
            raise APIError(
                message="Order status response is None",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        # At this point, data should be a dict
        if not is_dict_str_any(data):
            raise APIError(
                message=f"Order status response: expected dict after preprocessing, "
                f"got {type(data).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        # Handle nested or flat order structures
        # data is now properly typed as dict[str, Any] due to TypeGuard
        return cls._normalize_order_structure(data)

    @classmethod
    def _handle_list_response(cls, data: list[Any]) -> dict[str, Any]:
        """Handle list format responses."""
        if len(data) == 0:
            raise APIError(
                message="Order not found (empty list).",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )

        status_item = data[0]

        # Handle string responses in list
        if isinstance(status_item, str):
            if "Order not found" in status_item:
                raise APIError(
                    message=f"Order not found (string response: {status_item!r})",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                    metadata={"original_response_item": status_item},
                )
            raise APIError(
                message=f"Unexpected order status response: {status_item}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"original_response_item": status_item},
            )

        # Handle non-dict items
        if not is_dict_str_any(status_item):
            raise APIError(
                message=f"Order status response list: expected dict, "
                f"got {type(status_item).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"original_response_item": status_item},
            )

        # status_item is now properly typed as dict[str, Any] due to TypeGuard
        return status_item

    @classmethod
    def _handle_string_response(cls, data: str) -> None:
        """Handle string responses - always raises an error."""
        if "Order not found" in data:
            raise APIError(
                message=f"Order not found (direct string: {data!r})",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
                metadata={"original_response": data},
            )
        raise APIError(
            message=f"Invalid order status response format: got string {data!r}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    @classmethod
    def _normalize_order_structure(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Normalize nested or flat order structures."""
        # Handle nested order structures
        if "order" in data and is_dict_str_any(data["order"]):
            # data["order"] is now properly typed as dict[str, Any] due to TypeGuard
            nested_data = data["order"]

            # Check for double nesting (order.order)
            if "order" in nested_data and is_dict_str_any(nested_data["order"]):
                # Extract from double nesting
                # nested_data["order"] is now properly typed as dict[str, Any] due to TypeGuard
                inner_order = nested_data["order"]
                status = nested_data.get("status", "open")
                status_timestamp = nested_data.get("statusTimestamp")
            else:
                # Single level nesting
                inner_order = nested_data
                status = data.get("status", "open")
                status_timestamp = data.get("statusTimestamp")

            return {"order": inner_order, "status": status, "statusTimestamp": status_timestamp}

        # Handle flat order structure
        if "oid" in data and ("asset" in data or "coin" in data):
            # It's already a flat order object, wrap it
            status = data.get("status", "open")
            status_timestamp = data.get("statusTimestamp")

            return {"order": data, "status": status, "statusTimestamp": status_timestamp}

        # Return as-is if it's already in the expected format
        return data


class HyperliquidRawHistoricalOrdersResponse(
    RootModel[list[HyperliquidRawHistoricalOrderResponse]],
):
    """Response model for list of historical orders from historicalOrders endpoint.

    This RootModel validates an array of historical order responses, handling
    any preprocessing needed for the list structure.
    """

    root: list[HyperliquidRawHistoricalOrderResponse]

    @property
    def items(self) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Return the validated list of historical orders."""
        return self.root

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_orders_list(cls, v: object) -> list[dict[str, Any]]:
        """Validate and preprocess the list of historical orders.

        This validator handles:
        - Type checking that input is a list
        - Filtering out non-dict items with warnings
        - Ensuring all items are dictionaries for further validation
        """
        if not is_list_any(v):
            raise ValueError(f"Expected a list of orders, got {type(v).__name__}")

        # v is now properly typed as list[Any] due to TypeGuard
        validated_items: list[dict[str, Any]] = []
        for i, item in enumerate(v):
            if not is_dict_str_any(item):
                # Log warning but skip non-dict items
                from cyberdelta.config.structlog_config import get_logger

                logger = get_logger(__name__)
                logger.warning(
                    f"Skipping non-dict item in historical orders list at index {i}: {item!r}",
                )
                continue

            # item is now properly typed as dict[str, Any] due to TypeGuard
            validated_items.append(item)

        return validated_items
