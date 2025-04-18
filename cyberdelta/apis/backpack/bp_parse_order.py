"""
CyberDeltaEngine Backpack Order Parser

This module provides a function to parse raw order data from Backpack into a standardized Order object for CyberDeltaEngine.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.enums import APIErrorCode
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType


def bp_parse_order(data: dict[str, Any]) -> Order:
    """
    Parse raw order data from Backpack into an Order object (CyberDeltaEngine standard).

    Args:
        data: Raw order data from Backpack API (dict)
    Returns:
        Order: Standardized Order object for CyberDeltaEngine
    Raises:
        APIError: If required fields are missing or invalid
    """
    try:
        status_str = data.get("status", "").upper()
        order_status = (
            OrderStatus[status_str]
            if status_str in OrderStatus.__members__
            else OrderStatus.UNKNOWN
        )
        price = data.get("price")
        quantity = data.get("quantity")
        quantity_filled = data.get("filledQuantity")
        average_fill_price = data.get("avgFillPrice")
        order_time_ms = data.get("time")
        created_at = (
            datetime.fromtimestamp(order_time_ms / 1000, UTC)
            if order_time_ms
            else datetime.now(UTC)
        )
        return Order(
            symbol=data["symbol"] if "symbol" in data else "",
            side=OrderSide(data["side"]) if "side" in data else OrderSide.BUY,
            order_type=OrderType(data["orderType"]) if "orderType" in data else OrderType.LIMIT,
            quantity_requested=Decimal(str(quantity)) if quantity is not None else Decimal("0"),
            status=order_status,
            client_order_id=str(data.get("clientId", "")),
            price=Decimal(str(price)) if price is not None else None,
            quantity_filled=Decimal(str(quantity_filled))
            if quantity_filled is not None
            else Decimal("0"),
            average_fill_price=Decimal(str(average_fill_price))
            if average_fill_price is not None
            else None,
            created_at=created_at,
        )
    except KeyError as e:
        raise APIError(f"Missing key {e} in order data", code=APIErrorCode.INVALID_PARAMS) from e
    except Exception as e:
        raise APIError(f"Error parsing order data: {e}", code=APIErrorCode.INVALID_PARAMS) from e
