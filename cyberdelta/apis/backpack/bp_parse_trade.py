"""
CyberDeltaEngine Backpack Trade Parser

This module provides a function to parse raw trade data from Backpack into a standardized Trade object for CyberDeltaEngine.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.enums import APIErrorCode
from cyberdelta.core.models import OrderSide, Trade


def bp_parse_trade(data: dict[str, Any], exchange_name: str) -> Trade:
    """
    Parse raw trade data from Backpack into a Trade object.

    Args:
        data: Raw trade data from Backpack API or WebSocket.
        exchange_name: Name of the exchange (for attribution in Trade object).
    Returns:
        Trade: Standardized Trade object for CyberDeltaEngine.
    Raises:
        APIError: If required fields are missing or invalid.
    """
    try:
        symbol = data.get("s") or data.get("symbol") or ""
        price = Decimal(str(data.get("p") or data.get("price") or "0"))
        quantity = Decimal(str(data.get("q") or data.get("qty") or "0"))
        trade_id = str(data.get("t") or data.get("id") or "")
        buyer_order_id = str(data.get("b") or data.get("buyerOrderId") or "")
        seller_order_id = str(data.get("a") or data.get("sellerOrderId") or "")
        timestamp = int(data.get("E") or data.get("time") or data.get("T") or 0)
        executed_at = (
            datetime.fromtimestamp(timestamp / 1e6, UTC)
            if timestamp > 1e12
            else datetime.fromtimestamp(timestamp / 1e3, UTC)
        )
        is_maker = bool(data.get("m")) if "m" in data else None
        side = (
            OrderSide.BUY
            if is_maker is False
            else OrderSide.SELL
            if is_maker is not None
            else OrderSide.BUY
        )
        return Trade(
            id=trade_id,
            symbol=symbol,
            executed_at=executed_at,
            side=side,
            order_id=buyer_order_id if side == OrderSide.BUY else seller_order_id,
            exchange=exchange_name,
            client_order_id="",
            price=price,
            quantity=quantity,
            cost=price * quantity,
            fee=Decimal("0"),
            fee_asset="",
            is_maker=is_maker,
            timestamp=timestamp,
        )
    except Exception as e:
        raise APIError(f"Error parsing trade data: {e}", code=APIErrorCode.INVALID_PARAMS) from e
