"""
CyberDeltaEngine Backpack Position Parser

This module provides a function to parse raw position data from Backpack into a standardized Position object for CyberDeltaEngine.
"""

from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.enums import APIErrorCode
from cyberdelta.core.models import OrderSide, Position


def bp_parse_position(data: dict[str, Any]) -> Position:
    """
    Parse raw position data from Backpack into a Position object.

    Args:
        data: Raw position data from Backpack API or WebSocket.
    Returns:
        Position: Standardized Position object for CyberDeltaEngine.
    Raises:
        APIError: If required fields are missing or invalid.
    """
    try:
        symbol = data.get("symbol") or ""
        size = Decimal(str(data.get("positionSize") or "0"))
        entry_price = Decimal(str(data.get("entryPrice") or "0"))
        mark_price = Decimal(str(data.get("markPrice") or "0"))
        liquidation_price = Decimal(str(data.get("liquidationPrice") or "0"))
        unrealized_pnl = Decimal(str(data.get("unrealizedPnl") or "0"))
        leverage = Decimal(str(data.get("leverage") or "1"))
        side = OrderSide.BUY if size > 0 else OrderSide.SELL
        return Position(
            symbol=symbol,
            size=size,
            entry_price=entry_price,
            mark_price=mark_price,
            side=side,
            liquidation_price=liquidation_price,
            unrealized_pnl=unrealized_pnl,
            leverage=leverage,
        )
    except Exception as e:
        raise APIError(f"Error parsing position data: {e}", code=APIErrorCode.INVALID_PARAMS) from e
