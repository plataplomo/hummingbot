"""
CyberDeltaEngine Backpack Position Parser

This module provides a function to parse raw position data from Backpack into a
standardized Position object for CyberDeltaEngine.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base_api import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import DerivativePosition, OrderSide


def bp_parse_position(data: dict[str, Any]) -> DerivativePosition:
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
        side = OrderSide.BUY if size > 0 else OrderSide.SELL

        # TODO: Backpack specific details should be parsed and added to the correct slot
        # timestamp = parse_datetime_utc(data.get("lastUpdatedAtMs"))

        return DerivativePosition(
            exchange="backpack",
            timestamp=datetime.now(UTC),
            symbol=symbol,
            size=size,
            entry_price=entry_price,
            mark_price=mark_price,
            side=side,
            liquidation_price=liquidation_price,
            unrealized_pnl=unrealized_pnl,
        )
    except Exception as e:
        raise APIError(
            f"Error parsing position data: {e}", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
