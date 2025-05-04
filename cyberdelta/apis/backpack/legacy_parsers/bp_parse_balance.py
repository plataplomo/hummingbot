"""
CyberDeltaEngine Backpack Balance Parser

This module provides a function to parse raw balance data from Backpack into a standardized
Balance object for CyberDeltaEngine.
"""

from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import SpotBalance


def bp_parse_balance(data: dict[str, Any]) -> SpotBalance:
    """
    Parse raw balance data from Backpack into a Balance object.

    Args:
        data: Raw balance data from Backpack API or WebSocket.
    Returns:
        Balance: Standardized Balance object for CyberDeltaEngine.
    Raises:
        APIError: If required fields are missing or invalid.
    """
    try:
        asset = data.get("asset") or data.get("symbol") or ""
        available = Decimal(str(data.get("available") or "0"))
        total = Decimal(str(data.get("total") or "0"))
        return SpotBalance(
            exchange="backpack",
            asset=asset,
            available=available,
            total=total,
        )
    except Exception as e:
        raise APIError(
            f"Error parsing balance data: {e}", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
