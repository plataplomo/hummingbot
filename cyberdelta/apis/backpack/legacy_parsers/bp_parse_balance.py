"""
CyberDeltaEngine Backpack Balance Parser

This module provides a function to parse raw balance data from Backpack into a standardized
Balance object for CyberDeltaEngine.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base_api import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import SpotBalance
from cyberdelta.utils.parsing import parse_datetime_utc


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
        # Parse timestamp safely
        ts_raw = data.get("timestamp")
        timestamp = parse_datetime_utc(ts_raw, field_name="timestamp") or datetime.now(UTC)
        return SpotBalance(
            exchange="backpack",
            asset=asset,
            total_quantity=total,
            available_quantity=available,
            timestamp=timestamp,
        )
    except Exception as e:
        raise APIError(
            f"Error parsing balance data: {e}", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
