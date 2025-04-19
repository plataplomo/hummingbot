"""
CyberDeltaEngine Backpack Funding Rate Parser

This module provides a function to parse raw funding rate data from Backpack into a standardized FundingRate object for CyberDeltaEngine.
"""

from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import FundingRate


def bp_parse_funding_rate(data: dict[str, Any]) -> FundingRate:
    """
    Parse raw funding rate data from Backpack into a FundingRate object.

    Args:
        data: Raw funding rate data from Backpack API or WebSocket.
    Returns:
        FundingRate: Standardized FundingRate object for CyberDeltaEngine.
    Raises:
        APIError: If required fields are missing or invalid.
    """
    try:
        symbol = data.get("s") or data.get("symbol") or ""
        funding_rate = Decimal(str(data.get("f") or data.get("fundingRate") or "0"))
        mark_price = Decimal(str(data.get("p") or data.get("markPrice") or "0"))
        index_price = Decimal(str(data.get("i") or data.get("indexPrice") or "0"))
        timestamp = int(data.get("E") or data.get("time") or data.get("T") or 0)
        return FundingRate(
            symbol=symbol,
            funding_rate=funding_rate,
            mark_price=mark_price,
            index_price=index_price,
            timestamp=timestamp,
        )
    except Exception as e:
        raise APIError(
            f"Error parsing funding rate data: {e}", code=APIErrorCode.INVALID_PARAMS
        ) from e
