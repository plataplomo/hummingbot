"""
CyberDeltaEngine Backpack Order Book Parser

This module provides a function to parse raw order book data from Backpack into a standardized OrderBook object for CyberDeltaEngine.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import OrderBook


def bp_parse_order_book(data: dict[str, Any], symbol: str) -> OrderBook:
    """
    Parse raw order book data from Backpack into an OrderBook object.

    Args:
        data: Raw order book data from Backpack API or WebSocket.
        symbol: Trading symbol for the order book.
    Returns:
        OrderBook: Standardized OrderBook object for CyberDeltaEngine.
    Raises:
        APIError: If required fields are missing or invalid.
    """
    try:
        bids = [
            (Decimal(str(price)), Decimal(str(qty)))
            for price, qty in data.get("b", data.get("bids", []))
        ]
        asks = [
            (Decimal(str(price)), Decimal(str(qty)))
            for price, qty in data.get("a", data.get("asks", []))
        ]
        timestamp_ms = int(data.get("E") or data.get("time") or data.get("T") or 0)
        # Convert milliseconds timestamp to datetime object
        executed_at = datetime.fromtimestamp(timestamp_ms / 1e3, UTC)

        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=executed_at,  # Use datetime object
        )
    except Exception as e:
        raise APIError(
            f"Error parsing order book data: {e}",
            code=APIErrorCode.INVALID_PARAMS.value,  # Add .value
        ) from e
