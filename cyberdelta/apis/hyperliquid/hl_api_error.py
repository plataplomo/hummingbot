"""
Hyperliquid API Error Handling
-----------------------------

Hyperliquid does NOT provide official error codes or enums. All errors are returned as free-form strings
in the 'error' field of the response (see: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint).

This module provides a mapping of known error substrings to internal error codes for robust handling,
but always falls back to a generic error for unknown/unexpected messages.

References:
    - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
    - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses
    - https://github.com/hyperliquid-dex/hyperliquid-python-sdk/tree/master
    - https://docs.ccxt.com/#/exchanges/hyperliquid
"""

from enum import Enum


class HyperliquidAPIErrorCategory(Enum):
    """
    Known categories of Hyperliquid API errors (reverse-engineered, not official).
    This enum is based on observed error messages and public documentation.
    It is NOT exhaustive or guaranteed stable—always handle unknown errors defensively.
    """

    INSUFFICIENT_BALANCE = "Insufficient balance"
    INVALID_SIGNATURE = "Invalid signature"
    INVALID_ASSET = "Invalid asset"
    INVALID_ORDER_TYPE = "Invalid order type"
    ORDER_SIZE_TOO_SMALL = "Order size too small"
    ORDER_SIZE_TOO_LARGE = "Order size too large"
    PRICE_OUT_OF_BOUNDS = "Price out of bounds"
    RATE_LIMIT_EXCEEDED = "Rate limit exceeded"
    UNAUTHORIZED = "Unauthorized"
    INTERNAL_SERVER_ERROR = "Internal server error"
    ORDER_MIN_VALUE = "Order must have minimum value"
    ORDER_NOT_FOUND_OR_FILLED = "Order was never placed, already canceled, or filled"
    INVALID_TWAP_DURATION = "Invalid TWAP duration"
    TWAP_NOT_FOUND_OR_FILLED = "TWAP was never placed, already canceled, or filled"
    UNKNOWN = "Unknown error"
    ERROR = "error"  # Most common generic error string in Hyperliquid responses


def categorize_hyperliquid_error(error_message: str) -> HyperliquidAPIErrorCategory:
    """
    Map a Hyperliquid error message to a known error category, ERROR, or UNKNOWN.

    Args:
        error_message (str): The error message from the API.

    Returns:
        HyperliquidAPIErrorCategory: The mapped error category.
    """
    if not error_message:
        return HyperliquidAPIErrorCategory.UNKNOWN
    msg = error_message.strip().lower()
    if msg == "error":
        return HyperliquidAPIErrorCategory.ERROR
    if "insufficient balance" in msg:
        return HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE
    if "invalid signature" in msg:
        return HyperliquidAPIErrorCategory.INVALID_SIGNATURE
    if "invalid asset" in msg:
        return HyperliquidAPIErrorCategory.INVALID_ASSET
    if "invalid order type" in msg:
        return HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE
    if "order size too small" in msg:
        return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
    if "order size too large" in msg:
        return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE
    if "price out of bounds" in msg:
        return HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS
    if "rate limit exceeded" in msg:
        return HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED
    if "unauthorized" in msg:
        return HyperliquidAPIErrorCategory.UNAUTHORIZED
    if "internal server error" in msg:
        return HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR
    if "order must have minimum value" in msg:
        return HyperliquidAPIErrorCategory.ORDER_MIN_VALUE
    if "order was never placed" in msg or "already canceled" in msg or "already filled" in msg:
        return HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED
    if "invalid twap duration" in msg:
        return HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION
    if (
        "twap was never placed" in msg
        or "twap already canceled" in msg
        or "twap already filled" in msg
    ):
        return HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED
    return HyperliquidAPIErrorCategory.UNKNOWN
