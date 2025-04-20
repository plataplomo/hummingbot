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
