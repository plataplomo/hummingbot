"""Hyperliquid API Error Mapping.

-----------------------------

Hyperliquid does NOT provide official error codes or enums. All errors are returned as
free-form strings in the 'error' field of the response
(see: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint).

This module provides a mapping of known error substrings to internal error codes for
robust handling, but always falls back to a generic error for unknown/unexpected messages.

References:
    - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
    - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses
    - https://github.com/hyperliquid-dex/hyperliquid-python-sdk/tree/master
    - https://docs.ccxt.com/#/exchanges/hyperliquid

"""

from enum import Enum


class HyperliquidAPIErrorCategory(Enum):
    """Known categories of Hyperliquid API errors (reverse-engineered, not official).

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


HYPERLIQUID_ERROR_STRINGS = {
    "insufficient balance": HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE,
    "invalid signature": HyperliquidAPIErrorCategory.INVALID_SIGNATURE,
    "invalid asset": HyperliquidAPIErrorCategory.INVALID_ASSET,
    "invalid order type": HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE,
    "order size too small": HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL,
    "order size too large": HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE,
    "price out of bounds": HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS,
    "rate limit exceeded": HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED,
    "unauthorized": HyperliquidAPIErrorCategory.UNAUTHORIZED,
    "internal server error": HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR,
    "order must have minimum value": HyperliquidAPIErrorCategory.ORDER_MIN_VALUE,
    "order was never placed": HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED,
    "already canceled": HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED,
    "already filled": HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED,
    "invalid twap duration": HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION,
    "twap was never placed": HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED,
    "twap already canceled": HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED,
    "twap already filled": HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED,
}
