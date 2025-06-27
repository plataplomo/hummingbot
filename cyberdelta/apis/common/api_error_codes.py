"""Standardized API error codes for exchange operations."""

from enum import Enum


class APIErrorCode(Enum):
    """Standardized error codes for API failures to allow consistent handling across exchanges.

    These codes abstract the exchange-specific error codes into a common format and are used
    throughout CyberDeltaEngine for robust, cross-exchange error handling.

    Grouped by:
      - Network/Transport (0-99)
      - Market/Business Logic (100-199)
      - Unknown/Miscellaneous (200-299)

    Within each group, codes are ordered by code value (ascending).
    """

    # --- Network/Transport Errors (0-99) ---
    CONNECTION_ERROR = 0
    TIMEOUT = 1
    NETWORK_ISSUE = 2
    SERVICE_UNAVAILABLE = 3
    SERVER_ERROR = 4
    MAINTENANCE = 5
    INVALID_RESPONSE = 6

    # --- Market/Business Logic Errors (100-199) ---
    AUTHENTICATION_FAILED = 100
    INSUFFICIENT_FUNDS = 101
    INVALID_REQUEST = 102
    INVALID_PARAMS = 103
    INVALID_ORDER_SIZE = 104
    INVALID_SYMBOL = 105
    SYMBOL_NOT_FOUND = 106
    ORDER_NOT_FOUND = 107
    ORDER_REJECTED = 108
    RATE_LIMITED = 109
    PRICE_OUT_OF_RANGE = 110
    QUANTITY_OUT_OF_RANGE = 111
    PRECISION_ERROR = 112
    DUPLICATE_ORDER = 113
    MIN_NOTIONAL_NOT_MET = 114
    MAX_POSITION_EXCEEDED = 115
    MARKET_CLOSED = 116
    LIQUIDATION_IN_PROGRESS = 117
    FUNDING_RATE_UNAVAILABLE = 118
    IP_BAN_SUSPECTED = 119
    MIN_QUANTITY_NOT_MET = 120

    # --- Unknown/Miscellaneous Errors (200-299) ---
    UNKNOWN = 200
    EXCHANGE_SPECIFIC = 201
    TRANSFORMATION_FAILED = 202
    # (Add future catch-alls or unmapped errors here)
