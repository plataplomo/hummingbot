from enum import Enum


class APIErrorCode(Enum):
    """
    Standard error codes for API failures to allow consistent handling across exchanges.
    These codes abstract the exchange-specific error codes into a common format.
    """

    UNKNOWN = 0
    AUTHENTICATION_FAILED = 1
    INSUFFICIENT_FUNDS = 2
    RATE_LIMITED = 3
    ORDER_NOT_FOUND = 4
    SYMBOL_NOT_FOUND = 5
    INVALID_PARAMS = 6
    SERVER_ERROR = 7
    CONNECTION_ERROR = 8
    TIMEOUT = 9
    MAINTENANCE = 10
    # New error codes for exchange-specific failures
    ORDER_REJECTED = 11
    PRICE_OUT_OF_RANGE = 12
    MARKET_CLOSED = 13
    DUPLICATE_ORDER = 14
    MIN_NOTIONAL_NOT_MET = 15
    MAX_POSITION_EXCEEDED = 16
    LIQUIDATION_IN_PROGRESS = 17
    FUNDING_RATE_UNAVAILABLE = 18
    EXCHANGE_SPECIFIC = 19  # For truly exchange-specific errors that don't fit other categories
    NETWORK_ISSUE = 20  # Specific network connectivity issues (DNS, routing, etc.)
    QUANTITY_OUT_OF_RANGE = 21  # For min/max quantity violations
    PRECISION_ERROR = 22  # When price/quantity doesn't match required precision
    INVALID_REQUEST = 23  # Added for non-retryable client errors (like bad params)
    INVALID_SYMBOL = 24  # Symbol does not exist or is not tradable
    INVALID_ORDER_SIZE = 25  # Order size is invalid (too small/large)
    SERVICE_UNAVAILABLE = 26  # Exchange or endpoint is temporarily unavailable
