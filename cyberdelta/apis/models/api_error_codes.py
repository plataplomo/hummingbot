from enum import Enum


class APIErrorCode(Enum):
    """
    Standardized error codes for API failures to allow consistent handling across exchanges.
    These codes abstract the exchange-specific error codes into a common format and are used
    throughout CyberDeltaEngine for robust, cross-exchange error handling.
    - UNKNOWN: Unknown error.
    - AUTHENTICATION_FAILED: Authentication or signature error.
    - INSUFFICIENT_FUNDS: Not enough funds for operation.
    - RATE_LIMITED: Rate limit exceeded.
    - ORDER_NOT_FOUND: Order not found.
    - SYMBOL_NOT_FOUND: Symbol not found (use instead of INVALID_SYMBOL).
    - INVALID_PARAMS: General bad parameters.
    - SERVER_ERROR: General 5xx server error.
    - CONNECTION_ERROR: Network-level connection failure.
    - TIMEOUT: Request timed out.
    - MAINTENANCE: Exchange maintenance event.
    - ORDER_REJECTED: Order actively rejected (catch-all if not more specific).
    - PRICE_OUT_OF_RANGE: Price out of allowed range.
    - MARKET_CLOSED: Market is closed.
    - DUPLICATE_ORDER: Duplicate order (e.g., client order ID collision).
    - MIN_NOTIONAL_NOT_MET: Minimum notional not met.
    - MAX_POSITION_EXCEEDED: Maximum position exceeded.
    - LIQUIDATION_IN_PROGRESS: Liquidation in progress.
    - FUNDING_RATE_UNAVAILABLE: Funding rate unavailable.
    - EXCHANGE_SPECIFIC: Exchange-specific error (doesn't fit other categories).
    - NETWORK_ISSUE: Specific network connectivity issues (DNS, routing, etc.).
    - QUANTITY_OUT_OF_RANGE: Min/max order size issues.
    - PRECISION_ERROR: Price/quantity precision incorrect.
    - INVALID_REQUEST: Non-retryable client-side error (bad structure).
    - INVALID_SYMBOL: Symbol does not exist or is not tradable.
    - INVALID_ORDER_SIZE: Order size is invalid (too small/large).
    - SERVICE_UNAVAILABLE: Exchange or endpoint is temporarily unavailable.
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
    ORDER_REJECTED = 11
    PRICE_OUT_OF_RANGE = 12
    MARKET_CLOSED = 13
    DUPLICATE_ORDER = 14
    MIN_NOTIONAL_NOT_MET = 15
    MAX_POSITION_EXCEEDED = 16
    LIQUIDATION_IN_PROGRESS = 17
    FUNDING_RATE_UNAVAILABLE = 18
    EXCHANGE_SPECIFIC = 19
    NETWORK_ISSUE = 20
    QUANTITY_OUT_OF_RANGE = 21
    PRECISION_ERROR = 22
    INVALID_REQUEST = 23
    INVALID_SYMBOL = 24
    INVALID_ORDER_SIZE = 25
    SERVICE_UNAVAILABLE = 26
