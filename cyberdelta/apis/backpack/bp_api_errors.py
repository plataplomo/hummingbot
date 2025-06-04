"""Backpack API Error Codes Enum.

----------------------------

Defines all error codes returned by the Backpack Exchange API,
as enumerated in the official OpenAPI spec (see openapi_backpack.json, ApiErrorCode schema).

Reference:
    - https://docs.backpack.exchange/
    - openapi_backpack.json#/components/schemas/ApiErrorCode

This enum is the authoritative source for Backpack-specific error code handling in CyberDeltaEngine.
"""

from enum import Enum


class BackpackAPIErrorCode(Enum):
    """Enum of all Backpack API error codes as per the OpenAPI spec (2024-05-14).

    Use for strict validation, error mapping, and documentation.

    Codes:
        FORBIDDEN: Access denied or forbidden.
        INVALID_CLIENT_REQUEST: Malformed or invalid client request.
        INVALID_SIGNATURE: Signature is invalid or missing.
        SERVER_ERROR: Internal server error.
        UNAUTHORIZED: Authentication required or failed.
        TIMEOUT: Request timed out.
        TOO_MANY_REQUESTS: Rate limit exceeded.
        RESOURCE_NOT_FOUND: Requested resource does not exist.
        MAINTENANCE: Service is under maintenance.
        INVALID_QUANTITY: Order quantity is invalid.
        ORDER_LIMIT: Order limit reached.
        INVALID_ORDER: Order is invalid.
        INVALID_PRICE: Price is invalid.
        INVALID_MARKET: Market is invalid or not found.
        INVALID_SOURCE: Source is invalid.
        INSUFFICIENT_FUNDS: Not enough funds for operation.
        INSUFFICIENT_MARGIN: Not enough margin for operation.
        POSITION_LIMIT: Position limit reached.
        ACCOUNT_LIQUIDATING: Account is being liquidated.
        TRADING_PAUSED: Trading is paused.
        INVALID_ASSET: Asset is invalid or not supported.
        INVALID_SYMBOL: Symbol is invalid or not supported.
        INVALID_POSITION_ID: Position ID is invalid.
        BORROW_REQUIRES_LEND_REDEEM: Borrow requires lend redeem.
        LEND_REQUIRES_BORROW_REPAY: Lend requires borrow repay.
        INSUFFICIENT_SUPPLY: Not enough supply for operation.
        BORROW_LIMIT: Borrow limit reached.
        LEND_LIMIT: Lend limit reached.
        MAX_LEVERAGE_REACHED: Maximum leverage reached.
        PRECONDITION_FAILED: Precondition for request failed.
        NOT_IMPLEMENTED: Feature or endpoint not implemented.
    """

    FORBIDDEN = "FORBIDDEN"
    INVALID_CLIENT_REQUEST = "INVALID_CLIENT_REQUEST"
    INVALID_SIGNATURE = "INVALID_SIGNATURE"
    SERVER_ERROR = "SERVER_ERROR"
    UNAUTHORIZED = "UNAUTHORIZED"
    TIMEOUT = "TIMEOUT"
    TOO_MANY_REQUESTS = "TOO_MANY_REQUESTS"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    MAINTENANCE = "MAINTENANCE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    ORDER_LIMIT = "ORDER_LIMIT"
    INVALID_ORDER = "INVALID_ORDER"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_MARKET = "INVALID_MARKET"
    INVALID_SOURCE = "INVALID_SOURCE"
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN"
    POSITION_LIMIT = "POSITION_LIMIT"
    ACCOUNT_LIQUIDATING = "ACCOUNT_LIQUIDATING"
    TRADING_PAUSED = "TRADING_PAUSED"
    INVALID_ASSET = "INVALID_ASSET"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    INVALID_POSITION_ID = "INVALID_POSITION_ID"
    BORROW_REQUIRES_LEND_REDEEM = "BORROW_REQUIRES_LEND_REDEEM"
    LEND_REQUIRES_BORROW_REPAY = "LEND_REQUIRES_BORROW_REPAY"
    INSUFFICIENT_SUPPLY = "INSUFFICIENT_SUPPLY"
    BORROW_LIMIT = "BORROW_LIMIT"
    LEND_LIMIT = "LEND_LIMIT"
    MAX_LEVERAGE_REACHED = "MAX_LEVERAGE_REACHED"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
