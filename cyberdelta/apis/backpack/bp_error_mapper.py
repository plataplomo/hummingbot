"""
CyberDeltaEngine: Backpack API Error Mapper
-------------------------------------------

This module defines the `BackpackErrorMapper` class, responsible for translating
raw error responses from the Backpack Exchange API into CyberDeltaEngine's standardized
`APIError` exceptions and `APIErrorCode` enums.

Core Responsibilities:
- Parsing raw error data (typically a dictionary with `code` and `message` fields)
  using the `BackpackRawApiError` Pydantic model for initial validation.
- Mapping specific Backpack error code strings (e.g., "INVALID_SIGNATURE") to the
  appropriate internal `APIErrorCode` (e.g., `APIErrorCode.AUTHENTICATION_FAILED`).
- Constructing a fully populated `APIError` object, ensuring consistent error structure
  and information (HTTP status, exchange-specific codes, messages) for upstream handling.
- Logging unmapped or ambiguous Backpack error codes to aid in diagnostics and future
  enhancements to the error mapping logic.

The `map_error_response` static method is the primary entry point, designed to be used
by the `BackpackAPI` client when handling non-2xx HTTP responses or other error conditions.
"""

import logging
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.api_error_response import APIErrorResponse

logger = logging.getLogger(__name__)


class BackpackErrorMapper:
    """
    Maps and normalizes Backpack API errors to CyberDeltaEngine's canonical error model.

    Responsibilities:
    - Map Backpack error codes (from BackpackRawApiError) to APIErrorCode.
    - Validate and normalize all error data using APIErrorResponse.
    - Raise APIError with all validated fields for unified error propagation.
    - Log ambiguous/unmapped codes for diagnostics and future mapping improvements.

    Usage:
        raise BackpackErrorMapper.map_error_response(...)
    """

    @staticmethod
    def map_error_code(
        error_body: str, error_data: dict[str, Any] | None = None, status_code: int | None = None
    ) -> APIErrorCode:
        """
        Map Backpack error responses (body, data, status) to standardized APIErrorCode.

        Attempts to parse `error_data` using `BackpackRawApiError` to extract a structured
        error code. If parsing succeeds, it maps known Backpack codes to internal `APIErrorCode`s.
        If `error_data` is unavailable or parsing fails, it may rely on `status_code` or
        heuristics from `error_body` (though current implementation primarily uses parsed code).
        Unmapped or ambiguous codes are logged, and `APIErrorCode.EXCHANGE_SPECIFIC` is returned.

        Args:
            error_body: Raw error body as string (for fallback/logging).
            error_data: Parsed error data as dict (if available and expected to contain
                        `code` and `message` fields per Backpack standard).
            status_code: HTTP status code (if available, currently primarily used for logging
                         context if direct code mapping fails).

        Returns:
            APIErrorCode: Canonical error code for internal handling.
        """
        mapped_code = APIErrorCode.EXCHANGE_SPECIFIC
        if error_data:
            try:
                raw_api_error = BackpackRawApiError.model_validate(error_data)
                code = raw_api_error.code.upper()
                code_map = {
                    "INVALID_SIGNATURE": APIErrorCode.AUTHENTICATION_FAILED,
                    "UNAUTHORIZED": APIErrorCode.AUTHENTICATION_FAILED,
                    "FORBIDDEN": APIErrorCode.AUTHENTICATION_FAILED,
                    "TOO_MANY_REQUESTS": APIErrorCode.RATE_LIMITED,
                    "RATE_LIMIT_EXCEEDED": APIErrorCode.RATE_LIMITED,
                    "INVALID_SYMBOL": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_QUANTITY": APIErrorCode.INVALID_ORDER_SIZE,
                    "INVALID_ORDER": APIErrorCode.INVALID_REQUEST,
                    "INVALID_PRICE": APIErrorCode.INVALID_REQUEST,
                    "INVALID_CLIENT_REQUEST": APIErrorCode.INVALID_REQUEST,
                    "INSUFFICIENT_FUNDS": APIErrorCode.INSUFFICIENT_FUNDS,
                    "INSUFFICIENT_MARGIN": APIErrorCode.INSUFFICIENT_FUNDS,
                    "ORDER_LIMIT": APIErrorCode.ORDER_REJECTED,
                    "POSITION_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "SERVER_ERROR": APIErrorCode.SERVER_ERROR,
                    "MAINTENANCE": APIErrorCode.MAINTENANCE,
                    "RESOURCE_NOT_FOUND": APIErrorCode.ORDER_NOT_FOUND,
                    "INVALID_MARKET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_SOURCE": APIErrorCode.EXCHANGE_SPECIFIC,
                    "ACCOUNT_LIQUIDATING": APIErrorCode.EXCHANGE_SPECIFIC,
                    "TRADING_PAUSED": APIErrorCode.MARKET_CLOSED,
                    "INVALID_ASSET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_POSITION_ID": APIErrorCode.ORDER_NOT_FOUND,
                    "BORROW_REQUIRES_LEND_REDEEM": APIErrorCode.EXCHANGE_SPECIFIC,
                    "LEND_REQUIRES_BORROW_REPAY": APIErrorCode.EXCHANGE_SPECIFIC,
                    "INSUFFICIENT_SUPPLY": APIErrorCode.INSUFFICIENT_FUNDS,
                    "BORROW_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "LEND_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "MAX_LEVERAGE_REACHED": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "PRECONDITION_FAILED": APIErrorCode.INVALID_REQUEST,
                    "NOT_IMPLEMENTED": APIErrorCode.EXCHANGE_SPECIFIC,
                }
                mapped_code = code_map.get(code, APIErrorCode.EXCHANGE_SPECIFIC)
                if mapped_code == APIErrorCode.EXCHANGE_SPECIFIC:
                    logger.warning(
                        f"[BackpackErrorMapper] Unmapped or ambiguous Backpack error code: {code}"
                    )
                return mapped_code
            except Exception as e:
                logger.warning(
                    f"[BackpackErrorMapper] Failed to parse error_data as "
                    f"BackpackRawApiError: {e}. Falling back to heuristics."
                )
        return mapped_code

    @staticmethod
    def map_error_response(
        status_code: int | None,
        error_body: str,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        exchange_message: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """
        Map Backpack error responses to a standardized `APIError` exception object.

        This method orchestrates the error mapping process:
        1. Determines the internal `APIErrorCode` using `map_error_code`.
        2. Extracts or defaults the primary error message.
        3. Constructs an `APIErrorResponse` object to normalize error details.
        4. Converts the `APIErrorResponse` into an `APIError` exception, ready to be raised.

        Ensures all error propagation within the system uses the standardized `APIError`,
        carrying consistent information like HTTP status, exchange codes, and messages.

        Args:
            status_code: HTTP status code from the response, if available.
            error_body: Raw error response body string.
            error_data: Parsed error data dictionary from the response, if available.
            request_path: The API endpoint path that was called (for diagnostic metadata).
            exchange_message: Overrides message extracted from `error_body` or `error_data` if provided.
            original_exception: The original exception if this mapping is due to a caught error.

        Returns:
            APIError: A fully populated `APIError` exception object.
        """
        mapped_code = BackpackErrorMapper.map_error_code(
            error_body=error_body, error_data=error_data, status_code=status_code
        )
        # Guarantee message is always a str
        msg: str = error_body
        if exchange_message is not None:
            msg = exchange_message
        elif error_data and isinstance(error_data.get("msg"), str):
            msg = error_data["msg"]
        api_error_response = APIErrorResponse.from_exchange_error(
            message=msg,
            code=mapped_code.value,
            http_status=status_code,
            exchange_code=(
                str(error_data.get("code")) if error_data and "code" in error_data else None
            ),
            exchange_message=(error_data.get("msg") if error_data else None),
            metadata={"request_path": request_path} if request_path else None,
        )
        # Convert APIErrorResponse to APIErrorModel-compatible fields
        # APIErrorModel expects code: APIErrorCode, exchange_code: str|None
        # Defensive: ensure code is valid APIErrorCode, else EXCHANGE_SPECIFIC
        try:
            code_enum = (
                APIErrorCode(api_error_response.code)
                if isinstance(api_error_response.code, int)
                else APIErrorCode.EXCHANGE_SPECIFIC
            )
        except Exception:
            code_enum = APIErrorCode.EXCHANGE_SPECIFIC
        exchange_code_str = (
            str(api_error_response.exchange_code)
            if api_error_response.exchange_code is not None
            else None
        )
        # code_enum is always an APIErrorCode, so use .value directly
        return APIError(
            message=api_error_response.message,
            code=code_enum.value,
            http_status=api_error_response.http_status,
            exchange_code=exchange_code_str,
            exchange_message=api_error_response.exchange_message,
            retry_after=api_error_response.retry_after,
            original_exception=original_exception,
        )
