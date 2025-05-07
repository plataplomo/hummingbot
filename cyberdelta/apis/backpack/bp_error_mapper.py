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

The `map_exchange_error` method is the primary entry point, designed to be used
by the `BackpackAPI` client when handling non-2xx HTTP responses or other error conditions.
"""

import logging
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError
from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.api_error_response import APIErrorResponse

logger = logging.getLogger(__name__)


class BackpackErrorMapper(IErrorMapper):
    """
    Maps and normalizes Backpack API errors to CyberDeltaEngine's canonical error model.

    Implements the IErrorMapper interface for Backpack-specific error handling.
    """

    @staticmethod
    def _map_backpack_error_code_to_api_error_code(
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
                if mapped_code == APIErrorCode.EXCHANGE_SPECIFIC and code not in code_map:
                    logger.warning(
                        f"[{BackpackErrorMapper.__name__}] Unmapped or ambiguous Backpack error code: {code}"
                    )
                return mapped_code
            except ValidationError as e:
                detailed_errors = e.errors(include_url=False, include_context=False)
                logger.warning(
                    f"[{BackpackErrorMapper.__name__}] Failed to parse error_data as {BackpackRawApiError.__name__}. "
                    f"Pydantic errors: {detailed_errors}. Original exception string: {e}. "
                    f"Falling back to heuristics."
                )
        return mapped_code

    def map_string_error(self, error_message: str, http_status: int | None = None) -> APIError:
        """
        Maps a raw error string from Backpack to a standardized APIError.

        Args:
            error_message: The raw error string from the exchange.
            http_status: Optional HTTP status code associated with the error.

        Returns:
            APIError: A standardized APIError object.
        """
        # Default to EXCHANGE_SPECIFIC if no specific match is found
        mapped_code_enum = APIErrorCode.EXCHANGE_SPECIFIC
        normalized_error_message = error_message.lower()

        # This mapping can be expanded as more specific string errors are identified
        string_to_code_map = {
            "invalid signature": APIErrorCode.AUTHENTICATION_FAILED,
            "unauthorized": APIErrorCode.AUTHENTICATION_FAILED,
            "forbidden": APIErrorCode.AUTHENTICATION_FAILED,
            "too many requests": APIErrorCode.RATE_LIMITED,
            "ratelimit exceeded": APIErrorCode.RATE_LIMITED,  # General catch for rate limits
            "invalid symbol": APIErrorCode.INVALID_SYMBOL,
            "invalid quantity": APIErrorCode.INVALID_ORDER_SIZE,
            "invalid order": APIErrorCode.INVALID_REQUEST,
            "invalid price": APIErrorCode.INVALID_REQUEST,
            "insufficient funds": APIErrorCode.INSUFFICIENT_FUNDS,
            "insufficient balance": APIErrorCode.INSUFFICIENT_FUNDS,  # Common variation
            "insufficient margin": APIErrorCode.INSUFFICIENT_FUNDS,
            "order limit": APIErrorCode.ORDER_REJECTED,
            "position limit": APIErrorCode.MAX_POSITION_EXCEEDED,
            "server error": APIErrorCode.SERVER_ERROR,
            "internal server error": APIErrorCode.SERVER_ERROR,  # Common variation
            "maintenance": APIErrorCode.MAINTENANCE,
            "service temporarily unavailable": APIErrorCode.MAINTENANCE,  # Often implies maintenance
            "resource not found": APIErrorCode.ORDER_NOT_FOUND,  # Can also be other resources
            "order not found": APIErrorCode.ORDER_NOT_FOUND,  # More specific
            "trading paused": APIErrorCode.MARKET_CLOSED,
            "account liquidating": APIErrorCode.EXCHANGE_SPECIFIC,  # Potentially LIQUIDATION if we add it
        }

        for key_string, code_enum in string_to_code_map.items():
            if key_string in normalized_error_message:
                mapped_code_enum = code_enum
                break

        effective_http_status = (
            http_status if http_status is not None else 200
        )  # Default if not provided

        # If mapped_code_enum is still EXCHANGE_SPECIFIC but http_status suggests something else:
        if mapped_code_enum == APIErrorCode.EXCHANGE_SPECIFIC:
            if effective_http_status == 400:
                mapped_code_enum = APIErrorCode.INVALID_REQUEST
            elif effective_http_status == 401 or effective_http_status == 403:
                mapped_code_enum = APIErrorCode.AUTHENTICATION_FAILED
            elif effective_http_status == 404:
                mapped_code_enum = APIErrorCode.ORDER_NOT_FOUND  # Or generic NOT_FOUND
            elif effective_http_status == 429:
                mapped_code_enum = APIErrorCode.RATE_LIMITED
            elif effective_http_status == 500:
                mapped_code_enum = APIErrorCode.SERVER_ERROR
            elif effective_http_status == 503:
                mapped_code_enum = APIErrorCode.MAINTENANCE  # Or SERVICE_UNAVAILABLE

        return APIError(
            message=error_message,
            code=mapped_code_enum.value,
            http_status=effective_http_status,
            exchange_message=error_message,  # The raw string is the exchange message
        )

    def map_exchange_error(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
    ) -> APIError:
        """
        Map Backpack error responses to a standardized `APIError` exception object.

        This method orchestrates the error mapping process:
        1. Determines the internal `APIErrorCode` using `_map_backpack_error_code_to_api_error_code`.
        2. Extracts or defaults the primary error message.
        3. Constructs an `APIErrorResponse` object to normalize error details.
        4. Converts the `APIErrorResponse` into an `APIError` exception, ready to be raised.

        Ensures all error propagation within the system uses the standardized `APIError`,
        carrying consistent information like HTTP status, exchange codes, and messages.

        Args:
            status_code: HTTP status code from the response.
            error_body: Raw error response body string.
            error_data: Parsed error data dictionary from the response, if available.
            request_path: The API endpoint path that was called (for diagnostic metadata).

        Returns:
            APIError: A fully populated `APIError` exception object.
        """
        mapped_code = BackpackErrorMapper._map_backpack_error_code_to_api_error_code(
            error_body=error_body, error_data=error_data, status_code=status_code
        )
        msg: str = error_body
        if error_data and isinstance(error_data.get("msg"), str):
            msg = error_data["msg"]
        elif error_data and isinstance(error_data.get("message"), str):
            msg = error_data["message"]

        api_error_response = APIErrorResponse.from_exchange_error(
            message=msg,
            code=mapped_code.value,
            http_status=status_code,
            exchange_code=(
                str(error_data.get("code")) if error_data and "code" in error_data else None
            ),
            exchange_message=(
                error_data.get("msg")
                if error_data and "msg" in error_data
                else (error_data.get("message") if error_data and "message" in error_data else None)
            ),
            metadata={"request_path": request_path} if request_path else None,
        )
        try:
            code_enum = (
                APIErrorCode(api_error_response.code)
                if isinstance(api_error_response.code, int)
                else APIErrorCode.EXCHANGE_SPECIFIC
            )
        except ValueError:
            logger.warning(
                f"Value '{api_error_response.code}' not a valid APIErrorCode member. Defaulting."
            )
            code_enum = APIErrorCode.EXCHANGE_SPECIFIC

        exchange_code_str = (
            str(api_error_response.exchange_code)
            if api_error_response.exchange_code is not None
            else None
        )
        return APIError(
            message=api_error_response.message,
            code=code_enum.value,
            http_status=api_error_response.http_status,
            exchange_code=exchange_code_str,
            exchange_message=api_error_response.exchange_message,
            retry_after=api_error_response.retry_after,
            original_exception=None,
        )
