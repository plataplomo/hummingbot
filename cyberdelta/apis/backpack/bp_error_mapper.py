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
import re
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError
from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

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
                        f"[{BackpackErrorMapper.__name__}] Unmapped or ambiguous "
                        f"Backpack error code: {code}"
                    )
                return mapped_code
            except ValidationError as e:
                detailed_errors = e.errors(include_url=False, include_context=False)
                logger.warning(
                    f"[{BackpackErrorMapper.__name__}] Failed to parse error_data as "
                    f"{BackpackRawApiError.__name__}. "
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
            "service temporarily unavailable": APIErrorCode.MAINTENANCE,  # Often implies maint.
            "resource not found": APIErrorCode.ORDER_NOT_FOUND,  # Can also be other resources
            "order not found": APIErrorCode.ORDER_NOT_FOUND,  # More specific
            "trading paused": APIErrorCode.MARKET_CLOSED,
            "account liquidating": APIErrorCode.EXCHANGE_SPECIFIC,  # Potential LIQUIDATION
            # if added
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
        error_body: str | None,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """
        Maps a raw Backpack error response to a standardized APIError object.

        Args:
            status_code: HTTP status code.
            error_body: Raw error body as a string (may be None).
            error_data: Parsed error data (typically a dictionary).
            request_path: Optional path of the request that failed.
            original_exception: Optional original exception that led to this mapping.

        Returns:
            APIError: A standardized APIError object.
        """
        effective_error_body = error_body if error_body is not None else "No error body provided"

        # Initial determination of APIErrorCode based on error_data (if available) and status_code.
        # _map_backpack_error_code_to_api_error_code will return EXCHANGE_SPECIFIC
        # if error_data is unparseable or contains an unmapped Backpack code.
        api_error_code_enum = self._map_backpack_error_code_to_api_error_code(
            error_body=effective_error_body, error_data=error_data, status_code=status_code
        )

        bp_code_str: str | None = None
        effective_exchange_message: str = effective_error_body
        effective_message: str  # To be defined below

        if error_data:
            try:
                # Attempt to parse error_data as BackpackRawApiError to get specific details
                raw_error = BackpackRawApiError.model_validate(error_data)
                bp_code_str = raw_error.code
                effective_exchange_message = raw_error.message

                # If parsing error_data was successful, construct the message.
                # api_error_code_enum is already set based on raw_error.code if it was known,
                # or EXCHANGE_SPECIFIC if it was an unknown (but validly structured) Backpack code.
                if api_error_code_enum != APIErrorCode.EXCHANGE_SPECIFIC:
                    effective_message = (
                        f"{api_error_code_enum.name.replace('_', ' ').title()}: "
                        f"{effective_exchange_message}"
                    )
                else:
                    effective_message = effective_exchange_message  # For known EXCHANGE_SPECIFIC
                    # or unmapped valid BP code

            except ValidationError as e_val_specific:
                # This block is reached if error_data was present but NOT parseable by
                # BackpackRawApiError.
                # api_error_code_enum would have been set to EXCHANGE_SPECIFIC by
                # _map_backpack_error_code_to_api_error_code because of this parsing failure.
                # We should honor that EXCHANGE_SPECIFIC determination.
                # Build message pieces to avoid line length issues
                class_name = self.__class__.__name__
                has_errors_method = hasattr(e_val_specific, "errors")
                errors_str = (
                    e_val_specific.errors(include_url=False)
                    if has_errors_method
                    else str(e_val_specific)
                )
                exc_str = str(e_val_specific)
                code_name = api_error_code_enum.name

                logger.warning(
                    f"[{class_name}] Failed to parse error_data as BackpackRawApiError. "
                    f"Pydantic errors: {errors_str}. "
                    f"Original exception string: {exc_str}. "
                    f"Error classified as {code_name} based on initial mapping."
                )

                # Try to get a more specific exchange message from error_data if possible,
                # even if the whole structure failed validation.
                if isinstance(error_data.get("message"), str):
                    effective_exchange_message = error_data["message"]
                # else: effective_exchange_message remains effective_error_body
                # (the full JSON string)

                # Construct a generic message for this unparseable error_data scenario.
                effective_message = (
                    f"Backpack API Error (HTTP {status_code}), unparseable error data: "
                    f"{effective_error_body}"
                )
                # bp_code_str remains None as we couldn't parse it.
                # api_error_code_enum remains as determined by
                # _map_backpack_error_code_to_api_error_code
                # (which should be EXCHANGE_SPECIFIC in this path).

        else:  # No error_data was provided
            effective_exchange_message = effective_error_body
            # api_error_code_enum from _map_backpack_error_code_to_api_error_code would be
            # EXCHANGE_SPECIFIC
            # (as error_data was None). Test expectations suggest that when error_data is None,
            # we should generally stick to EXCHANGE_SPECIFIC if
            # _map_backpack_error_code_to_api_error_code
            # returned it, rather than applying broad status code heuristics, unless the status code
            # is very specific (like 429 for RATE_LIMITED or 401/403 for AUTHENTICATION_FAILED).
            if api_error_code_enum == APIErrorCode.EXCHANGE_SPECIFIC:
                if status_code == 401 or status_code == 403:
                    api_error_code_enum = APIErrorCode.AUTHENTICATION_FAILED
                elif status_code == 404:
                    api_error_code_enum = APIErrorCode.ORDER_NOT_FOUND
                elif status_code == 429:
                    api_error_code_enum = APIErrorCode.RATE_LIMITED

            # Construct message based on the potentially refined api_error_code_enum
            if api_error_code_enum != APIErrorCode.EXCHANGE_SPECIFIC:
                effective_message = (
                    f"{api_error_code_enum.name.replace('_', ' ').title()}: "
                    f"{effective_exchange_message}"
                )
            else:
                effective_message = (
                    f"Backpack API Error (HTTP {status_code}): {effective_exchange_message}"
                )

        # Parse retry_after if this is a rate limit error
        parsed_retry_after_seconds: float | None = None
        if api_error_code_enum == APIErrorCode.RATE_LIMITED:
            logger.debug(
                f"Attempting to parse retry_after from Backpack rate limit message: "
                f"'{effective_exchange_message}'"
            )

            # Define regex patterns to search for retry-after hints (case-insensitive)
            retry_patterns = [
                (re.compile(r"retry after (\d+) seconds?", re.IGNORECASE), "seconds"),
                (re.compile(r"try again in (\d+) ms", re.IGNORECASE), "milliseconds"),
                (re.compile(r"please wait (\d+)s", re.IGNORECASE), "seconds"),
                (re.compile(r"wait for (\d+) milliseconds", re.IGNORECASE), "milliseconds"),
                (re.compile(r"wait (\d+) seconds?", re.IGNORECASE), "seconds"),
            ]

            for pattern, unit in retry_patterns:
                match = pattern.search(effective_exchange_message)
                if match:
                    try:
                        numeric_value = int(match.group(1))
                        if unit == "milliseconds":
                            parsed_retry_after_seconds = float(numeric_value) / 1000.0
                        else:  # seconds
                            parsed_retry_after_seconds = float(numeric_value)

                        logger.info(
                            f"Parsed retry_after from Backpack message: "
                            f"{parsed_retry_after_seconds} seconds."
                        )
                        break  # Use first successful match
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Failed to parse numeric value from regex match: {e}")
                        continue

            if parsed_retry_after_seconds is None:
                logger.debug(
                    "No parsable retry_after information found in Backpack rate limit message."
                )

        # Construct the final APIError
        current_metadata = error_data if error_data is not None else {}
        if request_path:
            current_metadata["request_path"] = request_path
        current_metadata["exchange_name"] = "Backpack"  # Hardcode for Backpack mapper

        return APIError(
            message=effective_message,
            code=api_error_code_enum.value,
            http_status=status_code,
            exchange_code=bp_code_str,
            exchange_message=effective_exchange_message,
            metadata=current_metadata,  # Pass updated metadata
            original_exception=original_exception,
            retry_after=parsed_retry_after_seconds,  # Add the parsed retry_after
        )
