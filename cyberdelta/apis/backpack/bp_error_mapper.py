"""CyberDeltaEngine: Backpack API Error Mapper.

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

import re
from http import HTTPStatus
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError
from cyberdelta.apis.base.validation_context_domain import DataPresenceState, ErrorMappingContext
from cyberdelta.apis.common import APIError, APIErrorCode, IErrorMapper
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class BackpackErrorMapper(IErrorMapper):
    """Maps and normalizes Backpack API errors to CyberDeltaEngine's canonical error model.

    Implements the IErrorMapper interface for Backpack-specific error handling.
    """

    @staticmethod
    def _map_backpack_error_code_to_api_error_code(
        error_body: str,
        error_data: dict[str, Any] | None = None,
        status_code: int | None = None,
    ) -> APIErrorCode:
        """Map Backpack error responses (body, data, status) to standardized APIErrorCode.

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
                        "backpack_unmapped_error_code",
                        error_code=code,
                        mapper_class=BackpackErrorMapper.__name__,
                        message="Unmapped or ambiguous Backpack error code",
                    )
            except ValidationError as e:
                detailed_errors = e.errors(include_url=False, include_context=False)
                logger.warning(
                    "backpack_error_data_parse_failed",
                    mapper_class=BackpackErrorMapper.__name__,
                    target_model=BackpackRawApiError.__name__,
                    pydantic_errors=detailed_errors,
                    exception_str=str(e),
                    action="falling_back_to_heuristics",
                    message="Failed to parse error_data as BackpackRawApiError, "
                    "falling back to heuristics",
                )
            else:
                return mapped_code

        # If no error_data or parsing failed, delegate to string-based mapping
        if error_body:
            # Use the existing string mapping logic by calling map_string_error
            # and extracting just the error code from the result
            string_error_result = BackpackErrorMapper().map_string_error(
                error_body,
                http_status=status_code,
            )
            mapped_code = APIErrorCode(string_error_result.code)

        return mapped_code

    def map_string_error(self, error_message: str, http_status: int | None = None) -> APIError:
        """Map a raw error string from Backpack to a standardized APIError.

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
            "invalid market symbol": APIErrorCode.INVALID_SYMBOL,  # Specific Backpack message
            'failed to parse "marketsymbol"': APIErrorCode.INVALID_SYMBOL,  # Alternative pattern
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
            if effective_http_status == HTTPStatus.BAD_REQUEST.value:
                mapped_code_enum = APIErrorCode.INVALID_REQUEST
            elif effective_http_status in {
                HTTPStatus.UNAUTHORIZED.value,
                HTTPStatus.FORBIDDEN.value,
            }:
                mapped_code_enum = APIErrorCode.AUTHENTICATION_FAILED
            elif effective_http_status == HTTPStatus.NOT_FOUND.value:
                mapped_code_enum = APIErrorCode.ORDER_NOT_FOUND  # Or generic NOT_FOUND
            elif effective_http_status == HTTPStatus.TOO_MANY_REQUESTS.value:
                mapped_code_enum = APIErrorCode.RATE_LIMITED
            elif effective_http_status == HTTPStatus.INTERNAL_SERVER_ERROR.value:
                mapped_code_enum = APIErrorCode.SERVER_ERROR
            elif effective_http_status == HTTPStatus.SERVICE_UNAVAILABLE.value:
                mapped_code_enum = APIErrorCode.MAINTENANCE  # Or SERVICE_UNAVAILABLE

        return APIError(
            message=error_message,
            code=mapped_code_enum.value,
            http_status=effective_http_status,
            exchange_message=error_message,  # The raw string is the exchange message
        )

    def _parse_error_data(self, error_data: dict[str, Any] | None) -> tuple[str | None, str, str]:
        """Parse error data to extract code, exchange message, and effective message.

        Returns:
            Tuple of (bp_code_str, effective_exchange_message, effective_message)
        """
        if not error_data:
            return None, "", ""

        try:
            raw_error = BackpackRawApiError.model_validate(error_data)
        except ValidationError as e_val_specific:
            self._log_validation_error(e_val_specific)
            # Try to extract message even if structure validation failed
            exchange_message = ""
            if isinstance(error_data.get("message"), str):
                exchange_message = error_data["message"]
            return None, exchange_message, ""
        else:
            return raw_error.code, raw_error.message, ""

    def _log_validation_error(self, e_val_specific: ValidationError) -> None:
        """Log validation error details."""
        class_name = self.__class__.__name__
        has_errors_method = hasattr(e_val_specific, "errors")
        errors_str = (
            e_val_specific.errors(include_url=False) if has_errors_method else str(e_val_specific)
        )
        exc_str = str(e_val_specific)

        logger.warning(
            "backpack_validation_error",
            class_name=class_name,
            pydantic_errors=errors_str,
            exception_str=exc_str,
            classification="EXCHANGE_SPECIFIC",
            message=(
                "Failed to parse error_data as BackpackRawApiError, classified as EXCHANGE_SPECIFIC"
            ),
        )

    def _construct_effective_message(
        self,
        api_error_code_enum: APIErrorCode,
        effective_exchange_message: str,
        status_code: int,
        effective_error_body: str,
        error_mapping_context: ErrorMappingContext | None = None,
    ) -> str:
        """Construct the effective error message based on context.

        Returns:
            Formatted error message string.
        """
        # Use default context if none provided
        if error_mapping_context is None:
            error_mapping_context = ErrorMappingContext()

        if api_error_code_enum != APIErrorCode.EXCHANGE_SPECIFIC:
            return (
                f"{api_error_code_enum.name.replace('_', ' ').title()}: "
                f"{effective_exchange_message}"
            )

        # Use context to determine if error data should be included
        has_error_data = bool(effective_error_body and effective_error_body.strip())
        data_presence = DataPresenceState.PRESENT if has_error_data else DataPresenceState.ABSENT
        if error_mapping_context.should_include_error_data(data_presence):
            return effective_exchange_message
        return f"Backpack API Error (HTTP {status_code}): {effective_exchange_message}"

    def _parse_retry_after(
        self,
        api_error_code_enum: APIErrorCode,
        effective_exchange_message: str,
    ) -> float | None:
        """Parse retry_after from rate limit error messages.

        Returns:
            Retry after time in seconds if found, None otherwise.
        """
        if api_error_code_enum != APIErrorCode.RATE_LIMITED:
            return None

        logger.debug(
            "backpack_retry_after_parse_attempt",
            exchange_message=effective_exchange_message,
            message="Attempting to parse retry_after from Backpack rate limit message",
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
                        "backpack_retry_after_parsed",
                        retry_after_seconds=parsed_retry_after_seconds,
                        message="Parsed retry_after from Backpack message",
                    )
                except (ValueError, IndexError) as e:
                    logger.debug(
                        "retry_after_parse_failed",
                        action="parse_retry_after",
                        error=str(e),
                        message="Failed to parse numeric value from regex match",
                    )
                    continue
                else:
                    return parsed_retry_after_seconds

        logger.debug(
            "No parsable retry_after information found in Backpack rate limit message.",
        )
        return None

    def _refine_error_code_for_no_data(
        self,
        api_error_code_enum: APIErrorCode,
        status_code: int,
    ) -> APIErrorCode:
        """Refine error code when no error_data is available.

        Returns:
            Refined APIErrorCode based on status code and context.
        """
        if api_error_code_enum != APIErrorCode.EXCHANGE_SPECIFIC:
            return api_error_code_enum

        if status_code in {
            HTTPStatus.UNAUTHORIZED.value,
            HTTPStatus.FORBIDDEN.value,
        }:
            return APIErrorCode.AUTHENTICATION_FAILED
        if status_code == HTTPStatus.NOT_FOUND.value:
            return APIErrorCode.ORDER_NOT_FOUND
        if status_code == HTTPStatus.TOO_MANY_REQUESTS.value:
            return APIErrorCode.RATE_LIMITED
        return api_error_code_enum

    def map_exchange_error(
        self,
        status_code: int,
        error_body: str | None,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """Map a raw Backpack error response to a standardized APIError object.

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

        # Initial determination of APIErrorCode
        api_error_code_enum = self._map_backpack_error_code_to_api_error_code(
            error_body=effective_error_body,
            error_data=error_data,
            status_code=status_code,
        )

        # Parse error data
        bp_code_str, parsed_exchange_message, _ = self._parse_error_data(error_data)

        # Set effective exchange message
        effective_exchange_message = parsed_exchange_message or effective_error_body

        # Handle case where no error_data was provided
        if not error_data:
            api_error_code_enum = self._refine_error_code_for_no_data(
                api_error_code_enum,
                status_code,
            )

        # Construct effective message
        effective_message = self._construct_effective_message(
            api_error_code_enum,
            effective_exchange_message,
            status_code,
            effective_error_body,
        )

        # Handle unparseable error_data case
        if error_data and not bp_code_str and not parsed_exchange_message:
            effective_message = (
                f"Backpack API Error (HTTP {status_code}), unparseable error data: "
                f"{effective_error_body}"
            )

        # Parse retry_after if this is a rate limit error
        parsed_retry_after_seconds = self._parse_retry_after(
            api_error_code_enum,
            effective_exchange_message,
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
