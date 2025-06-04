"""CyberDeltaEngine: Hyperliquid API Error Mapper
---------------------------------------------

This module centralizes the logic for mapping Hyperliquid's unstructured error messages
into CyberDeltaEngine's standardized error framework (APIErrorCode, APIError).

Responsibilities:
- Categorize Hyperliquid error strings using known patterns and regex.
- Map error categories or raw strings to standardized APIErrorCode enums.
- Transform raw Hyperliquid error responses (dicts or strings) into fully structured
  and validated APIError exceptions, suitable for consistent handling throughout the application.
"""

import json
import logging
import re
from typing import Any

from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.hyperliquid.hl_api_error import (
    HYPERLIQUID_ERROR_STRINGS,
    HyperliquidAPIErrorCategory,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.api_error_response import APIErrorResponse

logger = logging.getLogger(__name__)


class HyperliquidErrorMapper(IErrorMapper):
    """Provides methods for mapping and normalizing Hyperliquid API errors.
    Implements the IErrorMapper interface.
    """

    @staticmethod
    def _regex_match(msg: str, patterns: str | list[str]) -> bool:
        """Helper for regex-based error message matching.
        Accepts a single pattern or a list of patterns.
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        return any(re.search(p, msg, re.IGNORECASE) for p in patterns)

    @staticmethod
    def _categorize_hyperliquid_error(error_message: str) -> HyperliquidAPIErrorCategory:
        """Map a Hyperliquid error message to a known error category, ERROR, or UNKNOWN.
        Uses canonical error substrings from hl_api_error.py for initial matching,
        then regex for variants.
        """
        if not error_message:
            return HyperliquidAPIErrorCategory.UNKNOWN
        msg = error_message.strip().lower()
        if msg == "error":
            return HyperliquidAPIErrorCategory.ERROR
        # Canonical string check (from hl_api_error.py)
        for canonical, category in HYPERLIQUID_ERROR_STRINGS.items():
            if canonical in msg:
                return category
        # Fallback to regex/robust matching for variants
        if HyperliquidErrorMapper._regex_match(msg, r"insufficient (balance|margin)"):
            return HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE
        if HyperliquidErrorMapper._regex_match(msg, r"invalid signature"):
            return HyperliquidAPIErrorCategory.INVALID_SIGNATURE
        if HyperliquidErrorMapper._regex_match(msg, r"invalid asset"):
            return HyperliquidAPIErrorCategory.INVALID_ASSET
        if HyperliquidErrorMapper._regex_match(msg, r"invalid order type"):
            return HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE
        if HyperliquidErrorMapper._regex_match(msg, r"invalid order size"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
        if HyperliquidErrorMapper._regex_match(msg, r"order size too small"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
        if HyperliquidErrorMapper._regex_match(msg, r"order size too large"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE
        if HyperliquidErrorMapper._regex_match(msg, r"price out of bounds"):
            return HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS
        if HyperliquidErrorMapper._regex_match(msg, r"(rate limit|ratelimit) exceeded"):
            return HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED
        # Detect address-based rate limit messages that indicate fallback to
        # "one request every 10 seconds"
        if HyperliquidErrorMapper._regex_match(
            msg,
            [
                r"please wait and retry",
                r"too many requests.*please wait",
                r"exceeded.*address.*limit",
                r"one request every \d+ seconds",
                r"your ip has been rate limited",  # IP ban pattern
                r"ip.*rate.*limit",  # General IP rate limit pattern
            ],
        ):
            return HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED
        if HyperliquidErrorMapper._regex_match(msg, r"unauthorized"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        if HyperliquidErrorMapper._regex_match(msg, r"user not found"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        if HyperliquidErrorMapper._regex_match(msg, r"internal server error"):
            return HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR
        if HyperliquidErrorMapper._regex_match(msg, r"order must have minimum value"):
            return HyperliquidAPIErrorCategory.ORDER_MIN_VALUE
        if HyperliquidErrorMapper._regex_match(
            msg,
            [r"order was never placed", r"already canceled", r"already filled", r"order not found"],
        ):
            return HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED
        if HyperliquidErrorMapper._regex_match(msg, r"invalid twap duration"):
            return HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION
        if HyperliquidErrorMapper._regex_match(
            msg, [r"twap was never placed", r"twap already canceled", r"twap already filled"],
        ):
            return HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _map_category_to_api_error_code(
        category_or_message: HyperliquidAPIErrorCategory | str,
    ) -> APIErrorCode:
        """Map a HyperliquidAPIErrorCategory or raw error message (str) to APIErrorCode.
        If a string is provided, it is first categorized using regex logic.
        """
        if isinstance(category_or_message, str):
            category = HyperliquidErrorMapper._categorize_hyperliquid_error(category_or_message)
        else:
            category = category_or_message
        mapping = {
            HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE: APIErrorCode.INSUFFICIENT_FUNDS,
            HyperliquidAPIErrorCategory.INVALID_SIGNATURE: APIErrorCode.AUTHENTICATION_FAILED,
            HyperliquidAPIErrorCategory.INVALID_ASSET: APIErrorCode.INVALID_SYMBOL,
            HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE: APIErrorCode.INVALID_REQUEST,
            HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL: APIErrorCode.INVALID_ORDER_SIZE,
            HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE: APIErrorCode.INVALID_ORDER_SIZE,
            HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS: APIErrorCode.PRICE_OUT_OF_RANGE,
            HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED: APIErrorCode.RATE_LIMITED,
            HyperliquidAPIErrorCategory.UNAUTHORIZED: APIErrorCode.AUTHENTICATION_FAILED,
            HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR: APIErrorCode.SERVER_ERROR,
            HyperliquidAPIErrorCategory.ORDER_MIN_VALUE: APIErrorCode.MIN_NOTIONAL_NOT_MET,
            HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED: APIErrorCode.ORDER_NOT_FOUND,
            HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION: APIErrorCode.INVALID_REQUEST,
            HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED: APIErrorCode.ORDER_NOT_FOUND,
            HyperliquidAPIErrorCategory.UNKNOWN: APIErrorCode.EXCHANGE_SPECIFIC,
            HyperliquidAPIErrorCategory.ERROR: APIErrorCode.EXCHANGE_SPECIFIC,
        }
        return mapping.get(category, APIErrorCode.EXCHANGE_SPECIFIC)

    def map_string_error(self, error_message: str, http_status: int | None = None) -> APIError:
        """Maps a raw error string from Hyperliquid to a standardized APIError."""
        category = HyperliquidErrorMapper._categorize_hyperliquid_error(error_message)
        api_error_code_enum = HyperliquidErrorMapper._map_category_to_api_error_code(category)

        exchange_specific_code: str | None = None
        if (
            category != HyperliquidAPIErrorCategory.UNKNOWN
            and category != HyperliquidAPIErrorCategory.ERROR
        ):
            exchange_specific_code = category.name

        # Check for specific address-based rate limit messages that indicate
        # the "one request every 10 seconds" fallback mode
        retry_after: float | None = None
        if category == HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED:
            msg_lower = error_message.lower()
            # Look for specific patterns that indicate the 10-second fallback
            if "one request every 10 seconds" in msg_lower:
                retry_after = 10.5  # Add small buffer
            elif "one request every" in msg_lower:
                # Try to extract the number of seconds from the message
                import re

                match = re.search(r"one request every (\d+) seconds", msg_lower)
                if match:
                    seconds = int(match.group(1))
                    retry_after = seconds + 0.5  # Add small buffer

        return APIError(
            message=error_message,  # Use the original error_message for clarity
            code=api_error_code_enum.value,
            http_status=http_status,  # Can be None if not from HTTP context
            exchange_code=exchange_specific_code,
            exchange_message=error_message,
            retry_after=retry_after,
        )

    def map_exchange_error(
        self,
        status_code: int,
        error_body: str | None,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """Transform a raw Hyperliquid error response into a standardized APIError.
        This is the single entry point for mapping/categorizing/normalizing Hyperliquid errors.

        Args:
            status_code: The HTTP status code received from the exchange.
            error_body: The raw error string from the response body.
            error_data: The parsed JSON error response dict, if available (e.g., {"error": "msg"}).
            request_path: The specific API endpoint path that was called, if available.
            original_exception: The original exception that caused the error, if available.

        Returns:
            APIError: The standardized internal error model for business logic.

        """
        # Priority check for Hyperliquid IP ban (403 + rate limit message)
        if status_code == 403:
            # Check if the error is categorized as rate limit
            error_category = self._categorize_hyperliquid_error(error_body or "")
            if error_category == HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED:
                logger.warning(
                    f"[HyperliquidErrorMapper] Detected IP ban pattern: "
                    f"HTTP 403 with rate limit message: {error_body}",
                )
                return APIError(
                    message=error_body or "IP ban suspected - rate limit on 403",
                    code=APIErrorCode.IP_BAN_SUSPECTED.value,
                    http_status=status_code,
                    exchange_message=error_body,
                    original_exception=original_exception,
                    # Hyperliquid IP bans typically last 60-65 seconds, but no explicit duration
                    # is provided in the error message
                    retry_after=None,
                )

        # Prioritize critical HTTP status codes for direct mapping if body/data is uninformative
        if status_code == 503:
            return APIError(
                message=error_body or "Service Unavailable (503)",
                code=APIErrorCode.SERVICE_UNAVAILABLE.value,
                http_status=status_code,
                exchange_message=error_body,
                original_exception=original_exception,
            )
        if status_code == 429:
            return APIError(
                message=error_body or "Rate limit exceeded (429)",
                code=APIErrorCode.RATE_LIMITED.value,
                http_status=status_code,
                exchange_message=error_body,
                original_exception=original_exception,
            )
        # Handle common authentication/authorization issues based on status code,
        # especially if the error_body might be generic or empty.
        if status_code == 401:
            # If error_body provides a more specific reason, map_string_error might refine it.
            # However, if error_body is empty or generic, this status code is a strong indicator.
            specific_error_from_string = self.map_string_error(
                error_body or "", http_status=status_code,
            )
            if (
                specific_error_from_string.code != APIErrorCode.EXCHANGE_SPECIFIC.value
                and error_body
            ):
                return specific_error_from_string  # Use if string mapping was specific
                # and body wasn't empty

            # If error_body is empty, provide a clearer default message for these statuses
            message = error_body if error_body else "Authentication failed"
            return APIError(
                message=message,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                http_status=status_code,
                exchange_message=error_body,
                original_exception=original_exception,
            )
        # Handle 403 that is NOT a rate limit (authentication/forbidden)
        if status_code == 403:
            # This is a non-rate-limit 403, treat as authentication failure
            message = error_body if error_body else "Forbidden"
            return APIError(
                message=message,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                http_status=status_code,
                exchange_message=error_body,
                original_exception=original_exception,
            )

        # Fallback to categorizing based on error_body content if not a critical HTTP status
        category: HyperliquidAPIErrorCategory
        extracted_message: str = error_body or "Unknown Hyperliquid error"

        if error_data and isinstance(error_data.get("error"), str):
            # If error_data contains an 'error' string, prioritize it.
            # This matches Hyperliquid's typical JSON error structure: {"error": "message"}
            extracted_message = error_data["error"]
            category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)
        elif error_body:
            # If no error_data or error_data.error is not a string, try parsing error_body
            # as it might be a JSON array like ["error string"], or just a plain string.
            try:
                potential_list = json.loads(error_body)  # Ensure error_body is not None here
                if (
                    isinstance(potential_list, list)
                    and potential_list
                    and isinstance(potential_list[0], str)
                ):
                    extracted_message = potential_list[0]
                # else, extracted_message remains error_body (which is now confirmed not None)
            except (json.JSONDecodeError, TypeError):
                # Not a JSON list, or error_body was None (TypeError from json.loads)
                # extracted_message remains error_body (which is now confirmed not None)
                pass  # extracted_message is already set to error_body
            category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)
        else:
            # If error_body is also None (or empty), and no error_data.error
            extracted_message = f"Unknown Hyperliquid error (HTTP {status_code})"
            category = HyperliquidAPIErrorCategory.UNKNOWN

        api_error_code_enum = HyperliquidErrorMapper._map_category_to_api_error_code(category)

        # Determine exchange_specific_code based on category
        exchange_specific_code: str | None = None
        if (
            category != HyperliquidAPIErrorCategory.UNKNOWN
            and category != HyperliquidAPIErrorCategory.ERROR
        ):
            exchange_specific_code = category.name  # Use the enum member name as a code

        # Check for specific address-based rate limit messages that indicate
        # the "one request every X seconds" fallback mode
        retry_after: float | None = None
        if category == HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED:
            msg_lower = extracted_message.lower()
            # Look for specific patterns that indicate the fallback mode
            if "one request every 10 seconds" in msg_lower:
                retry_after = 10.5  # Add small buffer
            elif "one request every" in msg_lower:
                # Try to extract the number of seconds from the message
                match = re.search(r"one request every (\d+) seconds", msg_lower)
                if match:
                    seconds = int(match.group(1))
                    retry_after = seconds + 0.5  # Add small buffer

        api_err_response_obj = APIErrorResponse.from_exchange_error(
            message=extracted_message,
            code=api_error_code_enum.value,
            http_status=status_code,
            exchange_code=exchange_specific_code,  # Pass the derived exchange_specific_code
            exchange_message=extracted_message,
            retry_after=retry_after,  # Pass the calculated retry_after
            # original_exception not part of interface, metadata can hold request_path
            metadata={"request_path": request_path} if request_path else None,
        )

        return APIError(
            message=api_err_response_obj.message,
            code=api_err_response_obj.code,  # This should be an int (APIErrorCode.value)
            http_status=api_err_response_obj.http_status,
            exchange_code=api_err_response_obj.exchange_code,
            exchange_message=api_err_response_obj.exchange_message,
            retry_after=api_err_response_obj.retry_after,
            original_exception=original_exception,
        )
