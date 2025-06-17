"""CyberDeltaEngine: Hyperliquid API Error Mapper.

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

        # Try canonical string matching first
        category = HyperliquidErrorMapper._try_canonical_string_match(msg)
        if category != HyperliquidAPIErrorCategory.UNKNOWN:
            return category

        # Fallback to regex pattern matching
        return HyperliquidErrorMapper._try_regex_pattern_match(msg)

    @staticmethod
    def _try_canonical_string_match(msg: str) -> HyperliquidAPIErrorCategory:
        """Try to match error message using canonical error strings."""
        for canonical, category in HYPERLIQUID_ERROR_STRINGS.items():
            if canonical in msg:
                return category
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _try_regex_pattern_match(msg: str) -> HyperliquidAPIErrorCategory:
        """Try to match error message using regex patterns."""
        # Try authentication and balance patterns
        category = HyperliquidErrorMapper._match_auth_and_balance_patterns(msg)
        if category != HyperliquidAPIErrorCategory.UNKNOWN:
            return category

        # Try asset and order validation patterns
        category = HyperliquidErrorMapper._match_asset_and_order_patterns(msg)
        if category != HyperliquidAPIErrorCategory.UNKNOWN:
            return category

        # Try specialized patterns (rate limit, price, server, order state, TWAP)
        return HyperliquidErrorMapper._match_specialized_patterns(msg)

    @staticmethod
    def _match_auth_and_balance_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match authentication and balance related patterns."""
        if HyperliquidErrorMapper._regex_match(msg, r"insufficient (balance|margin)"):
            return HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE
        if HyperliquidErrorMapper._regex_match(msg, r"invalid signature"):
            return HyperliquidAPIErrorCategory.INVALID_SIGNATURE
        if HyperliquidErrorMapper._regex_match(msg, r"unauthorized"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        if HyperliquidErrorMapper._regex_match(msg, r"user not found"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        if HyperliquidErrorMapper._regex_match(msg, r"does not exist"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _match_asset_and_order_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match asset and order validation patterns."""
        if HyperliquidErrorMapper._regex_match(msg, r"invalid asset"):
            return HyperliquidAPIErrorCategory.INVALID_ASSET
        if HyperliquidErrorMapper._regex_match(msg, r"invalid order type"):
            return HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE

        # Order size patterns
        return HyperliquidErrorMapper._match_order_size_patterns(msg)

    @staticmethod
    def _match_specialized_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match specialized error patterns (rate limit, price, server, order state, TWAP)."""
        # Rate limit patterns
        if HyperliquidErrorMapper._match_rate_limit_patterns(msg):
            return HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED

        # Price and value patterns
        if HyperliquidErrorMapper._regex_match(msg, r"price out of bounds"):
            return HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS
        if HyperliquidErrorMapper._regex_match(msg, r"order must have minimum value"):
            return HyperliquidAPIErrorCategory.ORDER_MIN_VALUE

        # Server error patterns
        if HyperliquidErrorMapper._regex_match(msg, r"internal server error"):
            return HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR

        # Order state patterns
        category = HyperliquidErrorMapper._match_order_state_patterns(msg)
        if category != HyperliquidAPIErrorCategory.UNKNOWN:
            return category

        # TWAP patterns
        return HyperliquidErrorMapper._match_twap_patterns(msg)

    @staticmethod
    def _match_order_size_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match order size related error patterns."""
        if HyperliquidErrorMapper._regex_match(msg, r"invalid order size"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
        if HyperliquidErrorMapper._regex_match(msg, r"order size too small"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
        if HyperliquidErrorMapper._regex_match(msg, r"order size too large"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE
        # Match "Order value too large. Max is $XXX" pattern
        if HyperliquidErrorMapper._regex_match(msg, r"(order )?value too large"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _match_rate_limit_patterns(msg: str) -> bool:
        """Check if message matches rate limit patterns."""
        if HyperliquidErrorMapper._regex_match(msg, r"(rate limit|ratelimit) exceeded"):
            return True

        # Detect address-based rate limit messages that indicate fallback to
        # "one request every 10 seconds"
        rate_limit_patterns = [
            r"please wait and retry",
            r"too many requests.*please wait",
            r"exceeded.*address.*limit",
            r"one request every \d+ seconds",
            r"your ip has been rate limited",  # IP ban pattern
            r"ip.*rate.*limit",  # General IP rate limit pattern
        ]
        return HyperliquidErrorMapper._regex_match(msg, rate_limit_patterns)

    @staticmethod
    def _match_order_state_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match order state related error patterns."""
        order_not_found_patterns = [
            r"order was never placed",
            r"already canceled",
            r"already filled",
            r"order not found",
        ]
        if HyperliquidErrorMapper._regex_match(msg, order_not_found_patterns):
            return HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _match_twap_patterns(msg: str) -> HyperliquidAPIErrorCategory:
        """Match TWAP related error patterns."""
        if HyperliquidErrorMapper._regex_match(msg, r"invalid twap duration"):
            return HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION

        twap_not_found_patterns = [
            r"twap was never placed",
            r"twap already canceled",
            r"twap already filled",
        ]
        if HyperliquidErrorMapper._regex_match(msg, twap_not_found_patterns):
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
        # Check for IP ban pattern first (403 + rate limit message)
        ip_ban_error = self._check_ip_ban_pattern(status_code, error_body, original_exception)
        if ip_ban_error:
            return ip_ban_error

        # Handle critical HTTP status codes
        critical_status_error = self._handle_critical_status_codes(
            status_code, error_body, original_exception
        )
        if critical_status_error:
            return critical_status_error

        # Extract and categorize error message
        extracted_message, category = self._extract_and_categorize_error(
            error_body, error_data, status_code
        )

        # Build the final API error
        return self._build_api_error(
            extracted_message, category, status_code, original_exception, request_path
        )

    def _check_ip_ban_pattern(
        self, status_code: int, error_body: str | None, original_exception: Exception | None
    ) -> APIError | None:
        """Check for Hyperliquid IP ban pattern (403 + rate limit message)."""
        if status_code == 403:
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
                    retry_after=None,
                )
        return None

    def _handle_critical_status_codes(
        self, status_code: int, error_body: str | None, original_exception: Exception | None
    ) -> APIError | None:
        """Handle critical HTTP status codes with direct mapping."""
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

        if status_code == 401:
            return self._handle_authentication_error(status_code, error_body, original_exception)

        if status_code == 403:
            # Non-rate-limit 403, treat as authentication failure
            message = error_body if error_body else "Forbidden"
            return APIError(
                message=message,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                http_status=status_code,
                exchange_message=error_body,
                original_exception=original_exception,
            )

        return None

    def _handle_authentication_error(
        self, status_code: int, error_body: str | None, original_exception: Exception | None
    ) -> APIError:
        """Handle 401 authentication errors with refined string mapping."""
        specific_error_from_string = self.map_string_error(
            error_body or "",
            http_status=status_code,
        )

        if specific_error_from_string.code != APIErrorCode.EXCHANGE_SPECIFIC.value and error_body:
            return specific_error_from_string

        message = error_body if error_body else "Authentication failed"
        return APIError(
            message=message,
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=status_code,
            exchange_message=error_body,
            original_exception=original_exception,
        )

    def _extract_and_categorize_error(
        self, error_body: str | None, error_data: dict[str, Any] | None, status_code: int
    ) -> tuple[str, HyperliquidAPIErrorCategory]:
        """Extract error message and categorize it."""
        extracted_message = error_body or "Unknown Hyperliquid error"

        if error_data and isinstance(error_data.get("error"), str):
            # Prioritize error_data["error"] structure
            extracted_message = error_data["error"]
            category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)
        elif error_body:
            # Try parsing error_body as JSON array or use as-is
            extracted_message = self._parse_error_body(error_body)
            category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)
        else:
            # No error_body or error_data available
            extracted_message = f"Unknown Hyperliquid error (HTTP {status_code})"
            category = HyperliquidAPIErrorCategory.UNKNOWN

        return extracted_message, category

    def _parse_error_body(self, error_body: str) -> str:
        """Parse error_body which might be JSON array or plain string."""
        try:
            potential_list = json.loads(error_body)
            if (
                isinstance(potential_list, list)
                and potential_list
                and isinstance(potential_list[0], str)
            ):
                return potential_list[0]
        except (json.JSONDecodeError, TypeError):
            pass
        return error_body

    def _build_api_error(
        self,
        extracted_message: str,
        category: HyperliquidAPIErrorCategory,
        status_code: int,
        original_exception: Exception | None,
        request_path: str | None,
    ) -> APIError:
        """Build the final APIError object."""
        api_error_code_enum = HyperliquidErrorMapper._map_category_to_api_error_code(category)

        # Determine exchange_specific_code
        exchange_specific_code = self._get_exchange_specific_code(category)

        # Calculate retry_after for rate limits
        retry_after = self._calculate_retry_after(category, extracted_message)

        api_err_response_obj = APIErrorResponse.from_exchange_error(
            message=extracted_message,
            code=api_error_code_enum.value,
            http_status=status_code,
            exchange_code=exchange_specific_code,
            exchange_message=extracted_message,
            retry_after=retry_after,
            metadata={"request_path": request_path} if request_path else None,
        )

        return APIError(
            message=api_err_response_obj.message,
            code=api_err_response_obj.code,
            http_status=api_err_response_obj.http_status,
            exchange_code=api_err_response_obj.exchange_code,
            exchange_message=api_err_response_obj.exchange_message,
            retry_after=api_err_response_obj.retry_after,
            original_exception=original_exception,
        )

    def _get_exchange_specific_code(self, category: HyperliquidAPIErrorCategory) -> str | None:
        """Get exchange-specific code based on category."""
        if (
            category != HyperliquidAPIErrorCategory.UNKNOWN
            and category != HyperliquidAPIErrorCategory.ERROR
        ):
            return category.name
        return None

    def _calculate_retry_after(
        self, category: HyperliquidAPIErrorCategory, extracted_message: str
    ) -> float | None:
        """Calculate retry_after for rate limit errors."""
        if category != HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED:
            return None

        msg_lower = extracted_message.lower()
        if "one request every 10 seconds" in msg_lower:
            return 10.5  # Add small buffer
        elif "one request every" in msg_lower:
            match = re.search(r"one request every (\d+) seconds", msg_lower)
            if match:
                seconds = int(match.group(1))
                return seconds + 0.5  # Add small buffer
        return None
