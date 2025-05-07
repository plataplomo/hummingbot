"""
CyberDeltaEngine: Hyperliquid API Error Mapper
---------------------------------------------

This module centralizes the logic for mapping Hyperliquid's unstructured error messages
into CyberDeltaEngine's standardized error framework (APIErrorCode, APIError).

Responsibilities:
- Categorize Hyperliquid error strings using known patterns and regex.
- Map error categories or raw strings to standardized APIErrorCode enums.
- Transform raw Hyperliquid error responses (dicts or strings) into fully structured
  and validated APIError exceptions, suitable for consistent handling throughout the application.
"""

import logging
import re
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.hyperliquid.hl_api_error import (
    HYPERLIQUID_ERROR_STRINGS,
    HyperliquidAPIErrorCategory,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.api_error_response import APIErrorResponse

logger = logging.getLogger(__name__)


class HyperliquidErrorMapper(IErrorMapper):
    """
    Provides methods for mapping and normalizing Hyperliquid API errors.
    Implements the IErrorMapper interface.
    """

    @staticmethod
    def _regex_match(msg: str, patterns: str | list[str]) -> bool:
        """
        Helper for regex-based error message matching.
        Accepts a single pattern or a list of patterns.
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        return any(re.search(p, msg, re.IGNORECASE) for p in patterns)

    @staticmethod
    def _categorize_hyperliquid_error(error_message: str) -> HyperliquidAPIErrorCategory:
        """
        Map a Hyperliquid error message to a known error category, ERROR, or UNKNOWN.
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
            msg, [r"twap was never placed", r"twap already canceled", r"twap already filled"]
        ):
            return HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def _map_category_to_api_error_code(
        category_or_message: HyperliquidAPIErrorCategory | str,
    ) -> APIErrorCode:
        """
        Map a HyperliquidAPIErrorCategory or raw error message (str) to APIErrorCode.
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

        return APIError(
            message=error_message,  # Use the original error_message for clarity
            code=api_error_code_enum.value,
            http_status=http_status,  # Can be None if not from HTTP context
            exchange_code=exchange_specific_code,
            exchange_message=error_message,
        )

    def map_exchange_error(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
    ) -> APIError:
        """
        Transform a raw Hyperliquid error response into a standardized APIError.
        This is the single entry point for mapping/categorizing/normalizing Hyperliquid errors.

        Args:
            status_code: The HTTP status code received from the exchange.
            error_body: The raw error string from the response body.
            error_data: The parsed JSON error response dict, if available (e.g., {"error": "msg"}).
            request_path: The specific API endpoint path that was called, if available.

        Returns:
            APIError: The standardized internal error model for business logic.
        """
        # Prioritize critical HTTP status codes for direct mapping if body/data is uninformative
        if status_code == 503:
            return APIError(
                message=error_body or "Service Unavailable (503)",
                code=APIErrorCode.SERVICE_UNAVAILABLE.value,
                http_status=status_code,
                exchange_message=error_body,
            )
        if status_code == 429:
            return APIError(
                message=error_body or "Rate limit exceeded (429)",
                code=APIErrorCode.RATE_LIMITED.value,
                http_status=status_code,
                exchange_message=error_body,
            )
        # Handle common authentication/authorization issues based on status code,
        # especially if the error_body might be generic or empty.
        if status_code == 401 or status_code == 403:
            # If error_body provides a more specific reason, map_string_error might refine it.
            # However, if error_body is empty or generic, this status code is a strong indicator.
            specific_error_from_string = self.map_string_error(error_body, http_status=status_code)
            if (
                specific_error_from_string.code != APIErrorCode.EXCHANGE_SPECIFIC.value
                and error_body
            ):
                return specific_error_from_string  # Use if string mapping was specific and body wasn't empty

            # If error_body is empty, provide a clearer default message for these statuses
            message = error_body if error_body else "Authentication failed"
            return APIError(
                message=message,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                http_status=status_code,
                exchange_message=error_body,
            )

        extracted_message = error_body
        category = HyperliquidAPIErrorCategory.UNKNOWN
        exchange_specific_code: str | None = None

        if error_data:
            try:
                # Use the full path for clarity
                error_obj = HyperliquidRawApiError.model_validate(error_data)
                extracted_message = error_obj.error
                # Use HyperliquidErrorMapper to call static method
                category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)
            except ValidationError:
                logger.warning(
                    f"Failed to validate Hyperliquid error response_data: {error_data}. "
                    f"Falling back to error_body: '{error_body}'"
                )
                category = HyperliquidErrorMapper._categorize_hyperliquid_error(error_body)
        else:
            # If no error_data, Hyperliquid often just sends a plain string error_body
            # or sometimes a JSON array where the first element is a string like ["error string"]
            # Try to parse if it looks like a list of strings
            import json

            try:
                potential_list = json.loads(error_body)
                if (
                    isinstance(potential_list, list)
                    and potential_list
                    and isinstance(potential_list[0], str)
                ):
                    extracted_message = potential_list[0]
                    logger.debug(f"Extracted error message from JSON list: {extracted_message}")
            except json.JSONDecodeError:
                pass  # Not a JSON list, use error_body as is

            category = HyperliquidErrorMapper._categorize_hyperliquid_error(extracted_message)

        if not extracted_message:
            extracted_message = "Unknown Hyperliquid error"

        api_error_code = HyperliquidErrorMapper._map_category_to_api_error_code(category)

        # For HyperLiquid, the `exchange_code` is less structured, we use the category for now
        # if it's specific, or leave it None.
        if (
            category != HyperliquidAPIErrorCategory.UNKNOWN
            and category != HyperliquidAPIErrorCategory.ERROR
        ):
            exchange_specific_code = category.name  # Use the enum member name as a code

        api_err_response_obj = APIErrorResponse.from_exchange_error(
            message=extracted_message,
            code=api_error_code.value,
            http_status=status_code,
            exchange_code=exchange_specific_code,  # Pass the derived exchange_specific_code
            exchange_message=extracted_message,
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
            original_exception=None,  # Not part of interface
        )
