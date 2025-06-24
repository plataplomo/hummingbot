"""Centralized response validation utilities for type-safe API handling.

This module provides exchange-agnostic validation functions that ensure
type safety and security at service boundaries while maintaining the
architectural separation between HTTP layer and exchange-specific logic.
"""

from typing import Any, TypeVar

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger


logger = get_logger(__name__)
T = TypeVar("T")


def ensure_dict_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> dict[str, Any]:
    """Validate that response is a dictionary with consistent error handling.

    Args:
        response: Raw response from HTTP client
        context: Description for error messages (e.g., "ticker (BTC-USD)")
        status_code: HTTP status code for error context

    Returns:
        Validated dictionary response

    Raises:
        APIError: If response is None or not a dictionary
    """
    if response is None:
        logger.error(f"SECURITY: Null response for {context}")
        raise APIError(
            message=f"No data received for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(response, dict):
        logger.error(
            f"SECURITY: Type mismatch for {context} - expected dict, got {type(response).__name__}"
        )
        raise APIError(
            message=(
                f"Unexpected {context} response format: expected dict, "
                f"got {type(response).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    # Log successful validation for audit
    logger.debug(f"Validated dict response for {context} with {len(response)} keys")
    return response


def ensure_list_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> list[Any]:
    """Validate that response is a list with consistent error handling."""
    if response is None:
        logger.error(f"SECURITY: Null response for {context}")
        raise APIError(
            message=f"No data received for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(response, list):
        logger.error(
            f"SECURITY: Type mismatch for {context} - expected list, got {type(response).__name__}"
        )
        raise APIError(
            message=(
                f"Unexpected {context} response format: expected list, "
                f"got {type(response).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    logger.debug(f"Validated list response for {context} with {len(response)} items")
    return response


def validate_required_fields(
    response: dict[str, Any],
    required_fields: list[str],
    context: str,
    status_code: int,
) -> None:
    """Validate that a dictionary contains required fields."""
    missing_fields = [field for field in required_fields if field not in response]

    if missing_fields:
        logger.error(f"SECURITY: Missing required fields in {context}: {missing_fields}")
        raise APIError(
            message=(f"Missing required fields in {context} response: {', '.join(missing_fields)}"),
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )


def ensure_string_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> str:
    """Validate that response is a string with consistent error handling."""
    if response is None:
        logger.error(f"SECURITY: Null response for {context}")
        raise APIError(
            message=f"No data received for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(response, str):
        logger.error(
            f"SECURITY: Type mismatch for {context} - expected str, got {type(response).__name__}"
        )
        raise APIError(
            message=(
                f"Unexpected {context} response format: expected str, got {type(response).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    # Check for suspiciously large strings (potential DoS)
    if len(response) > 1_000_000:  # 1MB limit
        logger.warning(f"SECURITY: Large string response for {context}: {len(response)} chars")

    return response


def validate_response_not_empty(
    response: dict[str, Any] | list[Any],
    context: str,
    status_code: int,
) -> None:
    """Validate that a response container is not empty."""
    if not response:
        logger.warning(f"Empty response for {context}")
        raise APIError(
            message=f"Empty response for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )
