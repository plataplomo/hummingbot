"""Centralized response validation utilities for type-safe API handling.

This module provides exchange-agnostic validation functions that ensure
type safety and security at service boundaries while maintaining the
architectural separation between HTTP layer and exchange-specific logic.
"""

from typing import Any, TypeVar

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


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
        logger.error(
            "response_validation_null_response",
            action="validate_dict_response",
            message="Null response received - security violation",
            context=context,
            security_alert=True,
        )
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
    logger.debug(
        "response_validation_dict_success",
        action="validate_dict_response",
        message="Successfully validated dictionary response",
        context=context,
        key_count=len(response),
    )
    return response


def ensure_list_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> list[Any]:
    """Validate that response is a list with consistent error handling."""
    if response is None:
        logger.error(
            "response_validation_null_response",
            action="validate_list_response",
            message="Null response received - security violation",
            context=context,
            security_alert=True,
        )
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

    logger.debug(
        "response_validation_list_success",
        action="validate_list_response",
        message="Successfully validated list response",
        context=context,
        item_count=len(response),
    )
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
        logger.error(
            "response_validation_missing_fields",
            action="validate_required_fields",
            message="Missing required fields - security violation",
            context=context,
            missing_fields=missing_fields,
            security_alert=True,
        )
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
        logger.error(
            "response_validation_null_response",
            action="validate_string_response",
            message="Null response received - security violation",
            context=context,
            security_alert=True,
        )
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
        logger.warning(
            "response_validation_large_string",
            action="validate_string_response",
            message="Large string response detected - potential DoS risk",
            context=context,
            string_length=len(response),
            security_alert=True,
        )

    return response


def validate_response_not_empty(
    response: dict[str, Any] | list[Any],
    context: str,
    status_code: int,
) -> None:
    """Validate that a response container is not empty."""
    if not response:
        logger.warning(
            "response_validation_empty_response",
            action="validate_response_not_empty",
            message="Empty response received",
            context=context,
        )
        raise APIError(
            message=f"Empty response for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )
