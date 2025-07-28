"""Base Response Handler for Hyperliquid API.

This module provides common functionality shared across all Hyperliquid response handlers.
"""

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


class HyperliquidResponseHandlerBase:
    """Base class providing common functionality for Hyperliquid response handlers."""

    @staticmethod
    def _handle_validation_error(
        e: ValidationError,
        context: str,
        raw_data: RawJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> APIError:
        """Helper to create a standardized APIError from a ValidationError.

        Returns:
            APIError with INVALID_RESPONSE code and structured logging of validation failure.
        """
        log_message = (
            "[HyperliquidResponseHandler] Pydantic validation failed for %s: %s. "
            "Status: %s. Headers: %s. Raw data: %r"
        )
        logger.error(
            "pydantic_validation_failed",
            action="validate_response",
            context=context,
            validation_error=str(e),
            status_code=status_code,
            headers=headers,
            raw_data=repr(raw_data),
            message=log_message,
            message_args=(
                context,
                str(e),
                status_code if status_code is not None else "N/A",
                headers if headers is not None else "N/A",
                raw_data,
            ),
        )
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
            http_status=status_code,
        )
