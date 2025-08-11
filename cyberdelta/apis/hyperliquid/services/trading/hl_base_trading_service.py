"""Base trading service for Hyperliquid.

This module provides shared functionality for all Hyperliquid trading services,
including common error handling patterns and utility methods.
"""

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


logger = get_logger(__name__)


class HyperliquidBaseTradingService:
    """Base class for Hyperliquid trading services.

    Provides shared error handling and common utility methods
    that are used across multiple trading services.
    """

    def __init__(self, exchange_name: ExchangeName = ExchangeName.HYPERLIQUID) -> None:
        """Initialize base trading service.

        Args:
            exchange_name: Name of the exchange (default: ExchangeName.HYPERLIQUID)
        """
        self._exchange_name = exchange_name.value

    def _handle_service_error(
        self,
        error: Exception,
        current_method: str,
        context: str,
        status_code: int = 0,
        raw_response_content: str | None = None,
    ) -> APIError:
        """Handle service errors in a standardized way.

        Args:
            error: The original exception
            current_method: Name of the method where error occurred
            context: Context description
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available

        Returns:
            APIError instance with appropriate error details
        """
        if isinstance(error, APIError):
            # Re-raise APIErrors (like SYMBOL_NOT_FOUND) to preserve their specific error codes
            logger.debug(
                "api_error_propagated",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error_code=error.code,
                error_message=error.message,
                message="Propagating APIError: %s",
            )
            return error

        if isinstance(error, TransformationError):
            logger.error(
                "transformation_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Failed to transform exchange data for %s: %s",
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        if isinstance(error, ValidationError):
            logger.error(
                "validation_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Internal data validation failed for %s: %s",
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        if isinstance(error, (ValueError, TypeError)):
            # Distinguish input validation from internal errors
            error_msg = str(error)
            if current_method in error_msg:
                # Re-raise input validation errors
                raise error

            logger.error(
                "service_logic_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Service internal logic error for %s: %s",
            )
            return APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        logger.error(
            "unexpected_service_failure",
            action=current_method,
            exchange=self._exchange_name,
            context=context,
            error=str(error),
            message="Unexpected service failure for %s: %s",
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
