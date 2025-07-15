"""Standardized error handling service for ExecutionHandler refactoring.

This module provides centralized error handling with proper logging, circuit breaker
integration, and standardized error classification for all execution operations.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common import APIError, TransformationError
from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.services.interfaces import (
    BaseService,
    ExecutionError,
    ExecutionErrorType,
    ExecutionResult,
    IErrorHandler,
)


if TYPE_CHECKING:
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


class ExecutionErrorHandler(BaseService, IErrorHandler):
    """Centralized error handling service with logging and circuit breaker integration."""

    def __init__(
        self,
        circuit_breaker: CircuitBreakerSystem | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize error handler with optional circuit breaker integration.

        Args:
            circuit_breaker: Optional circuit breaker system for integration
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.circuit_breaker = circuit_breaker
        self.logger = logger or get_logger(__name__)

    async def handle_api_error(
        self, error: APIError, context: str, exchange_id: str
    ) -> ExecutionResult:
        """Handle API errors with proper logging and circuit breaker updates.

        Args:
            error: The API error that occurred
            context: Context description of the operation
            exchange_id: Exchange identifier for circuit breaker tracking

        Returns:
            ExecutionResult with standardized error information
        """
        # Use rich APIError metadata for logging
        self.logger.error(
            "API error occurred",
            exchange_id=exchange_id,
            error_code=error.code,
            error_message=error.message,
            http_status=error.http_status,
            exchange_code=error.exchange_code,
            exchange_message=error.exchange_message,
            retry_after=error.retry_after,
            is_retryable=error.is_retryable,
            context=context,
            structured_metadata=error.metadata,
        )

        # Update circuit breaker if available
        if self.circuit_breaker:
            self.circuit_breaker.record_api_error(exchange_id, str(error.code))

        # Use built-in retry logic from APIError
        execution_error = ExecutionError(
            error_type=ExecutionErrorType.API_ERROR,
            message=f"API error in {context}: {error.message}",
            details={
                "error_code": error.code,
                "exchange_id": exchange_id,
                "context": context,
                "http_status": error.http_status,
                "exchange_code": error.exchange_code,
                "exchange_message": error.exchange_message,
                "retry_after": error.retry_after,
                "is_retryable": error.is_retryable,
                "structured_metadata": error.metadata or {},
            },
            recoverable=error.is_retryable,  # Use built-in logic
            retry_suggested=error.is_retryable,
            exchange_id=exchange_id,
            original_exception=error,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_transformation_error(
        self, error: TransformationError, context: str
    ) -> ExecutionResult:
        """Handle transformation errors with rich metadata extraction.

        Args:
            error: The transformation error that occurred
            context: Context description of the operation

        Returns:
            ExecutionResult with standardized error information
        """
        # Use rich TransformationError metadata for logging
        self.logger.error(
            "Transformation error occurred",
            field_name=error.field_name,
            source_value=str(error.source_value) if error.source_value is not None else None,
            transformation_code=error.code,
            context=context,
            source_data=error.source_data,
            error_message=str(error),
        )

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.VALIDATION_ERROR,  # Transformation is validation
            message=f"Data transformation failed in {context}: {error}",
            details={
                "field_name": error.field_name,
                "source_value": str(error.source_value) if error.source_value is not None else None,
                "source_data": error.source_data or {},
                "transformation_code": error.code,
                "context": context,
            },
            recoverable=False,  # Transformation errors are typically not recoverable
            retry_suggested=False,
            original_exception=error,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_validation_error(
        self, message: str, details: dict[str, Any]
    ) -> ExecutionResult:
        """Handle validation errors with proper logging.

        Args:
            message: Error message
            details: Additional error details

        Returns:
            ExecutionResult with validation error information
        """
        self.logger.warning("Validation error occurred", message=message, details=details)

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.VALIDATION_ERROR,
            message=message,
            details=details,
            recoverable=False,
            retry_suggested=False,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_timeout_error(
        self, operation: str, timeout_seconds: float, context: dict[str, Any] | None = None
    ) -> ExecutionResult:
        """Handle timeout errors with proper logging.

        Args:
            operation: Name of the operation that timed out
            timeout_seconds: Timeout duration that was exceeded
            context: Optional context information

        Returns:
            ExecutionResult with timeout error information
        """
        context = context or {}

        self.logger.error(
            "Operation timeout occurred",
            operation=operation,
            timeout_seconds=timeout_seconds,
            context=context,
        )

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.TIMEOUT_ERROR,
            message=f"Operation '{operation}' timed out after {timeout_seconds}s",
            details={
                "operation": operation,
                "timeout_seconds": timeout_seconds,
                "context": context,
            },
            recoverable=True,
            retry_suggested=True,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_system_error(
        self, exception: Exception, context: str, recoverable: bool = False
    ) -> ExecutionResult:
        """Handle system/unexpected errors with comprehensive logging.

        Args:
            exception: The exception that occurred
            context: Context description of the operation
            recoverable: Whether the error is potentially recoverable

        Returns:
            ExecutionResult with system error information
        """
        self.logger.error(
            "System error occurred",
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            context=context,
            recoverable=recoverable,
            traceback=traceback.format_exc(),
        )

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.SYSTEM_ERROR,
            message=f"System error in {context}: {exception}",
            details={
                "exception_type": type(exception).__name__,
                "context": context,
                "traceback": traceback.format_exc(),
            },
            recoverable=recoverable,
            retry_suggested=recoverable,
            original_exception=exception,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_circuit_breaker_error(
        self, exchange_id: str, reason: str, context: str
    ) -> ExecutionResult:
        """Handle circuit breaker errors with proper logging.

        Args:
            exchange_id: Exchange identifier
            reason: Reason for circuit breaker trip
            context: Context description

        Returns:
            ExecutionResult with circuit breaker error information
        """
        self.logger.warning(
            "Circuit breaker tripped", exchange_id=exchange_id, reason=reason, context=context
        )

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.CIRCUIT_BREAKER_ERROR,
            message=f"Circuit breaker tripped for {exchange_id}: {reason}",
            details={
                "exchange_id": exchange_id,
                "reason": reason,
                "context": context,
            },
            recoverable=True,
            retry_suggested=False,  # Don't retry until circuit breaker resets
            exchange_id=exchange_id,
        )

        return ExecutionResult.error_result(execution_error)

    async def handle_compensation_error(
        self,
        execution_id: str,
        failed_leg: str,
        error_message: str,
        details: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Handle compensation-related errors with proper logging.

        Args:
            execution_id: Execution identifier
            failed_leg: Which leg failed (long/short)
            error_message: Error description
            details: Additional error details

        Returns:
            ExecutionResult with compensation error information
        """
        details = details or {}

        self.logger.critical(
            "Compensation error occurred",
            execution_id=execution_id,
            failed_leg=failed_leg,
            error_message=error_message,
            details=details,
        )

        execution_error = ExecutionError(
            error_type=ExecutionErrorType.COMPENSATION_ERROR,
            message=f"Compensation failed for {failed_leg} leg: {error_message}",
            details={
                "execution_id": execution_id,
                "failed_leg": failed_leg,
                "error_message": error_message,
                **details,
            },
            recoverable=False,
            retry_suggested=False,
        )

        return ExecutionResult.error_result(execution_error)

    async def create_success_result(self, data: object = None) -> ExecutionResult:
        """Create a successful execution result.

        Args:
            data: Optional result data

        Returns:
            ExecutionResult indicating success
        """
        return ExecutionResult.success_result(data)

    async def log_operation_success(
        self, operation: str, exchange_id: str | None = None, context: dict[str, Any] | None = None
    ) -> None:
        """Log successful operation for monitoring and circuit breaker updates.

        Args:
            operation: Name of the successful operation
            exchange_id: Optional exchange identifier
            context: Optional context information
        """
        context = context or {}

        self.logger.info(
            "Operation completed successfully",
            operation=operation,
            exchange_id=exchange_id,
            context=context,
        )

        # Update circuit breaker for successful operations
        if self.circuit_breaker and exchange_id:
            self.circuit_breaker.record_api_success(exchange_id, operation)
