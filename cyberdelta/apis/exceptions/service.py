"""Service-related exceptions for CyberDelta.

These exceptions handle service-level errors including parameter validation,
operation failures, and service-specific errors.
"""

from datetime import UTC, datetime

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode


class ServiceParameterError(APIError):
    """Consolidated exception for service parameter validation errors.

    Replaces multiple specific parameter exceptions with a rich, context-aware exception.
    """

    def __init__(
        self,
        parameter: str,
        issue: str,
        value: object = None,
        exchange: str | None = None,
        operation: str | None = None,
        expected_type: str | None = None,
        suggestion: str | None = None,
    ) -> None:
        """Initialize service parameter error with rich context.

        Args:
            parameter: Name of the parameter that failed validation
            issue: Description of what's wrong with the parameter
            value: The actual value that was provided (if any)
            exchange: Exchange name where the error occurred
            operation: Operation being performed when error occurred
            expected_type: What type/format was expected
            suggestion: Helpful suggestion for fixing the issue
        """
        self.parameter = parameter
        self.issue = issue
        self.value = value
        self.exchange = exchange
        self.operation = operation
        self.expected_type = expected_type
        self.suggestion = suggestion

        # Build rich error message
        message = f"Parameter '{parameter}' {issue}"
        if value is not None:
            message += f" (got: {value})"
        if expected_type:
            message += f" (expected: {expected_type})"
        if exchange:
            message = f"[{exchange}] {message}"
        if operation:
            message += f" during {operation}"
        if suggestion:
            message += f". {suggestion}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="PARAMETER_ERROR",
            metadata={
                "parameter": parameter,
                "issue": issue,
                "value": str(value) if value is not None else None,
                "exchange": exchange,
                "operation": operation,
                "expected_type": expected_type,
                "suggestion": suggestion,
                "timestamp": datetime.now(tz=UTC).isoformat(),
            },
        )
