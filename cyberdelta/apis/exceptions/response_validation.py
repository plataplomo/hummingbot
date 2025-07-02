"""Response validation exceptions for CyberDelta.

These exceptions handle validation errors for API responses,
including empty responses, invalid data formats, and missing fields.
"""

from cyberdelta.apis.common import APIError, APIErrorCode


class EmptyResponseError(APIError):
    """Raised when an empty response is received where data was expected."""

    def __init__(
        self,
        response_type: str,
        operation: str | None = None,
        http_status: int | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize empty response error.

        Args:
            response_type: Type of response that was empty
            operation: Operation that returned empty response
            http_status: HTTP status code
            exchange: Exchange name
        """
        message = f"Empty {response_type} received"
        if operation:
            message = f"{message} for {operation}"
        if exchange:
            message = f"[{exchange}] {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=http_status,
            metadata={
                "response_type": response_type,
                "expected_format": "non-empty data",
                "actual_data": None,
                "operation": operation,
                "exchange": exchange,
            },
        )


class InvalidLeverageError(APIError):
    """Raised when leverage value is outside acceptable range."""

    def __init__(
        self,
        leverage: int,
        min_leverage: int = 1,
        max_leverage: int = 100,
        symbol: str | None = None,
    ) -> None:
        """Initialize invalid leverage error.

        Args:
            leverage: The invalid leverage value
            min_leverage: Minimum allowed leverage
            max_leverage: Maximum allowed leverage
            symbol: Symbol being leveraged
        """
        message = (
            f"Invalid leverage value: {leverage}. "
            f"Must be between {min_leverage} and {max_leverage}."
        )
        if symbol:
            message = f"{message} (symbol: {symbol})"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "leverage": leverage,
                "min_leverage": min_leverage,
                "max_leverage": max_leverage,
                "symbol": symbol,
            },
        )


class NotImplementedOperationError(APIError):
    """Raised when an operation is not yet implemented."""

    def __init__(
        self,
        operation: str,
        service: str,
        exchange: str | None = None,
    ) -> None:
        """Initialize not implemented operation error.

        Args:
            operation: Operation that is not implemented
            service: Service class name
            exchange: Exchange name
        """
        message = f"{operation} not yet implemented in {service}"
        if exchange:
            message = f"[{exchange}] {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            metadata={
                "operation": operation,
                "service": service,
                "exchange": exchange,
            },
        )


class UnreachableCodeError(AssertionError):
    """Raised when code that should be unreachable is executed."""

    def __init__(self, reason: str = "all error handlers should raise exceptions") -> None:
        """Initialize unreachable code error.

        Args:
            reason: Reason why the code should be unreachable
        """
        super().__init__(f"Unreachable code: {reason}")
