"""Trading-related exceptions for CyberDelta.

These exceptions handle trading operation errors including order placement,
execution, balance checks, and market availability.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


class TradingError(APIError):
    """Base class for trading-related errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize trading error.

        Args:
            message: Human-readable error description
            code: Error code (defaults to UNKNOWN if not provided)
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class InvalidBatchResponseError(TradingError):
    """Batch operation response is invalid or missing expected data."""

    def __init__(
        self,
        operation: str,
        expected_data: str,
        *,
        response_data: object = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize invalid batch response error.

        Args:
            operation: The batch operation that failed
            expected_data: Description of expected data that was missing
            response_data: The actual response data received
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            original_exception: The underlying exception
        """
        message = f"Invalid {operation} response: missing {expected_data}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata={
                "operation": operation,
                "expected_data": expected_data,
                "response_data": response_data,
            },
            original_exception=original_exception,
        )


class OrderError(TradingError):
    """Base class for order-related errors."""

    def __init__(
        self,
        message: str,
        *,
        order_id: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        order_type: str | None = None,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize order error with order context.

        Args:
            message: Human-readable error description
            order_id: Optional order identifier
            symbol: Trading symbol
            side: Order side (buy/sell)
            order_type: Type of order
            code: Error code
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            original_exception: The underlying exception
        """
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.order_type = order_type

        # Build metadata
        metadata: dict[str, Any] = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
        }

        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class OrderSizeError(OrderError):
    """Raised when order size is invalid."""

    def __init__(
        self,
        size: float | str,
        min_size: float | str | None = None,
        max_size: float | str | None = None,
        symbol: str | None = None,
    ) -> None:
        """Initialize order size error.

        Args:
            size: The invalid order size
            min_size: Minimum allowed size
            max_size: Maximum allowed size
            symbol: Trading symbol
        """
        self.size = size
        self.min_size = min_size
        self.max_size = max_size

        if min_size is not None and max_size is not None:
            message = f"Order size {size} must be between {min_size} and {max_size}"
        elif min_size is not None:
            message = f"Order size {size} below minimum {min_size}"
        elif max_size is not None:
            message = f"Order size {size} above maximum {max_size}"
        else:
            message = f"Order size {size} is invalid"

        if symbol:
            message = f"{message} for {symbol}"

        super().__init__(
            message=message,
            symbol=symbol,
            code=APIErrorCode.INVALID_ORDER_SIZE.value,
        )


class InsufficientBalanceError(TradingError):
    """Raised when account has insufficient balance for an operation."""

    def __init__(
        self,
        required: float | str,
        available: float | str,
        currency: str,
        operation: str = "order",
    ) -> None:
        """Initialize insufficient balance error.

        Args:
            required: Amount required
            available: Amount available
            currency: Currency symbol
            operation: Type of operation attempted
        """
        self.required = required
        self.available = available
        self.currency = currency
        self.operation = operation

        super().__init__(
            message=(
                f"Insufficient {currency} balance for {operation}: "
                f"required {required}, available {available}"
            ),
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
            metadata={
                "required": required,
                "available": available,
                "currency": currency,
                "operation": operation,
            },
        )


class OrderNotFoundError(OrderError):
    """Raised when an order cannot be found."""

    def __init__(
        self,
        order_id: str,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize order not found error.

        Args:
            order_id: The order identifier
            symbol: Optional trading symbol
            exchange: Optional exchange name
        """
        message = f"Order {order_id} not found"
        if exchange:
            message = f"{message} on {exchange}"

        super().__init__(
            message=message,
            order_id=order_id,
            symbol=symbol,
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            exchange_code="ORDER_NOT_FOUND",
        )


class MarketClosedError(TradingError):
    """Raised when attempting to trade in a closed market."""

    def __init__(
        self,
        symbol: str,
        market_state: str | None = None,
        next_open: str | None = None,
        exchange: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize market closed error.

        Args:
            symbol: Trading symbol
            market_state: Current market state
            next_open: When market will open next
            exchange: Optional exchange name
            reason: Optional specific reason for market being closed
        """
        self.symbol = symbol
        self.market_state = market_state
        self.next_open = next_open
        self.exchange = exchange
        self.reason = reason

        # Build message based on available information
        if reason:
            message = f"Market operation failed for {symbol}: {reason}"
        else:
            message = f"Market closed for {symbol}"

        if market_state:
            message = f"{message} (state: {market_state})"
        if next_open:
            message = f"{message}, opens at {next_open}"
        if exchange:
            message = f"{message} on {exchange}"

        super().__init__(
            message=message,
            code=APIErrorCode.MARKET_CLOSED.value,
            metadata={
                "symbol": symbol,
                "market_state": market_state,
                "next_open": next_open,
                "exchange": exchange,
                "reason": reason,
            },
        )


class PositionNotFoundError(TradingError):
    """Raised when a position cannot be found."""

    def __init__(
        self,
        position_id: str | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize position not found error.

        Args:
            position_id: Optional position identifier
            symbol: Optional trading symbol
            exchange: Optional exchange name
        """
        self.position_id = position_id
        self.symbol = symbol
        self.exchange = exchange

        if position_id:
            message = f"Position {position_id} not found"
        elif symbol:
            message = f"No position found for {symbol}"
        else:
            message = "Position not found"

        if exchange:
            message = f"{message} on {exchange}"

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific position error code available
            metadata={
                "position_id": position_id,
                "symbol": symbol,
                "exchange": exchange,
            },
        )
