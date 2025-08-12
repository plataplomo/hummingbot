"""Trading-related exceptions for CyberDelta.

These exceptions handle trading operation errors including order placement,
execution, balance checks, and market availability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode


if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName


class InvalidBatchResponseError(APIError):
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


class OrderError(APIError):
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


class OrderNotFoundError(OrderError):
    """Raised when an order cannot be found."""

    def __init__(
        self,
        order_id: str,
        symbol: str | None = None,
        exchange: ExchangeName | None = None,
    ) -> None:
        """Initialize order not found error.

        Args:
            order_id: The order identifier
            symbol: Optional trading symbol
            exchange: Optional exchange enum value
        """
        message = f"Order {order_id} not found"
        if exchange:
            message = f"{message} on {exchange.value}"

        super().__init__(
            message=message,
            order_id=order_id,
            symbol=symbol,
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            exchange_code="ORDER_NOT_FOUND",
        )


class MarketClosedError(APIError):
    """Raised when attempting to trade in a closed market."""

    def __init__(
        self,
        symbol: str,
        market_state: str | None = None,
        next_open: str | None = None,
        exchange: ExchangeName | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize market closed error.

        Args:
            symbol: Trading symbol
            market_state: Current market state
            next_open: When market will open next
            exchange: Optional exchange enum value
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
            message = f"{message} on {exchange.value}"

        super().__init__(
            message=message,
            code=APIErrorCode.MARKET_CLOSED.value,
            metadata={
                "symbol": symbol,
                "market_state": market_state,
                "next_open": next_open,
                "exchange": exchange.value if exchange else None,
                "reason": reason,
            },
        )
