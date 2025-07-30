"""Service interfaces for ExecutionHandler refactoring.

This module defines the contracts for all services extracted from the monolithic
ExecutionHandler. These interfaces support dependency injection and testability.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from cyberdelta.apis.common import APIError
from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.models import Order, Trade
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


if TYPE_CHECKING:
    from cyberdelta.core.models.execution import ExecutionStatus, TradeExecution


class ExecutionErrorType(Enum):
    """Standardized execution error categories."""

    VALIDATION_ERROR = "validation_error"
    API_ERROR = "api_error"
    CIRCUIT_BREAKER_ERROR = "circuit_breaker_error"
    COMPENSATION_ERROR = "compensation_error"
    TIMEOUT_ERROR = "timeout_error"
    SYSTEM_ERROR = "system_error"
    NETWORK_ERROR = "network_error"
    AUTHENTICATION_ERROR = "authentication_error"


@dataclass
class ExecutionError:
    """Standardized error information with context."""

    error_type: ExecutionErrorType
    message: str
    details: dict[str, Any]
    recoverable: bool
    retry_suggested: bool
    exchange_id: str | None = None
    order_id: str | None = None
    original_exception: Exception | None = None

    def __str__(self) -> str:
        """Return string representation."""
        return f"{self.error_type.value}: {self.message}"


@dataclass
class ExecutionResult:
    """Standardized result wrapper for all service operations."""

    success: bool
    data: Any = None
    error: ExecutionError | None = None

    @classmethod
    def success_result(cls, data: object = None) -> ExecutionResult:
        """Create a successful result.

        Args:
            data: Optional data to include in the result

        Returns:
            ExecutionResult: A successful execution result
        """
        return cls(success=True, data=data)

    @classmethod
    def error_result(cls, error: ExecutionError) -> ExecutionResult:
        """Create an error result.

        Args:
            error: The execution error to wrap

        Returns:
            ExecutionResult: An error execution result
        """
        return cls(success=False, error=error)

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        error_type: ExecutionErrorType,
        context: str = "",
        recoverable: bool = False,
    ) -> ExecutionResult:
        """Create error result from an exception.

        Args:
            exc: The exception that occurred
            error_type: The type of execution error
            context: Additional context for the error
            recoverable: Whether the error is recoverable

        Returns:
            ExecutionResult: An error execution result from the exception
        """
        error = ExecutionError(
            error_type=error_type,
            message=f"{context}: {exc!s}" if context else str(exc),
            details={"exception_type": type(exc).__name__},
            recoverable=recoverable,
            retry_suggested=recoverable,
            original_exception=exc,
        )
        return cls.error_result(error)


@dataclass
class OrderRequest:
    """Request for order placement operations."""

    exchange_id: str
    symbol: str
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    post_only: bool = False
    client_order_id: str | None = None


@dataclass
class OrderResult:
    """Result of order operations."""

    order: Order | None
    success: bool
    error_message: str | None = None


@dataclass
class ValidationResult:
    """Result of input validation operations."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]


# Service Interfaces


@runtime_checkable
class IOrderService(Protocol):
    """Interface for order management operations."""

    async def place_order_with_retry(self, request: OrderRequest) -> ExecutionResult:
        """Place order with retry logic and error handling."""
        ...

    async def get_order_status(
        self,
        order_id: str,
        exchange_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionResult:
        """Get order status with proper error handling."""
        ...

    async def cancel_order(
        self,
        order_id: str,
        exchange_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionResult:
        """Cancel an existing order."""
        ...

    async def monitor_order_until_terminal(
        self,
        order_id: str,
        exchange_id: str,
        timeout_seconds: float = 60.0,
        poll_interval: float = 2.0,
    ) -> ExecutionResult:
        """Monitor order until it reaches a terminal state."""
        ...


@runtime_checkable
class IPortfolioService(Protocol):
    """Interface for portfolio management operations."""

    async def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """Process a completed trade for portfolio tracking."""
        ...

    async def get_account_balances(self, exchange_id: str) -> ExecutionResult:
        """Get account balances for validation."""
        ...

    async def get_available_balance(self, exchange_id: str, asset: str) -> ExecutionResult:
        """Get available balance for a specific asset."""
        ...


@runtime_checkable
class ICircuitBreakerService(Protocol):
    """Interface for circuit breaker operations."""

    def can_execute(self, exchange_id: str) -> tuple[bool, str]:
        """Check if execution is allowed for an exchange."""
        ...

    def record_api_error(self, exchange_id: str, error_code: str) -> None:
        """Record an API error for circuit breaker tracking."""
        ...

    def record_api_success(self, exchange_id: str, context: str = "") -> None:
        """Record a successful API operation."""
        ...

    def reset_breaker(self, exchange_id: str) -> None:
        """Reset circuit breaker for an exchange."""
        ...


@runtime_checkable
class ISymbolMapper(Protocol):
    """Comprehensive interface for symbol mapping operations."""

    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str:
        """Get exchange-specific symbol from internal symbol.

        Raises:
            SymbolMappingError: If symbol not found or invalid parameters.
        """
        ...

    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str:
        """Get internal symbol from exchange-specific symbol.

        Raises:
            SymbolMappingError: If symbol not found or invalid parameters.
        """
        ...

    def get_all_internal_symbols(self) -> list[str]:
        """Get all configured internal symbols."""
        ...

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """Get all exchange symbols for an internal symbol.

        Raises:
            SymbolMappingError: If internal symbol not found.
        """
        ...

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """Get all internal symbols for an exchange.

        Raises:
            SymbolMappingError: If exchange not found.
        """
        ...

    def is_symbol_supported(self, internal_symbol: str, exchange_id: str) -> bool:
        """Check if symbol is supported on exchange."""
        ...

    def validate_symbol_pair(
        self, internal_symbol: str, long_exchange: str, short_exchange: str
    ) -> None:
        """Validate symbol is available on both exchanges for arbitrage.

        Raises:
            SymbolMappingError: If symbol not available on either exchange.
        """
        ...

    def get_symbol_coverage(self, internal_symbol: str) -> dict[str, bool]:
        """Get symbol availability across all configured exchanges."""
        ...

    def get_supported_exchanges(self) -> list[str]:
        """Get list of all supported exchange IDs."""
        ...


@runtime_checkable
class IInputValidator(Protocol):
    """Interface for input validation operations."""

    async def validate_execution_request(self, opportunity: SizedOpportunity) -> ValidationResult:
        """Validate execution request before processing."""
        ...

    def validate_order_request(self, request: OrderRequest) -> ValidationResult:
        """Validate order request parameters."""
        ...


@runtime_checkable
class IStateManager(Protocol):
    """Interface for execution state management."""

    async def create_execution(self, opportunity: SizedOpportunity) -> TradeExecution:
        """Create new execution with proper initialization."""
        ...

    async def update_execution_status(
        self, execution_id: str, status: ExecutionStatus, error_message: str | None = None
    ) -> bool:
        """Update execution status in a thread-safe manner."""
        ...

    async def get_execution(self, execution_id: str) -> TradeExecution | None:
        """Get execution by ID."""
        ...

    async def finalize_execution(self, execution_id: str) -> TradeExecution | None:
        """Move execution from active to history."""
        ...

    def get_active_executions(self) -> list[TradeExecution]:
        """Get all active executions."""
        ...


@runtime_checkable
class ICompensationService(Protocol):
    """Interface for position compensation operations."""

    async def compensate_position(
        self, execution: TradeExecution, failed_leg: str, quantity_to_compensate: Decimal
    ) -> ExecutionResult:
        """Attempt to compensate a failed position."""
        ...

    async def monitor_compensation_order(
        self,
        compensation_id: str,
        order_id: str,
        exchange_id: str,
        target_quantity: Decimal,
        timeout_seconds: float = 300.0,
    ) -> ExecutionResult:
        """Monitor compensation order until completion."""
        ...


@runtime_checkable
class IErrorHandler(Protocol):
    """Interface for centralized error handling."""

    async def handle_api_error(
        self, error: APIError, context: str, exchange_id: str
    ) -> ExecutionResult:
        """Handle API errors with proper logging and circuit breaker updates."""
        ...

    async def handle_validation_error(
        self, message: str, details: dict[str, Any]
    ) -> ExecutionResult:
        """Handle validation errors."""
        ...

    async def handle_timeout_error(
        self, operation: str, timeout_seconds: float, context: dict[str, Any] | None = None
    ) -> ExecutionResult:
        """Handle timeout errors."""
        ...

    async def handle_system_error(
        self, exception: Exception, context: str, recoverable: bool = False
    ) -> ExecutionResult:
        """Handle system/unexpected errors."""
        ...


@runtime_checkable
class IAlertService(Protocol):
    """Interface for alert/notification operations."""

    async def send_critical_alert(
        self, title: str, message: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Send critical alert for manual intervention."""
        ...

    async def send_warning_alert(
        self, title: str, message: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Send warning alert."""
        ...


# Abstract base classes for concrete implementations


class BaseService:
    """Base class for all services with common functionality."""

    def __init__(self, logger: TraceLevelLogger | None = None) -> None:
        """Initialize base service."""
        self.logger = logger or self._get_default_logger()

    def _get_default_logger(self) -> Any:  # noqa: ANN401
        """Get default logger instance.

        Returns:
            Any: Logger instance for this service class
        """
        return get_logger(self.__class__.__name__)


class BaseAsyncService(BaseService):
    """Base class for async services with lifecycle management."""

    def __init__(self, logger: TraceLevelLogger | None = None) -> None:
        """Initialize async service."""
        super().__init__(logger)
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task[Any]] = set()

    async def start(self) -> None:
        """Start the service."""
        self._shutdown_event.clear()

    async def stop(self) -> None:
        """Stop the service and cleanup."""
        self._shutdown_event.set()

        # Cancel all background tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()

    def _create_background_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Create and track a background task.

        Args:
            coro: The coroutine to run as a background task

        Returns:
            asyncio.Task[Any]: The created and tracked background task
        """
        task: asyncio.Task[Any] = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task


# Service configuration dataclasses


@dataclass
class OrderServiceConfig:
    """Configuration for order management service."""

    max_retries: int = 3
    retry_delay_base_seconds: float = 1.0
    max_retry_delay_seconds: float = 30.0
    order_timeout_seconds: float = 60.0
    monitoring_poll_interval_seconds: float = 2.0


@dataclass
class StateManagerConfig:
    """Configuration for execution state manager."""

    max_execution_history: int = 100
    cleanup_interval_seconds: float = 3600.0  # 1 hour
    max_execution_age_hours: float = 24.0


@dataclass
class CompensationConfig:
    """Configuration for compensation service."""

    monitor_timeout_seconds: float = 300.0  # 5 minutes
    use_limit_orders: bool = True
    limit_price_offset_pct: Decimal = Decimal("0.001")  # 0.1%
    partial_fill_threshold_pct: Decimal = Decimal("0.95")  # 95%


@dataclass
class ValidationConfig:
    """Configuration for input validation."""

    max_opportunity_age_seconds: float = 60.0
    min_position_size_usd: Decimal = Decimal("10.0")
    max_position_size_usd: Decimal = Decimal("10000.0")
    max_size_imbalance_pct: float = 5.0  # 5%
    required_balance_buffer_pct: Decimal = Decimal("0.1")  # 10%


__all__ = [
    "BaseAsyncService",
    "BaseService",
    "CompensationConfig",
    "ExecutionError",
    "ExecutionErrorType",
    "ExecutionResult",
    "IAlertService",
    "ICircuitBreakerService",
    "ICompensationService",
    "IErrorHandler",
    "IInputValidator",
    "IOrderService",
    "IPortfolioService",
    "IStateManager",
    "ISymbolMapper",
    "OrderRequest",
    "OrderResult",
    "OrderServiceConfig",
    "StateManagerConfig",
    "ValidationConfig",
    "ValidationResult",
]
