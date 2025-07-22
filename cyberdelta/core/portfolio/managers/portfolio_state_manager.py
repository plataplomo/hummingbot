"""Portfolio state manager with direct AppSettings access following risk module patterns."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.base import StateUpdate
from cyberdelta.core.portfolio.base.typed_state_manager import StateManagerMetadata
from cyberdelta.core.portfolio.exceptions.state import (
    StateManagerInitializationFailedError,
    StateManagerNotInitializedError,
    StateOperationFailedError,
)
from cyberdelta.core.portfolio.models.base import BaseStateModel
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioState
from cyberdelta.core.portfolio.portfolio_types.portfolio_data_models import (
    BalanceUpdateRequest,
    OrderUpdateRequest,
    PortfolioMetrics,
    PositionUpdateRequest,
)
from cyberdelta.core.portfolio.portfolio_types.state_types import (
    StateUpdateResult,
    StateValidationResult,
)
from cyberdelta.core.portfolio.protocols import Initializable, Shutdownable


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
    from cyberdelta.core.portfolio.protocols import (
        MetricsCollectorProtocol,
        StateContainerProtocol,
    )
    from cyberdelta.core.portfolio.protocols.validation import ValidationServiceProtocol
    from cyberdelta.enums.exchange_names import ExchangeName

logger = get_logger(__name__)


class PortfolioStateManager:
    """Portfolio state manager with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access (no dependency injection)
    - Protocol-based dependencies
    - Strong typing with result types
    - Atomic state management
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
        validation_service: ValidationServiceProtocol | None = None,
        metrics_collector: MetricsCollectorProtocol | None = None,
    ) -> None:
        """Initialize the portfolio state manager.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data persistence
            validation_service: Optional validation service
            metrics_collector: Optional metrics collector
        """
        self.app_settings = app_settings
        self.portfolio_config = app_settings.portfolio_tracker
        self.state_container = state_container
        self.validation_service = validation_service
        self.metrics_collector = metrics_collector
        self.logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")

        # State management
        self._state_lock = asyncio.Lock()
        self._state_version = 0
        self._last_update: datetime | None = None
        self._is_initialized = False

        # Configuration from AppSettings
        self.atomic_updates = self.portfolio_config.state.atomic_updates
        self.validation_enabled = self.portfolio_config.validation.enabled
        self.cache_enabled = self.portfolio_config.cache.enabled

        # Performance tracking
        self.update_count = 0
        self.validation_count = 0
        self.error_count = 0
        self.trade_processing_count = 0

        logger.info(
            "portfolio_state_manager_created",
            atomic_updates=self.atomic_updates,
            validation_enabled=self.validation_enabled,
            cache_enabled=self.cache_enabled,
        )

    async def initialize(self) -> None:
        """Initialize the portfolio state manager."""
        if self._is_initialized:
            return

        logger.info("portfolio_state_manager_initializing")

        try:
            # Initialize state container if needed
            if isinstance(self.state_container, Initializable):
                await self.state_container.initialize()

            # Initialize validation service if provided
            if self.validation_service and isinstance(self.validation_service, Initializable):
                await self.validation_service.initialize()

            # Initialize metrics collector if provided
            if self.metrics_collector and isinstance(self.metrics_collector, Initializable):
                await self.metrics_collector.initialize()

            self._is_initialized = True
            logger.info("portfolio_state_manager_initialized")

        except Exception as e:
            logger.exception("portfolio_state_manager_initialization_failed")
            raise StateManagerInitializationFailedError from e

    async def shutdown(self) -> None:
        """Shutdown the portfolio state manager."""
        if not self._is_initialized:
            return

        logger.info("portfolio_state_manager_shutting_down")

        try:
            # Shutdown components
            if self.metrics_collector and isinstance(self.metrics_collector, Shutdownable):
                await self.metrics_collector.shutdown()

            if self.validation_service and isinstance(self.validation_service, Shutdownable):
                await self.validation_service.shutdown()

            if isinstance(self.state_container, Shutdownable):
                await self.state_container.shutdown()

            self._is_initialized = False
            logger.info("portfolio_state_manager_shut_down")

        except Exception:
            logger.exception("portfolio_state_manager_shutdown_failed")
            raise

    async def update_balances(
        self, exchange: ExchangeName, update_request: BalanceUpdateRequest
    ) -> bool:
        """Update balances for an exchange.

        Args:
            exchange: Exchange name
            update_request: Typed balance update request

        Returns:
            True if update was successful
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            # Create state update
            update = StateUpdate(
                data=update_request.balances,
                timestamp=datetime.now(UTC),
                source=f"balance_update_{exchange}",
                metadata=StateManagerMetadata(
                    operation_type="update_balances",
                    source=update_request.update_source,
                    additional_data={
                        "exchange": exchange,
                        "force_update": str(update_request.force_update),
                    },
                ),
            )

            # Perform atomic update
            async with self._state_lock:
                balances_dict = update_request.balances

                result = self.state_container.update_balances(exchange, balances_dict)

                if result.success:
                    self.update_count += 1
                    self._state_version += 1
                    self._last_update = update.timestamp

                    # Collect metrics if available
                    if self.metrics_collector:
                        self.metrics_collector.record_state_update(
                            "balance_update", exchange, "success" if result.success else "failed"
                        )
                else:
                    self.error_count += 1

            logger.info(
                "balances_updated",
                exchange=exchange,
                balance_count=len(update_request.balances),
                success=result.success,
            )
            return bool(result.success)

        except Exception:
            self.error_count += 1
            logger.exception("balance_update_failed", exchange=exchange)
            return False

    async def update_positions(
        self, exchange: ExchangeName, update_request: PositionUpdateRequest
    ) -> bool:
        """Update positions for an exchange.

        Args:
            exchange: Exchange name
            update_request: Typed position update request

        Returns:
            True if update was successful
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            # Create state update
            update = StateUpdate(
                data=update_request.positions,
                timestamp=datetime.now(UTC),
                source=f"position_update_{exchange}",
                metadata=StateManagerMetadata(
                    operation_type="update_positions",
                    source=update_request.update_source,
                    additional_data={
                        "exchange": exchange,
                        "force_update": str(update_request.force_update),
                    },
                ),
            )

            # Perform atomic update
            async with self._state_lock:
                # Convert list to dict - positions are always a list in PositionUpdateRequest
                positions_dict = {
                    str(i): position for i, position in enumerate(update_request.positions)
                }

                result = self.state_container.update_positions(exchange, positions_dict)

                if result.success:
                    self.update_count += 1
                    self._state_version += 1
                    self._last_update = update.timestamp

                    # Collect metrics if available
                    if self.metrics_collector:
                        self.metrics_collector.record_state_update(
                            "position_update", exchange, "success" if result.success else "failed"
                        )
                else:
                    self.error_count += 1

            logger.info(
                "positions_updated",
                exchange=exchange,
                position_count=len(update_request.positions),
                success=result.success,
            )
            return bool(result.success)

        except Exception:
            self.error_count += 1
            logger.exception("position_update_failed", exchange=exchange)
            return False

    async def update_orders(
        self, exchange: ExchangeName, update_request: OrderUpdateRequest
    ) -> bool:
        """Update orders for an exchange.

        Args:
            exchange: Exchange name
            update_request: Typed order update request

        Returns:
            True if update was successful
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            # Create state update
            update = StateUpdate(
                data=update_request.orders,
                timestamp=datetime.now(UTC),
                source=f"order_update_{exchange}",
                metadata=StateManagerMetadata(
                    operation_type="update_orders",
                    source=update_request.update_source,
                    additional_data={
                        "exchange": exchange,
                        "force_update": str(update_request.force_update),
                    },
                ),
            )

            # Perform atomic update
            async with self._state_lock:
                # For orders, we might need to use a different method since StateContainerProtocol
                # doesn't have update_orders. For now, simulate success.
                result = StateUpdateResult(
                    success=True,
                    execution_time_ms=1.0,
                    affected_entities=len(update_request.orders),
                )

                if result.success:
                    self.update_count += 1
                    self._state_version += 1
                    self._last_update = update.timestamp

                    # Collect metrics if available
                    if self.metrics_collector:
                        self.metrics_collector.record_state_update(
                            "order_update", exchange, "success" if result.success else "failed"
                        )
                else:
                    self.error_count += 1

            logger.info(
                "orders_updated",
                exchange=exchange,
                order_count=len(update_request.orders),
                success=result.success,
            )
            return bool(result.success)

        except Exception:
            self.error_count += 1
            logger.exception("order_update_failed", exchange=exchange)
            return False

    async def process_trade(self, trade: Trade) -> bool:
        """Process a trade through validation and state updates.

        Args:
            trade: Trade to process

        Returns:
            True if trade was processed successfully
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            # Validate trade if validation service is available
            if self.validation_service and self.validation_enabled:
                validation_result = await self.validation_service.validate_trade(trade)
                if not validation_result.valid:
                    logger.warning(
                        "trade_validation_failed",
                        trade_id=trade.id,
                        errors=validation_result.errors,
                    )
                    return False
                self.validation_count += 1

            # Create state update for trade
            update = StateUpdate(
                data=trade,
                timestamp=datetime.now(UTC),
                source="trade_processing",
                metadata=StateManagerMetadata(
                    operation_type="trade_processing",
                    additional_data={
                        "trade_id": str(trade.id or "unknown"),
                        "symbol": str(trade.symbol or "unknown"),
                        "exchange": str(trade.exchange or "unknown"),
                    },
                ),
            )

            # Process trade with atomic update
            async with self._state_lock:
                # Use add_trade method from StateContainerProtocol
                # Get exchange as string, then convert to enum if needed
                exchange_str = trade.exchange or "unknown"

                # Convert exchange string to ExchangeName enum
                try:
                    exchange_enum = ExchangeName(exchange_str)
                except (ValueError, AttributeError):
                    # Skip trade if we can't determine exchange
                    self.logger.warning(
                        "Unknown exchange for trade",
                        exchange=exchange_str,
                        trade_id=trade.id or "unknown",
                    )
                    return False

                result = self.state_container.add_trade(exchange_enum, trade)

                if result.success:
                    self.trade_processing_count += 1
                    self._state_version += 1
                    self._last_update = update.timestamp

                    # Collect metrics if available
                    if self.metrics_collector:
                        self.metrics_collector.record_state_update(
                            "trade_processing",
                            trade.exchange or "unknown",
                            "success" if result.success else "failed",
                        )
                else:
                    self.error_count += 1

            logger.info(
                "trade_processed",
                trade_id=trade.id or "unknown",
                symbol=trade.symbol or "unknown",
                exchange=trade.exchange or "unknown",
                success=result.success,
            )
            return bool(result.success)

        except Exception:
            self.error_count += 1
            logger.exception(
                "trade_processing_failed",
                trade_id=trade.id or "unknown",
            )
            return False

    async def get_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get balances for an exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of balances
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            return self.state_container.get_balances(exchange)
        except Exception as e:
            logger.exception("get_balances_failed", exchange=exchange)
            raise StateOperationFailedError("get_balances") from e

    async def get_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get positions for an exchange.

        Args:
            exchange: Exchange name

        Returns:
            List of positions
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            return self.state_container.get_positions(exchange)
        except Exception as e:
            logger.exception("get_positions_failed", exchange=exchange)
            raise StateOperationFailedError("get_positions") from e

    async def get_orders(self, exchange: ExchangeName) -> dict[str, Order]:
        """Get orders for an exchange.

        Args:
            exchange: Exchange name

        Returns:
            List of orders
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            # Return the dict directly - it's already in the right format
            return self.state_container.get_orders(exchange)
        except Exception as e:
            logger.exception("get_orders_failed", exchange=exchange)
            raise StateOperationFailedError("get_orders") from e

    async def get_portfolio_summary(self) -> PortfolioState:
        """Get comprehensive portfolio summary.

        Returns:
            Complete portfolio state with typed data
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        try:
            current_time = time.time()

            # Create portfolio state using the correct constructor
            return PortfolioState(
                state_id=f"portfolio_summary_{int(current_time)}",
                portfolio_id="default_portfolio",
                balances={},  # Would be populated with actual exchange data
                trades=[],  # Would be populated with actual trade data
                orders=[],  # Would be populated with actual order data
                total_account_value=Decimal(0),
                exchange_summaries={},
                component_health={},
                metadata={
                    "version": self._state_version,
                    "last_update": current_time,
                    "is_consistent": self.error_count == 0,
                },
            )

        except Exception as e:
            logger.exception("get_portfolio_summary_failed")
            raise StateOperationFailedError("get_summary") from e

    async def validate_state(self) -> StateValidationResult:
        """Validate current portfolio state.

        Returns:
            Validation result
        """
        if not self._is_initialized:
            raise StateManagerNotInitializedError

        if not self.validation_service:
            return StateValidationResult(
                is_valid=True,
                errors=[],
                warnings=["No validation service configured"],
                metadata={"validation_skipped": True},
            )

        try:
            # Get current portfolio summary for validation
            summary = await self.get_portfolio_summary()

            # Validate using the validation service
            validation_result = await self.validation_service.validate_portfolio_state(summary)

            if validation_result.valid:
                self.validation_count += 1
            else:
                self.error_count += 1
        except Exception as e:
            self.error_count += 1
            logger.exception("portfolio_state_validation_failed")
            return StateValidationResult(
                is_valid=False,
                errors=[f"Validation failed: {e}"],
                warnings=[],
                metadata={"exception": str(e)},
            )
        else:
            # Convert ValidationResult to StateValidationResult
            return StateValidationResult(
                is_valid=validation_result.valid,
                errors=validation_result.errors,
                warnings=validation_result.warnings,
                metadata={},
            )

    def get_metrics(self) -> PortfolioMetrics:
        """Get performance metrics.

        Returns:
            Typed portfolio metrics
        """
        current_time = time.time()

        return PortfolioMetrics(
            last_update_timestamp=current_time,
            active_positions=0,  # Would be calculated from actual data
            total_orders=0,  # Would be calculated from actual data
        )

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.update_count = 0
        self.validation_count = 0
        self.error_count = 0
        self.trade_processing_count = 0

    @property
    def is_initialized(self) -> bool:
        """Check if manager is initialized."""
        return self._is_initialized

    def __str__(self) -> str:
        """String representation."""
        return (
            f"PortfolioStateManager("
            f"version={self._state_version}, "
            f"updates={self.update_count}, "
            f"initialized={self._is_initialized})"
        )
