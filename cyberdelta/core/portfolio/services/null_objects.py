"""Null object pattern implementations for optional services."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger

# Import classes used at runtime (not just type-checking)
from cyberdelta.core.models import DerivativePosition, SpotBalance, Trade
from cyberdelta.core.portfolio.portfolio_types.calculation_types import (
    PortfolioExposureResult,
)
from cyberdelta.core.portfolio.portfolio_types.manager_protocols import (
    BalanceManagerProtocol,
    OrderManagerProtocol,
    PortfolioManagerProtocol,
    PositionManagerProtocol,
    StateManagerProtocol,
)
from cyberdelta.core.portfolio.portfolio_types.portfolio_models import (
    PortfolioSnapshot,
    PortfolioUpdate,
)
from cyberdelta.core.portfolio.portfolio_types.resilience_types import (
    ResilienceMetrics,
    ResilienceResult,
)
from cyberdelta.core.portfolio.portfolio_types.update_models import (
    CapitalSummary,
    ExposureMetrics,
    ManagerStats,
    PerformanceMetadata,
    PnLSummary,
    PortfolioSummary,
)
from cyberdelta.core.portfolio.portfolio_types.validation_types import ValidationResult


if TYPE_CHECKING:
    from cyberdelta.core.models import Order


logger = get_logger(__name__)


class NullBalanceManager(BalanceManagerProtocol):
    """Null object implementation of BalanceManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null balance manager."""
        logger.warning(
            "null_balance_manager_created",
            message="Using null balance manager - balance operations will be no-ops",
        )

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """No-op balance update."""
        logger.debug("null_balance_update", exchange_id=exchange_id, balance_count=len(balances))

    async def get_balance(self, exchange_id: str, asset: str) -> SpotBalance | None:
        """Return None for all balance queries."""
        logger.debug("null_balance_get", exchange_id=exchange_id, asset=asset)
        return None

    async def get_total_balance_in_currency(self, asset: str, target_currency: str) -> Decimal:
        """Return zero balance."""
        logger.debug("null_balance_total", asset=asset, target_currency=target_currency)
        return Decimal(0)

    async def update_balance_from_trade(self, trade: Trade) -> None:
        """No-op trade update."""
        logger.debug("null_balance_trade_update", trade_id=trade.id)


class NullPositionManager(PositionManagerProtocol):
    """Null object implementation of PositionManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null position manager."""
        logger.warning(
            "null_position_manager_created",
            message="Using null position manager - position operations will be no-ops",
        )

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """No-op position update."""
        logger.debug("null_position_update", exchange_id=exchange_id, position_count=len(positions))

    async def update_position_from_trade(self, trade: Trade) -> None:
        """No-op trade update."""
        logger.debug("null_position_trade_update", trade_id=trade.id)

    async def get_position(self, exchange_id: str, symbol: str) -> DerivativePosition | None:
        """Return None for all position queries."""
        logger.debug("null_position_get", exchange_id=exchange_id, symbol=symbol)
        return None

    async def get_positions_by_symbol(self, symbol: str) -> list[DerivativePosition]:
        """Return empty list."""
        logger.debug("null_positions_by_symbol", symbol=symbol)
        return []


class NullOrderManager(OrderManagerProtocol):
    """Null object implementation of OrderManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null order manager."""
        logger.warning(
            "null_order_manager_created",
            message="Using null order manager - order operations will be no-ops",
        )

    async def update_orders(self, exchange_id: str, orders: list[Order]) -> None:
        """No-op order update."""
        logger.debug("null_order_update", exchange_id=exchange_id, order_count=len(orders))

    async def get_order(self, exchange_id: str, order_id: str) -> Order | None:
        """Return None for all order queries."""
        logger.debug("null_order_get", exchange_id=exchange_id, order_id=order_id)
        return None

    async def get_orders_by_symbol(self, symbol: str) -> list[Order]:
        """Return empty list."""
        logger.debug("null_orders_by_symbol", symbol=symbol)
        return []


class NullStateManager(StateManagerProtocol):
    """Null object implementation of StateManagerProtocol."""

    def __init__(self) -> None:
        """Initialize null state manager."""
        logger.warning(
            "null_state_manager_created",
            message="Using null state manager - all operations will return empty/zero values",
        )

    async def process_trade(self, trade: Trade) -> bool:
        """Always return True (success) but do nothing.

        Returns:
            bool: Always True indicating successful processing
        """
        logger.debug("null_process_trade", trade_id=trade.id)
        return True

    async def update_from_orchestrator(self, update_data: PortfolioUpdate) -> None:
        """No-op orchestrator update."""
        logger.debug("null_orchestrator_update", update_id=str(update_data.update_id))

    async def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        """Return empty portfolio snapshot."""
        return PortfolioSnapshot()

    async def calculate_total_pnl(self) -> Decimal:
        """Return zero PnL."""
        return Decimal(0)

    async def calculate_exposure_metrics(self) -> ExposureMetrics:
        """Return empty exposure metrics."""
        return ExposureMetrics(
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            long_exposure=Decimal(0),
            short_exposure=Decimal(0),
            exposure_by_symbol={},
            exposure_by_exchange={},
            leverage=Decimal(0),
            timestamp=0.0,
        )

    async def get_total_capital(self) -> CapitalSummary:
        """Return zero capital."""
        return CapitalSummary(
            total_capital=Decimal(0),
            free_capital=Decimal(0),
            used_capital=Decimal(0),
            capital_by_exchange={},
            capital_by_currency={},
            timestamp=0.0,
        )

    async def get_portfolio_summary(self) -> PortfolioSummary:
        """Return empty portfolio summary."""
        capital_summary = await self.get_total_capital()
        pnl_summary = await self.get_pnl_summary()
        exposure_metrics = await self.calculate_exposure_metrics()

        return PortfolioSummary(
            total_value=Decimal(0),
            capital_summary=capital_summary,
            pnl_summary=pnl_summary,
            exposure_metrics=exposure_metrics,
            position_count=0,
            order_count=0,
            trade_count=0,
            health_status="healthy",
            timestamp=0.0,
        )

    async def calculate_portfolio_exposure(self, base_currency: str) -> PortfolioExposureResult:
        """Return zero exposure."""
        return PortfolioExposureResult(
            total_long_exposure=Decimal(0),
            total_short_exposure=Decimal(0),
            total_net_exposure=Decimal(0),
            total_gross_exposure=Decimal(0),
            currency=base_currency,
            by_symbol={},
            by_exchange={},
        )

    async def get_pnl_summary(self) -> PnLSummary:
        """Return zero PnL summary."""
        return PnLSummary(
            total_realized_pnl=Decimal(0),
            total_unrealized_pnl=Decimal(0),
            daily_realized_pnl=Decimal(0),
            daily_unrealized_pnl=Decimal(0),
            pnl_by_exchange={},
            pnl_by_symbol={},
            timestamp=0.0,
        )

    async def get_all_positions(self) -> dict[str, DerivativePosition]:
        """Return empty positions."""
        return {}

    async def get_all_balances(self) -> dict[str, SpotBalance]:
        """Return empty balances."""
        return {}

    async def get_all_orders(self) -> dict[str, Order]:
        """Return empty orders."""
        return {}

    async def get_manager_stats(self) -> ManagerStats:
        """Return empty stats."""
        return ManagerStats(
            manager_name="null_state_manager",
            processed_items=0,
            error_count=0,
            last_update=0.0,
            performance_metrics=PerformanceMetadata(),
        )

    async def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None:
        """No-op balance update."""
        logger.debug("null_state_balance_update", exchange_id=exchange_id)

    async def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None:
        """No-op position update."""
        logger.debug("null_state_position_update", exchange_id=exchange_id)

    async def initialize(self) -> None:
        """No-op initialization."""
        logger.debug("null_state_initialize")

    async def shutdown(self) -> None:
        """No-op shutdown."""
        logger.debug("null_state_shutdown")


class NullPortfolioManager(PortfolioManagerProtocol[None]):
    """Null object implementation of PortfolioManagerProtocol."""

    @property
    def name(self) -> str:
        """Manager name."""
        return "NullPortfolioManager"

    @property
    def is_initialized(self) -> bool:
        """Always initialized."""
        return True

    async def initialize(self) -> None:
        """No-op initialization."""
        logger.debug("null_portfolio_initialize")

    async def shutdown(self) -> None:
        """No-op shutdown."""
        logger.debug("null_portfolio_shutdown")


class NullResilienceService:
    """Null object implementation for resilience service."""

    def __init__(self) -> None:
        """Initialize null resilience service."""
        logger.warning(
            "null_resilience_service_created",
            message="Using null resilience service - no resilience features active",
        )

    async def execute_with_resilience(
        self,
        service_name: str,
        func: Callable[..., Any],
        *args: object,
        **kwargs: object,
    ) -> object:
        """Execute function directly without resilience.

        Returns:
            object: Result of the function execution
        """
        logger.debug("null_resilience_execute", service_name=service_name)
        return await func(*args, **kwargs)

    def get_resilience_result(self, value: object) -> ResilienceResult[object]:
        """Return successful result without resilience metrics."""
        metrics = ResilienceMetrics(
            total_attempts=1,
            successful_attempts=1,
            failed_attempts=0,
            circuit_breaker_trips=0,
            fallback_executions=0,
            total_duration_ms=0,
        )
        return ResilienceResult[object].successful(value, metrics)


class NullValidationService:
    """Null object implementation for validation service."""

    def __init__(self) -> None:
        """Initialize null validation service."""
        logger.warning(
            "null_validation_service_created",
            message="Using null validation service - all validations will pass",
        )

    async def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Always return valid.

        Returns:
            ValidationResult[Trade]: Successful validation result for the trade
        """
        return ValidationResult[Trade].success(trade)

    async def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Always return valid.

        Returns:
            ValidationResult[SpotBalance]: Successful validation result for the balance
        """
        return ValidationResult[SpotBalance].success(balance)

    async def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Always return valid.

        Returns:
            ValidationResult[DerivativePosition]: Successful validation result for the position
        """
        return ValidationResult[DerivativePosition].success(position)

    async def validate_portfolio_state(self) -> ValidationResult[object]:
        """Always return valid.

        Returns:
            ValidationResult[object]: Successful validation result for the portfolio state
        """
        return ValidationResult[object].success({})


# Factory functions for creating null objects
def create_null_balance_manager() -> BalanceManagerProtocol:
    """Create null balance manager.

    Returns:
        BalanceManagerProtocol: Null balance manager instance
    """
    return NullBalanceManager()


def create_null_position_manager() -> PositionManagerProtocol:
    """Create null position manager.

    Returns:
        PositionManagerProtocol: Null position manager instance
    """
    return NullPositionManager()


def create_null_order_manager() -> OrderManagerProtocol:
    """Create null order manager.

    Returns:
        OrderManagerProtocol: Null order manager instance
    """
    return NullOrderManager()


def create_null_state_manager() -> StateManagerProtocol:
    """Create null state manager.

    Returns:
        StateManagerProtocol: Null state manager instance
    """
    return NullStateManager()


def create_null_portfolio_manager() -> PortfolioManagerProtocol[None]:
    """Create null portfolio manager.

    Returns:
        PortfolioManagerProtocol[None]: Null portfolio manager instance
    """
    return NullPortfolioManager()


def create_null_resilience_service() -> NullResilienceService:
    """Create null resilience service.

    Returns:
        NullResilienceService: Null resilience service instance
    """
    return NullResilienceService()


def create_null_validation_service() -> NullValidationService:
    """Create null validation service.

    Returns:
        NullValidationService: Null validation service instance
    """
    return NullValidationService()
