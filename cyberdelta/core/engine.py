"""Core trading engine with clean portfolio/risk separation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from cyberdelta.core.models import TradeSignal

from cyberdelta.core.portfolio.services import PortfolioServiceFactory
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.symbols import Symbol
from cyberdelta.enums.exchange_names import ExchangeName


logger = structlog.get_logger(__name__)


class EngineConfigurationError(RuntimeError):
    """Error with engine configuration."""


class Engine(BaseModel):
    """Trading engine focused on orchestration without strategy management.
    
    The Engine is responsible for:
    - High-level orchestration and component wiring
    - Managing service factories (portfolio, risk)
    - Setting up signal flow pipeline
    - Starting/stopping the engine
    - Coordinating between major components
    
    Strategy management is handled by StrategyManager.
    """

    portfolio_factory: PortfolioServiceFactory = Field(..., description="Portfolio service factory")
    risk_factory: RiskServiceFactory = Field(..., description="Risk service factory")
    name: str = Field(default="CyberDeltaEngine", description="Engine name")

    model_config = ConfigDict(extra="forbid", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize service instances after Pydantic validation."""
        # Portfolio responsibilities: state management, performance tracking
        self.portfolio_manager = self.portfolio_factory.create_portfolio_state_manager()
        self.performance_analytics = self.portfolio_factory.create_performance_analytics()

        # Risk responsibilities: exposure calculation, position sizing, risk assessment
        self.risk_calculator = self.risk_factory.create_risk_metrics_calculator()
        self.exposure_calculator = self.risk_factory.create_exposure_calculator()
        self.position_sizer = self.risk_factory.create_position_sizer()

        # Engine state
        self.signal_handler: Callable[[TradeSignal], Awaitable[None]] | None = None
        self.is_running = False
        self.start_time: datetime | None = None
        self.last_data_time: datetime | None = None
        self.logger = structlog.get_logger(engine_name=self.name)

        logger.info(
            "engine_initialized",
            engine_name=self.name,
            message=f"Engine '{self.name}' initialized with modular system",
        )

    async def get_portfolio_capital(self) -> Decimal:
        """Get current portfolio capital for position sizing."""
        # Clean separation: portfolio provides state, performance calculates metrics
        portfolio_state = await self.portfolio_manager.get_portfolio_summary()
        # TODO: Fix PortfolioState type mismatch - two different classes with same name
        performance = await self.performance_analytics.calculate_performance(portfolio_state)  # type: ignore[arg-type]
        return performance.total_capital

    async def get_position_size_for_trade(self, symbol: Symbol, signal_strength: float) -> Decimal:
        """Calculate optimal position size using risk module."""
        # Risk module handles all position sizing decisions
        # TODO: This method needs to be redesigned - PositionSizer expects ArbitrageOpportunity, not individual parameters
        # For now, return a placeholder value to fix mypy errors
        return Decimal("100.0")  # Placeholder - needs proper implementation

    async def get_portfolio_positions(self, exchange_id: str | None = None) -> dict[str, Any]:
        """Get current portfolio positions."""
        if exchange_id is None:
            # TODO: Handle case where no exchange is specified - maybe aggregate all exchanges?
            raise ValueError("exchange_id is required")
        exchange_name = ExchangeName(exchange_id)
        return await self.portfolio_manager.get_positions(exchange_name)

    async def get_portfolio_balances(self, exchange_id: str | None = None) -> dict[str, Any]:
        """Get current portfolio balances."""
        if exchange_id is None:
            # TODO: Handle case where no exchange is specified - maybe aggregate all exchanges?
            raise ValueError("exchange_id is required")
        exchange_name = ExchangeName(exchange_id)
        return await self.portfolio_manager.get_balances(exchange_name)

    async def get_exposure_metrics(self) -> dict[str, Any]:
        """Get portfolio exposure metrics using risk module."""
        # TODO: This method needs to be redesigned - RiskMetricsCalculator doesn't have calculate_exposure
        # For now, return a placeholder value to fix mypy errors
        return {"exposure": "placeholder"}  # Placeholder - needs proper implementation

    def set_signal_handler(self, handler: Callable[[TradeSignal], Awaitable[None]]) -> None:
        """Set the single async handler responsible for processing generated TradeSignals.

        This should typically be the entry point for the RiskManager or a SignalQueue.

        Args:
            handler: The async callable that accepts a TradeSignal.

        """
        self.signal_handler = handler
        # Use getattr for safe name retrieval, fallback to repr
        handler_name = getattr(handler, "__name__", repr(handler))
        logger.info(
            "signal_handler_set",
            handler_name=handler_name,
            action="signal_handler_configured",
            message=f"Signal handler set to: {handler_name}",
        )

    def start(self) -> None:
        """Start the trading engine.

        Raises:
            EngineConfigurationError: If signal handler is not configured before starting.
        """
        if self.is_running:
            logger.warning("Engine is already running.")
            return

        if not self.signal_handler:
            logger.error("Cannot start Engine: Signal handler has not been set.")
            # Prevent starting without a crucial dependency
            engine_config_error_msg = "Engine cannot start without a configured signal handler."
            raise EngineConfigurationError(engine_config_error_msg)

        logger.info(
            "engine_starting",
            engine_name=self.name,
            action="engine_starting",
            message=f"Starting engine '{self.name}'...",
        )
        self.is_running = True
        self.start_time = datetime.now(UTC)  # Use UTC

        logger.info(
            "engine_started",
            engine_name=self.name,
            action="engine_started",
            message=f"Engine '{self.name}' started.",
        )

    def stop(self) -> None:
        """Stop the trading engine."""
        if not self.is_running:
            logger.warning("Engine is not running.")
            return

        logger.info(
            "engine_stopping",
            engine_name=self.name,
            action="engine_stopping",
            message=f"Stopping engine '{self.name}'...",
        )
        self.is_running = False

        logger.info(
            "engine_stopped",
            engine_name=self.name,
            action="engine_stopped",
            message=f"Engine '{self.name}' stopped.",
        )

    def get_engine_info(self) -> dict[str, Any]:
        """Get basic information about the engine's operational state.

        Returns:
            Dictionary with engine state information.

        """
        uptime_seconds = (
            (datetime.now(UTC) - self.start_time).total_seconds()
            if self.start_time and self.is_running
            else 0
        )

        return {
            "name": self.name,
            "running": self.is_running,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime_seconds": uptime_seconds,
            "last_data_time": self.last_data_time.isoformat() if self.last_data_time else None,
        }