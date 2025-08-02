"""Complete application integration with modular portfolio system."""
from __future__ import annotations

import asyncio
import signal
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Optional

from cyberdelta.core.engines.clean_trading_engine import CleanTradingEngine
from cyberdelta.core.portfolio.coordinators.unified_service_factory import UnifiedServiceFactory
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioState
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioConfig


class CyberDeltaApplication:
    """Main application with fully integrated modular portfolio system."""

    def __init__(self, config: Any):
        """Initialize application with configuration."""
        self.config = config
        self.unified_factory = UnifiedServiceFactory(config=config)

        # Core components will be initialized during startup
        self.engine: Optional[CleanTradingEngine] = None
        self.strategy_manager: Optional[Any] = None  # Week 6
        self.execution_handler: Optional[Any] = None  # Week 6
        self.risk_manager: Optional[Any] = None  # Week 6

        # Application state
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the complete application."""
        try:
            # Initialize portfolio system
            await self.unified_factory.initialize_all()

            # Initialize core components
            await self._initialize_components()

            # Start all components
            await self._start_components()

            # Setup signal handlers
            self._setup_signal_handlers()

            self._running = True

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except Exception as e:
            await self._emergency_shutdown(e)
            raise

    async def stop(self) -> None:
        """Stop the complete application."""
        if not self._running:
            return

        self._running = False

        # Stop components in reverse order
        await self._stop_components()

        # Shutdown portfolio system
        await self.unified_factory.shutdown_all()

        # Signal shutdown complete
        self._shutdown_event.set()

    async def _initialize_components(self) -> None:
        """Initialize all application components."""
        # Get core services from unified factory
        self.portfolio_manager = self.unified_factory.get_portfolio_manager()
        self.risk_coordinator = self.unified_factory.get_risk_coordinator()

        # Initialize Clean Trading Engine with unified factory
        self.engine = CleanTradingEngine(unified_factory=self.unified_factory)
        
        # Initialize other components (to be implemented in Week 6)
        self.strategy_manager = None  # Week 6: StrategyManager replacement
        self.execution_handler = None  # Already handled by engine's trade_executor
        self.risk_manager = None      # Already handled by engine's risk_manager

    async def _start_components(self) -> None:
        """Start all application components."""
        # Core services are already initialized via unified factory
        
        # Start the Clean Trading Engine
        if self.engine:
            await self.engine.start()
        
        # Start other components when available (Week 6)
        # if self.strategy_manager:
        #     await self.strategy_manager.start()

    async def _stop_components(self) -> None:
        """Stop all application components."""
        # Stop components in reverse order
        
        # Stop strategy manager when available (Week 6)
        # if self.strategy_manager:
        #     await self.strategy_manager.stop()
            
        # Stop the Clean Trading Engine
        if self.engine:
            await self.engine.stop()
            
        # Core services are stopped via unified factory in stop()

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown signal handlers."""
        def signal_handler(signum: int, frame: Any) -> None:
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _emergency_shutdown(self, error: Exception) -> None:
        """Emergency shutdown on critical error."""
        try:
            await self._stop_components()
            await self.unified_factory.shutdown_all()
        except Exception:
            # Best effort cleanup
            pass

    @asynccontextmanager
    async def application_context(self) -> Any:
        """Context manager for application lifecycle."""
        try:
            await self.start()
            yield self
        finally:
            await self.stop()

    # Application API methods for external access
    async def get_portfolio_state(self) -> Any:
        """Get current portfolio state."""
        return await self.portfolio_manager.get_portfolio_summary()

    async def get_portfolio_with_risk(self) -> Any:
        """Get portfolio state with risk assessment."""
        return await self.unified_factory.get_portfolio_with_risk_assessment()

    async def validate_trade(self, trade_request: dict[str, Any]) -> Any:
        """Validate a trade request."""
        from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import TradeRequestModel
        
        # Convert dict to validated model
        trade_model = TradeRequestModel(**trade_request)
        
        # Validate through risk coordinator
        return await self.risk_coordinator.validate_trade_request(trade_model)
    
    async def process_trading_signal(self, signal: Any) -> dict[str, Any]:
        """Process a trading signal through the engine."""
        if not self.engine or not self._running:
            return {"status": "error", "reason": "Application not running"}
        
        return await self.engine.process_trading_signal(signal)
    
    async def get_engine_status(self) -> dict[str, Any]:
        """Get engine operational status."""
        if not self.engine:
            return {"status": "error", "reason": "Engine not initialized"}
            
        return await self.engine.get_engine_status()
    
    async def pause_trading(self) -> None:
        """Pause trading while maintaining monitoring."""
        if self.engine:
            await self.engine.pause()
    
    async def resume_trading(self) -> None:
        """Resume trading from paused state."""
        if self.engine:
            await self.engine.resume()
    
    async def emergency_stop(self, reason: str) -> None:
        """Emergency stop with immediate order cancellation."""
        if self.engine:
            await self.engine.emergency_stop(reason)
    
    async def get_portfolio_status(self) -> dict[str, Any]:
        """Get portfolio status with analytics.
        
        Returns:
            Portfolio status including value, positions, and P&L
        """
        portfolio_state: PortfolioState = await self.portfolio_manager.get_portfolio_summary()
        
        # Use aggregated data from portfolio state
        return {
            "total_capital": str(portfolio_state.total_account_value),
            "position_count": portfolio_state.active_positions,
            "portfolio_id": portfolio_state.portfolio_id,
            "timestamp": portfolio_state.updated_at.isoformat()
        }