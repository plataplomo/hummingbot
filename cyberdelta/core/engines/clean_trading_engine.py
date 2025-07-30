"""Portfolio-aware trading engine with complete modular integration.

This engine replaces the legacy Engine component with clean portfolio/risk coordination
through the PortfolioRiskCoordinator established in Week 4.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.engines.components.position_sizer import PortfolioAwarePositionSizer
from cyberdelta.core.engines.components.risk_manager import AdvancedRiskManager
from cyberdelta.core.engines.components.trade_executor import PortfolioAwareTradeExecutor
from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import (
    TradeRequestModel,
)
from cyberdelta.core.portfolio.coordinators.unified_service_factory import UnifiedServiceFactory


class AlertSystem:
    """Simple alert system for engine notifications."""
    
    async def send_alert(self, alert_type: str, message: str) -> None:
        """Send an alert message."""
        print(f"ALERT [{alert_type}]: {message}")
    
    async def send_critical_alert(self, alert_type: str, message: str) -> None:
        """Send a critical alert message."""
        print(f"CRITICAL ALERT [{alert_type}]: {message}")


class EngineState(Enum):
    """Engine operational states."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class TradingSignal:
    """Trading signal with portfolio context."""

    symbol: str
    direction: str  # "long" | "short" | "close"
    strength: float  # 0.0 to 1.0
    strategy_id: str
    confidence: float  # 0.0 to 1.0
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("strength", "confidence", mode="before")
    @classmethod
    def validate_percentage(cls, v: float, info: ValidationInfo) -> float:
        """Validate percentage values are between 0 and 1."""
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"{info.field_name} must be between 0.0 and 1.0")
        return v

    @field_validator("direction", mode="before")
    @classmethod
    def validate_direction(cls, v: str) -> str:
        """Validate direction is valid."""
        valid_directions = {"long", "short", "close"}
        if v not in valid_directions:
            raise ValueError(f"Direction must be one of {valid_directions}")
        return v


class CleanTradingEngine:
    """Trading engine with clean portfolio/risk separation via coordinator."""

    def __init__(self, unified_factory: UnifiedServiceFactory):
        # Clean architecture: engine uses coordinator for portfolio-risk integration
        self.unified_factory = unified_factory
        self.coordinator = unified_factory.get_risk_coordinator()

        # Direct access to clean module interfaces (no cross-boundary access)
        self.portfolio_manager = unified_factory.get_portfolio_manager()

        # Engine components (will be initialized in _initialize_components)
        self.position_sizer: PortfolioAwarePositionSizer | None = None
        self.risk_manager: AdvancedRiskManager | None = None
        self.trade_executor: PortfolioAwareTradeExecutor | None = None
        self.alert_system: AlertSystem | None = None

        # Engine state
        self._state = EngineState.STOPPED
        self._config: dict[str, Any] = {}
        self._active_orders: dict[str, dict[str, Any]] = {}
        self._pending_signals: list[TradingSignal] = []
        self._execution_stats = {
            "trades_executed": 0,
            "total_volume": Decimal(0),
            "success_rate": Decimal(0),
            "avg_execution_time": 0.0
        }

        # Background tasks
        self._tasks: list[asyncio.Task[Any]] = []
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the portfolio-aware trading engine."""
        if self._state != EngineState.STOPPED:
            raise RuntimeError(f"Cannot start engine in state: {self._state}")

        self._state = EngineState.STARTING

        try:
            # Initialize unified system (portfolio + risk modules)
            await self.unified_factory.initialize_all()

            # Initialize engine components
            await self._initialize_components()

            # Load configuration
            await self._load_engine_configuration()

            # Register event handlers
            await self._register_event_handlers()

            # Start background tasks
            await self._start_background_tasks()

            self._state = EngineState.RUNNING

        except Exception as e:
            self._state = EngineState.ERROR
            raise RuntimeError(f"Failed to start engine: {e}") from e

    async def stop(self) -> None:
        """Stop the trading engine gracefully."""
        if self._state == EngineState.STOPPED:
            return

        self._state = EngineState.STOPPING

        try:
            # Cancel all active orders
            await self._cancel_all_orders()

            # Stop background tasks
            await self._stop_background_tasks()

            # Shutdown components
            await self._shutdown_components()

            # Shutdown portfolio system
            await self.unified_factory.shutdown_all()

            self._state = EngineState.STOPPED

        except Exception as e:
            self._state = EngineState.ERROR
            raise RuntimeError(f"Failed to stop engine: {e}") from e

    async def pause(self) -> None:
        """Pause trading while maintaining monitoring."""
        if self._state != EngineState.RUNNING:
            raise RuntimeError(f"Cannot pause engine in state: {self._state}")

        # Cancel pending orders but keep monitoring
        await self._cancel_all_orders()
        self._state = EngineState.PAUSED

    async def resume(self) -> None:
        """Resume trading from paused state."""
        if self._state != EngineState.PAUSED:
            raise RuntimeError(f"Cannot resume engine in state: {self._state}")

        # Restart trading logic
        self._state = EngineState.RUNNING

    async def process_trading_signal(self, signal: TradingSignal) -> dict[str, Any]:
        """Process a trading signal with complete portfolio context."""
        if self._state != EngineState.RUNNING:
            return {"status": "rejected", "reason": f"Engine not running: {self._state}"}

        try:
            # Validate signal
            signal_validation = await self._validate_signal(signal)
            if not signal_validation["valid"]:
                return {"status": "rejected", "reason": signal_validation["reason"]}

            # Get current portfolio context
            portfolio_context = await self._get_portfolio_context()

            # Evaluate signal with portfolio context
            evaluation = await self._evaluate_signal_with_context(signal, portfolio_context)
            if not evaluation["should_execute"]:
                return {"status": "rejected", "reason": evaluation["reason"]}

            # Calculate position size using position sizer
            if self.position_sizer:
                position_size = await self.position_sizer.calculate_size(
                    signal, portfolio_context, evaluation
                )
            else:
                position_size = Decimal(100)  # Fallback
            
            if position_size <= 0:
                return {"status": "rejected", "reason": "Position size too small"}
            
            # Check trade risk using risk manager
            risk_check = None
            if self.risk_manager:
                risk_check = await self.risk_manager.check_trade_risk(
                    signal, position_size, portfolio_context
                )
                if not risk_check.approved:
                    return {
                        "status": "rejected",
                        "reason": risk_check.reasons[0] if risk_check.reasons else "Risk check failed",
                        "risk_score": risk_check.risk_score
                    }
            
            # Create trade request for coordinator validation
            trade_request = TradeRequestModel(
                symbol=signal.symbol,
                side="buy" if signal.direction == "long" else "sell",
                quantity=position_size,
                signal_strength=signal.strength,
                exchange_id="hyperliquid"  # Default exchange
            )
            
            # Use coordinator to validate trade with integrated risk assessment
            validation_result = await self.coordinator.validate_trade_request(trade_request)
            
            if not validation_result.approved:
                return {
                    "status": "rejected", 
                    "reason": validation_result.reason or "Risk validation failed",
                    "risk_violations": validation_result.risk_violations
                }
            
            # Use the coordinator's optimal size if provided
            final_position_size = validation_result.optimal_size or position_size

            # Execute trade using trade executor
            if self.trade_executor:
                execution_result = await self.trade_executor.execute_trade({
                    "signal": signal,
                    "position_size": final_position_size,
                    "portfolio_context": portfolio_context,
                    "risk_parameters": risk_check.parameters if risk_check else {}
                })
            else:
                # Fallback to internal execution
                execution_result = await self._execute_trade_with_validation({
                    "signal": signal,
                    "position_size": final_position_size,
                    "trade_request": trade_request,
                    "validation_result": validation_result
                })

            # Update execution stats
            await self._update_execution_stats(execution_result)

            return {
                "status": "executed",
                "execution_id": execution_result.get("execution_id", "unknown"),
                "position_size": final_position_size,
                "portfolio_impact": execution_result.get("portfolio_impact", {})
            }

        except Exception as e:
            if self.alert_system:
                await self.alert_system.send_alert(
                    "signal_processing_error",
                    f"Failed to process signal for {signal.symbol}: {e}"
                )
            return {"status": "error", "reason": str(e)}

    async def get_engine_status(self) -> dict[str, Any]:
        """Get comprehensive engine status."""
        # Get portfolio state with risk assessment through coordinator
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()

        return {
            "state": self._state.value,
            "uptime": self._get_uptime(),
            "portfolio": {
                "total_capital": portfolio_with_risk.portfolio_state.total_capital,
                "total_exposure": portfolio_with_risk.risk_assessment.get("total_exposure", Decimal(0)),
                "position_count": len(portfolio_with_risk.portfolio_state.positions),
                "unrealized_pnl": getattr(portfolio_with_risk.portfolio_state, "unrealized_pnl", Decimal(0))
            },
            "execution_stats": self._execution_stats.copy(),
            "active_orders": len(self._active_orders),
            "pending_signals": len(self._pending_signals),
            "health_score": await self._get_health_score()
        }

    async def emergency_stop(self, reason: str) -> None:
        """Emergency stop with immediate order cancellation."""
        # Cancel all orders immediately
        await self._emergency_cancel_all_orders()

        # Send critical alert
        if self.alert_system:
            await self.alert_system.send_critical_alert(
                "emergency_stop",
                f"Emergency stop triggered: {reason}"
            )

        # Set error state
        self._state = EngineState.ERROR

        # Stop all background tasks
        await self._stop_background_tasks()

    async def _initialize_components(self) -> None:
        """Initialize all engine components."""
        # Initialize engine components with the coordinator
        self.position_sizer = PortfolioAwarePositionSizer(self.coordinator)
        self.risk_manager = AdvancedRiskManager(self.coordinator)
        self.trade_executor = PortfolioAwareTradeExecutor(self.coordinator)
        self.alert_system = AlertSystem()
        
        # Initialize each component
        await self.position_sizer.initialize()
        await self.risk_manager.initialize()
        await self.trade_executor.initialize()

    async def _start_background_tasks(self) -> None:
        """Start all background monitoring and processing tasks."""
        # Portfolio monitoring task
        self._tasks.append(asyncio.create_task(self._portfolio_monitoring_loop()))

        # Risk monitoring task
        self._tasks.append(asyncio.create_task(self._risk_monitoring_loop()))

        # Signal processing task
        self._tasks.append(asyncio.create_task(self._signal_processing_loop()))

        # Health checking task
        self._tasks.append(asyncio.create_task(self._health_checking_loop()))

    async def _portfolio_monitoring_loop(self) -> None:
        """Continuous portfolio state monitoring."""
        while self._state != EngineState.STOPPED:
            try:
                # Get current portfolio state through coordinator
                portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()

                # Monitor for significant changes
                await self._check_portfolio_changes(portfolio_with_risk)

                await asyncio.sleep(5)  # Monitor every 5 seconds

            except Exception as e:
                if self.alert_system:
                    await self.alert_system.send_alert(
                        "portfolio_monitoring_error",
                        f"Portfolio monitoring error: {e}"
                    )
                await asyncio.sleep(5)

    async def _risk_monitoring_loop(self) -> None:
        """Continuous risk monitoring with automatic responses."""
        while self._state != EngineState.STOPPED:
            try:
                # Check all risk limits through coordinator
                risk_status = await self._check_all_risk_limits()

                # Handle violations
                if risk_status["violations"]:
                    await self._handle_risk_violations(risk_status["violations"])

                await asyncio.sleep(2)  # Check every 2 seconds

            except Exception as e:
                if self.alert_system:
                    await self.alert_system.send_alert(
                        "risk_monitoring_error",
                        f"Risk monitoring error: {e}"
                    )
                await asyncio.sleep(2)

    async def _signal_processing_loop(self) -> None:
        """Process pending trading signals."""
        while self._state != EngineState.STOPPED:
            try:
                if self._pending_signals and self._state == EngineState.RUNNING:
                    # Process oldest signal first
                    signal = self._pending_signals.pop(0)
                    result = await self.process_trading_signal(signal)

                    # Log result
                    if result["status"] == "executed":
                        self._execution_stats["trades_executed"] = int(str(self._execution_stats["trades_executed"])) + 1

                await asyncio.sleep(0.1)  # Fast signal processing

            except Exception as e:
                if self.alert_system:
                    await self.alert_system.send_alert(
                        "signal_processing_error",
                        f"Signal processing error: {e}"
                    )
                await asyncio.sleep(1)

    async def _health_checking_loop(self) -> None:
        """Background health checking."""
        while self._state != EngineState.STOPPED:
            try:
                health_score = await self._get_health_score()
                
                if health_score < 0.5:  # Health score below 50%
                    if self.alert_system:
                        await self.alert_system.send_alert(
                            "low_health_score",
                            f"Engine health score: {health_score}"
                        )

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception:
                await asyncio.sleep(30)

    async def _get_portfolio_context(self) -> dict[str, Any]:
        """Get comprehensive portfolio context for decision making."""
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        
        return {
            "portfolio_state": portfolio_with_risk.portfolio_state,
            "risk_assessment": portfolio_with_risk.risk_assessment,
            "total_capital": portfolio_with_risk.portfolio_state.total_capital,
            "available_capital": portfolio_with_risk.portfolio_state.total_capital - 
                               portfolio_with_risk.risk_assessment.get("total_exposure", Decimal(0)),
            "position_count": len(portfolio_with_risk.portfolio_state.positions),
            "timestamp": portfolio_with_risk.timestamp
        }

    async def _validate_signal(self, signal: TradingSignal) -> dict[str, Any]:
        """Validate trading signal before processing."""
        # Basic validation
        if not signal.symbol:
            return {"valid": False, "reason": "Missing symbol"}

        if signal.direction not in ["long", "short", "close"]:
            return {"valid": False, "reason": "Invalid direction"}

        if not 0 <= signal.strength <= 1:
            return {"valid": False, "reason": "Invalid strength"}

        if not 0 <= signal.confidence <= 1:
            return {"valid": False, "reason": "Invalid confidence"}

        # Check signal age
        signal_age = (datetime.now(UTC) - signal.timestamp).total_seconds()
        if signal_age > 300:  # 5 minutes
            return {"valid": False, "reason": "Signal too old"}

        return {"valid": True}

    async def _evaluate_signal_with_context(
        self, 
        signal: TradingSignal, 
        portfolio_context: dict[str, Any]
    ) -> dict[str, Any]:
        """Evaluate signal with portfolio context."""
        # For now, simple evaluation logic
        # In production, this would integrate with strategy evaluation logic
        
        should_execute = (
            signal.confidence > 0.5 and 
            signal.strength > 0.3 and
            portfolio_context["available_capital"] > 1000
        )
        
        reason = "Signal evaluation passed" if should_execute else "Signal evaluation failed"
        
        # Calculate expected metrics based on signal and market conditions
        risk_assessment = portfolio_context.get("risk_assessment", {})
        
        # Base win rate from signal confidence
        base_win_rate = 0.5 + (signal.confidence * 0.3)  # 50-80% based on confidence
        
        # Adjust for market conditions
        volatility_factor = 1.0
        if "portfolio_volatility" in risk_assessment:
            vol = float(risk_assessment["portfolio_volatility"])
            if vol > 0.03:  # High volatility
                volatility_factor = 0.9  # Reduce win rate
            elif vol < 0.01:  # Low volatility
                volatility_factor = 1.1  # Increase win rate
        
        expected_win_rate = min(0.9, base_win_rate * volatility_factor)
        
        # Risk/reward based on signal strength
        risk_reward_ratio = 1.0 + (signal.strength * 1.0)  # 1:1 to 1:2 RR
        
        return {
            "should_execute": should_execute,
            "reason": reason,
            "expected_win_rate": expected_win_rate,
            "expected_win_amount": risk_reward_ratio,
            "expected_loss_amount": 1.0,
            "signal_quality": signal.strength * signal.confidence,
            "volatility_adjusted": volatility_factor != 1.0
        }

    async def _execute_trade_with_validation(
        self, trade_params: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute trade with validation results."""
        # Since we don't have actual trade execution yet,
        # simulate a successful trade execution
        import uuid
        
        signal = trade_params["signal"]
        position_size = trade_params["position_size"]
        
        # Simulate execution result
        execution_id = str(uuid.uuid4())
        
        # Calculate simulated portfolio impact
        price = Decimal(50000)  # Simulated price
        notional_value = position_size * price
        fees = notional_value * Decimal("0.001")  # 0.1% fee
        
        portfolio_impact = {
            "capital_change": -notional_value - fees,
            "exposure_change": notional_value,
            "fees_paid": fees
        }
        
        return {
            "execution_id": execution_id,
            "success": True,
            "portfolio_impact": portfolio_impact,
            "executed_quantity": position_size,
            "average_price": price,
            "total_fees": fees
        }

    async def _check_all_risk_limits(self) -> dict[str, Any]:
        """Check all risk limits through coordinator."""
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        
        # Basic risk limit checks
        violations = []
        warnings: list[dict[str, Any]] = []
        
        total_exposure = portfolio_with_risk.risk_assessment.get("total_exposure", Decimal(0))
        total_capital = portfolio_with_risk.portfolio_state.total_capital
        
        # Check exposure limit (80% of capital)
        if total_exposure > total_capital * Decimal("0.8"):
            violations.append({
                "type": "exposure_limit",
                "message": f"Total exposure {total_exposure} exceeds 80% of capital"
            })
        
        return {
            "violations": violations,
            "warnings": warnings
        }

    async def _check_portfolio_changes(self, portfolio_with_risk: Any) -> None:
        """Check for significant portfolio changes."""
        # Monitor for significant drawdown
        risk_assessment = portfolio_with_risk.risk_assessment
        if "drawdown" in risk_assessment:
            drawdown = risk_assessment["drawdown"]
            if drawdown > Decimal("0.1"):  # 10% drawdown
                if self.alert_system:
                    await self.alert_system.send_alert(
                        "high_drawdown",
                        f"Portfolio drawdown: {drawdown}"
                    )

    async def _handle_risk_violations(self, violations: list[dict[str, Any]]) -> None:
        """Handle risk violations."""
        for violation in violations:
            if self.alert_system:
                await self.alert_system.send_alert(
                    "risk_violation",
                    f"Risk violation: {violation['message']}"
                )
            
            # Could implement automatic risk reduction here
            if violation["type"] == "exposure_limit":
                # Consider pausing new trades
                pass

    async def _update_execution_stats(self, execution_result: dict[str, Any]) -> None:
        """Update execution statistics."""
        self._execution_stats["trades_executed"] = int(str(self._execution_stats["trades_executed"])) + 1
        
        if execution_result.get("success"):
            self._execution_stats["total_volume"] += execution_result.get("position_size", Decimal(0))

    async def _get_health_score(self) -> float:
        """Calculate engine health score."""
        # Simple health score calculation
        score = 1.0
        
        # Reduce score based on errors
        if self._state == EngineState.ERROR:
            score -= 0.5
        elif self._state == EngineState.PAUSED:
            score -= 0.2
        
        # Check if coordinator is responsive
        try:
            await asyncio.wait_for(
                self.coordinator.get_current_portfolio_with_risk_assessment(), 
                timeout=1.0
            )
        except TimeoutError:
            score -= 0.3
        except Exception:
            score -= 0.5
        
        return max(0.0, score)

    def _get_uptime(self) -> float:
        """Get engine uptime in seconds."""
        # Implement uptime tracking
        return 0.0

    async def _cancel_all_orders(self) -> None:
        """Cancel all active orders."""
        # Implement order cancellation
        self._active_orders.clear()

    async def _emergency_cancel_all_orders(self) -> None:
        """Emergency cancel all orders."""
        await self._cancel_all_orders()

    async def _stop_background_tasks(self) -> None:
        """Stop all background tasks."""
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        self._tasks.clear()

    async def _shutdown_components(self) -> None:
        """Shutdown all components."""
        # Currently all risk/portfolio functionality is provided by coordinator
        # Only the alert system needs potential cleanup

    async def _load_engine_configuration(self) -> None:
        """Load engine configuration."""
        self._config = {
            "max_position_percent": Decimal("0.1"),
            "max_leverage": Decimal("3.0"),
            "max_daily_trades": 100
        }

    async def _register_event_handlers(self) -> None:
        """Register event handlers for portfolio events."""
        # This would register handlers with the event dispatcher

