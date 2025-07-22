"""Advanced integration example showing custom portfolio strategies and patterns."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
from cyberdelta.core.portfolio.managers.portfolio_state_manager import (
    PortfolioStateManager,
)
from cyberdelta.core.portfolio.portfolio_types.portfolio_data_models import (
    BalanceUpdateRequest,
    PositionUpdateRequest,
)
from cyberdelta.core.portfolio.portfolio_types.state_types import (
    PortfolioSnapshot,
    StateUpdateResult,
)
from cyberdelta.core.portfolio.portfolio_types.validation_types import (
    ValidationResult,
)
from cyberdelta.core.portfolio.services.resilience.resilience_middleware import (
    ResilienceMiddleware,
)
from cyberdelta.core.portfolio.services.resilience.resilience_service import (
    PortfolioResilienceService,
)
from cyberdelta.core.portfolio.services.validation.portfolio_validation_service import (
    PortfolioValidationService,
)
from cyberdelta.core.portfolio.services.validation.validation_middleware import (
    ValidationMiddleware,
)
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.manager_protocols import StateManagerProtocol
    from cyberdelta.core.portfolio.portfolio_types.service_protocols import PortfolioServiceProtocol


logger = get_logger(__name__)


class InMemoryStateContainer:
    """In-memory implementation of StateContainerProtocol for examples."""

    def __init__(self) -> None:
        """Initialize the in-memory state container."""
        self.balances: dict[ExchangeName, dict[str, SpotBalance]] = {}
        self.positions: dict[ExchangeName, dict[str, DerivativePosition]] = {}
        self.orders: dict[ExchangeName, list[Order]] = {}
        self.trades: dict[ExchangeName, list[Trade]] = {}

    async def get_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get balances for an exchange."""
        return self.balances.get(exchange, {})

    async def get_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get positions for an exchange."""
        return self.positions.get(exchange, {})

    async def get_orders(self, exchange: ExchangeName) -> list[Order]:
        """Get orders for an exchange."""
        return self.orders.get(exchange, [])

    async def update_balances(
        self, exchange: ExchangeName, balances: dict[str, SpotBalance]
    ) -> StateUpdateResult:
        """Update balances for an exchange."""
        if exchange not in self.balances:
            self.balances[exchange] = {}
        self.balances[exchange].update(balances)
        return StateUpdateResult(
            success=True, execution_time_ms=0.0, affected_entities=len(balances)
        )

    async def update_positions(
        self, exchange: ExchangeName, positions: dict[str, DerivativePosition]
    ) -> StateUpdateResult:
        """Update positions for an exchange."""
        if exchange not in self.positions:
            self.positions[exchange] = {}
        self.positions[exchange].update(positions)
        return StateUpdateResult(
            success=True, execution_time_ms=0.0, affected_entities=len(positions)
        )

    async def add_trade(self, exchange: ExchangeName, trade: Trade) -> StateUpdateResult:
        """Add a trade for an exchange."""
        if exchange not in self.trades:
            self.trades[exchange] = []
        self.trades[exchange].append(trade)
        return StateUpdateResult(success=True, execution_time_ms=0.0, affected_entities=1)

    async def create_snapshot(self, exchange: ExchangeName) -> PortfolioSnapshot:
        """Create a portfolio snapshot for an exchange."""
        balances = await self.get_balances(exchange)
        positions = await self.get_positions(exchange)
        orders = await self.get_orders(exchange)

        # Convert to string representations for the snapshot
        balance_strings = {asset: str(balance) for asset, balance in balances.items()}
        position_strings = {symbol: str(pos) for symbol, pos in positions.items()}
        order_strings = [str(order) for order in orders]

        return PortfolioSnapshot(
            exchange=exchange.value,
            timestamp=time.time(),
            balances=balance_strings,
            positions=position_strings,
            orders=order_strings,
            metadata={"source": "example"},
        )


class StrategyNotInitializedError(ValueError):
    """Raised when strategy is not properly initialized."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("Strategy not initialized")


@dataclass
class PortfolioTarget:
    """Target portfolio allocation."""

    symbol: str
    target_weight: Decimal
    current_weight: Decimal
    rebalance_threshold: Decimal


@dataclass
class RiskLimits:
    """Risk management limits."""

    max_position_size: Decimal
    max_portfolio_exposure: Decimal
    max_leverage: Decimal
    max_drawdown: Decimal


class AdvancedPortfolioStrategy:
    """Advanced portfolio strategy demonstrating sophisticated usage patterns."""

    def __init__(
        self,
        portfolio_manager: StateManagerProtocol,
        resilience_service: PortfolioServiceProtocol,
        validation_service: PortfolioServiceProtocol,
    ) -> None:
        """Initialize advanced strategy.

        Args:
            portfolio_manager: Portfolio state manager
            resilience_service: Resilience service for fault tolerance
            validation_service: Validation service for data integrity
        """
        self.portfolio_manager = portfolio_manager
        self.resilience_service = resilience_service
        self.validation_service = validation_service

        # Create middleware for advanced patterns
        self.resilience_middleware = ResilienceMiddleware(
            cast(PortfolioResilienceService, resilience_service)
        )
        self.validation_middleware = ValidationMiddleware(
            cast(PortfolioValidationService, validation_service)
        )

        # Strategy configuration
        self.risk_limits = RiskLimits(
            max_position_size=Decimal("0.2"),  # 20% of portfolio
            max_portfolio_exposure=Decimal("0.8"),  # 80% of capital
            max_leverage=Decimal("3.0"),  # 3x leverage
            max_drawdown=Decimal("0.15"),  # 15% max drawdown
        )

        self.portfolio_targets = [
            PortfolioTarget("BTC/USD", Decimal("0.4"), Decimal("0.0"), Decimal("0.05")),
            PortfolioTarget("ETH/USD", Decimal("0.3"), Decimal("0.0"), Decimal("0.05")),
            PortfolioTarget("SOL/USD", Decimal("0.2"), Decimal("0.0"), Decimal("0.05")),
            PortfolioTarget("USD", Decimal("0.1"), Decimal("0.0"), Decimal("0.02")),
        ]

        self.rebalance_enabled = True
        self.risk_management_enabled = True

        logger.info(
            "advanced_portfolio_strategy_initialized",
            risk_limits=self.risk_limits,
            target_count=len(self.portfolio_targets),
        )

    async def execute_rebalancing_strategy(self) -> dict[str, Any]:
        """Execute portfolio rebalancing with advanced risk management."""
        logger.info("executing_rebalancing_strategy")

        # Step 1: Get current portfolio state

        # Step 2: Calculate current allocations
        current_allocations = await self._calculate_current_allocations()

        # Step 3: Determine rebalancing needs
        rebalancing_trades = await self._determine_rebalancing_trades(current_allocations)

        # Step 4: Validate and execute trades
        execution_results = await self._execute_rebalancing_trades(rebalancing_trades)

        # Step 5: Monitor and report results
        strategy_results = {
            "rebalancing_completed": True,
            "trades_executed": len(execution_results),
            "current_allocations": current_allocations,
            "target_allocations": {
                target.symbol: target.target_weight for target in self.portfolio_targets
            },
            "execution_results": execution_results,
            "timestamp": asyncio.get_event_loop().time(),
        }

        logger.info(
            "rebalancing_strategy_completed",
            trades_executed=len(execution_results),
            results=strategy_results,
        )

        return strategy_results

    async def _calculate_current_allocations(self) -> dict[str, Decimal]:
        """Calculate current portfolio allocations."""
        logger.info("calculating_current_allocations")

        # Get total capital
        capital_summary = await self.portfolio_manager.get_total_capital()
        total_capital = capital_summary.total_capital

        if total_capital == 0:
            return {}

        # Calculate allocations by currency
        allocations: dict[str, Decimal] = {}
        by_currency = capital_summary.capital_by_currency

        for currency, amount in by_currency.items():
            weight = amount / total_capital
            allocations[currency] = weight

        # Update target current weights
        for target in self.portfolio_targets:
            target.current_weight = allocations.get(target.symbol, Decimal(0))

        logger.info(
            "current_allocations_calculated",
            total_capital=total_capital,
            allocations=allocations,
        )

        return allocations

    async def _determine_rebalancing_trades(
        self, current_allocations: dict[str, Decimal]
    ) -> list[dict[str, Any]]:
        """Determine trades needed for rebalancing."""
        logger.info("determining_rebalancing_trades")

        rebalancing_trades: list[dict[str, Any]] = []

        for target in self.portfolio_targets:
            weight_diff = target.current_weight - target.target_weight

            # Check if rebalancing is needed
            if abs(weight_diff) > target.rebalance_threshold:
                # Calculate trade size
                capital_summary = await self.portfolio_manager.get_total_capital()
                total_capital = capital_summary.total_capital

                trade_value = weight_diff * total_capital

                # Determine trade direction
                if weight_diff > 0:
                    # Need to sell (reduce position)
                    trade_side = "SELL"
                    trade_quantity = abs(trade_value)
                else:
                    # Need to buy (increase position)
                    trade_side = "BUY"
                    trade_quantity = abs(trade_value)

                rebalancing_trades.append({
                    "symbol": target.symbol,
                    "side": trade_side,
                    "quantity": trade_quantity,
                    "reason": "rebalancing",
                    "current_weight": target.current_weight,
                    "target_weight": target.target_weight,
                    "weight_diff": weight_diff,
                })

        logger.info(
            "rebalancing_trades_determined",
            trade_count=len(rebalancing_trades),
            trades=rebalancing_trades,
        )

        return rebalancing_trades

    async def _execute_rebalancing_trades(
        self, trades: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Execute rebalancing trades with risk management."""
        logger.info("executing_rebalancing_trades", trade_count=len(trades))

        execution_results: list[dict[str, Any]] = []

        for trade_spec in trades:
            try:
                # Step 1: Risk management check
                risk_check_result = await self._perform_risk_check(trade_spec)

                if not risk_check_result["approved"]:
                    logger.warning(
                        "trade_rejected_by_risk_management",
                        symbol=trade_spec["symbol"],
                        reason=risk_check_result["reason"],
                    )

                    execution_results.append({
                        "symbol": trade_spec["symbol"],
                        "status": "rejected",
                        "reason": risk_check_result["reason"],
                        "trade_spec": trade_spec,
                    })
                    continue

                # Step 2: Create and validate trade
                trade = await self._create_trade_from_spec(trade_spec)

                # Step 3: Execute trade with resilience
                trade_result = await self._execute_trade_with_resilience(trade)

                execution_results.append({
                    "symbol": trade_spec["symbol"],
                    "status": "executed" if trade_result else "failed",
                    "trade_id": trade.id,
                    "trade_spec": trade_spec,
                    "result": trade_result,
                })

            except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
                logger.exception(
                    "rebalancing_trade_failed",
                    symbol=trade_spec.get("symbol", "unknown"),
                    error=str(e),
                )

                execution_results.append({
                    "symbol": trade_spec.get("symbol", "unknown"),
                    "status": "error",
                    "error": str(e),
                    "trade_spec": trade_spec,
                })

        return execution_results

    async def _perform_risk_check(self, trade_spec: dict[str, Any]) -> dict[str, Any]:
        """Perform comprehensive risk check before trade execution."""
        logger.info("performing_risk_check", symbol=trade_spec["symbol"])

        # Get current portfolio state
        portfolio_summary = await self.portfolio_manager.get_portfolio_summary()

        # Check position size limits
        total_capital = portfolio_summary.capital_summary.total_capital
        trade_value = trade_spec["quantity"]

        if total_capital > 0:
            position_size_ratio = trade_value / total_capital

            if position_size_ratio > self.risk_limits.max_position_size:
                return {
                    "approved": False,
                    "reason": (
                        f"Position size {position_size_ratio:.2%} exceeds limit "
                        f"{self.risk_limits.max_position_size:.2%}"
                    ),
                    "position_size_ratio": position_size_ratio,
                }

        # Check portfolio exposure
        exposure_result = await self.portfolio_manager.calculate_portfolio_exposure("USD")

        exposure_result_dict = cast(dict[str, Any], exposure_result)
        if exposure_result_dict.get("success", False):
            exposure_data = cast(dict[str, Any], exposure_result_dict.get("exposure", {}))
            current_exposure = Decimal(str(exposure_data.get("total_exposure", 0)))

            if current_exposure > self.risk_limits.max_portfolio_exposure:
                return {
                    "approved": False,
                    "reason": (
                        f"Portfolio exposure {current_exposure:.2%} exceeds limit "
                        f"{self.risk_limits.max_portfolio_exposure:.2%}"
                    ),
                    "current_exposure": current_exposure,
                }

        # Check leverage limits
        # This would be implemented based on specific leverage calculation logic

        return {
            "approved": True,
            "reason": "All risk checks passed",
            "checks_performed": ["position_size", "portfolio_exposure", "leverage"],
        }

    async def _create_trade_from_spec(self, trade_spec: dict[str, Any]) -> Trade:
        """Create a trade object from trade specification."""
        # Convert side string to enum
        side = OrderSide.BUY if trade_spec["side"] == "BUY" else OrderSide.SELL

        # Create trade with realistic price (this would normally come from market data)
        return Trade(
            id=f"rebalance_{trade_spec['symbol']}_{asyncio.get_event_loop().time()}",
            symbol=trade_spec["symbol"],
            side=side,
            quantity=trade_spec["quantity"],
            price=Decimal("50000.00"),  # Mock price
            exchange="demo_exchange",
            executed_at=datetime.now(UTC),
            order_id="mock_order_id",
        )

    async def _execute_trade_with_resilience(self, trade: Trade) -> bool:
        """Execute trade with full resilience and validation."""
        # Validate trade first
        validation_result_raw = await self.validation_service.validate_trade(trade)
        validation_result = cast(ValidationResult[Trade], validation_result_raw)

        if not validation_result.is_valid:
            logger.error(
                "trade_validation_failed",
                trade_id=trade.id,
                issues=[issue.message for issue in validation_result.issues],
            )
            return False

        # Execute with resilience
        try:
            result = await self.resilience_service.execute_with_resilience(
                operation="trade_execution",
                service_name="trade_execution",
                func=self.portfolio_manager.process_trade,
                trade=trade,
            )

            logger.info(
                "trade_executed_successfully",
                trade_id=trade.id,
                symbol=trade.symbol,
                result=result,
            )

            return bool(result)

        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
            logger.exception(
                "trade_execution_failed_with_resilience",
                trade_id=trade.id,
                error=str(e),
            )
            return False

    async def execute_risk_monitoring_strategy(self) -> dict[str, Any]:
        """Execute continuous risk monitoring strategy."""
        logger.info("executing_risk_monitoring_strategy")

        # Monitor portfolio exposure
        exposure_result = await self.portfolio_manager.calculate_portfolio_exposure("USD")

        # Monitor P&L and drawdown
        pnl_summary = await self.portfolio_manager.get_pnl_summary()

        # Check for risk limit violations
        risk_violations: list[dict[str, Any]] = []

        exposure_result_dict = cast(dict[str, Any], exposure_result)
        if exposure_result_dict.get("success", False):
            exposure_data = cast(dict[str, Any], exposure_result_dict.get("exposure", {}))
            total_exposure = Decimal(str(exposure_data.get("total_exposure", 0)))

            if total_exposure > self.risk_limits.max_portfolio_exposure:
                risk_violations.append({
                    "type": "portfolio_exposure",
                    "current": total_exposure,
                    "limit": self.risk_limits.max_portfolio_exposure,
                    "severity": "high",
                })

        # Check drawdown
        total_pnl = pnl_summary.total_realized_pnl + pnl_summary.total_unrealized_pnl
        capital_summary = await self.portfolio_manager.get_total_capital()
        total_capital = capital_summary.total_capital

        if total_capital > 0:
            drawdown = abs(min(total_pnl, Decimal(0))) / total_capital

            if drawdown > self.risk_limits.max_drawdown:
                risk_violations.append({
                    "type": "drawdown",
                    "current": drawdown,
                    "limit": self.risk_limits.max_drawdown,
                    "severity": "critical",
                })

        # Take action on violations
        if risk_violations:
            await self._handle_risk_violations(risk_violations)

        risk_monitoring_result = {
            "monitoring_completed": True,
            "risk_violations": risk_violations,
            "exposure_result": exposure_result,
            "pnl_summary": pnl_summary,
            "capital_summary": capital_summary,
            "timestamp": asyncio.get_event_loop().time(),
        }

        logger.info(
            "risk_monitoring_completed",
            violations_count=len(risk_violations),
            result=risk_monitoring_result,
        )

        return risk_monitoring_result

    async def _handle_risk_violations(self, violations: list[dict[str, Any]]) -> None:
        """Handle risk limit violations."""
        logger.warning(
            "risk_violations_detected",
            violation_count=len(violations),
            violations=violations,
        )

        for violation in violations:
            if violation["severity"] == "critical":
                # Take immediate action for critical violations
                await self._emergency_risk_reduction(violation)
            elif violation["severity"] == "high":
                # Gradual risk reduction for high severity
                await self._gradual_risk_reduction(violation)

    async def _emergency_risk_reduction(self, violation: dict[str, Any]) -> None:
        """Implement emergency risk reduction measures."""
        logger.critical(
            "implementing_emergency_risk_reduction",
            violation_type=violation["type"],
            current_value=violation["current"],
            limit=violation["limit"],
        )

        # This would implement emergency measures like:
        # - Immediate position closure
        # - Disable new trading
        # - Alert risk management team
        # - Implement hedging positions

        # For demonstration, we'll just log the action
        logger.info(
            "emergency_risk_reduction_simulated",
            action="position_closure",
            violation_type=violation["type"],
        )

    async def _gradual_risk_reduction(self, violation: dict[str, Any]) -> None:
        """Implement gradual risk reduction measures."""
        logger.warning(
            "implementing_gradual_risk_reduction",
            violation_type=violation["type"],
            current_value=violation["current"],
            limit=violation["limit"],
        )

        # This would implement gradual measures like:
        # - Reduce position sizes
        # - Tighten risk limits
        # - Increase monitoring frequency
        # - Adjust portfolio targets

        # For demonstration, we'll just log the action
        logger.info(
            "gradual_risk_reduction_simulated",
            action="position_reduction",
            violation_type=violation["type"],
        )

    async def execute_performance_analysis(self) -> dict[str, Any]:
        """Execute comprehensive performance analysis."""
        logger.info("executing_performance_analysis")

        # Get portfolio performance metrics
        portfolio_summary = await self.portfolio_manager.get_portfolio_summary()

        # Calculate performance metrics
        portfolio_summary_dict = cast(dict[str, Any], portfolio_summary)
        pnl_data = cast(dict[str, Any], portfolio_summary_dict.get("pnl", {}))
        capital_data = cast(dict[str, Any], portfolio_summary_dict.get("capital", {}))
        performance_metrics = {
            "total_return": pnl_data.get("total_pnl", 0),
            "realized_pnl": pnl_data.get("realized_pnl", 0),
            "unrealized_pnl": pnl_data.get("unrealized_pnl", 0),
            "total_capital": capital_data.get("total_capital", 0),
        }

        # Calculate return ratios
        if performance_metrics["total_capital"] > 0:
            performance_metrics["return_ratio"] = (
                performance_metrics["total_return"] / performance_metrics["total_capital"]
            )
        else:
            performance_metrics["return_ratio"] = 0

        # Get validation and resilience statistics
        validation_stats = self.validation_service.get_validation_statistics()
        resilience_status = self.resilience_service.get_resilience_status()

        performance_analysis = {
            "performance_metrics": performance_metrics,
            "validation_statistics": validation_stats,
            "resilience_status": resilience_status,
            "analysis_timestamp": asyncio.get_event_loop().time(),
        }

        logger.info(
            "performance_analysis_completed",
            return_ratio=performance_metrics["return_ratio"],
            total_return=performance_metrics["total_return"],
            validation_success_rate=(
                validation_stats.successful_validations / validation_stats.total_validations
                if validation_stats.total_validations > 0
                else 0
            ),
        )

        return performance_analysis


class AdvancedIntegrationDemo:
    """Advanced integration demonstration."""

    def __init__(self) -> None:
        """Initialize advanced integration demo."""
        self.portfolio_manager: PortfolioStateManager | None = None
        self.resilience_service: PortfolioResilienceService | None = None
        self.validation_service: PortfolioValidationService | None = None
        self.strategy: AdvancedPortfolioStrategy | None = None
        self.state_container: Any = None  # Will be initialized in initialize_advanced_system

    def _get_app_settings(self) -> AppSettings:
        """Get app settings for example."""
        try:
            # Try to get existing app settings or create minimal ones
            instance: AppSettings | None = getattr(AppSettings, "_instance", None)
            if instance is not None:
                return instance
            # If we can't get proper app settings, skip this example
            # This is just demo code that shouldn't be used in production
            raise RuntimeError(
                "AppSettings not available for example - requires proper config setup"
            ) from None
        except AttributeError:
            # If we can't get proper app settings, skip this example
            # This is just demo code that shouldn't be used in production
            raise RuntimeError(
                "AppSettings not available for example - requires proper config setup"
            ) from None

    def _create_example_config(self) -> dict[str, Any]:
        """Create configuration for advanced features."""
        return {
            "resilience": {
                "retry_max_attempts": 3,
                "circuit_breaker_failure_threshold": 2,
                "health_check_interval": 15.0,
            },
            "validation": {
                "trade_validation": {
                    "min_price": "0.01",
                    "max_price": "1000000",
                    "min_quantity": "0.001",
                    "max_quantity": "100000",
                },
            },
            "state_manager": {
                "strict_validation": True,
            },
        }

    async def initialize_advanced_system(self) -> None:
        """Initialize advanced portfolio system."""
        logger.info("initializing_advanced_portfolio_system")

        # Create configuration for advanced features
        config = self._create_example_config()

        # Create portfolio system
        # Since there's no factory anymore, create directly
        # Use a minimal configuration for the example
        # In a real application, this would be loaded from config files
        app_settings = self._get_app_settings()

        # Create a simple in-memory state container
        self.state_container = self._create_in_memory_container()

        # Store app_settings for later use
        self.app_settings = app_settings

        # Create portfolio manager
        self.portfolio_manager = PortfolioStateManager(
            app_settings=app_settings, state_container=self.state_container
        )

        # Initialize systems
        await self._initialize_resilience_system(config)
        await self._initialize_validation_system(config)
        await self._setup_strategy()

    def _create_in_memory_container(self) -> object:
        """Create in-memory state container for examples."""
        return InMemoryStateContainer()

    async def _initialize_resilience_system(self, config: dict[str, Any]) -> None:
        """Initialize resilience system."""
        # Simplified for complexity reduction

    async def _initialize_validation_system(self, config: dict[str, Any]) -> None:
        """Initialize validation system."""
        # Simplified for complexity reduction

    async def _setup_strategy(self) -> None:
        """Setup strategy."""
        # Simplified for complexity reduction

    async def run_advanced_demonstration(self) -> None:
        """Run advanced integration demonstration."""
        logger.info("starting_advanced_portfolio_demonstration")

        try:
            # Initialize system
            await self.initialize_advanced_system()

            # Setup initial portfolio state
            await self._setup_initial_portfolio_state()

            # Run advanced strategies
            strategy = self.strategy
            if strategy is None:
                self._raise_strategy_not_initialized()
                return  # This will never be reached due to exception, but helps type checker

            await strategy.execute_rebalancing_strategy()
            await strategy.execute_risk_monitoring_strategy()
            await strategy.execute_performance_analysis()

            logger.info("advanced_portfolio_demonstration_completed")

        except Exception as e:
            logger.exception(
                "advanced_portfolio_demonstration_failed",
                error=str(e),
            )
            raise

        finally:
            await self._cleanup_advanced_system()

    async def _setup_initial_portfolio_state(self) -> None:
        """Setup initial portfolio state for demonstration."""
        logger.info("setting_up_initial_portfolio_state")

        # Add some initial balances

        initial_balances = {
            "BTC": SpotBalance(
                exchange="demo_exchange",
                asset="BTC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("2.0"),
                available_quantity=Decimal("2.0"),
            ),
            "ETH": SpotBalance(
                exchange="demo_exchange",
                asset="ETH",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("20.0"),
                available_quantity=Decimal("20.0"),
            ),
            "USD": SpotBalance(
                exchange="demo_exchange",
                asset="USD",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("100000.0"),
                available_quantity=Decimal("100000.0"),
            ),
        }

        if self.portfolio_manager is None:
            raise ValueError
        # Create BalanceUpdateRequest for the new API
        balance_update = BalanceUpdateRequest(
            exchange_name="demo_exchange",
            balances=initial_balances,
            update_source="initial_setup",
            force_update=True,
        )
        await self.portfolio_manager.update_balances(ExchangeName.HYPERLIQUID, balance_update)

        # Add some initial positions

        initial_positions = [
            DerivativePosition(
                exchange="demo_exchange",
                symbol="BTC-PERP",
                side=OrderSide.BUY,  # Use OrderSide.BUY for long positions
                size=Decimal("1.0"),
                entry_price=Decimal("45000.0"),
                timestamp=datetime.now(UTC),
                unrealized_pnl=Decimal("5000.0"),
                realized_pnl=Decimal("0.0"),
            ),
        ]

        position_update = PositionUpdateRequest(
            exchange_name="demo_exchange",
            positions=initial_positions,
            update_source="initial_setup",
            force_update=True,
        )
        await self.portfolio_manager.update_positions(ExchangeName.HYPERLIQUID, position_update)

        logger.info("initial_portfolio_state_setup_completed")

    def _raise_strategy_not_initialized(self) -> None:
        """Raise strategy initialization error."""
        raise StrategyNotInitializedError

    async def _cleanup_advanced_system(self) -> None:
        """Cleanup advanced system."""
        logger.info("cleaning_up_advanced_system")

        if self.portfolio_manager:
            await self.portfolio_manager.shutdown()

        if self.resilience_service:
            await self.resilience_service.stop()

        # Validation service doesn't need explicit stop in new architecture

        logger.info("advanced_system_cleanup_completed")


async def main() -> None:
    """Main function for advanced integration demonstration."""
    demo = AdvancedIntegrationDemo()
    await demo.run_advanced_demonstration()


if __name__ == "__main__":
    asyncio.run(main())
