"""Risk service for assessment and position sizing.

This module provides the main risk service that orchestrates risk assessment
using modular components for position sizing, limit checking, and validation.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.logic.risk.drawdown_monitor import DrawdownMonitor
from cyberdelta.logic.risk.limit_checker import LimitChecker
from cyberdelta.logic.risk.portfolio_analyzer import PortfolioAnalyzer
from cyberdelta.logic.risk.position_sizer import PositionSizer
from cyberdelta.logic.risk.risk_checker import RiskChecker
from cyberdelta.models import DerivativePosition, TradeSignal
from cyberdelta.models.risk.assessment import PositionSize, RiskAssessment


logger = get_logger(__name__)


class RiskService:
    """Risk assessment orchestrator using modular components.

    This service orchestrates risk assessment using specialized components:
    - PositionSizer: Handles position size calculations
    - RiskChecker: Validates basic risk limits and signal quality
    - LimitChecker: Enforces position and concentration limits
    - PortfolioAnalyzer: Provides portfolio data and analysis
    - DrawdownMonitor: Monitors portfolio drawdown limits

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - Modular design for separation of concerns
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
    ) -> None:
        """Initialize risk service with configuration and dependencies.

        Args:
            config: Application settings containing all risk configuration
            portfolio_service: Portfolio service for state access
        """
        self.config = config
        self._portfolio_service = portfolio_service

        # Initialize modular components
        self._position_sizer = PositionSizer(config)
        self._risk_checker = RiskChecker(config)
        self._limit_checker = LimitChecker(config, portfolio_service)
        self._portfolio_analyzer = PortfolioAnalyzer(portfolio_service)
        self._drawdown_monitor = DrawdownMonitor(config, portfolio_service)

        logger.info(
            "risk_service_initialized",
            sizing_method=config.risk.sizing.method,
            drawdown_monitoring_enabled=True,
            max_drawdown_pct=float(config.risk.global_risk.max_drawdown_pct),
            components_initialized=5,
        )

    async def assess_signal(self, signal: TradeSignal) -> RiskAssessment:
        """Assess risk for trading signal.

        Args:
            signal: Trading signal to assess


        Returns:
            Risk assessment with approval status and calculated position size


        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL limits from config, NO hardcoded values
        - Explicit violation messages
        - Uses Symbol/ExchangeName types
        - Returns Decimal values
        """
        logger.debug(
            "risk_assessment_starting",
            signal_id=signal.signal_id,
            symbol=signal.symbol.value,
            exchange=(
                signal.exchange[0].value
                if isinstance(signal.exchange, list)
                else signal.exchange.value
            ),
            side=signal.side.value,
        )

        try:
            # Get portfolio data and calculate position size
            (
                exchange_name,
                position,
                total_equity,
                current_exposure,
            ) = await self._portfolio_analyzer.get_portfolio_data(signal)

            position_size = self._position_sizer.calculate_position_size(
                signal, total_equity, current_exposure
            )

            # Run all risk checks
            limit_violations = await self._run_all_risk_checks(
                signal, exchange_name, position, position_size
            )

            # Calculate max loss estimate
            max_loss_usd = self._risk_checker.calculate_max_loss(
                position_size, signal.price, signal.stop_loss
            )

            assessment = RiskAssessment(
                signal_id=signal.signal_id,
                approved=len(limit_violations) == 0,
                position_size=position_size,
                current_exposure=current_exposure,
                limit_violations=limit_violations,
                max_loss_usd=max_loss_usd,
            )

            logger.info(
                "risk_assessment_completed",
                signal_id=signal.signal_id,
                approved=assessment.approved,
                position_size_usd=float(position_size.value_usd),
                violation_count=len(limit_violations),
                current_exposure=float(current_exposure),
            )

        except Exception as e:
            logger.exception(
                "risk_assessment_failed",
                signal_id=signal.signal_id,
                error=str(e),
            )
            raise
        else:
            return assessment

    async def _run_all_risk_checks(
        self,
        signal: TradeSignal,
        exchange_name: ExchangeName,
        position: DerivativePosition | None,
        position_size: PositionSize,
    ) -> list[str]:
        """Run all risk checks and return violations.

        Args:
            signal: Trading signal being assessed
            exchange_name: Exchange for the signal
            position: Current position object
            position_size: Calculated position size

        Returns:
            List of limit violations
        """
        limit_violations: list[str] = []

        # Get portfolio state for basic checks
        portfolio_state = await self._portfolio_service.get_state()
        if not portfolio_state:
            return limit_violations

        total_equity = portfolio_state.total_equity_usd or Decimal(0)
        current_exposure = self._portfolio_analyzer.calculate_exposure(position, signal.price)

        # Basic limit checks
        self._risk_checker.check_basic_limits(
            position_size, current_exposure, total_equity, limit_violations
        )

        # Signal-specific checks
        self._risk_checker.check_signal_limits(signal, limit_violations)

        # Position limits
        position_limit_violations = await self._limit_checker.check_position_limits(
            signal.symbol, exchange_name
        )
        limit_violations.extend(position_limit_violations)

        # Concentration limits
        concentration_violations = await self._limit_checker.check_concentration_limits(
            signal.symbol, position_size.value_usd
        )
        limit_violations.extend(concentration_violations)

        # Drawdown limits
        drawdown_violations = await self._drawdown_monitor.check_drawdown_limits()
        limit_violations.extend(drawdown_violations)

        # Block new positions if drawdown violated
        if self._drawdown_monitor.should_block_new_positions():
            limit_violations.append(
                f"New positions blocked due to drawdown violation: "
                f"{self._drawdown_monitor.get_current_drawdown_pct():.2f}% > "
                f"{self._drawdown_monitor.get_max_allowed_drawdown_pct():.2f}%"
            )

        return limit_violations

    async def update_drawdown_monitoring(self, portfolio_value: Decimal | None = None) -> None:
        """Update drawdown monitoring with current portfolio value.

        Args:
            portfolio_value: Current portfolio value (if None, fetches from service)


        Note:
            Following CODING_STANDARDS.md:
            - Delegates to DrawdownMonitor for calculations
            - NO assumptions about value availability
            - Should be called regularly to maintain accurate monitoring
        """
        try:
            await self._drawdown_monitor.update_portfolio_value(portfolio_value)
        except Exception as e:
            logger.exception(
                "drawdown_monitoring_update_failed",
                error=str(e),
            )

    def get_drawdown_status(self) -> dict[str, object]:
        """Get current drawdown monitoring status.

        Returns:
            Dictionary with drawdown status and configuration


        Note:
            Following CODING_STANDARDS.md:
            - Returns structured status from DrawdownMonitor
            - Configuration context included
        """
        return self._drawdown_monitor.get_drawdown_status()

    def is_drawdown_violated(self) -> bool:
        """Check if drawdown limits are currently violated.

        Returns:
            True if drawdown exceeds configured limits


        Note:
            Following CODING_STANDARDS.md:
            - Uses DrawdownMonitor state
            - NO assumptions about violation handling
        """
        return self._drawdown_monitor.is_drawdown_violated()

    async def get_historical_max_drawdown(self) -> Decimal | None:
        """Get historical maximum drawdown over configured lookback period.

        Returns:
            Maximum drawdown percentage, None if insufficient data


        Note:
            Following CODING_STANDARDS.md:
            - Returns Decimal, NOT float
            - Based on configured lookback period
        """
        return await self._drawdown_monitor.get_historical_max_drawdown()

    def reset_drawdown_tracking(self) -> None:
        """Reset drawdown tracking state.

        Note:
            Following CODING_STANDARDS.md:
            - Explicit state reset for emergency situations
            - Logs reset action for audit trail
        """
        logger.warning("risk_service_drawdown_reset_requested", reason="manual_intervention")
        self._drawdown_monitor.reset_drawdown_tracking()
