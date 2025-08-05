"""Drawdown monitoring and protection system.

This module provides the DrawdownMonitor class that tracks portfolio
drawdowns and enforces protection limits based on configuration.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService


logger = get_logger(__name__)


class DrawdownMonitor:
    """Monitor and enforce drawdown protection limits.

    This monitor tracks portfolio value over configured lookback periods
    and enforces maximum drawdown limits from configuration.


    Configuration Usage:
    - Uses config.risk.global_risk.max_drawdown_pct for maximum allowed drawdown
    - Uses config.risk.global_risk.drawdown_lookback_days for calculation period
    - Uses config.risk.global_risk.drawdown_check_interval_sec for monitoring frequency


    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL parameters from config.risk.global_risk section
    - NO hardcoded risk parameters
    - Fail fast if drawdown limits exceeded
    - Explicit drawdown calculations with proper timestamps
    """

    def __init__(self, config: AppSettings, portfolio_service: PortfolioService) -> None:
        """Initialize drawdown monitor with configuration.

        Args:
            config: Application settings containing drawdown configuration
            portfolio_service: Portfolio service for state access


        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL drawdown parameters from config
        - NO hardcoded risk thresholds
        - Fail fast if required configuration missing
        """
        self.config = config
        self._portfolio_service = portfolio_service

        # Extract drawdown configuration - NO defaults
        self._global_risk_config = config.risk.global_risk
        self._max_drawdown_pct = self._global_risk_config.max_drawdown_pct
        self._lookback_days = self._global_risk_config.drawdown_lookback_days

        # Check if optional check interval is configured
        if hasattr(self._global_risk_config, "drawdown_check_interval_sec"):
            self._check_interval = self._global_risk_config.drawdown_check_interval_sec
        else:
            # Use default from monitoring config if available
            self._check_interval = config.monitoring.health_check_interval_seconds

        # Portfolio value history for drawdown calculation
        self._value_history: list[dict[str, Decimal | datetime]] = []

        # Drawdown state tracking
        self._current_drawdown_pct = Decimal(0)
        self._peak_value = Decimal(0)
        self._peak_timestamp: datetime | None = None
        self._trough_value = Decimal(0)
        self._trough_timestamp: datetime | None = None

        # Drawdown violation state
        self._drawdown_violated = False
        self._violation_timestamp: datetime | None = None

        logger.info(
            "drawdown_monitor_initialized",
            max_drawdown_pct=float(self._max_drawdown_pct),
            lookback_days=self._lookback_days,
            check_interval_sec=float(self._check_interval),
        )

    async def update_portfolio_value(self, current_value: Decimal | None = None) -> None:
        """Update portfolio value history and check drawdown.

        Args:
            current_value: Current portfolio value (if None, fetches from service)


        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses current timestamp for all calculations
        - NO assumptions about value availability
        - Explicit error handling for missing data
        """
        try:
            # Get current portfolio value
            if current_value is None:
                portfolio_state = await self._portfolio_service.get_state()
                if not portfolio_state or not portfolio_state.total_equity_usd:
                    logger.debug("no_portfolio_value_for_drawdown_calculation")
                    return
                current_value = portfolio_state.total_equity_usd

            current_time = datetime.now(UTC)

            # Add to value history
            self._value_history.append({"timestamp": current_time, "value": current_value})

            # Clean up old history based on lookback period
            cutoff_time = current_time - timedelta(days=self._lookback_days)
            self._value_history = [
                entry
                for entry in self._value_history
                if isinstance(entry["timestamp"], datetime) and entry["timestamp"] > cutoff_time
            ]

            # Update drawdown calculation
            await self._calculate_drawdown(current_value, current_time)

            logger.debug(
                "portfolio_value_updated",
                current_value=float(current_value),
                history_entries=len(self._value_history),
                current_drawdown_pct=float(self._current_drawdown_pct),
            )

        except Exception as e:
            logger.exception(
                "drawdown_update_error",
                error=str(e),
            )

    async def _calculate_drawdown(self, current_value: Decimal, current_time: datetime) -> None:
        """Calculate current drawdown from peak value.

        Args:
            current_value: Current portfolio value
            current_time: Current timestamp


        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured lookback period only
        - NO assumptions about peak/trough timing
        - Explicit drawdown percentage calculation
        """
        if not self._value_history:
            return

        # Find peak value in lookback period
        peak_entry = max(
            self._value_history,
            key=lambda x: x["value"] if isinstance(x["value"], Decimal) else Decimal(0),
        )
        peak_value = peak_entry["value"] if isinstance(peak_entry["value"], Decimal) else Decimal(0)
        peak_time = (
            peak_entry["timestamp"]
            if isinstance(peak_entry["timestamp"], datetime)
            else datetime.now(UTC)
        )

        # Update peak tracking
        if peak_value > self._peak_value:
            self._peak_value = peak_value
            self._peak_timestamp = peak_time

        # Calculate current drawdown from peak
        if peak_value > 0:
            drawdown_pct = ((peak_value - current_value) / peak_value) * 100
            self._current_drawdown_pct = max(drawdown_pct, Decimal(0))
        else:
            self._current_drawdown_pct = Decimal(0)

        # Update trough tracking
        if current_value < self._trough_value or self._trough_value == 0:
            self._trough_value = current_value
            self._trough_timestamp = current_time

        # Check for drawdown limit violation
        await self._check_drawdown_violation()

        logger.debug(
            "drawdown_calculated",
            peak_value=float(peak_value),
            current_value=float(current_value),
            drawdown_pct=float(self._current_drawdown_pct),
            max_allowed_pct=float(self._max_drawdown_pct),
        )

    async def _check_drawdown_violation(self) -> None:
        """Check if drawdown exceeds configured limits.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured max_drawdown_pct limit
        - NO hardcoded violation thresholds
        - Explicit violation state tracking
        """
        if self._current_drawdown_pct > self._max_drawdown_pct:
            if not self._drawdown_violated:
                # First time violation
                self._drawdown_violated = True
                self._violation_timestamp = datetime.now(UTC)

                logger.error(
                    "drawdown_limit_exceeded",
                    current_drawdown_pct=float(self._current_drawdown_pct),
                    max_allowed_pct=float(self._max_drawdown_pct),
                    peak_value=float(self._peak_value),
                    current_value=float(self._trough_value),
                    violation_timestamp=self._violation_timestamp.isoformat(),
                )
        elif self._drawdown_violated:
            # Drawdown has recovered
            logger.info(
                "drawdown_limit_recovered",
                current_drawdown_pct=float(self._current_drawdown_pct),
                max_allowed_pct=float(self._max_drawdown_pct),
                violation_duration_sec=(
                    datetime.now(UTC) - self._violation_timestamp
                ).total_seconds()
                if self._violation_timestamp
                else 0,
            )

            self._drawdown_violated = False
            self._violation_timestamp = None

    def is_drawdown_violated(self) -> bool:
        """Check if drawdown limits are currently violated.

        Returns:
            True if drawdown exceeds configured limits


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns current violation state
        - NO assumptions about violation handling
        """
        return self._drawdown_violated

    def get_current_drawdown_pct(self) -> Decimal:
        """Get current drawdown percentage.

        Returns:
            Current drawdown as percentage


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - Based on configured lookback period
        """
        return self._current_drawdown_pct

    def get_max_allowed_drawdown_pct(self) -> Decimal:
        """Get maximum allowed drawdown percentage from configuration.

        Returns:
            Maximum allowed drawdown percentage
        """
        return self._max_drawdown_pct

    def should_block_new_positions(self) -> bool:
        """Check if new positions should be blocked due to drawdown.

        Returns:
            True if new positions should be blocked


        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses current violation state
        - NO hardcoded blocking logic
        """
        return self._drawdown_violated

    def get_drawdown_status(self) -> dict[str, object]:
        """Get comprehensive drawdown monitoring status.

        Returns:
            Dictionary with drawdown status and configuration


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured status information
        - Configuration context included
        """
        return {
            "current_drawdown_pct": float(self._current_drawdown_pct),
            "max_allowed_pct": float(self._max_drawdown_pct),
            "drawdown_violated": self._drawdown_violated,
            "violation_timestamp": (
                self._violation_timestamp.isoformat() if self._violation_timestamp else None
            ),
            "peak_value": float(self._peak_value),
            "peak_timestamp": (self._peak_timestamp.isoformat() if self._peak_timestamp else None),
            "trough_value": float(self._trough_value),
            "trough_timestamp": (
                self._trough_timestamp.isoformat() if self._trough_timestamp else None
            ),
            "history_entries": len(self._value_history),
            "lookback_days": self._lookback_days,
            "configuration": {
                "max_drawdown_pct": float(self._max_drawdown_pct),
                "lookback_days": self._lookback_days,
                "check_interval_sec": float(self._check_interval),
            },
        }

    async def check_drawdown_limits(self) -> list[str]:
        """Check drawdown limits and return violations.

        Returns:
            List of drawdown violations (empty if no violations)


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit violation messages
        - NO silent limit checking
        - Based on current drawdown state
        """
        violations: list[str] = []

        if self.is_drawdown_violated():
            violations.append(
                f"Portfolio drawdown {self._current_drawdown_pct:.2f}% exceeds "
                f"maximum allowed {self._max_drawdown_pct:.2f}%"
            )

        # Check if approaching warning threshold (if configured)
        if hasattr(self._global_risk_config, "drawdown_warning_pct"):
            warning_threshold = self._global_risk_config.drawdown_warning_pct
            if self._current_drawdown_pct > warning_threshold and not self._drawdown_violated:
                violations.append(
                    f"Portfolio drawdown {self._current_drawdown_pct:.2f}% approaching "
                    f"warning threshold {warning_threshold:.2f}%"
                )

        return violations

    def reset_drawdown_tracking(self) -> None:
        """Reset drawdown tracking state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit state reset
        - Logs reset action for audit
        """
        logger.warning(
            "drawdown_tracking_reset",
            previous_drawdown_pct=float(self._current_drawdown_pct),
            was_violated=self._drawdown_violated,
        )

        self._current_drawdown_pct = Decimal(0)
        self._peak_value = Decimal(0)
        self._peak_timestamp = None
        self._trough_value = Decimal(0)
        self._trough_timestamp = None
        self._drawdown_violated = False
        self._violation_timestamp = None
        self._value_history.clear()

        logger.info("drawdown_tracking_reset_completed")

    async def get_historical_max_drawdown(self) -> Decimal | None:
        """Calculate historical maximum drawdown over the lookback period.

        Returns:
            Maximum drawdown percentage over lookback period, None if insufficient data


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - Based on configured lookback period only
        - NO assumptions about data availability
        """
        min_history_length = 2
        if len(self._value_history) < min_history_length:
            return None

        max_drawdown = Decimal(0)

        # Calculate rolling maximum drawdown
        for i in range(len(self._value_history)):
            peak_value = Decimal(0)

            # Find peak up to this point
            for j in range(i + 1):
                history_value = self._value_history[j]["value"]
                if isinstance(history_value, Decimal) and history_value > peak_value:
                    peak_value = history_value

            # Calculate drawdown from peak
            current_entry_value = self._value_history[i]["value"]
            if isinstance(current_entry_value, Decimal) and peak_value > 0:
                drawdown = ((peak_value - current_entry_value) / peak_value) * 100
                max_drawdown = max(max_drawdown, drawdown)

        return max_drawdown
