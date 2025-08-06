"""Risk validation and checking logic.

This module handles basic risk validation checks for trading signals
including position limits, signal quality, and loss calculations.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models import TradeSignal
from cyberdelta.models.risk.assessment import PositionSize


logger = get_logger(__name__)


class RiskChecker:
    """Risk validation checker using configured limits.

    This class handles:
    - Basic position and exposure limit checks
    - Signal-specific validation (profitability, price sanity)
    - Maximum loss calculations

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL limits from AppSettings configuration
    - Explicit violation messages
    - NO hardcoded risk parameters
    - Returns Decimal values, NOT float
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize risk checker with configuration.

        Args:
            config: Application settings containing risk configuration
        """
        self.config = config
        self._global_risk = config.risk.global_risk
        self._checker_config = config.risk.checkers

        # Cache frequently accessed values for performance
        self._max_position_usd = self._global_risk.max_position_usd
        self._max_exposure_usd = self._global_risk.max_total_exposure_usd

        logger.debug(
            "risk_checker_initialized",
            max_position_usd=float(self._max_position_usd),
            max_exposure_usd=float(self._max_exposure_usd),
            profitability_enabled=self._checker_config.enable_profitability,
            price_sanity_enabled=self._checker_config.enable_price_sanity,
        )

    def check_basic_limits(
        self,
        position_size: PositionSize,
        current_exposure: Decimal,
        total_equity: Decimal,
        limit_violations: list[str],
    ) -> None:
        """Check basic position and exposure limits.

        Args:
            position_size: Calculated position size
            current_exposure: Current exposure amount
            total_equity: Total portfolio equity
            limit_violations: List to append violations to
        """
        # Check 1: Max position size from config.risk.global_risk
        if position_size.value_usd > self._max_position_usd:
            limit_violations.append(
                f"Position size ${position_size.value_usd} exceeds max ${self._max_position_usd}"
            )

        # Check 2: Total exposure limit from config.risk.global_risk
        new_exposure = current_exposure + position_size.value_usd
        if new_exposure > self._max_exposure_usd:
            limit_violations.append(
                f"New exposure ${new_exposure} would exceed max ${self._max_exposure_usd}"
            )

        # Check 3: Minimum equity check (ensure we have funds)
        if total_equity < position_size.value_usd:
            limit_violations.append(
                f"Insufficient equity ${total_equity} for position ${position_size.value_usd}"
            )

    def check_signal_limits(self, signal: TradeSignal, limit_violations: list[str]) -> None:
        """Check signal-specific limits like profitability and price sanity.

        Args:
            signal: Trading signal to check
            limit_violations: List to append violations to
        """
        # Check 4: Additional checks from config.risk.checkers if enabled
        if self._checker_config.enable_profitability:
            min_profit = self._checker_config.thresholds.min_profitability
            # TradeSignal doesn't have expected_profit - calculate from take_profit if provided
            if signal.take_profit and signal.price:
                # Calculate expected profit percentage
                expected_profit = (signal.take_profit - signal.price) / signal.price
                if expected_profit < min_profit:
                    limit_violations.append(
                        f"Expected profit {expected_profit:.4f} below minimum {min_profit}"
                    )

        # Check 5: Price sanity checks if enabled
        if self._checker_config.enable_price_sanity:
            thresholds = self._checker_config.thresholds
            if signal.price and (
                signal.price < thresholds.min_price or signal.price > thresholds.max_price
            ):
                limit_violations.append(
                    f"Price {signal.price} outside valid range ["
                    f"{thresholds.min_price}, {thresholds.max_price}]"
                )

    def calculate_max_loss(
        self, position_size: PositionSize, entry_price: Decimal | None, stop_loss: Decimal | None
    ) -> Decimal | None:
        """Calculate maximum potential loss for a position.

        Args:
            position_size: Calculated position size
            entry_price: Entry price for the position
            stop_loss: Stop loss price (if set)

        Returns:
            Maximum loss in USD, None if cannot calculate

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about stop loss presence
        """
        if not entry_price or not stop_loss or position_size.quantity <= 0:
            return None

        # Calculate loss per unit
        loss_per_unit = abs(entry_price - stop_loss)

        # Return total max loss
        return loss_per_unit * position_size.quantity
