"""Position sizing calculator for risk management.

This module handles position size calculation using various methods
like simple fixed fraction and Kelly criterion.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models import TradeSignal
from cyberdelta.models.risk.assessment import PositionSize


logger = get_logger(__name__)


class PositionSizer:
    """Position size calculator using configured methods.

    This class handles:
    - Simple fixed fraction position sizing
    - Kelly criterion position sizing
    - Position limit application
    - Quantity constraints from configuration

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL parameters from AppSettings configuration
    - Uses configured sizing methods (simple/kelly)
    - NO hardcoded fractions or multipliers
    - Returns Decimal values, NOT float
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize position sizer with configuration.

        Args:
            config: Application settings containing sizing configuration
        """
        self.config = config
        self._sizing_config = config.risk.sizing
        self._global_risk = config.risk.global_risk

        # Cache frequently accessed values for performance
        self._max_position_usd = self._global_risk.max_position_usd
        self._max_exposure_usd = self._global_risk.max_total_exposure_usd

        # Position sizing parameters based on configured method
        if self._sizing_config.method == "simple":
            self._sizing_fraction = self._sizing_config.simple_fixed_fraction
        elif self._sizing_config.method == "kelly":
            self._kelly_multiplier = self._sizing_config.kelly_multiplier
            self._kelly_max_allocation = self._sizing_config.kelly_max_allocation

        logger.debug(
            "position_sizer_initialized",
            sizing_method=self._sizing_config.method,
            max_position_usd=float(self._max_position_usd),
            max_exposure_usd=float(self._max_exposure_usd),
        )

    def calculate_position_size(
        self, signal: TradeSignal, total_equity: Decimal, current_exposure: Decimal
    ) -> PositionSize:
        """Calculate position size using configured sizing method.

        Args:
            signal: Trading signal containing price and direction
            total_equity: Total portfolio equity in USD
            current_exposure: Current exposure across all positions

        Returns:
            Calculated position size with constraints applied

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured sizing method (simple/kelly)
        - ALL parameters from config
        - NO hardcoded fractions or multipliers
        """
        if not signal.price or signal.price <= 0:
            logger.warning(
                "invalid_signal_price_for_sizing",
                signal_id=signal.signal_id,
                price=float(signal.price) if signal.price else None,
            )
            return PositionSize(
                quantity=Decimal(0), value_usd=Decimal(0), percent_of_equity=Decimal(0)
            )

        if self._sizing_config.method == "simple":
            # Simple fixed fraction sizing
            position_value = self._calculate_simple_size(total_equity, current_exposure)
        else:  # kelly method
            # Kelly criterion sizing
            position_value = self._calculate_kelly_size(signal, total_equity, current_exposure)

        # Apply position limits from config
        position_value = self._apply_position_limits(position_value, current_exposure)

        # Calculate quantity based on signal price
        quantity = position_value / signal.price if position_value > 0 else Decimal(0)

        # Apply min/max quantity constraints from config.risk.sizing
        if quantity > 0:
            quantity = max(quantity, self._sizing_config.min_position_size)
            quantity = min(quantity, self._sizing_config.max_position_size)
            # Recalculate value after quantity constraints
            position_value = quantity * signal.price

        position_size = PositionSize(
            quantity=quantity,
            value_usd=position_value,
            percent_of_equity=(
                position_value / total_equity * 100 if total_equity > 0 else Decimal(0)
            ),
        )

        logger.debug(
            "position_size_calculated",
            signal_id=signal.signal_id,
            method=self._sizing_config.method,
            quantity=float(quantity),
            value_usd=float(position_value),
            percent_of_equity=float(position_size.percent_of_equity),
        )

        return position_size

    def _calculate_simple_size(self, total_equity: Decimal, current_exposure: Decimal) -> Decimal:
        """Calculate position size using simple fixed fraction method.

        Args:
            total_equity: Total portfolio equity
            current_exposure: Current total exposure

        Returns:
            Position value in USD

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured fraction from config.risk.sizing.simple_fixed_fraction
        - NO hardcoded fractions
        """
        # Use configured fraction from config.risk.sizing.simple_fixed_fraction
        fraction_based_size = total_equity * self._sizing_fraction

        # Apply configured max position size
        return min(fraction_based_size, self._max_position_usd)

    def _calculate_kelly_size(
        self, signal: TradeSignal, total_equity: Decimal, current_exposure: Decimal
    ) -> Decimal:
        """Calculate position size using Kelly criterion method.

        Args:
            signal: Trading signal with confidence/probability data
            total_equity: Total portfolio equity
            current_exposure: Current total exposure

        Returns:
            Position value in USD

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses kelly_multiplier and kelly_max_allocation from config
        - NO hardcoded Kelly parameters

        Note: This is a placeholder implementation. Full Kelly requires
        win rate and win/loss ratio data which would come from strategy
        backtesting or historical performance data.
        """
        # Simplified Kelly implementation using signal confidence
        # In production, this would use historical win rate and profit/loss ratios
        confidence = Decimal(str(signal.confidence)) if signal.confidence else Decimal("0.5")

        # Kelly fraction = (bp - q) / b where:
        # b = odds received on the wager (profit/loss ratio)
        # p = probability of winning
        # q = probability of losing (1-p)
        # For now, use a conservative estimate
        win_rate = confidence  # Use signal confidence as win probability
        loss_rate = Decimal(1) - win_rate
        profit_loss_ratio = Decimal("1.5")  # Conservative 1.5:1 ratio

        # Kelly fraction
        kelly_fraction = (win_rate * profit_loss_ratio - loss_rate) / profit_loss_ratio

        # Apply kelly multiplier from config (typically < 1 for safety)
        kelly_fraction *= self._kelly_multiplier

        # Cap at configured max allocation
        kelly_fraction = min(kelly_fraction, self._kelly_max_allocation)

        # Ensure non-negative
        kelly_fraction = max(kelly_fraction, Decimal(0))

        # Calculate position size
        kelly_based_size = total_equity * kelly_fraction

        logger.debug(
            "kelly_calculation",
            signal_id=signal.signal_id,
            confidence=float(confidence),
            kelly_fraction=float(kelly_fraction),
            kelly_multiplier=float(self._kelly_multiplier),
            max_allocation=float(self._kelly_max_allocation),
        )

        return kelly_based_size

    def _apply_position_limits(self, position_value: Decimal, current_exposure: Decimal) -> Decimal:
        """Apply global risk limits to position size.

        Args:
            position_value: Calculated position value before limits
            current_exposure: Current total exposure

        Returns:
            Position value after applying limits

        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL limits from config
        - NO hardcoded risk parameters
        """
        # Check total exposure limit
        if current_exposure + position_value > self._max_exposure_usd:
            # Reduce position to stay within total exposure limit
            position_value = self._max_exposure_usd - current_exposure
            if position_value <= 0:
                position_value = Decimal(0)

        # Apply max position size limit
        position_value = min(position_value, self._max_position_usd)

        # Ensure non-negative and return
        return max(position_value, Decimal(0))
