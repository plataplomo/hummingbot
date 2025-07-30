"""Simple sizer implementation for fixed and fractional position sizing with AppSettings access."""

from decimal import Decimal
from typing import Any, Literal

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext
from cyberdelta.core.risk.sizing.strategies.typed_base_sizer import TypedBaseSizer
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class SimpleSizer(TypedBaseSizer):
    """Simple position sizer using fixed USD amounts or fixed fractions."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the simple sizer with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        super().__init__(app_settings)

        # Simple sizing method from AppSettings
        self.sizing_method_type: Literal["fixed_usd", "fixed_fraction"] = (
            self.sizing_settings.simple_method
        )

        # Fixed USD sizing from AppSettings
        self.fixed_usd_amount = self.sizing_settings.simple_fixed_usd
        # Capital scaling (hardcoded as not in new config)
        self.enable_capital_scaling = True

        # Fixed fraction sizing from AppSettings
        self.fixed_fraction = self.sizing_settings.simple_fixed_fraction
        # Spread adjustment already inherited from TypedBaseSizer

        # Scaling parameters (hardcoded as not in new config)
        self.min_capital_for_scaling = Decimal(10000)  # $10k minimum
        self.max_capital_for_scaling = Decimal(1000000)  # $1M maximum

        # Spread adjustment parameters (hardcoded as not in new config)
        self.base_spread_threshold = Decimal("0.005")  # 0.5%
        self.spread_adjustment_factor = Decimal("2.0")  # 2x

        # Risk adjustment parameters
        # Volatility adjustment already inherited from TypedBaseSizer
        self.volatility_adjustment_factor = Decimal("0.5")

        # Safety parameters (hardcoded as not in new config)
        self.min_spread_for_trading = Decimal("0.0001")  # 0.01%
        self.max_position_multiplier = Decimal("10.0")  # 10x

    @property
    def name(self) -> str:
        """Name of the sizer."""
        return "simple"

    @property
    def sizing_method(self) -> str:
        """Sizing method identifier."""
        return f"simple_{self.sizing_method_type}"

    async def _calculate_base_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate base position size using simple methods.

        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing

        Returns:
            Base position size in USD

        Raises:
            SizingError: If unknown sizing method is specified.
        """
        if self.sizing_method_type == "fixed_usd":
            base_size = await self._calculate_fixed_usd_size(opportunity, context)
        elif self.sizing_method_type == "fixed_fraction":
            base_size = await self._calculate_fixed_fraction_size(opportunity, context)
        else:
            raise SizingError(
                SizingError.UNKNOWN_SIZING_METHOD, sizing_method=self.sizing_method_type
            )

        # Apply spread adjustment if enabled
        if self.enable_spread_adjustment:
            base_size = self._apply_spread_adjustment(base_size, opportunity)

        # Apply volatility adjustment if enabled
        if self.enable_volatility_adjustment:
            base_size = await self._apply_volatility_adjustment(base_size, opportunity, context)

        # Store calculation details
        context.add_metadata("sizing_method_type", self.sizing_method_type)
        context.add_metadata("base_size_before_adjustments", float(base_size))

        return base_size

    async def _calculate_fixed_usd_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate position size using fixed USD amount.

        Args:
            opportunity: The arbitrage opportunity
            context: Sizing context

        Returns:
            Position size in USD
        """
        base_size = self.fixed_usd_amount

        # Apply capital scaling if enabled
        if self.enable_capital_scaling:
            base_size = self._apply_capital_scaling(base_size, context.available_capital)

        context.add_metadata("fixed_usd_amount", float(self.fixed_usd_amount))
        context.add_metadata("capital_scaling_enabled", self.enable_capital_scaling)

        return base_size

    async def _calculate_fixed_fraction_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate position size using fixed fraction of capital.

        Args:
            opportunity: The arbitrage opportunity
            context: Sizing context

        Returns:
            Position size in USD
        """
        base_size = context.available_capital * self.fixed_fraction

        context.add_metadata("fixed_fraction", float(self.fixed_fraction))
        context.add_metadata("available_capital", float(context.available_capital))

        return base_size

    def _apply_capital_scaling(self, base_size: Decimal, available_capital: Decimal) -> Decimal:
        """Apply capital scaling to fixed USD amounts.

        Args:
            base_size: Base position size
            available_capital: Available capital

        Returns:
            Scaled position size
        """
        if available_capital <= self.min_capital_for_scaling:
            # For small accounts, use a smaller fixed amount
            scaling_factor = available_capital / self.min_capital_for_scaling
            factor = min(Decimal("1.0"), scaling_factor)
            return base_size * factor

        if available_capital >= self.max_capital_for_scaling:
            # For large accounts, scale up but with diminishing returns
            scaling_factor = (
                Decimal(1)
                + (available_capital - self.max_capital_for_scaling)
                / self.max_capital_for_scaling
                / 4
            )
            factor_large = min(self.max_position_multiplier, scaling_factor)
            return base_size * factor_large

        # Linear scaling in the middle range
        scaling_factor = available_capital / self.min_capital_for_scaling
        factor_mid = min(self.max_position_multiplier, scaling_factor)
        return base_size * factor_mid

    def _apply_spread_adjustment(
        self, base_size: Decimal, opportunity: ArbitrageOpportunity
    ) -> Decimal:
        """Apply spread-based adjustment to position size.

        Args:
            base_size: Base position size
            opportunity: The arbitrage opportunity

        Returns:
            Adjusted position size
        """
        # Get spread percentage
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if not spread_percentage:
            return base_size

        try:
            spread_decimal = self._to_decimal(spread_percentage)

            # Check minimum spread
            if spread_decimal < self.min_spread_for_trading:
                return Decimal(0)  # Don't trade if spread is too small

            # Calculate spread adjustment factor
            if spread_decimal > self.base_spread_threshold:
                # Higher spread = higher confidence = larger position
                spread_multiplier = (
                    Decimal(1)
                    + (spread_decimal - self.base_spread_threshold) * self.spread_adjustment_factor
                )
            else:
                # Lower spread = lower confidence = smaller position
                spread_multiplier = spread_decimal / self.base_spread_threshold

            # Apply bounds
            spread_multiplier = max(spread_multiplier, Decimal("0.1"))
            spread_multiplier = min(spread_multiplier, self.max_position_multiplier)

            return base_size * spread_multiplier
        except (ValueError, TypeError):
            return base_size

    async def _apply_volatility_adjustment(
        self,
        base_size: Decimal,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Apply volatility-based adjustment to position size.

        Args:
            base_size: Base position size
            opportunity: The arbitrage opportunity
            context: Sizing context

        Returns:
            Adjusted position size
        """
        # Get volatility estimate
        volatility = getattr(opportunity, "volatility", None)
        if not volatility:
            # Try to get from context
            volatility = context.get_metadata_value("volatility")

        if not volatility:
            return base_size

        try:
            if isinstance(volatility, (str, int, float, Decimal)):
                volatility_decimal = self._to_decimal(volatility)
            else:
                return base_size

            # Apply volatility adjustment
            # Higher volatility = smaller position
            volatility_multiplier = Decimal(1) / (
                Decimal(1) + volatility_decimal * self.volatility_adjustment_factor
            )

            # Apply bounds
            volatility_multiplier = max(volatility_multiplier, Decimal("0.1"))
            volatility_multiplier = min(volatility_multiplier, Decimal("1.0"))

            return base_size * volatility_multiplier
        except (ValueError, TypeError):
            return base_size

    def set_sizing_method(self, method: Literal["fixed_usd", "fixed_fraction"]) -> None:
        """Set the sizing method.

        Args:
            method: Sizing method to use

        Raises:
            SizingError: If method is not 'fixed_usd' or 'fixed_fraction'.
        """
        if method not in {"fixed_usd", "fixed_fraction"}:
            raise SizingError(SizingError.METHOD_MUST_BE_VALID)

        self.sizing_method_type = method
        self.logger.info("Set sizing method", method=method)

    def set_fixed_usd_amount(self, amount: Decimal) -> None:
        """Set fixed USD amount.

        Args:
            amount: Fixed USD amount for sizing

        Raises:
            SizingError: If amount is not positive.
        """
        if amount <= 0:
            raise SizingError(SizingError.FIXED_USD_AMOUNT_MUST_BE_POSITIVE)

        self.fixed_usd_amount = amount
        self.logger.info("Set fixed USD amount", amount_usd=float(amount))

    def set_fixed_fraction(self, fraction: Decimal) -> None:
        """Set fixed fraction of capital.

        Args:
            fraction: Fixed fraction of capital (0-1)

        Raises:
            SizingError: If fraction is not between 0 and 1 (exclusive of 0, inclusive of 1).
        """
        if fraction <= 0 or fraction > 1:
            raise SizingError(SizingError.FIXED_FRACTION_OUT_OF_RANGE)

        self.fixed_fraction = fraction
        self.logger.info("Set fixed fraction", fraction=float(fraction))

    def set_spread_adjustment(
        self, enabled: bool, threshold: Decimal | None = None, factor: Decimal | None = None
    ) -> None:
        """Configure spread adjustment.

        Args:
            enabled: Whether to enable spread adjustment
            threshold: Base spread threshold (optional)
            factor: Spread adjustment factor (optional)
        """
        self.enable_spread_adjustment = enabled

        if threshold is not None:
            self.base_spread_threshold = threshold

        if factor is not None:
            self.spread_adjustment_factor = factor

        self.logger.info(
            "Spread adjustment configured",
            enabled=enabled,
            threshold=float(self.base_spread_threshold),
            factor=float(self.spread_adjustment_factor),
        )

    def set_capital_scaling(
        self, enabled: bool, min_capital: Decimal | None = None, max_capital: Decimal | None = None
    ) -> None:
        """Configure capital scaling.

        Args:
            enabled: Whether to enable capital scaling
            min_capital: Minimum capital for scaling (optional)
            max_capital: Maximum capital for scaling (optional)
        """
        self.enable_capital_scaling = enabled

        if min_capital is not None:
            self.min_capital_for_scaling = min_capital

        if max_capital is not None:
            self.max_capital_for_scaling = max_capital

        self.logger.info(
            "Capital scaling configured",
            enabled=enabled,
            min_capital_usd=float(self.min_capital_for_scaling),
            max_capital_usd=float(self.max_capital_for_scaling),
        )

    def set_volatility_adjustment(self, enabled: bool, factor: Decimal | None = None) -> None:
        """Configure volatility adjustment.

        Args:
            enabled: Whether to enable volatility adjustment
            factor: Volatility adjustment factor (optional)
        """
        self.enable_volatility_adjustment = enabled

        if factor is not None:
            self.volatility_adjustment_factor = factor

        self.logger.info(
            "Volatility adjustment configured",
            enabled=enabled,
            factor=float(self.volatility_adjustment_factor),
        )

    def get_simple_stats(self) -> dict[str, Any]:
        """Get simple sizer statistics.

        Returns:
            Dictionary with simple sizer statistics
        """
        return {
            "sizing_method": self.sizing_method_type,
            "fixed_usd_amount": float(self.fixed_usd_amount),
            "fixed_fraction": float(self.fixed_fraction),
            "enable_capital_scaling": self.enable_capital_scaling,
            "enable_spread_adjustment": self.enable_spread_adjustment,
            "enable_volatility_adjustment": self.enable_volatility_adjustment,
            "base_spread_threshold": float(self.base_spread_threshold),
            "spread_adjustment_factor": float(self.spread_adjustment_factor),
            "min_capital_for_scaling": float(self.min_capital_for_scaling),
            "max_capital_for_scaling": float(self.max_capital_for_scaling),
            "min_spread_for_trading": float(self.min_spread_for_trading),
        }

    def estimate_position_size(
        self, available_capital: Decimal, opportunity: ArbitrageOpportunity | None = None
    ) -> Decimal:
        """Estimate position size without full calculation.

        Args:
            available_capital: Available capital
            opportunity: Optional opportunity for spread adjustment

        Returns:
            Estimated position size
        """
        if self.sizing_method_type == "fixed_usd":
            base_size = self.fixed_usd_amount
            if self.enable_capital_scaling:
                base_size = self._apply_capital_scaling(base_size, available_capital)
        else:
            base_size = available_capital * self.fixed_fraction

        # Apply spread adjustment if opportunity provided
        if opportunity and self.enable_spread_adjustment:
            base_size = self._apply_spread_adjustment(base_size, opportunity)

        # Apply constraints
        base_size = max(base_size, self._min_position_size)
        return min(base_size, self._max_position_size)
