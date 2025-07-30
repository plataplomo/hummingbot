"""Kelly criterion sizer implementation with direct AppSettings access."""

from decimal import Decimal
from typing import Any

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.exceptions.sizing_exceptions import (
    KellyCalculationError,
    VolatilityCalculationError,
)
from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext
from cyberdelta.core.risk.sizing.strategies.typed_base_sizer import TypedBaseSizer
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class KellyCriterionSizer(TypedBaseSizer):
    """Position sizer using Kelly criterion for optimal position sizing with AppSettings access."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the Kelly criterion sizer with direct AppSettings access.

        Args:
            app_settings: Application settings with enhanced risk configuration
        """
        super().__init__(app_settings)

        # Cache Kelly-specific parameters from AppSettings for performance
        self._kelly_multiplier = self.sizing_settings.kelly_multiplier
        self._kelly_max_allocation = self.sizing_settings.kelly_max_allocation
        self._kelly_min_allocation = self.sizing_settings.kelly_min_allocation

        # Volatility parameters from AppSettings
        self._min_volatility = self.sizing_settings.min_volatility
        self._max_volatility = self.sizing_settings.max_volatility_bound
        self._volatility_lookback_hours = self.sizing_settings.volatility_lookback_hours

        # Risk parameters from AppSettings
        self._risk_free_rate = float(self.sizing_settings.kelly_risk_free_rate)
        self._expected_return_adjustment = Decimal("0.8")  # Hardcoded default

        # Calculation parameters - hardcoded defaults as not in enhanced config
        self._enable_sharpe_adjustment = True
        self._enable_drawdown_adjustment = True
        self._max_drawdown_threshold = Decimal("0.2")  # 20%

        # Safety parameters - hardcoded defaults as not in enhanced config
        self._enable_kelly_floor = True
        self._kelly_floor = Decimal("0.001")  # 0.1% minimum
        self._enable_kelly_ceiling = True
        self._kelly_ceiling = Decimal("0.5")  # 50% maximum

    @property
    def name(self) -> str:
        """Name of the sizer."""
        return "kelly_criterion"

    @property
    def sizing_method(self) -> str:
        """Sizing method identifier."""
        return "kelly"

    @property
    def kelly_multiplier(self) -> Decimal:
        """Get Kelly multiplier."""
        return self._kelly_multiplier

    @property
    def kelly_max_allocation(self) -> Decimal:
        """Get Kelly max allocation."""
        return self._kelly_max_allocation

    @property
    def kelly_min_allocation(self) -> Decimal:
        """Get Kelly min allocation."""
        return self._kelly_min_allocation

    @property
    def risk_free_rate(self) -> float:
        """Get risk-free rate."""
        return self._risk_free_rate

    async def _calculate_base_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate base position size using Kelly criterion.

        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing

        Returns:
            Base position size in USD
        """
        # Get expected return
        expected_return = self._calculate_expected_return(opportunity)

        # Get volatility
        volatility = await self._calculate_volatility(opportunity, context)

        # Calculate Kelly fraction
        kelly_fraction = self._calculate_kelly_fraction(expected_return, volatility)

        # Apply Kelly multiplier
        adjusted_kelly = kelly_fraction * self._kelly_multiplier

        # Apply Kelly bounds
        bounded_kelly = self._apply_kelly_bounds(adjusted_kelly)

        # Calculate position size
        position_size = context.available_capital * bounded_kelly

        # Store calculation details in context
        context.add_metadata("expected_return", float(expected_return))
        context.add_metadata("volatility", float(volatility))
        context.add_metadata("kelly_fraction", float(kelly_fraction))
        context.add_metadata("adjusted_kelly", float(adjusted_kelly))
        context.add_metadata("bounded_kelly", float(bounded_kelly))

        return position_size

    def _calculate_expected_return(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Calculate expected return for the opportunity.

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            Expected return as decimal (e.g., 0.01 for 1%)
        """
        # Try to get spread percentage from opportunity
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                spread_decimal = self._to_decimal(spread_percentage)
                # Adjust for execution risk and fees
                result = spread_decimal * self._expected_return_adjustment
            except (ValueError, TypeError):
                pass
            else:
                return result

        # Try to get expected return directly
        expected_return = getattr(opportunity, "expected_return", None)
        if expected_return:
            try:
                return self._to_decimal(expected_return)
            except (ValueError, TypeError):
                pass

        # Fallback to calculating from prices
        long_price = getattr(opportunity, "long_price", None)
        short_price = getattr(opportunity, "short_price", None)

        if long_price and short_price:
            try:
                long_decimal = self._to_decimal(long_price)
                short_decimal = self._to_decimal(short_price)

                if long_decimal > 0 and short_decimal > 0:
                    # Calculate simple arbitrage return
                    price_diff = abs(long_decimal - short_decimal)
                    avg_price = (long_decimal + short_decimal) / 2

                    if avg_price > 0:
                        return (price_diff / avg_price) * self._expected_return_adjustment
            except (ValueError, TypeError):
                pass

        # Default fallback
        return Decimal("0.001")  # 0.1% default

    async def _calculate_volatility(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate volatility for the opportunity.

        Args:
            opportunity: The arbitrage opportunity
            context: Sizing context

        Returns:
            Volatility estimate
        """
        # Try to get volatility from opportunity
        vol_from_opportunity = self._get_volatility_from_attribute(opportunity, "volatility")
        if vol_from_opportunity is not None:
            return vol_from_opportunity

        # Try to get historical volatility from context
        historical_volatility = context.get_metadata_value("historical_volatility")
        vol_from_history = self._parse_volatility_value(historical_volatility)
        if vol_from_history is not None:
            return vol_from_history

        # Estimate volatility from price data
        volatility_estimate = self._estimate_volatility_from_prices(opportunity)
        if volatility_estimate:
            return volatility_estimate

        # Use spread as volatility proxy
        vol_from_spread = self._estimate_volatility_from_spread(opportunity)
        if vol_from_spread is not None:
            return vol_from_spread

        # Default volatility
        return self._min_volatility * 10  # Conservative default

    def _get_volatility_from_attribute(
        self, obj: ArbitrageOpportunity, attr_name: str
    ) -> Decimal | None:
        """Extract volatility from object attribute.

        Returns:
            Decimal | None: The extracted volatility value or None if not found.
        """
        value = getattr(obj, attr_name, None)
        if value:
            return self._parse_volatility_value(value)
        return None

    def _parse_volatility_value(
        self, value: str | float | Decimal | list[Any] | dict[str, Any] | None
    ) -> Decimal | None:
        """Parse and validate volatility value.

        Returns:
            Decimal | None: Parsed and bounded volatility value or None.
        """
        if not value:
            return None
        try:
            if isinstance(value, (str, int, float, Decimal)):
                vol_decimal = self._to_decimal(value)
                return min(max(vol_decimal, self._min_volatility), self._max_volatility)
            return self._min_volatility  # noqa: TRY300
        except (ValueError, TypeError):
            return self._min_volatility

    def _estimate_volatility_from_spread(self, opportunity: ArbitrageOpportunity) -> Decimal | None:
        """Estimate volatility from spread percentage.

        Returns:
            Decimal | None: Estimated volatility based on spread or None.
        """
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if not spread_percentage:
            return None
        try:
            if isinstance(spread_percentage, (str, int, float, Decimal)):
                spread_decimal = self._to_decimal(spread_percentage)
                # Spread often correlates with volatility
                volatility_proxy = spread_decimal * 5  # Rough multiplier
                return min(max(volatility_proxy, self._min_volatility), self._max_volatility)
            return self._min_volatility  # noqa: TRY300
        except (ValueError, TypeError):
            return self._min_volatility

    def _estimate_volatility_from_prices(self, opportunity: ArbitrageOpportunity) -> Decimal | None:
        """Estimate volatility from price information.

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            Volatility estimate or None
        """
        try:
            long_price = getattr(opportunity, "long_price", None)
            short_price = getattr(opportunity, "short_price", None)

            if long_price and short_price:
                long_decimal = self._to_decimal(long_price)
                short_decimal = self._to_decimal(short_price)

                if long_decimal > 0 and short_decimal > 0:
                    # Calculate price volatility as proxy
                    price_diff = abs(long_decimal - short_decimal)
                    avg_price = (long_decimal + short_decimal) / 2

                    if avg_price > 0:
                        price_volatility = price_diff / avg_price
                        # Scale to daily volatility estimate
                        scaled_vol = price_volatility * 3
                        return min(scaled_vol, self._max_volatility)

        except (ValueError, TypeError):
            pass

        return None

    def _calculate_kelly_fraction(self, expected_return: Decimal, volatility: Decimal) -> Decimal:
        """Calculate Kelly fraction using the Kelly formula.

        Args:
            expected_return: Expected return
            volatility: Volatility (standard deviation)

        Returns:
            Kelly fraction

        Raises:
            KellyCalculationError: If volatility is invalid (zero or negative).
        """
        if volatility <= 0:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_VOLATILITY,
                metadata={"volatility": float(volatility)},
            )

        # Kelly formula: f = (mu - r) / sigma^2
        # where mu = expected return, r = risk-free rate, sigma = volatility
        # Daily risk-free rate
        excess_return = expected_return - (Decimal(str(self._risk_free_rate)) / 365)

        if excess_return <= 0:
            return Decimal(0)  # No positive expected return

        # Calculate Kelly fraction
        kelly_fraction = excess_return / (volatility * volatility)

        # Apply Sharpe ratio adjustment if enabled
        if self._enable_sharpe_adjustment:
            sharpe_ratio = excess_return / volatility

            # Reduce Kelly fraction for low Sharpe ratios
            if sharpe_ratio < 1:
                kelly_fraction *= sharpe_ratio

        return kelly_fraction

    def _apply_kelly_bounds(self, kelly_fraction: Decimal) -> Decimal:
        """Apply bounds to Kelly fraction.

        Args:
            kelly_fraction: Raw Kelly fraction

        Returns:
            Bounded Kelly fraction
        """
        bounded_kelly = kelly_fraction

        # Apply floor
        if self._enable_kelly_floor:
            bounded_kelly = max(bounded_kelly, self._kelly_floor)

        # Apply ceiling
        if self._enable_kelly_ceiling:
            bounded_kelly = min(bounded_kelly, self._kelly_ceiling)

        # Apply allocation bounds
        bounded_kelly = max(bounded_kelly, self._kelly_min_allocation)
        return min(bounded_kelly, self._kelly_max_allocation)

    def calculate_kelly_with_win_probability(
        self,
        win_probability: Decimal,
        win_amount: Decimal,
        loss_amount: Decimal,
    ) -> Decimal:
        """Calculate Kelly fraction using win probability formula.

        This is an alternative Kelly calculation for binary outcomes.

        Args:
            win_probability: Probability of winning (0-1)
            win_amount: Amount won if successful
            loss_amount: Amount lost if unsuccessful

        Returns:
            Kelly fraction

        Raises:
            KellyCalculationError: If win probability is out of valid range (0-1).
        """
        if win_probability <= 0 or win_probability >= 1:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_WIN_PROBABILITY,
                win_rate=float(win_probability),
            )

        if win_amount <= 0 or loss_amount <= 0:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_WIN_LOSS_AMOUNTS,
                metadata={"win_amount": float(win_amount), "loss_amount": float(loss_amount)},
            )

        # Kelly formula: f = (bp - q) / b
        # where b = win_amount/loss_amount, p = win_probability, q = 1-p
        b = win_amount / loss_amount
        p = win_probability
        q = 1 - p

        kelly_fraction = (b * p - q) / b

        # Return non-negative Kelly fraction
        if kelly_fraction < Decimal(0):
            return Decimal(0)
        return kelly_fraction

    def set_kelly_multiplier(self, multiplier: Decimal) -> None:
        """Set Kelly multiplier.

        Args:
            multiplier: Kelly multiplier (0-1)

        Raises:
            KellyCalculationError: If multiplier is out of valid range (0-1).
        """
        if multiplier <= 0 or multiplier > 1:
            raise KellyCalculationError(KellyCalculationError.KELLY_MULTIPLIER_OUT_OF_RANGE)

        self._kelly_multiplier = multiplier
        self.logger.info("Set Kelly multiplier", multiplier=float(multiplier))

    def set_kelly_allocation_bounds(self, min_allocation: Decimal, max_allocation: Decimal) -> None:
        """Set Kelly allocation bounds.

        Args:
            min_allocation: Minimum Kelly allocation
            max_allocation: Maximum Kelly allocation

        Raises:
            KellyCalculationError: If min_allocation >= max_allocation.
        """
        if min_allocation >= max_allocation:
            raise KellyCalculationError(KellyCalculationError.KELLY_ALLOCATION_BOUNDS_INVALID)

        self._kelly_min_allocation = min_allocation
        self._kelly_max_allocation = max_allocation
        self.logger.info(
            "Set Kelly allocation bounds",
            min_allocation=float(min_allocation),
            max_allocation=float(max_allocation),
        )

    def set_volatility_bounds(self, min_volatility: Decimal, max_volatility: Decimal) -> None:
        """Set volatility bounds.

        Args:
            min_volatility: Minimum volatility
            max_volatility: Maximum volatility

        Raises:
            VolatilityCalculationError: If min_volatility >= max_volatility.
        """
        if min_volatility >= max_volatility:
            raise VolatilityCalculationError(
                VolatilityCalculationError.MIN_VOLATILITY_MUST_BE_LESS_THAN_MAX
            )

        self._min_volatility = min_volatility
        self._max_volatility = max_volatility
        self.logger.info(
            "Set volatility bounds",
            min_volatility=float(min_volatility),
            max_volatility=float(max_volatility),
        )

    def set_risk_free_rate(self, rate: Decimal) -> None:
        """Set risk-free rate.

        Args:
            rate: Annual risk-free rate

        Raises:
            KellyCalculationError: If rate is negative.
        """
        if rate < 0:
            raise KellyCalculationError(KellyCalculationError.NEGATIVE_RISK_FREE_RATE)

        self._risk_free_rate = float(rate)
        self.logger.info("Set risk-free rate", rate=float(rate))

    def set_sharpe_adjustment(self, enable: bool) -> None:
        """Enable or disable Sharpe ratio adjustment.

        Args:
            enable: Whether to enable Sharpe adjustment
        """
        self._enable_sharpe_adjustment = enable
        self.logger.info("Sharpe adjustment configured", enabled=enable)

    def get_kelly_stats(self) -> dict[str, Any]:
        """Get Kelly sizer statistics.

        Returns:
            Dictionary with Kelly statistics
        """

        def safe_float(value: float | Decimal) -> float:
            return float(value)

        return {
            "kelly_multiplier": safe_float(self._kelly_multiplier),
            "kelly_min_allocation": safe_float(self._kelly_min_allocation),
            "kelly_max_allocation": safe_float(self._kelly_max_allocation),
            "min_volatility": safe_float(self._min_volatility),
            "max_volatility": safe_float(self._max_volatility),
            "risk_free_rate": safe_float(self._risk_free_rate),
            "sharpe_adjustment_enabled": self._enable_sharpe_adjustment,
            "kelly_floor": safe_float(self._kelly_floor),
            "kelly_ceiling": safe_float(self._kelly_ceiling),
        }
