"""Volatility calculator utility for risk management."""

import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.sizing_exceptions import VolatilityCalculationError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Constants
MIN_SAMPLES_FOR_SKEW_KURTOSIS = 30  # Minimum samples for reliable skewness/kurtosis calculation
MIN_SAMPLES_FOR_PERCENTILES = 20  # Minimum samples for reliable percentile calculation
MIN_SAMPLES_FOR_PERIOD_CALC = 2  # Minimum samples for period calculation
MIN_SAMPLES_FOR_RELIABLE_VOLATILITY = 30  # Minimum samples for reliable volatility calculation
MIN_SAMPLES_FOR_REALIZED_VOLATILITY = 2  # Minimum samples for realized volatility
MIN_SAMPLES_FOR_SKEWNESS = 3  # Minimum samples for skewness calculation
MIN_SAMPLES_FOR_KURTOSIS = 4  # Minimum samples for kurtosis calculation
MIN_SAMPLES_FOR_UPSIDE_VOLATILITY = 2  # Minimum samples for upside volatility
MIN_SAMPLES_FOR_DOWNSIDE_VOLATILITY = 2  # Minimum samples for downside volatility
MIN_VOLATILITIES_FOR_VOL_OF_VOL = 2  # Minimum volatilities for vol-of-vol calculation


class VolatilityMethod(Enum):
    """Methods for calculating volatility."""

    SIMPLE = "simple"
    EXPONENTIAL = "exponential"
    GARCH = "garch"
    REALIZED = "realized"
    IMPLIED = "implied"
    PARKINSON = "parkinson"


class VolatilityTimeframe(Enum):
    """Timeframes for volatility calculation."""

    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAILY = "1d"
    WEEKLY = "1w"


@dataclass
class PriceData:
    """Price data for volatility calculation."""

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None

    @property
    def typical_price(self) -> Decimal:
        """Calculate typical price (HLC/3)."""
        return (self.high + self.low + self.close) / 3

    def true_range(self, previous_close: Decimal | None = None) -> Decimal:
        """Calculate true range."""
        high_low = self.high - self.low

        if previous_close is None:
            return high_low

        high_prev_close = abs(self.high - previous_close)
        low_prev_close = abs(self.low - previous_close)

        return max(high_low, high_prev_close, low_prev_close)


@dataclass
class VolatilityResult:
    """Result of volatility calculation."""

    # Primary results
    volatility: Decimal
    annualized_volatility: Decimal

    # Calculation details
    method: VolatilityMethod
    timeframe: VolatilityTimeframe
    data_points: int
    period_hours: float

    # Statistical measures
    mean_return: Decimal
    std_deviation: Decimal
    skewness: Decimal | None = None
    kurtosis: Decimal | None = None

    # Volatility components
    upside_volatility: Decimal | None = None
    downside_volatility: Decimal | None = None
    volatility_of_volatility: Decimal | None = None

    # Additional metrics
    max_volatility: Decimal | None = None
    min_volatility: Decimal | None = None
    percentile_95: Decimal | None = None
    percentile_5: Decimal | None = None

    # Metadata
    calculation_timestamp: datetime | None = None
    warnings: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "volatility": float(self.volatility),
            "annualized_volatility": float(self.annualized_volatility),
            "method": self.method.value,
            "timeframe": self.timeframe.value,
            "data_points": self.data_points,
            "period_hours": self.period_hours,
            "mean_return": float(self.mean_return),
            "std_deviation": float(self.std_deviation),
            "skewness": float(self.skewness) if self.skewness else None,
            "kurtosis": float(self.kurtosis) if self.kurtosis else None,
            "upside_volatility": float(self.upside_volatility) if self.upside_volatility else None,
            "downside_volatility": (
                float(self.downside_volatility) if self.downside_volatility else None
            ),
            "volatility_of_volatility": (
                float(self.volatility_of_volatility) if self.volatility_of_volatility else None
            ),
            "max_volatility": float(self.max_volatility) if self.max_volatility else None,
            "min_volatility": float(self.min_volatility) if self.min_volatility else None,
            "percentile_95": float(self.percentile_95) if self.percentile_95 else None,
            "percentile_5": float(self.percentile_5) if self.percentile_5 else None,
            "calculation_timestamp": (
                self.calculation_timestamp.isoformat() if self.calculation_timestamp else None
            ),
            "warnings": self.warnings,
        }


class VolatilityCalculator:
    """Advanced volatility calculator for risk management."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the volatility calculator."""
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)

        # Configuration
        self.default_method = VolatilityMethod(self.config.get("default_method", "exponential"))
        self.default_timeframe = VolatilityTimeframe(self.config.get("default_timeframe", "1h"))
        self.min_data_points = self.config.get("min_data_points", 20)
        self.max_data_points = self.config.get("max_data_points", 1000)
        self.default_volatility = Decimal(str(self.config.get("default_volatility", "0.01")))

        # EWMA parameters
        # RiskMetrics standard
        self.ewma_lambda = Decimal(str(self.config.get("ewma_lambda", "0.94")))

        # GARCH parameters
        self.garch_omega = Decimal(str(self.config.get("garch_omega", "0.000001")))
        self.garch_alpha = Decimal(str(self.config.get("garch_alpha", "0.06")))
        self.garch_beta = Decimal(str(self.config.get("garch_beta", "0.92")))

        # Annualization factors (hours per year)
        self.annualization_factors = {
            VolatilityTimeframe.MINUTE_1: 525600,  # 365.25 * 24 * 60
            VolatilityTimeframe.MINUTE_5: 105120,  # 365.25 * 24 * 12
            VolatilityTimeframe.MINUTE_15: 35040,  # 365.25 * 24 * 4
            VolatilityTimeframe.HOUR_1: 8766,  # 365.25 * 24
            VolatilityTimeframe.HOUR_4: 2191.5,  # 365.25 * 6
            VolatilityTimeframe.DAILY: 365.25,
            VolatilityTimeframe.WEEKLY: 52.18,
        }

        # Cache for recent calculations
        self.calculation_cache: dict[str, VolatilityResult] = {}
        self.cache_ttl_seconds = self.config.get("cache_ttl_seconds", 300)  # 5 minutes

    def _validate_input_data(self, price_data: list[PriceData]) -> None:
        """Validate input data for volatility calculation."""
        if not price_data:
            raise VolatilityCalculationError(
                VolatilityCalculationError.NO_PRICE_DATA,
                data_points=0,
            )

        if len(price_data) < self.min_data_points:
            raise VolatilityCalculationError(
                VolatilityCalculationError.INSUFFICIENT_DATA_POINTS,
                data_points=len(price_data),
                metadata={"min_required": self.min_data_points},
            )

    def _prepare_calculation_inputs(
        self,
        price_data: list[PriceData],
        method: VolatilityMethod | None,
        timeframe: VolatilityTimeframe | None,
    ) -> tuple[VolatilityMethod, VolatilityTimeframe, list[PriceData]]:
        """Prepare inputs for volatility calculation."""
        method = method or self.default_method
        timeframe = timeframe or self.default_timeframe

        # Limit data points
        if len(price_data) > self.max_data_points:
            price_data = price_data[-self.max_data_points :]

        return method, timeframe, price_data

    def _calculate_volatility_by_method(
        self, method: VolatilityMethod, returns: list[Decimal], price_data: list[PriceData]
    ) -> Decimal:
        """Calculate volatility using the specified method."""
        if method == VolatilityMethod.SIMPLE:
            return self._calculate_simple_volatility(returns)
        if method == VolatilityMethod.EXPONENTIAL:
            return self._calculate_ewma_volatility(returns)
        if method == VolatilityMethod.GARCH:
            return self._calculate_garch_volatility(returns)
        if method == VolatilityMethod.REALIZED:
            return self._calculate_realized_volatility(price_data)
        if method == VolatilityMethod.PARKINSON:
            return self._calculate_parkinson_volatility(price_data)
        raise VolatilityCalculationError(
            VolatilityCalculationError.UNSUPPORTED_METHOD,
            calculation_method=method.value if hasattr(method, "value") else str(method),
        )

    def _calculate_additional_metrics(self, returns: list[Decimal]) -> dict[str, Any]:
        """Calculate additional statistical metrics."""
        mean_return = sum(returns) / len(returns) if returns else Decimal(0)

        # Calculate skewness and kurtosis if enough data
        skewness = None
        kurtosis = None
        if len(returns) >= MIN_SAMPLES_FOR_SKEW_KURTOSIS:
            std_deviation = self._calculate_simple_volatility(returns)
            skewness = self._calculate_skewness(returns, Decimal(str(mean_return)), std_deviation)
            kurtosis = self._calculate_kurtosis(returns, Decimal(str(mean_return)), std_deviation)

        # Calculate upside/downside volatility
        upside_volatility = self._calculate_upside_volatility(returns)
        downside_volatility = self._calculate_downside_volatility(returns)
        volatility_of_volatility = self._calculate_vol_of_vol(returns)

        # Calculate percentiles
        percentile_95 = None
        percentile_5 = None
        if len(returns) >= MIN_SAMPLES_FOR_PERCENTILES:
            sorted_returns = sorted([abs(r) for r in returns])
            percentile_95 = Decimal(str(sorted_returns[int(len(sorted_returns) * 0.95)]))
            percentile_5 = Decimal(str(sorted_returns[int(len(sorted_returns) * 0.05)]))

        return {
            "mean_return": mean_return,
            "skewness": skewness,
            "kurtosis": kurtosis,
            "upside_volatility": upside_volatility,
            "downside_volatility": downside_volatility,
            "volatility_of_volatility": volatility_of_volatility,
            "percentile_95": percentile_95,
            "percentile_5": percentile_5,
        }

    def _annualize_volatility(self, volatility: Decimal, timeframe: VolatilityTimeframe) -> Decimal:
        """Annualize volatility based on timeframe."""
        annualization_factor = self.annualization_factors.get(timeframe, 365.25)
        return volatility * Decimal(str(math.sqrt(annualization_factor)))

    def _calculate_period_hours(self, price_data: list[PriceData]) -> float:
        """Calculate period hours from price data."""
        if len(price_data) >= MIN_SAMPLES_FOR_PERIOD_CALC:
            time_diff = price_data[-1].timestamp - price_data[0].timestamp
            return time_diff.total_seconds() / 3600
        return 0

    def _create_volatility_result(
        self,
        volatility: Decimal,
        annualized_volatility: Decimal,
        method: VolatilityMethod,
        timeframe: VolatilityTimeframe,
        price_data: list[PriceData],
        period_hours: float,
        additional_metrics: dict[str, Any],
    ) -> VolatilityResult:
        """Create the final volatility result."""
        return VolatilityResult(
            volatility=volatility,
            annualized_volatility=annualized_volatility,
            method=method,
            timeframe=timeframe,
            data_points=len(price_data),
            period_hours=period_hours,
            mean_return=Decimal(str(additional_metrics["mean_return"])),
            std_deviation=volatility,
            skewness=additional_metrics["skewness"],
            kurtosis=additional_metrics["kurtosis"],
            upside_volatility=additional_metrics["upside_volatility"],
            downside_volatility=additional_metrics["downside_volatility"],
            volatility_of_volatility=additional_metrics["volatility_of_volatility"],
            percentile_95=additional_metrics["percentile_95"],
            percentile_5=additional_metrics["percentile_5"],
            calculation_timestamp=datetime.now(tz=UTC),
            warnings=[],
        )

    def _add_warnings_if_needed(
        self, result: VolatilityResult, price_data: list[PriceData]
    ) -> None:
        """Add warnings to the result if needed."""
        if len(price_data) < MIN_SAMPLES_FOR_RELIABLE_VOLATILITY:
            if result.warnings is None:
                result.warnings = []
            result.warnings.append(
                f"Limited data points ({len(price_data)}), results may be less reliable"
            )

        if result.volatility > Decimal("1.0"):
            if result.warnings is None:
                result.warnings = []
            result.warnings.append(f"High volatility detected: {result.volatility:.4f}")

    def calculate_volatility(
        self,
        price_data: list[PriceData],
        method: VolatilityMethod | None = None,
        timeframe: VolatilityTimeframe | None = None,
    ) -> VolatilityResult:
        """Calculate volatility using specified method.

        Args:
            price_data: List of price data points
            method: Volatility calculation method
            timeframe: Timeframe of the data

        Returns:
            VolatilityResult with detailed calculations
        """
        self._validate_input_data(price_data)
        method, timeframe, filtered_data = self._prepare_calculation_inputs(
            price_data, method, timeframe
        )

        returns = self._calculate_returns(filtered_data)
        volatility = self._calculate_volatility_by_method(method, returns, filtered_data)

        additional_metrics = self._calculate_additional_metrics(returns)
        annualized_volatility = self._annualize_volatility(volatility, timeframe)
        period_hours = self._calculate_period_hours(filtered_data)

        result = self._create_volatility_result(
            volatility,
            annualized_volatility,
            method,
            timeframe,
            filtered_data,
            period_hours,
            additional_metrics,
        )

        self._add_warnings_if_needed(result, filtered_data)
        self.logger.debug(
            "Volatility calculated",
            volatility=float(volatility),
            method=method.value,
        )
        return result

    def _calculate_returns(self, price_data: list[PriceData]) -> list[Decimal]:
        """Calculate returns from price data."""
        returns: list[Decimal] = []

        for i in range(1, len(price_data)):
            if price_data[i - 1].close > 0:
                log_return = Decimal(
                    str(math.log(float(price_data[i].close / price_data[i - 1].close)))
                )
                returns.append(log_return)

        return returns

    def _calculate_simple_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate simple historical volatility."""
        if not returns:
            return Decimal(0)

        # Convert to float for statistics calculation
        float_returns = [float(r) for r in returns]

        try:
            std_dev = statistics.stdev(float_returns) if len(float_returns) > 1 else 0
            return Decimal(str(std_dev))
        except (ValueError, TypeError, ZeroDivisionError, statistics.StatisticsError):
            return Decimal(0)

    def _calculate_ewma_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate exponentially weighted moving average volatility."""
        if not returns:
            return Decimal(0)

        # Initialize with simple variance
        variance = returns[0] ** 2

        # Calculate EWMA variance
        for i in range(1, len(returns)):
            variance = self.ewma_lambda * variance + (1 - self.ewma_lambda) * (returns[i] ** 2)

        # Return volatility (square root of variance)
        return Decimal(str(math.sqrt(float(variance))))

    def _calculate_garch_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate GARCH(1,1) volatility."""
        if not returns:
            return Decimal(0)

        # Initialize with unconditional variance
        long_run_variance = sum(r**2 for r in returns) / Decimal(len(returns))
        variance = long_run_variance

        # Calculate GARCH variance
        for i in range(1, len(returns)):
            variance = (
                self.garch_omega
                + self.garch_alpha * (returns[i - 1] ** 2)
                + self.garch_beta * variance
            )

        # Return volatility (square root of variance)
        return Decimal(str(math.sqrt(float(variance))))

    def _calculate_realized_volatility(self, price_data: list[PriceData]) -> Decimal:
        """Calculate realized volatility using high-frequency data."""
        if len(price_data) < MIN_SAMPLES_FOR_REALIZED_VOLATILITY:
            return Decimal(0)

        # Calculate squared returns
        squared_returns: list[Decimal] = []
        for i in range(1, len(price_data)):
            if price_data[i - 1].close > 0:
                log_return = math.log(float(price_data[i].close / price_data[i - 1].close))
                squared_returns.append(Decimal(str(log_return**2)))

        if not squared_returns:
            return Decimal(0)

        # Sum of squared returns
        realized_variance = sum(squared_returns)

        # Annualize and take square root
        return Decimal(str(math.sqrt(realized_variance)))

    def _calculate_parkinson_volatility(self, price_data: list[PriceData]) -> Decimal:
        """Calculate Parkinson volatility using high-low prices."""
        if not price_data:
            return Decimal(0)

        # Calculate sum of squared log high/low ratios
        sum_squared = Decimal(0)
        valid_points = 0

        for data in price_data:
            if data.high > 0 and data.low > 0:
                log_ratio = Decimal(str(math.log(float(data.high / data.low))))
                sum_squared += log_ratio**2
                valid_points += 1

        if valid_points == 0:
            return Decimal(0)

        # Parkinson volatility formula
        factor = Decimal(str(1 / (4 * math.log(2))))
        variance = factor * (sum_squared / valid_points)

        return Decimal(str(math.sqrt(float(variance))))

    def _calculate_skewness(self, returns: list[Decimal], mean: Decimal, std: Decimal) -> Decimal:
        """Calculate skewness of returns."""
        if std == 0 or len(returns) < MIN_SAMPLES_FOR_SKEWNESS:
            return Decimal(0)

        n = len(returns)
        sum_cubed = sum((r - mean) ** 3 for r in returns)

        return (Decimal(str(n)) / (Decimal(str(n - 1)) * Decimal(str(n - 2)))) * (
            sum_cubed / (std**3)
        )

    def _calculate_kurtosis(self, returns: list[Decimal], mean: Decimal, std: Decimal) -> Decimal:
        """Calculate kurtosis of returns."""
        if std == 0 or len(returns) < MIN_SAMPLES_FOR_KURTOSIS:
            return Decimal(0)

        n = len(returns)
        sum_fourth = sum((r - mean) ** 4 for r in returns)

        # Excess kurtosis (subtract 3 for normal distribution)
        return (
            Decimal(str(n))
            * Decimal(str(n + 1))
            / (Decimal(str(n - 1)) * Decimal(str(n - 2)) * Decimal(str(n - 3)))
        ) * (sum_fourth / (std**4)) - Decimal(3)

    def _calculate_upside_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate upside volatility (volatility of positive returns)."""
        positive_returns = [r for r in returns if r > 0]

        if len(positive_returns) < MIN_SAMPLES_FOR_UPSIDE_VOLATILITY:
            return Decimal(0)

        return self._calculate_simple_volatility(positive_returns)

    def _calculate_downside_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate downside volatility (volatility of negative returns)."""
        negative_returns = [r for r in returns if r < 0]

        if len(negative_returns) < MIN_SAMPLES_FOR_DOWNSIDE_VOLATILITY:
            return Decimal(0)

        return self._calculate_simple_volatility(negative_returns)

    def _calculate_vol_of_vol(self, returns: list[Decimal], window: int = 20) -> Decimal | None:
        """Calculate volatility of volatility."""
        if len(returns) < window * 2:
            return None

        # Calculate rolling volatilities
        volatilities: list[Decimal] = []
        for i in range(window, len(returns)):
            window_returns = returns[i - window : i]
            vol = self._calculate_simple_volatility(window_returns)
            volatilities.append(vol)

        if len(volatilities) < MIN_VOLATILITIES_FOR_VOL_OF_VOL:
            return None

        # Calculate volatility of volatilities
        return self._calculate_simple_volatility(volatilities)

    def calculate_volatility_for_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        historical_prices: list[PriceData] | None = None,
    ) -> VolatilityResult:
        """Calculate volatility for an arbitrage opportunity.

        Args:
            opportunity: Arbitrage opportunity
            historical_prices: Optional historical price data

        Returns:
            VolatilityResult
        """
        # Try to get volatility from opportunity
        if opportunity.volatility is not None:
            try:
                volatility = opportunity.volatility  # Already Decimal from Pydantic

                # Create simple result
                return VolatilityResult(
                    volatility=volatility,
                    annualized_volatility=volatility * Decimal(str(math.sqrt(365.25))),
                    method=VolatilityMethod.IMPLIED,
                    timeframe=VolatilityTimeframe.DAILY,
                    data_points=1,
                    period_hours=24,
                    mean_return=Decimal(0),
                    std_deviation=volatility,
                    calculation_timestamp=datetime.now(tz=UTC),
                )
            except (ValueError, TypeError):
                pass

        # Use historical prices if provided
        if historical_prices:
            return self.calculate_volatility(historical_prices)

        # Estimate from spread
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                spread = Decimal(str(spread_percentage))
                # Rough estimate: volatility is typically 2-3x spread
                estimated_volatility = spread * Decimal("2.5")

                return VolatilityResult(
                    volatility=estimated_volatility,
                    annualized_volatility=estimated_volatility * Decimal(str(math.sqrt(365.25))),
                    method=VolatilityMethod.IMPLIED,
                    timeframe=VolatilityTimeframe.DAILY,
                    data_points=1,
                    period_hours=24,
                    mean_return=Decimal(0),
                    std_deviation=estimated_volatility,
                    calculation_timestamp=datetime.now(tz=UTC),
                    warnings=["Volatility estimated from spread"],
                )
            except (ValueError, TypeError):
                pass

        # Use configured default volatility
        return VolatilityResult(
            volatility=self.default_volatility,
            annualized_volatility=self.default_volatility * Decimal(str(math.sqrt(365.25))),
            method=VolatilityMethod.IMPLIED,
            timeframe=VolatilityTimeframe.DAILY,
            data_points=0,
            period_hours=0,
            mean_return=Decimal(0),
            std_deviation=self.default_volatility,
            calculation_timestamp=datetime.now(tz=UTC),
            warnings=[f"Using default volatility: {self.default_volatility}"],
        )

    def set_ewma_lambda(self, lambda_value: Decimal) -> None:
        """Set EWMA lambda parameter."""
        if lambda_value <= 0 or lambda_value >= 1:
            raise VolatilityCalculationError(VolatilityCalculationError.LAMBDA_OUT_OF_RANGE)

        self.ewma_lambda = lambda_value
        self.logger.info("Set EWMA lambda", lambda_value=float(lambda_value))

    def set_garch_parameters(self, omega: Decimal, alpha: Decimal, beta: Decimal) -> None:
        """Set GARCH parameters."""
        if omega < 0 or alpha < 0 or beta < 0:
            raise VolatilityCalculationError(VolatilityCalculationError.GARCH_PARAMS_NEGATIVE)

        if alpha + beta >= 1:
            raise VolatilityCalculationError(VolatilityCalculationError.GARCH_PARAMS_UNSTABLE)

        self.garch_omega = omega
        self.garch_alpha = alpha
        self.garch_beta = beta
        self.logger.info(
            "Set GARCH parameters",
            omega=float(omega),
            alpha=float(alpha),
            beta=float(beta),
        )

    def get_calculator_stats(self) -> dict[str, Any]:
        """Get calculator statistics."""
        return {
            "default_method": self.default_method.value,
            "default_timeframe": self.default_timeframe.value,
            "min_data_points": self.min_data_points,
            "max_data_points": self.max_data_points,
            "ewma_lambda": float(self.ewma_lambda),
            "garch_omega": float(self.garch_omega),
            "garch_alpha": float(self.garch_alpha),
            "garch_beta": float(self.garch_beta),
            "cache_size": len(self.calculation_cache),
            "cache_ttl_seconds": self.cache_ttl_seconds,
        }

    def clear_cache(self) -> None:
        """Clear calculation cache."""
        self.calculation_cache.clear()
        self.logger.info("Cleared volatility calculation cache")
