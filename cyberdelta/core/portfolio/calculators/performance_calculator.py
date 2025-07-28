"""Performance calculator with direct AppSettings access following risk module patterns."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.base import CalculationResult, TypedCalculator
from cyberdelta.core.portfolio.base.typed_calculator import CalculationMetadata
from cyberdelta.core.portfolio.exceptions import (
    CalculatorValidationError,
    InvalidCalculationInputError,
)


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol


# Constants for magic values
MIN_PORTFOLIO_VALUES = 2
MIN_RETURNS_FOR_CALCULATION = 2
MIN_TRADES_FOR_METRICS = 2
MIN_TRADES_FOR_WIN_RATE = 2
TRADING_DAYS_PER_YEAR = 252
MINIMUM_TRADES_FOR_STATS = 10


class PerformanceMetrics(BaseModel):
    """Performance calculation result with validation."""

    # Return metrics
    total_return: Decimal = Field(description="Total return amount")
    total_return_percent: Decimal = Field(description="Total return percentage")
    annualized_return: Decimal = Field(description="Annualized return percentage")

    # Risk metrics
    volatility: Decimal = Field(ge=0, description="Portfolio volatility (non-negative)")
    sharpe_ratio: Decimal | None = Field(default=None, description="Risk-adjusted return metric")
    max_drawdown: Decimal = Field(le=0, description="Maximum drawdown (non-positive)")
    max_drawdown_percent: Decimal = Field(
        le=0, description="Maximum drawdown percentage (non-positive)"
    )

    # Additional metrics
    calmar_ratio: Decimal | None = Field(default=None, description="Return over max drawdown")
    sortino_ratio: Decimal | None = Field(default=None, description="Downside risk-adjusted return")
    win_rate: Decimal | None = Field(
        default=None, ge=0, le=1, description="Percentage of winning trades"
    )
    average_win: Decimal | None = Field(
        default=None, ge=0, description="Average winning trade amount"
    )
    average_loss: Decimal | None = Field(
        default=None, le=0, description="Average losing trade amount"
    )
    profit_factor: Decimal | None = Field(
        default=None, ge=0, description="Ratio of gross profit to gross loss"
    )

    # Time metrics
    analysis_period_days: int = Field(gt=0, description="Number of days in analysis period")
    number_of_trades: int = Field(ge=0, description="Total number of trades")

    # Statistical metrics
    var_95: Decimal | None = Field(default=None, le=0, description="95% Value at Risk")
    var_99: Decimal | None = Field(default=None, le=0, description="99% Value at Risk")
    expected_shortfall: Decimal | None = Field(
        default=None, le=0, description="Expected shortfall beyond VaR"
    )

    @field_validator("total_return", "total_return_percent", "annualized_return", mode="before")
    @classmethod
    def validate_finite_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure all return metrics are finite.
        
        Args:
            v: The value to validate as a finite decimal
            
        Returns:
            The validated decimal value
            
        Raises:
            InvalidCalculationInputError: If the value is not finite (e.g., infinity, NaN)
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="return_metric", value=value, expected="finite decimal value"
            )
        return value

    @field_validator("win_rate", mode="before")
    @classmethod
    def validate_win_rate(cls, v: Decimal | str | float | None) -> Decimal | None:
        """Validate win rate is a valid percentage.
        
        Args:
            v: The win rate value to validate (should be between 0 and 1)
            
        Returns:
            The validated win rate as a decimal, or None if input was None
            
        Raises:
            InvalidCalculationInputError: If the win rate is not between 0 and 1
        """
        if v is not None:
            value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
            if not (Decimal(0) <= value <= Decimal(1)):
                raise InvalidCalculationInputError(
                    parameter="win_rate", value=value, expected="value between 0 and 1"
                )
            return value
        return v


class PerformanceInput(BaseModel):
    """Input for performance calculation with validation."""

    portfolio_values: list[Decimal] = Field(
        min_length=MIN_PORTFOLIO_VALUES, description="Time series of portfolio values"
    )
    timestamps: list[int] | None = Field(default=None, description="Unix timestamps (optional)")
    benchmark_values: list[Decimal] | None = Field(
        default=None, description="Benchmark comparison values"
    )
    trade_returns: list[Decimal] | None = Field(
        default=None, description="Individual trade returns"
    )
    risk_free_rate: Decimal | None = Field(
        default=None, ge=0, le=1, description="Risk-free rate for Sharpe ratio"
    )

    @field_validator("portfolio_values", mode="before")
    @classmethod
    def validate_portfolio_values(cls, v: list[Decimal | str | float | int]) -> list[Decimal]:
        """Validate portfolio values are positive and finite.
        
        Args:
            v: List of portfolio values to validate
            
        Returns:
            List of validated Decimal portfolio values
            
        Raises:
            CalculatorValidationError: If the portfolio values list is empty
            InvalidCalculationInputError: If any value is not finite or not positive
        """
        if not v:
            raise CalculatorValidationError(
                field_name="portfolio_values", constraint="cannot be empty"
            )

        validated_values: list[Decimal] = []
        for i, value in enumerate(v):
            val: Decimal = value if isinstance(value, Decimal) else Decimal(str(value))
            if not val.is_finite():
                raise InvalidCalculationInputError(
                    parameter=f"portfolio_value[{i}]", value=val, expected="finite decimal value"
                )
            if val <= 0:
                raise InvalidCalculationInputError(
                    parameter=f"portfolio_value[{i}]", value=val, expected="positive decimal value"
                )
            validated_values.append(val)

        return validated_values

    @field_validator("timestamps", mode="before")
    @classmethod
    def validate_timestamps(cls, v: list[int] | None, info: ValidationInfo) -> list[int] | None:
        """Validate timestamps if provided.
        
        Args:
            v: List of timestamps to validate, or None
            info: Validation context information
            
        Returns:
            The validated list of timestamps, or None if input was None
            
        Raises:
            CalculatorValidationError: If timestamps length doesn't match portfolio values
            InvalidCalculationInputError: If any timestamp is negative
        """
        if v is not None:
            portfolio_values = info.data.get("portfolio_values", [])
            if len(v) != len(portfolio_values):
                raise CalculatorValidationError(
                    field_name="timestamps",
                    constraint=(
                        f"length ({len(v)}) must match portfolio values "
                        f"length ({len(portfolio_values)})"
                    ),
                )
            if any(ts < 0 for ts in v):
                raise InvalidCalculationInputError(
                    parameter="timestamps",
                    value="negative timestamp found",
                    expected="non-negative integers",
                )
        return v

    @field_validator("benchmark_values", mode="before")
    @classmethod
    def validate_benchmark_values(
        cls, v: list[Decimal | str | float | int] | None, info: ValidationInfo
    ) -> list[Decimal] | None:
        """Validate benchmark values if provided.
        
        Args:
            v: List of benchmark values to validate, or None
            info: Validation context information
            
        Returns:
            List of validated Decimal benchmark values, or None if input was None
            
        Raises:
            CalculatorValidationError: If benchmark values length doesn't match portfolio values
            InvalidCalculationInputError: If any value is not finite or not positive
        """
        if v is not None:
            portfolio_values = info.data.get("portfolio_values", [])
            if len(v) != len(portfolio_values):
                raise CalculatorValidationError(
                    field_name="benchmark_values",
                    constraint=(
                        f"length ({len(v)}) must match portfolio values "
                        f"length ({len(portfolio_values)})"
                    ),
                )

            validated_values: list[Decimal] = []
            for i, value in enumerate(v):
                val: Decimal = value if isinstance(value, Decimal) else Decimal(str(value))
                if not val.is_finite():
                    raise InvalidCalculationInputError(
                        parameter=f"benchmark_value[{i}]",
                        value=val,
                        expected="finite decimal value",
                    )
                if val <= 0:
                    raise InvalidCalculationInputError(
                        parameter=f"benchmark_value[{i}]",
                        value=val,
                        expected="positive decimal value",
                    )
                validated_values.append(val)

            return validated_values
        return v


class PerformanceCalculator(TypedCalculator[PerformanceInput, PerformanceMetrics]):
    """Performance calculator with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Inherits from TypedCalculator
    - Protocol-based dependencies
    - Strong typing with result types
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
    ) -> None:
        """Initialize the performance calculator.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
        """
        super().__init__(app_settings, state_container, "PerformanceCalculator")

        # Configuration from AppSettings
        self.risk_free_rate = self.portfolio_config.calculation.risk_free_rate
        self.var_confidence_levels = [Decimal("0.95"), Decimal("0.99")]
        self.min_periods_for_ratios = 30  # Minimum observations for meaningful ratios

        self.logger.info(
            "performance_calculator_created",
            risk_free_rate=self.risk_free_rate,
            min_periods_for_ratios=self.min_periods_for_ratios,
        )

    async def calculate(
        self, input_data: PerformanceInput
    ) -> CalculationResult[PerformanceMetrics]:
        """Calculate performance metrics for a portfolio.

        Args:
            input_data: Input containing portfolio value time series

        Returns:
            Calculation result with performance metrics
        """
        try:
            portfolio_values = input_data.portfolio_values
            trade_returns = input_data.trade_returns
            risk_free_rate = input_data.risk_free_rate or self.risk_free_rate

            if len(portfolio_values) < MIN_PORTFOLIO_VALUES:
                return CalculationResult[PerformanceMetrics].failure_result(
                    errors=["Insufficient data: need at least 2 portfolio values"],
                    metadata=CalculationMetadata(
                        calculator=self.calculator_name, error_type="InsufficientData"
                    ),
                )

            # Calculate returns from portfolio values
            returns = self._calculate_returns(portfolio_values)

            # Basic return metrics
            total_return = portfolio_values[-1] - portfolio_values[0]
            if portfolio_values[0] > 0:
                total_return_percent = (total_return / portfolio_values[0]) * 100
            else:
                total_return_percent = Decimal(0)

            # Annualized return (assume daily data)
            days = len(portfolio_values) - 1
            annualized_return = self._calculate_annualized_return(total_return_percent, days)

            # Calculate volatility (annualized)
            volatility = self._calculate_volatility(returns)

            # Sharpe ratio
            if len(returns) >= self.min_periods_for_ratios:
                sharpe_ratio = self._calculate_sharpe_ratio(returns, risk_free_rate)
            else:
                sharpe_ratio = None

            # Drawdown metrics
            max_drawdown, max_drawdown_percent = self._calculate_max_drawdown(portfolio_values)

            # Additional risk metrics
            calmar_ratio = self._calculate_calmar_ratio(annualized_return, max_drawdown_percent)
            if len(returns) >= self.min_periods_for_ratios:
                sortino_ratio = self._calculate_sortino_ratio(returns, risk_free_rate)
            else:
                sortino_ratio = None

            # Trade-based metrics
            win_rate = None
            average_win = None
            average_loss = None
            profit_factor = None

            if trade_returns:
                trade_metrics = self._calculate_trade_metrics(trade_returns)
                win_rate, average_win, average_loss, profit_factor = trade_metrics

            # VaR and Expected Shortfall
            var_95, var_99, expected_shortfall = self._calculate_var_metrics(returns)

            metrics = PerformanceMetrics(
                total_return=total_return,
                total_return_percent=total_return_percent,
                annualized_return=annualized_return,
                volatility=volatility,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                max_drawdown_percent=max_drawdown_percent,
                calmar_ratio=calmar_ratio,
                sortino_ratio=sortino_ratio,
                win_rate=win_rate,
                average_win=average_win,
                average_loss=average_loss,
                profit_factor=profit_factor,
                analysis_period_days=days,
                number_of_trades=len(trade_returns) if trade_returns else 0,
                var_95=var_95,
                var_99=var_99,
                expected_shortfall=expected_shortfall,
            )

            # Generate warnings
            warnings: list[str] = []
            if sharpe_ratio is not None and sharpe_ratio < Decimal(0):
                warnings.append(f"Negative Sharpe ratio: {sharpe_ratio}")
            if max_drawdown_percent > Decimal(20):
                warnings.append(f"High maximum drawdown: {max_drawdown_percent}%")
            if len(returns) < self.min_periods_for_ratios:
                warnings.append(
                    f"Insufficient data for reliable ratios "
                    f"(need {self.min_periods_for_ratios}+ periods)"
                )

            return CalculationResult[PerformanceMetrics].success_result(
                result=metrics,
                warnings=warnings,
                metadata=CalculationMetadata(calculator=self.calculator_name),
            )

        except (ValueError, TypeError, ArithmeticError) as e:
            return CalculationResult[PerformanceMetrics].failure_result(
                errors=[f"Performance calculation failed: {e}"],
                metadata=CalculationMetadata(
                    calculator=self.calculator_name, error_type=type(e).__name__
                ),
            )

    async def validate_input(self, input_data: PerformanceInput) -> tuple[bool, list[str]]:
        """Validate input data for performance calculation.

        Args:
            input_data: Input to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors: list[str] = []

        if not input_data.portfolio_values:
            errors.append("Portfolio values are required")
        elif len(input_data.portfolio_values) < MIN_PORTFOLIO_VALUES:
            errors.append("Need at least 2 portfolio values for performance calculation")

        # Check for non-positive values
        if any(value <= 0 for value in input_data.portfolio_values):
            errors.append("Portfolio values must be positive")

        # Validate optional inputs
        if input_data.timestamps and len(input_data.timestamps) != len(input_data.portfolio_values):
            errors.append("Timestamps length must match portfolio values length")

        if input_data.benchmark_values and len(input_data.benchmark_values) != len(
            input_data.portfolio_values
        ):
            errors.append("Benchmark values length must match portfolio values length")

        if input_data.risk_free_rate is not None and input_data.risk_free_rate < 0:
            errors.append("Risk-free rate cannot be negative")

        return len(errors) == 0, errors

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period returns from value series.
        
        Args:
            values: List of portfolio values over time
            
        Returns:
            List of period-over-period returns as decimals
        """
        returns: list[Decimal] = []
        for i in range(1, len(values)):
            if values[i - 1] > 0:
                ret = (values[i] - values[i - 1]) / values[i - 1]
                returns.append(ret)
            else:
                returns.append(Decimal(0))
        return returns

    def _calculate_annualized_return(self, total_return_percent: Decimal, days: int) -> Decimal:
        """Calculate annualized return from total return.
        
        Args:
            total_return_percent: Total return as a percentage
            days: Number of days in the period
            
        Returns:
            Annualized return as a percentage
        """
        if days <= 0:
            return Decimal(0)

        # Convert to decimal form and annualize
        total_return_decimal = total_return_percent / 100
        periods_per_year = Decimal(365) / Decimal(days)

        # Compound annual growth rate formula: (1 + total_return)^(1/years) - 1
        try:
            annualized = (1 + float(total_return_decimal)) ** float(periods_per_year) - 1
            return Decimal(str(annualized)) * 100  # Convert back to percentage
        except (ValueError, OverflowError):
            return Decimal(0)

    def _calculate_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate annualized volatility from returns.
        
        Args:
            returns: List of period returns
            
        Returns:
            Annualized volatility as a percentage
        """
        if len(returns) < MIN_RETURNS_FOR_CALCULATION:
            return Decimal(0)

        # Calculate standard deviation
        mean_return = sum(returns) / Decimal(len(returns))
        variance = sum((r - mean_return) ** 2 for r in returns) / Decimal(len(returns) - 1)

        try:
            std_dev = Decimal(str(math.sqrt(float(variance))))
            # Annualize assuming daily returns
            annualized_vol = std_dev * Decimal(365).sqrt()
            return annualized_vol * 100  # Convert to percentage
        except (ValueError, OverflowError):
            return Decimal(0)

    def _calculate_sharpe_ratio(
        self, returns: list[Decimal], risk_free_rate: Decimal
    ) -> Decimal | None:
        """Calculate Sharpe ratio.
        
        Args:
            returns: List of period returns
            risk_free_rate: Annual risk-free rate as a decimal
            
        Returns:
            Annualized Sharpe ratio, or None if insufficient data
        """
        if len(returns) < MIN_RETURNS_FOR_CALCULATION:
            return None

        # Convert annual risk-free rate to daily
        daily_rf_rate = risk_free_rate / 365 / 100

        # Calculate excess returns
        excess_returns = [r - daily_rf_rate for r in returns]

        # Calculate mean and std of excess returns
        mean_excess = sum(excess_returns) / Decimal(len(excess_returns))

        if len(excess_returns) < MIN_RETURNS_FOR_CALCULATION:
            return None

        variance = sum((r - mean_excess) ** 2 for r in excess_returns) / Decimal(
            len(excess_returns) - 1
        )

        try:
            std_excess = Decimal(str(math.sqrt(float(variance))))
            if std_excess > 0:
                # Annualize the Sharpe ratio
                return mean_excess / std_excess * Decimal(365).sqrt()
        except (ValueError, OverflowError, ZeroDivisionError):
            pass

        return None

    def _calculate_max_drawdown(self, values: list[Decimal]) -> tuple[Decimal, Decimal]:
        """Calculate maximum drawdown in absolute and percentage terms.
        
        Args:
            values: List of portfolio values over time
            
        Returns:
            Tuple of (absolute maximum drawdown, maximum drawdown percentage)
        """
        max_drawdown = Decimal(0)
        max_drawdown_percent = Decimal(0)
        peak = values[0]

        for value in values[1:]:
            if value > peak:
                peak = value
            else:
                drawdown = peak - value
                drawdown_percent = (drawdown / peak) * 100 if peak > 0 else Decimal(0)

                max_drawdown = max(max_drawdown, drawdown)
                max_drawdown_percent = max(max_drawdown_percent, drawdown_percent)

        return max_drawdown, max_drawdown_percent

    def _calculate_calmar_ratio(
        self, annualized_return: Decimal, max_drawdown_percent: Decimal
    ) -> Decimal | None:
        """Calculate Calmar ratio (annualized return / max drawdown).
        
        Args:
            annualized_return: Annualized return as a percentage
            max_drawdown_percent: Maximum drawdown as a percentage
            
        Returns:
            Calmar ratio, or None if maximum drawdown is zero
        """
        if max_drawdown_percent > 0:
            return annualized_return / max_drawdown_percent
        return None

    def _calculate_sortino_ratio(
        self, returns: list[Decimal], risk_free_rate: Decimal
    ) -> Decimal | None:
        """Calculate Sortino ratio (excess return / downside deviation).
        
        Args:
            returns: List of period returns
            risk_free_rate: Annual risk-free rate as a decimal
            
        Returns:
            Annualized Sortino ratio, or None if insufficient data or zero downside deviation
        """
        if len(returns) < MIN_RETURNS_FOR_CALCULATION:
            return None

        daily_rf_rate = risk_free_rate / 365 / 100
        excess_returns = [r - daily_rf_rate for r in returns]

        # Calculate downside deviation (only negative excess returns)
        downside_returns = [min(r, Decimal(0)) for r in excess_returns]
        mean_excess = sum(excess_returns) / Decimal(len(excess_returns))

        if len(downside_returns) < MIN_RETURNS_FOR_CALCULATION:
            return None

        downside_variance = sum(r**2 for r in downside_returns) / Decimal(len(downside_returns))

        try:
            downside_std = Decimal(str(math.sqrt(float(downside_variance))))
            if downside_std > 0:
                return (mean_excess / downside_std) * Decimal(365).sqrt()
        except (ValueError, OverflowError, ZeroDivisionError):
            pass

        return None

    def _calculate_trade_metrics(
        self, trade_returns: list[Decimal]
    ) -> tuple[Decimal, Decimal, Decimal, Decimal | None]:
        """Calculate trade-based performance metrics.
        
        Args:
            trade_returns: List of individual trade returns
            
        Returns:
            Tuple of (win_rate_percent, average_win, average_loss, profit_factor)
        """
        if not trade_returns:
            return Decimal(0), Decimal(0), Decimal(0), None

        winning_trades = [r for r in trade_returns if r > 0]
        losing_trades = [r for r in trade_returns if r < 0]

        win_rate = Decimal(len(winning_trades)) / Decimal(len(trade_returns)) * 100

        if winning_trades:
            average_win = sum(winning_trades) / Decimal(len(winning_trades))
        else:
            average_win = Decimal(0)
        if losing_trades:
            average_loss = sum(losing_trades) / Decimal(len(losing_trades))
        else:
            average_loss = Decimal(0)

        # Profit factor = gross profit / gross loss
        gross_profit = sum(winning_trades, Decimal(0))
        gross_loss = abs(sum(losing_trades, Decimal(0)))
        profit_factor = gross_profit / gross_loss if gross_loss > Decimal(0) else None

        return win_rate, average_win, average_loss, profit_factor

    def _calculate_var_metrics(
        self, returns: list[Decimal]
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        """Calculate Value at Risk and Expected Shortfall metrics.
        
        Args:
            returns: List of period returns
            
        Returns:
            Tuple of (VaR_95%, VaR_99%, Expected_Shortfall) or None values if insufficient data
        """
        if len(returns) < MINIMUM_TRADES_FOR_STATS:  # Need sufficient data for meaningful VaR
            return None, None, None

        # Sort returns for percentile calculation
        sorted_returns = sorted(returns)
        n = len(sorted_returns)

        # Calculate VaR at 95% and 99% confidence levels
        var_95_index = int(n * 0.05)  # 5th percentile (worst 5%)
        var_99_index = int(n * 0.01)  # 1st percentile (worst 1%)

        var_95 = abs(sorted_returns[var_95_index]) if var_95_index < n else None
        var_99 = abs(sorted_returns[var_99_index]) if var_99_index < n else None

        # Expected Shortfall (Conditional VaR) - average of returns worse than VaR 95%
        if var_95_index > 0:
            tail_returns = sorted_returns[: var_95_index + 1]
            expected_shortfall = abs(sum(tail_returns) / Decimal(len(tail_returns)))
        else:
            expected_shortfall = None

        return var_95, var_99, expected_shortfall
