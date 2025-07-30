"""Risk metrics calculator for comprehensive risk analysis."""

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.base_exceptions import RiskCalculationError
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


def _create_str_list() -> list[str]:
    """Create typed string list for dataclass fields.

    Returns:
        Empty list of strings for use as dataclass field default factory.
    """
    return []


# Statistical calculation constants
MIN_SAMPLES_FOR_STATISTICAL_CALCULATION = 2  # Minimum data points for statistical metrics


class RiskMetricType(Enum):
    """Types of risk metrics."""

    VALUE_AT_RISK = "value_at_risk"
    CONDITIONAL_VAR = "conditional_var"
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    MAX_DRAWDOWN = "max_drawdown"
    BETA = "beta"
    ALPHA = "alpha"
    TREYNOR_RATIO = "treynor_ratio"
    CALMAR_RATIO = "calmar_ratio"
    OMEGA_RATIO = "omega_ratio"
    INFORMATION_RATIO = "information_ratio"


@dataclass
class PortfolioSnapshot:
    """Snapshot of portfolio state at a point in time."""

    timestamp: datetime
    total_value: Decimal
    positions: list[SizedOpportunity]
    cash_balance: Decimal

    # P&L metrics
    unrealized_pnl: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)

    # Risk metrics
    total_exposure: Decimal = Decimal(0)
    net_exposure: Decimal = Decimal(0)
    gross_leverage: Decimal = Decimal(0)
    net_leverage: Decimal = Decimal(0)

    @property
    def total_pnl(self) -> Decimal:
        """Total P&L (realized + unrealized).

        Returns:
            Sum of realized and unrealized P&L
        """
        return self.realized_pnl + self.unrealized_pnl

    @property
    def position_count(self) -> int:
        """Number of active positions.

        Returns:
            Count of positions in the snapshot
        """
        return len(self.positions)

    @property
    def capital_deployed(self) -> Decimal:
        """Total capital deployed in positions.

        Returns:
            Total value minus cash balance
        """
        return self.total_value - self.cash_balance


@dataclass
class RiskMetricsResult:
    """Comprehensive risk metrics calculation result."""

    # Core risk metrics
    value_at_risk_95: Decimal
    value_at_risk_99: Decimal
    conditional_var_95: Decimal
    conditional_var_99: Decimal

    # Performance metrics
    sharpe_ratio: Decimal
    sortino_ratio: Decimal
    calmar_ratio: Decimal

    # Drawdown metrics
    max_drawdown: Decimal
    max_drawdown_duration_days: int
    current_drawdown: Decimal

    # Volatility metrics
    portfolio_volatility: Decimal
    downside_volatility: Decimal
    upside_volatility: Decimal

    # Exposure metrics
    gross_exposure: Decimal
    net_exposure: Decimal
    concentration_ratio: Decimal

    # Greek-like metrics
    portfolio_beta: Decimal | None = None
    portfolio_alpha: Decimal | None = None

    # Additional metrics
    win_rate: Decimal = Decimal(0)
    profit_factor: Decimal = Decimal(0)
    expected_return: Decimal = Decimal(0)

    # Metadata
    calculation_period_days: int = 0
    data_points: int = 0
    calculation_timestamp: datetime | None = None
    warnings: list[str] = field(default_factory=_create_str_list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of risk metrics organized by category.
        """
        return {
            "value_at_risk": {
                "var_95": float(self.value_at_risk_95),
                "var_99": float(self.value_at_risk_99),
                "cvar_95": float(self.conditional_var_95),
                "cvar_99": float(self.conditional_var_99),
            },
            "performance": {
                "sharpe_ratio": float(self.sharpe_ratio),
                "sortino_ratio": float(self.sortino_ratio),
                "calmar_ratio": float(self.calmar_ratio),
                "win_rate": float(self.win_rate),
                "profit_factor": float(self.profit_factor),
                "expected_return": float(self.expected_return),
            },
            "drawdown": {
                "max_drawdown": float(self.max_drawdown),
                "max_drawdown_duration_days": self.max_drawdown_duration_days,
                "current_drawdown": float(self.current_drawdown),
            },
            "volatility": {
                "portfolio_volatility": float(self.portfolio_volatility),
                "downside_volatility": float(self.downside_volatility),
                "upside_volatility": float(self.upside_volatility),
            },
            "exposure": {
                "gross_exposure": float(self.gross_exposure),
                "net_exposure": float(self.net_exposure),
                "concentration_ratio": float(self.concentration_ratio),
            },
            "greeks": {
                "beta": float(self.portfolio_beta) if self.portfolio_beta else None,
                "alpha": float(self.portfolio_alpha) if self.portfolio_alpha else None,
            },
            "metadata": {
                "calculation_period_days": self.calculation_period_days,
                "data_points": self.data_points,
                "calculation_timestamp": (
                    self.calculation_timestamp.isoformat() if self.calculation_timestamp else None
                ),
                "warnings": self.warnings,
            },
        }


class RiskMetricsCalculator:
    """Advanced risk metrics calculator for portfolio analysis."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the risk metrics calculator."""
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)

        # Configuration
        self.risk_free_rate = self.config.get("risk_free_rate", Decimal("0.02"))  # 2% annual
        self.confidence_levels = self.config.get("confidence_levels", [0.95, 0.99])
        self.min_data_points = self.config.get("min_data_points", 30)

        # VaR parameters
        # historical, parametric, monte_carlo
        self.var_method = self.config.get("var_method", "historical")
        self.var_horizon_days = self.config.get("var_horizon_days", 1)

        # Benchmark data
        self.benchmark_returns: list[Decimal] = []
        self.benchmark_symbol = self.config.get("benchmark_symbol", "BTC")

    def calculate_risk_metrics(
        self,
        portfolio_snapshots: list[PortfolioSnapshot],
        benchmark_returns: list[Decimal] | None = None,
    ) -> RiskMetricsResult:
        """Calculate comprehensive risk metrics for portfolio.

        Args:
            portfolio_snapshots: Historical portfolio snapshots
            benchmark_returns: Optional benchmark returns for relative metrics

        Returns:
            RiskMetricsResult with all calculated metrics

        Raises:
            RiskCalculationError: If insufficient data points provided for calculation.
        """
        if len(portfolio_snapshots) < self.min_data_points:
            raise RiskCalculationError(
                RiskCalculationError.INSUFFICIENT_DATA,
                calculation_type="risk_metrics",
                input_data_size=len(portfolio_snapshots),
                metadata={"min_required": self.min_data_points},
            )

        # Sort snapshots by timestamp
        snapshots = sorted(portfolio_snapshots, key=lambda s: s.timestamp)

        # Calculate returns
        returns = self._calculate_returns(snapshots)

        # Calculate VaR metrics
        var_95 = self._calculate_var(returns, 0.95)
        var_99 = self._calculate_var(returns, 0.99)
        cvar_95 = self._calculate_cvar(returns, 0.95)
        cvar_99 = self._calculate_cvar(returns, 0.99)

        # Calculate performance metrics
        sharpe_ratio = self._calculate_sharpe_ratio(returns)
        sortino_ratio = self._calculate_sortino_ratio(returns)

        # Calculate drawdown metrics
        max_drawdown, max_dd_duration = self._calculate_max_drawdown(snapshots)
        current_drawdown = self._calculate_current_drawdown(snapshots)
        calmar_ratio = self._calculate_calmar_ratio(returns, max_drawdown)

        # Calculate volatility metrics
        portfolio_volatility = self._calculate_volatility(returns)
        downside_volatility = self._calculate_downside_volatility(returns)
        upside_volatility = self._calculate_upside_volatility(returns)

        # Calculate exposure metrics
        latest_snapshot = snapshots[-1]
        gross_exposure = latest_snapshot.total_exposure
        net_exposure = latest_snapshot.net_exposure
        concentration_ratio = self._calculate_concentration_ratio(latest_snapshot)

        # Calculate relative metrics if benchmark provided
        portfolio_beta = None
        portfolio_alpha = None
        if benchmark_returns:
            portfolio_beta = self._calculate_beta(returns, benchmark_returns)
            portfolio_alpha = self._calculate_alpha(returns, benchmark_returns, portfolio_beta)

        # Calculate additional metrics
        win_rate = self._calculate_win_rate(returns)
        profit_factor = self._calculate_profit_factor(returns)
        expected_return = sum(returns) / Decimal(len(returns)) if returns else Decimal(0)

        # Calculate metadata
        time_diff = snapshots[-1].timestamp - snapshots[0].timestamp
        calculation_period_days = time_diff.days

        # Create result
        result = RiskMetricsResult(
            value_at_risk_95=var_95,
            value_at_risk_99=var_99,
            conditional_var_95=cvar_95,
            conditional_var_99=cvar_99,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            max_drawdown_duration_days=max_dd_duration,
            current_drawdown=current_drawdown,
            portfolio_volatility=portfolio_volatility,
            downside_volatility=downside_volatility,
            upside_volatility=upside_volatility,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            concentration_ratio=concentration_ratio,
            portfolio_beta=portfolio_beta,
            portfolio_alpha=portfolio_alpha,
            win_rate=win_rate,
            profit_factor=profit_factor,
            expected_return=Decimal(str(expected_return)),
            calculation_period_days=calculation_period_days,
            data_points=len(snapshots),
            calculation_timestamp=datetime.now(tz=UTC),
        )

        # Add warnings
        if portfolio_volatility > Decimal("0.5"):
            result.warnings.append(f"High portfolio volatility: {portfolio_volatility:.2%}")

        if max_drawdown > Decimal("0.2"):
            result.warnings.append(f"Significant maximum drawdown: {max_drawdown:.2%}")

        if sharpe_ratio < Decimal("0.5"):
            result.warnings.append(f"Low Sharpe ratio: {sharpe_ratio:.2f}")

        self.logger.debug(
            "Calculated risk metrics",
            snapshot_count=len(snapshots),
        )
        return result

    def _calculate_returns(self, snapshots: list[PortfolioSnapshot]) -> list[Decimal]:
        """Calculate portfolio returns from snapshots.

        Returns:
            List of simple returns calculated from consecutive portfolio values.
        """
        returns: list[Decimal] = []

        for i in range(1, len(snapshots)):
            if snapshots[i - 1].total_value > 0:
                simple_return = (
                    snapshots[i].total_value - snapshots[i - 1].total_value
                ) / snapshots[i - 1].total_value
                returns.append(simple_return)

        return returns

    def _calculate_var(self, returns: list[Decimal], confidence_level: float) -> Decimal:
        """Calculate Value at Risk.

        Returns:
            Value at Risk at the specified confidence level, or 0 if no returns.
        """
        if not returns:
            return Decimal(0)

        # Sort returns in ascending order
        sorted_returns = sorted(returns)

        # Find the percentile
        index = int((1 - confidence_level) * len(sorted_returns))

        if index < len(sorted_returns):
            return -sorted_returns[index]  # Negative because VaR is a loss
        return -sorted_returns[0]

    def _calculate_cvar(self, returns: list[Decimal], confidence_level: float) -> Decimal:
        """Calculate Conditional Value at Risk (Expected Shortfall).

        Returns:
            Expected shortfall beyond VaR threshold, or 0 if no returns.
        """
        if not returns:
            return Decimal(0)

        # Sort returns in ascending order
        sorted_returns = sorted(returns)

        # Find the VaR threshold
        index = int((1 - confidence_level) * len(sorted_returns))

        # Calculate average of returns worse than VaR
        if index > 0:
            tail_returns = sorted_returns[:index]
            return (
                -sum(tail_returns, Decimal(0)) / Decimal(str(len(tail_returns)))
                if tail_returns
                else Decimal(0)
            )
        return -sorted_returns[0]

    def _calculate_sharpe_ratio(self, returns: list[Decimal]) -> Decimal:
        """Calculate Sharpe ratio.

        Returns:
            Annualized Sharpe ratio measuring risk-adjusted return, or 0 if insufficient data.
        """
        if not returns or len(returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION:
            return Decimal(0)

        # Calculate average return
        avg_return = sum(returns, Decimal(0)) / Decimal(str(len(returns)))

        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in returns) / Decimal(str(len(returns) - 1))
        std_dev = Decimal(str(math.sqrt(float(variance))))

        if std_dev == 0:
            return Decimal(0)

        # Annualized Sharpe ratio
        daily_risk_free = self.risk_free_rate / Decimal(365)
        excess_return = avg_return - daily_risk_free

        # Annualize
        annual_factor = Decimal(str(math.sqrt(365)))
        sharpe_ratio: Decimal = excess_return * annual_factor / std_dev

        return sharpe_ratio

    def _calculate_sortino_ratio(self, returns: list[Decimal]) -> Decimal:
        """Calculate Sortino ratio (uses downside volatility).

        Returns:
            Annualized Sortino ratio using downside deviation, or 0 if insufficient downside data.
        """
        if not returns:
            return Decimal(0)

        # Calculate average return
        avg_return = sum(returns) / Decimal(len(returns))

        # Calculate downside deviation
        downside_returns = [r for r in returns if r < 0]
        if len(downside_returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION:
            return Decimal(0)

        downside_variance = sum(r**2 for r in downside_returns) / Decimal(len(downside_returns))
        downside_dev = Decimal(str(math.sqrt(float(downside_variance))))

        if downside_dev == 0:
            return Decimal(0)

        # Annualized Sortino ratio
        daily_risk_free = self.risk_free_rate / Decimal(365)
        excess_return = avg_return - daily_risk_free

        # Annualize
        annual_factor = Decimal(str(math.sqrt(365)))
        sortino_ratio: Decimal = excess_return * annual_factor / downside_dev

        return sortino_ratio

    def _calculate_max_drawdown(self, snapshots: list[PortfolioSnapshot]) -> tuple[Decimal, int]:
        """Calculate maximum drawdown and duration.

        Returns:
            Tuple of (maximum drawdown percentage, duration in days).
        """
        if not snapshots:
            return Decimal(0), 0

        peak_value = snapshots[0].total_value
        max_drawdown = Decimal(0)
        max_duration = 0

        current_peak = peak_value
        current_trough = peak_value
        peak_date = snapshots[0].timestamp

        for snapshot in snapshots[1:]:
            if snapshot.total_value > current_peak:
                current_peak = snapshot.total_value
                peak_date = snapshot.timestamp
            else:
                current_trough = snapshot.total_value
                drawdown = (
                    (current_peak - current_trough) / current_peak
                    if current_peak > 0
                    else Decimal(0)
                )

                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    duration = (snapshot.timestamp - peak_date).days
                    max_duration = max(max_duration, duration)

        return max_drawdown, max_duration

    def _calculate_current_drawdown(self, snapshots: list[PortfolioSnapshot]) -> Decimal:
        """Calculate current drawdown from peak.

        Returns:
            Current drawdown percentage from historical peak, or 0 if no data.
        """
        if not snapshots:
            return Decimal(0)

        # Find peak value
        peak_value = max(s.total_value for s in snapshots)
        current_value = snapshots[-1].total_value

        if peak_value > 0:
            return (peak_value - current_value) / peak_value

        return Decimal(0)

    def _calculate_calmar_ratio(self, returns: list[Decimal], max_drawdown: Decimal) -> Decimal:
        """Calculate Calmar ratio (return / max drawdown).

        Returns:
            Calmar ratio measuring return per unit of maximum drawdown, or 0 if no data
                or zero drawdown.
        """
        if not returns or max_drawdown == 0:
            return Decimal(0)

        # Annualized return
        avg_daily_return = sum(returns, Decimal(0)) / Decimal(str(len(returns)))
        annualized_return = avg_daily_return * Decimal(365)

        return annualized_return / max_drawdown

    def _calculate_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate portfolio volatility.

        Returns:
            Annualized portfolio volatility, or 0 if insufficient data.
        """
        if len(returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION:
            return Decimal(0)

        avg_return = sum(returns, Decimal(0)) / Decimal(str(len(returns)))
        variance = sum((r - avg_return) ** 2 for r in returns) / Decimal(str(len(returns) - 1))

        # Annualized volatility
        daily_vol = Decimal(str(math.sqrt(float(variance))))
        return daily_vol * Decimal(str(math.sqrt(365)))

    def _calculate_downside_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate downside volatility.

        Returns:
            Annualized volatility of negative returns only, or 0 if insufficient negative returns.
        """
        negative_returns = [r for r in returns if r < 0]

        if len(negative_returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION:
            return Decimal(0)

        return self._calculate_volatility(negative_returns)

    def _calculate_upside_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate upside volatility.

        Returns:
            Annualized volatility of positive returns only, or 0 if insufficient positive returns.
        """
        positive_returns = [r for r in returns if r > 0]

        if len(positive_returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION:
            return Decimal(0)

        return self._calculate_volatility(positive_returns)

    def _calculate_concentration_ratio(self, snapshot: PortfolioSnapshot) -> Decimal:
        """Calculate portfolio concentration ratio.

        Returns:
            Herfindahl index measuring portfolio concentration, or 0 if no positions.
        """
        if not snapshot.positions or snapshot.total_value == 0:
            return Decimal(0)

        # Calculate Herfindahl index
        position_values = [pos.total_size_usd for pos in snapshot.positions]
        total_position_value = sum(position_values)

        if total_position_value == 0:
            return Decimal(0)

        # Sum of squared weights
        herfindahl = sum((value / total_position_value) ** 2 for value in position_values)

        return herfindahl or Decimal(0)

    def _calculate_beta(
        self, portfolio_returns: list[Decimal], benchmark_returns: list[Decimal]
    ) -> Decimal:
        """Calculate portfolio beta relative to benchmark.

        Returns:
            Portfolio beta measuring systematic risk relative to benchmark, default 1.0
                if insufficient data.
        """
        if (
            len(portfolio_returns) != len(benchmark_returns)
            or len(portfolio_returns) < MIN_SAMPLES_FOR_STATISTICAL_CALCULATION
        ):
            return Decimal(1)  # Default beta

        # Calculate covariance
        portfolio_avg = sum(portfolio_returns, Decimal(0)) / Decimal(str(len(portfolio_returns)))
        benchmark_avg = sum(benchmark_returns, Decimal(0)) / Decimal(str(len(benchmark_returns)))

        covariance = sum(
            (p - portfolio_avg) * (b - benchmark_avg)
            for p, b in zip(portfolio_returns, benchmark_returns, strict=False)
        ) / Decimal(str(len(portfolio_returns) - 1))

        # Calculate benchmark variance
        benchmark_variance = sum((b - benchmark_avg) ** 2 for b in benchmark_returns) / Decimal(
            str(len(benchmark_returns) - 1)
        )

        if benchmark_variance == 0:
            return Decimal(1)

        return covariance / benchmark_variance

    def _calculate_alpha(
        self,
        portfolio_returns: list[Decimal],
        benchmark_returns: list[Decimal],
        beta: Decimal,
    ) -> Decimal:
        """Calculate portfolio alpha (Jensen's alpha).

        Returns:
            Annualized Jensen's alpha measuring excess return above CAPM prediction, or 0
                if insufficient data.
        """
        if len(portfolio_returns) != len(benchmark_returns) or not portfolio_returns:
            return Decimal(0)

        # Average returns
        portfolio_avg = sum(portfolio_returns, Decimal(0)) / Decimal(str(len(portfolio_returns)))
        benchmark_avg = sum(benchmark_returns, Decimal(0)) / Decimal(str(len(benchmark_returns)))

        # Daily risk-free rate
        daily_rf = self.risk_free_rate / Decimal(365)

        # Jensen's alpha
        alpha = portfolio_avg - (daily_rf + beta * (benchmark_avg - daily_rf))

        # Annualize
        result: Decimal = alpha * Decimal(365)
        return result

    def _calculate_win_rate(self, returns: list[Decimal]) -> Decimal:
        """Calculate win rate (percentage of positive returns).

        Returns:
            Percentage of periods with positive returns as decimal (0.0 to 1.0).
        """
        if not returns:
            return Decimal(0)

        positive_returns = sum(1 for r in returns if r > 0)
        return Decimal(str(positive_returns)) / Decimal(str(len(returns)))

    def _calculate_profit_factor(self, returns: list[Decimal]) -> Decimal:
        """Calculate profit factor (gross profit / gross loss).

        Returns:
            Ratio of gross profits to gross losses, capped at 999 for infinite values.
        """
        if not returns:
            return Decimal(0)

        gross_profit = sum((r for r in returns if r > 0), Decimal(0))
        gross_loss = -sum((r for r in returns if r < 0), Decimal(0))

        if gross_loss == 0:
            return Decimal(999)  # Cap at 999 for infinite profit factor

        return gross_profit / gross_loss

    def calculate_position_risk_contribution(
        self,
        position: SizedOpportunity,
        portfolio_snapshot: PortfolioSnapshot,
    ) -> dict[str, Decimal]:
        """Calculate a position's contribution to portfolio risk.

        Args:
            position: The position to analyze
            portfolio_snapshot: Current portfolio state

        Returns:
            Dictionary with risk contribution metrics
        """
        if portfolio_snapshot.total_value == 0:
            return {
                "weight": Decimal(0),
                "marginal_var": Decimal(0),
                "component_var": Decimal(0),
                "risk_contribution": Decimal(0),
            }

        # Position weight
        weight = position.total_size_usd / portfolio_snapshot.total_value

        # Simplified risk contribution (would need covariance matrix for accurate calculation)
        volatility = getattr(position.opportunity, "volatility", Decimal("0.01"))

        # Marginal VaR (simplified)
        marginal_var = volatility * Decimal("2.33")  # 99% confidence

        # Component VaR
        component_var = weight * marginal_var

        # Risk contribution percentage
        total_risk = portfolio_snapshot.total_value * Decimal("0.05")  # Simplified
        risk_contribution = component_var / total_risk if total_risk > 0 else Decimal(0)

        return {
            "weight": weight,
            "marginal_var": marginal_var,
            "component_var": component_var,
            "risk_contribution": risk_contribution,
        }

    def set_risk_free_rate(self, rate: Decimal) -> None:
        """Set risk-free rate for calculations.

        Raises:
            RiskCalculationError: If risk-free rate is negative.
        """
        if rate < 0:
            raise RiskCalculationError(RiskCalculationError.NEGATIVE_RISK_FREE_RATE)

        self.risk_free_rate = rate
        self.logger.info("Set risk-free rate", rate=float(rate))

    def set_var_parameters(self, method: str, horizon_days: int) -> None:
        """Set VaR calculation parameters.

        Raises:
            RiskCalculationError: If VaR method is not supported.
        """
        if method not in {"historical", "parametric", "monte_carlo"}:
            raise RiskCalculationError(RiskCalculationError.INVALID_VAR_METHOD)

        self.var_method = method
        self.var_horizon_days = horizon_days
        self.logger.info(
            "Set VaR parameters",
            method=method,
            horizon_days=horizon_days,
        )

    def get_calculator_stats(self) -> dict[str, Any]:
        """Get calculator statistics.

        Returns:
            Dictionary containing current calculator configuration and settings.
        """
        return {
            "risk_free_rate": float(self.risk_free_rate),
            "confidence_levels": self.confidence_levels,
            "min_data_points": self.min_data_points,
            "var_method": self.var_method,
            "var_horizon_days": self.var_horizon_days,
            "benchmark_symbol": self.benchmark_symbol,
        }
