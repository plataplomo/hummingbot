"""Unit tests for the performance metrics module.

Tests financial performance calculation functionality.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from __future__ import annotations

import warnings
from decimal import Decimal
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from cyberdelta.monitoring.performance_metrics import (
    EmptyMaxDrawdownError,
    EmptySharpeRatioError,
    EmptySortinoRatioError,
    PerformanceMetricsCalculator,
)


if TYPE_CHECKING:
    from pandas import Series


@pytest.fixture
def positive_returns() -> Series[float]:
    """Create sample positive returns series for testing."""
    return pd.Series([0.01, 0.02, 0.015, 0.025, 0.01], dtype=float)


@pytest.fixture
def mixed_returns() -> Series[float]:
    """Create sample mixed returns series for testing."""
    return pd.Series([0.02, -0.01, 0.015, -0.005, 0.01, -0.02, 0.03], dtype=float)


@pytest.fixture
def zero_returns() -> Series[float]:
    """Create sample zero returns series for testing."""
    return pd.Series([0.0, 0.0, 0.0, 0.0, 0.0], dtype=float)


@pytest.fixture
def sample_trades() -> pd.DataFrame:
    """Create sample trades dataframe for testing."""
    return pd.DataFrame({
        "pnl": [100.0, -50.0, 200.0, -25.0, 150.0, -75.0],
        "symbol": ["BTC", "ETH", "BTC", "ETH", "BTC", "ETH"],
        "timestamp": pd.date_range("2024-01-01", periods=6, freq="D"),
    })


@pytest.fixture
def all_winning_trades() -> pd.DataFrame:
    """Create sample trades with all winners for testing."""
    return pd.DataFrame({
        "pnl": [100.0, 50.0, 200.0, 25.0, 150.0],
        "symbol": ["BTC", "ETH", "BTC", "ETH", "BTC"],
    })


@pytest.fixture
def all_losing_trades() -> pd.DataFrame:
    """Create sample trades with all losers for testing."""
    return pd.DataFrame({
        "pnl": [-100.0, -50.0, -200.0, -25.0, -150.0],
        "symbol": ["BTC", "ETH", "BTC", "ETH", "BTC"],
    })


class TestCalculateSharpeRatio:
    """Test suite for calculate_sharpe_ratio method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_sharpe_ratio_success_positive_returns(
        self, positive_returns: Series[float]
    ) -> None:
        """Test successful Sharpe ratio calculation with positive returns."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(
            positive_returns, risk_free_rate=Decimal("0.02"), periods_per_year=252
        )

        # Assert
        assert isinstance(result, Decimal)
        assert result > 0  # Should be positive for profitable strategy
        # Verify calculation components exist
        assert len(positive_returns) == 5

    def test_calculate_sharpe_ratio_success_zero_risk_free_rate(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test Sharpe ratio calculation with zero risk-free rate."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(
            mixed_returns, risk_free_rate=Decimal("0.0"), periods_per_year=252
        )

        # Assert
        assert isinstance(result, Decimal)
        # With mixed returns, result could be positive or negative

    def test_calculate_sharpe_ratio_success_custom_periods(
        self, positive_returns: Series[float]
    ) -> None:
        """Test Sharpe ratio calculation with custom periods per year."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(
            positive_returns,
            risk_free_rate=Decimal("0.05"),
            periods_per_year=365,  # Daily compounding
        )

        # Assert
        assert isinstance(result, Decimal)
        assert result != 0

    # ==================== EDGE CASES ====================

    def test_calculate_sharpe_ratio_edge_zero_standard_deviation(
        self, zero_returns: Series[float]
    ) -> None:
        """Test Sharpe ratio calculation with zero standard deviation."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(zero_returns)

        # Assert
        assert result == Decimal("0.0")

    def test_calculate_sharpe_ratio_edge_single_return(self) -> None:
        """Test Sharpe ratio calculation with single return value."""
        # Arrange
        single_return = pd.Series([0.01], dtype=float)

        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(single_return)

        # Assert
        assert result == Decimal("0.0")  # Standard deviation is 0 for single value

    def test_calculate_sharpe_ratio_edge_high_risk_free_rate(
        self, positive_returns: Series[float]
    ) -> None:
        """Test Sharpe ratio with high risk-free rate."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sharpe_ratio(
            positive_returns,
            risk_free_rate=Decimal("0.50"),  # 50% annual risk-free rate
        )

        # Assert
        assert isinstance(result, Decimal)
        # Result might be negative if returns don't exceed high risk-free rate

    # ==================== FAILURE CASES ====================

    def test_calculate_sharpe_ratio_failure_empty_series(self) -> None:
        """Test Sharpe ratio calculation with empty returns series."""
        # Arrange
        empty_returns = pd.Series([], dtype=float)

        # Act & Assert
        with pytest.raises(EmptySharpeRatioError) as exc_info:
            PerformanceMetricsCalculator.calculate_sharpe_ratio(empty_returns)

        # Assert specific error message
        assert str(exc_info.value) == "Cannot calculate Sharpe ratio for empty returns series"


class TestCalculateSortinoRatio:
    """Test suite for calculate_sortino_ratio method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_sortino_ratio_success_mixed_returns(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test successful Sortino ratio calculation with mixed returns."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sortino_ratio(
            mixed_returns, risk_free_rate=Decimal("0.02"), periods_per_year=252
        )

        # Assert
        assert isinstance(result, Decimal)
        # Should handle both positive and negative returns

    def test_calculate_sortino_ratio_success_with_downside(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test Sortino ratio calculation focusing on downside deviation."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sortino_ratio(
            mixed_returns, risk_free_rate=Decimal("0.0")
        )

        # Assert
        assert isinstance(result, Decimal)
        assert result != 0

    # ==================== EDGE CASES ====================

    def test_calculate_sortino_ratio_edge_no_downside_returns(
        self, positive_returns: Series[float]
    ) -> None:
        """Test Sortino ratio calculation with no downside returns."""
        # Act
        result = PerformanceMetricsCalculator.calculate_sortino_ratio(positive_returns)

        # Assert
        assert result == Decimal("Infinity")

    def test_calculate_sortino_ratio_edge_zero_downside_deviation(self) -> None:
        """Test Sortino ratio with minimal downside variation."""
        # Arrange
        # Returns with very small negative returns (same magnitude)
        minimal_downside = pd.Series([0.02, -0.01, 0.015, -0.01, 0.01], dtype=float)

        # Act
        result = PerformanceMetricsCalculator.calculate_sortino_ratio(minimal_downside)

        # Assert
        assert isinstance(result, Decimal)

    # ==================== FAILURE CASES ====================

    def test_calculate_sortino_ratio_failure_empty_series(self) -> None:
        """Test Sortino ratio calculation with empty returns series."""
        # Arrange
        empty_returns = pd.Series([], dtype=float)

        # Act & Assert
        with pytest.raises(EmptySortinoRatioError) as exc_info:
            PerformanceMetricsCalculator.calculate_sortino_ratio(empty_returns)

        # Assert specific error message
        assert str(exc_info.value) == "Cannot calculate Sortino ratio for empty returns series"

    def test_calculate_sortino_ratio_failure_all_negative_mean(
        self, all_losing_trades: pd.DataFrame
    ) -> None:
        """Test Sortino ratio with consistently negative returns."""
        # Arrange
        negative_returns = pd.Series(
            all_losing_trades["pnl"] / 10000, dtype=float
        )  # Convert to returns
        # Act
        result = PerformanceMetricsCalculator.calculate_sortino_ratio(negative_returns)

        # Assert
        assert isinstance(result, Decimal)
        assert result < 0  # Should be negative for consistently losing strategy


class TestCalculateMaxDrawdown:
    """Test suite for calculate_max_drawdown method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_max_drawdown_success_with_losses(self, mixed_returns: Series[float]) -> None:
        """Test successful max drawdown calculation with losses."""
        # Act
        result = PerformanceMetricsCalculator.calculate_max_drawdown(mixed_returns)

        # Assert
        assert isinstance(result, Decimal)
        assert result <= 0  # Drawdown should be negative or zero

    def test_calculate_max_drawdown_success_positive_returns(
        self, positive_returns: Series[float]
    ) -> None:
        """Test max drawdown calculation with only positive returns."""
        # Act
        result = PerformanceMetricsCalculator.calculate_max_drawdown(positive_returns)

        # Assert
        assert isinstance(result, Decimal)
        assert result == 0  # No drawdown with only positive returns

    # ==================== EDGE CASES ====================

    def test_calculate_max_drawdown_edge_flat_returns(self, zero_returns: Series[float]) -> None:
        """Test max drawdown calculation with flat returns."""
        # Act
        result = PerformanceMetricsCalculator.calculate_max_drawdown(zero_returns)

        # Assert
        assert result == Decimal("0.0")

    def test_calculate_max_drawdown_edge_large_drawdown(self) -> None:
        """Test max drawdown with significant losses."""
        # Arrange
        large_loss_returns = pd.Series([0.1, -0.5, 0.2, -0.3, 0.1], dtype=float)

        # Act
        result = PerformanceMetricsCalculator.calculate_max_drawdown(large_loss_returns)

        # Assert
        assert isinstance(result, Decimal)
        assert result < -0.3  # Should capture the significant drawdown

    # ==================== FAILURE CASES ====================

    def test_calculate_max_drawdown_failure_empty_series(self) -> None:
        """Test max drawdown calculation with empty returns series."""
        # Arrange
        empty_returns = pd.Series([], dtype=float)

        # Act & Assert
        with pytest.raises(EmptyMaxDrawdownError) as exc_info:
            PerformanceMetricsCalculator.calculate_max_drawdown(empty_returns)

        # Assert specific error message
        assert str(exc_info.value) == "Cannot calculate max drawdown for empty returns series"


class TestCalculateCalmarRatio:
    """Test suite for calculate_calmar_ratio method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_calmar_ratio_success_with_drawdown(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test successful Calmar ratio calculation with drawdown."""
        # Act
        result = PerformanceMetricsCalculator.calculate_calmar_ratio(mixed_returns)

        # Assert
        assert isinstance(result, Decimal)

    def test_calculate_calmar_ratio_success_custom_periods(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test Calmar ratio with custom periods per year."""
        # Act
        result = PerformanceMetricsCalculator.calculate_calmar_ratio(
            mixed_returns, periods_per_year=365
        )

        # Assert
        assert isinstance(result, Decimal)

    # ==================== EDGE CASES ====================

    def test_calculate_calmar_ratio_edge_zero_drawdown(
        self, positive_returns: Series[float]
    ) -> None:
        """Test Calmar ratio calculation with zero max drawdown."""
        # Act
        result = PerformanceMetricsCalculator.calculate_calmar_ratio(positive_returns)

        # Assert
        assert result == Decimal("Infinity")

    def test_calculate_calmar_ratio_edge_negative_returns_zero_drawdown(
        self, zero_returns: Series[float]
    ) -> None:
        """Test Calmar ratio with zero returns and zero drawdown."""
        # Act
        result = PerformanceMetricsCalculator.calculate_calmar_ratio(zero_returns)

        # Assert
        assert result == Decimal("0.0")

    # ==================== FAILURE CASES ====================

    def test_calculate_calmar_ratio_failure_all_negative_with_drawdown(self) -> None:
        """Test Calmar ratio with negative returns and drawdown."""
        # Arrange
        negative_returns = pd.Series([-0.01, -0.02, -0.015, -0.025], dtype=float)

        # Act
        result = PerformanceMetricsCalculator.calculate_calmar_ratio(negative_returns)

        # Assert
        assert isinstance(result, Decimal)
        assert result < 0  # Negative ratio for negative returns

    def test_calculate_calmar_ratio_failure_empty_series(self) -> None:
        """Test Calmar ratio calculation with empty returns series."""
        # Arrange
        empty_returns = pd.Series([], dtype=float)

        # Act & Assert
        with pytest.raises(EmptyMaxDrawdownError) as exc_info:
            PerformanceMetricsCalculator.calculate_calmar_ratio(empty_returns)

        # Assert specific error message (raised by calculate_max_drawdown)
        assert str(exc_info.value) == "Cannot calculate max drawdown for empty returns series"


class TestCalculateWinRate:
    """Test suite for calculate_win_rate method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_win_rate_success_mixed_trades(self, sample_trades: pd.DataFrame) -> None:
        """Test successful win rate calculation with mixed trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(sample_trades)

        # Assert
        assert isinstance(result, Decimal)
        assert 0 <= result <= 100  # Win rate should be percentage
        # Sample has 3 winning trades out of 6 total = 50%
        assert result == Decimal("50.0")

    def test_calculate_win_rate_success_all_winners(self, all_winning_trades: pd.DataFrame) -> None:
        """Test win rate calculation with all winning trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(all_winning_trades)

        # Assert
        assert result == Decimal("100.0")

    def test_calculate_win_rate_success_all_losers(self, all_losing_trades: pd.DataFrame) -> None:
        """Test win rate calculation with all losing trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(all_losing_trades)

        # Assert
        assert result == Decimal("0.0")

    # ==================== EDGE CASES ====================

    def test_calculate_win_rate_edge_zero_pnl_trades(self) -> None:
        """Test win rate calculation with zero PnL trades."""
        # Arrange
        zero_pnl_trades = pd.DataFrame({
            "pnl": [0.0, 0.0, 0.0],
            "symbol": ["BTC", "ETH", "BTC"],
        })

        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(zero_pnl_trades)

        # Assert
        assert result == Decimal("0.0")  # Zero PnL trades are not winners

    def test_calculate_win_rate_edge_single_trade(self) -> None:
        """Test win rate calculation with single trade."""
        # Arrange
        single_trade = pd.DataFrame({"pnl": [100.0]})

        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(single_trade)

        # Assert
        assert result == Decimal("100.0")

    # ==================== FAILURE CASES ====================

    def test_calculate_win_rate_failure_empty_dataframe(self) -> None:
        """Test win rate calculation with empty trades dataframe."""
        # Arrange
        empty_trades = pd.DataFrame()

        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(empty_trades)

        # Assert
        assert str(result) == "NaN"

    def test_calculate_win_rate_failure_missing_pnl_column(self) -> None:
        """Test win rate calculation with missing PnL column."""
        # Arrange
        invalid_trades = pd.DataFrame({"symbol": ["BTC", "ETH"], "quantity": [1.0, 2.0]})

        # Act
        result = PerformanceMetricsCalculator.calculate_win_rate(invalid_trades)

        # Assert
        assert str(result) == "NaN"


class TestCalculateProfitFactor:
    """Test suite for calculate_profit_factor method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_profit_factor_success_mixed_trades(
        self, sample_trades: pd.DataFrame
    ) -> None:
        """Test successful profit factor calculation with mixed trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(sample_trades)

        # Assert
        assert isinstance(result, Decimal)
        assert result > 0
        # Sample trades: profits=450, losses=150, factor=3.0
        assert result == Decimal("3.0")

    def test_calculate_profit_factor_success_profitable_strategy(
        self, all_winning_trades: pd.DataFrame
    ) -> None:
        """Test profit factor calculation with all winning trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(all_winning_trades)

        # Assert
        assert result == Decimal("Infinity")

    # ==================== EDGE CASES ====================

    def test_calculate_profit_factor_edge_equal_profits_losses(self) -> None:
        """Test profit factor calculation with equal profits and losses."""
        # Arrange
        balanced_trades = pd.DataFrame({"pnl": [100.0, -100.0, 50.0, -50.0]})

        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(balanced_trades)

        # Assert
        assert result == Decimal("1.0")

    def test_calculate_profit_factor_edge_zero_pnl_trades(self) -> None:
        """Test profit factor calculation with zero PnL trades."""
        # Arrange
        zero_pnl_trades = pd.DataFrame({"pnl": [0.0, 0.0, 0.0]})

        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(zero_pnl_trades)

        # Assert
        assert str(result) == "NaN"

    # ==================== FAILURE CASES ====================

    def test_calculate_profit_factor_failure_all_losing_trades(
        self, all_losing_trades: pd.DataFrame
    ) -> None:
        """Test profit factor calculation with all losing trades."""
        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(all_losing_trades)

        # Assert
        assert str(result) == "NaN"

    def test_calculate_profit_factor_failure_empty_dataframe(self) -> None:
        """Test profit factor calculation with empty trades dataframe."""
        # Arrange
        empty_trades = pd.DataFrame()

        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(empty_trades)

        # Assert
        assert str(result) == "NaN"

    def test_calculate_profit_factor_failure_missing_pnl_column(self) -> None:
        """Test profit factor calculation with missing PnL column."""
        # Arrange
        invalid_trades = pd.DataFrame({"symbol": ["BTC"], "quantity": [1.0]})

        # Act
        result = PerformanceMetricsCalculator.calculate_profit_factor(invalid_trades)

        # Assert
        assert str(result) == "NaN"


class TestCalculateAllMetrics:
    """Test suite for calculate_all_metrics method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_all_metrics_success_with_trades(
        self, mixed_returns: Series[float], sample_trades: pd.DataFrame
    ) -> None:
        """Test successful calculation of all metrics with trades data."""
        # Arrange
        calculator = PerformanceMetricsCalculator()

        # Act
        result = calculator.calculate_all_metrics(
            mixed_returns, trades=sample_trades, risk_free_rate=Decimal("0.02")
        )

        # Assert
        assert isinstance(result, dict)
        expected_metrics = {
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "calmar_ratio",
            "win_rate",
            "profit_factor",
            "cumulative_return",
            "annualized_return",
            "annualized_volatility",
        }
        assert set(result.keys()) == expected_metrics

        # Check all values are Decimal
        for key, value in result.items():
            assert isinstance(value, Decimal), f"{key} should be Decimal, got {type(value)}"

    def test_calculate_all_metrics_success_without_trades(
        self, positive_returns: Series[float]
    ) -> None:
        """Test calculation of all metrics without trades data."""
        # Arrange
        calculator = PerformanceMetricsCalculator()

        # Act
        result = calculator.calculate_all_metrics(positive_returns)

        # Assert
        assert isinstance(result, dict)
        assert "win_rate" in result
        assert "profit_factor" in result
        assert result["win_rate"] == Decimal("0.0")
        assert result["profit_factor"] == Decimal("0.0")

    # ==================== EDGE CASES ====================

    def test_calculate_all_metrics_edge_zero_returns(self, zero_returns: Series[float]) -> None:
        """Test calculation of all metrics with zero returns."""
        # Arrange
        calculator = PerformanceMetricsCalculator()

        # Act
        result = calculator.calculate_all_metrics(zero_returns)

        # Assert
        assert isinstance(result, dict)
        assert result["sharpe_ratio"] == Decimal("0.0")
        assert result["cumulative_return"] == Decimal("0.0")
        assert result["annualized_return"] == Decimal("0.0")

    def test_calculate_all_metrics_edge_custom_parameters(
        self, mixed_returns: Series[float]
    ) -> None:
        """Test calculation with custom risk-free rate and periods."""
        # Arrange
        calculator = PerformanceMetricsCalculator()

        # Act
        result = calculator.calculate_all_metrics(
            mixed_returns, risk_free_rate=Decimal("0.05"), periods_per_year=365
        )

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 9  # Should have all metrics

    # ==================== FAILURE CASES ====================

    def test_calculate_all_metrics_failure_exception_handling(self) -> None:
        """Test calculation handles extreme values gracefully."""
        # Arrange
        calculator = PerformanceMetricsCalculator()
        extreme_returns = pd.Series([float("inf"), float("-inf")], dtype=float)

        # Act - suppress numpy warnings for this specific test case
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            result = calculator.calculate_all_metrics(extreme_returns)

        # Assert
        # Should still return metrics dict even with extreme values
        assert isinstance(result, dict)
        # Should contain expected metric keys
        assert "sharpe_ratio" in result
        assert "max_drawdown" in result
        assert "cumulative_return" in result


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("returns_data", "expected_drawdown_sign"),
    [
        ([0.01, 0.02, 0.01], 0),  # All positive - no drawdown
        ([0.01, -0.05, 0.02], -1),  # Mixed - negative drawdown
        ([-0.01, -0.02, -0.01], -1),  # All negative - negative drawdown
        ([0.0, 0.0, 0.0], 0),  # All zero - no drawdown
    ],
)
def test_max_drawdown_parametrized(returns_data: list[float], expected_drawdown_sign: int) -> None:
    """Test max drawdown calculation for various return patterns."""
    # Arrange
    returns = pd.Series(returns_data, dtype=float)

    # Act
    result = PerformanceMetricsCalculator.calculate_max_drawdown(returns)

    # Assert
    assert isinstance(result, Decimal)
    if expected_drawdown_sign == 0:
        assert result == 0
    elif expected_drawdown_sign == -1:
        assert result < 0
    else:
        assert result >= 0


@pytest.mark.parametrize(
    ("periods_per_year", "risk_free_rate"),
    [
        (252, Decimal("0.02")),  # Standard daily trading
        (365, Decimal("0.03")),  # Daily calendar
        (12, Decimal("0.01")),  # Monthly
        (4, Decimal("0.04")),  # Quarterly
        (1, Decimal("0.05")),  # Annual
    ],
)
def test_sharpe_ratio_different_periods_parametrized(
    positive_returns: Series[float], periods_per_year: int, risk_free_rate: Decimal
) -> None:
    """Test Sharpe ratio calculation with different time periods."""
    # Act
    result = PerformanceMetricsCalculator.calculate_sharpe_ratio(
        positive_returns, risk_free_rate, periods_per_year
    )

    # Assert
    assert isinstance(result, Decimal)
    assert result != 0  # Should have some value with positive returns


@pytest.mark.parametrize(
    ("pnl_values", "expected_win_rate"),
    [
        ([100, -50, 200], Decimal("66.666666666666666666666666667")),  # 2 of 3 winners
        ([100, 50, 200], Decimal("100.0")),  # All winners
        ([-100, -50, -200], Decimal("0.0")),  # All losers
        ([100], Decimal("100.0")),  # Single winner
        ([-100], Decimal("0.0")),  # Single loser
    ],
)
def test_win_rate_parametrized(pnl_values: list[float], expected_win_rate: Decimal) -> None:
    """Test win rate calculation for various PnL patterns."""
    # Arrange
    trades = pd.DataFrame({"pnl": pnl_values})

    # Act
    result = PerformanceMetricsCalculator.calculate_win_rate(trades)

    # Assert
    assert isinstance(result, Decimal)
    # Use approximate comparison for calculated percentages
    if expected_win_rate != Decimal("66.666666666666666666666666667"):
        assert result == expected_win_rate
    else:
        assert abs(result - expected_win_rate) < Decimal("0.1")
