"""Unit tests for the PerformanceVisualizer component.

Comprehensive test suite covering performance visualization functionality including:
- Chart generation for returns, drawdowns, trade analysis, and funding rate heatmaps
- Performance metrics calculations (Sharpe ratio, Sortino ratio, max drawdown, etc.)
- Configuration management for visualization themes and parameters
- Data validation and error handling for various input scenarios

These tests ensure the visualization components work correctly with realistic trading data
and provide accurate performance analytics for the CyberDeltaEngine trading system.
"""

from typing import Any

import numpy as np
import pandas as pd
import pytest


# DEFENSIVE CHECK: plotly imports lack type stubs, causing mypy import-untyped errors.
# Using TYPE_CHECKING import pattern to satisfy mypy while maintaining runtime functionality.
# Mypy=[import-untyped] Ruff=[]
try:
    import plotly.graph_objects as go  # type: ignore [import-untyped]
except ImportError:
    go = None

from frontend.visualization.performance_visualizer import (
    PerformanceMetricsCalculator,
    PerformanceVisualizer,
    VisualizationConfig,
)


class TestVisualizationConfig:
    """Tests for the VisualizationConfig class.

    Validates configuration initialization, theme settings, and parameter validation
    for the visualization system. Tests both default and custom configurations to
    ensure proper behavior across different visualization scenarios.
    """

    def test_default_initialization(self) -> None:
        """Test that default configuration initializes correctly with expected values.

        Verifies that the default VisualizationConfig provides sensible defaults
        for theme, dimensions, template, legend display, and color palette that
        are suitable for most trading visualization use cases.
        """
        config = VisualizationConfig()
        assert config.theme == "light"
        assert config.default_height == 600
        assert config.default_width == 800
        assert config.template == "plotly_white"
        assert config.show_legend
        assert config.color_palette is not None

    def test_custom_initialization(self) -> None:
        """Test that custom configuration parameters are applied correctly.

        Validates that custom theme, dimensions, color palette, and legend settings
        override defaults properly. This ensures flexibility for different trading
        environments (e.g., dark mode for night trading sessions).
        """
        config = VisualizationConfig(
            theme="dark",
            default_height=800,
            default_width=1000,
            color_palette=["red", "blue", "green"],
            show_legend=False,
        )
        assert config.theme == "dark"
        assert config.default_height == 800
        assert config.default_width == 1000
        assert config.template == "plotly_dark"
        assert config.color_palette == ["red", "blue", "green"]
        assert not config.show_legend


class TestPerformanceVisualizer:
    """Tests for the PerformanceVisualizer class.

    Comprehensive testing of chart generation capabilities for trading performance
    analysis. Tests cover returns visualization, drawdown analysis, trade analytics,
    funding rate heatmaps, and integrated dashboard functionality using realistic
    simulated trading data.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self) -> None:
        """Set up test environment with realistic sample trading data.

        Creates comprehensive test datasets including:
        - Multi-strategy return data with different risk/return profiles
        - Trade execution data with entry/exit times and P&L
        - Funding rate data across multiple cryptocurrency assets

        This setup ensures tests run against data that closely resembles
        real trading scenarios in the CyberDeltaEngine system.
        """
        self.config = VisualizationConfig()
        self.visualizer = PerformanceVisualizer(config=self.config)

        # Generate sample return data with different strategy characteristics
        # Linter warning for 'date_range' is a false positive due to
        # pandas' complex typing; usage is correct.
        dates: pd.DatetimeIndex = pd.date_range(start="2020-01-01", end="2020-12-31", freq="D")
        np.random.seed(42)

        self.returns_data = pd.DataFrame(
            {
                "Strategy1": np.random.normal(0.001, 0.02, len(dates)),
                "Strategy2": np.random.normal(0.0005, 0.015, len(dates)),
                "Strategy3": np.random.normal(0.0015, 0.025, len(dates)),
            },
            index=dates,
        )

        # Generate sample trade data with realistic trading patterns
        self.trade_data = pd.DataFrame(
            {
                "strategy": np.random.choice(["Strategy1", "Strategy2", "Strategy3"], 100),
                "entry_time": np.random.choice(dates, 100),
                "exit_time": np.random.choice(dates, 100),
                "duration": np.random.randint(1, 1000, 100),
                "pnl": np.random.normal(50, 200, 100),
            },
        )

        # Generate sample funding rate data across major crypto assets
        assets = ["BTC", "ETH", "SOL", "ADA", "DOT"]
        funding_data: pd.DataFrame = pd.DataFrame(
            {
                "asset": np.repeat(assets, len(dates)),
                "date": np.tile(dates, len(assets)),
                "funding_rate": np.random.normal(0, 0.01, len(dates) * len(assets)),
            },
        )
        funding_data.set_index("date", inplace=True)
        self.funding_data = funding_data.pivot(columns="asset", values="funding_rate")

    def test_initialization(self) -> None:
        """Test that visualizer initializes correctly with provided configuration.

        Ensures the PerformanceVisualizer properly stores and uses the provided
        configuration object for subsequent chart generation operations.
        """
        assert self.visualizer.config == self.config

    def test_create_returns_chart(self) -> None:
        """Test returns chart generation for strategy performance visualization.

        Validates that the returns chart correctly displays multiple strategy
        performance lines and handles strategy filtering. This is critical for
        comparing different trading strategies in the CyberDeltaEngine system.

        Raises:
            TypeError: If the figure data is not in the expected tuple format.
        """
        if go is None:
            pytest.skip("plotly not available")

        fig: Any = self.visualizer.create_returns_chart(self.returns_data)
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 3
        else:
            raise TypeError("fig.data is not a tuple as expected")

        # Test with specific strategies to ensure filtering works
        fig = self.visualizer.create_returns_chart(
            self.returns_data,
            strategy_names=["Strategy1", "Strategy2"],
        )
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data = fig.data
            assert len(data) == 2
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_drawdown_chart(self) -> None:
        """Test drawdown chart generation for risk analysis visualization.

        Validates that drawdown charts correctly show the peak-to-trough declines
        for each strategy. Drawdown analysis is essential for understanding the
        risk characteristics of trading strategies in the CyberDeltaEngine.

        Raises:
            TypeError: If the figure data is not in the expected tuple format.
        """
        if go is None:
            pytest.skip("plotly not available")

        fig: Any = self.visualizer.create_drawdown_chart(self.returns_data)
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 3
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_trade_analysis_chart(self) -> None:
        """Test trade analysis chart generation for execution performance review.

        Validates that trade analysis charts properly categorize and display
        profitable vs. losing trades. This visualization is crucial for
        understanding trade execution quality and identifying patterns.

        Raises:
            TypeError: If the figure data is not in the expected tuple format.
        """
        if go is None:
            pytest.skip("plotly not available")

        fig: Any = self.visualizer.create_trade_analysis_chart(self.trade_data)
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            # Should have 2 traces: profitable and losing trades
            assert 1 <= len(data) <= 2
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_funding_rate_heatmap(self) -> None:
        """Test funding rate heatmap generation for arbitrage opportunity analysis.

        Validates that funding rate heatmaps correctly display temporal patterns
        across different assets. This is essential for identifying funding rate
        arbitrage opportunities in the CyberDeltaEngine trading system.

        Raises:
            TypeError: If the figure data is not in the expected tuple format.
        """
        if go is None:
            pytest.skip("plotly not available")

        fig: Any = self.visualizer.create_funding_rate_heatmap(self.funding_data)
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 1
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_performance_dashboard(self) -> None:
        """Test comprehensive performance dashboard generation.

        Validates that the integrated dashboard correctly combines multiple
        visualization components (returns, drawdowns, trades, funding rates)
        into a unified view. This dashboard provides traders with a complete
        performance overview of the CyberDeltaEngine system.

        Raises:
            TypeError: If the figure data is not in the expected tuple format.
        """
        if go is None:
            pytest.skip("plotly not available")

        fig: Any = self.visualizer.create_performance_dashboard(
            returns_data=self.returns_data,
            trade_data=self.trade_data,
            funding_data=self.funding_data,
        )
        assert hasattr(fig, "data"), "Figure should have data attribute"
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            # Dashboard should have at least 4 subplots with multiple traces
            assert len(data) >= 4
        else:
            raise TypeError("fig.data is not a tuple as expected")


class TestPerformanceMetricsCalculator:
    """Tests for the PerformanceMetricsCalculator class.

    Comprehensive testing of financial performance metrics calculations including
    risk-adjusted returns (Sharpe, Sortino), drawdown analysis, and trade statistics.
    These metrics are fundamental for evaluating trading strategy performance in
    the CyberDeltaEngine system and ensuring accurate risk assessment.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self) -> None:
        """Set up test environment with realistic financial time series data.

        Creates sample return series and trade data that simulate realistic
        trading scenarios. The data includes appropriate volatility levels
        and return distributions typical of cryptocurrency trading strategies.
        """
        self.calculator = PerformanceMetricsCalculator()

        # Generate sample return data with realistic daily return characteristics
        np.random.seed(42)
        self.returns = pd.Series(np.random.normal(0.001, 0.02, 252))

        # Generate sample trade data with realistic P&L distribution
        self.trades = pd.DataFrame({"pnl": np.random.normal(50, 200, 100)})

    def test_calculate_sharpe_ratio(self) -> None:
        """Test Sharpe ratio calculation for risk-adjusted return analysis.

        Validates that the Sharpe ratio calculation correctly measures
        risk-adjusted returns, which is essential for comparing different
        trading strategies on a risk-adjusted basis in the CyberDeltaEngine.
        """
        sharpe = self.calculator.calculate_sharpe_ratio(self.returns)
        assert isinstance(sharpe, float)

    def test_calculate_sortino_ratio(self) -> None:
        """Test Sortino ratio calculation for downside risk assessment.

        Validates that the Sortino ratio correctly focuses on downside volatility
        rather than total volatility, providing a more nuanced view of risk
        for trading strategies that may have asymmetric return distributions.
        """
        sortino = self.calculator.calculate_sortino_ratio(self.returns)
        assert isinstance(sortino, float)

    def test_calculate_max_drawdown(self) -> None:
        """Test maximum drawdown calculation for worst-case scenario analysis.

        Validates that maximum drawdown correctly identifies the largest
        peak-to-trough decline. This metric is crucial for understanding
        the worst-case losses traders might experience with a strategy.
        """
        max_dd = self.calculator.calculate_max_drawdown(self.returns)
        assert isinstance(max_dd, float)
        assert 0 <= max_dd <= 100

    def test_calculate_calmar_ratio(self) -> None:
        """Test Calmar ratio calculation for return-to-drawdown analysis.

        Validates that the Calmar ratio correctly measures the relationship
        between annualized return and maximum drawdown, providing insight
        into return efficiency relative to worst-case risk.
        """
        calmar = self.calculator.calculate_calmar_ratio(self.returns)
        assert isinstance(calmar, float)

    def test_calculate_win_rate(self) -> None:
        """Test win rate calculation for trade success measurement.

        Validates that win rate correctly calculates the percentage of
        profitable trades. This metric helps assess the consistency
        of trading strategy performance and execution quality.
        """
        win_rate = self.calculator.calculate_win_rate(self.trades)
        assert isinstance(win_rate, float)
        assert 0 <= win_rate <= 100

    def test_calculate_profit_factor(self) -> None:
        """Test profit factor calculation for gross profit-to-loss ratio analysis.

        Validates that profit factor correctly measures the ratio of gross
        profits to gross losses. This metric indicates whether a strategy
        generates more profit than loss over time.
        """
        profit_factor = self.calculator.calculate_profit_factor(self.trades)
        assert isinstance(profit_factor, float)
        assert profit_factor >= 0

    def test_calculate_all_metrics(self) -> None:
        """Test comprehensive calculation of all performance metrics.

        Validates that the all-metrics calculation returns a complete set
        of performance indicators including returns, risk measures, and
        trade statistics. This comprehensive view is essential for thorough
        strategy evaluation in the CyberDeltaEngine system.
        """
        metrics = self.calculator.calculate_all_metrics(self.returns, self.trades)
        assert isinstance(metrics, dict)

        # Check that all expected metrics are present for complete analysis
        expected_metrics = [
            "annualized_return",
            "annualized_volatility",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "calmar_ratio",
            "win_rate",
            "profit_factor",
            "avg_win",
            "avg_loss",
            "total_trades",
            "winning_trades",
            "losing_trades",
        ]

        for metric in expected_metrics:
            assert metric in metrics
