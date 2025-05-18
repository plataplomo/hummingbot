from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore
import pytest

from cyberdelta.visualization.performance_visualizer import (
    PerformanceMetricsCalculator,
    PerformanceVisualizer,
    VisualizationConfig,
)


class TestVisualizationConfig:
    """Tests for the VisualizationConfig class"""

    def test_default_initialization(self) -> None:
        """Test that default configuration initializes correctly"""
        config = VisualizationConfig()
        assert config.theme == "light"
        assert config.default_height == 600
        assert config.default_width == 800
        assert config.template == "plotly_white"
        assert config.show_legend
        assert config.color_palette is not None

    def test_custom_initialization(self) -> None:
        """Test that custom configuration parameters are applied correctly"""
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
    """Tests for the PerformanceVisualizer class"""

    @pytest.fixture(autouse=True)
    def setup_method(self) -> None:
        """Set up test environment with sample data"""
        self.config = VisualizationConfig()
        self.visualizer = PerformanceVisualizer(config=self.config)

        # Generate sample return data
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

        # Generate sample trade data
        self.trade_data = pd.DataFrame({
            "strategy": np.random.choice(["Strategy1", "Strategy2", "Strategy3"], 100),
            "entry_time": np.random.choice(dates, 100),
            "exit_time": np.random.choice(dates, 100),
            "duration": np.random.randint(1, 1000, 100),
            "pnl": np.random.normal(50, 200, 100),
        })

        # Generate sample funding rate data
        assets = ["BTC", "ETH", "SOL", "ADA", "DOT"]
        funding_data: pd.DataFrame = pd.DataFrame({
            "asset": np.repeat(assets, len(dates)),
            "date": np.tile(dates, len(assets)),
            "funding_rate": np.random.normal(0, 0.01, len(dates) * len(assets)),
        })
        funding_data.set_index("date", inplace=True)
        self.funding_data = funding_data.pivot(columns="asset", values="funding_rate")

    def test_initialization(self) -> None:
        """Test that visualizer initializes correctly"""
        assert self.visualizer.config == self.config

    def test_create_returns_chart(self) -> None:
        """Test returns chart generation"""
        fig: go.Figure = self.visualizer.create_returns_chart(self.returns_data)
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 3
        else:
            raise TypeError("fig.data is not a tuple as expected")

        # Test with specific strategies
        fig = self.visualizer.create_returns_chart(
            self.returns_data, strategy_names=["Strategy1", "Strategy2"]
        )
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data = fig.data
            assert len(data) == 2
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_drawdown_chart(self) -> None:
        """Test drawdown chart generation"""
        fig: go.Figure = self.visualizer.create_drawdown_chart(self.returns_data)
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 3
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_trade_analysis_chart(self) -> None:
        """Test trade analysis chart generation"""
        fig: go.Figure = self.visualizer.create_trade_analysis_chart(self.trade_data)
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            # Should have 2 traces: profitable and losing trades
            assert 1 <= len(data) <= 2
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_funding_rate_heatmap(self) -> None:
        """Test funding rate heatmap generation"""
        fig: go.Figure = self.visualizer.create_funding_rate_heatmap(self.funding_data)
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            assert len(data) == 1
        else:
            raise TypeError("fig.data is not a tuple as expected")

    def test_create_performance_dashboard(self) -> None:
        """Test performance dashboard generation"""
        fig: go.Figure = self.visualizer.create_performance_dashboard(
            returns_data=self.returns_data,
            trade_data=self.trade_data,
            funding_data=self.funding_data,
        )
        assert isinstance(fig, go.Figure)
        if isinstance(fig.data, tuple):
            data: tuple[Any, ...] = fig.data
            # Dashboard should have at least 4 subplots with multiple traces
            assert len(data) >= 4
        else:
            raise TypeError("fig.data is not a tuple as expected")


class TestPerformanceMetricsCalculator:
    """Tests for the PerformanceMetricsCalculator class"""

    @pytest.fixture(autouse=True)
    def setup_method(self) -> None:
        """Set up test environment with sample data"""
        self.calculator = PerformanceMetricsCalculator()

        # Generate sample return data
        np.random.seed(42)
        self.returns = pd.Series(np.random.normal(0.001, 0.02, 252))

        # Generate sample trade data
        self.trades = pd.DataFrame({"pnl": np.random.normal(50, 200, 100)})

    def test_calculate_sharpe_ratio(self) -> None:
        """Test Sharpe ratio calculation"""
        sharpe = self.calculator.calculate_sharpe_ratio(self.returns)
        assert isinstance(sharpe, float)

    def test_calculate_sortino_ratio(self) -> None:
        """Test Sortino ratio calculation"""
        sortino = self.calculator.calculate_sortino_ratio(self.returns)
        assert isinstance(sortino, float)

    def test_calculate_max_drawdown(self) -> None:
        """Test maximum drawdown calculation"""
        max_dd = self.calculator.calculate_max_drawdown(self.returns)
        assert isinstance(max_dd, float)
        assert 0 <= max_dd <= 100

    def test_calculate_calmar_ratio(self) -> None:
        """Test Calmar ratio calculation"""
        calmar = self.calculator.calculate_calmar_ratio(self.returns)
        assert isinstance(calmar, float)

    def test_calculate_win_rate(self) -> None:
        """Test win rate calculation"""
        win_rate = self.calculator.calculate_win_rate(self.trades)
        assert isinstance(win_rate, float)
        assert 0 <= win_rate <= 100

    def test_calculate_profit_factor(self) -> None:
        """Test profit factor calculation"""
        profit_factor = self.calculator.calculate_profit_factor(self.trades)
        assert isinstance(profit_factor, float)
        assert profit_factor >= 0

    def test_calculate_all_metrics(self) -> None:
        """Test calculation of all metrics"""
        metrics = self.calculator.calculate_all_metrics(self.returns, self.trades)
        assert isinstance(metrics, dict)

        # Check that all expected metrics are present
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
