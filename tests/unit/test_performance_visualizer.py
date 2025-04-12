import unittest

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from cyberdelta.visualization.performance_visualizer import (
    PerformanceMetricsCalculator,
    PerformanceVisualizer,
    VisualizationConfig,
)


class TestVisualizationConfig(unittest.TestCase):
    """Tests for the VisualizationConfig class"""

    def test_default_initialization(self):
        """Test that default configuration initializes correctly"""
        config = VisualizationConfig()
        self.assertEqual(config.theme, "light")
        self.assertEqual(config.default_height, 600)
        self.assertEqual(config.default_width, 800)
        self.assertEqual(config.template, "plotly_white")
        self.assertTrue(config.show_legend)
        self.assertIsNotNone(config.color_palette)

    def test_custom_initialization(self):
        """Test that custom configuration parameters are applied correctly"""
        config = VisualizationConfig(
            theme="dark",
            default_height=800,
            default_width=1000,
            color_palette=["red", "blue", "green"],
            show_legend=False,
        )
        self.assertEqual(config.theme, "dark")
        self.assertEqual(config.default_height, 800)
        self.assertEqual(config.default_width, 1000)
        self.assertEqual(config.template, "plotly_dark")  # Should be set based on theme
        self.assertEqual(config.color_palette, ["red", "blue", "green"])
        self.assertFalse(config.show_legend)


class TestPerformanceVisualizer(unittest.TestCase):
    """Tests for the PerformanceVisualizer class"""

    def setUp(self):
        """Set up test environment with sample data"""
        self.config = VisualizationConfig()
        self.visualizer = PerformanceVisualizer(config=self.config)

        # Generate sample return data
        dates = pd.date_range(start="2020-01-01", end="2020-12-31", freq="D")
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
        self.trade_data = pd.DataFrame(
            {
                "strategy": np.random.choice(["Strategy1", "Strategy2", "Strategy3"], 100),
                "entry_time": np.random.choice(dates, 100),
                "exit_time": np.random.choice(dates, 100),
                "duration": np.random.randint(1, 1000, 100),
                "pnl": np.random.normal(50, 200, 100),
            }
        )

        # Generate sample funding rate data
        assets = ["BTC", "ETH", "SOL", "ADA", "DOT"]
        funding_data = pd.DataFrame(
            {
                "asset": np.repeat(assets, len(dates)),
                "date": np.tile(dates, len(assets)),
                "funding_rate": np.random.normal(0, 0.01, len(dates) * len(assets)),
            }
        )
        funding_data.set_index("date", inplace=True)
        self.funding_data = funding_data.pivot(columns="asset", values="funding_rate")

    def test_initialization(self):
        """Test that visualizer initializes correctly"""
        self.assertEqual(self.visualizer.config, self.config)

    def test_create_returns_chart(self):
        """Test returns chart generation"""
        fig = self.visualizer.create_returns_chart(self.returns_data)
        self.assertIsInstance(fig, go.Figure)
        self.assertEqual(len(fig.data), 3)  # Should have 3 traces for 3 strategies

        # Test with specific strategies
        fig = self.visualizer.create_returns_chart(
            self.returns_data, strategy_names=["Strategy1", "Strategy2"]
        )
        self.assertEqual(len(fig.data), 2)  # Should have 2 traces

    def test_create_drawdown_chart(self):
        """Test drawdown chart generation"""
        fig = self.visualizer.create_drawdown_chart(self.returns_data)
        self.assertIsInstance(fig, go.Figure)
        self.assertEqual(len(fig.data), 3)  # Should have 3 traces for 3 strategies

    def test_create_trade_analysis_chart(self):
        """Test trade analysis chart generation"""
        fig = self.visualizer.create_trade_analysis_chart(self.trade_data)
        self.assertIsInstance(fig, go.Figure)

        # Should have 2 traces: profitable and losing trades
        self.assertTrue(1 <= len(fig.data) <= 2)  # Could be 1 if all trades are profitable/losing

    def test_create_funding_rate_heatmap(self):
        """Test funding rate heatmap generation"""
        fig = self.visualizer.create_funding_rate_heatmap(self.funding_data)
        self.assertIsInstance(fig, go.Figure)
        self.assertEqual(len(fig.data), 1)  # Should have 1 heatmap trace

    def test_create_performance_dashboard(self):
        """Test performance dashboard generation"""
        fig = self.visualizer.create_performance_dashboard(
            returns_data=self.returns_data,
            trade_data=self.trade_data,
            funding_data=self.funding_data,
        )
        self.assertIsInstance(fig, go.Figure)
        # Dashboard should have at least 4 subplots with multiple traces
        self.assertTrue(len(fig.data) >= 4)


class TestPerformanceMetricsCalculator(unittest.TestCase):
    """Tests for the PerformanceMetricsCalculator class"""

    def setUp(self):
        """Set up test environment with sample data"""
        self.calculator = PerformanceMetricsCalculator()

        # Generate sample return data
        np.random.seed(42)
        self.returns = pd.Series(np.random.normal(0.001, 0.02, 252))

        # Generate sample trade data
        self.trades = pd.DataFrame({"pnl": np.random.normal(50, 200, 100)})

    def test_calculate_sharpe_ratio(self):
        """Test Sharpe ratio calculation"""
        sharpe = self.calculator.calculate_sharpe_ratio(self.returns)
        self.assertIsInstance(sharpe, float)

    def test_calculate_sortino_ratio(self):
        """Test Sortino ratio calculation"""
        sortino = self.calculator.calculate_sortino_ratio(self.returns)
        self.assertIsInstance(sortino, float)

    def test_calculate_max_drawdown(self):
        """Test maximum drawdown calculation"""
        max_dd = self.calculator.calculate_max_drawdown(self.returns)
        self.assertIsInstance(max_dd, float)
        self.assertTrue(0 <= max_dd <= 100)  # Drawdown should be a percentage between 0-100

    def test_calculate_calmar_ratio(self):
        """Test Calmar ratio calculation"""
        calmar = self.calculator.calculate_calmar_ratio(self.returns)
        self.assertIsInstance(calmar, float)

    def test_calculate_win_rate(self):
        """Test win rate calculation"""
        win_rate = self.calculator.calculate_win_rate(self.trades)
        self.assertIsInstance(win_rate, float)
        self.assertTrue(0 <= win_rate <= 100)  # Win rate should be a percentage between 0-100

    def test_calculate_profit_factor(self):
        """Test profit factor calculation"""
        profit_factor = self.calculator.calculate_profit_factor(self.trades)
        self.assertIsInstance(profit_factor, float)
        self.assertTrue(profit_factor >= 0)  # Profit factor should be non-negative

    def test_calculate_all_metrics(self):
        """Test calculation of all metrics"""
        metrics = self.calculator.calculate_all_metrics(self.returns, self.trades)
        self.assertIsInstance(metrics, dict)

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
            self.assertIn(metric, metrics)


if __name__ == "__main__":
    unittest.main()
