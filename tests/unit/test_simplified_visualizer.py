import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from decimal import Decimal

import matplotlib.pyplot as plt
import numpy as np

from cyberdelta.monitoring.simplified_performance_tracker import (
    SimplePerformanceTracker,
)
from cyberdelta.visualization.simplified_visualizer import SimpleVisualizer
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal


class TestSimpleVisualizer(unittest.TestCase):
    def setUp(self) -> None:
        """Set up test environment before each test method."""
        # Create a temporary directory for output files
        self.test_dir = tempfile.mkdtemp()

        # Create a tracker with sample data
        self.tracker = SimplePerformanceTracker("TestStrategy", output_dir=self.test_dir)

        # Populate with sample data
        self.populate_sample_data()

        # Create visualizer
        self.visualizer = SimpleVisualizer(self.tracker, output_dir=self.test_dir)

        # Disable showing plots during tests
        plt.ioff()

    def tearDown(self) -> None:
        """Clean up after each test method."""
        # Remove temporary directory
        shutil.rmtree(self.test_dir)

        # Close all plots
        plt.close("all")

    def populate_sample_data(self) -> None:
        """Populate tracker with sample data for testing."""
        now = datetime.now()
        base_time = now - timedelta(days=60)

        # Track some signals
        signal1 = TradeSignal(
            symbol="BTC-USDT",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            timestamp=now,
            source_strategy="TestStrategy",
            confidence=0.9,
            metadata={},
        )
        signal2 = TradeSignal(
            symbol="ETH-USDT",
            signal_type=SignalType.ENTER_SHORT,
            side=OrderSide.SELL,
            price=Decimal("3000.0"),
            quantity=Decimal("10.0"),
            timestamp=now,
            source_strategy="TestStrategy",
            confidence=0.8,
            metadata={},
        )

        # Track signals and capture their generated IDs
        signal1_metrics = self.tracker.track_signal(signal1)
        signal2_metrics = self.tracker.track_signal(signal2)
        self.tracker.track_signal_execution(signal1_metrics.signal_id, True)
        self.tracker.track_signal_execution(signal2_metrics.signal_id, False)

        # Create sample trades data
        for i in range(30):
            trade_time = base_time + timedelta(days=i * 2)
            exit_time = trade_time + timedelta(days=1)

            # Alternate between winning and losing trades with some randomness
            pnl = 1000 + np.random.normal(0, 500) if i % 2 == 0 else -800 + np.random.normal(0, 300)

            # Track trade
            trade_id = f"trade_{i + 1}"
            symbol = "BTC-USDT" if i % 3 != 0 else "ETH-USDT"
            direction = "LONG" if i % 2 == 0 else "SHORT"

            self.tracker.track_trade(
                trade_id, symbol, "Binance", direction, 1.0, 50000.0, trade_time
            )
            self.tracker.track_trade_exit(trade_id, 51000.0, exit_time, pnl)

    def test_initialization(self) -> None:
        """Test that visualizer initializes correctly."""
        self.assertEqual(self.visualizer.tracker, self.tracker)
        self.assertEqual(self.visualizer.output_dir, self.test_dir)
        self.assertTrue(os.path.exists(self.test_dir))

    def test_plot_cumulative_pnl(self) -> None:
        """Test cumulative PnL plot generation."""
        # Test with show=False to avoid displaying during tests
        fig = self.visualizer.plot_cumulative_pnl(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_cumulative_pnl.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_plot_drawdown(self) -> None:
        """Test drawdown plot generation."""
        fig = self.visualizer.plot_drawdown(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_drawdown.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_plot_trade_distribution(self) -> None:
        """Test trade distribution plot generation."""
        fig = self.visualizer.plot_trade_distribution(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_trade_distribution.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_plot_winning_vs_losing_trades(self) -> None:
        """Test winning vs losing trades plot generation."""
        fig = self.visualizer.plot_winning_vs_losing_trades(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_win_loss_ratio.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_plot_monthly_performance(self) -> None:
        """Test monthly performance plot generation."""
        fig = self.visualizer.plot_monthly_performance(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_monthly_performance.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_plot_performance_metrics(self) -> None:
        """Test performance metrics plot generation."""
        fig = self.visualizer.plot_performance_metrics(save=True, show=False)

        # Check that figure was created
        self.assertIsInstance(fig, plt.Figure)

        # Check that file was saved
        file_path = os.path.join(self.test_dir, "TestStrategy_performance_metrics.png")
        self.assertTrue(os.path.exists(file_path))

        # Check file size to ensure it's not empty
        self.assertGreater(os.path.getsize(file_path), 0)

    def test_generate_performance_report(self) -> None:
        """Test generation of performance report with all plots."""
        report_files = self.visualizer.generate_performance_report()

        # Check that all expected plot files were created
        expected_plots = [
            "cumulative_pnl",
            "drawdown",
            "trade_distribution",
            "win_loss_ratio",
            "monthly_performance",
            "performance_metrics",
        ]

        for plot_name in expected_plots:
            self.assertIn(plot_name, report_files)
            self.assertTrue(os.path.exists(report_files[plot_name]))
            self.assertGreater(os.path.getsize(report_files[plot_name]), 0)

    def test_empty_data_handling(self) -> None:
        """Test handling of empty data."""
        # Create empty tracker
        empty_tracker = SimplePerformanceTracker("EmptyStrategy", output_dir=self.test_dir)
        empty_visualizer = SimpleVisualizer(empty_tracker, output_dir=self.test_dir)

        # Test each plot with empty data
        fig1 = empty_visualizer.plot_cumulative_pnl(save=True, show=False)
        fig2 = empty_visualizer.plot_drawdown(save=True, show=False)
        fig3 = empty_visualizer.plot_trade_distribution(save=True, show=False)
        fig4 = empty_visualizer.plot_winning_vs_losing_trades(save=True, show=False)
        fig5 = empty_visualizer.plot_monthly_performance(save=True, show=False)
        fig6 = empty_visualizer.plot_performance_metrics(save=True, show=False)

        # Check that figures were still created despite empty data
        self.assertIsInstance(fig1, plt.Figure)
        self.assertIsInstance(fig2, plt.Figure)
        self.assertIsInstance(fig3, plt.Figure)
        self.assertIsInstance(fig4, plt.Figure)
        self.assertIsInstance(fig5, plt.Figure)
        self.assertIsInstance(fig6, plt.Figure)


if __name__ == "__main__":
    unittest.main()
