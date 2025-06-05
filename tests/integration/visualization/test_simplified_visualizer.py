"""Integration tests for SimplifiedVisualizer component.

Tests the simplified visualization system for generating performance charts,
trade analysis plots, and market data visualizations in real integration
scenarios with file I/O and data processing capabilities.
"""
import os
import shutil
import tempfile
from collections.abc import Generator
from datetime import datetime, timedelta
from decimal import Decimal

import matplotlib
import pytest

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.monitoring.simplified_performance_tracker import (
    SimplePerformanceTracker,
)
from cyberdelta.visualization.simplified_visualizer import SimpleVisualizer


def _populate_sample_data(tracker: SimplePerformanceTracker) -> None:
    """Populate tracker with sample data for testing."""
    now = datetime.now()
    base_time = now - timedelta(days=60)

    # Track some signals
    signal1 = TradeSignal(
        exchange="mock_exchange",
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
        exchange="mock_exchange",
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
    signal1_metrics = tracker.track_signal(signal1)
    signal2_metrics = tracker.track_signal(signal2)
    tracker.track_signal_execution(signal1_metrics.signal_id, True)
    tracker.track_signal_execution(signal2_metrics.signal_id, False)

    # Create sample trades data
    for i in range(30):
        trade_time = base_time + timedelta(days=i * 2)
        exit_time = trade_time + timedelta(days=1)

        pnl = 1000 + np.random.normal(0, 500) if i % 2 == 0 else -800 + np.random.normal(0, 300)

        trade_id = f"trade_{i + 1}"
        symbol = "BTC-USDT" if i % 3 != 0 else "ETH-USDT"
        direction = "LONG" if i % 2 == 0 else "SHORT"

        tracker.track_trade(trade_id, symbol, "Binance", direction, 1.0, 50000.0, trade_time)
        tracker.track_trade_exit(trade_id, 51000.0, exit_time, pnl)


@pytest.fixture
def visualizer_setup() -> Generator[tuple[SimpleVisualizer, SimplePerformanceTracker, str]]:
    """Set up test environment for SimpleVisualizer tests."""
    test_dir = tempfile.mkdtemp()
    tracker = SimplePerformanceTracker("TestStrategy", output_dir=test_dir)
    _populate_sample_data(tracker)
    visualizer = SimpleVisualizer(tracker, output_dir=test_dir)
    plt.ioff()  # Disable showing plots during tests

    yield visualizer, tracker, test_dir  # Provide fixture values

    # Teardown: Remove temporary directory and close plots
    shutil.rmtree(test_dir)
    plt.close("all")


def test_initialization(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test that visualizer initializes correctly."""
    visualizer, tracker, test_dir = visualizer_setup
    assert visualizer.tracker == tracker
    assert visualizer.output_dir == test_dir
    assert os.path.exists(test_dir)


def test_plot_cumulative_pnl(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test cumulative PnL plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_cumulative_pnl(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_cumulative_pnl.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_plot_drawdown(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test drawdown plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_drawdown(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_drawdown.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_plot_trade_distribution(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test trade distribution plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_trade_distribution(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_trade_distribution.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_plot_winning_vs_losing_trades(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test winning vs losing trades plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_winning_vs_losing_trades(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_win_loss_ratio.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_plot_monthly_performance(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test monthly performance plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_monthly_performance(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_monthly_performance.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_plot_performance_metrics(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test performance metrics plot generation."""
    visualizer, _, test_dir = visualizer_setup
    fig = visualizer.plot_performance_metrics(save=True, show=False)
    assert isinstance(fig, Figure)
    file_path = os.path.join(test_dir, "TestStrategy_performance_metrics.png")
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0


def test_generate_performance_report(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test generation of performance report with all plots."""
    visualizer, _, _ = visualizer_setup
    report_files = visualizer.generate_performance_report()

    expected_plots = [
        "cumulative_pnl",
        "drawdown",
        "trade_distribution",
        "win_loss_ratio",
        "monthly_performance",
        "performance_metrics",
    ]

    for plot_name in expected_plots:
        assert plot_name in report_files
        assert os.path.exists(report_files[plot_name])
        assert os.path.getsize(report_files[plot_name]) > 0


def test_empty_data_handling(
    visualizer_setup: tuple[SimpleVisualizer, SimplePerformanceTracker, str],
) -> None:
    """Test handling of empty data."""
    _, _, test_dir = visualizer_setup  # Use test_dir from main fixture for consistency
    # Create empty tracker
    empty_tracker = SimplePerformanceTracker("EmptyStrategy", output_dir=test_dir)
    empty_visualizer = SimpleVisualizer(empty_tracker, output_dir=test_dir)

    # Test each plot with empty data
    fig1 = empty_visualizer.plot_cumulative_pnl(save=True, show=False)
    fig2 = empty_visualizer.plot_drawdown(save=True, show=False)
    fig3 = empty_visualizer.plot_trade_distribution(save=True, show=False)
    fig4 = empty_visualizer.plot_winning_vs_losing_trades(save=True, show=False)
    fig5 = empty_visualizer.plot_monthly_performance(save=True, show=False)
    fig6 = empty_visualizer.plot_performance_metrics(save=True, show=False)

    # Check that figures were still created despite empty data
    assert isinstance(fig1, Figure)
    assert isinstance(fig2, Figure)
    assert isinstance(fig3, Figure)
    assert isinstance(fig4, Figure)
    assert isinstance(fig5, Figure)
    assert isinstance(fig6, Figure)
