"""Unit tests for market order metrics module.

Tests market order execution metrics tracking and analysis functionality
through public interface only, focusing on business logic verification.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.execution.orders.market_order_metrics import (
    MarketOrderExecutionMetric,
    MarketOrderMetrics,
)
from cyberdelta.enums import OrderSide


class TestMarketOrderExecutionMetric:
    """Test MarketOrderExecutionMetric data class and properties."""

    @pytest.fixture
    def filled_buy_metric(self) -> MarketOrderExecutionMetric:
        """Create a filled buy order metric for testing.
        
        Returns:
            MarketOrderExecutionMetric: A filled buy order metric for testing.
        """
        return MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("1.0"),
            filled_quantity=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=250.0,
        )

    def test_metric_initializes_with_all_required_fields(self) -> None:
        """Test that metric initializes correctly with all required fields."""
        # Arrange
        timestamp = datetime.now(UTC)

        # Act
        metric = MarketOrderExecutionMetric(
            timestamp=timestamp,
            symbol="ETH-PERP",
            side=OrderSide.SELL,
            requested_quantity=Decimal("2.5"),
            filled_quantity=Decimal("2.0"),
            expected_price=Decimal(3000),
            actual_price=Decimal(2990),
            expected_slippage=Decimal("0.002"),
            status=OrderStatus.PARTIALLY_FILLED,
            execution_time_ms=180.5,
        )

        # Assert
        assert metric.timestamp == timestamp
        assert metric.symbol == "ETH-PERP"
        assert metric.side == OrderSide.SELL
        assert metric.requested_quantity == Decimal("2.5")
        assert metric.filled_quantity == Decimal("2.0")
        assert metric.expected_price == Decimal(3000)
        assert metric.actual_price == Decimal(2990)
        assert metric.expected_slippage == Decimal("0.002")
        assert metric.status == OrderStatus.PARTIALLY_FILLED
        assert metric.execution_time_ms == 180.5

    def test_fill_rate_calculation_for_complete_fill(
        self, filled_buy_metric: MarketOrderExecutionMetric
    ) -> None:
        """Test fill rate calculation for completely filled order."""
        # Act
        fill_rate = filled_buy_metric.fill_rate

        # Assert
        assert fill_rate == Decimal("100.0")

    def test_fill_rate_calculation_for_partial_fill(self) -> None:
        """Test fill rate calculation for partially filled order."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("4.0"),
            filled_quantity=Decimal("3.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50050),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.PARTIALLY_FILLED,
            execution_time_ms=200.0,
        )

        # Act
        fill_rate = metric.fill_rate

        # Assert
        assert fill_rate == Decimal("75.0")

    def test_fill_rate_returns_zero_for_zero_requested_quantity(self) -> None:
        """Test fill rate returns zero when requested quantity is zero."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("0.0"),
            filled_quantity=Decimal("0.0"),
            expected_price=Decimal(50000),
            actual_price=None,
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.REJECTED,
            execution_time_ms=10.0,
        )

        # Act
        fill_rate = metric.fill_rate

        # Assert
        assert fill_rate == Decimal(0)

    def test_actual_slippage_calculation_for_buy_order(self) -> None:
        """Test actual slippage calculation for buy orders (paid more than expected)."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("1.0"),
            filled_quantity=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50200),  # Paid 200 more
            expected_slippage=Decimal("0.002"),
            status=OrderStatus.FILLED,
            execution_time_ms=300.0,
        )

        # Act
        actual_slippage = metric.actual_slippage

        # Assert - (50200 - 50000) / 50000 = 0.004
        assert actual_slippage == Decimal("0.004")

    def test_actual_slippage_calculation_for_sell_order(self) -> None:
        """Test actual slippage calculation for sell orders (received less than expected)."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="ETH-PERP",
            side=OrderSide.SELL,
            requested_quantity=Decimal("2.0"),
            filled_quantity=Decimal("2.0"),
            expected_price=Decimal(3000),
            actual_price=Decimal(2940),  # Received 60 less
            expected_slippage=Decimal("0.01"),
            status=OrderStatus.FILLED,
            execution_time_ms=220.0,
        )

        # Act
        actual_slippage = metric.actual_slippage

        # Assert - (3000 - 2940) / 3000 = 0.02
        assert actual_slippage == Decimal("0.02")

    def test_actual_slippage_returns_none_when_actual_price_none(self) -> None:
        """Test actual slippage returns None when actual price is None."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("1.0"),
            filled_quantity=Decimal("0.0"),
            expected_price=Decimal(50000),
            actual_price=None,
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.CANCELED,
            execution_time_ms=75.0,
        )

        # Act
        actual_slippage = metric.actual_slippage

        # Assert
        assert actual_slippage is None

    def test_price_improvement_calculation_with_better_execution(self) -> None:
        """Test price improvement calculation when execution was better than expected."""
        # Arrange - buy order that paid less than expected slippage
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("1.0"),
            filled_quantity=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50050),  # Only 50 more instead of expected 100
            expected_slippage=Decimal("0.002"),  # Expected 0.2%
            status=OrderStatus.FILLED,
            execution_time_ms=180.0,
        )

        # Act
        price_improvement = metric.price_improvement

        # Assert
        # Expected slippage: 0.002, Actual slippage: 0.001, Improvement: 0.001
        assert price_improvement == Decimal("0.001")

    def test_price_improvement_returns_none_when_actual_slippage_none(self) -> None:
        """Test price improvement returns None when actual slippage is None."""
        # Arrange
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_quantity=Decimal("1.0"),
            filled_quantity=Decimal("0.0"),
            expected_price=Decimal(50000),
            actual_price=None,
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.CANCELED,
            execution_time_ms=50.0,
        )

        # Act
        price_improvement = metric.price_improvement

        # Assert
        assert price_improvement is None


class TestMarketOrderMetrics:
    """Test MarketOrderMetrics aggregation and analysis class."""

    @pytest.fixture
    def metrics_tracker(self) -> MarketOrderMetrics:
        """Create a MarketOrderMetrics instance for testing.
        
        Returns:
            MarketOrderMetrics: A market order metrics tracker for testing.
        """
        return MarketOrderMetrics(max_history=100)

    def test_metrics_tracker_initializes_with_empty_state(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test that metrics tracker starts with empty state."""
        # Act
        recent_metrics = metrics_tracker.get_recent_metrics()

        # Assert
        assert len(recent_metrics) == 0

    def test_record_execution_creates_and_stores_metric(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test that recording execution creates and stores a metric."""
        # Act
        metrics_tracker.record_execution(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_qty=Decimal("1.0"),
            filled_qty=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=250.0,
        )

        # Assert
        recent_metrics = metrics_tracker.get_recent_metrics()
        assert len(recent_metrics) == 1

        metric = recent_metrics[0]
        assert metric.symbol == "BTC-PERP"
        assert metric.side == OrderSide.BUY
        assert metric.status == OrderStatus.FILLED

    def test_max_history_constraint_removes_old_metrics(self) -> None:
        """Test that metrics tracker respects max history constraint."""
        # Arrange
        metrics_tracker = MarketOrderMetrics(max_history=2)

        # Act - record 3 metrics
        for i in range(3):
            metrics_tracker.record_execution(
                symbol=f"BTC-PERP-{i}",
                side=OrderSide.BUY,
                requested_qty=Decimal("1.0"),
                filled_qty=Decimal("1.0"),
                expected_price=Decimal(50000),
                actual_price=Decimal(50100),
                expected_slippage=Decimal("0.001"),
                status=OrderStatus.FILLED,
                execution_time_ms=250.0,
            )

        # Assert
        recent_metrics = metrics_tracker.get_recent_metrics()
        assert len(recent_metrics) == 2  # Only last 2 kept
        assert recent_metrics[0].symbol == "BTC-PERP-1"  # Oldest of the kept
        assert recent_metrics[1].symbol == "BTC-PERP-2"  # Newest

    def test_get_symbol_stats_returns_statistics_for_symbol(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test get_symbol_stats returns statistics for a symbol."""
        # Arrange - record metrics for symbol
        metrics_tracker.record_execution(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_qty=Decimal("1.0"),
            filled_qty=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=200.0,
        )

        # Act
        stats = metrics_tracker.get_symbol_stats("BTC-PERP")

        # Assert
        assert "count" in stats
        assert "avg_fill_rate" in stats
        assert "avg_execution_time_ms" in stats
        assert stats["count"] == 1

    def test_get_overall_stats_aggregates_all_metrics(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test get_overall_stats aggregates statistics across all symbols."""
        # Arrange - record metrics for different symbols
        metrics_tracker.record_execution(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_qty=Decimal("1.0"),
            filled_qty=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=200.0,
        )
        metrics_tracker.record_execution(
            symbol="ETH-PERP",
            side=OrderSide.SELL,
            requested_qty=Decimal("2.0"),
            filled_qty=Decimal("2.0"),
            expected_price=Decimal(3000),
            actual_price=Decimal(2990),
            expected_slippage=Decimal("0.002"),
            status=OrderStatus.FILLED,
            execution_time_ms=250.0,
        )

        # Act
        stats = metrics_tracker.get_overall_stats()

        # Assert
        assert "total_count" in stats
        assert "avg_fill_rate" in stats
        assert "avg_execution_time_ms" in stats
        assert stats["total_count"] == 2

    def test_get_slippage_analysis_provides_slippage_statistics(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test get_slippage_analysis provides slippage statistics."""
        # Arrange - record metric with slippage
        metrics_tracker.record_execution(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_qty=Decimal("1.0"),
            filled_qty=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=200.0,
        )

        # Act
        analysis = metrics_tracker.get_slippage_analysis()

        # Assert
        assert "samples" in analysis
        assert "avg_actual_slippage" in analysis
        assert "avg_price_improvement" in analysis

    def test_clear_metrics_removes_all_stored_metrics(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test clear_metrics removes all stored metrics."""
        # Arrange
        metrics_tracker.record_execution(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            requested_qty=Decimal("1.0"),
            filled_qty=Decimal("1.0"),
            expected_price=Decimal(50000),
            actual_price=Decimal(50100),
            expected_slippage=Decimal("0.001"),
            status=OrderStatus.FILLED,
            execution_time_ms=250.0,
        )
        assert len(metrics_tracker.get_recent_metrics()) == 1

        # Act
        metrics_tracker.clear_metrics()

        # Assert
        assert len(metrics_tracker.get_recent_metrics()) == 0

    def test_get_recent_metrics_respects_count_parameter(
        self, metrics_tracker: MarketOrderMetrics
    ) -> None:
        """Test get_recent_metrics respects the count parameter."""
        # Arrange - record 5 metrics
        for i in range(5):
            metrics_tracker.record_execution(
                symbol=f"BTC-PERP-{i}",
                side=OrderSide.BUY,
                requested_qty=Decimal("1.0"),
                filled_qty=Decimal("1.0"),
                expected_price=Decimal(50000),
                actual_price=Decimal(50100),
                expected_slippage=Decimal("0.001"),
                status=OrderStatus.FILLED,
                execution_time_ms=200.0,
            )

        # Act
        recent_metrics = metrics_tracker.get_recent_metrics(count=3)

        # Assert
        assert len(recent_metrics) == 3
        # Should return the 3 most recent
        assert recent_metrics[0].symbol == "BTC-PERP-2"
        assert recent_metrics[1].symbol == "BTC-PERP-3"
        assert recent_metrics[2].symbol == "BTC-PERP-4"
