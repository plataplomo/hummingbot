"""Market Order Metrics for monitoring and observability.

This module provides metrics tracking for market order execution,
including fill rates, slippage analysis, and performance monitoring.
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import Symbol
from cyberdelta.enums import OrderSide
from cyberdelta.models import OrderStatus


logger = get_logger(__name__)


class MarketOrderExecutionMetric:
    """Individual market order execution metric."""

    def __init__(
        self,
        timestamp: datetime,
        symbol: Symbol,
        side: OrderSide,
        requested_quantity: Decimal,
        filled_quantity: Decimal,
        expected_price: Decimal,
        actual_price: Decimal | None,
        expected_slippage: Decimal,
        status: OrderStatus,
        execution_time_ms: float,
    ) -> None:
        """Initialize execution metric.

        Args:
            timestamp: Execution timestamp
            symbol: Trading Symbol object
            side: Order side
            requested_quantity: Requested quantity
            filled_quantity: Actually filled quantity
            expected_price: Expected execution price
            actual_price: Actual execution price (None if not filled)
            expected_slippage: Expected slippage percentage
            status: Final order status
            execution_time_ms: Execution time in milliseconds
        """
        self.timestamp = timestamp
        self.symbol = symbol
        self.side = side
        self.requested_quantity = requested_quantity
        self.filled_quantity = filled_quantity
        self.expected_price = expected_price
        self.actual_price = actual_price
        self.expected_slippage = expected_slippage
        self.status = status
        self.execution_time_ms = execution_time_ms

    @property
    def fill_rate(self) -> Decimal:
        """Calculate fill rate as percentage."""
        if self.requested_quantity == Decimal(0):
            return Decimal(0)
        return (self.filled_quantity / self.requested_quantity) * Decimal(100)

    @property
    def actual_slippage(self) -> Decimal | None:
        """Calculate actual slippage if price available."""
        if self.actual_price is None or self.expected_price == Decimal(0):
            return None

        if self.side == OrderSide.BUY:
            # For buys, positive slippage means we paid more
            return (self.actual_price - self.expected_price) / self.expected_price
        # For sells, positive slippage means we received less
        return (self.expected_price - self.actual_price) / self.expected_price

    @property
    def price_improvement(self) -> Decimal | None:
        """Calculate price improvement (negative slippage is good)."""
        actual_slip = self.actual_slippage
        if actual_slip is None:
            return None
        return self.expected_slippage - actual_slip


class MarketOrderMetrics:
    """Track market order execution metrics for monitoring and analysis."""

    def __init__(self, max_history: int = 1000) -> None:
        """Initialize metrics tracker.

        Args:
            max_history: Maximum number of metrics to keep in memory
        """
        self._metrics: list[MarketOrderExecutionMetric] = []
        self._max_history = max_history

    def record_execution(
        self,
        symbol: Symbol,
        side: OrderSide,
        requested_qty: Decimal,
        filled_qty: Decimal,
        expected_price: Decimal,
        actual_price: Decimal | None,
        expected_slippage: Decimal,
        status: OrderStatus,
        execution_time_ms: float,
    ) -> None:
        """Record execution metrics for analysis.

        Args:
            symbol: Trading symbol
            side: Order side
            requested_qty: Requested quantity
            filled_qty: Filled quantity
            expected_price: Expected execution price
            actual_price: Actual execution price
            expected_slippage: Expected slippage
            status: Order status
            execution_time_ms: Execution time in milliseconds
        """
        metric = MarketOrderExecutionMetric(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            side=side,
            requested_quantity=requested_qty,
            filled_quantity=filled_qty,
            expected_price=expected_price,
            actual_price=actual_price,
            expected_slippage=expected_slippage,
            status=status,
            execution_time_ms=execution_time_ms,
        )

        self._metrics.append(metric)

        # Maintain max history
        if len(self._metrics) > self._max_history:
            self._metrics = self._metrics[-self._max_history :]

        # Log for monitoring
        self._log_metric(metric)

    def _log_metric(self, metric: MarketOrderExecutionMetric) -> None:
        """Log metric for monitoring systems."""
        actual_slippage = metric.actual_slippage
        price_improvement = metric.price_improvement

        logger.info(
            "market_order_execution",
            extra={
                "symbol": metric.symbol.value,
                "side": metric.side.value,
                "status": metric.status.value,
                "fill_rate": float(metric.fill_rate),
                "expected_slippage": float(metric.expected_slippage),
                "actual_slippage": float(actual_slippage) if actual_slippage else None,
                "price_improvement": float(price_improvement) if price_improvement else None,
                "execution_time_ms": metric.execution_time_ms,
                "requested_qty": float(metric.requested_quantity),
                "filled_qty": float(metric.filled_quantity),
            },
        )

    def get_symbol_stats(self, symbol: Symbol) -> dict[str, float]:
        """Get execution statistics for a specific symbol.

        Args:
            symbol: Trading Symbol object

        Returns:
            Dict with average fill rate, slippage, etc.
        """
        symbol_metrics = [m for m in self._metrics if m.symbol == symbol]

        if not symbol_metrics:
            return {
                "count": 0,
                "avg_fill_rate": 0.0,
                "avg_actual_slippage": 0.0,
                "avg_execution_time_ms": 0.0,
                "success_rate": 0.0,
            }

        fill_rates = [float(m.fill_rate) for m in symbol_metrics]
        execution_times = [m.execution_time_ms for m in symbol_metrics]
        success_count = sum(1 for m in symbol_metrics if m.status == OrderStatus.FILLED)

        # Calculate actual slippages (excluding None values)
        actual_slippages = [
            float(m.actual_slippage) for m in symbol_metrics if m.actual_slippage is not None
        ]

        return {
            "count": len(symbol_metrics),
            "avg_fill_rate": sum(fill_rates) / len(fill_rates) if fill_rates else 0.0,
            "avg_actual_slippage": sum(actual_slippages) / len(actual_slippages)
            if actual_slippages
            else 0.0,
            "avg_execution_time_ms": sum(execution_times) / len(execution_times)
            if execution_times
            else 0.0,
            "success_rate": (success_count / len(symbol_metrics) * 100) if symbol_metrics else 0.0,
        }

    def get_overall_stats(self) -> dict[str, float]:
        """Get overall execution statistics.

        Returns:
            Dict with overall metrics
        """
        if not self._metrics:
            return {
                "total_count": 0,
                "avg_fill_rate": 0.0,
                "avg_actual_slippage": 0.0,
                "avg_execution_time_ms": 0.0,
                "success_rate": 0.0,
                "partial_fill_rate": 0.0,
                "cancel_rate": 0.0,
            }

        fill_rates = [float(m.fill_rate) for m in self._metrics]
        execution_times = [m.execution_time_ms for m in self._metrics]

        # Count by status
        filled_count = sum(1 for m in self._metrics if m.status == OrderStatus.FILLED)
        partial_count = sum(1 for m in self._metrics if m.status == OrderStatus.PARTIALLY_FILLED)
        cancelled_count = sum(1 for m in self._metrics if m.status == OrderStatus.CANCELED)

        # Calculate actual slippages
        actual_slippages = [
            float(m.actual_slippage) for m in self._metrics if m.actual_slippage is not None
        ]

        total = len(self._metrics)
        return {
            "total_count": total,
            "avg_fill_rate": sum(fill_rates) / len(fill_rates) if fill_rates else 0.0,
            "avg_actual_slippage": sum(actual_slippages) / len(actual_slippages)
            if actual_slippages
            else 0.0,
            "avg_execution_time_ms": sum(execution_times) / len(execution_times)
            if execution_times
            else 0.0,
            "success_rate": (filled_count / total * 100) if total else 0.0,
            "partial_fill_rate": (partial_count / total * 100) if total else 0.0,
            "cancel_rate": (cancelled_count / total * 100) if total else 0.0,
        }

    def get_slippage_analysis(self) -> dict[str, float]:
        """Analyze slippage performance.

        Returns:
            Dict with slippage analysis metrics
        """
        metrics_with_slippage = [m for m in self._metrics if m.actual_slippage is not None]

        if not metrics_with_slippage:
            return {
                "avg_expected_slippage": 0.0,
                "avg_actual_slippage": 0.0,
                "avg_price_improvement": 0.0,
                "positive_slippage_rate": 0.0,
                "samples": 0,
            }

        expected_slippages = [float(m.expected_slippage) for m in metrics_with_slippage]
        actual_slippages = [
            float(m.actual_slippage) for m in metrics_with_slippage if m.actual_slippage is not None
        ]
        price_improvements = [
            float(m.price_improvement)
            for m in metrics_with_slippage
            if m.price_improvement is not None
        ]

        positive_slippage_count = sum(1 for s in actual_slippages if s > 0)

        return {
            "avg_expected_slippage": sum(expected_slippages) / len(expected_slippages),
            "avg_actual_slippage": sum(actual_slippages) / len(actual_slippages),
            "avg_price_improvement": sum(price_improvements) / len(price_improvements)
            if price_improvements
            else 0.0,
            "positive_slippage_rate": (positive_slippage_count / len(actual_slippages) * 100),
            "samples": len(metrics_with_slippage),
        }

    def clear_metrics(self) -> None:
        """Clear all stored metrics."""
        self._metrics.clear()
        logger.info("Cleared all market order metrics")

    def get_recent_metrics(self, count: int = 100) -> list[MarketOrderExecutionMetric]:
        """Get most recent execution metrics.

        Args:
            count: Number of recent metrics to return

        Returns:
            List of recent metrics
        """
        return self._metrics[-count:] if self._metrics else []
