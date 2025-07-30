"""Metrics aggregation component for portfolio analytics."""
from __future__ import annotations

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Dict, List, Any, Optional

from cyberdelta.core.portfolio.portfolio_types.infrastructure import PortfolioEvent, EventType
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class MetricsAggregator:
    """Aggregator for various portfolio metrics."""
    
    def __init__(self) -> None:
        """Initialize metrics aggregator."""
        self._initialized = False
        self._metrics_buffer: Dict[str, List[Dict[str, Any]]] = {}
        self._aggregated_metrics: Dict[str, Dict[str, Any]] = {}
        self._aggregation_intervals = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
        }
        
    async def initialize(self) -> None:
        """Initialize the metrics aggregator."""
        if self._initialized:
            return
            
        logger.info("Initializing metrics aggregator")
        
        # Initialize metric categories
        self._initialize_metric_categories()
        
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the metrics aggregator."""
        if not self._initialized:
            return
            
        logger.info("Shutting down metrics aggregator")
        
        # Flush any pending metrics
        await self._flush_metrics()
        
        self._initialized = False
        
    async def update_position_metrics(self, event: PortfolioEvent) -> None:
        """Update position-related metrics.
        
        Args:
            event: Portfolio event containing position data
        """
        timestamp = datetime.now(UTC)
        metric_data = {
            "timestamp": timestamp,
            "event_type": event.event_type,
            "data": event.data
        }
        
        # Add to position metrics buffer
        if "positions" not in self._metrics_buffer:
            self._metrics_buffer["positions"] = []
            
        self._metrics_buffer["positions"].append(metric_data)
        
        # Trigger aggregation if needed
        await self._check_aggregation_trigger("positions")
        
    async def update_trade_metrics(self, event: PortfolioEvent) -> None:
        """Update trade-related metrics.
        
        Args:
            event: Portfolio event containing trade data
        """
        timestamp = datetime.now(UTC)
        metric_data = {
            "timestamp": timestamp,
            "event_type": event.event_type,
            "data": event.data
        }
        
        # Add to trade metrics buffer
        if "trades" not in self._metrics_buffer:
            self._metrics_buffer["trades"] = []
            
        self._metrics_buffer["trades"].append(metric_data)
        
        # Update trade counters
        await self._update_trade_counters(event)
        
        # Trigger aggregation if needed
        await self._check_aggregation_trigger("trades")
        
    async def update_balance_metrics(self, event: PortfolioEvent) -> None:
        """Update balance-related metrics.
        
        Args:
            event: Portfolio event containing balance data
        """
        timestamp = datetime.now(UTC)
        metric_data = {
            "timestamp": timestamp,
            "event_type": event.event_type,
            "data": event.data
        }
        
        # Add to balance metrics buffer
        if "balances" not in self._metrics_buffer:
            self._metrics_buffer["balances"] = []
            
        self._metrics_buffer["balances"].append(metric_data)
        
        # Trigger aggregation if needed
        await self._check_aggregation_trigger("balances")
        
    async def get_aggregated_metrics(
        self, 
        category: str,
        interval: str = "5m",
        lookback_periods: int = 12
    ) -> List[Dict[str, Any]]:
        """Get aggregated metrics for a category.
        
        Args:
            category: Metric category (positions, trades, balances)
            interval: Aggregation interval (1m, 5m, 15m, 1h, 1d)
            lookback_periods: Number of periods to look back
            
        Returns:
            List of aggregated metric data points
        """
        if category not in self._aggregated_metrics:
            return []
            
        category_metrics = self._aggregated_metrics[category]
        interval_metrics = category_metrics.get(interval, [])
        
        # Return most recent periods
        return interval_metrics[-lookback_periods:]
        
    async def get_summary_metrics(self) -> Dict[str, Any]:
        """Get summary of all metrics.
        
        Returns:
            Summary of key metrics across all categories
        """
        summary = {
            "timestamp": datetime.now(UTC),
            "categories": {}
        }
        
        for category in ["positions", "trades", "balances"]:
            if category in self._aggregated_metrics:
                # Get latest metrics for each interval
                category_summary = {}
                
                for interval in ["1m", "5m", "1h", "1d"]:
                    metrics = self._aggregated_metrics[category].get(interval, [])
                    if metrics:
                        category_summary[interval] = metrics[-1]
                        
                summary["categories"][category] = category_summary
                
        return summary
        
    def _initialize_metric_categories(self) -> None:
        """Initialize metric categories and structures."""
        categories = ["positions", "trades", "balances", "performance", "risk"]
        
        for category in categories:
            self._metrics_buffer[category] = []
            self._aggregated_metrics[category] = {
                interval: [] for interval in self._aggregation_intervals
            }
            
        # Initialize counters
        self._trade_counters = {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "total_volume": Decimal("0"),
            "total_fees": Decimal("0"),
        }
        
    async def _check_aggregation_trigger(self, category: str) -> None:
        """Check if aggregation should be triggered for a category."""
        if category not in self._metrics_buffer:
            return
            
        buffer = self._metrics_buffer[category]
        if not buffer:
            return
            
        # Check each interval
        now = datetime.now(UTC)
        
        for interval_name, interval_delta in self._aggregation_intervals.items():
            # Get last aggregation time for this interval
            last_agg = self._get_last_aggregation_time(category, interval_name)
            
            if now - last_agg >= interval_delta:
                # Perform aggregation
                await self._aggregate_metrics(category, interval_name, interval_delta)
                
    async def _aggregate_metrics(
        self, 
        category: str, 
        interval_name: str,
        interval_delta: timedelta
    ) -> None:
        """Aggregate metrics for a specific interval."""
        now = datetime.now(UTC)
        cutoff = now - interval_delta
        
        # Get metrics within interval
        buffer = self._metrics_buffer.get(category, [])
        interval_metrics = [
            m for m in buffer 
            if m["timestamp"] >= cutoff
        ]
        
        if not interval_metrics:
            return
            
        # Perform aggregation based on category
        if category == "positions":
            aggregated = await self._aggregate_position_metrics(interval_metrics)
        elif category == "trades":
            aggregated = await self._aggregate_trade_metrics(interval_metrics)
        elif category == "balances":
            aggregated = await self._aggregate_balance_metrics(interval_metrics)
        else:
            aggregated = {}
            
        # Add metadata
        aggregated["interval"] = interval_name
        aggregated["period_start"] = cutoff
        aggregated["period_end"] = now
        aggregated["data_points"] = len(interval_metrics)
        
        # Store aggregated metrics
        if category not in self._aggregated_metrics:
            self._aggregated_metrics[category] = {}
        if interval_name not in self._aggregated_metrics[category]:
            self._aggregated_metrics[category][interval_name] = []
            
        self._aggregated_metrics[category][interval_name].append(aggregated)
        
        # Trim old aggregated metrics
        self._trim_aggregated_metrics(category, interval_name)
        
    async def _aggregate_position_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate position metrics."""
        total_positions = 0
        total_value = Decimal("0")
        symbols = set()
        
        for metric in metrics:
            data = metric.get("data", {})
            if "position_count" in data:
                total_positions = max(total_positions, data["position_count"])
            if "total_value" in data:
                total_value = max(total_value, Decimal(str(data["total_value"])))
            if "symbol" in data:
                symbols.add(data["symbol"])
                
        return {
            "avg_position_count": total_positions,
            "max_total_value": str(total_value),
            "unique_symbols": len(symbols),
            "position_changes": len(metrics)
        }
        
    async def _aggregate_trade_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate trade metrics."""
        trade_count = 0
        total_volume = Decimal("0")
        total_pnl = Decimal("0")
        
        for metric in metrics:
            data = metric.get("data", {})
            trade_count += 1
            
            if "quantity" in data and "price" in data:
                volume = Decimal(str(data["quantity"])) * Decimal(str(data["price"]))
                total_volume += volume
                
            if "pnl" in data:
                total_pnl += Decimal(str(data["pnl"]))
                
        return {
            "trade_count": trade_count,
            "total_volume": str(total_volume),
            "total_pnl": str(total_pnl),
            "avg_trade_size": str(total_volume / trade_count) if trade_count > 0 else "0"
        }
        
    async def _aggregate_balance_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate balance metrics."""
        total_balance = Decimal("0")
        currencies = set()
        
        for metric in metrics:
            data = metric.get("data", {})
            if "total_quantity" in data:
                total_balance += Decimal(str(data["total_quantity"]))
            if "asset" in data:
                currencies.add(data["asset"])
                
        return {
            "total_balance": str(total_balance),
            "currency_count": len(currencies),
            "balance_updates": len(metrics)
        }
        
    async def _update_trade_counters(self, event: PortfolioEvent) -> None:
        """Update running trade counters."""
        if event.event_type != EventType.TRADE_EXECUTED:
            return
            
        data = event.data
        self._trade_counters["total_trades"] += 1
        
        # Update volume
        if "quantity" in data and "price" in data:
            volume = Decimal(str(data["quantity"])) * Decimal(str(data["price"]))
            self._trade_counters["total_volume"] += volume
            
        # Update P&L counters
        if "pnl" in data:
            pnl = Decimal(str(data["pnl"]))
            if pnl > 0:
                self._trade_counters["winning_trades"] += 1
            elif pnl < 0:
                self._trade_counters["losing_trades"] += 1
                
        # Update fees
        if "fees" in data:
            self._trade_counters["total_fees"] += Decimal(str(data["fees"]))
            
    def _get_last_aggregation_time(self, category: str, interval: str) -> datetime:
        """Get last aggregation time for category/interval."""
        if category not in self._aggregated_metrics:
            return datetime.min.replace(tzinfo=UTC)
            
        interval_metrics = self._aggregated_metrics[category].get(interval, [])
        if not interval_metrics:
            return datetime.min.replace(tzinfo=UTC)
            
        return interval_metrics[-1]["period_end"]
        
    def _trim_aggregated_metrics(self, category: str, interval: str) -> None:
        """Trim old aggregated metrics to prevent memory growth."""
        # Keep different amounts based on interval
        max_entries = {
            "1m": 60,    # 1 hour
            "5m": 288,   # 24 hours
            "15m": 96,   # 24 hours
            "1h": 168,   # 1 week
            "1d": 365,   # 1 year
        }
        
        max_count = max_entries.get(interval, 100)
        metrics = self._aggregated_metrics[category][interval]
        
        if len(metrics) > max_count:
            self._aggregated_metrics[category][interval] = metrics[-max_count:]
            
    async def _flush_metrics(self) -> None:
        """Flush any pending metrics."""
        # Aggregate all pending metrics
        for category in self._metrics_buffer:
            for interval_name, interval_delta in self._aggregation_intervals.items():
                await self._aggregate_metrics(category, interval_name, interval_delta)
                
        # Clear buffers
        for category in self._metrics_buffer:
            self._metrics_buffer[category].clear()