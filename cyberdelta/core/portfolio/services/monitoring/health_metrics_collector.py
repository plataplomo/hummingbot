"""Focused health metrics collection service."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import psutil
from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance

logger = get_logger(__name__)


@dataclass
class SystemMetrics:
    """System resource metrics."""
    
    cpu_usage_percent: float = Field(ge=0.0, le=100.0)
    memory_usage_percent: float = Field(ge=0.0, le=100.0)
    memory_available_mb: float = Field(ge=0.0)
    disk_usage_percent: float = Field(ge=0.0, le=100.0)
    network_connections_count: int = Field(ge=0)
    process_count: int = Field(ge=0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


@dataclass
class PortfolioMetrics:
    """Portfolio-specific metrics."""
    
    total_positions: int = Field(ge=0)
    total_open_orders: int = Field(ge=0)
    total_capital: Decimal = Field(ge=Decimal(0))
    unrealized_pnl: Decimal = Field(default=Decimal(0))
    realized_pnl: Decimal = Field(default=Decimal(0))
    active_exchanges: int = Field(default=0, ge=0)
    last_trade_time: datetime | None = None
    last_balance_update: datetime | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


@dataclass
class ApplicationMetrics:
    """Application performance metrics."""
    
    event_queue_size: int = Field(ge=0)
    websocket_connections: int = Field(ge=0)
    api_calls_per_minute: int = Field(ge=0)
    average_response_time_ms: float = Field(ge=0.0)
    error_rate_percent: float = Field(ge=0.0, le=100.0)
    uptime_seconds: float = Field(ge=0.0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class HealthMetricsCollector(BasePortfolioService):
    """Collects various health metrics for portfolio monitoring."""

    def __init__(self, collection_interval: float = 30.0) -> None:
        """Initialize the health metrics collector.
        
        Args:
            collection_interval: Metrics collection interval in seconds.
        """
        super().__init__(name="HealthMetricsCollector")
        self.collection_interval = collection_interval
        self._collection_task: asyncio.Task[None] | None = None
        self._last_metrics_time = datetime.now(UTC)
        
    async def _initialize_service(self) -> None:
        """Initialize the metrics collection service."""
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info("Health metrics collector initialized")

    async def _shutdown_service(self) -> None:
        """Shutdown the metrics collection service."""
        if self._collection_task and not self._collection_task.done():
            self._collection_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._collection_task
        logger.info("Health metrics collector shutdown")

    async def collect_system_metrics(self) -> SystemMetrics:
        """Collect system resource metrics.
        
        Returns:
            SystemMetrics with current system resource usage.
        """
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_available_mb = memory.available / (1024 * 1024)
            
            # Disk usage
            disk = psutil.disk_usage("/")
            disk_percent = (disk.used / disk.total) * 100
            
            # Network connections
            connections = len(psutil.net_connections())
            
            # Process count
            process_count = len(psutil.pids())
            
            return SystemMetrics(
                cpu_usage_percent=cpu_percent,
                memory_usage_percent=memory_percent,
                memory_available_mb=memory_available_mb,
                disk_usage_percent=disk_percent,
                network_connections_count=connections,
                process_count=process_count
            )
            
        except Exception as e:
            logger.exception("Failed to collect system metrics", error=str(e))
            # Return default metrics on error
            return SystemMetrics(
                cpu_usage_percent=0.0,
                memory_usage_percent=0.0,
                memory_available_mb=0.0,
                disk_usage_percent=0.0,
                network_connections_count=0,
                process_count=0
            )

    async def collect_portfolio_metrics(
        self,
        positions: list[DerivativePosition],
        orders: list[Order],
        balances: list[SpotBalance]
    ) -> PortfolioMetrics:
        """Collect portfolio-specific metrics.
        
        Args:
            positions: List of derivative positions.
            orders: List of orders.
            balances: List of spot balances.
            
        Returns:
            PortfolioMetrics with current portfolio data.
        """
        try:
            # Count active positions
            total_positions = len([p for p in positions if p.size != Decimal(0)])
            
            # Count open orders
            total_open_orders = len([o for o in orders if o.status.value in {"open", "partial"}])
            
            # Calculate total capital from balances
            total_capital = sum(balance.total_quantity for balance in balances)
            
            # Calculate P&L
            unrealized_pnl = sum(p.unrealized_pnl or Decimal(0) for p in positions)
            realized_pnl = sum(p.realized_pnl or Decimal(0) for p in positions)
            
            # Count active exchanges
            active_exchanges = len({p.exchange for p in positions})
            
            # Find last trade and balance update times
            last_trade_time = None
            if positions:
                # Use timestamp field which is available on all positions
                position_times = [p.timestamp for p in positions if p.timestamp]
                if position_times:
                    last_trade_time = max(position_times)
            
            last_balance_update = None
            if balances:
                balance_times = [b.timestamp for b in balances if b.timestamp]
                if balance_times:
                    last_balance_update = max(balance_times)
            
            return PortfolioMetrics(
                total_positions=total_positions,
                total_open_orders=total_open_orders,
                total_capital=total_capital,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                active_exchanges=active_exchanges,
                last_trade_time=last_trade_time,
                last_balance_update=last_balance_update
            )
            
        except Exception as e:
            logger.exception("Failed to collect portfolio metrics", error=str(e))
            # Return default metrics on error
            return PortfolioMetrics(
                total_positions=0,
                total_open_orders=0,
                total_capital=Decimal(0),
                unrealized_pnl=Decimal(0),
                realized_pnl=Decimal(0),
                active_exchanges=0
            )

    async def collect_application_metrics(self) -> ApplicationMetrics:
        """Collect application performance metrics.
        
        Returns:
            ApplicationMetrics with current application performance data.
        """
        try:
            # These would typically come from application state
            # For now, providing placeholder implementation
            current_time = datetime.now(UTC)
            uptime_seconds = (current_time - self._last_metrics_time).total_seconds()
            
            return ApplicationMetrics(
                event_queue_size=0,  # Would come from event dispatcher
                websocket_connections=0,  # Would come from WS manager
                api_calls_per_minute=0,  # Would come from API rate limiter
                average_response_time_ms=0.0,  # Would come from request tracker
                error_rate_percent=0.0,  # Would come from error tracker
                uptime_seconds=uptime_seconds
            )
            
        except Exception as e:
            logger.exception("Failed to collect application metrics", error=str(e))
            return ApplicationMetrics(
                event_queue_size=0,
                websocket_connections=0,
                api_calls_per_minute=0,
                average_response_time_ms=0.0,
                error_rate_percent=0.0,
                uptime_seconds=0.0
            )

    async def collect_all_metrics(
        self,
        positions: list[DerivativePosition] | None = None,
        orders: list[Order] | None = None,
        balances: list[SpotBalance] | None = None
    ) -> dict[str, Any]:
        """Collect all available metrics.
        
        Args:
            positions: List of derivative positions (optional).
            orders: List of orders (optional).
            balances: List of spot balances (optional).
            
        Returns:
            Dictionary containing system, portfolio (if data provided), and application metrics.
        """
        metrics = {}
        
        # Collect system metrics
        metrics["system"] = await self.collect_system_metrics()
        
        # Collect portfolio metrics if data provided
        if positions is not None and orders is not None and balances is not None:
            metrics["portfolio"] = await self.collect_portfolio_metrics(positions, orders, balances)
        
        # Collect application metrics
        metrics["application"] = await self.collect_application_metrics()
        
        return metrics

    async def _collection_loop(self) -> None:
        """Background metrics collection loop."""
        while True:
            try:
                # Collect system metrics periodically
                system_metrics = await self.collect_system_metrics()
                app_metrics = await self.collect_application_metrics()
                
                logger.debug(
                    "Metrics collected",
                    cpu_usage=system_metrics.cpu_usage_percent,
                    memory_usage=system_metrics.memory_usage_percent,
                    uptime=app_metrics.uptime_seconds
                )
                
                await asyncio.sleep(self.collection_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in metrics collection loop", error=str(e))
                await asyncio.sleep(self.collection_interval)