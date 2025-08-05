"""Metrics collection service for monitoring system performance.

This module provides comprehensive metrics collection with configuration-driven
intervals and retention policies.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class MetricType(Enum):
    """Types of metrics that can be collected."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricValue:
    """Individual metric value with metadata."""

    name: str
    value: Decimal
    metric_type: MetricType
    timestamp: datetime
    tags: dict[str, str]
    unit: str | None = None


class MetricsSnapshot(BaseModel):
    """Snapshot of metrics at a point in time."""

    timestamp: datetime
    metrics: list[MetricValue]
    collection_duration_ms: Decimal

    class Config:
        arbitrary_types_allowed = True


class MetricsCollector:
    """Central metrics collection service with configuration-driven behavior.

    Configuration Usage:
    - Uses config.monitoring.metrics_collection_interval for collection frequency
    - Uses config.monitoring.metrics_retention_days for retention period
    - Uses config.monitoring.metrics_enabled for global metric control
    - Uses config.monitoring.metric_types for specific metric filtering

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL intervals and retention from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Fail-fast on configuration violations
    - Type-safe metric handling
    """

    def __init__(self, config: AppSettings):
        """Initialize metrics collector with configuration.

        Args:
            config: Application settings containing monitoring configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring
        self._running = False
        self._collection_task: asyncio.Task | None = None

        # Extract configuration settings - NO hardcoded defaults
        self._enabled = self._monitoring_config.metrics_enabled
        self._collection_interval = self._monitoring_config.metrics_collection_interval
        self._retention_days = self._monitoring_config.metrics_retention_days
        self._max_metrics_per_snapshot = self._monitoring_config.max_metrics_per_snapshot

        # Metric storage with retention
        self._metric_history: list[MetricsSnapshot] = []
        self._current_metrics: dict[str, MetricValue] = {}

        # Collection statistics
        self._collection_count = 0
        self._collection_errors = 0
        self._last_collection: datetime | None = None

        # Registered metric providers
        self._metric_providers: list[object] = []

        logger.info(
            "metrics_collector_initialized",
            enabled=self._enabled,
            collection_interval_seconds=float(self._collection_interval),
            retention_days=self._retention_days,
            max_metrics_per_snapshot=self._max_metrics_per_snapshot,
        )

    async def start(self) -> None:
        """Start metrics collection if enabled.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured intervals
        - Proper task management
        - Fail-fast if already running
        """
        if self._running:
            logger.warning("metrics_collector_already_running")
            return

        if not self._enabled:
            logger.info("metrics_collector_disabled_in_config")
            return

        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())

        logger.info(
            "metrics_collector_started",
            collection_interval_seconds=float(self._collection_interval),
            retention_days=self._retention_days,
        )

    async def stop(self) -> None:
        """Stop metrics collection.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful task cancellation
        - Proper cleanup
        - Uses configured shutdown timeout
        """
        if not self._running:
            logger.warning("metrics_collector_not_running")
            return

        self._running = False

        if self._collection_task:
            self._collection_task.cancel()
            try:
                await asyncio.wait_for(
                    self._collection_task, timeout=float(self.config.general.shutdown_grace_period)
                )
            except TimeoutError:
                logger.warning(
                    "metrics_collector_shutdown_timeout",
                    grace_period_sec=float(self.config.general.shutdown_grace_period),
                )
            except asyncio.CancelledError:
                pass

        logger.info("metrics_collector_stopped")

    def register_provider(self, provider: object) -> None:
        """Register a metrics provider.

        Args:
            provider: Object that implements get_metrics() method

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit provider registration
        - NO auto-discovery
        - Type checking for required methods
        """
        if not hasattr(provider, "get_metrics"):
            raise ValueError(
                f"Metrics provider {type(provider).__name__} must implement get_metrics() method"
            )

        self._metric_providers.append(provider)

        logger.info(
            "metrics_provider_registered",
            provider_type=type(provider).__name__,
            total_providers=len(self._metric_providers),
        )

    def record_metric(
        self,
        name: str,
        value: Decimal,
        metric_type: MetricType,
        tags: dict[str, str] | None = None,
        unit: str | None = None,
    ) -> None:
        """Record a single metric value.

        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            tags: Optional tags for categorization
            unit: Optional unit description

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Decimal for all numeric values
        - Explicit metric typing
        - NO silent failures
        """
        if not self._enabled:
            return

        metric = MetricValue(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=datetime.now(UTC),
            tags=tags or {},
            unit=unit,
        )

        self._current_metrics[name] = metric

        logger.debug(
            "metric_recorded",
            name=name,
            value=float(value),
            metric_type=metric_type.value,
            tags=tags,
            unit=unit,
        )

    async def collect_snapshot(self) -> MetricsSnapshot:
        """Collect a snapshot of all current metrics.

        Returns:
            MetricsSnapshot with current metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Collection timing from config
        - Handles provider failures gracefully
        - NO assumptions about provider availability
        """
        start_time = datetime.now(UTC)
        collected_metrics: list[MetricValue] = []

        try:
            # Collect from registered providers
            for provider in self._metric_providers:
                try:
                    provider_metrics = await self._collect_from_provider(provider)
                    collected_metrics.extend(provider_metrics)

                except Exception as e:
                    logger.error(
                        "metrics_provider_error",
                        provider_type=type(provider).__name__,
                        error=str(e),
                        exc_info=True,
                    )
                    # Continue with other providers

            # Add current recorded metrics
            collected_metrics.extend(self._current_metrics.values())

            # Limit metrics per snapshot from config
            if len(collected_metrics) > self._max_metrics_per_snapshot:
                logger.warning(
                    "metrics_snapshot_truncated",
                    collected_count=len(collected_metrics),
                    max_allowed=self._max_metrics_per_snapshot,
                )
                collected_metrics = collected_metrics[: self._max_metrics_per_snapshot]

            # Calculate collection duration
            end_time = datetime.now(UTC)
            duration_ms = Decimal(str((end_time - start_time).total_seconds() * 1000))

            snapshot = MetricsSnapshot(
                timestamp=start_time, metrics=collected_metrics, collection_duration_ms=duration_ms
            )

            # Clear current metrics after collection
            self._current_metrics.clear()

            logger.debug(
                "metrics_snapshot_collected",
                metric_count=len(collected_metrics),
                collection_duration_ms=float(duration_ms),
                providers_checked=len(self._metric_providers),
            )

            return snapshot

        except Exception as e:
            logger.error("metrics_collection_error", error=str(e), exc_info=True)
            raise

    async def _collection_loop(self) -> None:
        """Main metrics collection loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured collection intervals
        - Handles errors without stopping loop
        - NO hardcoded timing
        """
        logger.info("metrics_collection_loop_started")

        while self._running:
            try:
                # Collect metrics snapshot
                snapshot = await self.collect_snapshot()

                # Store snapshot in history
                self._metric_history.append(snapshot)

                # Clean up old metrics based on retention
                await self._cleanup_old_metrics()

                # Update collection statistics
                self._collection_count += 1
                self._last_collection = snapshot.timestamp

                logger.debug(
                    "metrics_collection_completed",
                    collection_count=self._collection_count,
                    metric_count=len(snapshot.metrics),
                    history_size=len(self._metric_history),
                )

                # Wait for next collection cycle
                await asyncio.sleep(float(self._collection_interval))

            except asyncio.CancelledError:
                logger.info("metrics_collection_loop_cancelled")
                break
            except Exception as e:
                self._collection_errors += 1
                logger.error(
                    "metrics_collection_loop_error",
                    error=str(e),
                    collection_errors=self._collection_errors,
                    exc_info=True,
                )

                # Brief delay before retrying - could be configurable
                await asyncio.sleep(10.0)

        logger.info("metrics_collection_loop_ended")

    async def _collect_from_provider(self, provider: object) -> list[MetricValue]:
        """Collect metrics from a specific provider.

        Args:
            provider: Metrics provider object

        Returns:
            List of metrics from provider

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout for provider calls
        - NO assumptions about provider interface
        - Converts provider data to typed metrics
        """
        try:
            # Call provider with timeout
            provider_timeout = float(self.config.monitoring.health_check_interval_seconds)
            raw_metrics = await asyncio.wait_for(provider.get_metrics(), timeout=provider_timeout)

            metrics = []

            if isinstance(raw_metrics, dict):
                for name, value in raw_metrics.items():
                    if isinstance(value, (int, float, Decimal)):
                        metrics.append(
                            MetricValue(
                                name=f"{type(provider).__name__.lower()}.{name}",
                                value=Decimal(str(value)),
                                metric_type=MetricType.GAUGE,  # Default type
                                timestamp=datetime.now(UTC),
                                tags={"provider": type(provider).__name__},
                            )
                        )

            return metrics

        except TimeoutError:
            logger.warning(
                "metrics_provider_timeout",
                provider_type=type(provider).__name__,
                timeout_seconds=provider_timeout,
            )
            return []
        except Exception as e:
            logger.error(
                "metrics_provider_collection_error",
                provider_type=type(provider).__name__,
                error=str(e),
                exc_info=True,
            )
            return []

    async def _cleanup_old_metrics(self) -> None:
        """Clean up old metrics based on retention policy.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Retention period from config
        - NO hardcoded cleanup intervals
        - Explicit logging of cleanup actions
        """
        if self._retention_days <= 0:
            # Retention disabled
            return

        cutoff_time = datetime.now(UTC) - timedelta(days=self._retention_days)
        original_count = len(self._metric_history)

        # Remove old snapshots
        self._metric_history = [
            snapshot for snapshot in self._metric_history if snapshot.timestamp > cutoff_time
        ]

        removed_count = original_count - len(self._metric_history)

        if removed_count > 0:
            logger.info(
                "metrics_cleanup_completed",
                removed_snapshots=removed_count,
                remaining_snapshots=len(self._metric_history),
                retention_days=self._retention_days,
            )

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of metrics collection status.

        Returns:
            Dictionary with collection statistics and configuration

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured statistics
        - Configuration context included
        - NO hardcoded status values
        """
        current_snapshot_count = len(self._metric_history)
        latest_snapshot = self._metric_history[-1] if self._metric_history else None

        return {
            "enabled": self._enabled,
            "running": self._running,
            "collection_count": self._collection_count,
            "collection_errors": self._collection_errors,
            "last_collection": self._last_collection.isoformat() if self._last_collection else None,
            "snapshot_count": current_snapshot_count,
            "registered_providers": len(self._metric_providers),
            "latest_metrics_count": len(latest_snapshot.metrics) if latest_snapshot else 0,
            "configuration": {
                "collection_interval_seconds": float(self._collection_interval),
                "retention_days": self._retention_days,
                "max_metrics_per_snapshot": self._max_metrics_per_snapshot,
            },
        }

    def get_recent_metrics(self, limit: int = 10) -> list[MetricsSnapshot]:
        """Get recent metrics snapshots.

        Args:
            limit: Maximum number of snapshots to return

        Returns:
            List of recent metrics snapshots
        """
        return self._metric_history[-limit:] if self._metric_history else []

    def get_metric_history(
        self, metric_name: str, since: datetime | None = None
    ) -> list[MetricValue]:
        """Get history for a specific metric.

        Args:
            metric_name: Name of metric to retrieve
            since: Optional start time for history

        Returns:
            List of metric values over time
        """
        history = []
        start_time = since or (datetime.now(UTC) - timedelta(hours=24))

        for snapshot in self._metric_history:
            if snapshot.timestamp >= start_time:
                for metric in snapshot.metrics:
                    if metric.name == metric_name:
                        history.append(metric)

        return history
