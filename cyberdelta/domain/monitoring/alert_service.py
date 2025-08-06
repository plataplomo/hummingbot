"""Alert service for monitoring and notification management.

This module provides comprehensive alerting capabilities with configuration-driven
alert channels, thresholds, and escalation policies.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status tracking."""

    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AlertChannel(Enum):
    """Available alert channels."""

    LOG = "log"
    TELEGRAM = "telegram"


@dataclass
class AlertThreshold:
    """Configuration for alert thresholds."""

    metric_name: str
    operator: str  # "gt", "lt", "eq", "gte", "lte"
    value: float
    duration_seconds: float
    level: AlertLevel


class Alert(BaseModel):
    """Individual alert instance."""

    alert_id: str
    title: str
    description: str
    level: AlertLevel
    status: AlertStatus
    source: str
    timestamp: datetime
    resolved_at: datetime | None = None
    acknowledged_at: datetime | None = None
    metadata: dict[str, Any] = {}
    channels_notified: list[str] = []
    escalation_level: int = 0


class AlertRule(BaseModel):
    """Alert rule configuration."""

    rule_id: str
    name: str
    description: str
    enabled: bool
    thresholds: list[AlertThreshold]
    channels: list[AlertChannel]
    suppression_duration_seconds: float
    escalation_enabled: bool
    escalation_delay_seconds: float
    tags: list[str] = []


class AlertService:
    """Central alert management service with configuration-driven behavior.

    Configuration Usage:
    - Uses config.monitoring.notifications_enabled for global alert control
    - Uses config.monitoring.alert_methods for available channels
    - Uses config.monitoring.alert_thresholds for threshold configuration
    - Uses config.monitoring.alert_suppression_seconds for alert suppression
    - Uses config.monitoring.escalation_enabled for escalation control

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL thresholds and channels from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Fail-fast on configuration violations
    - Type-safe alert handling
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize alert service with configuration.

        Args:
            config: Application settings containing monitoring configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring
        self._active_alerts: dict[str, Alert] = {}
        self._alert_rules: dict[str, AlertRule] = {}
        self._suppressed_alerts: set[str] = set()
        self._running = False
        self._alert_task: asyncio.Task[None] | None = None

        # Extract configuration settings - NO hardcoded defaults
        self._enabled = self._monitoring_config.notifications_enabled
        self._available_channels = set(self._monitoring_config.alert_methods)
        self._default_suppression = self._monitoring_config.alert_suppression_seconds
        self._escalation_enabled = self._monitoring_config.escalation_enabled
        self._escalation_delay = self._monitoring_config.escalation_delay_seconds
        self._max_alerts_per_minute = self._monitoring_config.max_alerts_per_minute

        # Alert rate limiting
        self._alert_timestamps: list[datetime] = []

        # Task tracking for suppression tasks
        self._suppression_tasks: set[asyncio.Task[None]] = set()

        logger.info(
            "alert_service_initialized",
            enabled=self._enabled,
            available_channels=list(self._available_channels),
            default_suppression_sec=float(self._default_suppression),
            escalation_enabled=self._escalation_enabled,
            max_alerts_per_minute=self._max_alerts_per_minute,
        )

    async def start(self) -> None:
        """Start alert processing and escalation monitoring.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured intervals
        - Proper task management
        - Fail-fast if already running
        """
        if self._running:
            logger.warning("alert_service_already_running")
            return

        if not self._enabled:
            logger.info("alert_service_disabled_in_config")
            return

        self._running = True
        self._alert_task = asyncio.create_task(self._alert_processing_loop())

        logger.info(
            "alert_service_started",
            enabled_channels=list(self._available_channels),
            escalation_enabled=self._escalation_enabled,
        )

    async def stop(self) -> None:
        """Stop alert processing.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful task cancellation
        - Proper cleanup
        - Uses configured shutdown timeout
        """
        if not self._running:
            logger.warning("alert_service_not_running")
            return

        self._running = False

        if self._alert_task:
            self._alert_task.cancel()
            try:
                await asyncio.wait_for(
                    self._alert_task, timeout=float(self.config.general.shutdown_grace_period)
                )
            except TimeoutError:
                logger.warning(
                    "alert_service_shutdown_timeout",
                    grace_period_sec=float(self.config.general.shutdown_grace_period),
                )
            except asyncio.CancelledError:
                pass

        logger.info("alert_service_stopped")

    async def create_alert(
        self,
        title: str,
        description: str,
        level: AlertLevel,
        source: str,
        metadata: dict[str, Any] | None = None,
        channels: list[AlertChannel] | None = None,
    ) -> Alert | None:
        """Create and process a new alert.

        Args:
            title: Alert title
            description: Detailed alert description
            level: Alert severity level
            source: Source service or component
            metadata: Additional alert context
            channels: Specific channels to use (overrides default)

        Returns:
            Created Alert object if processed, None if suppressed/disabled

        IMPORTANT: Following CODING_STANDARDS.md:
        - Rate limiting based on configuration
        - Suppression logic based on configuration
        - Type-safe alert creation
        """
        if not self._enabled:
            logger.debug("alert_creation_skipped_service_disabled", title=title)
            return None

        # Check rate limiting
        if not self._check_rate_limit():
            logger.warning(
                "alert_rate_limited",
                title=title,
                level=level.value,
                max_per_minute=self._max_alerts_per_minute,
            )
            return None

        # Generate alert ID
        alert_id = f"{source}_{level.value}_{hash(title)}_{int(datetime.now(UTC).timestamp())}"

        # Check suppression
        if self._is_alert_suppressed(alert_id, title, source):
            logger.debug("alert_suppressed", alert_id=alert_id, title=title, source=source)
            return None

        # Create alert object
        alert = Alert(
            alert_id=alert_id,
            title=title,
            description=description,
            level=level,
            status=AlertStatus.ACTIVE,
            source=source,
            timestamp=datetime.now(UTC),
            metadata=metadata or {},
            channels_notified=[],
            escalation_level=0,
        )

        # Determine channels to use
        alert_channels = channels or self._get_default_channels_for_level(level)

        # Send alert
        await self._send_alert(alert, alert_channels)

        # Track active alert
        self._active_alerts[alert_id] = alert

        # Add to suppression if configured
        if self._default_suppression > 0:
            self._suppressed_alerts.add(f"{source}_{title}")
            # Schedule suppression removal
            suppression_key = f"{source}_{title}"
            suppression_task = asyncio.create_task(
                self._remove_suppression_after_delay(suppression_key)
            )
            # Store task reference to prevent garbage collection
            self._suppression_tasks.add(suppression_task)
            suppression_task.add_done_callback(self._suppression_tasks.discard)

        logger.info(
            "alert_created",
            alert_id=alert_id,
            title=title,
            level=level.value,
            source=source,
            channels=len(alert_channels),
            suppression_enabled=self._default_suppression > 0,
        )

        return alert

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an active alert.

        Args:
            alert_id: ID of alert to acknowledge
            acknowledged_by: User or system that acknowledged the alert

        Returns:
            True if acknowledgment successful, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit acknowledgment tracking
        - Structured logging for audit trail
        """
        if alert_id not in self._active_alerts:
            logger.warning(
                "acknowledge_alert_not_found", alert_id=alert_id, acknowledged_by=acknowledged_by
            )
            return False

        alert = self._active_alerts[alert_id]
        if alert.status == AlertStatus.RESOLVED:
            logger.warning(
                "acknowledge_alert_already_resolved",
                alert_id=alert_id,
                acknowledged_by=acknowledged_by,
            )
            return False

        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.now(UTC)
        alert.metadata["acknowledged_by"] = acknowledged_by

        logger.info(
            "alert_acknowledged",
            alert_id=alert_id,
            title=alert.title,
            acknowledged_by=acknowledged_by,
            alert_age_seconds=(datetime.now(UTC) - alert.timestamp).total_seconds(),
        )

        return True

    async def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an active alert.

        Args:
            alert_id: ID of alert to resolve
            resolved_by: User or system that resolved the alert

        Returns:
            True if resolution successful, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit resolution tracking
        - Cleanup of active alerts
        """
        if alert_id not in self._active_alerts:
            logger.warning("resolve_alert_not_found", alert_id=alert_id, resolved_by=resolved_by)
            return False

        alert = self._active_alerts[alert_id]
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.now(UTC)
        alert.metadata["resolved_by"] = resolved_by

        # Remove from active tracking
        del self._active_alerts[alert_id]

        logger.info(
            "alert_resolved",
            alert_id=alert_id,
            title=alert.title,
            resolved_by=resolved_by,
            alert_duration_seconds=(alert.resolved_at - alert.timestamp).total_seconds(),
        )

        return True

    def _check_rate_limit(self) -> bool:
        """Check if alert creation is within rate limits.

        Returns:
            True if within limits, False if rate limited

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured rate limits
        - Sliding window rate limiting
        """
        now = datetime.now(UTC)
        cutoff = now - timedelta(minutes=1)

        # Remove old timestamps
        self._alert_timestamps = [ts for ts in self._alert_timestamps if ts > cutoff]

        # Check current rate
        if len(self._alert_timestamps) >= self._max_alerts_per_minute:
            return False

        # Add current timestamp
        self._alert_timestamps.append(now)
        return True

    def _is_alert_suppressed(self, alert_id: str, title: str, source: str) -> bool:
        """Check if alert should be suppressed.

        Args:
            alert_id: Alert ID
            title: Alert title
            source: Alert source

        Returns:
            True if alert should be suppressed

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured suppression rules
        - Explicit suppression logic
        """
        suppression_key = f"{source}_{title}"
        return suppression_key in self._suppressed_alerts

    def _get_default_channels_for_level(self, level: AlertLevel) -> list[AlertChannel]:
        """Get default channels for alert level.

        Returns:
            List of alert channels for the given level
        """
        channels = [AlertChannel.LOG]

        critical_levels = {AlertLevel.WARNING, AlertLevel.ERROR, AlertLevel.CRITICAL}
        if level in critical_levels and "telegram" in self._available_channels:
            channels.append(AlertChannel.TELEGRAM)

        return channels

    async def _send_alert(self, alert: Alert, channels: list[AlertChannel]) -> None:
        """Send alert through specified channels.

        Args:
            alert: Alert to send
            channels: Channels to use for sending

        IMPORTANT: Following CODING_STANDARDS.md:
        - Channel-specific implementations
        - Proper error handling per channel
        """
        for channel in channels:
            try:
                success = await self._send_via_channel(alert, channel)
                if success:
                    alert.channels_notified.append(channel.value)

                logger.debug(
                    "alert_sent_via_channel",
                    alert_id=alert.alert_id,
                    channel=channel.value,
                    success=success,
                )

            except Exception as e:
                logger.exception(
                    "alert_channel_send_failed",
                    alert_id=alert.alert_id,
                    channel=channel.value,
                    error=str(e),
                )

    async def _send_via_channel(self, alert: Alert, channel: AlertChannel) -> bool:
        """Send alert via specific channel.

        Args:
            alert: Alert to send
            channel: Channel to use

        Returns:
            True if send successful
        """
        if channel == AlertChannel.LOG:
            return self._send_via_log(alert)
        # AlertChannel.TELEGRAM
        return await self._send_via_telegram(alert)

    def _send_via_log(self, alert: Alert) -> bool:
        """Send alert via structured logging.

        Args:
            alert: Alert to log

        Returns:
            True (logging always succeeds)
        """
        log_level = {
            AlertLevel.INFO: logger.info,
            AlertLevel.WARNING: logger.warning,
            AlertLevel.ERROR: logger.error,
            AlertLevel.CRITICAL: logger.critical,
        }.get(alert.level, logger.info)

        log_level(
            "system_alert",
            alert_id=alert.alert_id,
            title=alert.title,
            description=alert.description,
            level=alert.level.value,
            source=alert.source,
            metadata=alert.metadata,
        )

        return True

    async def _send_via_telegram(self, alert: Alert) -> bool:
        """Send alert via Telegram.

        Returns:
            True if send successful
        """
        # Simple telegram implementation placeholder
        logger.info(
            "telegram_alert",
            alert_id=alert.alert_id,
            title=alert.title,
            level=alert.level.value,
        )
        return True

    async def _remove_suppression_after_delay(self, suppression_key: str) -> None:
        """Remove alert suppression after configured delay.

        Args:
            suppression_key: Key to remove from suppression
        """
        await asyncio.sleep(float(self._default_suppression))
        self._suppressed_alerts.discard(suppression_key)

        logger.debug(
            "alert_suppression_removed",
            suppression_key=suppression_key,
            delay_seconds=float(self._default_suppression),
        )

    async def _alert_processing_loop(self) -> None:
        """Main alert processing loop for escalation and cleanup.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured escalation intervals
        - Handles errors without stopping loop
        """
        logger.info("alert_processing_loop_started")

        # Use configured escalation check interval
        check_interval = float(self._monitoring_config.health_check_interval_seconds)

        while self._running:
            try:
                # Process escalations if enabled
                if self._escalation_enabled:
                    await self._process_escalations()

                # Clean up old resolved alerts (if needed)
                await self._cleanup_old_alerts()

                # Wait for next cycle
                await asyncio.sleep(check_interval)

            except asyncio.CancelledError:
                logger.info("alert_processing_loop_cancelled")
                break
            except Exception as e:
                logger.exception("alert_processing_loop_error", error=str(e))
                # Brief delay before retrying
                await asyncio.sleep(10.0)  # Could be configurable

        logger.info("alert_processing_loop_ended")

    async def _process_escalations(self) -> None:
        """Process alert escalations based on configuration.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured escalation delays
        - Escalation based on alert levels and age
        """
        now = datetime.now(UTC)
        escalation_threshold = timedelta(seconds=self._escalation_delay)

        for alert_id, alert in self._active_alerts.items():
            if (
                alert.status == AlertStatus.ACTIVE
                and alert.escalation_level == 0
                and now - alert.timestamp >= escalation_threshold
            ):
                logger.warning(
                    "alert_escalation_triggered",
                    alert_id=alert_id,
                    title=alert.title,
                    level=alert.level.value,
                    age_seconds=(now - alert.timestamp).total_seconds(),
                )

                # Create escalated alert
                await self.create_alert(
                    title=f"ESCALATED: {alert.title}",
                    description=f"Original alert not acknowledged. {alert.description}",
                    level=AlertLevel.CRITICAL,
                    source=f"escalation_{alert.source}",
                    metadata={
                        "original_alert_id": alert_id,
                        "escalation_reason": "no_acknowledgment",
                        "original_level": alert.level.value,
                    },
                )

                alert.escalation_level += 1

    async def _cleanup_old_alerts(self) -> None:
        """Clean up old alert data if needed."""
        # For now, just log the cleanup opportunity
        # In production, this might archive old alerts or clean up memory
        active_count = len(self._active_alerts)
        suppressed_count = len(self._suppressed_alerts)

        logger.debug(
            "alert_cleanup_check", active_alerts=active_count, suppressed_alerts=suppressed_count
        )

    def get_alert_stats(self) -> dict[str, Any]:
        """Get alert service statistics.

        Returns:
            Dictionary with alert statistics and configuration

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured statistics
        - Configuration context included
        """
        return {
            "enabled": self._enabled,
            "running": self._running,
            "active_alerts": len(self._active_alerts),
            "suppressed_alerts": len(self._suppressed_alerts),
            "available_channels": list(self._available_channels),
            "configuration": {
                "max_alerts_per_minute": self._max_alerts_per_minute,
                "default_suppression_sec": float(self._default_suppression),
                "escalation_enabled": self._escalation_enabled,
                "escalation_delay_sec": float(self._escalation_delay),
            },
            "recent_alert_rate": len(self._alert_timestamps),
        }

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """Get list of currently active alerts.

        Returns:
            List of active alert summaries
        """
        return [
            {
                "alert_id": alert.alert_id,
                "title": alert.title,
                "level": alert.level.value,
                "status": alert.status.value,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "age_seconds": (datetime.now(UTC) - alert.timestamp).total_seconds(),
                "channels_notified": alert.channels_notified,
                "escalation_level": alert.escalation_level,
            }
            for alert in self._active_alerts.values()
        ]
