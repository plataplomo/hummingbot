"""Portfolio audit trail service for tracking all portfolio changes."""

from __future__ import annotations

import csv
import io
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


# Type-preserving factory functions
def _str_any_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, Any] type information."""
    return {}


def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information."""
    return []


def _str_float_dict_factory() -> dict[str, float]:
    """Factory function that preserves dict[str, float] type information."""
    return {}


def _audit_entry_list_factory() -> list[AuditEntry]:
    """Factory function that preserves list[AuditEntry] type information."""
    return []


logger = get_logger(__name__)

# Constants
DEFAULT_MAX_ENTRIES = 100000
DEFAULT_RETENTION_DAYS = 30
DEFAULT_CLEANUP_INTERVAL = 3600  # 1 hour


class AuditAction(Enum):
    """Audit action types."""

    TRADE_PROCESSED = "trade_processed"
    BALANCE_UPDATED = "balance_updated"
    POSITION_OPENED = "position_opened"
    POSITION_UPDATED = "position_updated"
    POSITION_CLOSED = "position_closed"
    ORDER_PLACED = "order_placed"
    ORDER_UPDATED = "order_updated"
    ORDER_CANCELLED = "order_cancelled"
    PNL_CALCULATED = "pnl_calculated"
    EXPOSURE_CALCULATED = "exposure_calculated"
    PORTFOLIO_INITIALIZED = "portfolio_initialized"
    PORTFOLIO_SHUTDOWN = "portfolio_shutdown"
    CONFIGURATION_CHANGED = "configuration_changed"
    ERROR_OCCURRED = "error_occurred"
    SYSTEM_EVENT = "system_event"


class AuditLevel(Enum):
    """Audit level classifications."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEntry:
    """Individual audit trail entry."""

    id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: float = field(default_factory=time.time)
    action: AuditAction = AuditAction.SYSTEM_EVENT
    level: AuditLevel = AuditLevel.INFO
    component: str = "unknown"
    exchange_id: str | None = None
    symbol: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=_str_any_dict_factory)
    before_state: dict[str, Any] | None = None
    after_state: dict[str, Any] | None = None
    correlation_id: str | None = None
    duration_ms: float | None = None
    tags: list[str] = field(default_factory=_str_list_factory)
    metadata: dict[str, Any] = field(default_factory=_str_any_dict_factory)

    def to_dict(self) -> dict[str, Any]:
        """Convert audit entry to dictionary."""
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "action": self.action.value,
            "level": self.level.value,
            "component": self.component,
            "exchange_id": self.exchange_id,
            "symbol": self.symbol,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "message": self.message,
            "details": self.details,
            "before_state": self.before_state,
            "after_state": self.after_state,
            "correlation_id": self.correlation_id,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuditEntry:
        """Create audit entry from dictionary."""
        return cls(
            id=data.get("id", str(uuid4())),
            timestamp=data.get("timestamp", time.time()),
            action=AuditAction(data.get("action", AuditAction.SYSTEM_EVENT.value)),
            level=AuditLevel(data.get("level", AuditLevel.INFO.value)),
            component=data.get("component", "unknown"),
            exchange_id=data.get("exchange_id"),
            symbol=data.get("symbol"),
            user_id=data.get("user_id"),
            session_id=data.get("session_id"),
            message=data.get("message", ""),
            details=data.get("details", {}),
            before_state=data.get("before_state"),
            after_state=data.get("after_state"),
            correlation_id=data.get("correlation_id"),
            duration_ms=data.get("duration_ms"),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


@dataclass
class AuditFilter:
    """Filter criteria for audit queries."""

    start_time: float | None = None
    end_time: float | None = None
    actions: list[AuditAction] | None = None
    levels: list[AuditLevel] | None = None
    components: list[str] | None = None
    exchanges: list[str] | None = None
    symbols: list[str] | None = None
    user_ids: list[str] | None = None
    session_ids: list[str] | None = None
    correlation_ids: list[str] | None = None
    tags: list[str] | None = None
    text_search: str | None = None
    limit: int | None = None
    offset: int | None = None


@dataclass
class AuditQuery:
    """Query for audit trail entries."""

    filter: AuditFilter = field(default_factory=AuditFilter)
    sort_by: str = "timestamp"
    sort_order: str = "desc"  # "asc" or "desc"
    include_details: bool = True
    include_states: bool = True


@dataclass
class AuditReport:
    """Audit report with statistics and entries."""

    total_entries: int = 0
    filtered_entries: int = 0
    entries: list[AuditEntry] = field(default_factory=_audit_entry_list_factory)
    statistics: dict[str, Any] = field(default_factory=_str_any_dict_factory)
    time_range: dict[str, float] = field(default_factory=_str_float_dict_factory)
    query: AuditQuery | None = None
    generated_at: float = field(default_factory=time.time)


class PortfolioAuditTrailService(BasePortfolioService):
    """Comprehensive audit trail service for portfolio operations."""

    def __init__(
        self, name: str = "PortfolioAuditTrailService", config: dict[str, Any] | None = None
    ) -> None:
        """Initialize the audit trail service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Configuration
        cfg = config or {}
        self.max_entries = cfg.get("max_entries", DEFAULT_MAX_ENTRIES)
        self.retention_days = cfg.get("retention_days", DEFAULT_RETENTION_DAYS)
        self.auto_cleanup_enabled = cfg.get("auto_cleanup_enabled", True)
        self.cleanup_interval = cfg.get("cleanup_interval", DEFAULT_CLEANUP_INTERVAL)  # 1 hour
        self.enable_state_tracking = cfg.get("enable_state_tracking", True)
        self.enable_performance_tracking = cfg.get("enable_performance_tracking", True)
        self.compression_enabled = cfg.get("compression_enabled", True)

        # Storage
        self.entries: list[AuditEntry] = []
        self.entries_by_id: dict[str, AuditEntry] = {}
        self.entries_by_correlation: dict[str, list[AuditEntry]] = defaultdict(list)
        self.entries_by_component: dict[str, list[AuditEntry]] = defaultdict(list)
        self.entries_by_action: dict[AuditAction, list[AuditEntry]] = defaultdict(list)

        # Statistics
        self.statistics: dict[str, Any] = {
            "total_entries": 0,
            "entries_by_action": defaultdict(int),
            "entries_by_level": defaultdict(int),
            "entries_by_component": defaultdict(int),
            "last_cleanup": 0,
            "cleanup_count": 0,
        }

        # State tracking
        self.state_snapshots: dict[str, dict[str, Any]] = {}
        self.performance_metrics: dict[str, list[float]] = defaultdict(list)

        # Session tracking
        self.current_session_id: str | None = None
        self.current_user_id: str | None = None

        logger.info(
            "portfolio_audit_trail_service_initialized",
            name=name,
            max_entries=self.max_entries,
            retention_days=self.retention_days,
            auto_cleanup_enabled=self.auto_cleanup_enabled,
        )

    async def _initialize_internal(self) -> None:
        """Initialize audit trail service."""
        # Generate session ID
        self.current_session_id = str(uuid4())

        # Record initialization
        await self.record_audit_entry(
            action=AuditAction.PORTFOLIO_INITIALIZED,
            level=AuditLevel.INFO,
            component="AuditTrailService",
            message="Portfolio audit trail service initialized",
            details={"session_id": self.current_session_id},
        )

        logger.info("portfolio_audit_trail_service_initialized_internal")

    async def _shutdown_internal(self) -> None:
        """Shutdown audit trail service."""
        # Record shutdown
        await self.record_audit_entry(
            action=AuditAction.PORTFOLIO_SHUTDOWN,
            level=AuditLevel.INFO,
            component="AuditTrailService",
            message="Portfolio audit trail service shutting down",
            details={"total_entries": len(self.entries)},
        )

        logger.info("portfolio_audit_trail_service_shutdown_internal")

    async def record_audit_entry(
        self,
        action: AuditAction,
        level: AuditLevel = AuditLevel.INFO,
        component: str = "unknown",
        message: str = "",
        exchange_id: str | None = None,
        symbol: str | None = None,
        details: dict[str, Any] | None = None,
        before_state: dict[str, Any] | None = None,
        after_state: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        duration_ms: float | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Record an audit entry."""
        try:
            # Create audit entry
            entry = AuditEntry(
                timestamp=time.time(),
                action=action,
                level=level,
                component=component,
                exchange_id=exchange_id,
                symbol=symbol,
                user_id=self.current_user_id,
                session_id=self.current_session_id,
                message=message,
                details=details or {},
                before_state=before_state,
                after_state=after_state,
                correlation_id=correlation_id,
                duration_ms=duration_ms,
                tags=tags or [],
                metadata=metadata or {},
            )

            # Store entry
            self.entries.append(entry)
            self.entries_by_id[entry.id] = entry

            # Update indexes
            if correlation_id:
                self.entries_by_correlation[correlation_id].append(entry)
            self.entries_by_component[component].append(entry)
            self.entries_by_action[action].append(entry)

            # Update statistics
            self.statistics["total_entries"] += 1
            self.statistics["entries_by_action"][action.value] += 1
            self.statistics["entries_by_level"][level.value] += 1
            self.statistics["entries_by_component"][component] += 1

            # Performance tracking
            if self.enable_performance_tracking and duration_ms is not None:
                self.performance_metrics[f"{component}_{action.value}"].append(duration_ms)

            # Cleanup if needed
            if self.auto_cleanup_enabled and len(self.entries) > self.max_entries:
                await self._cleanup_old_entries()

            logger.debug(
                "audit_entry_recorded",
                entry_id=entry.id,
                action=action.value,
                component=component,
                level=level.value,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception(
                "audit_entry_recording_failed", action=action.value, component=component
            )
            raise
        else:
            return entry.id

    async def get_audit_entries(self, query: AuditQuery | None = None) -> AuditReport:
        """Get audit entries based on query."""
        query = query or AuditQuery()
        filter_criteria = query.filter

        # Apply all filters
        filtered_entries = self._apply_all_filters(self.entries.copy(), filter_criteria)

        # Sort and paginate
        filtered_entries = self._sort_entries(filtered_entries, query)
        total_filtered = len(filtered_entries)
        filtered_entries = self._paginate_entries(filtered_entries, filter_criteria)

        # Build report
        report = self._build_audit_report(filtered_entries, total_filtered, query)

        logger.info(
            "audit_entries_retrieved",
            total_entries=report.total_entries,
            filtered_entries=report.filtered_entries,
            returned_entries=len(report.entries),
        )

        return report

    def _apply_all_filters(
        self, entries: list[AuditEntry], filter_criteria: AuditFilter
    ) -> list[AuditEntry]:
        """Apply all filter criteria to entries."""
        entries = self._apply_time_filters(entries, filter_criteria)
        entries = self._apply_categorical_filters(entries, filter_criteria)
        return self._apply_text_search(entries, filter_criteria)

    def _apply_time_filters(
        self, entries: list[AuditEntry], filter_criteria: AuditFilter
    ) -> list[AuditEntry]:
        """Apply time-based filters."""
        if filter_criteria.start_time is not None:
            entries = [entry for entry in entries if entry.timestamp >= filter_criteria.start_time]

        if filter_criteria.end_time is not None:
            entries = [entry for entry in entries if entry.timestamp <= filter_criteria.end_time]

        return entries

    def _apply_categorical_filters(
        self, entries: list[AuditEntry], filter_criteria: AuditFilter
    ) -> list[AuditEntry]:
        """Apply categorical filters (actions, levels, components, etc.)."""
        # Apply action filters
        if filter_criteria.actions:
            entries = [entry for entry in entries if entry.action in filter_criteria.actions]

        # Apply level filters
        if filter_criteria.levels:
            entries = [entry for entry in entries if entry.level in filter_criteria.levels]

        # Apply component filters
        if filter_criteria.components:
            entries = [entry for entry in entries if entry.component in filter_criteria.components]

        # Apply exchange filters
        if filter_criteria.exchanges:
            entries = [entry for entry in entries if entry.exchange_id in filter_criteria.exchanges]

        # Apply symbol filters
        if filter_criteria.symbols:
            entries = [entry for entry in entries if entry.symbol in filter_criteria.symbols]

        # Apply user filters
        if filter_criteria.user_ids:
            entries = [entry for entry in entries if entry.user_id in filter_criteria.user_ids]

        # Apply session filters
        if filter_criteria.session_ids:
            entries = [
                entry for entry in entries if entry.session_id in filter_criteria.session_ids
            ]

        # Apply correlation filters
        if filter_criteria.correlation_ids:
            entries = [
                entry
                for entry in entries
                if entry.correlation_id in filter_criteria.correlation_ids
            ]

        # Apply tag filters
        if filter_criteria.tags:
            entries = [
                entry for entry in entries if any(tag in entry.tags for tag in filter_criteria.tags)
            ]

        return entries

    def _apply_text_search(
        self, entries: list[AuditEntry], filter_criteria: AuditFilter
    ) -> list[AuditEntry]:
        """Apply text search filter."""
        if filter_criteria.text_search:
            search_text = filter_criteria.text_search.lower()
            entries = [
                entry
                for entry in entries
                if (
                    search_text in entry.message.lower()
                    or search_text in entry.component.lower()
                    or any(search_text in str(v).lower() for v in entry.details.values())
                )
            ]

        return entries

    def _sort_entries(self, entries: list[AuditEntry], query: AuditQuery) -> list[AuditEntry]:
        """Sort entries based on query parameters."""
        reverse = query.sort_order == "desc"
        if query.sort_by == "timestamp":
            entries.sort(key=lambda x: x.timestamp, reverse=reverse)
        elif query.sort_by == "action":
            entries.sort(key=lambda x: x.action.value, reverse=reverse)
        elif query.sort_by == "level":
            entries.sort(key=lambda x: x.level.value, reverse=reverse)
        elif query.sort_by == "component":
            entries.sort(key=lambda x: x.component, reverse=reverse)

        return entries

    def _paginate_entries(
        self, entries: list[AuditEntry], filter_criteria: AuditFilter
    ) -> list[AuditEntry]:
        """Apply pagination to entries."""
        if filter_criteria.offset:
            entries = entries[filter_criteria.offset :]
        if filter_criteria.limit:
            entries = entries[: filter_criteria.limit]

        return entries

    def _build_audit_report(
        self, entries: list[AuditEntry], total_filtered: int, query: AuditQuery
    ) -> AuditReport:
        """Build the final audit report."""
        # Calculate statistics
        statistics = self._calculate_statistics(entries)

        # Calculate time range
        time_range = {}
        if entries:
            time_range = {
                "start": min(entry.timestamp for entry in entries),
                "end": max(entry.timestamp for entry in entries),
            }

        # Create report
        return AuditReport(
            total_entries=len(self.entries),
            filtered_entries=total_filtered,
            entries=entries,
            statistics=statistics,
            time_range=time_range,
            query=query,
        )

    async def get_audit_entry_by_id(self, entry_id: str) -> AuditEntry | None:
        """Get specific audit entry by ID."""
        return self.entries_by_id.get(entry_id)

    async def get_audit_entries_by_correlation(self, correlation_id: str) -> list[AuditEntry]:
        """Get all audit entries for a correlation ID."""
        return self.entries_by_correlation.get(correlation_id, [])

    async def get_audit_trail_for_component(
        self, component: str, limit: int | None = None
    ) -> list[AuditEntry]:
        """Get audit trail for a specific component."""
        entries = self.entries_by_component.get(component, [])

        # Sort by timestamp (newest first)
        entries.sort(key=lambda x: x.timestamp, reverse=True)

        if limit:
            entries = entries[:limit]

        return entries

    async def get_audit_statistics(self) -> dict[str, Any]:
        """Get audit trail statistics."""
        time.time()

        # Calculate time-based statistics
        time_stats = {}
        if self.entries:
            oldest_entry = min(self.entries, key=lambda x: x.timestamp)
            newest_entry = max(self.entries, key=lambda x: x.timestamp)

            time_stats = {
                "oldest_entry": oldest_entry.timestamp,
                "newest_entry": newest_entry.timestamp,
                "time_span_seconds": newest_entry.timestamp - oldest_entry.timestamp,
            }

        # Calculate performance statistics
        performance_stats = {}
        if self.enable_performance_tracking:
            for operation, durations in self.performance_metrics.items():
                if durations:
                    performance_stats[operation] = {
                        "count": len(durations),
                        "avg_ms": sum(durations) / len(durations),
                        "min_ms": min(durations),
                        "max_ms": max(durations),
                    }

        return {
            **self.statistics,
            "time_statistics": time_stats,
            "performance_statistics": performance_stats,
            "memory_usage": {
                "entries_count": len(self.entries),
                "indexes_count": {
                    "by_id": len(self.entries_by_id),
                    "by_correlation": len(self.entries_by_correlation),
                    "by_component": len(self.entries_by_component),
                    "by_action": len(self.entries_by_action),
                },
            },
            "current_session": self.current_session_id,
            "current_user": self.current_user_id,
        }

    async def export_audit_trail(
        self, output_format: str = "json", query: AuditQuery | None = None
    ) -> str:
        """Export audit trail in specified format."""
        report = await self.get_audit_entries(query)

        if output_format.lower() == "json":
            return json.dumps(
                {
                    "report": {
                        "total_entries": report.total_entries,
                        "filtered_entries": report.filtered_entries,
                        "statistics": report.statistics,
                        "time_range": report.time_range,
                        "generated_at": report.generated_at,
                    },
                    "entries": [entry.to_dict() for entry in report.entries],
                },
                indent=2,
            )
        if output_format.lower() == "csv":
            return self._export_as_csv(report)
        raise ValueError

    async def import_audit_trail(
        self, data: str, data_format: str = "json", merge: bool = True
    ) -> dict[str, Any]:
        """Import audit trail from data."""
        if data_format.lower() == "json":
            imported_data = json.loads(data)
            entries_data = imported_data.get("entries", [])

            imported_count = 0
            for entry_data in entries_data:
                entry = AuditEntry.from_dict(entry_data)

                # Check if entry already exists (if merging)
                if merge and entry.id in self.entries_by_id:
                    continue

                # Add entry
                self.entries.append(entry)
                self.entries_by_id[entry.id] = entry

                # Update indexes
                if entry.correlation_id:
                    self.entries_by_correlation[entry.correlation_id].append(entry)
                self.entries_by_component[entry.component].append(entry)
                self.entries_by_action[entry.action].append(entry)

                imported_count += 1

            # Update statistics
            self.statistics["total_entries"] = len(self.entries)

            # Re-sort entries by timestamp
            self.entries.sort(key=lambda x: x.timestamp)

            return {
                "imported_count": imported_count,
                "total_entries": len(self.entries),
                "merge_mode": merge,
            }
        raise ValueError

    async def clear_audit_trail(self, confirm: bool = False) -> bool:
        """Clear all audit trail entries."""
        if not confirm:
            return False

        # Record clearing action
        await self.record_audit_entry(
            action=AuditAction.SYSTEM_EVENT,
            level=AuditLevel.WARNING,
            component="AuditTrailService",
            message="Audit trail cleared",
            details={"entries_cleared": len(self.entries)},
        )

        # Clear all data
        self.entries.clear()
        self.entries_by_id.clear()
        self.entries_by_correlation.clear()
        self.entries_by_component.clear()
        self.entries_by_action.clear()
        self.state_snapshots.clear()
        self.performance_metrics.clear()

        # Reset statistics
        self.statistics = {
            "total_entries": 0,
            "entries_by_action": defaultdict(int),
            "entries_by_level": defaultdict(int),
            "entries_by_component": defaultdict(int),
            "last_cleanup": time.time(),
            "cleanup_count": self.statistics.get("cleanup_count", 0) + 1,
        }

        logger.warning("audit_trail_cleared")
        return True

    async def set_user_context(
        self, user_id: str | None = None, session_id: str | None = None
    ) -> None:
        """Set user context for audit entries."""
        self.current_user_id = user_id
        if session_id:
            self.current_session_id = session_id

        await self.record_audit_entry(
            action=AuditAction.SYSTEM_EVENT,
            level=AuditLevel.INFO,
            component="AuditTrailService",
            message="User context updated",
            details={"user_id": user_id, "session_id": session_id},
        )

    def _calculate_statistics(self, entries: list[AuditEntry]) -> dict[str, Any]:
        """Calculate statistics for a list of entries."""
        if not entries:
            return {}

        # Count by action
        action_counts: dict[str, int] = defaultdict(int)
        for entry in entries:
            action_counts[entry.action.value] += 1

        # Count by level
        level_counts: dict[str, int] = defaultdict(int)
        for entry in entries:
            level_counts[entry.level.value] += 1

        # Count by component
        component_counts: dict[str, int] = defaultdict(int)
        for entry in entries:
            component_counts[entry.component] += 1

        # Count by exchange
        exchange_counts: dict[str, int] = defaultdict(int)
        for entry in entries:
            if entry.exchange_id:
                exchange_counts[entry.exchange_id] += 1

        return {
            "entry_count": len(entries),
            "actions": dict(action_counts),
            "levels": dict(level_counts),
            "components": dict(component_counts),
            "exchanges": dict(exchange_counts),
        }

    async def _cleanup_old_entries(self) -> int:
        """Clean up old entries based on retention policy."""
        current_time = time.time()
        retention_cutoff = current_time - (self.retention_days * 24 * DEFAULT_CLEANUP_INTERVAL)

        # Find entries to remove
        entries_to_remove = [entry for entry in self.entries if entry.timestamp < retention_cutoff]

        # Remove entries
        for entry in entries_to_remove:
            self.entries.remove(entry)
            self.entries_by_id.pop(entry.id, None)

            # Remove from indexes
            if entry.correlation_id and entry in self.entries_by_correlation[entry.correlation_id]:
                self.entries_by_correlation[entry.correlation_id].remove(entry)

            if entry in self.entries_by_component[entry.component]:
                self.entries_by_component[entry.component].remove(entry)

            if entry in self.entries_by_action[entry.action]:
                self.entries_by_action[entry.action].remove(entry)

        # Update statistics
        self.statistics["last_cleanup"] = current_time
        self.statistics["cleanup_count"] = self.statistics.get("cleanup_count", 0) + 1

        if entries_to_remove:
            logger.info(
                "audit_trail_cleanup_completed",
                entries_removed=len(entries_to_remove),
                retention_days=self.retention_days,
            )

        return len(entries_to_remove)

    def _export_as_csv(self, report: AuditReport) -> str:
        """Export audit report as CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow([
            "id",
            "timestamp",
            "action",
            "level",
            "component",
            "exchange_id",
            "symbol",
            "user_id",
            "session_id",
            "message",
            "correlation_id",
            "duration_ms",
            "tags",
            "details",
        ])

        # Write entries
        for entry in report.entries:
            writer.writerow([
                entry.id,
                entry.timestamp,
                entry.action.value,
                entry.level.value,
                entry.component,
                entry.exchange_id or "",
                entry.symbol or "",
                entry.user_id or "",
                entry.session_id or "",
                entry.message,
                entry.correlation_id or "",
                entry.duration_ms or "",
                "|".join(entry.tags),
                json.dumps(entry.details) if entry.details else "",
            ])

        return output.getvalue()
