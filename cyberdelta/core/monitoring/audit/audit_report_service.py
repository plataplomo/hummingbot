"""Audit report service - generates statistics and reports."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService

from .audit_recorder_service import AuditEntry

logger = get_logger(__name__)


class AuditStatistics(BaseModel):
    """Statistics about audit entries."""
    
    total_entries: int = Field(..., description="Total number of entries")
    entries_by_level: dict[str, int] = Field(..., description="Count by level")
    entries_by_action: dict[str, int] = Field(..., description="Count by action")
    entries_by_component: dict[str, int] = Field(..., description="Count by component")
    entries_by_user: dict[str, int] = Field(..., description="Count by user")
    entries_by_hour: dict[int, int] = Field(..., description="Count by hour of day")
    active_users: list[str] = Field(..., description="List of active users")
    active_components: list[str] = Field(..., description="List of active components")
    error_rate: float = Field(..., description="Percentage of error entries")
    warning_rate: float = Field(..., description="Percentage of warning entries")
    time_range: dict[str, float] = Field(..., description="Time range of entries")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditReportService(BaseService):
    """Generates audit statistics and reports."""
    
    # Configuration
    include_user_stats: bool = Field(
        default=True,
        description="Include user statistics in reports"
    )
    include_hourly_stats: bool = Field(
        default=True,
        description="Include hourly statistics in reports"
    )
    top_n_limit: int = Field(
        default=10,
        gt=0,
        description="Limit for top N statistics"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def generate_statistics(
        self,
        entries: list[AuditEntry],
    ) -> AuditStatistics:
        """Generate comprehensive statistics from audit entries.
        
        Args:
            entries: List of audit entries
            
        Returns:
            Audit statistics
        """
        if not entries:
            return AuditStatistics(
                total_entries=0,
                entries_by_level={},
                entries_by_action={},
                entries_by_component={},
                entries_by_user={},
                entries_by_hour={},
                active_users=[],
                active_components=[],
                error_rate=0.0,
                warning_rate=0.0,
                time_range={},
            )
        
        # Count by level
        level_counts: dict[str, int] = defaultdict(int)
        action_counts: dict[str, int] = defaultdict(int)
        component_counts: dict[str, int] = defaultdict(int)
        user_counts: dict[str, int] = defaultdict(int)
        hour_counts: dict[int, int] = defaultdict(int)
        
        # Track unique values
        unique_users: set[str] = set()
        unique_components: set[str] = set()
        
        # Track time range
        min_timestamp = float('inf')
        max_timestamp = float('-inf')
        
        # Process entries
        for entry in entries:
            # Level counts
            level_counts[entry.level] += 1
            
            # Action counts
            action_counts[entry.action] += 1
            
            # Component counts
            component_counts[entry.component] += 1
            unique_components.add(entry.component)
            
            # User counts
            if entry.user_id and self.include_user_stats:
                user_counts[entry.user_id] += 1
                unique_users.add(entry.user_id)
            
            # Hourly counts
            if self.include_hourly_stats:
                hour = int((entry.timestamp % 86400) / 3600)  # Hour of day
                hour_counts[hour] += 1
            
            # Time range
            min_timestamp = min(min_timestamp, entry.timestamp)
            max_timestamp = max(max_timestamp, entry.timestamp)
        
        # Calculate rates
        total = len(entries)
        error_rate = (level_counts.get("error", 0) / total * 100) if total > 0 else 0.0
        warning_rate = (level_counts.get("warning", 0) / total * 100) if total > 0 else 0.0
        
        # Get top N actions
        top_actions = dict(
            sorted(action_counts.items(), key=lambda x: x[1], reverse=True)[:self.top_n_limit]
        )
        
        # Get top N components
        top_components = dict(
            sorted(component_counts.items(), key=lambda x: x[1], reverse=True)[:self.top_n_limit]
        )
        
        return AuditStatistics(
            total_entries=total,
            entries_by_level=dict(level_counts),
            entries_by_action=top_actions,
            entries_by_component=top_components,
            entries_by_user=dict(user_counts) if self.include_user_stats else {},
            entries_by_hour=dict(hour_counts) if self.include_hourly_stats else {},
            active_users=sorted(unique_users),
            active_components=sorted(unique_components),
            error_rate=round(error_rate, 2),
            warning_rate=round(warning_rate, 2),
            time_range={
                "start": min_timestamp,
                "end": max_timestamp,
                "duration_hours": (max_timestamp - min_timestamp) / 3600,
            } if min_timestamp != float('inf') else {},
        )
    
    async def generate_summary_report(
        self,
        entries: list[AuditEntry],
        include_details: bool = False,
    ) -> dict[str, Any]:
        """Generate a summary report of audit entries.
        
        Args:
            entries: List of audit entries
            include_details: Include detailed breakdowns
            
        Returns:
            Summary report dictionary
        """
        stats = await self.generate_statistics(entries)
        
        report = {
            "summary": {
                "total_entries": stats.total_entries,
                "error_rate": f"{stats.error_rate}%",
                "warning_rate": f"{stats.warning_rate}%",
                "time_range_hours": stats.time_range.get("duration_hours", 0),
            },
            "levels": stats.entries_by_level,
            "top_actions": stats.entries_by_action,
            "top_components": stats.entries_by_component,
            "active_users_count": len(stats.active_users),
            "active_components_count": len(stats.active_components),
        }
        
        if include_details:
            report["details"] = {
                "user_activity": stats.entries_by_user,
                "hourly_distribution": stats.entries_by_hour,
                "active_users": stats.active_users,
                "active_components": stats.active_components,
            }
        
        return report
    
    async def generate_component_report(
        self,
        entries: list[AuditEntry],
        component: str,
    ) -> dict[str, Any]:
        """Generate a report for a specific component.
        
        Args:
            entries: List of audit entries
            component: Component to report on
            
        Returns:
            Component-specific report
        """
        # Filter entries for component
        component_entries = [e for e in entries if e.component == component]
        
        if not component_entries:
            return {
                "component": component,
                "total_entries": 0,
                "message": "No entries found for component",
            }
        
        # Generate statistics for component
        stats = await self.generate_statistics(component_entries)
        
        return {
            "component": component,
            "total_entries": stats.total_entries,
            "levels": stats.entries_by_level,
            "top_actions": stats.entries_by_action,
            "error_rate": f"{stats.error_rate}%",
            "warning_rate": f"{stats.warning_rate}%",
            "active_users": stats.active_users,
            "time_range": stats.time_range,
        }
    
    async def generate_user_report(
        self,
        entries: list[AuditEntry],
        user_id: str,
    ) -> dict[str, Any]:
        """Generate a report for a specific user.
        
        Args:
            entries: List of audit entries
            user_id: User ID to report on
            
        Returns:
            User-specific report
        """
        # Filter entries for user
        user_entries = [e for e in entries if e.user_id == user_id]
        
        if not user_entries:
            return {
                "user_id": user_id,
                "total_entries": 0,
                "message": "No entries found for user",
            }
        
        # Generate statistics for user
        stats = await self.generate_statistics(user_entries)
        
        return {
            "user_id": user_id,
            "total_entries": stats.total_entries,
            "levels": stats.entries_by_level,
            "top_actions": stats.entries_by_action,
            "components_used": sorted(stats.entries_by_component.keys()),
            "error_rate": f"{stats.error_rate}%",
            "warning_rate": f"{stats.warning_rate}%",
            "time_range": stats.time_range,
        }