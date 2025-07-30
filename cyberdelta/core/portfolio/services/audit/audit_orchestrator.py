"""Audit orchestrator - coordinates focused audit services."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.models import OperationMetadata
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

from .audit_export_service import AuditExportService
from .audit_query_service import AuditFilter, AuditQuery, AuditQueryService
from .audit_recorder_service import AuditEntry, AuditRecorderService
from .audit_report_service import AuditReportService

logger = get_logger(__name__)


class AuditOrchestrator(BasePortfolioService):
    """Orchestrates audit operations using focused services."""
    
    # Service dependencies
    recorder_service: AuditRecorderService = Field(
        ..., description="Audit recorder service"
    )
    query_service: AuditQueryService = Field(
        ..., description="Audit query service"
    )
    report_service: AuditReportService = Field(
        ..., description="Audit report service"
    )
    export_service: AuditExportService = Field(
        ..., description="Audit export service"
    )
    
    # Configuration
    enable_auto_cleanup: bool = Field(
        default=True,
        description="Enable automatic cleanup of old entries"
    )
    retention_days: int = Field(
        default=30,
        gt=0,
        description="Days to retain audit entries"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def _initialize_internal(self) -> None:
        """Initialize orchestrator and all services."""
        # Services are already initialized as they're BasePortfolioService instances
        logger.info("Audit orchestrator initialized")
    
    async def _shutdown_internal(self) -> None:
        """Shutdown orchestrator."""
        logger.info("Audit orchestrator shutdown")
    
    # Recorder methods
    async def record_audit_entry(
        self,
        action: str,
        level: str = "info",
        component: str | None = None,
        details: dict[str, Any] | None = None,
        metadata: OperationMetadata | dict[str, Any] | None = None,
        tags: list[str] | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Record a new audit entry (delegated to recorder)."""
        return await self.recorder_service.record_audit_entry(
            action=action,
            level=level,
            component=component,
            details=details,
            metadata=metadata,
            tags=tags,
            correlation_id=correlation_id,
        )
    
    async def set_user_context(
        self,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        """Set user context (delegated to recorder)."""
        await self.recorder_service.set_user_context(user_id, session_id)
    
    # Query methods
    async def get_audit_entries(
        self,
        query: AuditQuery | None = None,
    ) -> dict[str, Any]:
        """Query audit entries with comprehensive results."""
        # Get all entries from recorder
        all_entries = await self.recorder_service.get_all_entries()
        
        # Query entries
        result = await self.query_service.query_audit_entries(all_entries, query)
        
        # Generate report for the results
        report = await self.report_service.generate_summary_report(
            result.entries,
            include_details=True
        )
        
        return {
            "entries": [e.model_dump() for e in result.entries],
            "total_count": result.total_count,
            "filtered_count": result.filtered_count,
            "report": report,
            "query": result.query.model_dump(),
        }
    
    async def get_audit_entry_by_id(self, entry_id: str) -> AuditEntry | None:
        """Get specific audit entry (delegated to recorder)."""
        return await self.recorder_service.get_audit_entry(entry_id)
    
    async def get_audit_entries_by_correlation(
        self,
        correlation_id: str,
    ) -> list[AuditEntry]:
        """Get entries by correlation ID (delegated to recorder)."""
        return await self.recorder_service.get_entries_by_correlation(correlation_id)
    
    async def get_audit_trail_for_component(
        self,
        component: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Get audit trail for a specific component."""
        # Get entries from recorder
        entries = await self.recorder_service.get_entries_by_component(component)
        
        # Sort by timestamp (newest first)
        sorted_entries = sorted(entries, key=lambda e: e.timestamp, reverse=True)
        
        # Apply limit
        limited_entries = sorted_entries[:limit]
        
        # Generate component report
        all_entries = await self.recorder_service.get_all_entries()
        report = await self.report_service.generate_component_report(
            all_entries,
            component
        )
        
        return {
            "component": component,
            "entries": [e.model_dump() for e in limited_entries],
            "total_count": len(entries),
            "report": report,
        }
    
    # Report methods
    async def get_audit_statistics(self) -> dict[str, Any]:
        """Get comprehensive audit statistics."""
        # Get all entries
        all_entries = await self.recorder_service.get_all_entries()
        
        # Generate statistics
        stats = await self.report_service.generate_statistics(all_entries)
        
        return {
            "statistics": stats.model_dump(),
            "storage": {
                "total_entries": len(self.recorder_service.audit_entries),
                "max_entries": self.recorder_service.max_entries,
                "usage_percent": (
                    len(self.recorder_service.audit_entries) / 
                    self.recorder_service.max_entries * 100
                ),
            },
        }
    
    # Export/Import methods
    async def export_audit_trail(
        self,
        format: str = "json",
        query: AuditQuery | None = None,
    ) -> str:
        """Export audit trail in specified format."""
        # Get entries
        all_entries = await self.recorder_service.get_all_entries()
        
        # Apply query if provided
        if query:
            result = await self.query_service.query_audit_entries(all_entries, query)
            entries = result.entries
        else:
            entries = all_entries
        
        # Export based on format
        if format == "csv":
            return await self.export_service.export_to_csv(entries)
        elif format == "summary":
            return await self.export_service.export_summary(entries, "text")
        else:  # Default to JSON
            return await self.export_service.export_to_json(entries, pretty=True)
    
    async def import_audit_trail(
        self,
        data: str,
        format: str = "json",
    ) -> dict[str, Any]:
        """Import audit trail from external data."""
        # Import based on format
        if format == "csv":
            entries = await self.export_service.import_from_csv(data)
        else:  # Default to JSON
            entries = await self.export_service.import_from_json(data)
        
        # Add imported entries to recorder
        imported_count = 0
        for entry in entries:
            # Check if entry already exists
            if entry.id not in self.recorder_service.audit_entries:
                self.recorder_service.audit_entries[entry.id] = entry
                
                # Update indices
                if entry.correlation_id:
                    if entry.correlation_id not in self.recorder_service.entries_by_correlation:
                        self.recorder_service.entries_by_correlation[entry.correlation_id] = []
                    self.recorder_service.entries_by_correlation[entry.correlation_id].append(entry.id)
                
                if entry.component not in self.recorder_service.entries_by_component:
                    self.recorder_service.entries_by_component[entry.component] = []
                self.recorder_service.entries_by_component[entry.component].append(entry.id)
                
                imported_count += 1
        
        return {
            "imported": imported_count,
            "total_provided": len(entries),
            "duplicates_skipped": len(entries) - imported_count,
        }
    
    async def cleanup_old_entries(self) -> int:
        """Clean up old audit entries based on retention policy."""
        if not self.enable_auto_cleanup:
            return 0
        
        import time
        current_time = time.time()
        retention_seconds = self.retention_days * 86400
        cutoff_time = current_time - retention_seconds
        
        # Find entries to remove
        entries_to_remove = []
        for entry_id, entry in self.recorder_service.audit_entries.items():
            if entry.timestamp < cutoff_time:
                entries_to_remove.append((entry_id, entry))
        
        # Remove old entries
        for entry_id, entry in entries_to_remove:
            # Remove from main storage
            del self.recorder_service.audit_entries[entry_id]
            
            # Remove from indices
            if entry.correlation_id:
                corr_entries = self.recorder_service.entries_by_correlation.get(
                    entry.correlation_id, []
                )
                if entry_id in corr_entries:
                    corr_entries.remove(entry_id)
                if not corr_entries:
                    del self.recorder_service.entries_by_correlation[entry.correlation_id]
            
            comp_entries = self.recorder_service.entries_by_component.get(
                entry.component, []
            )
            if entry_id in comp_entries:
                comp_entries.remove(entry_id)
            if not comp_entries:
                del self.recorder_service.entries_by_component[entry.component]
        
        if entries_to_remove:
            logger.info(
                "Cleaned up old audit entries",
                removed_count=len(entries_to_remove),
                retention_days=self.retention_days,
            )
        
        return len(entries_to_remove)