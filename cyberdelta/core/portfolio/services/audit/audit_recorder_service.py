"""Audit recorder service - records audit entries."""

from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.models import OperationMetadata
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

logger = get_logger(__name__)


class AuditEntry(BaseModel):
    """Represents a single audit entry."""
    
    id: str = Field(..., description="Unique audit entry ID")
    timestamp: float = Field(..., description="Unix timestamp of the event")
    action: str = Field(..., description="Action that was performed")
    level: str = Field(..., description="Audit level (info, warning, error, critical)")
    component: str = Field(..., description="Component that performed the action")
    user_id: str | None = Field(default=None, description="User who performed the action")
    session_id: str | None = Field(default=None, description="Session ID")
    correlation_id: str | None = Field(default=None, description="Correlation ID for tracking")
    details: dict[str, Any] = Field(default_factory=dict, description="Additional details")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Metadata")
    tags: list[str] = Field(default_factory=list, description="Tags for categorization")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditRecorderService(BasePortfolioService):
    """Records audit entries for portfolio operations."""
    
    # Configuration
    max_entries: int = Field(
        default=100000,
        gt=0,
        description="Maximum number of audit entries to keep in memory"
    )
    enable_detailed_logging: bool = Field(
        default=True,
        description="Enable detailed logging of audit entries"
    )
    
    # Storage
    audit_entries: dict[str, AuditEntry] = Field(
        default_factory=dict,
        description="In-memory storage of audit entries"
    )
    entries_by_correlation: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Index of entries by correlation ID"
    )
    entries_by_component: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Index of entries by component"
    )
    
    # Context
    user_context: dict[str, str | None] = Field(
        default_factory=lambda: {"user_id": None, "session_id": None},
        description="Current user context"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
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
        """Record a new audit entry.
        
        Args:
            action: Action that was performed
            level: Audit level (info, warning, error, critical)
            component: Component that performed the action
            details: Additional details about the action
            metadata: Operation metadata or custom metadata
            tags: Tags for categorization
            correlation_id: Correlation ID for tracking related operations
            
        Returns:
            The ID of the created audit entry
        """
        # Validate level
        valid_levels = {"info", "warning", "error", "critical"}
        if level not in valid_levels:
            level = "info"
        
        # Generate entry ID
        entry_id = str(uuid4())
        
        # Convert metadata if needed
        metadata_dict = {}
        if metadata:
            if isinstance(metadata, OperationMetadata):
                metadata_dict = {
                    "request_id": metadata.request_id,
                    "source": metadata.source,
                    "request_timestamp": metadata.request_timestamp,
                    "priority": metadata.priority,
                }
            else:
                metadata_dict = dict(metadata)
        
        # Create audit entry
        entry = AuditEntry(
            id=entry_id,
            timestamp=time.time(),
            action=action,
            level=level,
            component=component or self.name,
            user_id=self.user_context["user_id"],
            session_id=self.user_context["session_id"],
            correlation_id=correlation_id,
            details=details or {},
            metadata=metadata_dict,
            tags=tags or [],
        )
        
        # Store entry
        self.audit_entries[entry_id] = entry
        
        # Update indices
        if correlation_id:
            if correlation_id not in self.entries_by_correlation:
                self.entries_by_correlation[correlation_id] = []
            self.entries_by_correlation[correlation_id].append(entry_id)
        
        component_key = entry.component
        if component_key not in self.entries_by_component:
            self.entries_by_component[component_key] = []
        self.entries_by_component[component_key].append(entry_id)
        
        # Check if we need to cleanup old entries
        if len(self.audit_entries) > self.max_entries:
            await self._cleanup_oldest_entries()
        
        # Log if enabled
        if self.enable_detailed_logging:
            logger.info(
                "audit_entry_recorded",
                entry_id=entry_id,
                action=action,
                level=level,
                component=entry.component,
                correlation_id=correlation_id,
            )
        
        return entry_id
    
    async def get_audit_entry(self, entry_id: str) -> AuditEntry | None:
        """Get a specific audit entry by ID."""
        return self.audit_entries.get(entry_id)
    
    async def get_entries_by_correlation(self, correlation_id: str) -> list[AuditEntry]:
        """Get all audit entries for a correlation ID."""
        entry_ids = self.entries_by_correlation.get(correlation_id, [])
        return [
            self.audit_entries[entry_id]
            for entry_id in entry_ids
            if entry_id in self.audit_entries
        ]
    
    async def get_entries_by_component(self, component: str) -> list[AuditEntry]:
        """Get all audit entries for a component."""
        entry_ids = self.entries_by_component.get(component, [])
        return [
            self.audit_entries[entry_id]
            for entry_id in entry_ids
            if entry_id in self.audit_entries
        ]
    
    async def set_user_context(
        self,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        """Set the current user context for audit entries."""
        if user_id is not None:
            self.user_context["user_id"] = user_id
        if session_id is not None:
            self.user_context["session_id"] = session_id
        
        logger.info(
            "audit_user_context_updated",
            user_id=user_id,
            session_id=session_id,
        )
    
    async def get_all_entries(self) -> list[AuditEntry]:
        """Get all audit entries."""
        return list(self.audit_entries.values())
    
    async def _cleanup_oldest_entries(self) -> None:
        """Remove oldest entries when max_entries is exceeded."""
        # Sort entries by timestamp
        sorted_entries = sorted(
            self.audit_entries.items(),
            key=lambda x: x[1].timestamp
        )
        
        # Calculate how many to remove (10% of max)
        remove_count = max(1, self.max_entries // 10)
        
        # Remove oldest entries
        for entry_id, entry in sorted_entries[:remove_count]:
            # Remove from main storage
            del self.audit_entries[entry_id]
            
            # Remove from indices
            if entry.correlation_id:
                corr_entries = self.entries_by_correlation.get(entry.correlation_id, [])
                if entry_id in corr_entries:
                    corr_entries.remove(entry_id)
                if not corr_entries:
                    del self.entries_by_correlation[entry.correlation_id]
            
            comp_entries = self.entries_by_component.get(entry.component, [])
            if entry_id in comp_entries:
                comp_entries.remove(entry_id)
            if not comp_entries:
                del self.entries_by_component[entry.component]