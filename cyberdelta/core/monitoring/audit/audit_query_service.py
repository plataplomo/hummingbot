"""Audit query service - searches and filters audit entries."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService

from .audit_recorder_service import AuditEntry

logger = get_logger(__name__)


class AuditFilter(BaseModel):
    """Filter criteria for audit queries."""
    
    start_time: float | None = Field(default=None, description="Start timestamp")
    end_time: float | None = Field(default=None, description="End timestamp")
    actions: list[str] | None = Field(default=None, description="Filter by actions")
    levels: list[str] | None = Field(default=None, description="Filter by levels")
    components: list[str] | None = Field(default=None, description="Filter by components")
    user_ids: list[str] | None = Field(default=None, description="Filter by user IDs")
    tags: list[str] | None = Field(default=None, description="Filter by tags")
    correlation_id: str | None = Field(default=None, description="Filter by correlation ID")
    search_text: str | None = Field(default=None, description="Search in details")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditQuery(BaseModel):
    """Query parameters for audit trail."""
    
    filter: AuditFilter | None = Field(default=None, description="Filter criteria")
    limit: int = Field(default=100, gt=0, le=10000, description="Maximum results")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")
    sort_by: str = Field(default="timestamp", description="Sort field")
    sort_order: str = Field(default="desc", description="Sort order (asc/desc)")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditQueryResult(BaseModel):
    """Result of an audit query."""
    
    entries: list[AuditEntry] = Field(..., description="Matching audit entries")
    total_count: int = Field(..., description="Total count before pagination")
    filtered_count: int = Field(..., description="Count after filtering")
    query: AuditQuery = Field(..., description="Query that produced this result")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditQueryService(BaseService):
    """Queries and filters audit entries."""
    
    # Configuration
    max_search_results: int = Field(
        default=10000,
        gt=0,
        description="Maximum number of search results"
    )
    enable_full_text_search: bool = Field(
        default=True,
        description="Enable full text search in details"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def query_audit_entries(
        self,
        entries: list[AuditEntry],
        query: AuditQuery | None = None,
    ) -> AuditQueryResult:
        """Query audit entries with filtering and pagination.
        
        Args:
            entries: List of audit entries to query
            query: Query parameters
            
        Returns:
            Query result with filtered and paginated entries
        """
        if not query:
            query = AuditQuery()
        
        # Apply filters
        filtered_entries = await self._apply_filters(entries, query.filter)
        
        # Sort entries
        sorted_entries = await self._sort_entries(
            filtered_entries,
            query.sort_by,
            query.sort_order
        )
        
        # Apply pagination
        start_idx = query.offset
        end_idx = start_idx + min(query.limit, self.max_search_results)
        paginated_entries = sorted_entries[start_idx:end_idx]
        
        return AuditQueryResult(
            entries=paginated_entries,
            total_count=len(entries),
            filtered_count=len(filtered_entries),
            query=query,
        )
    
    async def _apply_filters(
        self,
        entries: list[AuditEntry],
        filter_criteria: AuditFilter | None,
    ) -> list[AuditEntry]:
        """Apply filter criteria to audit entries."""
        if not filter_criteria:
            return entries
        
        filtered = entries
        
        # Time range filter
        if filter_criteria.start_time is not None:
            filtered = [e for e in filtered if e.timestamp >= filter_criteria.start_time]
        
        if filter_criteria.end_time is not None:
            filtered = [e for e in filtered if e.timestamp <= filter_criteria.end_time]
        
        # Action filter
        if filter_criteria.actions:
            filtered = [e for e in filtered if e.action in filter_criteria.actions]
        
        # Level filter
        if filter_criteria.levels:
            filtered = [e for e in filtered if e.level in filter_criteria.levels]
        
        # Component filter
        if filter_criteria.components:
            filtered = [e for e in filtered if e.component in filter_criteria.components]
        
        # User ID filter
        if filter_criteria.user_ids:
            filtered = [
                e for e in filtered
                if e.user_id and e.user_id in filter_criteria.user_ids
            ]
        
        # Tag filter
        if filter_criteria.tags:
            tag_set = set(filter_criteria.tags)
            filtered = [
                e for e in filtered
                if any(tag in tag_set for tag in e.tags)
            ]
        
        # Correlation ID filter
        if filter_criteria.correlation_id:
            filtered = [
                e for e in filtered
                if e.correlation_id == filter_criteria.correlation_id
            ]
        
        # Full text search
        if filter_criteria.search_text and self.enable_full_text_search:
            search_lower = filter_criteria.search_text.lower()
            filtered = [
                e for e in filtered
                if await self._search_in_entry(e, search_lower)
            ]
        
        return filtered
    
    async def _search_in_entry(self, entry: AuditEntry, search_text: str) -> bool:
        """Search for text within an audit entry."""
        # Search in action
        if search_text in entry.action.lower():
            return True
        
        # Search in component
        if search_text in entry.component.lower():
            return True
        
        # Search in tags
        if any(search_text in tag.lower() for tag in entry.tags):
            return True
        
        # Search in details (convert to string representation)
        if entry.details:
            details_str = str(entry.details).lower()
            if search_text in details_str:
                return True
        
        # Search in metadata
        if entry.metadata:
            metadata_str = str(entry.metadata).lower()
            if search_text in metadata_str:
                return True
        
        return False
    
    async def _sort_entries(
        self,
        entries: list[AuditEntry],
        sort_by: str,
        sort_order: str,
    ) -> list[AuditEntry]:
        """Sort audit entries by specified field."""
        # Define sort key functions
        sort_keys = {
            "timestamp": lambda e: e.timestamp,
            "action": lambda e: e.action,
            "level": lambda e: e.level,
            "component": lambda e: e.component,
            "user_id": lambda e: e.user_id or "",
        }
        
        # Get sort key function
        key_func = sort_keys.get(sort_by, sort_keys["timestamp"])
        
        # Sort entries
        reverse = sort_order == "desc"
        return sorted(entries, key=key_func, reverse=reverse)
    
    async def find_related_entries(
        self,
        entry: AuditEntry,
        entries: list[AuditEntry],
        time_window: float = 60.0,
    ) -> list[AuditEntry]:
        """Find entries related to a given entry.
        
        Args:
            entry: The reference entry
            entries: List of entries to search
            time_window: Time window in seconds
            
        Returns:
            List of related entries
        """
        related = []
        
        # Find by correlation ID
        if entry.correlation_id:
            related.extend([
                e for e in entries
                if e.correlation_id == entry.correlation_id and e.id != entry.id
            ])
        
        # Find by time proximity and component
        min_time = entry.timestamp - time_window
        max_time = entry.timestamp + time_window
        
        time_related = [
            e for e in entries
            if (min_time <= e.timestamp <= max_time and
                e.component == entry.component and
                e.id != entry.id and
                e not in related)
        ]
        related.extend(time_related)
        
        # Sort by timestamp
        return sorted(related, key=lambda e: e.timestamp)