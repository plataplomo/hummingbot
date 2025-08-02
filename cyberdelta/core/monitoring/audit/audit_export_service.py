"""Audit export service - handles export and import of audit data."""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService

from .audit_recorder_service import AuditEntry

logger = get_logger(__name__)


class AuditExportService(BaseService):
    """Handles export and import of audit entries."""
    
    # Configuration
    max_export_size: int = Field(
        default=1000000,
        gt=0,
        description="Maximum number of entries to export at once"
    )
    include_metadata_in_csv: bool = Field(
        default=False,
        description="Include metadata column in CSV exports"
    )
    csv_delimiter: str = Field(
        default=",",
        description="CSV delimiter character"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def export_to_json(
        self,
        entries: list[AuditEntry],
        pretty: bool = False,
    ) -> str:
        """Export audit entries to JSON format.
        
        Args:
            entries: Entries to export
            pretty: Pretty print the JSON
            
        Returns:
            JSON string
        """
        # Limit entries
        export_entries = entries[:self.max_export_size]
        
        # Convert to dict format
        export_data = {
            "audit_trail": [
                entry.model_dump() for entry in export_entries
            ],
            "export_info": {
                "total_entries": len(export_entries),
                "truncated": len(entries) > self.max_export_size,
            }
        }
        
        # Export to JSON
        if pretty:
            return json.dumps(export_data, indent=2, default=str)
        else:
            return json.dumps(export_data, default=str)
    
    async def export_to_csv(
        self,
        entries: list[AuditEntry],
    ) -> str:
        """Export audit entries to CSV format.
        
        Args:
            entries: Entries to export
            
        Returns:
            CSV string
        """
        # Limit entries
        export_entries = entries[:self.max_export_size]
        
        # Create CSV in memory
        output = io.StringIO()
        
        # Define fields
        fields = [
            "id", "timestamp", "action", "level", "component",
            "user_id", "session_id", "correlation_id", "tags"
        ]
        
        if self.include_metadata_in_csv:
            fields.extend(["details", "metadata"])
        
        # Create CSV writer
        writer = csv.DictWriter(
            output,
            fieldnames=fields,
            delimiter=self.csv_delimiter
        )
        
        # Write header
        writer.writeheader()
        
        # Write entries
        for entry in export_entries:
            row = {
                "id": entry.id,
                "timestamp": entry.timestamp,
                "action": entry.action,
                "level": entry.level,
                "component": entry.component,
                "user_id": entry.user_id or "",
                "session_id": entry.session_id or "",
                "correlation_id": entry.correlation_id or "",
                "tags": ";".join(entry.tags),
            }
            
            if self.include_metadata_in_csv:
                row["details"] = json.dumps(entry.details, default=str)
                row["metadata"] = json.dumps(entry.metadata, default=str)
            
            writer.writerow(row)
        
        return output.getvalue()
    
    async def import_from_json(
        self,
        json_data: str,
    ) -> list[AuditEntry]:
        """Import audit entries from JSON format.
        
        Args:
            json_data: JSON string containing audit data
            
        Returns:
            List of imported audit entries
        """
        try:
            data = json.loads(json_data)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON", error=str(e))
            raise ValueError(f"Invalid JSON format: {e}")
        
        # Extract audit trail
        if isinstance(data, dict) and "audit_trail" in data:
            entries_data = data["audit_trail"]
        elif isinstance(data, list):
            entries_data = data
        else:
            raise ValueError("Invalid audit data format")
        
        # Parse entries
        entries = []
        for entry_data in entries_data[:self.max_export_size]:
            try:
                entry = AuditEntry.model_validate(entry_data)
                entries.append(entry)
            except Exception as e:
                logger.warning(
                    "Failed to import entry",
                    entry_id=entry_data.get("id"),
                    error=str(e)
                )
                continue
        
        logger.info(f"Imported {len(entries)} audit entries")
        return entries
    
    async def import_from_csv(
        self,
        csv_data: str,
    ) -> list[AuditEntry]:
        """Import audit entries from CSV format.
        
        Args:
            csv_data: CSV string containing audit data
            
        Returns:
            List of imported audit entries
        """
        # Create CSV reader
        input_stream = io.StringIO(csv_data)
        reader = csv.DictReader(input_stream, delimiter=self.csv_delimiter)
        
        entries = []
        for row in reader:
            if len(entries) >= self.max_export_size:
                break
            
            try:
                # Parse basic fields
                entry_data = {
                    "id": row["id"],
                    "timestamp": float(row["timestamp"]),
                    "action": row["action"],
                    "level": row["level"],
                    "component": row["component"],
                    "user_id": row["user_id"] or None,
                    "session_id": row["session_id"] or None,
                    "correlation_id": row["correlation_id"] or None,
                    "tags": row["tags"].split(";") if row["tags"] else [],
                }
                
                # Parse JSON fields if present
                if "details" in row and row["details"]:
                    entry_data["details"] = json.loads(row["details"])
                else:
                    entry_data["details"] = {}
                
                if "metadata" in row and row["metadata"]:
                    entry_data["metadata"] = json.loads(row["metadata"])
                else:
                    entry_data["metadata"] = {}
                
                # Create entry
                entry = AuditEntry.model_validate(entry_data)
                entries.append(entry)
                
            except Exception as e:
                logger.warning(
                    "Failed to import CSV row",
                    row_id=row.get("id"),
                    error=str(e)
                )
                continue
        
        logger.info(f"Imported {len(entries)} audit entries from CSV")
        return entries
    
    async def export_summary(
        self,
        entries: list[AuditEntry],
        format: str = "json",
    ) -> str:
        """Export a summary of audit entries.
        
        Args:
            entries: Entries to summarize
            format: Export format (json or text)
            
        Returns:
            Summary string
        """
        # Calculate summary statistics
        total = len(entries)
        if total == 0:
            return "{}" if format == "json" else "No audit entries"
        
        # Count by level
        level_counts = {}
        component_counts = {}
        
        for entry in entries:
            level_counts[entry.level] = level_counts.get(entry.level, 0) + 1
            component_counts[entry.component] = component_counts.get(entry.component, 0) + 1
        
        # Get time range
        timestamps = [e.timestamp for e in entries]
        min_time = min(timestamps)
        max_time = max(timestamps)
        
        summary = {
            "total_entries": total,
            "time_range": {
                "start": min_time,
                "end": max_time,
                "duration_hours": (max_time - min_time) / 3600,
            },
            "levels": level_counts,
            "top_components": dict(
                sorted(component_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
        }
        
        if format == "json":
            return json.dumps(summary, indent=2, default=str)
        else:
            # Text format
            lines = [
                f"Total Entries: {total}",
                f"Time Range: {summary['time_range']['duration_hours']:.1f} hours",
                "\nLevels:",
            ]
            for level, count in level_counts.items():
                lines.append(f"  {level}: {count}")
            
            lines.append("\nTop Components:")
            for comp, count in summary["top_components"].items():
                lines.append(f"  {comp}: {count}")
            
            return "\n".join(lines)