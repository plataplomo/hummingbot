"""Migration progress tracker for WebSocket error system.

Tracks the progress of migrating from the old APIError-based system
to the new WebSocketStreamError system, providing visibility into
component adoption and migration status.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

from cyberdelta.apis.websocket.ws_dual_error_manager import ErrorSystemMode


logger = logging.getLogger(__name__)


class ComponentStatus(Enum):
    """Status of a component in the migration."""

    NOT_STARTED = "not_started"  # Migration not begun
    IN_PROGRESS = "in_progress"  # Actively migrating
    TESTING = "testing"  # Testing new system
    DUAL_MODE = "dual_mode"  # Running both systems
    MIGRATED = "migrated"  # Fully migrated
    VERIFIED = "verified"  # Migrated and verified


class MigrationPhase(Enum):
    """Phase of the overall migration."""

    PLANNING = "planning"  # Planning phase
    FOUNDATION = "foundation"  # Building foundation (Phase 1)
    INTEGRATION = "integration"  # Core integration (Phase 2)
    TESTING = "testing"  # Testing & performance (Phase 3)
    MIGRATION = "migration"  # Active migration (Phase 4)
    CLEANUP = "cleanup"  # Cleanup phase
    COMPLETE = "complete"  # Migration complete


@dataclass
class ComponentMigration:
    """Track migration status of a single component."""

    name: str
    status: ComponentStatus = ComponentStatus.NOT_STARTED
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_mode: ErrorSystemMode | None = None
    compatibility_rate: float = 100.0
    errors_handled: int = 0
    issues: list[str] = field(default_factory=lambda: list[str]())
    notes: str = ""

    def start_migration(self) -> None:
        """Mark component migration as started."""
        self.status = ComponentStatus.IN_PROGRESS
        self.started_at = datetime.now()

    def complete_migration(self) -> None:
        """Mark component migration as complete."""
        self.status = ComponentStatus.MIGRATED
        self.completed_at = datetime.now()

    def get_duration(self) -> timedelta | None:
        """Get migration duration."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        if self.started_at:
            return datetime.now() - self.started_at
        return None

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary for serialization."""
        data = {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_mode": self.error_mode.value if self.error_mode else None,
            "compatibility_rate": self.compatibility_rate,
            "errors_handled": self.errors_handled,
            "issues": self.issues,
            "notes": self.notes,
        }
        return data


@dataclass
class MigrationCheckpoint:
    """Checkpoint in the migration process."""

    timestamp: datetime = field(default_factory=datetime.now)
    phase: MigrationPhase = MigrationPhase.PLANNING
    components_total: int = 0
    components_migrated: int = 0
    overall_compatibility: float = 100.0
    critical_issues: list[str] = field(default_factory=lambda: list[str]())
    notes: str = ""

    def get_progress_percentage(self) -> float:
        """Get migration progress as percentage."""
        if self.components_total == 0:
            return 0.0
        return (self.components_migrated / self.components_total) * 100


class WebSocketMigrationTracker:
    """Tracks WebSocket error system migration progress."""

    # Core components to track
    CORE_COMPONENTS = [
        "ws_processor",
        "ws_router",
        "ws_error_handler",
        "ws_error_recovery",
        "ws_connection_manager",
        "ws_subscription_manager",
        "ws_state_manager",
        "ws_message_parser",
        "ws_event_publisher",
        "ws_metrics_collector",
    ]

    # Exchange-specific components
    EXCHANGE_COMPONENTS = {
        "hyperliquid": [
            "hl_websocket_client",
            "hl_message_processor",
            "hl_subscription_handler",
        ],
        "backpack": [
            "bp_websocket_client",
            "bp_message_processor",
            "bp_subscription_handler",
        ],
    }

    def __init__(
        self,
        state_file: Path | None = None,
        auto_save: bool = True,
        save_interval_minutes: int = 30,
    ):
        """Initialize migration tracker.

        Args:
            state_file: Path to save migration state
            auto_save: Automatically save state periodically
            save_interval_minutes: Minutes between auto-saves
        """
        self.state_file = state_file
        self.auto_save = auto_save
        self.save_interval_minutes = save_interval_minutes

        self.current_phase = MigrationPhase.PLANNING
        self.components: dict[str, ComponentMigration] = {}
        self.checkpoints: list[MigrationCheckpoint] = []
        self.started_at = datetime.now()
        self.last_save = datetime.now()

        # Initialize components
        self._initialize_components()

        # Load existing state if available
        if self.state_file and self.state_file.exists():
            self.load_state()

    def _initialize_components(self) -> None:
        """Initialize component tracking."""
        # Add core components
        for component in self.CORE_COMPONENTS:
            self.components[component] = ComponentMigration(name=component)

        # Add exchange-specific components
        for _exchange, components in self.EXCHANGE_COMPONENTS.items():
            for component in components:
                self.components[component] = ComponentMigration(name=component)

    def update_component(
        self,
        component_name: str,
        status: ComponentStatus | None = None,
        error_mode: ErrorSystemMode | None = None,
        compatibility_rate: float | None = None,
        errors_handled: int | None = None,
        issue: str | None = None,
        notes: str | None = None,
    ) -> None:
        """Update component migration status.

        Args:
            component_name: Name of the component
            status: New status
            error_mode: Current error system mode
            compatibility_rate: Compatibility percentage
            errors_handled: Number of errors handled
            issue: Issue to add
            notes: Notes to set
        """
        if component_name not in self.components:
            self.components[component_name] = ComponentMigration(name=component_name)

        component = self.components[component_name]

        if status:
            old_status = component.status
            component.status = status

            # Handle status transitions
            if old_status == ComponentStatus.NOT_STARTED and status == ComponentStatus.IN_PROGRESS:
                component.start_migration()
            elif status == ComponentStatus.MIGRATED and not component.completed_at:
                component.complete_migration()

        if error_mode is not None:
            component.error_mode = error_mode

        if compatibility_rate is not None:
            component.compatibility_rate = compatibility_rate

        if errors_handled is not None:
            component.errors_handled = errors_handled

        if issue:
            component.issues.append(f"{datetime.now().isoformat()}: {issue}")

        if notes is not None:
            component.notes = notes

        # Auto-save if enabled
        if self.auto_save:
            self._check_auto_save()

    def set_phase(self, phase: MigrationPhase) -> None:
        """Set current migration phase.

        Args:
            phase: New migration phase
        """
        old_phase = self.current_phase
        self.current_phase = phase

        logger.info(f"Migration phase changed from {old_phase.value} to {phase.value}")

        # Create checkpoint
        self.create_checkpoint()

    def create_checkpoint(self, notes: str = "") -> MigrationCheckpoint:
        """Create a migration checkpoint.

        Args:
            notes: Optional notes for the checkpoint

        Returns:
            Created checkpoint
        """
        # Calculate statistics
        total = len(self.components)
        migrated = sum(
            1
            for c in self.components.values()
            if c.status in [ComponentStatus.MIGRATED, ComponentStatus.VERIFIED]
        )

        # Calculate overall compatibility
        compatibility_rates = [
            c.compatibility_rate for c in self.components.values() if c.errors_handled > 0
        ]
        overall_compatibility = (
            sum(compatibility_rates) / len(compatibility_rates) if compatibility_rates else 100.0
        )

        # Collect critical issues
        critical_issues: list[str] = []
        for component in self.components.values():
            if component.issues:
                critical_issues.extend([
                    f"{component.name}: {issue}"
                    for issue in component.issues[-3:]  # Last 3 issues
                ])

        checkpoint = MigrationCheckpoint(
            phase=self.current_phase,
            components_total=total,
            components_migrated=migrated,
            overall_compatibility=overall_compatibility,
            critical_issues=critical_issues,
            notes=notes,
        )

        self.checkpoints.append(checkpoint)

        # Auto-save if enabled
        if self.auto_save:
            self.save_state()

        return checkpoint

    def get_status_summary(self) -> dict[str, object]:
        """Get migration status summary.

        Returns:
            Dictionary with status summary
        """
        # Component statistics
        status_counts: dict[str, int] = {}
        for status in ComponentStatus:
            count = sum(1 for c in self.components.values() if c.status == status)
            status_counts[status.value] = count

        # Calculate progress
        total = len(self.components)
        migrated: int = status_counts.get(ComponentStatus.MIGRATED.value, 0)
        migrated += status_counts.get(ComponentStatus.VERIFIED.value, 0)
        progress = (migrated / total * 100) if total > 0 else 0

        # Get active components
        active_components = [
            c.name
            for c in self.components.values()
            if c.status in [ComponentStatus.IN_PROGRESS, ComponentStatus.TESTING]
        ]

        # Get recent issues
        recent_issues: list[dict[str, str]] = []
        for component in self.components.values():
            if component.issues:
                recent_issues.append({
                    "component": component.name,
                    "issue": component.issues[-1],
                })

        # Calculate time statistics
        duration = datetime.now() - self.started_at

        return {
            "current_phase": self.current_phase.value,
            "overall_progress": f"{progress:.1f}%",
            "components": {
                "total": total,
                "migrated": migrated,
                "by_status": status_counts,
            },
            "active_components": active_components,
            "recent_issues": recent_issues[-5:],  # Last 5 issues
            "duration": {
                "days": duration.days,
                "hours": duration.seconds // 3600,
            },
            "checkpoints": len(self.checkpoints),
            "last_checkpoint": (
                self.checkpoints[-1].timestamp.isoformat() if self.checkpoints else None
            ),
        }

    def get_component_report(self, component_name: str) -> dict[str, object] | None:
        """Get detailed report for a component.

        Args:
            component_name: Name of the component

        Returns:
            Component report or None if not found
        """
        if component_name not in self.components:
            return None

        component = self.components[component_name]
        duration = component.get_duration()

        return {
            "name": component.name,
            "status": component.status.value,
            "started_at": (component.started_at.isoformat() if component.started_at else None),
            "completed_at": (
                component.completed_at.isoformat() if component.completed_at else None
            ),
            "duration": (f"{duration.days}d {duration.seconds // 3600}h" if duration else None),
            "error_mode": (component.error_mode.value if component.error_mode else None),
            "compatibility_rate": f"{component.compatibility_rate:.2f}%",
            "errors_handled": component.errors_handled,
            "issues": component.issues,
            "notes": component.notes,
        }

    def get_phase_report(self) -> dict[str, object]:
        """Get report for current phase.

        Returns:
            Phase report
        """
        # Get components relevant to current phase
        phase_components = self._get_phase_components()

        # Calculate phase progress
        total = len(phase_components)
        completed = sum(
            1
            for name in phase_components
            if self.components[name].status
            in [
                ComponentStatus.MIGRATED,
                ComponentStatus.VERIFIED,
            ]
        )

        progress = (completed / total * 100) if total > 0 else 0

        return {
            "phase": self.current_phase.value,
            "progress": f"{progress:.1f}%",
            "components": {
                "total": total,
                "completed": completed,
                "remaining": total - completed,
            },
            "component_list": phase_components,
        }

    def _get_phase_components(self) -> list[str]:
        """Get components relevant to current phase.

        Returns:
            List of component names
        """
        # Map phases to components
        if self.current_phase == MigrationPhase.FOUNDATION:
            return ["ws_error_handler", "ws_error_recovery"]
        if self.current_phase == MigrationPhase.INTEGRATION:
            return ["ws_processor", "ws_router", "ws_connection_manager"]
        if self.current_phase == MigrationPhase.TESTING:
            return self.CORE_COMPONENTS
        return list(self.components.keys())

    def recommend_next_component(self) -> str | None:
        """Recommend next component to migrate.

        Returns:
            Component name or None if all migrated
        """
        # Priority order for migration
        priority_order = [
            "ws_error_handler",
            "ws_error_recovery",
            "ws_processor",
            "ws_router",
            "ws_connection_manager",
            "ws_subscription_manager",
            "ws_state_manager",
            "ws_message_parser",
            "ws_event_publisher",
            "ws_metrics_collector",
        ]

        # Find first non-migrated component in priority order
        for component_name in priority_order:
            if component_name in self.components:
                component = self.components[component_name]
                if component.status in [ComponentStatus.NOT_STARTED, ComponentStatus.IN_PROGRESS]:
                    return component_name

        # Check other components
        for component in self.components.values():
            if component.status in [ComponentStatus.NOT_STARTED, ComponentStatus.IN_PROGRESS]:
                return component.name

        return None

    def save_state(self, file_path: Path | None = None) -> None:
        """Save migration state to file.

        Args:
            file_path: Optional path to save to
        """
        save_path = file_path or self.state_file
        if not save_path:
            return

        state = {
            "version": "1.0",
            "started_at": self.started_at.isoformat(),
            "current_phase": self.current_phase.value,
            "components": {name: comp.to_dict() for name, comp in self.components.items()},
            "checkpoints": [
                {
                    "timestamp": cp.timestamp.isoformat(),
                    "phase": cp.phase.value,
                    "components_total": cp.components_total,
                    "components_migrated": cp.components_migrated,
                    "overall_compatibility": cp.overall_compatibility,
                    "critical_issues": cp.critical_issues,
                    "notes": cp.notes,
                }
                for cp in self.checkpoints
            ],
        }

        save_path.write_text(json.dumps(state, indent=2))
        self.last_save = datetime.now()
        logger.debug(f"Migration state saved to {save_path}")

    def load_state(self, file_path: Path | None = None) -> None:
        """Load migration state from file.

        Args:
            file_path: Optional path to load from
        """
        load_path = file_path or self.state_file
        if not load_path or not load_path.exists():
            return

        try:
            state = json.loads(load_path.read_text())

            self.started_at = datetime.fromisoformat(state["started_at"])
            self.current_phase = MigrationPhase(state["current_phase"])

            # Load components
            for name, comp_data in state.get("components", {}).items():
                component = ComponentMigration(name=name)
                component.status = ComponentStatus(comp_data["status"])

                if comp_data.get("started_at"):
                    component.started_at = datetime.fromisoformat(comp_data["started_at"])
                if comp_data.get("completed_at"):
                    component.completed_at = datetime.fromisoformat(comp_data["completed_at"])
                if comp_data.get("error_mode"):
                    component.error_mode = ErrorSystemMode(comp_data["error_mode"])

                component.compatibility_rate = comp_data.get("compatibility_rate", 100.0)
                component.errors_handled = comp_data.get("errors_handled", 0)
                component.issues = comp_data.get("issues", [])
                component.notes = comp_data.get("notes", "")

                self.components[name] = component

            # Load checkpoints
            for cp_data in state.get("checkpoints", []):
                checkpoint = MigrationCheckpoint(
                    timestamp=datetime.fromisoformat(cp_data["timestamp"]),
                    phase=MigrationPhase(cp_data["phase"]),
                    components_total=cp_data["components_total"],
                    components_migrated=cp_data["components_migrated"],
                    overall_compatibility=cp_data["overall_compatibility"],
                    critical_issues=cp_data.get("critical_issues", []),
                    notes=cp_data.get("notes", ""),
                )
                self.checkpoints.append(checkpoint)

            logger.info(f"Migration state loaded from {load_path}")

        except Exception as e:
            logger.error(f"Failed to load migration state: {e}")

    def _check_auto_save(self) -> None:
        """Check if auto-save is needed."""
        if not self.auto_save or not self.state_file:
            return

        time_since_save = datetime.now() - self.last_save
        if time_since_save > timedelta(minutes=self.save_interval_minutes):
            self.save_state()

    def export_report(self, file_path: Path) -> None:
        """Export full migration report.

        Args:
            file_path: Path to export report to
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": self.get_status_summary(),
            "phase_report": self.get_phase_report(),
            "components": {
                name: self.get_component_report(name) for name in self.components.keys()
            },
            "checkpoints": [
                {
                    "timestamp": cp.timestamp.isoformat(),
                    "phase": cp.phase.value,
                    "progress": f"{cp.get_progress_percentage():.1f}%",
                    "compatibility": f"{cp.overall_compatibility:.2f}%",
                    "issues": len(cp.critical_issues),
                    "notes": cp.notes,
                }
                for cp in self.checkpoints
            ],
        }

        file_path.write_text(json.dumps(report, indent=2))
        logger.info(f"Migration report exported to {file_path}")


# Global tracker instance
_global_tracker: WebSocketMigrationTracker | None = None


def get_migration_tracker() -> WebSocketMigrationTracker:
    """Get or create global migration tracker.

    Returns:
        Global migration tracker instance
    """
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = WebSocketMigrationTracker()
    return _global_tracker


def update_component_status(
    component_name: str,
    status: ComponentStatus,
    **kwargs: object,
) -> None:
    """Update component status in global tracker.

    Args:
        component_name: Name of the component
        status: New status
        **kwargs: Additional update parameters
    """
    tracker = get_migration_tracker()
    tracker.update_component(component_name, status=status, **kwargs)
