"""Risk manager state persistence and management."""

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psutil

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.base_exceptions import RiskConfigError
from cyberdelta.core.risk.orchestrator.risk_manager_orchestrator import ProcessingStatus
from cyberdelta.core.risk.persistence.state_models import (
    CheckResult,
    CheckStatus,
    ConstraintResult,
    PerformanceMetrics,
    RiskManagerState,
    SizingResult,
    StateSnapshot,
)


# State management constants
MAX_RECENT_CHECK_RESULTS = 100  # Maximum number of recent check results to keep
MAX_RECENT_SIZING_RESULTS = 50  # Maximum number of recent sizing results to keep
MAX_RECENT_CONSTRAINT_RESULTS = 50  # Maximum number of recent constraint results to keep
MAX_RECENT_ERRORS = 50  # Maximum number of recent errors to keep
MAX_PROCESSING_TIME_HISTORY = 1000  # Maximum processing time history entries to keep
MAX_SNAPSHOTS = 1000  # Maximum number of snapshots to keep
RECENT_RESULTS_SAMPLE_SIZE = 20  # Sample size for constraint violation rate calculation


class RiskManagerStateManager:
    """Manages persistence and retrieval of risk manager state.

    Provides functionality for:
    - Persistent state storage (SQLite, JSON files, memory)
    - State snapshots for debugging and analysis
    - Performance metrics tracking
    - Historical data analysis
    - State recovery and restoration
    """

    def __init__(
        self,
        storage_path: str | Path | None = None,
        storage_type: str = "sqlite",
        max_history_days: int = 30,
        snapshot_interval_seconds: int = 60,
        enable_performance_tracking: bool = True,
    ) -> None:
        """Initialize the state manager.

        Args:
            storage_path: Path for persistent storage
            storage_type: Type of storage (sqlite, json, memory)
            max_history_days: Maximum days to keep historical data
            snapshot_interval_seconds: Interval for automatic snapshots
            enable_performance_tracking: Enable performance metrics collection
        """
        self.logger = get_logger(self.__class__.__name__)
        self.storage_type = storage_type
        self.max_history_days = max_history_days
        self.snapshot_interval = snapshot_interval_seconds
        self.enable_performance_tracking = enable_performance_tracking

        # Setup storage
        if storage_path:
            self.storage_path = Path(storage_path)
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            self.storage_path = Path("risk_manager_state.db")

        # Initialize storage backend
        self._init_storage()

        # Runtime state
        self.current_state: RiskManagerState | None = None
        self.snapshots: list[StateSnapshot] = []
        self.session_start_time = datetime.now(UTC)

        # Performance tracking
        self.processing_times: list[float] = []
        self.error_counts: dict[str, int] = {}

        self.logger.info(
            "Initialized state manager",
            storage_type=storage_type,
            storage_path=str(self.storage_path),
        )

    def _init_storage(self) -> None:
        """Initialize the storage backend.
        
        Raises:
            RiskConfigError: If unsupported storage type is specified
        """
        if self.storage_type == "sqlite":
            self._init_sqlite()
        elif self.storage_type == "json":
            self._init_json()
        elif self.storage_type == "memory":
            self._init_memory()
        else:
            raise RiskConfigError(
                RiskConfigError.UNSUPPORTED_STORAGE_TYPE,
                config_field="storage_type",
                config_value=self.storage_type,
            )

    def _init_sqlite(self) -> None:
        """Initialize SQLite storage."""
        self.db_path = self.storage_path

        # Create tables
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS risk_manager_states (
                    session_id TEXT PRIMARY KEY,
                    configuration_hash TEXT,
                    start_time TEXT,
                    last_update TEXT,
                    status TEXT,
                    state_data TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS check_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    checker_name TEXT,
                    status TEXT,
                    message TEXT,
                    execution_time_ms REAL,
                    timestamp TEXT,
                    metadata TEXT,
                    FOREIGN KEY (session_id) REFERENCES risk_manager_states (session_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS sizing_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    strategy_name TEXT,
                    recommended_size TEXT,
                    risk_adjusted_size TEXT,
                    allocation_percentage REAL,
                    timestamp TEXT,
                    metadata TEXT,
                    FOREIGN KEY (session_id) REFERENCES risk_manager_states (session_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    timestamp TEXT,
                    metrics_data TEXT,
                    FOREIGN KEY (session_id) REFERENCES risk_manager_states (session_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS state_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    opportunity_id TEXT,
                    timestamp TEXT,
                    snapshot_data TEXT,
                    FOREIGN KEY (session_id) REFERENCES risk_manager_states (session_id)
                )
            """)

            # Create indexes for performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_check_results_session ON check_results(session_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_check_results_timestamp ON check_results(timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshots_session ON state_snapshots(session_id)"
            )

            conn.commit()

    def _init_json(self) -> None:
        """Initialize JSON file storage."""
        self.state_file = self.storage_path.with_suffix(".json")
        if not self.state_file.exists():
            self.state_file.write_text("{}")

    def _init_memory(self) -> None:
        """Initialize in-memory storage."""
        self.memory_store: dict[str, Any] = {
            "states": {},
            "check_results": [],
            "sizing_results": [],
            "snapshots": [],
            "metrics": [],
        }

    def create_session(
        self,
        configuration: AppSettings,
        session_id: str | None = None,
    ) -> str:
        """Create a new risk manager session.

        Args:
            configuration: Risk manager configuration
            session_id: Optional session ID (generated if not provided)

        Returns:
            Session ID
        """
        if not session_id:
            session_id = f"session_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

        # Create configuration hash for tracking changes
        config_dict = configuration.model_dump()
        config_str = json.dumps(config_dict, sort_keys=True, default=str)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()

        # Initialize performance metrics
        performance_metrics = PerformanceMetrics(
            total_opportunities_processed=0,
            successful_processing=0,
            failed_processing=0,
            average_processing_time_ms=0.0,
            max_processing_time_ms=0.0,
            min_processing_time_ms=float("inf"),
            check_success_rate=1.0,
            sizing_accuracy_score=1.0,
            constraint_violation_rate=0.0,
            uptime_percentage=100.0,
            memory_usage_mb=0.0,
            cpu_usage_percentage=0.0,
            timestamp=datetime.now(tz=UTC),
        )

        # Create initial state
        self.current_state = RiskManagerState(
            session_id=session_id,
            configuration_hash=config_hash,
            start_time=datetime.now(tz=UTC),
            last_update=datetime.now(tz=UTC),
            status=ProcessingStatus.PENDING,
            active_checks={},
            recent_check_results=[],
            recent_sizing_results=[],
            recent_constraint_results=[],
            performance_metrics=performance_metrics,
            configuration=config_dict,
            recent_errors=[],
            error_counts={},
            circuit_breaker_states={},
        )

        # Persist initial state
        self._save_state(self.current_state)

        self.logger.info("Created new session", session_id=session_id)
        return session_id

    def update_check_result(self, result: CheckResult) -> None:
        """Update state with check result."""
        if not self.current_state:
            self.logger.warning("No active session for check result update")
            return

        # Update active checks
        self.current_state.active_checks[result.checker_name] = result

        # Add to recent results (keep last 100)
        self.current_state.recent_check_results.append(result)
        if len(self.current_state.recent_check_results) > MAX_RECENT_CHECK_RESULTS:
            self.current_state.recent_check_results.pop(0)

        # Update performance metrics
        if result.status == CheckStatus.PASSED:
            self._update_success_metrics()
        else:
            self._update_failure_metrics(result.checker_name, result.message)

        self.current_state.last_update = datetime.now(tz=UTC)
        self._save_check_result(result)

    def update_sizing_result(self, result: SizingResult) -> None:
        """Update state with sizing result."""
        if not self.current_state:
            self.logger.warning("No active session for sizing result update")
            return

        # Add to recent results (keep last 50)
        self.current_state.recent_sizing_results.append(result)
        if len(self.current_state.recent_sizing_results) > MAX_RECENT_SIZING_RESULTS:
            self.current_state.recent_sizing_results.pop(0)

        self.current_state.last_update = datetime.now(tz=UTC)
        self._save_sizing_result(result)

    def update_constraint_result(self, result: ConstraintResult) -> None:
        """Update state with constraint result."""
        if not self.current_state:
            self.logger.warning("No active session for constraint result update")
            return

        # Add to recent results (keep last 50)
        self.current_state.recent_constraint_results.append(result)
        if len(self.current_state.recent_constraint_results) > MAX_RECENT_CONSTRAINT_RESULTS:
            self.current_state.recent_constraint_results.pop(0)

        # Update constraint violation rate
        if not result.is_valid:
            self._update_constraint_violation_metrics()

        self.current_state.last_update = datetime.now(tz=UTC)
        self._save_constraint_result(result)

    def update_performance_metrics(self, processing_time_ms: float) -> None:
        """Update performance metrics with processing time."""
        if not self.current_state or not self.enable_performance_tracking:
            return

        metrics = self.current_state.performance_metrics

        # Update processing counts
        metrics.total_opportunities_processed += 1

        # Update timing metrics
        self.processing_times.append(processing_time_ms)
        if len(self.processing_times) > MAX_PROCESSING_TIME_HISTORY:  # Keep last 1000
            self.processing_times.pop(0)

        metrics.average_processing_time_ms = sum(self.processing_times) / len(self.processing_times)
        metrics.max_processing_time_ms = max(metrics.max_processing_time_ms, processing_time_ms)
        metrics.min_processing_time_ms = min(metrics.min_processing_time_ms, processing_time_ms)

        # Update system metrics
        try:
            process = psutil.Process()
            metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            metrics.cpu_usage_percentage = process.cpu_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError) as e:
            self.logger.debug("Failed to update system metrics", error=str(e))

        # Update timestamp
        metrics.timestamp = datetime.now(tz=UTC)

        self.current_state.last_update = datetime.now(tz=UTC)
        self._save_performance_metrics(metrics)

    def create_snapshot(
        self,
        opportunity_id: str,
        processing_stage: str,
        check_results: list[CheckResult],
        sizing_result: SizingResult | None = None,
        constraint_result: ConstraintResult | None = None,
        final_decision: dict[str, Any] | None = None,
        execution_metrics: dict[str, float] | None = None,
    ) -> StateSnapshot:
        """Create a point-in-time snapshot.
        
        Returns:
            StateSnapshot containing current processing state
        """
        snapshot = StateSnapshot(
            timestamp=datetime.now(tz=UTC),
            opportunity_id=opportunity_id,
            processing_stage=processing_stage,
            check_results=check_results,
            sizing_result=sizing_result,
            constraint_result=constraint_result,
            final_decision=final_decision or {},
            execution_metrics=execution_metrics or {},
        )

        self.snapshots.append(snapshot)
        if len(self.snapshots) > MAX_SNAPSHOTS:  # Keep last 1000 snapshots
            self.snapshots.pop(0)

        self._save_snapshot(snapshot)
        return snapshot

    def get_current_state(self) -> RiskManagerState | None:
        """Get the current risk manager state.
        
        Returns:
            Current state if active session exists, None otherwise
        """
        return self.current_state

    def get_session_history(self, session_id: str) -> RiskManagerState | None:
        """Get historical state for a specific session.
        
        Returns:
            Historical state data for the session, None if not found
        """
        return self._load_state(session_id)

    def get_check_results_history(
        self,
        session_id: str | None = None,
        checker_name: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[CheckResult]:
        """Get historical check results.
        
        Returns:
            List of check results matching the specified criteria
        """
        return self._load_check_results(session_id, checker_name, since, limit)

    def get_performance_summary(
        self,
        session_id: str | None = None,
        since: datetime | None = None,
    ) -> dict[str, Any]:
        """Get performance summary statistics.
        
        Returns:
            Dictionary containing performance metrics and statistics
        """
        if not session_id and self.current_state:
            session_id = self.current_state.session_id

        if not session_id:
            return {}

        # Get check results for analysis
        check_results = self.get_check_results_history(session_id=session_id, since=since)

        # Calculate summary statistics
        total_checks = len(check_results)
        passed_checks = sum(1 for r in check_results if r.status == CheckStatus.PASSED)
        failed_checks = total_checks - passed_checks

        avg_execution_time = sum(r.execution_time_ms for r in check_results) / max(total_checks, 1)

        # Checker-specific statistics
        checker_stats: dict[str, dict[str, float]] = {}
        for result in check_results:
            name = result.checker_name
            if name not in checker_stats:
                checker_stats[name] = {"total": 0, "passed": 0, "avg_time": 0.0}

            checker_stats[name]["total"] += 1
            if result.status == CheckStatus.PASSED:
                checker_stats[name]["passed"] += 1
            checker_stats[name]["avg_time"] += result.execution_time_ms

        # Calculate averages
        for stats in checker_stats.values():
            if stats["total"] > 0:
                stats["success_rate"] = stats["passed"] / stats["total"]
                stats["avg_time"] /= stats["total"]

        return {
            "session_id": session_id,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "success_rate": passed_checks / max(total_checks, 1),
            "average_execution_time_ms": avg_execution_time,
            "checker_statistics": checker_stats,
            "current_performance_metrics": (
                self.current_state.performance_metrics.to_dict() if self.current_state else None
            ),
        }

    def cleanup_old_data(self, before_date: datetime | None = None) -> int:
        """Clean up old historical data.
        
        Returns:
            Number of records deleted
        """
        if not before_date:
            before_date = datetime.now(tz=UTC) - timedelta(days=self.max_history_days)

        deleted_count = 0

        if self.storage_type == "sqlite":
            deleted_count = self._cleanup_sqlite_data(before_date)
        elif self.storage_type == "json":
            deleted_count = self._cleanup_json_data(before_date)
        elif self.storage_type == "memory":
            deleted_count = self._cleanup_memory_data(before_date)

        self.logger.info(
            "Cleaned up old records",
            deleted_count=deleted_count,
            before_date=str(before_date),
        )
        return deleted_count

    def export_state(
        self,
        export_path: str | Path,
        session_id: str | None = None,
        include_snapshots: bool = True,
    ) -> None:
        """Export state data to JSON file.
        
        Raises:
            RiskConfigError: If no session ID is provided and no active session exists
        """
        export_path = Path(export_path)

        if not session_id and self.current_state:
            session_id = self.current_state.session_id

        if not session_id:
            raise RiskConfigError(RiskConfigError.NO_SESSION_ID_PROVIDED)

        # Collect all data for export
        export_data = {
            "session_id": session_id,
            "export_timestamp": datetime.now(tz=UTC).isoformat(),
            "state": (
                loaded_state.to_dict() if (loaded_state := self._load_state(session_id)) else None
            ),
            "check_results": [r.to_dict() for r in self.get_check_results_history(session_id)],
        }

        if include_snapshots:
            export_data["snapshots"] = [s.to_dict() for s in self.snapshots]

        # Write to file
        with Path(export_path).open("w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, default=str)

        self.logger.info("Exported state data", export_path=str(export_path))

    def _update_success_metrics(self) -> None:
        """Update metrics for successful processing."""
        if self.current_state:
            self.current_state.performance_metrics.successful_processing += 1

    def _update_failure_metrics(self, component: str, error_message: str) -> None:
        """Update metrics for failed processing."""
        if not self.current_state:
            return

        self.current_state.performance_metrics.failed_processing += 1

        # Track error counts
        self.current_state.error_counts[component] = (
            self.current_state.error_counts.get(component, 0) + 1
        )

        # Add to recent errors (keep last 50)
        error_record = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "component": component,
            "message": error_message,
        }
        self.current_state.recent_errors.append(error_record)
        if len(self.current_state.recent_errors) > MAX_RECENT_ERRORS:
            self.current_state.recent_errors.pop(0)

    def _update_constraint_violation_metrics(self) -> None:
        """Update metrics for constraint violations."""
        if not self.current_state:
            return

        # Recalculate violation rate based on recent results
        # Last 20 results
        recent_results = self.current_state.recent_constraint_results[-RECENT_RESULTS_SAMPLE_SIZE:]
        if recent_results:
            violations = sum(1 for r in recent_results if not r.is_valid)
            self.current_state.performance_metrics.constraint_violation_rate = violations / len(
                recent_results
            )

    # Storage backend methods

    def _save_state(self, state: RiskManagerState) -> None:
        """Save state to storage backend."""
        if self.storage_type == "sqlite":
            self._save_state_sqlite(state)
        elif self.storage_type == "json":
            self._save_state_json(state)
        elif self.storage_type == "memory":
            self._save_state_memory(state)

    def _save_check_result(self, result: CheckResult) -> None:
        """Save check result to storage backend."""
        if self.storage_type == "sqlite":
            self._save_check_result_sqlite(result)
        elif self.storage_type == "json":
            self._save_check_result_json(result)
        elif self.storage_type == "memory":
            self._save_check_result_memory(result)

    def _save_sizing_result(self, result: SizingResult) -> None:
        """Save sizing result to storage backend."""
        if self.storage_type == "sqlite":
            self._save_sizing_result_sqlite(result)
        # Add other backends as needed

    def _save_constraint_result(self, result: ConstraintResult) -> None:
        """Save constraint result to storage backend."""
        if self.storage_type == "sqlite":
            self._save_constraint_result_sqlite(result)
        # Add other backends as needed

    def _save_performance_metrics(self, metrics: PerformanceMetrics) -> None:
        """Save performance metrics to storage backend."""
        if self.storage_type == "sqlite":
            self._save_performance_metrics_sqlite(metrics)
        # Add other backends as needed

    def _save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save snapshot to storage backend."""
        if self.storage_type == "sqlite":
            self._save_snapshot_sqlite(snapshot)
        # Add other backends as needed

    # SQLite-specific implementations

    def _save_state_sqlite(self, state: RiskManagerState) -> None:
        """Save state to SQLite."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO risk_manager_states
                (session_id, configuration_hash, start_time, last_update, status, state_data)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    state.session_id,
                    state.configuration_hash,
                    state.start_time.isoformat(),
                    state.last_update.isoformat(),
                    state.status.value,
                    json.dumps(state.to_dict(), default=str),
                ),
            )

    def _save_check_result_sqlite(self, result: CheckResult) -> None:
        """Save check result to SQLite."""
        if not self.current_state:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO check_results
                (session_id, checker_name, status, message, execution_time_ms, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.current_state.session_id,
                    result.checker_name,
                    result.status.value,
                    result.message,
                    result.execution_time_ms,
                    result.timestamp.isoformat(),
                    json.dumps(result.metadata, default=str),
                ),
            )

    def _save_sizing_result_sqlite(self, result: SizingResult) -> None:
        """Save sizing result to SQLite."""
        if not self.current_state:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO sizing_results
                (session_id, strategy_name, recommended_size, risk_adjusted_size,
                 allocation_percentage, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.current_state.session_id,
                    result.strategy_name,
                    str(result.recommended_size),
                    str(result.risk_adjusted_size),
                    result.allocation_percentage,
                    result.timestamp.isoformat(),
                    json.dumps(result.metadata, default=str),
                ),
            )

    def _save_constraint_result_sqlite(self, result: ConstraintResult) -> None:
        """Save constraint result to SQLite."""
        # Implementation would go here

    def _save_performance_metrics_sqlite(self, metrics: PerformanceMetrics) -> None:
        """Save performance metrics to SQLite."""
        if not self.current_state:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO performance_metrics
                (session_id, timestamp, metrics_data)
                VALUES (?, ?, ?)
            """,
                (
                    self.current_state.session_id,
                    metrics.timestamp.isoformat(),
                    json.dumps(metrics.to_dict(), default=str),
                ),
            )

    def _save_snapshot_sqlite(self, snapshot: StateSnapshot) -> None:
        """Save snapshot to SQLite."""
        if not self.current_state:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO state_snapshots
                (session_id, opportunity_id, timestamp, snapshot_data)
                VALUES (?, ?, ?, ?)
            """,
                (
                    self.current_state.session_id,
                    snapshot.opportunity_id,
                    snapshot.timestamp.isoformat(),
                    json.dumps(snapshot.to_dict(), default=str),
                ),
            )

    def _load_state(self, session_id: str) -> RiskManagerState | None:
        """Load state from SQLite.
        
        Returns:
            Loaded state data if found, None otherwise
        """
        if self.storage_type != "sqlite":
            return None

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT state_data FROM risk_manager_states WHERE session_id = ?
            """,
                (session_id,),
            )

            row = cursor.fetchone()
            if row:
                state_data = json.loads(row[0])
                return RiskManagerState.from_dict(state_data)

        return None

    def _load_check_results(
        self,
        session_id: str | None,
        checker_name: str | None,
        since: datetime | None,
        limit: int,
    ) -> list[CheckResult]:
        """Load check results from SQLite.
        
        Returns:
            List of check results matching the query criteria
        """
        if self.storage_type != "sqlite":
            return []

        query = (
            "SELECT checker_name, status, message, execution_time_ms, timestamp, metadata "
            "FROM check_results"
        )
        params: list[str] = []
        conditions: list[str] = []

        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)

        if checker_name:
            conditions.append("checker_name = ?")
            params.append(checker_name)

        if since:
            conditions.append("timestamp >= ?")
            params.append(since.isoformat())

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(str(limit))

        results: list[CheckResult] = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            for row in cursor.fetchall():
                result = CheckResult(
                    checker_name=row[0],
                    status=CheckStatus(row[1]),
                    message=row[2],
                    execution_time_ms=row[3],
                    timestamp=datetime.fromisoformat(row[4]),
                    metadata=json.loads(row[5]) if row[5] else {},
                )
                results.append(result)

        return results

    def _cleanup_sqlite_data(self, before_date: datetime) -> int:
        """Clean up old SQLite data.
        
        Returns:
            Number of records deleted from SQLite database
        """
        deleted_count = 0

        with sqlite3.connect(self.db_path) as conn:
            # Clean up old check results
            cursor = conn.execute(
                """
                DELETE FROM check_results WHERE timestamp < ?
            """,
                (before_date.isoformat(),),
            )
            deleted_count += cursor.rowcount

            # Clean up old snapshots
            cursor = conn.execute(
                """
                DELETE FROM state_snapshots WHERE timestamp < ?
            """,
                (before_date.isoformat(),),
            )
            deleted_count += cursor.rowcount

            # Clean up old performance metrics
            cursor = conn.execute(
                """
                DELETE FROM performance_metrics WHERE timestamp < ?
            """,
                (before_date.isoformat(),),
            )
            deleted_count += cursor.rowcount

            conn.commit()

        return deleted_count

    # JSON and memory implementations would be similar
    def _save_state_json(self, state: RiskManagerState) -> None:
        """Save state to JSON file."""
        # Implementation for JSON storage

    def _save_check_result_json(self, result: CheckResult) -> None:
        """Save check result to JSON file."""
        # Implementation for JSON storage

    def _save_state_memory(self, state: RiskManagerState) -> None:
        """Save state to memory."""
        self.memory_store["states"][state.session_id] = state.to_dict()

    def _save_check_result_memory(self, result: CheckResult) -> None:
        """Save check result to memory."""
        self.memory_store["check_results"].append(result.to_dict())

    def _cleanup_json_data(self, before_date: datetime) -> int:
        """Clean up old JSON data.
        
        Returns:
            Number of records deleted from JSON storage
        """
        # Implementation for JSON cleanup
        return 0

    def _cleanup_memory_data(self, before_date: datetime) -> int:
        """Clean up old memory data.
        
        Returns:
            Number of records deleted from memory storage
        """
        # Implementation for memory cleanup
        return 0
