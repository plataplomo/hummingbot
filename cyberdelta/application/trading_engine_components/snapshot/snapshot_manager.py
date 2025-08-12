"""Snapshot manager for trading engine portfolio state snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager


logger = get_logger(__name__)


class SnapshotManager:
    """Manages portfolio snapshot operations."""

    def __init__(
        self,
        config: AppSettings,
        circuit_breakers: CircuitBreakerManager,
        portfolio_service: PortfolioService,
    ) -> None:
        """Initialize snapshot manager with required services.

        Args:
            config: Application settings
            circuit_breakers: Circuit breaker manager
            portfolio_service: Portfolio service
        """
        self.config = config
        self._circuit_breakers = circuit_breakers
        self._portfolio_service = portfolio_service

    async def create_manual_snapshot(self, snapshot_name: str | None = None) -> str:
        """Create a manual portfolio snapshot.

        Args:
            snapshot_name: Optional custom name for snapshot

        Returns:
            Name of created snapshot

        IMPORTANT: Following CODING_STANDARDS.md:
        - Snapshot naming based on timestamp if not provided
        - Circuit breaker protection
        - Explicit error handling
        """
        if snapshot_name is None:
            # Generate timestamp-based name
            snapshot_name = f"manual_snapshot_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

        try:
            await self._circuit_breakers.protect(
                "portfolio_service", "create_snapshot", self._portfolio_service.create_snapshot
            )

            logger.info(
                "manual_snapshot_created",
                snapshot_name=snapshot_name,
                requested_by="trading_engine",
            )

        except Exception as e:
            logger.exception("manual_snapshot_failed", snapshot_name=snapshot_name, error=str(e))
            raise
        else:
            return snapshot_name

    async def list_snapshots(self) -> list[str]:
        """List all available portfolio snapshots.

        Returns:
            List of snapshot names/identifiers
        """
        try:
            snapshots = await self._portfolio_service.list_snapshots()

            logger.info(
                "snapshots_listed",
                snapshot_count=len(snapshots),
            )

        except Exception as e:
            logger.exception("list_snapshots_failed", error=str(e))
            raise
        else:
            return snapshots

    async def delete_snapshot(self, snapshot_name: str) -> bool:
        """Delete a specific snapshot.

        Args:
            snapshot_name: Name of snapshot to delete.

        Returns:
            True if deletion was successful
        """
        try:
            await self._portfolio_service.delete_snapshot(snapshot_name)

            logger.info(
                "snapshot_deleted",
                snapshot_name=snapshot_name,
            )

        except Exception as e:
            logger.exception("delete_snapshot_failed", snapshot_name=snapshot_name, error=str(e))
            raise
        else:
            return True

    def get_snapshot_status(self) -> dict[str, Any]:
        """Get snapshot system status.

        Returns:
            Dictionary with snapshot configuration and status

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured status information
        - Configuration context included
        """
        snapshot_interval = float(self.config.general.state_save_interval)

        return {
            "snapshot_enabled": snapshot_interval > 0,
            "snapshot_interval_seconds": snapshot_interval,
            "backup_directory": str(Path(self.config.general.state_backup_directory)),
            "backup_count": self.config.general.state_backup_count,
            "storage_type": "portfolio_storage",
        }
