"""Snapshot management component for portfolio analytics."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import List, Optional, Dict, Any

from cyberdelta.core.portfolio.analytics.performance import PerformanceSnapshot
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class SnapshotManager:
    """Manager for performance snapshot persistence and retrieval."""
    
    def __init__(self, retention_days: int = 90, storage_path: Optional[Path] = None) -> None:
        """Initialize snapshot manager.
        
        Args:
            retention_days: Number of days to retain snapshots
            storage_path: Path for snapshot storage (uses temp if not provided)
        """
        self.retention_days = retention_days
        self.storage_path = storage_path or Path("/tmp/cyberdelta/snapshots")
        self._initialized = False
        self._snapshot_cache: Dict[str, PerformanceSnapshot] = {}
        
    async def initialize(self) -> None:
        """Initialize the snapshot manager."""
        if self._initialized:
            return
            
        logger.info("Initializing snapshot manager")
        
        # Create storage directory
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Load recent snapshots into cache
        await self._load_cache()
        
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the snapshot manager."""
        if not self._initialized:
            return
            
        logger.info("Shutting down snapshot manager")
        
        # Flush cache to disk
        await self._flush_cache()
        
        self._initialized = False
        
    async def save_snapshots(self, snapshots: List[PerformanceSnapshot]) -> None:
        """Save performance snapshots to storage.
        
        Args:
            snapshots: List of snapshots to save
        """
        if not snapshots:
            return
            
        logger.info(f"Saving {len(snapshots)} performance snapshots")
        
        for snapshot in snapshots:
            # Generate filename based on timestamp
            filename = self._generate_filename(snapshot.timestamp)
            filepath = self.storage_path / filename
            
            # Convert to dict for serialization
            snapshot_data = self._snapshot_to_dict(snapshot)
            
            # Save to file
            try:
                with open(filepath, 'w') as f:
                    json.dump(snapshot_data, f, indent=2)
                    
                # Update cache
                cache_key = snapshot.timestamp.strftime("%Y%m%d_%H%M%S")
                self._snapshot_cache[cache_key] = snapshot
                
            except Exception as e:
                logger.error(f"Failed to save snapshot: {e}")
                
        # Clean old snapshots
        await self._cleanup_old_snapshots()
        
    async def load_historical_snapshots(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[PerformanceSnapshot]:
        """Load historical snapshots from storage.
        
        Args:
            start_date: Start date for snapshots (defaults to retention period)
            end_date: End date for snapshots (defaults to now)
            
        Returns:
            List of historical snapshots
        """
        if not start_date:
            start_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        if not end_date:
            end_date = datetime.now(UTC)
            
        logger.info(f"Loading snapshots from {start_date} to {end_date}")
        
        snapshots = []
        
        # First check cache
        for cache_key, snapshot in self._snapshot_cache.items():
            if start_date <= snapshot.timestamp <= end_date:
                snapshots.append(snapshot)
                
        # Then load from disk if needed
        for filepath in self.storage_path.glob("snapshot_*.json"):
            # Parse timestamp from filename
            try:
                timestamp_str = filepath.stem.replace("snapshot_", "")
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                timestamp = timestamp.replace(tzinfo=UTC)
                
                if start_date <= timestamp <= end_date:
                    # Check if already in results from cache
                    if not any(s.timestamp == timestamp for s in snapshots):
                        # Load from file
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                            snapshot = self._dict_to_snapshot(data)
                            snapshots.append(snapshot)
                            
            except Exception as e:
                logger.error(f"Failed to load snapshot from {filepath}: {e}")
                
        # Sort by timestamp
        snapshots.sort(key=lambda s: s.timestamp)
        
        logger.info(f"Loaded {len(snapshots)} historical snapshots")
        return snapshots
        
    async def get_latest_snapshot(self) -> Optional[PerformanceSnapshot]:
        """Get the most recent snapshot.
        
        Returns:
            Latest snapshot or None if no snapshots exist
        """
        # Check cache first
        if self._snapshot_cache:
            latest_key = max(self._snapshot_cache.keys())
            return self._snapshot_cache[latest_key]
            
        # Load from disk
        snapshots = await self.load_historical_snapshots(
            start_date=datetime.now(UTC) - timedelta(days=1)
        )
        
        return snapshots[-1] if snapshots else None
        
    async def get_snapshot_at_time(self, target_time: datetime) -> Optional[PerformanceSnapshot]:
        """Get snapshot closest to a specific time.
        
        Args:
            target_time: Target timestamp
            
        Returns:
            Closest snapshot or None if not found
        """
        # Load snapshots around target time
        snapshots = await self.load_historical_snapshots(
            start_date=target_time - timedelta(hours=1),
            end_date=target_time + timedelta(hours=1)
        )
        
        if not snapshots:
            return None
            
        # Find closest
        closest = min(
            snapshots,
            key=lambda s: abs((s.timestamp - target_time).total_seconds())
        )
        
        return closest
        
    def _generate_filename(self, timestamp: datetime) -> str:
        """Generate filename for snapshot."""
        return f"snapshot_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
    def _snapshot_to_dict(self, snapshot: PerformanceSnapshot) -> Dict[str, Any]:
        """Convert snapshot to dictionary for serialization."""
        return {
            "timestamp": snapshot.timestamp.isoformat(),
            "total_value": str(snapshot.total_value),
            "daily_pnl": str(snapshot.daily_pnl),
            "cumulative_pnl": str(snapshot.cumulative_pnl),
            "realized_pnl": str(snapshot.realized_pnl),
            "unrealized_pnl": str(snapshot.unrealized_pnl),
            "win_rate": str(snapshot.win_rate),
            "sharpe_ratio": str(snapshot.sharpe_ratio),
            "max_drawdown": str(snapshot.max_drawdown),
            "positions_count": snapshot.positions_count,
        }
        
    def _dict_to_snapshot(self, data: Dict[str, Any]) -> PerformanceSnapshot:
        """Convert dictionary to snapshot object."""
        from decimal import Decimal
        
        return PerformanceSnapshot(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            total_value=Decimal(data["total_value"]),
            daily_pnl=Decimal(data["daily_pnl"]),
            cumulative_pnl=Decimal(data["cumulative_pnl"]),
            realized_pnl=Decimal(data["realized_pnl"]),
            unrealized_pnl=Decimal(data["unrealized_pnl"]),
            win_rate=Decimal(data["win_rate"]),
            sharpe_ratio=Decimal(data["sharpe_ratio"]),
            max_drawdown=Decimal(data["max_drawdown"]),
            positions_count=data["positions_count"],
        )
        
    async def _load_cache(self) -> None:
        """Load recent snapshots into cache."""
        # Load last 24 hours into cache
        recent_snapshots = await self.load_historical_snapshots(
            start_date=datetime.now(UTC) - timedelta(days=1)
        )
        
        for snapshot in recent_snapshots:
            cache_key = snapshot.timestamp.strftime("%Y%m%d_%H%M%S")
            self._snapshot_cache[cache_key] = snapshot
            
        logger.info(f"Loaded {len(self._snapshot_cache)} snapshots into cache")
        
    async def _flush_cache(self) -> None:
        """Flush cache to disk."""
        snapshots = list(self._snapshot_cache.values())
        if snapshots:
            await self.save_snapshots(snapshots)
            
    async def _cleanup_old_snapshots(self) -> None:
        """Remove snapshots older than retention period."""
        cutoff = datetime.now(UTC) - timedelta(days=self.retention_days)
        
        removed_count = 0
        for filepath in self.storage_path.glob("snapshot_*.json"):
            try:
                # Parse timestamp from filename
                timestamp_str = filepath.stem.replace("snapshot_", "")
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                timestamp = timestamp.replace(tzinfo=UTC)
                
                if timestamp < cutoff:
                    filepath.unlink()
                    removed_count += 1
                    
                    # Remove from cache if present
                    cache_key = timestamp.strftime("%Y%m%d_%H%M%S")
                    self._snapshot_cache.pop(cache_key, None)
                    
            except Exception as e:
                logger.error(f"Failed to cleanup snapshot {filepath}: {e}")
                
        if removed_count > 0:
            logger.info(f"Removed {removed_count} old snapshots")
            
        # Also trim cache
        self._trim_cache()
        
    def _trim_cache(self) -> None:
        """Trim cache to reasonable size."""
        max_cache_size = 1440  # 24 hours of minute snapshots
        
        if len(self._snapshot_cache) > max_cache_size:
            # Keep most recent entries
            sorted_keys = sorted(self._snapshot_cache.keys())
            keys_to_remove = sorted_keys[:-max_cache_size]
            
            for key in keys_to_remove:
                del self._snapshot_cache[key]