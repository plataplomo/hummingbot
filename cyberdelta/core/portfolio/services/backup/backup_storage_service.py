"""Backup file storage and retrieval service."""

from __future__ import annotations

import gzip
import hashlib
import json
import pickle
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import BackupServiceValidationError
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Constants for storage
DEFAULT_CHUNK_SIZE = 4096
HASH_CHUNK_SIZE = 4096


class BackupFormat(Enum):
    """Backup storage formats."""

    JSON = "json"
    PICKLE = "pickle"
    COMPRESSED_JSON = "compressed_json"
    COMPRESSED_PICKLE = "compressed_pickle"


class UnsupportedBackupFormatError(ValueError):
    """Raised when an unsupported backup format is encountered."""

    def __init__(self, backup_format: BackupFormat) -> None:
        self.backup_format = backup_format
        super().__init__(f"Backup format {backup_format} is not supported")


class BackupStorageService(BasePortfolioService):
    """Handles backup file storage and retrieval only."""

    backup_dir: Path = Field(..., description="Directory for backup files")
    default_format: BackupFormat = Field(
        default=BackupFormat.COMPRESSED_JSON, description="Default backup format"
    )

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def _initialize_internal(self) -> None:
        """Initialize storage service."""
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Backup storage service initialized", backup_dir=str(self.backup_dir))

    async def _shutdown_internal(self) -> None:
        """Shutdown storage service."""
        logger.info("Backup storage service shutdown")

    async def save_backup(
        self,
        backup_id: str,
        state_data: PortfolioStateData,
        backup_format: BackupFormat | None = None,
    ) -> dict[str, object]:
        """Save portfolio state to backup file.
        
        Returns:
            Dictionary with file path, size, checksum, and format.
        """
        format_to_use = backup_format or self.default_format
        file_path = await self._create_backup_file(backup_id, state_data, format_to_use)
        
        # Calculate file info
        file_size = Path(file_path).stat().st_size
        checksum = await self._calculate_checksum(file_path)
        
        logger.info(
            "Backup file saved",
            backup_id=backup_id,
            file_path=file_path,
            file_size=file_size,
            format=format_to_use.value,
        )
        
        return {
            "file_path": file_path,
            "file_size": file_size,
            "checksum": checksum,
            "format": format_to_use.value,
        }

    async def load_backup(
        self, backup_id: str, backup_format: BackupFormat
    ) -> PortfolioStateData:
        """Load portfolio state from backup file."""
        file_path = self._get_backup_file_path(backup_id, backup_format)
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Backup file not found: {file_path}")
        
        # Load based on format
        if backup_format == BackupFormat.JSON:
            return await self._load_json_file(file_path)
        elif backup_format == BackupFormat.PICKLE:
            return await self._load_pickle_file(file_path)
        elif backup_format == BackupFormat.COMPRESSED_JSON:
            return await self._load_compressed_json_file(file_path)
        elif backup_format == BackupFormat.COMPRESSED_PICKLE:
            return await self._load_compressed_pickle_file(file_path)
        else:
            raise UnsupportedBackupFormatError(backup_format)

    async def delete_backup_file(self, backup_id: str, backup_format: BackupFormat) -> bool:
        """Delete backup file from storage."""
        file_path = self._get_backup_file_path(backup_id, backup_format)
        
        try:
            Path(file_path).unlink(missing_ok=True)
            logger.info("Backup file deleted", backup_id=backup_id, file_path=file_path)
            return True
        except OSError as e:
            logger.error(
                "Failed to delete backup file",
                backup_id=backup_id,
                file_path=file_path,
                error=str(e),
            )
            return False

    async def validate_backup_file(
        self, backup_id: str, backup_format: BackupFormat, expected_checksum: str
    ) -> bool:
        """Validate backup file integrity."""
        file_path = self._get_backup_file_path(backup_id, backup_format)
        
        if not Path(file_path).exists():
            return False
        
        try:
            actual_checksum = await self._calculate_checksum(file_path)
            return actual_checksum == expected_checksum
        except OSError:
            return False

    def _get_backup_file_path(self, backup_id: str, backup_format: BackupFormat) -> str:
        """Get file path for backup."""
        extension = self._get_file_extension(backup_format)
        return str(self.backup_dir / f"{backup_id}{extension}")

    def _get_file_extension(self, backup_format: BackupFormat) -> str:
        """Get file extension for backup format."""
        if backup_format == BackupFormat.JSON:
            return ".json"
        elif backup_format == BackupFormat.PICKLE:
            return ".pkl"
        elif backup_format == BackupFormat.COMPRESSED_JSON:
            return ".json.gz"
        elif backup_format == BackupFormat.COMPRESSED_PICKLE:
            return ".pkl.gz"
        else:
            raise UnsupportedBackupFormatError(backup_format)

    async def _create_backup_file(
        self, backup_id: str, state_data: PortfolioStateData, backup_format: BackupFormat
    ) -> str:
        """Create backup file in specified format."""
        file_path = self._get_backup_file_path(backup_id, backup_format)
        
        # Serialize data for storage
        serialized_data = self._serialize_portfolio_state(state_data)
        
        if backup_format == BackupFormat.JSON:
            await self._save_json_file(file_path, serialized_data)
        elif backup_format == BackupFormat.PICKLE:
            await self._save_pickle_file(file_path, serialized_data)
        elif backup_format == BackupFormat.COMPRESSED_JSON:
            await self._save_compressed_json_file(file_path, serialized_data)
        elif backup_format == BackupFormat.COMPRESSED_PICKLE:
            await self._save_compressed_pickle_file(file_path, serialized_data)
        else:
            raise UnsupportedBackupFormatError(backup_format)
        
        return file_path

    def _serialize_portfolio_state(self, state_data: PortfolioStateData) -> dict[str, Any]:
        """Serialize portfolio state for storage."""
        return {
            "state_id": state_data.state_id,
            "portfolio_id": state_data.portfolio_id,
            "total_account_value": str(state_data.total_account_value),
            "total_collateral": str(state_data.total_collateral),
            "free_collateral": str(state_data.free_collateral),
            "total_realized_pnl": str(state_data.total_realized_pnl),
            "total_unrealized_pnl": str(state_data.total_unrealized_pnl),
            "daily_pnl": str(state_data.daily_pnl),
            "gross_exposure": str(state_data.gross_exposure),
            "net_exposure": str(state_data.net_exposure),
            "leverage": str(state_data.leverage),
            "portfolio_var_95": str(state_data.portfolio_var_95),
            "max_drawdown": str(state_data.max_drawdown),
            "sharpe_ratio": str(state_data.sharpe_ratio) if state_data.sharpe_ratio is not None else None,
            "active_positions": state_data.active_positions,
            "open_orders": state_data.open_orders,
            "total_trades": state_data.total_trades,
            "exchange_summaries": {k: v.model_dump() for k, v in state_data.exchange_summaries.items()},
            "currency_exposures": {k: str(v) for k, v in state_data.currency_exposures.items()},
            "component_health": {k: v.model_dump() for k, v in state_data.component_health.items()},
            "metadata": state_data.metadata,
            "timestamp": state_data.created_at.isoformat() if hasattr(state_data, 'created_at') else None,
        }

    async def _save_json_file(self, file_path: str, data: dict[str, Any]) -> None:
        """Save data as JSON file."""
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    async def _save_pickle_file(self, file_path: str, data: dict[str, Any]) -> None:
        """Save data as pickle file."""
        with open(file_path, "wb") as f:
            pickle.dump(data, f)

    async def _save_compressed_json_file(self, file_path: str, data: dict[str, Any]) -> None:
        """Save data as compressed JSON file."""
        json_str = json.dumps(data, indent=2, default=str)
        with gzip.open(file_path, "wt") as f:
            f.write(json_str)

    async def _save_compressed_pickle_file(self, file_path: str, data: dict[str, Any]) -> None:
        """Save data as compressed pickle file."""
        with gzip.open(file_path, "wb") as f:
            pickle.dump(data, f)

    async def _load_json_file(self, file_path: str) -> PortfolioStateData:
        """Load portfolio state from JSON file."""
        with open(file_path) as f:
            data = json.load(f)
        return self._deserialize_portfolio_state(data)

    async def _load_pickle_file(self, file_path: str) -> PortfolioStateData:
        """Load portfolio state from pickle file."""
        with open(file_path, "rb") as f:
            data = pickle.load(f)
        return self._deserialize_portfolio_state(data)

    async def _load_compressed_json_file(self, file_path: str) -> PortfolioStateData:
        """Load portfolio state from compressed JSON file."""
        with gzip.open(file_path, "rt") as f:
            data = json.load(f)
        return self._deserialize_portfolio_state(data)

    async def _load_compressed_pickle_file(self, file_path: str) -> PortfolioStateData:
        """Load portfolio state from compressed pickle file."""
        with gzip.open(file_path, "rb") as f:
            data = pickle.load(f)
        return self._deserialize_portfolio_state(data)

    def _deserialize_portfolio_state(self, data: dict[str, Any]) -> PortfolioStateData:
        """Deserialize portfolio state from storage format."""
        from decimal import Decimal
        
        return PortfolioStateData(
            state_id=data.get("state_id", "backup_state"),
            portfolio_id=data.get("portfolio_id", ""),
            total_account_value=Decimal(data.get("total_account_value", "0")),
            total_collateral=Decimal(data.get("total_collateral", "0")),
            free_collateral=Decimal(data.get("free_collateral", "0")),
            total_realized_pnl=Decimal(data.get("total_realized_pnl", "0")),
            total_unrealized_pnl=Decimal(data.get("total_unrealized_pnl", "0")),
            daily_pnl=Decimal(data.get("daily_pnl", "0")),
            gross_exposure=Decimal(data.get("gross_exposure", "0")),
            net_exposure=Decimal(data.get("net_exposure", "0")),
            leverage=Decimal(data.get("leverage", "0")),
            portfolio_var_95=Decimal(data.get("portfolio_var_95", "0")),
            max_drawdown=Decimal(data.get("max_drawdown", "0")),
            sharpe_ratio=Decimal(data["sharpe_ratio"]) if data.get("sharpe_ratio") is not None else None,
            active_positions=data.get("active_positions", 0),
            open_orders=data.get("open_orders", 0),
            total_trades=data.get("total_trades", 0),
            exchange_summaries=data.get("exchange_summaries", {}),
            currency_exposures={k: Decimal(v) for k, v in data.get("currency_exposures", {}).items()},
            component_health=data.get("component_health", {}),
            metadata=data.get("metadata", {}),
        )

    async def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum of file."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(HASH_CHUNK_SIZE):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
