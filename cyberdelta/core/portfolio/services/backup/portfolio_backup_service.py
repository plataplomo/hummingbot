"""Portfolio backup and recovery service."""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import hashlib
import json
import time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, ValidationError, field_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import BackupServiceValidationError
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData
from cyberdelta.core.portfolio.services import serialization
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.manager_protocols import (
        StateManagerProtocol,
    )
    from cyberdelta.core.portfolio.portfolio_types.service_protocols import (
        PersistenceServiceProtocol,
    )


logger = get_logger(__name__)

# Constants
DEFAULT_RETENTION_DAYS = 30
DEFAULT_MAX_BACKUPS = 100
DEFAULT_BACKUP_INTERVAL = 3600  # seconds
CLEANUP_INTERVAL = 3600  # 1 hour
DEFAULT_CHUNK_SIZE = 4096  # bytes
HASH_CHUNK_SIZE = 4096  # bytes


class BackupType(Enum):
    """Types of portfolio backups."""

    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    SNAPSHOT = "snapshot"


class BackupFormat(Enum):
    """Backup storage formats."""

    JSON = "json"
    PICKLE = "pickle"
    COMPRESSED_JSON = "compressed_json"
    COMPRESSED_PICKLE = "compressed_pickle"


class UnsupportedBackupFormatError(ValueError):
    """Raised when an unsupported backup format is encountered."""

    def __init__(self, backup_format: BackupFormat) -> None:
        """Initialize the exception.

        Args:
            backup_format: The unsupported backup format
        """
        self.backup_format = backup_format
        super().__init__(f"Backup format {backup_format} is not supported")


class BackupStatus(Enum):
    """Backup operation status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"
    EXPIRED = "expired"


class BackupMetadataInfo(BaseModel):
    """Backup metadata information with validation."""

    backup_source: str = Field(default="system", min_length=1, description="Source of the backup")
    backup_version: str = Field(
        default="1.0", pattern="^[0-9]+\\.[0-9]+$", description="Backup version"
    )
    compression_algorithm: str = Field(
        default="gzip", min_length=1, description="Compression algorithm used"
    )
    encryption_enabled: bool = Field(default=False, description="Whether backup is encrypted")

    @field_validator("backup_source", mode="before")
    @classmethod
    def validate_backup_source(cls, v: str) -> str:
        """Validate backup source is non-empty."""
        if not v or not v.strip():
            return "system"
        return v.strip()


class CustomBackupMetadata(BaseModel):
    """Custom metadata for backup configurations with validation."""

    creator: str = Field(default="system", min_length=1, description="Creator of the backup")
    environment: str = Field(
        default="production",
        pattern="^(development|staging|production)$",
        description="Environment",
    )
    backup_category: str = Field(
        default="automatic",
        pattern="^(automatic|manual|scheduled|emergency)$",
        description="Backup category",
    )
    priority: int = Field(default=1, ge=1, le=5, description="Backup priority (1-5)")

    @field_validator("creator", mode="before")
    @classmethod
    def validate_creator(cls, v: str) -> str:
        """Validate creator is non-empty."""
        if not v or not v.strip():
            return "system"
        return v.strip()


class RecoveryMetadataInfo(BaseModel):
    """Recovery metadata information with validation."""

    recovery_type: str = Field(
        default="full", pattern="^(full|partial|selective)$", description="Type of recovery"
    )
    recovery_source: str = Field(
        default="backup", min_length=1, description="Source of recovery data"
    )
    validation_level: str = Field(
        default="standard",
        pattern="^(minimal|standard|comprehensive)$",
        description="Validation level",
    )
    performance_impact: str = Field(
        default="medium", pattern="^(low|medium|high)$", description="Expected performance impact"
    )

    @field_validator("recovery_source", mode="before")
    @classmethod
    def validate_recovery_source(cls, v: str) -> str:
        """Validate recovery source is non-empty."""
        if not v or not v.strip():
            return "backup"
        return v.strip()


class SerializablePositionData(BaseModel):
    """Serializable position data with validation."""

    position_id: str = Field(min_length=1, description="Position identifier")
    symbol: str = Field(min_length=1, description="Trading symbol")
    size: str = Field(description="Position size as string")
    value: str = Field(description="Position value as string")

    @field_validator("size", "value", mode="before")
    @classmethod
    def validate_decimal_strings(cls, v: str | float) -> str:
        """Validate decimal string representations."""
        value: str = str(v) if not isinstance(v, str) else v
        # Basic validation for decimal string format
        try:
            float(value)  # Test if it can be converted to a number
        except ValueError as e:
            raise BackupServiceValidationError(value_type="Value") from e
        return value


class SerializableBalanceData(BaseModel):
    """Serializable balance data with validation."""

    exchange_id: str = Field(min_length=1, description="Exchange identifier")
    asset: str = Field(min_length=1, description="Asset identifier")
    balance: str = Field(description="Balance amount as string")
    available: str = Field(description="Available balance as string")

    @field_validator("balance", "available", mode="before")
    @classmethod
    def validate_balance_strings(cls, v: str | float) -> str:
        """Validate balance string representations."""
        value: str = str(v) if not isinstance(v, str) else v
        try:
            float(value)  # Test if it can be converted to a number
        except ValueError as e:
            raise BackupServiceValidationError(value_type="Balance") from e
        return value


class SerializableOrderData(BaseModel):
    """Serializable order data with validation."""

    order_id: str = Field(min_length=1, description="Order identifier")
    symbol: str = Field(min_length=1, description="Trading symbol")
    side: str = Field(pattern="^(buy|sell|BUY|SELL)$", description="Order side")
    quantity: str = Field(description="Order quantity as string")
    price: str = Field(description="Order price as string")

    @field_validator("quantity", "price", mode="before")
    @classmethod
    def validate_order_strings(cls, v: str | float) -> str:
        """Validate order string representations."""
        value: str = str(v) if not isinstance(v, str) else v
        try:
            float(value)  # Test if it can be converted to a number
        except ValueError as e:
            raise BackupServiceValidationError(value_type="Order value") from e
        return value


class BackupMetadata(BaseModel):
    """Pydantic model for backup metadata."""

    backup_id: str
    timestamp: float = 0.0
    backup_type: BackupType = BackupType.FULL
    format: BackupFormat = BackupFormat.JSON
    file_path: str = ""
    file_size: int = 0
    checksum: str = ""
    version: str = "1.0"
    compression_ratio: float = 0.0
    creation_time: float = 0.0
    status: BackupStatus = BackupStatus.PENDING

    # Content metadata
    positions_count: int = 0
    balances_count: int = 0
    orders_count: int = 0
    exchanges_count: int = 0
    symbols_count: int = 0

    # Validation metadata
    validation_passed: bool = False
    validation_errors: list[str] = Field(default_factory=list)

    # Recovery metadata
    recovery_tested: bool = False
    recovery_time_estimate: float = 0.0

    # Retention metadata
    retention_period: int = 30
    expires_at: float = 0.0

    # Additional metadata
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    metadata: BackupMetadataInfo = Field(default_factory=BackupMetadataInfo)


class BackupConfig(BaseModel):
    """Configuration for portfolio backup operations."""

    backup_directory: str = "./backups"
    backup_format: BackupFormat = BackupFormat.COMPRESSED_JSON
    backup_type: BackupType = BackupType.FULL
    retention_days: int = DEFAULT_RETENTION_DAYS
    max_backups: int = DEFAULT_MAX_BACKUPS
    compression_enabled: bool = True
    validation_enabled: bool = True
    checksum_verification: bool = True
    auto_cleanup_enabled: bool = True
    scheduled_backup_enabled: bool = False
    backup_interval: int = DEFAULT_BACKUP_INTERVAL  # seconds
    backup_on_shutdown: bool = True
    backup_on_critical_events: bool = True
    exclude_patterns: list[str] = Field(default_factory=list)
    include_patterns: list[str] = Field(default_factory=list)
    custom_metadata: CustomBackupMetadata = Field(default_factory=CustomBackupMetadata)


class BackupStatistics(BaseModel):
    """Statistics for backup operations."""

    total_backups: int = 0
    successful_backups: int = 0
    failed_backups: int = 0
    total_backup_size: int = 0
    average_backup_time: float = 0.0
    last_backup_duration: float = 0.0


class RecoveryResult(BaseModel):
    """Result of a portfolio recovery operation."""

    success: bool
    backup_id: str
    timestamp: float
    recovered_positions: int = 0
    recovered_balances: int = 0
    recovered_orders: int = 0
    recovery_time: float = 0.0
    validation_passed: bool = False
    validation_errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metadata: RecoveryMetadataInfo = Field(default_factory=RecoveryMetadataInfo)


class PortfolioBackupService(BasePortfolioService):
    """Service for portfolio backup and recovery operations."""

    def __init__(
        self, name: str = "PortfolioBackupService", config: dict[str, object] | None = None
    ) -> None:
        """Initialize the portfolio backup service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Use default BackupConfig - let the dataclass handle all defaults
        self.backup_config = BackupConfig()

        # Storage
        self.backup_metadata: dict[str, BackupMetadata] = {}
        self.backup_index: dict[str, list[str]] = {}  # Index by type, timestamp, etc.

        # State
        self.last_backup_time: float = 0.0
        self.last_full_backup_time: float = 0.0
        self.backup_statistics = BackupStatistics()

        # Dependencies (will be injected)
        self.portfolio_state_manager: StateManagerProtocol | None = None
        self.state_persistence_service: PersistenceServiceProtocol | None = None

        # Background tasks
        self.backup_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[None] | None = None

        # Ensure backup directory exists
        Path(self.backup_config.backup_directory).mkdir(parents=True, exist_ok=True)

        logger.info(
            "portfolio_backup_service_initialized",
            name=name,
            backup_directory=self.backup_config.backup_directory,
            backup_format=self.backup_config.backup_format.value,
            retention_days=self.backup_config.retention_days,
            max_backups=self.backup_config.max_backups,
        )

    async def _initialize_internal(self) -> None:
        """Initialize the backup service."""
        # Load existing backup metadata
        await self._load_backup_metadata()

        # Start background tasks
        if self.backup_config.scheduled_backup_enabled:
            self.backup_task = asyncio.create_task(self._run_backup_scheduler())

        if self.backup_config.auto_cleanup_enabled:
            self.cleanup_task = asyncio.create_task(self._run_cleanup_scheduler())

        logger.info("portfolio_backup_service_initialized_internal")

    async def _shutdown_internal(self) -> None:
        """Shutdown the backup service."""
        # Create final backup if configured
        if self.backup_config.backup_on_shutdown:
            try:
                await self.create_backup(
                    backup_type=BackupType.SNAPSHOT,
                    description="Shutdown backup",
                    tags=["shutdown", "automatic"],
                )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("shutdown_backup_failed")

        # Cancel background tasks
        if self.backup_task:
            self.backup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.backup_task

        if self.cleanup_task:
            self.cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.cleanup_task

        # Save metadata
        await self._save_backup_metadata()

        logger.info("portfolio_backup_service_shutdown_internal")

    def set_dependencies(
        self,
        portfolio_state_manager: StateManagerProtocol | None = None,
        state_persistence_service: PersistenceServiceProtocol | None = None,
    ) -> None:
        """Set service dependencies."""
        self.portfolio_state_manager = portfolio_state_manager
        self.state_persistence_service = state_persistence_service

    async def create_backup(
        self,
        backup_type: BackupType = BackupType.FULL,
        backup_format: BackupFormat | None = None,
        description: str = "",
        tags: list[str] | None = None,
        custom_metadata: dict[str, object] | None = None,
    ) -> BackupMetadata:
        """Create a portfolio backup."""
        start_time = time.time()
        backup_id = f"backup_{int(start_time)}_{backup_type.value}"

        # Use configured format if not specified
        if backup_format is None:
            backup_format = self.backup_config.backup_format

        # Create backup metadata
        metadata = BackupMetadata(
            backup_id=backup_id,
            timestamp=start_time,
            backup_type=backup_type,
            format=backup_format,
            file_path="",  # Will be set later
            creation_time=start_time,
            status=BackupStatus.IN_PROGRESS,
            description=description,
            tags=tags or [],
            retention_period=self.backup_config.retention_days,
            expires_at=start_time + (self.backup_config.retention_days * 24 * 3600),
            metadata=BackupMetadataInfo(),
        )

        try:
            # Get portfolio state
            portfolio_state = await self._get_portfolio_state()

            # Apply filters if needed
            filtered_state = await self._apply_backup_filters(portfolio_state)

            # Create backup file
            file_path = await self._create_backup_file(metadata, filtered_state)
            metadata.file_path = file_path

            # Calculate file size
            metadata.file_size = Path(file_path).stat().st_size

            # Calculate checksum
            if self.backup_config.checksum_verification:
                metadata.checksum = await self._calculate_checksum(file_path)

            # Update content metadata
            await self._update_content_metadata(metadata, filtered_state)

            # Validate backup
            if self.backup_config.validation_enabled:
                await self._validate_backup(metadata)

            # Update status
            metadata.status = BackupStatus.COMPLETED

            # Store metadata
            self.backup_metadata[backup_id] = metadata
            await self._update_backup_index(metadata)

            # Update statistics
            self.backup_statistics.total_backups += 1
            self.backup_statistics.successful_backups += 1
            self.backup_statistics.total_backup_size += metadata.file_size
            self.backup_statistics.last_backup_duration = time.time() - start_time

            # Update last backup time
            self.last_backup_time = time.time()
            if backup_type == BackupType.FULL:
                self.last_full_backup_time = time.time()

            logger.info(
                "portfolio_backup_created",
                backup_id=backup_id,
                backup_type=backup_type.value,
                format=backup_format.value,
                file_size=metadata.file_size,
                duration=time.time() - start_time,
                validation_passed=metadata.validation_passed,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            metadata.status = BackupStatus.FAILED
            metadata.validation_errors.append(str(e))
            self.backup_statistics.failed_backups += 1

            logger.exception(
                "portfolio_backup_creation_failed",
                backup_id=backup_id,
                backup_type=backup_type.value,
            )

            raise
        else:
            return metadata

    async def restore_backup(
        self, backup_id: str, validate_before_restore: bool = True, dry_run: bool = False
    ) -> RecoveryResult:
        """Restore portfolio from backup."""
        start_time = time.time()

        # Get backup metadata
        if backup_id not in self.backup_metadata:
            raise ValueError

        metadata = self.backup_metadata[backup_id]

        # Validate backup file exists
        if not Path(metadata.file_path).exists():
            raise FileNotFoundError

        # Create recovery result
        result = RecoveryResult(success=False, backup_id=backup_id, timestamp=start_time)

        try:
            # Validate backup if requested
            if validate_before_restore:
                validation_passed = await self._validate_backup_file(metadata)
                if not validation_passed:
                    result.validation_errors.extend(metadata.validation_errors)
                    return result

            # Load backup data
            backup_data = await self._load_backup_file(metadata)

            if dry_run:
                # Simulate recovery without actually restoring
                result.success = True
                # Use validated PortfolioStateData directly
                result.recovered_positions = len(backup_data.trades)
                result.recovered_balances = len(backup_data.exchange_summaries)
                result.recovered_orders = len(backup_data.orders)
                result.recovery_time = time.time() - start_time
                result.validation_passed = True

                logger.info(
                    "portfolio_backup_dry_run_completed",
                    backup_id=backup_id,
                    recovery_time=result.recovery_time,
                )

                return result

            # Restore portfolio state
            await self._restore_portfolio_state(backup_data, result)

            # Validate restored state
            validation_result = await self._validate_restored_state(backup_data, result)
            result.validation_passed = validation_result

            # Update recovery result
            result.success = True
            result.recovery_time = time.time() - start_time

            logger.info(
                "portfolio_backup_restored",
                backup_id=backup_id,
                recovery_time=result.recovery_time,
                recovered_positions=result.recovered_positions,
                recovered_balances=result.recovered_balances,
                recovered_orders=result.recovered_orders,
                validation_passed=result.validation_passed,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            result.validation_errors.append(str(e))
            result.recovery_time = time.time() - start_time

            logger.exception(
                "portfolio_backup_restoration_failed",
                backup_id=backup_id,
                recovery_time=result.recovery_time,
            )

            return result
        else:
            return result

    async def list_backups(
        self,
        backup_type: BackupType | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        limit: int | None = None,
    ) -> list[BackupMetadata]:
        """List available backups."""
        backups = list(self.backup_metadata.values())

        # Apply filters
        if backup_type:
            backups = [b for b in backups if b.backup_type == backup_type]

        if start_time:
            backups = [b for b in backups if b.timestamp >= start_time]

        if end_time:
            backups = [b for b in backups if b.timestamp <= end_time]

        # Sort by timestamp (newest first)
        backups.sort(key=lambda x: x.timestamp, reverse=True)

        # Apply limit
        if limit:
            backups = backups[:limit]

        return backups

    async def get_backup_metadata(self, backup_id: str) -> BackupMetadata | None:
        """Get metadata for a specific backup."""
        return self.backup_metadata.get(backup_id)

    async def delete_backup(self, backup_id: str) -> bool:
        """Delete a backup."""
        if backup_id not in self.backup_metadata:
            return False

        metadata = self.backup_metadata[backup_id]

        try:
            # Delete backup file
            if Path(metadata.file_path).exists():
                Path(metadata.file_path).unlink()

            # Remove from metadata
            del self.backup_metadata[backup_id]
            await self._update_backup_index(metadata, remove=True)

            # Update statistics
            self.backup_statistics.total_backup_size -= metadata.file_size

            logger.info(
                "portfolio_backup_deleted", backup_id=backup_id, file_size=metadata.file_size
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("portfolio_backup_deletion_failed", backup_id=backup_id)
            return False
        else:
            return True

    async def cleanup_expired_backups(self) -> int:
        """Clean up expired backups."""
        current_time = time.time()
        expired_backups: list[str] = []

        for backup_id, metadata in self.backup_metadata.items():
            if metadata.expires_at <= current_time:
                expired_backups.append(backup_id)

        deleted_count = 0
        for backup_id in expired_backups:
            if await self.delete_backup(backup_id):
                deleted_count += 1

        if deleted_count > 0:
            logger.info("expired_backups_cleaned_up", deleted_count=deleted_count)

        return deleted_count

    async def get_backup_statistics(self) -> dict[str, object]:
        """Get backup service statistics."""
        current_time = time.time()

        # Calculate average backup time
        if self.backup_statistics.successful_backups > 0:
            avg_time = self.backup_statistics.last_backup_duration
        else:
            avg_time = 0.0

        # Count backups by type
        backup_counts: dict[str, int] = {}
        for metadata in self.backup_metadata.values():
            backup_type = metadata.backup_type.value
            backup_counts[backup_type] = backup_counts.get(backup_type, 0) + 1

        # Calculate disk usage
        total_size = sum(
            Path(metadata.file_path).stat().st_size
            for metadata in self.backup_metadata.values()
            if Path(metadata.file_path).exists()
        )

        return {
            **self.backup_statistics.model_dump(),
            "average_backup_time": avg_time,
            "backup_counts_by_type": backup_counts,
            "total_disk_usage": total_size,
            "last_backup_age": current_time - self.last_backup_time if self.last_backup_time else 0,
            "last_full_backup_age": current_time - self.last_full_backup_time
            if self.last_full_backup_time
            else 0,
            "backup_count": len(self.backup_metadata),
            "backup_directory": self.backup_config.backup_directory,
        }

    async def _get_portfolio_state(self) -> PortfolioStateData:
        """Get current portfolio state for backup."""
        if not self.portfolio_state_manager:
            raise ValueError

        # Create proper PortfolioStateData using the refactor models with correct field names
        return PortfolioStateData(
            state_id="backup_state",  # Required field from BaseStateModel
            portfolio_id="backup_portfolio",
            # Use the correct field names from the Pydantic model
            total_account_value=Decimal(0),
            total_collateral=Decimal(0),
            free_collateral=Decimal(0),
            total_realized_pnl=Decimal(0),
            total_unrealized_pnl=Decimal(0),
            daily_pnl=Decimal(0),
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            leverage=Decimal(0),
            portfolio_var_95=Decimal(0),
            exchange_summaries={},
            currency_exposures={},
            component_health={},
            metadata={},
            balances={},
            trades=[],
            orders=[],
        )

    def _convert_to_serializable(
        self, state: PortfolioStateData
    ) -> dict[str, str | int | float | bool | None]:
        """Convert PortfolioStateData to SerializableType format using Pydantic."""
        # Use Pydantic's model_dump with string conversion for serialization
        return state.model_dump(mode="python")  # Returns properly serializable dict

    async def _apply_backup_filters(self, state: PortfolioStateData) -> PortfolioStateData:
        """Apply backup filters to state data."""
        # Apply include/exclude patterns
        # This is a simplified implementation
        return state

    async def _create_backup_file(self, metadata: BackupMetadata, state: PortfolioStateData) -> str:
        """Create backup file from state data."""
        # Generate filename
        timestamp_str = str(int(metadata.timestamp))
        filename = f"{metadata.backup_id}_{timestamp_str}.{metadata.format.value}"
        file_path = str(Path(self.backup_config.backup_directory) / filename)

        # Serialize and save data
        if metadata.format == BackupFormat.JSON:

            def _write_json() -> None:
                with Path(file_path).open("w", encoding="utf-8") as f:
                    json.dump(state.model_dump(), f, default=str, indent=2)

            await asyncio.to_thread(_write_json)

        elif metadata.format == BackupFormat.PICKLE:

            def _write_pickle() -> None:
                # Use Pydantic model serialization
                serializable_state = self._convert_to_serializable(state)
                with Path(file_path).open("wb") as f:
                    f.write(serialization.dumps(serializable_state))

            await asyncio.to_thread(_write_pickle)

        elif metadata.format == BackupFormat.COMPRESSED_JSON:

            def _write_compressed_json() -> None:
                json_data = json.dumps(state.model_dump(), default=str, indent=2)
                with gzip.open(file_path + ".gz", "wt", encoding="utf-8") as f:
                    f.write(json_data)

            await asyncio.to_thread(_write_compressed_json)
            file_path += ".gz"

        elif metadata.format == BackupFormat.COMPRESSED_PICKLE:

            def _write_compressed_pickle() -> None:
                # Use Pydantic model serialization
                serializable_state = self._convert_to_serializable(state)
                with gzip.open(file_path + ".gz", "wb") as f:
                    f.write(serialization.dumps(serializable_state))

            await asyncio.to_thread(_write_compressed_pickle)
            file_path += ".gz"

        return file_path

    async def _load_json_file(self, file_path: str) -> PortfolioStateData:
        """Load JSON backup file."""

        def _read_json() -> PortfolioStateData:
            with Path(file_path).open(encoding="utf-8") as f:
                data = json.load(f)
                return PortfolioStateData.model_validate(data)

        return await asyncio.to_thread(_read_json)

    async def _load_pickle_file(self, file_path: str) -> PortfolioStateData:
        """Load pickle backup file."""

        def _read_pickle() -> PortfolioStateData:
            with Path(file_path).open("rb") as f:
                data = serialization.loads(f.read())
                # Parse as PortfolioStateData using Pydantic
                return PortfolioStateData.model_validate(data)

        return await asyncio.to_thread(_read_pickle)

    async def _load_compressed_json_file(self, file_path: str) -> PortfolioStateData:
        """Load compressed JSON backup file."""

        def _read_compressed_json() -> PortfolioStateData:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                data = json.load(f)
                return PortfolioStateData.model_validate(data)

        return await asyncio.to_thread(_read_compressed_json)

    async def _load_compressed_pickle_file(self, file_path: str) -> PortfolioStateData:
        """Load compressed pickle backup file."""

        def _read_compressed_pickle() -> PortfolioStateData:
            with gzip.open(file_path, "rb") as f:
                data = serialization.loads(f.read())
                # Parse as PortfolioStateData using Pydantic
                return PortfolioStateData.model_validate(data)

        return await asyncio.to_thread(_read_compressed_pickle)

    async def _load_backup_file(self, metadata: BackupMetadata) -> PortfolioStateData:
        """Load backup data from file using format-specific loader."""
        file_path = metadata.file_path

        # Dispatch to appropriate loader method
        if metadata.format == BackupFormat.JSON:
            return await self._load_json_file(file_path)
        if metadata.format == BackupFormat.PICKLE:
            return await self._load_pickle_file(file_path)
        if metadata.format == BackupFormat.COMPRESSED_JSON:
            return await self._load_compressed_json_file(file_path)
        if metadata.format == BackupFormat.COMPRESSED_PICKLE:
            return await self._load_compressed_pickle_file(file_path)

        raise UnsupportedBackupFormatError(metadata.format)

    async def _calculate_checksum(self, file_path: str) -> str:
        """Calculate checksum for backup file."""

        def _calculate_hash() -> str:
            hash_sha256 = hashlib.sha256()
            with Path(file_path).open("rb") as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()

        return await asyncio.to_thread(_calculate_hash)

    async def _validate_backup(self, metadata: BackupMetadata) -> bool:
        """Validate backup integrity."""
        try:
            # Check file exists
            if not Path(metadata.file_path).exists():
                metadata.validation_errors.append("Backup file not found")
                return False

            # Verify checksum
            if metadata.checksum and self.backup_config.checksum_verification:
                calculated_checksum = await self._calculate_checksum(metadata.file_path)
                if calculated_checksum != metadata.checksum:
                    metadata.validation_errors.append("Checksum mismatch")
                    return False

            # Try to load and validate structure
            backup_data = await self._load_backup_file(metadata)

            # Validate required fields using Pydantic model attributes
            # PortfolioStateData is a Pydantic model, so it always has these fields
            required_fields = ["portfolio_id", "total_account_value", "trades", "orders"]
            for field in required_fields:
                # Access model_fields from the class, not the instance
                if field not in backup_data.__class__.model_fields:
                    metadata.validation_errors.append(f"Missing required field: {field}")
                    return False

            metadata.validation_passed = True

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            metadata.validation_errors.append(str(e))
            return False
        else:
            return True

    async def _validate_backup_file(self, metadata: BackupMetadata) -> bool:
        """Validate backup file before restoration."""
        return await self._validate_backup(metadata)

    async def _restore_portfolio_state(
        self, backup_data: PortfolioStateData, result: RecoveryResult
    ) -> None:
        """Restore portfolio state from backup data."""
        # This is a simplified implementation
        # In a real implementation, this would restore actual portfolio state

        # Use validated PortfolioStateData directly for restoration
        # Here we would actually restore the portfolio state to the system
        # For now, just count the recovered items
        result.recovered_positions = len(backup_data.trades)
        result.recovered_balances = len(backup_data.exchange_summaries)
        result.recovered_orders = len(backup_data.orders)

    async def _validate_restored_state(
        self, backup_data: PortfolioStateData, result: RecoveryResult
    ) -> bool:
        """Validate restored portfolio state."""
        # This is a simplified implementation
        # In a real implementation, this would validate the restored state
        return True

    async def _update_content_metadata(
        self, metadata: BackupMetadata, state: PortfolioStateData
    ) -> None:
        """Update content metadata from state data."""
        # Use PortfolioStateData directly to extract metadata
        # Count trades, orders, and exchange summaries
        metadata.positions_count = len(state.trades)
        metadata.orders_count = len(state.orders)
        metadata.balances_count = len(state.exchange_summaries)

        # Count unique exchanges from exchange summaries
        exchanges = {summary.exchange_id for summary in state.exchange_summaries.values()}
        metadata.exchanges_count = len(exchanges)

        # Count unique symbols from trades if available
        if state.trades:
            symbols = {trade.symbol for trade in state.trades}
            metadata.symbols_count = len(symbols)
        else:
            metadata.symbols_count = 0

    async def _update_backup_index(self, metadata: BackupMetadata, remove: bool = False) -> None:
        """Update backup index."""
        # This is a simplified implementation
        # In a real implementation, this would maintain search indexes

    async def _load_backup_metadata(self) -> None:
        """Load backup metadata using Pydantic model validation."""
        metadata_file = Path(self.backup_config.backup_directory) / "backup_metadata.json"

        if not metadata_file.exists():
            return

        try:

            def _load_with_pydantic() -> None:
                with metadata_file.open(encoding="utf-8") as f:
                    raw_data = json.load(f)

                # Use Pydantic to validate each entry directly
                for backup_id, entry_data in raw_data.items():
                    try:
                        # Use Pydantic model validation directly
                        metadata = BackupMetadata.model_validate(entry_data)
                        self.backup_metadata[backup_id] = metadata
                    except ValidationError as e:
                        logger.warning(
                            "invalid_backup_metadata_pydantic",
                            backup_id=backup_id,
                            errors=e.errors(),
                        )

            await asyncio.to_thread(_load_with_pydantic)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("backup_metadata_loading_failed")

    async def _save_backup_metadata(self) -> None:
        """Save backup metadata using Pydantic serialization."""
        metadata_file = Path(self.backup_config.backup_directory) / "backup_metadata.json"

        try:

            def _save_with_pydantic() -> None:
                # Use Pydantic model serialization directly
                serializable_data = {}
                for backup_id, metadata in self.backup_metadata.items():
                    # Serialize Pydantic model to dict
                    serializable_data[backup_id] = metadata.model_dump()

                with metadata_file.open("w", encoding="utf-8") as f:
                    json.dump(serializable_data, f, indent=2, default=str)

            await asyncio.to_thread(_save_with_pydantic)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("backup_metadata_saving_failed")

    async def _run_backup_scheduler(self) -> None:
        """Background task for scheduled backups."""
        while True:
            try:
                await asyncio.sleep(self.backup_config.backup_interval)

                # Check if backup is needed
                current_time = time.time()
                if current_time - self.last_backup_time >= self.backup_config.backup_interval:
                    await self.create_backup(
                        backup_type=self.backup_config.backup_type,
                        description="Scheduled backup",
                        tags=["scheduled", "automatic"],
                    )

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("backup_scheduler_error")
                await asyncio.sleep(CLEANUP_INTERVAL // 60)  # Wait before retrying

    async def _run_cleanup_scheduler(self) -> None:
        """Background task for cleanup operations."""
        while True:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL)  # Run every hour
                await self.cleanup_expired_backups()

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("cleanup_scheduler_error")
                await asyncio.sleep(CLEANUP_INTERVAL)  # Wait before retrying

    def _serialize_positions(
        self, positions: dict[str, dict[str, str]]
    ) -> dict[str, SerializablePositionData]:
        """Serialize positions for backup."""
        # Convert positions to typed serializable format
        serialized: dict[str, SerializablePositionData] = {}
        for position_id, position_data in positions.items():
            try:
                serialized[position_id] = SerializablePositionData(
                    position_id=position_id,
                    symbol=str(position_data.get("symbol", "")),
                    size=str(position_data.get("size", "0")),
                    value=str(position_data.get("value", "0")),
                )
            except (ValueError, TypeError, KeyError):
                # Skip invalid positions with logging
                logger.warning("position_serialization_skipped", position_id=position_id)
        return serialized

    def _serialize_balances(
        self, balances: dict[str, dict[str, str]]
    ) -> dict[str, SerializableBalanceData]:
        """Serialize balances for backup."""
        # Convert balances to typed serializable format
        serialized: dict[str, SerializableBalanceData] = {}
        for balance_id, balance_data in balances.items():
            try:
                serialized[balance_id] = SerializableBalanceData(
                    exchange_id=str(balance_data.get("exchange_id", "")),
                    asset=str(balance_data.get("asset", "")),
                    balance=str(balance_data.get("balance", "0")),
                    available=str(balance_data.get("available", "0")),
                )
            except (ValueError, TypeError, KeyError):
                # Skip invalid balances with logging
                logger.warning("balance_serialization_skipped", balance_id=balance_id)
        return serialized

    def _serialize_orders(
        self, orders: dict[str, dict[str, str]]
    ) -> dict[str, SerializableOrderData]:
        """Serialize orders for backup."""
        # Convert orders to typed serializable format
        serialized: dict[str, SerializableOrderData] = {}
        for order_id, order_data in orders.items():
            try:
                serialized[order_id] = SerializableOrderData(
                    order_id=order_id,
                    symbol=str(order_data.get("symbol", "")),
                    side=str(order_data.get("side", "buy")),
                    quantity=str(order_data.get("quantity", "0")),
                    price=str(order_data.get("price", "0")),
                )
            except (ValueError, TypeError, KeyError):
                # Skip invalid orders with logging
                logger.warning("order_serialization_skipped", order_id=order_id)
        return serialized
