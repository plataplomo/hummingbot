"""Data management module for persistence, backup, and serialization.

This module provides data management capabilities including state persistence,
backup orchestration, and data serialization.
"""

from cyberdelta.core.data_management.persistence.persistence_models import (
    PersistenceConfig,
    PersistenceStats,
)
from cyberdelta.core.data_management.persistence.simple_persistence_manager import (
    SimplePersistenceManager,
)
from cyberdelta.core.data_management.persistence.state_serializer import (
    StateSerializer,
)
from cyberdelta.core.data_management.backup.backup_orchestrator import (
    BackupOrchestrator,
)
from cyberdelta.core.data_management.backup.backup_scheduler_service import (
    BackupSchedulerService,
)
from cyberdelta.core.data_management.backup.backup_storage_service import (
    BackupStorageService,
)
from cyberdelta.core.data_management.serialization.serializers import (
    SecureSerializer,
    SerializedPortfolioData,
)

__all__ = [
    # Persistence
    "PersistenceConfig",
    "PersistenceStats",
    "SimplePersistenceManager",
    "StateSerializer",
    # Backup
    "BackupOrchestrator",
    "BackupSchedulerService",
    "BackupStorageService",
    # Serialization
    "SecureSerializer",
    "SerializedPortfolioData",
]