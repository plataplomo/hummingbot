"""Factory functions for creating persistence services."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from cyberdelta.core.portfolio.services.persistence.persistence_models import PersistenceConfig
from cyberdelta.core.portfolio.services.persistence.simple_persistence_manager import (
    SimplePersistenceManager,
)


def create_persistence_manager(
    storage_path: str | Path,
    auto_backup: bool = True,
    max_backups: int = 10,
    compression: bool = False,
) -> SimplePersistenceManager[BaseModel]:
    """Create a persistence manager with default configuration.

    Args:
        storage_path: Base path for state storage
        auto_backup: Whether to auto-backup before overwriting
        max_backups: Maximum number of backups to keep
        compression: Whether to compress state files

    Returns:
        Configured StatePersistenceManager instance
    """
    config = PersistenceConfig(
        storage_path=str(storage_path),
        auto_backup=auto_backup,
        max_backups=max_backups,
        compression=compression,
    )

    manager: SimplePersistenceManager[BaseModel] = SimplePersistenceManager(config=config)
    return manager


def create_persistence_config(
    storage_path: str | Path,
    auto_backup: bool = True,
    max_backups: int = 10,
    compression: bool = False,
    file_extension: str = ".json",
) -> PersistenceConfig:
    """Create a persistence configuration.

    Args:
        storage_path: Base path for state storage
        auto_backup: Whether to auto-backup before overwriting
        max_backups: Maximum number of backups to keep
        compression: Whether to compress state files
        file_extension: File extension to use for state files

    Returns:
        Configured PersistenceConfig instance
    """
    return PersistenceConfig(
        storage_path=str(storage_path),
        auto_backup=auto_backup,
        max_backups=max_backups,
        compression=compression,
        file_extension=file_extension,
    )