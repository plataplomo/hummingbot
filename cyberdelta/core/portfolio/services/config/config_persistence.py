"""Clean configuration persistence service."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import toml
import yaml
from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioConfig


class ConfigPersistenceService(BaseModel):
    """Saves and manages configuration persistence only."""

    default_format: str = Field(default="yaml", description="Default save format")
    backup_enabled: bool = Field(default=True, description="Enable automatic backups")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def save_to_file(
        self, 
        config: PortfolioConfig, 
        config_path: Path,
        format: str | None = None
    ) -> None:
        """Save configuration to file."""
        save_format = format or self.default_format

        # Create backup if enabled
        if self.backup_enabled and config_path.exists():
            await self._create_backup(config_path)

        # Save in specified format
        if save_format == "yaml":
            await self._save_yaml(config, config_path)
        elif save_format == "json":
            await self._save_json(config, config_path)
        elif save_format == "toml":
            await self._save_toml(config, config_path)
        else:
            raise ValueError(f"Unsupported save format: {save_format}")

    async def save_to_dict(self, config: PortfolioConfig) -> dict[str, Any]:
        """Convert configuration to dictionary for saving."""
        return config.model_dump()

    async def create_backup(self, config_path: Path) -> Path:
        """Create backup of existing configuration file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Cannot backup non-existent file: {config_path}")

        return await self._create_backup(config_path)

    async def list_backups(self, config_path: Path) -> list[Path]:
        """List available backup files for configuration."""
        backup_pattern = f"{config_path.name}.backup.*"
        backup_dir = config_path.parent
        
        return list(backup_dir.glob(backup_pattern))

    async def restore_from_backup(self, config_path: Path, backup_path: Path) -> None:
        """Restore configuration from backup file."""
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_path}")

        # Copy backup to main config file
        import shutil
        shutil.copy2(backup_path, config_path)

    async def _create_backup(self, config_path: Path) -> Path:
        """Create timestamped backup of configuration file."""
        import time
        timestamp = int(time.time())
        backup_path = config_path.with_suffix(f"{config_path.suffix}.backup.{timestamp}")
        
        import shutil
        shutil.copy2(config_path, backup_path)
        
        return backup_path

    async def _save_yaml(self, config: PortfolioConfig, path: Path) -> None:
        """Save configuration as YAML."""
        config_dict = config.model_dump()
        with path.open('w') as file:
            yaml.dump(config_dict, file, default_flow_style=False, indent=2)

    async def _save_json(self, config: PortfolioConfig, path: Path) -> None:
        """Save configuration as JSON."""
        config_dict = config.model_dump()
        with path.open('w') as file:
            json.dump(config_dict, file, indent=2)

    async def _save_toml(self, config: PortfolioConfig, path: Path) -> None:
        """Save configuration as TOML."""
        config_dict = config.model_dump()
        with path.open('w') as file:
            toml.dump(config_dict, file)