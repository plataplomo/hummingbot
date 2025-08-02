"""Clean configuration loading service."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:
    import toml
except ImportError:
    toml = None  # type: ignore[assignment]

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]
from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioConfig
from cyberdelta.core.portfolio.portfolio_types.infrastructure import ValidationResult


class ConfigLoaderService(BaseModel):
    """Loads configuration from various sources only."""

    supported_formats: list[str] = Field(default_factory=lambda: [".yaml", ".json", ".toml"])

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def load_from_file(self, config_path: Path) -> PortfolioConfig:
        """Load configuration from file."""

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Determine format and load
        suffix = config_path.suffix.lower()
        if suffix == ".yaml":
            return await self._load_yaml(config_path)
        elif suffix == ".json":
            return await self._load_json(config_path)
        elif suffix == ".toml":
            return await self._load_toml(config_path)
        else:
            raise ValueError(f"Unsupported config format: {suffix}")

    async def load_from_env(self) -> PortfolioConfig:
        """Load configuration from environment variables."""
        env_config = {
            "base_currency": os.getenv("PORTFOLIO_BASE_CURRENCY", "USDC"),
            "enable_pnl_tracking": os.getenv("PORTFOLIO_ENABLE_PNL", "true").lower() == "true",
            "enable_exposure_monitoring": os.getenv("PORTFOLIO_ENABLE_EXPOSURE", "true").lower() == "true",
        }
        
        return PortfolioConfig(**env_config)  # type: ignore[arg-type]

    async def load_from_dict(self, config_data: dict[str, Any]) -> PortfolioConfig:
        """Load configuration from dictionary."""
        return PortfolioConfig(**config_data)

    async def get_supported_formats(self) -> list[str]:
        """Get list of supported configuration formats."""
        return self.supported_formats.copy()

    async def _load_yaml(self, path: Path) -> PortfolioConfig:
        """Load YAML configuration."""
        with path.open('r') as file:
            config_data = yaml.safe_load(file)
        return PortfolioConfig(**config_data)

    async def _load_json(self, path: Path) -> PortfolioConfig:
        """Load JSON configuration."""
        with path.open('r') as file:
            config_data = json.load(file)
        return PortfolioConfig(**config_data)

    async def _load_toml(self, path: Path) -> PortfolioConfig:
        """Load TOML configuration."""
        with path.open('r') as file:
            config_data = toml.load(file)
        return PortfolioConfig(**config_data)