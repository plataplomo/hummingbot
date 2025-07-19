"""Comprehensive configuration management service for portfolio system."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.config.portfolio_config import PortfolioConfiguration
from cyberdelta.core.portfolio.exceptions.service import ConfigurationError
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


logger = get_logger(__name__)


# Type-preserving factory functions
def _str_object_dict_factory() -> dict[str, object]:
    """Factory function that preserves dict[str, object] type information."""
    return {}


def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information."""
    return []


def _config_source_list_factory() -> list[ConfigSourceDescriptor]:
    """Factory function that preserves list[ConfigSourceDescriptor] type information."""
    return []


def _config_change_list_factory() -> list[ConfigChange]:
    """Factory function that preserves list[ConfigChange] type information."""
    return []


class ConfigSource(Enum):
    """Configuration source types."""

    FILE = "file"
    ENVIRONMENT = "environment"
    COMMAND_LINE = "command_line"
    REMOTE = "remote"
    DATABASE = "database"
    MEMORY = "memory"
    DEFAULT = "default"


class ConfigFormat(Enum):
    """Configuration file formats."""

    JSON = "json"
    YAML = "yaml"
    TOML = "toml"
    INI = "ini"
    ENV = "env"


class ConfigChangeType(Enum):
    """Types of configuration changes."""

    ADDED = "added"
    UPDATED = "updated"
    DELETED = "deleted"
    RELOADED = "reloaded"


@dataclass
class ConfigSourceDescriptor:
    """Configuration source descriptor."""

    name: str
    source_type: ConfigSource
    location: str
    format: ConfigFormat
    priority: int = 100
    enabled: bool = True
    readonly: bool = False
    watch_enabled: bool = True
    reload_interval: float = 60.0
    last_modified: float = 0.0
    checksum: str = ""
    metadata: dict[str, object] = field(default_factory=_str_object_dict_factory)


@dataclass
class ConfigChange:
    """Configuration change descriptor."""

    key: str
    change_type: ConfigChangeType
    old_value: object = None
    new_value: object = None
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    user: str = ""
    reason: str = ""
    metadata: dict[str, object] = field(default_factory=_str_object_dict_factory)


@dataclass
class ConfigValidationResult:
    """Configuration validation result."""

    is_valid: bool
    errors: list[str] = field(default_factory=_str_list_factory)
    warnings: list[str] = field(default_factory=_str_list_factory)
    affected_components: list[str] = field(default_factory=_str_list_factory)
    validation_time: float = field(default_factory=time.time)
    metadata: dict[str, object] = field(default_factory=_str_object_dict_factory)


@dataclass
class ConfigProfile:
    """Configuration profile for different environments."""

    name: str
    description: str = ""
    base_config: dict[str, object] = field(default_factory=_str_object_dict_factory)
    overrides: dict[str, object] = field(default_factory=_str_object_dict_factory)
    sources: list[ConfigSourceDescriptor] = field(default_factory=_config_source_list_factory)
    active: bool = False
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict[str, object] = field(default_factory=_str_object_dict_factory)


@dataclass
class ConfigSnapshot:
    """Configuration snapshot for versioning."""

    snapshot_id: str
    timestamp: float
    profile: str
    configuration: dict[str, object]
    changes: list[ConfigChange] = field(default_factory=_config_change_list_factory)
    description: str = ""
    tags: list[str] = field(default_factory=_str_list_factory)
    metadata: dict[str, object] = field(default_factory=_str_object_dict_factory)


class PortfolioConfigManager(BasePortfolioService):
    """Comprehensive configuration management service."""

    # Configuration paths
    config_directory: str
    profiles_directory: str
    snapshots_directory: str

    # Profile management
    default_profile: str
    current_profile: str

    # Configuration flags
    auto_reload_enabled: bool
    validation_enabled: bool
    change_tracking_enabled: bool
    snapshot_enabled: bool
    encryption_enabled: bool
    backup_enabled: bool
    watch_file_changes: bool

    # Numeric settings
    max_snapshots: int
    max_change_history: int

    # State management
    current_config: PortfolioConfiguration | None
    config_sources: dict[str, ConfigSourceDescriptor]
    config_profiles: dict[str, ConfigProfile]
    config_snapshots: dict[str, ConfigSnapshot]
    change_history: list[ConfigChange]
    change_subscribers: dict[str, list[Callable[[str, object, object], None]]]

    # Validation
    validation_rules: dict[str, list[Callable[[dict[str, object]], bool]]]
    validation_schemas: dict[str, dict[str, object]]

    # Monitoring
    config_statistics: dict[str, Any]

    # Background tasks
    reload_task: asyncio.Task[None] | None
    cleanup_task: asyncio.Task[None] | None
    file_watchers: list[asyncio.Task[None]]

    def __init__(
        self, name: str = "PortfolioConfigManager", config: dict[str, object] | None = None
    ) -> None:
        """Initialize the portfolio configuration manager.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Configuration management settings
        cfg = config or {}

        # Type-safe configuration extraction with proper casting
        config_dir = cfg.get("config_directory", "./config")
        self.config_directory = str(config_dir) if config_dir is not None else "./config"

        profiles_dir = cfg.get("profiles_directory", "./config/profiles")
        self.profiles_directory = (
            str(profiles_dir) if profiles_dir is not None else "./config/profiles"
        )

        snapshots_dir = cfg.get("snapshots_directory", "./config/snapshots")
        self.snapshots_directory = (
            str(snapshots_dir) if snapshots_dir is not None else "./config/snapshots"
        )

        default_prof = cfg.get("default_profile", "default")
        self.default_profile = str(default_prof) if default_prof is not None else "default"

        current_prof = cfg.get("current_profile", self.default_profile)
        self.current_profile = (
            str(current_prof) if current_prof is not None else self.default_profile
        )

        auto_reload = cfg.get("auto_reload_enabled", True)
        self.auto_reload_enabled = bool(auto_reload) if auto_reload is not None else True

        validation = cfg.get("validation_enabled", True)
        self.validation_enabled = bool(validation) if validation is not None else True

        change_tracking = cfg.get("change_tracking_enabled", True)
        self.change_tracking_enabled = (
            bool(change_tracking) if change_tracking is not None else True
        )

        snapshot = cfg.get("snapshot_enabled", True)
        self.snapshot_enabled = bool(snapshot) if snapshot is not None else True

        encryption = cfg.get("encryption_enabled", False)
        self.encryption_enabled = bool(encryption) if encryption is not None else False

        backup = cfg.get("backup_enabled", True)
        self.backup_enabled = bool(backup) if backup is not None else True

        watch_changes = cfg.get("watch_file_changes", True)
        self.watch_file_changes = bool(watch_changes) if watch_changes is not None else True

        max_snaps = cfg.get("max_snapshots", 100)
        self.max_snapshots = (
            int(max_snaps)
            if isinstance(max_snaps, (int, str)) and str(max_snaps).isdigit()
            else 100
        )

        max_history = cfg.get("max_change_history", 1000)
        self.max_change_history = (
            int(max_history)
            if isinstance(max_history, (int, str)) and str(max_history).isdigit()
            else 1000
        )

        # State
        self.current_config: PortfolioConfiguration | None = None
        self.config_sources: dict[str, ConfigSourceDescriptor] = {}
        self.config_profiles: dict[str, ConfigProfile] = {}
        self.config_snapshots: dict[str, ConfigSnapshot] = {}
        self.change_history: list[ConfigChange] = []
        self.change_subscribers: dict[str, list[Callable[[str, object, object], None]]] = {}

        # Validation
        self.validation_rules: dict[str, list[Callable[[dict[str, object]], bool]]] = {}
        self.validation_schemas: dict[str, dict[str, object]] = {}

        # Monitoring
        self.config_statistics = {
            "total_reloads": 0,
            "successful_reloads": 0,
            "failed_reloads": 0,
            "total_changes": 0,
            "validation_errors": 0,
            "last_reload_time": 0.0,
            "last_change_time": 0.0,
        }

        # Background tasks
        self.reload_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[None] | None = None
        self.file_watchers: list[asyncio.Task[None]] = []

        # Ensure directories exist
        for directory in [self.config_directory, self.profiles_directory, self.snapshots_directory]:
            Path(directory).mkdir(parents=True, exist_ok=True)

        logger.info(
            "portfolio_config_manager_initialized",
            name=name,
            config_directory=self.config_directory,
            default_profile=self.default_profile,
            current_profile=self.current_profile,
            auto_reload_enabled=self.auto_reload_enabled,
        )

    async def _initialize_internal(self) -> None:
        """Initialize the configuration manager."""
        # Load configuration profiles
        await self._load_profiles()

        # Load configuration sources
        await self._load_sources()

        # Load current configuration
        await self._load_configuration()

        # Initialize validation rules
        await self._initialize_validation_rules()

        # Start background tasks
        if self.auto_reload_enabled:
            self.reload_task = asyncio.create_task(self._run_reload_monitor())

        self.cleanup_task = asyncio.create_task(self._run_cleanup_monitor())

        # Start file watchers
        if self.watch_file_changes:
            await self._start_file_watchers()

        logger.info("portfolio_config_manager_initialized_internal")

    async def _shutdown_internal(self) -> None:
        """Shutdown the configuration manager."""
        # Create final snapshot
        if self.snapshot_enabled:
            await self.create_snapshot(
                description="Shutdown snapshot", tags=["shutdown", "automatic"]
            )

        # Cancel background tasks
        if self.reload_task:
            self.reload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reload_task

        if self.cleanup_task:
            self.cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.cleanup_task

        # Cancel file watchers
        for watcher in self.file_watchers:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher

        logger.info("portfolio_config_manager_shutdown_internal")

    async def get_configuration(self) -> PortfolioConfiguration:
        """Get current portfolio configuration."""
        if self.current_config is None:
            await self._load_configuration()
        if self.current_config is None:
            raise RuntimeError
        return self.current_config

    async def get_config_value(self, key: str, default: object = None) -> object:
        """Get specific configuration value."""
        config = await self.get_configuration()
        keys = key.split(".")

        current: object = config.to_dict()
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = cast(object, current[k])
            else:
                return default

        return current

    async def set_config_value(
        self,
        key: str,
        value: object,
        source: str = "manual",
        user: str = "",
        reason: str = "",
        persist: bool = True,
    ) -> bool:
        """Set configuration value."""
        try:
            # Get current value
            old_value = await self.get_config_value(key)

            # Update configuration
            if self.current_config is None:
                raise RuntimeError
            config_dict = self.current_config.to_dict()
            keys = key.split(".")
            current = config_dict

            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]

            current[keys[-1]] = value

            # Create new configuration
            new_config = PortfolioConfiguration()
            new_config.merge_with_dict(config_dict)

            # Validate configuration
            validation_result = await self._validate_configuration(new_config)
            if not validation_result.is_valid:
                logger.error("config_validation_failed", key=key, errors=validation_result.errors)
                return False

            # Apply configuration
            self.current_config = new_config

            # Track change
            if self.change_tracking_enabled:
                change = ConfigChange(
                    key=key,
                    change_type=ConfigChangeType.UPDATED,
                    old_value=old_value,
                    new_value=value,
                    source=source,
                    user=user,
                    reason=reason,
                )
                await self._track_change(change)

            # Persist if requested
            if persist:
                await self._persist_configuration()

            # Notify subscribers
            await self._notify_change_subscribers(key, old_value, value)

            # Update statistics
            self.config_statistics["total_changes"] += 1
            self.config_statistics["last_change_time"] = time.time()

            logger.info(
                "config_value_updated",
                key=key,
                old_value=old_value,
                new_value=value,
                source=source,
                user=user,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("config_value_update_failed", key=key, value=value)
            return False
        else:
            return True

    async def reload_configuration(self, force: bool = False) -> bool:
        """Reload configuration from sources."""
        try:
            if not force and not self._should_reload():
                return True

            # Create snapshot before reload
            if self.snapshot_enabled:
                await self.create_snapshot(
                    description="Pre-reload snapshot", tags=["reload", "backup"]
                )

            # Reload configuration
            await self._load_configuration()

            # Validate reloaded configuration
            if self.current_config is None:
                logger.error("config_reload_failed_no_config")
                self.config_statistics["failed_reloads"] += 1
                return False

            validation_result = await self._validate_configuration(self.current_config)
            if not validation_result.is_valid:
                logger.error("config_reload_validation_failed", errors=validation_result.errors)
                self.config_statistics["failed_reloads"] += 1
                return False

            # Track reload
            if self.change_tracking_enabled:
                change = ConfigChange(
                    key="*",
                    change_type=ConfigChangeType.RELOADED,
                    source="reload",
                    reason="Configuration reloaded",
                )
                await self._track_change(change)

            # Update statistics
            self.config_statistics["total_reloads"] += 1
            self.config_statistics["successful_reloads"] += 1
            self.config_statistics["last_reload_time"] = time.time()

            logger.info("configuration_reloaded")

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            self.config_statistics["failed_reloads"] += 1
            logger.exception("configuration_reload_failed")
            return False
        else:
            return True

    async def switch_profile(self, profile_name: str) -> bool:
        """Switch to a different configuration profile."""
        if profile_name not in self.config_profiles:
            logger.error("profile_not_found", profile=profile_name)
            return False

        try:
            # Create snapshot before switch
            if self.snapshot_enabled:
                await self.create_snapshot(
                    description=f"Pre-switch snapshot (switching to {profile_name})",
                    tags=["profile_switch", "backup"],
                )

            # Switch profile
            old_profile = self.current_profile
            self.current_profile = profile_name

            # Reload configuration with new profile
            await self._load_configuration()

            # Validate new configuration
            if self.current_config is None:
                # Rollback on load failure
                self.current_profile = old_profile
                await self._load_configuration()
                logger.error("profile_switch_failed_no_config", profile=profile_name)
                return False

            validation_result = await self._validate_configuration(self.current_config)
            if not validation_result.is_valid:
                # Rollback on validation failure
                self.current_profile = old_profile
                await self._load_configuration()
                logger.error(
                    "profile_switch_validation_failed",
                    profile=profile_name,
                    errors=validation_result.errors,
                )
                return False

            # Track change
            if self.change_tracking_enabled:
                change = ConfigChange(
                    key="profile",
                    change_type=ConfigChangeType.UPDATED,
                    old_value=old_profile,
                    new_value=profile_name,
                    source="profile_switch",
                    reason=f"Switched from {old_profile} to {profile_name}",
                )
                await self._track_change(change)

            logger.info("profile_switched", old_profile=old_profile, new_profile=profile_name)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("profile_switch_failed", profile=profile_name)
            return False
        else:
            return True

    async def create_snapshot(self, description: str = "", tags: list[str] | None = None) -> str:
        """Create configuration snapshot."""
        snapshot_id = f"snapshot_{int(time.time())}"

        if self.current_config is None:
            raise ValueError

        # Use current_profile (already a string)
        profile_name = self.current_profile

        snapshot = ConfigSnapshot(
            snapshot_id=snapshot_id,
            timestamp=time.time(),
            profile=profile_name,
            configuration=self.current_config.to_dict(),
            description=description,
            tags=tags or [],
        )

        # Add recent changes
        recent_changes = self.change_history[-10:]  # Last 10 changes
        snapshot.changes = recent_changes

        # Store snapshot
        self.config_snapshots[snapshot_id] = snapshot

        # Persist snapshot
        await self._persist_snapshot(snapshot)

        # Cleanup old snapshots
        max_snaps = self.max_snapshots
        if len(self.config_snapshots) > max_snaps:
            await self._cleanup_old_snapshots()

        logger.info(
            "config_snapshot_created", snapshot_id=snapshot_id, description=description, tags=tags
        )

        return snapshot_id

    async def restore_snapshot(self, snapshot_id: str) -> bool:
        """Restore configuration from snapshot."""
        if snapshot_id not in self.config_snapshots:
            logger.error("snapshot_not_found", snapshot_id=snapshot_id)
            return False

        try:
            snapshot = self.config_snapshots[snapshot_id]

            # Create backup snapshot
            backup_snapshot_id = await self.create_snapshot(
                description=f"Backup before restoring {snapshot_id}", tags=["restore", "backup"]
            )

            # Restore configuration
            new_config = PortfolioConfiguration()
            new_config.merge_with_dict(snapshot.configuration)

            # Validate restored configuration
            validation_result = await self._validate_configuration(new_config)
            if not validation_result.is_valid:
                logger.error(
                    "snapshot_restore_validation_failed",
                    snapshot_id=snapshot_id,
                    errors=validation_result.errors,
                )
                return False

            # Apply restored configuration
            self.current_config = new_config
            self.current_profile = snapshot.profile

            # Track restore
            if self.change_tracking_enabled:
                change = ConfigChange(
                    key="*",
                    change_type=ConfigChangeType.RELOADED,
                    source="snapshot_restore",
                    reason=f"Restored from snapshot {snapshot_id}",
                )
                await self._track_change(change)

            # Persist restored configuration
            await self._persist_configuration()

            logger.info(
                "config_snapshot_restored",
                snapshot_id=snapshot_id,
                backup_snapshot_id=backup_snapshot_id,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("snapshot_restore_failed", snapshot_id=snapshot_id)
            return False
        else:
            return True

    async def subscribe_to_changes(
        self, key_pattern: str, callback: Callable[[str, Any, Any], None]
    ) -> None:
        """Subscribe to configuration changes."""
        if key_pattern not in self.change_subscribers:
            self.change_subscribers[key_pattern] = []

        self.change_subscribers[key_pattern].append(callback)

        logger.info(
            "config_change_subscription_added", key_pattern=key_pattern, callback=callback.__name__
        )

    async def unsubscribe_from_changes(
        self, key_pattern: str, callback: Callable[[str, Any, Any], None]
    ) -> None:
        """Unsubscribe from configuration changes."""
        if (
            key_pattern in self.change_subscribers
            and callback in self.change_subscribers[key_pattern]
        ):
            self.change_subscribers[key_pattern].remove(callback)

            if not self.change_subscribers[key_pattern]:
                del self.change_subscribers[key_pattern]

            logger.info(
                "config_change_subscription_removed",
                key_pattern=key_pattern,
                callback=callback.__name__,
            )

    async def get_change_history(
        self, limit: int | None = None, since: float | None = None, key_filter: str | None = None
    ) -> list[ConfigChange]:
        """Get configuration change history."""
        changes = self.change_history

        # Apply filters
        if since is not None:
            changes = [c for c in changes if c.timestamp >= since]

        if key_filter is not None:
            changes = [c for c in changes if key_filter in c.key]

        # Sort by timestamp (newest first)
        changes.sort(key=lambda x: x.timestamp, reverse=True)

        # Apply limit
        if limit is not None:
            changes = changes[:limit]

        return changes

    async def get_config_statistics(self) -> dict[str, object]:
        """Get configuration management statistics."""
        return {
            **self.config_statistics,
            "profiles_count": len(self.config_profiles),
            "sources_count": len(self.config_sources),
            "snapshots_count": len(self.config_snapshots),
            "change_history_count": len(self.change_history),
            "subscribers_count": len(self.change_subscribers),
            "current_profile": self.current_profile,
            "validation_enabled": self.validation_enabled,
            "auto_reload_enabled": self.auto_reload_enabled,
        }

    async def _load_profiles(self) -> None:
        """Load configuration profiles."""
        # Create default profile if not exists
        if "default" not in self.config_profiles:
            self.config_profiles["default"] = ConfigProfile(
                name="default", description="Default configuration profile", active=True
            )

        # Load profiles from files
        profiles_dir = Path(str(self.profiles_directory))
        if profiles_dir.exists():
            for profile_file in profiles_dir.glob("*.json"):
                try:

                    def _load_profile(file_path: Path) -> dict[str, object]:
                        with file_path.open(encoding="utf-8") as f:
                            return json.load(f)  # type: ignore[no-any-return]

                    profile_data = await asyncio.to_thread(_load_profile, profile_file)

                    # Type-safe profile data extraction with proper casting
                    profile_name = profile_data.get("name", "")
                    profile_desc = profile_data.get("description", "")
                    profile_base_raw = profile_data.get("base_config", {})
                    profile_overrides_raw = profile_data.get("overrides", {})
                    profile_base = (
                        cast(dict[str, object], profile_base_raw)
                        if isinstance(profile_base_raw, dict)
                        else {}
                    )
                    profile_overrides = (
                        cast(dict[str, object], profile_overrides_raw)
                        if isinstance(profile_overrides_raw, dict)
                        else {}
                    )
                    profile_active = profile_data.get("active", False)

                    profile = ConfigProfile(
                        name=str(profile_name) if profile_name is not None else "",
                        description=str(profile_desc) if profile_desc is not None else "",
                        base_config=profile_base,
                        overrides=profile_overrides,
                        active=bool(profile_active) if profile_active is not None else False,
                    )

                    self.config_profiles[profile.name] = profile

                except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                    logger.exception("profile_loading_failed", file=str(profile_file))

    async def _load_sources(self) -> None:
        """Load configuration sources."""
        # Add default sources
        self.config_sources["default"] = ConfigSourceDescriptor(
            name="default",
            source_type=ConfigSource.DEFAULT,
            location="built-in",
            format=ConfigFormat.JSON,
            priority=1000,
        )

        # Add file sources
        config_dir = Path(str(self.config_directory))
        if config_dir.exists():
            for config_file in config_dir.glob("*.json"):
                source_name = config_file.stem
                self.config_sources[source_name] = ConfigSourceDescriptor(
                    name=source_name,
                    source_type=ConfigSource.FILE,
                    location=str(config_file),
                    format=ConfigFormat.JSON,
                    priority=100,
                )

        # Add environment source
        self.config_sources["environment"] = ConfigSourceDescriptor(
            name="environment",
            source_type=ConfigSource.ENVIRONMENT,
            location="environment",
            format=ConfigFormat.ENV,
            priority=10,
        )

    async def _load_configuration(self) -> None:
        """Load configuration from sources."""
        # Start with default configuration
        if self.current_profile == "development":
            config = PortfolioConfiguration.create_development()
        elif self.current_profile == "production":
            config = PortfolioConfiguration.create_production()
        elif self.current_profile == "test":
            config = PortfolioConfiguration.create_test()
        else:
            config = PortfolioConfiguration.create_default()

        # Get profile for dict access
        profile_key = self.current_profile
        profile = self.config_profiles.get(profile_key)
        if profile:
            # Apply profile base config
            config.merge_with_dict(profile.base_config)

            # Apply profile overrides
            config.merge_with_dict(profile.overrides)

        # Apply sources in priority order
        sorted_sources = sorted(self.config_sources.values(), key=lambda x: x.priority)

        for source in sorted_sources:
            if not source.enabled:
                continue

            try:
                source_config = await self._load_source_config(source)
                if source_config:
                    config.merge_with_dict(source_config)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("source_config_loading_failed", source=source.name)

        self.current_config = config

    async def _load_source_config(self, source: ConfigSourceDescriptor) -> dict[str, object] | None:
        """Load configuration from a specific source."""
        if source.source_type == ConfigSource.FILE:
            if Path(source.location).exists():

                def _load_source() -> dict[str, object]:
                    with Path(source.location).open(encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            raise ConfigurationError(source.source_type.value)
                        return cast(dict[str, object], data)

                return await asyncio.to_thread(_load_source)

        elif source.source_type == ConfigSource.ENVIRONMENT:
            # Load from environment variables
            env_config: dict[str, object] = {}
            for key, value in os.environ.items():
                if key.startswith("PORTFOLIO_"):
                    config_key = key[10:].lower().replace("_", ".")
                    env_config[config_key] = value
            return env_config

        return None

    async def _validate_configuration(
        self, config: PortfolioConfiguration
    ) -> ConfigValidationResult:
        """Validate configuration."""
        if not self.validation_enabled:
            return ConfigValidationResult(is_valid=True)

        # Use built-in validation
        validation_errors = config.validate()

        result = ConfigValidationResult(
            is_valid=len(validation_errors) == 0, errors=validation_errors
        )

        # Update statistics
        if not result.is_valid:
            self.config_statistics["validation_errors"] += 1

        return result

    async def _initialize_validation_rules(self) -> None:
        """Initialize validation rules."""
        # This would contain custom validation rules

    async def _track_change(self, change: ConfigChange) -> None:
        """Track configuration change."""
        self.change_history.append(change)

        # Limit history size
        if len(self.change_history) > self.max_change_history:
            self.change_history = self.change_history[-self.max_change_history :]

    async def _notify_change_subscribers(
        self, key: str, old_value: object, new_value: object
    ) -> None:
        """Notify change subscribers."""
        for pattern, callbacks in self.change_subscribers.items():
            if pattern == "*" or pattern in key:
                for callback in callbacks:
                    try:
                        callback(key, old_value, new_value)
                    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                        logger.exception(
                            "change_subscriber_callback_failed",
                            pattern=pattern,
                            callback=callback.__name__,
                        )

    async def _persist_configuration(self) -> None:
        """Persist current configuration."""
        if self.current_config is None:
            return

        # Save to profile file
        profile_file = Path(str(self.profiles_directory)) / f"{self.current_profile}.json"

        try:
            profile_data = {
                "name": self.current_profile,
                "description": f"Configuration for {self.current_profile} profile",
                "base_config": self.current_config.to_dict(),
                "overrides": {},
                "active": True,
            }

            def _save_profile() -> None:
                with Path(profile_file).open("w", encoding="utf-8") as f:
                    json.dump(profile_data, f, indent=2)

            await asyncio.to_thread(_save_profile)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("configuration_persistence_failed", profile=self.current_profile)

    async def _persist_snapshot(self, snapshot: ConfigSnapshot) -> None:
        """Persist configuration snapshot."""
        snapshot_file = Path(str(self.snapshots_directory)) / f"{snapshot.snapshot_id}.json"

        try:
            snapshot_data = {
                "snapshot_id": snapshot.snapshot_id,
                "timestamp": snapshot.timestamp,
                "profile": snapshot.profile,
                "configuration": snapshot.configuration,
                "description": snapshot.description,
                "tags": snapshot.tags,
                "changes": [
                    {
                        "key": c.key,
                        "change_type": c.change_type.value,
                        "old_value": c.old_value,
                        "new_value": c.new_value,
                        "timestamp": c.timestamp,
                        "source": c.source,
                        "user": c.user,
                        "reason": c.reason,
                    }
                    for c in snapshot.changes
                ],
            }

            def _save_snapshot() -> None:
                with Path(snapshot_file).open("w", encoding="utf-8") as f:
                    json.dump(snapshot_data, f, indent=2)

            await asyncio.to_thread(_save_snapshot)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("snapshot_persistence_failed", snapshot_id=snapshot.snapshot_id)

    async def _cleanup_old_snapshots(self) -> None:
        """Clean up old snapshots."""
        if len(self.config_snapshots) <= self.max_snapshots:
            return

        # Sort by timestamp and remove oldest
        sorted_snapshots = sorted(self.config_snapshots.items(), key=lambda x: x[1].timestamp)

        to_remove = sorted_snapshots[: len(self.config_snapshots) - self.max_snapshots]

        for snapshot_id, _snapshot in to_remove:
            # Remove from memory
            del self.config_snapshots[snapshot_id]

            # Remove file
            snapshot_file = Path(str(self.snapshots_directory)) / f"{snapshot_id}.json"
            if snapshot_file.exists():
                snapshot_file.unlink()

    def _should_reload(self) -> bool:
        """Check if configuration should be reloaded."""
        # Check if any source files have changed
        for source in self.config_sources.values():
            if (
                source.source_type == ConfigSource.FILE
                and source.watch_enabled
                and Path(source.location).exists()
            ):
                file_mtime = Path(source.location).stat().st_mtime
                if file_mtime > source.last_modified:
                    source.last_modified = file_mtime
                    return True

        return False

    async def _start_file_watchers(self) -> None:
        """Start file watchers for configuration files."""
        # This would implement file watching using watchdog or similar

    async def _run_reload_monitor(self) -> None:
        """Background task for monitoring configuration reloads."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                if self._should_reload():
                    await self.reload_configuration()

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("reload_monitor_error")
                await asyncio.sleep(60)

    async def _run_cleanup_monitor(self) -> None:
        """Background task for cleanup operations."""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour

                # Cleanup old snapshots
                await self._cleanup_old_snapshots()

                # Cleanup old change history
                if len(self.change_history) > self.max_change_history:
                    self.change_history = self.change_history[-self.max_change_history :]

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("cleanup_monitor_error")
                await asyncio.sleep(3600)
