"""General application configuration models.

This module contains general application settings and configuration models
that don't belong to specific subsystems.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.config.models.config_types import NonEmptyConfigString
from cyberdelta.utils.parsing import validate_enum_field, validate_str_field


class AddressActionSafetyNetConfig(BaseModel):
    """Configuration for address-based action safety net rate limiting."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    rate_per_minute: int = Field(
        ...,
        gt=0,
        description="Client-side safety net rate for address-based actions, in actions per minute.",
    )


class GeneralSettings(BaseModel):
    """General application settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    log_level: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file: NonEmptyConfigString | None = None
    module_log_levels: (
        dict[str, Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]] | None
    ) = None
    safe_mode: bool = True
    state_file: NonEmptyConfigString = "data/state.json"
    state_backup_directory: NonEmptyConfigString = "data/state_backups"
    state_save_interval: int = Field(default=300, gt=0)  # seconds
    state_backup_count: int = Field(default=5, gt=0)  # Number of previous state files to keep

    # Missing fields referenced in monitoring modules
    shutdown_grace_period: float = Field(
        default=30.0, gt=0, le=300, description="Graceful shutdown timeout in seconds"
    )
    audit_log_enabled: bool = Field(default=True, description="Enable audit logging")
    log_sensitive_data: bool = Field(
        default=False, description="Enable logging of sensitive data (use with caution)"
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"},
            field_name=info.field_name or "log_level",
        )

    @field_validator("module_log_levels", mode="before")
    @classmethod
    def _validate_module_log_levels(
        cls,
        v: dict[str, str] | list[str] | str | float | bool | None,
        info: ValidationInfo,
    ) -> dict[str, str] | None:
        """Validate module_log_levels dictionary structure and values.

        Args:
            v: The value to validate (dict, list, str, float, bool, or None)
            info: Validation context containing field information

        Returns:
            Validated dictionary of module names to log levels, or None

        Raises:
            TypeError: If value is not a dict or None
        """
        if v is None:
            return None

        if not isinstance(v, dict):
            field_name = info.field_name or "module_log_levels"
            msg = f"{field_name}: Expected dict or None, got {type(v).__name__}"
            raise TypeError(msg)

        validated_levels: dict[str, str] = {}
        allowed_levels = {"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"}

        for raw_key, raw_value in v.items():
            # Validate module name
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'module_log_levels'}.key",
                allow_empty=False,
            )
            # Validate log level value
            validated_value = validate_enum_field(
                raw_value,
                allowed=allowed_levels,
                field_name=f"{info.field_name or 'module_log_levels'}.{raw_key}",
            )
            validated_levels[validated_key] = validated_value

        return validated_levels
