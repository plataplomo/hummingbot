"""Infrastructure configuration domain objects with Pydantic validation.

This module provides comprehensive domain objects to replace boolean matrices
in infrastructure configuration, making system behavior explicit and validatable.
"""

from __future__ import annotations

import warnings
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cyberdelta.apis.exceptions.configuration_validation import (
    CachingPolicyError,
    PerformanceProfileError,
    ThreatModelError,
)


# Cache duration constants
MAX_DEVELOPMENT_CACHE_DURATION = 30.0  # seconds
MIN_AGGRESSIVE_CACHE_DURATION = 60.0  # seconds

# Request weight threshold for authentication warnings
HIGH_WEIGHT_THRESHOLD = 10


class PerformanceProfile(Enum):
    """Performance profile for system configuration.

    Replaces multiple boolean performance flags with explicit profiles.
    """

    ULTRA_FAST = "ultra_fast"
    """Minimum validation, maximum speed - for trading critical paths."""

    BALANCED = "balanced"
    """Standard validation, good performance - default configuration."""

    SECURE = "secure"
    """Maximum validation, security over speed - for sensitive operations."""

    DEBUG = "debug"
    """All checks enabled, detailed logging - for development only."""


class ValidationMode(Enum):
    """Validation mode for data processing.

    Replaces boolean validation flags with explicit validation levels.
    """

    MINIMAL = "minimal"
    """Basic type checking only - fastest validation."""

    STANDARD = "standard"
    """Normal Pydantic validation - balanced approach."""

    STRICT = "strict"
    """All validation enabled - comprehensive checking."""

    PARANOID = "paranoid"
    """Maximum validation with runtime checks - slowest but safest."""


class MemoryStrategy(Enum):
    """Memory management strategy for system operations.

    Replaces boolean memory flags with explicit memory management approaches.
    """

    MINIMAL = "minimal"
    """Lowest memory usage - may impact performance."""

    STANDARD = "standard"
    """Balanced memory vs performance - default strategy."""

    OPTIMIZED = "optimized"
    """Object pooling and caching enabled - higher memory, better performance."""

    UNLIMITED = "unlimited"
    """Maximum performance, memory not constrained - for high-throughput scenarios."""


class ObservabilityLevel(Enum):
    """Observability level for monitoring and telemetry.

    Replaces boolean monitoring flags with explicit observability levels.
    """

    NONE = "none"
    """No monitoring - production fast path."""

    BASIC = "basic"
    """Essential metrics only - minimal overhead."""

    DETAILED = "detailed"
    """Full metrics and tracing - comprehensive monitoring."""

    DEBUG = "debug"
    """All telemetry enabled - for development and debugging."""


class CachingPolicy(Enum):
    """Caching policy for service operations.

    Replaces boolean enable_cache parameter with explicit caching strategies.
    """

    DISABLED = "disabled"
    """No caching - always fetch fresh data."""

    ENABLED = "enabled"
    """Standard caching with configurable duration."""

    AGGRESSIVE = "aggressive"
    """Long-duration caching for stable data."""

    DEVELOPMENT = "development"
    """Short-duration caching for development/testing."""


class CachingConfiguration(BaseModel):
    """Caching configuration with validation.

    Replaces enable_cache boolean with structured caching policy.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    policy: CachingPolicy = Field(
        default=CachingPolicy.ENABLED,
        description="Caching policy determining cache behavior",
    )

    duration_seconds: float = Field(
        default=5.0,
        gt=0,
        le=3600,  # Max 1 hour
        description="Cache duration in seconds",
    )

    @model_validator(mode="after")
    def validate_caching_consistency(self) -> CachingConfiguration:
        """Validate caching configuration consistency.

        Returns:
            Self if all validation checks pass

        Raises:
            CachingPolicyError: If caching policy and duration are inconsistent

        """
        # Development caching should be short duration
        if (
            self.policy == CachingPolicy.DEVELOPMENT
            and self.duration_seconds > MAX_DEVELOPMENT_CACHE_DURATION
        ):
            raise CachingPolicyError(
                policy=self.policy.value,
                duration=self.duration_seconds,
                constraint=f"duration must be <= {MAX_DEVELOPMENT_CACHE_DURATION} seconds",
            )

        # Aggressive caching should be longer duration
        if (
            self.policy == CachingPolicy.AGGRESSIVE
            and self.duration_seconds < MIN_AGGRESSIVE_CACHE_DURATION
        ):
            raise CachingPolicyError(
                policy=self.policy.value,
                duration=self.duration_seconds,
                constraint=f"duration must be >= {MIN_AGGRESSIVE_CACHE_DURATION} seconds",
            )

        # Disabled caching ignores duration
        if self.policy == CachingPolicy.DISABLED:
            warnings.warn(
                "Cache duration ignored when caching is DISABLED",
                UserWarning,
                stacklevel=2,
            )

        return self

    def is_enabled(self) -> bool:
        """Check if caching is enabled.

        Returns:
            True if caching policy is not DISABLED, False otherwise

        """
        return self.policy != CachingPolicy.DISABLED

    def get_effective_duration(self) -> float:
        """Get effective cache duration based on policy.

        Returns:
            Effective cache duration in seconds, adjusted based on caching policy

        """
        if self.policy == CachingPolicy.DISABLED:
            return 0.0
        if self.policy == CachingPolicy.DEVELOPMENT:
            return min(self.duration_seconds, 30.0)
        if self.policy == CachingPolicy.AGGRESSIVE:
            return max(self.duration_seconds, 60.0)
        return self.duration_seconds


class RegistrationMode(Enum):
    """Component registration mode for registries.

    Replaces boolean auto_register parameter with explicit registration strategies.
    """

    MANUAL = "manual"
    """No automatic registration - manual component registration only."""

    DEFAULT_COMPONENTS = "default_components"
    """Register default components only."""

    ALL_AVAILABLE = "all_available"
    """Register all available components automatically."""

    LAZY = "lazy"
    """Register components on first access (lazy loading)."""


class RegistrationConfiguration(BaseModel):
    """Component registration configuration.

    Replaces auto_register boolean with structured registration policy.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    mode: RegistrationMode = Field(
        default=RegistrationMode.DEFAULT_COMPONENTS,
        description="Registration mode for component discovery",
    )

    def should_auto_register(self) -> bool:
        """Check if automatic registration is enabled.

        Returns:
            True if automatic registration is enabled, False for manual mode

        """
        return self.mode != RegistrationMode.MANUAL

    def should_register_defaults(self) -> bool:
        """Check if default components should be registered.

        Returns:
            True if default components should be registered based on the mode

        """
        return self.mode in {
            RegistrationMode.DEFAULT_COMPONENTS,
            RegistrationMode.ALL_AVAILABLE,
            RegistrationMode.LAZY,
        }


class SystemConfiguration(BaseModel):
    """System configuration with performance and validation policies.

    Replaces boolean configuration matrices with structured, validated configuration.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    performance_profile: PerformanceProfile = Field(
        default=PerformanceProfile.BALANCED,
        description="Performance profile for system optimization",
    )

    validation_mode: ValidationMode = Field(
        default=ValidationMode.STANDARD,
        description="Validation mode for data processing",
    )

    memory_strategy: MemoryStrategy = Field(
        default=MemoryStrategy.STANDARD,
        description="Memory management strategy",
    )

    observability_level: ObservabilityLevel = Field(
        default=ObservabilityLevel.BASIC,
        description="Observability level for monitoring",
    )

    @model_validator(mode="after")
    def validate_configuration_consistency(self) -> SystemConfiguration:
        """Ensure configuration settings are internally consistent.

        Returns:
            Self if all configuration settings are consistent

        Raises:
            PerformanceProfileError: If performance profile conflicts with other settings

        """
        # Ultra-fast profile should not have strict validation
        if (
            self.performance_profile == PerformanceProfile.ULTRA_FAST
            and self.validation_mode == ValidationMode.PARANOID
        ):
            raise PerformanceProfileError(
                profile=self.performance_profile.value,
                conflicting_setting="validation_mode",
                setting_value=self.validation_mode.value,
                reason="creates performance contradiction",
            )

        # Debug profile should have detailed observability
        if (
            self.performance_profile == PerformanceProfile.DEBUG
            and self.observability_level == ObservabilityLevel.NONE
        ):
            raise PerformanceProfileError(
                profile=self.performance_profile.value,
                conflicting_setting="observability_level",
                setting_value=self.observability_level.value,
                reason="debugging requires observability",
            )

        # Secure profile should have strict validation
        if (
            self.performance_profile == PerformanceProfile.SECURE
            and self.validation_mode == ValidationMode.MINIMAL
        ):
            warnings.warn(
                "SECURE performance profile with MINIMAL validation may compromise security",
                UserWarning,
                stacklevel=2,
            )

        # Unlimited memory with minimal strategy is contradictory
        if (
            self.memory_strategy == MemoryStrategy.UNLIMITED
            and self.performance_profile == PerformanceProfile.SECURE
        ):
            warnings.warn(
                "UNLIMITED memory strategy with SECURE profile may have security implications",
                UserWarning,
                stacklevel=2,
            )

        return self

    def to_pydantic_config(self) -> dict[str, Any]:
        """Convert to Pydantic ConfigDict settings.

        Returns:
            Dictionary suitable for Pydantic ConfigDict construction.
        """
        config = {
            "frozen": True,
            "extra": "forbid",
        }

        # Map validation mode to Pydantic settings
        if self.validation_mode == ValidationMode.MINIMAL:
            config.update({
                "validate_assignment": False,
                "validate_default": False,
                "str_strip_whitespace": False,
            })
        elif self.validation_mode == ValidationMode.STANDARD:
            config.update({
                "validate_assignment": True,
                "validate_default": True,
                "str_strip_whitespace": True,
            })
        elif self.validation_mode == ValidationMode.STRICT:
            config.update({
                "validate_assignment": True,
                "validate_default": True,
                "str_strip_whitespace": True,
                "arbitrary_types_allowed": False,
            })
        elif self.validation_mode == ValidationMode.PARANOID:
            config.update({
                "validate_assignment": True,
                "validate_default": True,
                "str_strip_whitespace": True,
                "arbitrary_types_allowed": False,
                "use_enum_values": False,  # Keep enum objects for type safety
            })

        # Map performance profile to optimizations
        if self.performance_profile == PerformanceProfile.ULTRA_FAST:
            config.update({
                "validate_assignment": False,  # Override for speed
                "defer_build": True,
            })
        elif self.performance_profile == PerformanceProfile.DEBUG:
            config.update({
                "validate_assignment": True,
                "extra": "forbid",
                "arbitrary_types_allowed": False,
            })

        return config


class ErrorRecoveryMode(Enum):
    """Error recovery mode for WebSocket operations.

    Replaces the boolean `enable_error_recovery` parameter.
    """

    ENABLED = "enabled"
    """Error recovery is enabled - attempt to recover from errors."""

    DISABLED = "disabled"
    """Error recovery is disabled - fail fast on errors."""

    @property
    def is_enabled(self) -> bool:
        """Check if error recovery is enabled."""
        return self == ErrorRecoveryMode.ENABLED


class MemoryOptimizationMode(Enum):
    """Memory optimization mode for WebSocket operations.

    Replaces the boolean `enable_memory_optimization` parameter.
    """

    ENABLED = "enabled"
    """Memory optimization is enabled - use memory-efficient strategies."""

    DISABLED = "disabled"
    """Memory optimization is disabled - prioritize performance over memory."""

    @property
    def is_enabled(self) -> bool:
        """Check if memory optimization is enabled."""
        return self == MemoryOptimizationMode.ENABLED


class WebSocketConfiguration(BaseModel):
    """WebSocket configuration with infrastructure policies.

    Replaces multiple boolean flags with structured configuration.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    error_recovery_mode: ErrorRecoveryMode = Field(
        default=ErrorRecoveryMode.ENABLED,
        description="Error recovery mode for WebSocket operations",
    )

    memory_optimization_mode: MemoryOptimizationMode = Field(
        default=MemoryOptimizationMode.DISABLED,
        description="Memory optimization mode for WebSocket operations",
    )

    system_config: SystemConfiguration = Field(
        default_factory=SystemConfiguration,
        description="System configuration for WebSocket processing",
    )

    @model_validator(mode="after")
    def validate_websocket_consistency(self) -> WebSocketConfiguration:
        """Validate WebSocket configuration consistency.

        Returns:
            Self if all WebSocket configuration settings are consistent

        """
        # High-performance systems should have memory optimization
        if (
            self.system_config.performance_profile == PerformanceProfile.ULTRA_FAST
            and self.memory_optimization_mode == MemoryOptimizationMode.DISABLED
        ):
            warnings.warn(
                "ULTRA_FAST performance profile should consider enabling memory optimization",
                UserWarning,
                stacklevel=2,
            )

        # Debug systems should have error recovery
        if (
            self.system_config.performance_profile == PerformanceProfile.DEBUG
            and self.error_recovery_mode == ErrorRecoveryMode.DISABLED
        ):
            warnings.warn(
                "DEBUG performance profile should enable error recovery for debugging",
                UserWarning,
                stacklevel=2,
            )

        return self


class RateLimitBehavior(Enum):
    """Rate limit behavior for API operations.

    Replaces the boolean `raise_on_limit` parameter.
    """

    RAISE_ON_LIMIT = "raise_on_limit"
    """Raise exception when rate limit is exceeded."""

    RETURN_RESULT = "return_result"
    """Return result without raising exception, let caller handle."""

    @property
    def should_raise(self) -> bool:
        """Check if this behavior should raise on rate limit exceeded."""
        return self == RateLimitBehavior.RAISE_ON_LIMIT


class ComponentRegistrationMode(Enum):
    """Component registration mode for system components.

    Replaces the boolean `auto_register` parameter.
    """

    AUTO_REGISTER = "auto_register"
    """Automatically register components on creation."""

    MANUAL_REGISTER = "manual_register"
    """Require explicit registration of components."""

    @property
    def is_auto(self) -> bool:
        """Check if auto-registration is enabled."""
        return self == ComponentRegistrationMode.AUTO_REGISTER


class RequestAuthMode(Enum):
    """Request authentication mode for API requests.

    Replaces the boolean `is_signed` parameter.
    """

    SIGNED = "signed"
    """Request requires authentication signature (was is_signed=True)."""

    UNSIGNED = "unsigned"
    """Request does not require authentication (was is_signed=False)."""

    @property
    def requires_signature(self) -> bool:
        """Check if this mode requires authentication signature."""
        return self == RequestAuthMode.SIGNED


class SerializationMode(Enum):
    """Serialization mode for request data.

    Replaces the boolean `serialize_none_as_null` parameter.
    """

    STANDARD = "standard"
    """Standard serialization - omit None values (was serialize_none_as_null=False)."""

    EXPLICIT_NULL = "explicit_null"
    """Explicit null serialization - serialize None as null (was serialize_none_as_null=True)."""

    @property
    def should_serialize_none(self) -> bool:
        """Check if None values should be serialized as null."""
        return self == SerializationMode.EXPLICIT_NULL


class RequestConfiguration(BaseModel):
    """Request configuration for API calls.

    Replaces boolean request parameters with structured configuration.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    auth_mode: RequestAuthMode = Field(
        default=RequestAuthMode.UNSIGNED,
        description="Authentication mode for the request",
    )

    serialization_mode: SerializationMode = Field(
        default=SerializationMode.STANDARD,
        description="Serialization mode for request data",
    )

    endpoint_group: str = Field(
        default="default",
        description="Endpoint group for rate limiting and categorization",
    )

    request_weight: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Weight of the request for rate limiting",
    )

    @model_validator(mode="after")
    def validate_request_consistency(self) -> RequestConfiguration:
        """Validate request configuration consistency.

        Returns:
            Self if all request configuration settings are consistent

        """
        # High weight requests should generally be signed
        if (
            self.request_weight > HIGH_WEIGHT_THRESHOLD
            and self.auth_mode == RequestAuthMode.UNSIGNED
        ):
            message = (
                f"High weight requests (>{HIGH_WEIGHT_THRESHOLD}) should typically be authenticated"
            )
            warnings.warn(message, UserWarning, stacklevel=2)

        # Explicit null serialization often used with authenticated requests
        if (
            self.serialization_mode == SerializationMode.EXPLICIT_NULL
            and self.auth_mode == RequestAuthMode.UNSIGNED
        ):
            warnings.warn(
                "Explicit null serialization typically used with authenticated requests",
                UserWarning,
                stacklevel=2,
            )

        return self


class ReconnectionResult(Enum):
    """Result of a WebSocket reconnection attempt.

    Replaces the boolean `success` parameter in reconnection handling.
    """

    SUCCESS = "success"
    """Connection successfully re-established (was success=True)."""

    FAILED = "failed"
    """Connection attempt failed (was success=False)."""

    PARTIAL = "partial"
    """Connection established but some subscriptions failed."""

    TIMEOUT = "timeout"
    """Connection attempt timed out."""

    @property
    def is_successful(self) -> bool:
        """Check if reconnection was successful."""
        return self == ReconnectionResult.SUCCESS


class SecurityThreatModel(Enum):
    """Security threat model for system operations.

    Replaces boolean security flags with explicit threat modeling.
    """

    DEVELOPMENT = "development"
    """Minimal checks, fast iteration - development only."""

    STANDARD = "standard"
    """Production baseline security - normal operations."""

    PARANOID = "paranoid"
    """Maximum security, performance cost - high-risk environments."""

    AUDITED = "audited"
    """Compliance-grade validation - regulatory environments."""


class MonitoringLevel(Enum):
    """Monitoring level for security operations.

    Replaces the boolean `enable_monitoring` parameter.
    """

    DISABLED = "disabled"
    """No monitoring - development only."""

    BASIC = "basic"
    """Essential monitoring only - minimal overhead."""

    DETAILED = "detailed"
    """Full monitoring and metrics - comprehensive tracking."""

    COMPREHENSIVE = "comprehensive"
    """All monitoring plus real-time analysis - maximum observability."""

    @property
    def is_enabled(self) -> bool:
        """Check if monitoring is enabled."""
        return self != MonitoringLevel.DISABLED


class AuditLevel(Enum):
    """Audit level for security operations.

    Replaces the boolean `enable_audit` parameter.
    """

    DISABLED = "disabled"
    """No audit logging - development only."""

    BASIC = "basic"
    """Essential audit events only - key security actions."""

    DETAILED = "detailed"
    """Full audit trail - all security-relevant operations."""

    FORENSIC = "forensic"
    """Maximum audit detail - complete forensic trail."""

    @property
    def is_enabled(self) -> bool:
        """Check if audit logging is enabled."""
        return self != AuditLevel.DISABLED


class SecurityPolicy(BaseModel):
    """Security policy configuration with validation.

    Replaces multiple boolean security flags with structured, validated policy.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    threat_model: SecurityThreatModel = Field(
        default=SecurityThreatModel.STANDARD,
        description="Security threat model for operations",
    )

    monitoring_level: MonitoringLevel = Field(
        default=MonitoringLevel.BASIC,
        description="Monitoring level for security events",
    )

    audit_level: AuditLevel = Field(
        default=AuditLevel.DISABLED,
        description="Audit level for security operations",
    )

    source_exchange: str | None = Field(
        default=None,
        description="Source exchange for context-aware security",
    )

    @model_validator(mode="after")
    def validate_security_consistency(self) -> SecurityPolicy:
        """Ensure security settings provide adequate protection.

        Returns:
            Self if all security settings are consistent and adequate

        Raises:
            ThreatModelError: If threat model requirements are not met by other settings

        """
        # Paranoid threat model requires comprehensive monitoring and auditing
        if self.threat_model == SecurityThreatModel.PARANOID:
            detailed_levels = {MonitoringLevel.DETAILED, MonitoringLevel.COMPREHENSIVE}
            if self.monitoring_level not in detailed_levels:
                raise ThreatModelError(
                    threat_model=self.threat_model.value,
                    requirement="DETAILED or COMPREHENSIVE monitoring",
                    current_value=self.monitoring_level.value,
                )
            if self.audit_level not in {AuditLevel.DETAILED, AuditLevel.FORENSIC}:
                raise ThreatModelError(
                    threat_model=self.threat_model.value,
                    requirement="DETAILED or FORENSIC audit logging",
                    current_value=self.audit_level.value,
                )

        # Audited environments need strong monitoring and audit
        if self.threat_model == SecurityThreatModel.AUDITED:
            if self.monitoring_level == MonitoringLevel.DISABLED:
                raise ThreatModelError(
                    threat_model=self.threat_model.value,
                    requirement="monitoring cannot be disabled",
                    current_value=self.monitoring_level.value,
                )
            if self.audit_level == AuditLevel.DISABLED:
                raise ThreatModelError(
                    threat_model=self.threat_model.value,
                    requirement="audit logging must be enabled",
                    current_value=self.audit_level.value,
                )

        # Development can have relaxed security but warn about it
        if self.threat_model == SecurityThreatModel.DEVELOPMENT:
            warnings.warn(
                "DEVELOPMENT threat model should not be used in production",
                UserWarning,
                stacklevel=2,
            )

        # Warn about disabled monitoring in production threat models
        production_models = {
            SecurityThreatModel.STANDARD,
            SecurityThreatModel.PARANOID,
            SecurityThreatModel.AUDITED,
        }
        if (
            self.threat_model in production_models
            and self.monitoring_level == MonitoringLevel.DISABLED
        ):
            threat_name = self.threat_model.value.upper()
            message = (
                f"{threat_name} threat model with disabled monitoring creates security blind spots"
            )
            warnings.warn(message, UserWarning, stacklevel=2)

        return self

    def get_legacy_flags(self) -> dict[str, bool]:
        """Convert to legacy boolean flags for backward compatibility.

        Returns:
            Dictionary mapping legacy flag names to boolean values

        """
        return {
            "enable_monitoring": self.monitoring_level.is_enabled,
            "enable_audit": self.audit_level.is_enabled,
        }
