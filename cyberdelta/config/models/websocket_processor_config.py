"""Configuration for WebSocket processor system.

Provides configuration options for WebSocket message processing,
including integration with the new type-safe error system.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from cyberdelta.config.utils.config_overrides import apply_config_overrides


class ProcessorMetricsConfig(BaseModel):
    """Configuration for processor metrics collection."""

    enabled: bool = Field(default=True, description="Enable processor metrics collection")

    log_interval_messages: int = Field(
        default=1000, ge=100, le=10000, description="Log metrics every N messages"
    )

    track_processing_times: bool = Field(default=True, description="Track message processing times")

    track_transformation_times: bool = Field(
        default=True, description="Track message transformation times"
    )

    track_handler_times: bool = Field(
        default=True, description="Track message handler execution times"
    )


class ProcessorErrorHandlingConfig(BaseModel):
    """Configuration for processor error handling."""

    use_typed_error_system: bool = Field(
        default=True, description="Use new typed WebSocket error system"
    )

    fallback_to_legacy: bool = Field(
        default=True, description="Fall back to legacy error handling if typed system unavailable"
    )

    log_validation_errors: bool = Field(default=True, description="Log validation errors")

    log_transformation_errors: bool = Field(default=True, description="Log transformation errors")

    log_handler_errors: bool = Field(default=True, description="Log handler invocation errors")

    include_payload_in_errors: bool = Field(
        default=False,
        description="Include raw payload in error logging (may contain sensitive data)",
    )


class ProcessorPerformanceConfig(BaseModel):
    """Configuration for processor performance optimization."""

    enable_caching: bool = Field(default=True, description="Enable caching of transformed models")

    cache_size: int = Field(
        default=1000, ge=100, le=10000, description="Maximum number of cached transformed models"
    )

    cache_ttl_seconds: int = Field(
        default=300, ge=10, le=3600, description="Cache time-to-live in seconds"
    )

    enable_batch_processing: bool = Field(
        default=False, description="Enable batch processing of messages"
    )

    batch_size: int = Field(
        default=10, ge=1, le=100, description="Maximum batch size for batch processing"
    )

    batch_timeout_ms: int = Field(
        default=100, ge=10, le=1000, description="Maximum time to wait for batch to fill"
    )


class WebSocketProcessorConfig(BaseModel):
    """Complete configuration for WebSocket processor system."""

    # ========================================================================
    # Sub-configurations
    # ========================================================================
    metrics: ProcessorMetricsConfig = Field(
        default_factory=ProcessorMetricsConfig, description="Metrics collection configuration"
    )

    error_handling: ProcessorErrorHandlingConfig = Field(
        default_factory=ProcessorErrorHandlingConfig, description="Error handling configuration"
    )

    performance: ProcessorPerformanceConfig = Field(
        default_factory=ProcessorPerformanceConfig,
        description="Performance optimization configuration",
    )

    # ========================================================================
    # Global Settings
    # ========================================================================
    enabled: bool = Field(default=True, description="Enable WebSocket processor")

    max_concurrent_processors: int = Field(
        default=10, ge=1, le=100, description="Maximum concurrent processor instances"
    )

    processor_timeout_ms: int = Field(
        default=5000, ge=100, le=30000, description="Timeout for individual message processing"
    )

    # ========================================================================
    # Exchange-Specific Overrides
    # ========================================================================
    exchange_overrides: dict[str, dict[str, object]] = Field(
        default_factory=dict, description="Exchange-specific processor configuration overrides"
    )

    def get_exchange_config(self, exchange: str) -> WebSocketProcessorConfig:
        """Get configuration with exchange-specific overrides applied.

        Args:
            exchange: Exchange name

        Returns:
            Configuration with overrides applied
        """
        if exchange not in self.exchange_overrides:
            return self

        # Create a copy and apply overrides
        config_dict = self.model_dump()
        overrides = self.exchange_overrides[exchange]

        # Apply overrides recursively
        apply_config_overrides(config_dict, overrides)
        return WebSocketProcessorConfig.model_validate(config_dict)

    def should_use_typed_errors(self, exchange: str | None = None) -> bool:
        """Check if typed error system should be used.

        Args:
            exchange: Optional exchange name for specific config

        Returns:
            True if typed error system should be used
        """
        if exchange:
            config = self.get_exchange_config(exchange)
            return config.error_handling.use_typed_error_system
        return self.error_handling.use_typed_error_system

    class Config:
        """Pydantic configuration."""

        frozen = False
        validate_assignment = True
