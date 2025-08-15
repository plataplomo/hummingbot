"""Configuration for WebSocket error handling system.

Provides configuration options for the new type-safe WebSocket error system,
including recovery strategies, metrics collection, and monitoring settings.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from cyberdelta.config.utils.config_overrides import apply_config_overrides


class WebSocketErrorRecoveryConfig(BaseModel):
    """Configuration for WebSocket error recovery strategies."""

    # ========================================================================
    # Retry Configuration
    # ========================================================================
    max_recovery_attempts: int = Field(
        default=3, ge=0, le=10, description="Maximum number of recovery attempts before giving up"
    )

    initial_backoff_ms: int = Field(
        default=1000, ge=100, le=60000, description="Initial backoff delay in milliseconds"
    )

    max_backoff_ms: int = Field(
        default=60000, ge=1000, le=300000, description="Maximum backoff delay in milliseconds"
    )

    backoff_multiplier: float = Field(
        default=2.0, ge=1.0, le=10.0, description="Multiplier for exponential backoff"
    )

    jitter_enabled: bool = Field(default=True, description="Add random jitter to backoff delays")

    jitter_factor: float = Field(
        default=0.1, ge=0.0, le=0.5, description="Jitter factor (0.1 = ±10% random variation)"
    )

    # ========================================================================
    # Reconnection Configuration
    # ========================================================================
    max_reconnect_attempts: int = Field(
        default=5, ge=0, le=20, description="Maximum reconnection attempts"
    )

    reconnect_delay_ms: int = Field(
        default=5000, ge=1000, le=60000, description="Delay between reconnection attempts"
    )

    switch_endpoint_after_failures: int = Field(
        default=3, ge=1, le=10, description="Switch to different endpoint after N failures"
    )

    # ========================================================================
    # Circuit Breaker Configuration
    # ========================================================================
    circuit_breaker_enabled: bool = Field(
        default=True, description="Enable circuit breaker pattern"
    )

    circuit_breaker_threshold: int = Field(
        default=5, ge=1, le=20, description="Number of failures to trigger circuit breaker"
    )

    circuit_breaker_timeout_ms: int = Field(
        default=30000, ge=5000, le=300000, description="Circuit breaker timeout in milliseconds"
    )

    circuit_breaker_half_open_requests: int = Field(
        default=1, ge=1, le=5, description="Number of test requests in half-open state"
    )

    circuit_breaker_success_threshold: int = Field(
        default=3, ge=1, le=10, description="Number of successes to close circuit breaker"
    )

    # ========================================================================
    # Sequence Gap Handling
    # ========================================================================
    max_acceptable_sequence_gap: int = Field(
        default=100, ge=1, le=10000, description="Maximum acceptable message sequence gap"
    )

    sequence_gap_recovery_enabled: bool = Field(
        default=True, description="Enable automatic recovery from sequence gaps"
    )

    sequence_gap_recovery_method: str = Field(
        default="request_missing",
        description="Recovery method: 'request_missing', 'full_resync', 'ignore'",
    )

    @field_validator("sequence_gap_recovery_method")
    @classmethod
    def validate_recovery_method(cls, v: str) -> str:
        """Validate sequence gap recovery method.

        Returns:
            The validated recovery method string.

        Raises:
            ValueError: If the recovery method is not valid.
        """
        valid_methods = {"request_missing", "full_resync", "ignore"}
        if v not in valid_methods:
            msg = f"Invalid recovery method: {v}. Must be one of {valid_methods}"
            raise ValueError(msg)
        return v

    # ========================================================================
    # Strategy Selection (Unified Recovery System)
    # ========================================================================
    enable_adaptive_strategy: bool = Field(
        default=False, description="Enable adaptive strategy selection based on error patterns"
    )

    prefer_reconnect_for_connection_errors: bool = Field(
        default=True, description="Prefer reconnection for connection-related errors"
    )

    prefer_resubscribe_for_subscription_errors: bool = Field(
        default=True, description="Prefer resubscription for subscription-related errors"
    )


class WebSocketErrorMetricsConfig(BaseModel):
    """Configuration for WebSocket error metrics collection."""

    # ========================================================================
    # Metrics Collection
    # ========================================================================
    enable_metrics_collection: bool = Field(
        default=True, description="Enable error metrics collection"
    )

    metrics_buffer_size: int = Field(
        default=1000, ge=100, le=10000, description="Size of metrics buffer"
    )

    metrics_flush_interval_ms: int = Field(
        default=60000, ge=5000, le=300000, description="Metrics flush interval in milliseconds"
    )

    # ========================================================================
    # Error Rate Tracking
    # ========================================================================
    track_error_rates: bool = Field(default=True, description="Track error rates over time")

    error_rate_window_ms: int = Field(
        default=300000,  # 5 minutes
        ge=60000,
        le=3600000,
        description="Error rate calculation window in milliseconds",
    )

    error_rate_buckets: int = Field(
        default=10, ge=5, le=60, description="Number of time buckets for error rate calculation"
    )

    # ========================================================================
    # Performance Metrics
    # ========================================================================
    track_recovery_times: bool = Field(default=True, description="Track recovery time metrics")

    track_connection_durations: bool = Field(
        default=True, description="Track connection duration metrics"
    )

    track_message_latencies: bool = Field(default=True, description="Track message latency metrics")

    latency_histogram_buckets: int = Field(
        default=10, ge=1, le=100, description="Number of buckets for latency histogram metrics"
    )

    # ========================================================================
    # Detailed Tracking
    # ========================================================================
    track_error_chains: bool = Field(
        default=True, description="Track full error chains for root cause analysis"
    )

    max_error_chain_depth: int = Field(
        default=10, ge=1, le=50, description="Maximum depth of error chain tracking"
    )

    track_stack_traces: bool = Field(
        default=False,  # Off by default for performance
        description="Capture stack traces for errors",
    )


class WebSocketErrorAlertingConfig(BaseModel):
    """Configuration for WebSocket error alerting."""

    # ========================================================================
    # Alert Thresholds
    # ========================================================================
    enable_alerting: bool = Field(default=True, description="Enable error alerting")

    critical_error_threshold: int = Field(
        default=1, ge=1, le=10, description="Number of critical errors to trigger alert"
    )

    error_rate_alert_threshold: float = Field(
        default=0.1,  # 10%
        ge=0.01,
        le=1.0,
        description="Error rate threshold to trigger alert",
    )

    connection_failure_alert_threshold: int = Field(
        default=3, ge=1, le=10, description="Consecutive connection failures to trigger alert"
    )

    # ========================================================================
    # Alert Cooldown
    # ========================================================================
    alert_cooldown_ms: int = Field(
        default=300000,  # 5 minutes
        ge=60000,
        le=3600000,
        description="Cooldown period between similar alerts",
    )

    alert_aggregation_window_ms: int = Field(
        default=60000,  # 1 minute
        ge=10000,
        le=300000,
        description="Window for aggregating similar alerts",
    )

    # ========================================================================
    # Alert Channels
    # ========================================================================
    log_alerts: bool = Field(default=True, description="Log alerts to error logger")

    console_alerts: bool = Field(default=False, description="Print alerts to console")

    webhook_alerts: bool = Field(default=False, description="Send alerts to webhook")

    webhook_url: str | None = Field(default=None, description="Webhook URL for alerts")


class WebSocketErrorLoggingConfig(BaseModel):
    """Configuration for WebSocket error logging."""

    # ========================================================================
    # Log Levels
    # ========================================================================
    log_all_errors: bool = Field(default=False, description="Log all errors regardless of severity")

    min_severity_to_log: str = Field(default="WARNING", description="Minimum severity level to log")

    log_recovery_attempts: bool = Field(default=True, description="Log recovery attempt details")

    log_error_context: bool = Field(default=True, description="Include full error context in logs")

    # ========================================================================
    # Log Formatting
    # ========================================================================
    structured_logging: bool = Field(default=True, description="Use structured JSON logging")

    include_timestamps: bool = Field(default=True, description="Include timestamps in logs")

    include_correlation_ids: bool = Field(
        default=True, description="Include correlation IDs in logs"
    )

    # ========================================================================
    # Log Filtering
    # ========================================================================
    filter_sensitive_data: bool = Field(default=True, description="Filter sensitive data from logs")

    max_log_message_length: int = Field(
        default=10000, ge=100, le=100000, description="Maximum log message length"
    )

    @field_validator("min_severity_to_log")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        """Validate severity level.

        Returns:
            The validated severity level string.

        Raises:
            ValueError: If the severity level is not valid.
        """
        valid_severities = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"}
        if v.upper() not in valid_severities:
            msg = f"Invalid severity: {v}. Must be one of {valid_severities}"
            raise ValueError(msg)
        return v.upper()


class WebSocketErrorRouterConfig(BaseModel):
    """Configuration for WebSocket router error handling."""

    # ========================================================================
    # Envelope Validation Configuration
    # ========================================================================
    strict_envelope_validation: bool = Field(
        default=True, description="Enable strict envelope validation"
    )

    log_envelope_validation_failures: bool = Field(
        default=True, description="Log envelope validation failures"
    )

    envelope_validation_timeout_ms: int = Field(
        default=500, ge=50, le=5000, description="Timeout for envelope validation in milliseconds"
    )

    # ========================================================================
    # Routing Key Configuration
    # ========================================================================
    allow_empty_routing_keys: bool = Field(
        default=False, description="Allow messages with empty routing keys"
    )

    routing_key_max_length: int = Field(
        default=100, ge=1, le=1000, description="Maximum allowed routing key length"
    )

    log_missing_routing_keys: bool = Field(
        default=True, description="Log when routing keys cannot be extracted"
    )

    # ========================================================================
    # Processor and Handler Configuration
    # ========================================================================
    log_missing_processors: bool = Field(
        default=True, description="Log when processors are not found for routing keys"
    )

    log_missing_handlers: bool = Field(
        default=True, description="Log when handlers are not found for routing keys"
    )

    processor_lookup_timeout_ms: int = Field(
        default=100, ge=10, le=1000, description="Timeout for processor lookup in milliseconds"
    )

    handler_lookup_timeout_ms: int = Field(
        default=100, ge=10, le=1000, description="Timeout for handler lookup in milliseconds"
    )

    # ========================================================================
    # Message Send Configuration
    # ========================================================================
    enable_message_send_error_tracking: bool = Field(
        default=True, description="Track message send failures"
    )

    message_send_timeout_ms: int = Field(
        default=5000, ge=100, le=30000, description="Timeout for message sending in milliseconds"
    )

    message_send_retry_attempts: int = Field(
        default=2, ge=0, le=10, description="Number of retry attempts for failed message sends"
    )

    # ========================================================================
    # Performance Configuration
    # ========================================================================
    enable_routing_performance_tracking: bool = Field(
        default=True, description="Track routing performance metrics"
    )

    routing_performance_warning_threshold_ms: int = Field(
        default=100, ge=1, le=10000, description="Routing time threshold for performance warnings"
    )

    max_concurrent_routing_operations: int = Field(
        default=100, ge=1, le=1000, description="Maximum concurrent routing operations"
    )

    # ========================================================================
    # Context Creation Configuration
    # ========================================================================
    enable_enhanced_error_contexts: bool = Field(
        default=True, description="Include enhanced information in error contexts"
    )

    include_message_metadata_in_contexts: bool = Field(
        default=True, description="Include message metadata in error contexts"
    )

    context_creation_timeout_ms: int = Field(
        default=200,
        ge=10,
        le=2000,
        description="Timeout for error context creation in milliseconds",
    )

    max_context_extra_data_size_bytes: int = Field(
        default=10240,  # 10KB
        ge=1024,
        le=102400,
        description="Maximum size of extra context data in bytes",
    )


class WebSocketErrorConfig(BaseModel):
    """Complete configuration for WebSocket error system."""

    # ========================================================================
    # Sub-configurations
    # ========================================================================
    recovery: WebSocketErrorRecoveryConfig = Field(
        default_factory=WebSocketErrorRecoveryConfig, description="Error recovery configuration"
    )

    metrics: WebSocketErrorMetricsConfig = Field(
        default_factory=WebSocketErrorMetricsConfig, description="Metrics collection configuration"
    )

    alerting: WebSocketErrorAlertingConfig = Field(
        default_factory=WebSocketErrorAlertingConfig, description="Error alerting configuration"
    )

    logging: WebSocketErrorLoggingConfig = Field(
        default_factory=WebSocketErrorLoggingConfig, description="Error logging configuration"
    )

    # ========================================================================
    # Global Settings
    # ========================================================================
    enabled: bool = Field(default=True, description="Enable new error system")

    validate_contexts: bool = Field(
        default=True, description="Validate error contexts before processing"
    )

    sanitize_invalid_contexts: bool = Field(
        default=True, description="Sanitize invalid contexts instead of rejecting"
    )

    # ========================================================================
    # Performance Settings
    # ========================================================================
    async_error_handling: bool = Field(
        default=True, description="Handle errors asynchronously where possible"
    )

    error_handling_timeout_ms: int = Field(
        default=5000, ge=100, le=30000, description="Timeout for error handling operations"
    )

    max_concurrent_error_handlers: int = Field(
        default=10, ge=1, le=100, description="Maximum concurrent error handlers"
    )

    # ========================================================================
    # Router-Specific Configuration
    # ========================================================================
    router: WebSocketErrorRouterConfig = Field(
        default_factory=WebSocketErrorRouterConfig,
        description="Router-specific error configuration",
    )

    # ========================================================================
    # Exchange-Specific Overrides
    # ========================================================================
    exchange_overrides: dict[str, dict[str, object]] = Field(
        default_factory=dict, description="Exchange-specific configuration overrides"
    )

    def get_exchange_config(self, exchange: str) -> WebSocketErrorConfig:
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
        return WebSocketErrorConfig.model_validate(config_dict)

    class Config:
        """Pydantic configuration."""

        frozen = False
        validate_assignment = True
