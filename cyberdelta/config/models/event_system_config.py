"""Event system configuration models.

This module defines Pydantic configuration models for the event system,
integrating with the existing AppSettings configuration structure.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class EventRetryConfig(BaseModel):
    """Retry configuration for event handlers and workflows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_attempts: int = Field(default=3, description="Maximum retry attempts")
    initial_delay_sec: float = Field(default=1.0, description="Initial retry delay in seconds")
    max_delay_sec: float = Field(default=60.0, description="Maximum retry delay in seconds")
    exponential_base: float = Field(default=2.0, description="Exponential backoff base")
    jitter: bool = Field(default=True, description="Add jitter to retry delays")


class EventHandlerConfig(BaseModel):
    """Configuration for event handler behavior."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Error handling
    max_consecutive_errors: int = Field(
        default=10, description="Maximum consecutive errors before degradation"
    )
    auto_degrade_after_errors: int = Field(
        default=10, description="Auto-degrade handler after this many consecutive errors"
    )
    auto_fault_after_errors: int = Field(
        default=20, description="Auto-fault handler after this many consecutive errors"
    )

    # Retry configuration
    retry_config: EventRetryConfig = Field(
        default_factory=EventRetryConfig, description="Retry configuration for handler operations"
    )

    # Cache settings
    cache_ttl_seconds: int = Field(default=300, description="Handler cache TTL in seconds")
    max_cache_size: int = Field(
        default=10000, description="Maximum number of cached items per handler"
    )

    # Performance
    batch_size: int = Field(default=100, description="Batch size for event processing")
    processing_timeout_sec: float = Field(default=30.0, description="Timeout for event processing")


class EventBusConfig(BaseModel):
    """Configuration for the event bus."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Queue settings
    max_queue_size: int = Field(default=100000, description="Maximum event queue size")
    queue_timeout_sec: float = Field(default=1.0, description="Queue operation timeout")

    # Priority queue sizes
    critical_queue_size: int = Field(default=10000, description="Critical priority queue size")
    high_queue_size: int = Field(default=20000, description="High priority queue size")
    normal_queue_size: int = Field(default=50000, description="Normal priority queue size")
    low_queue_size: int = Field(default=20000, description="Low priority queue size")

    # Performance
    batch_publish_size: int = Field(default=1000, description="Batch size for publishing events")
    publish_timeout_sec: float = Field(default=5.0, description="Timeout for publishing events")

    # Request/Response
    request_timeout_sec: float = Field(
        default=10.0, description="Timeout for request/response pattern"
    )
    max_pending_requests: int = Field(
        default=1000, description="Maximum pending request/response operations"
    )


class EventWorkflowConfig(BaseModel):
    """Configuration for workflow orchestration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Timeouts
    workflow_timeout_sec: float = Field(
        default=300.0, description="Overall workflow execution timeout"
    )
    step_timeout_sec: float = Field(default=60.0, description="Individual workflow step timeout")
    pending_orders_timeout_sec: float = Field(
        default=30.0, description="Timeout for waiting for pending orders to complete"
    )
    pending_orders_poll_interval_sec: float = Field(
        default=1.0, description="Poll interval for checking pending orders status"
    )

    # Retries
    retry_config: EventRetryConfig = Field(
        default_factory=EventRetryConfig, description="Retry configuration for workflow steps"
    )

    # Audit
    enable_audit_trail: bool = Field(default=True, description="Enable workflow audit trail")
    max_audit_entries: int = Field(
        default=1000, description="Maximum audit trail entries per workflow"
    )

    # Rollback
    enable_rollback: bool = Field(default=True, description="Enable workflow rollback on failure")
    rollback_timeout_sec: float = Field(
        default=120.0, description="Timeout for rollback operations"
    )

    # PlaceOrder workflow configuration
    place_order_risk_checks: list[str] = Field(
        default_factory=lambda: [
            "position_limit",
            "drawdown",
            "exposure",
            "leverage",
            "concentration",
        ],
        description="Risk checks to perform in PlaceOrderWorkflow",
    )

    # Rebalance workflow configuration
    rebalance_max_slippage: Decimal = Field(
        default=Decimal("0.01"), description="Maximum allowed slippage for rebalancing"
    )
    rebalance_default_mode: str = Field(
        default="proportional",
        description="Default rebalancing mode (proportional, threshold, aggressive)",
    )

    # Emergency liquidation configuration
    emergency_alert_channels: list[str] = Field(
        default_factory=lambda: ["email", "slack", "telegram"],
        description="Alert channels for emergency liquidation",
    )
    emergency_retry_multiplier: float = Field(
        default=0.5, description="Retry delay multiplier for emergency operations (faster retries)"
    )
    emergency_retry_attempts_factor: int = Field(
        default=3, description="Multiply retry attempts for emergency operations"
    )

    # Standard workflow retry factors
    workflow_retry_attempts_factor: float = Field(
        default=0.5, description="Factor to adjust retry attempts for workflow steps"
    )


class EventMonitoringConfig(BaseModel):
    """Configuration for event system monitoring."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Metrics collection
    enable_metrics: bool = Field(default=True, description="Enable event metrics collection")
    metrics_interval_sec: int = Field(default=60, description="Metrics collection interval")

    # Health checks
    health_check_interval_sec: int = Field(
        default=30, description="Health check interval for handlers"
    )
    unhealthy_threshold: int = Field(
        default=3, description="Consecutive failures before marking unhealthy"
    )
    handler_shutdown_timeout_sec: float = Field(
        default=30.0, description="Maximum time for graceful handler shutdown"
    )

    # Performance monitoring
    slow_event_threshold_ms: float = Field(
        default=100.0, description="Threshold for slow event processing in milliseconds"
    )
    track_event_latency: bool = Field(default=True, description="Track event processing latency")

    # Alerting
    alert_on_handler_fault: bool = Field(
        default=True, description="Alert when handler enters fault state"
    )
    alert_on_queue_overflow: bool = Field(default=True, description="Alert on event queue overflow")
    alert_on_slow_processing: bool = Field(
        default=True, description="Alert on slow event processing"
    )


class EventLoggingConfig(BaseModel):
    """Configuration for event system logging."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    log_level: str = Field(default="INFO", description="Event system log level")

    # Log rotation
    max_log_size_mb: int = Field(default=100, description="Maximum log file size in MB")
    backup_count: int = Field(default=10, description="Number of log backup files to keep")

    # Structured logging
    use_json_format: bool = Field(
        default=False, description="Use JSON format for structured logging"
    )
    include_metrics: bool = Field(default=True, description="Include metrics in log output")

    # Component-specific levels
    event_bus_log_level: str = Field(default="INFO", description="Event bus log level")
    handlers_log_level: str = Field(default="INFO", description="Handlers log level")
    workflows_log_level: str = Field(default="INFO", description="Workflows log level")
    msgspec_log_level: str = Field(default="WARNING", description="msgspec library log level")
    bubus_log_level: str = Field(default="INFO", description="bubus library log level")


class EventWebSocketConfig(BaseModel):
    """Configuration for WebSocket message processing."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Buffer settings
    buffer_size: int = Field(default=65536, description="WebSocket buffer size")
    max_message_size: int = Field(
        default=1048576,  # 1MB
        description="Maximum WebSocket message size",
    )

    # Processing
    batch_process_size: int = Field(
        default=100, description="Batch size for processing WebSocket messages"
    )
    process_timeout_ms: float = Field(
        default=10.0, description="Timeout for processing WebSocket messages in milliseconds"
    )

    # Decoder cache
    cache_decoders: bool = Field(
        default=True, description="Cache msgspec decoders for WebSocket messages"
    )
    max_decoder_cache: int = Field(default=100, description="Maximum number of cached decoders")

    # Event publishing
    publish_internal_updates: bool = Field(
        default=False, description="Publish internal state updates as events"
    )
    publish_market_data: bool = Field(
        default=True, description="Publish market data events from WebSocket"
    )
    publish_order_updates: bool = Field(
        default=True, description="Publish order update events from WebSocket"
    )


class EventCircuitBreakerConfig(BaseModel):
    """Configuration for event system circuit breaker integration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Thresholds
    failure_threshold: int = Field(default=5, description="Failures before circuit breaker trips")
    recovery_timeout_sec: float = Field(
        default=60.0, description="Recovery timeout after circuit breaker trips"
    )

    # Monitoring
    monitor_handlers: bool = Field(
        default=True, description="Monitor event handlers with circuit breaker"
    )
    monitor_workflows: bool = Field(
        default=True, description="Monitor workflows with circuit breaker"
    )

    # Actions
    auto_degrade_on_trip: bool = Field(
        default=True, description="Auto-degrade handler when circuit breaker trips"
    )
    alert_on_trip: bool = Field(default=True, description="Send alert when circuit breaker trips")


class EventSystemSettings(BaseModel):
    """Complete configuration for the event system.

    This integrates with the main AppSettings configuration.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Core components
    handler: EventHandlerConfig = Field(
        default_factory=EventHandlerConfig, description="Event handler configuration"
    )
    event_bus: EventBusConfig = Field(
        default_factory=EventBusConfig, description="Event bus configuration"
    )
    workflow: EventWorkflowConfig = Field(
        default_factory=EventWorkflowConfig, description="Workflow orchestration configuration"
    )

    # Supporting systems
    monitoring: EventMonitoringConfig = Field(
        default_factory=EventMonitoringConfig, description="Event monitoring configuration"
    )
    logging: EventLoggingConfig = Field(
        default_factory=EventLoggingConfig, description="Event logging configuration"
    )
    websocket: EventWebSocketConfig = Field(
        default_factory=EventWebSocketConfig, description="WebSocket processing configuration"
    )
    circuit_breaker: EventCircuitBreakerConfig = Field(
        default_factory=EventCircuitBreakerConfig, description="Circuit breaker configuration"
    )

    # Performance tracking (only keeping this as it's a useful operational flag)
    enable_performance_tracking: bool = Field(
        default=True, description="Enable event performance tracking"
    )

    # Safety
    safe_mode: bool = Field(
        default=False, description="Restrict to cancellations only in safe mode"
    )
    max_memory_usage_mb: int = Field(
        default=4096, description="Maximum memory usage for event system in MB"
    )

    @classmethod
    def for_production(cls) -> "EventSystemSettings":
        """Create a production-ready configuration.

        Returns:
            EventSystemSettings configured for production use
        """
        return cls(
            handler=EventHandlerConfig(
                max_consecutive_errors=5,
                auto_degrade_after_errors=5,
                auto_fault_after_errors=10,
            ),
            monitoring=EventMonitoringConfig(
                enable_metrics=True,
                health_check_interval_sec=10,
                alert_on_handler_fault=True,
                alert_on_queue_overflow=True,
            ),
            logging=EventLoggingConfig(
                log_level="WARNING",
                use_json_format=True,
                include_metrics=True,
            ),
            circuit_breaker=EventCircuitBreakerConfig(
                failure_threshold=3,
                auto_degrade_on_trip=True,
                alert_on_trip=True,
            ),
            safe_mode=False,
        )

    @classmethod
    def for_testing(cls) -> "EventSystemSettings":
        """Create a test configuration.

        Returns:
            EventSystemSettings configured for testing
        """
        return cls(
            handler=EventHandlerConfig(
                max_consecutive_errors=1,
                auto_degrade_after_errors=1,
                auto_fault_after_errors=2,
                cache_ttl_seconds=1,
            ),
            event_bus=EventBusConfig(
                max_queue_size=1000,
                batch_publish_size=10,
            ),
            monitoring=EventMonitoringConfig(
                enable_metrics=False,
                health_check_interval_sec=1,
            ),
            logging=EventLoggingConfig(
                log_level="DEBUG",
                use_json_format=False,
            ),
            enable_performance_tracking=False,
        )
