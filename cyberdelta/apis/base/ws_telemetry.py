"""WebSocket OpenTelemetry integration.

This module provides comprehensive OpenTelemetry tracing and metrics
for WebSocket operations including message flows, connection health,
and performance monitoring.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics, trace
from opentelemetry.trace import Span, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import BaseModel, Field

from cyberdelta.apis.base.validation_context_domain import OperationResult
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from collections.abc import Generator


# Type aliases for telemetry attributes
TelemetryAttributeValue = str | int | float | bool | None
TelemetryAttributes = dict[str, str | int | float | bool]
TraceContextDict = dict[str, str]


class TelemetryConfig(BaseModel):
    """Configuration for OpenTelemetry integration."""

    service_name: str = "cyberdelta-websocket"
    service_version: str = "1.0.0"
    trace_enabled: bool = True
    metrics_enabled: bool = True
    export_endpoint: str | None = None
    export_headers: dict[str, str] = Field(default_factory=dict)
    resource_attributes: dict[str, str] = Field(default_factory=dict)
    span_processor_type: str = "batch"  # "batch" or "simple"
    max_export_batch_size: int = 512
    export_timeout_millis: int = 30000


class SpanContext(BaseModel):
    """Context information for spans."""

    operation_name: str
    exchange: str | None = None
    connection_id: str | None = None
    message_type: str | None = None
    user_id: str | None = None
    additional_attributes: dict[str, Any] = Field(default_factory=dict)


class MetricLabels(BaseModel):
    """Standard labels for metrics."""

    exchange: str
    connection_id: str | None = None
    message_type: str | None = None
    operation: str | None = None
    status: str | None = None
    error_type: str | None = None


class WebSocketTelemetry:
    """OpenTelemetry integration for WebSocket operations."""

    def __init__(self, config: TelemetryConfig) -> None:
        """Initialize WebSocket telemetry.

        Args:
            config: Telemetry configuration
        """
        self.config = config
        self.logger = get_logger("WebSocketTelemetry")

        # Get tracer and meter
        self.tracer = trace.get_tracer(config.service_name)
        self.meter = metrics.get_meter(config.service_name)

        # Initialize metrics
        self._initialize_metrics()

    def _initialize_metrics(self) -> None:
        """Initialize OpenTelemetry metrics."""
        if not self.config.metrics_enabled:
            return

        # Connection metrics
        self.connection_counter = self.meter.create_counter(
            name="websocket_connections_total",
            description="Total number of WebSocket connections",
            unit="1",
        )

        self.connection_duration = self.meter.create_histogram(
            name="websocket_connection_duration_seconds",
            description="Duration of WebSocket connections",
            unit="s",
        )

        self.active_connections = self.meter.create_up_down_counter(
            name="websocket_connections_active",
            description="Current number of active WebSocket connections",
            unit="1",
        )

        # Message metrics
        self.message_counter = self.meter.create_counter(
            name="websocket_messages_total",
            description="Total number of WebSocket messages processed",
            unit="1",
        )

        self.message_size = self.meter.create_histogram(
            name="websocket_message_size_bytes",
            description="Size of WebSocket messages",
            unit="bytes",
        )

        self.message_processing_duration = self.meter.create_histogram(
            name="websocket_message_processing_duration_seconds",
            description="Time spent processing WebSocket messages",
            unit="s",
        )

        # Error metrics
        self.error_counter = self.meter.create_counter(
            name="websocket_errors_total", description="Total number of WebSocket errors", unit="1"
        )

        # Rate limiting metrics
        self.rate_limit_counter = self.meter.create_counter(
            name="websocket_rate_limits_total",
            description="Total number of rate limit violations",
            unit="1",
        )

        # Reconnection metrics
        self.reconnection_counter = self.meter.create_counter(
            name="websocket_reconnections_total",
            description="Total number of reconnection attempts",
            unit="1",
        )

        self.reconnection_duration = self.meter.create_histogram(
            name="websocket_reconnection_duration_seconds",
            description="Time spent reconnecting",
            unit="s",
        )

    @contextmanager
    def trace_operation(
        self, context: SpanContext, **kwargs: TelemetryAttributeValue
    ) -> Generator[Span | None]:
        """Create a traced operation context.

        Args:
            context: Span context information
            **kwargs: Additional span attributes

        Yields:
            Active span for the operation
        """
        if not self.config.trace_enabled:
            yield None
            return

        # Prepare span attributes
        attributes = {
            "service.name": self.config.service_name,
            "service.version": self.config.service_version,
        }

        if context.exchange:
            attributes["websocket.exchange"] = context.exchange
        if context.connection_id:
            attributes["websocket.connection_id"] = context.connection_id
        if context.message_type:
            attributes["websocket.message_type"] = context.message_type
        if context.user_id:
            attributes["websocket.user_id"] = context.user_id

        # Add additional attributes (convert to strings and filter out None values)
        filtered_additional = {
            k: str(v) for k, v in context.additional_attributes.items() if v is not None
        }
        filtered_kwargs = {k: str(v) for k, v in kwargs.items() if v is not None}
        attributes.update(filtered_additional)
        attributes.update(filtered_kwargs)

        # Create and start span
        with self.tracer.start_as_current_span(
            name=context.operation_name, attributes=attributes
        ) as span:
            try:
                yield span
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
            else:
                span.set_status(Status(StatusCode.OK))

    def record_connection_start(self, labels: MetricLabels) -> None:
        """Record connection start event.

        Args:
            labels: Metric labels
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        self.connection_counter.add(1, attributes)
        self.active_connections.add(1, attributes)

    def record_connection_end(self, labels: MetricLabels, duration_seconds: float) -> None:
        """Record connection end event.

        Args:
            labels: Metric labels
            duration_seconds: Connection duration
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        self.active_connections.add(-1, attributes)
        self.connection_duration.record(duration_seconds, attributes)

    def record_message_processed(
        self, labels: MetricLabels, size_bytes: int, processing_time_seconds: float
    ) -> None:
        """Record message processing event.

        Args:
            labels: Metric labels
            size_bytes: Message size in bytes
            processing_time_seconds: Processing time
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        self.message_counter.add(1, attributes)
        self.message_size.record(size_bytes, attributes)
        self.message_processing_duration.record(processing_time_seconds, attributes)

    def record_error(self, labels: MetricLabels, error_type: str) -> None:
        """Record error event.

        Args:
            labels: Metric labels
            error_type: Type of error
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        attributes["error.type"] = error_type
        self.error_counter.add(1, attributes)

    def record_rate_limit_violation(self, labels: MetricLabels, limit_type: str) -> None:
        """Record rate limit violation.

        Args:
            labels: Metric labels
            limit_type: Type of rate limit
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        attributes["rate_limit.type"] = limit_type
        self.rate_limit_counter.add(1, attributes)

    def record_reconnection_attempt(
        self,
        labels: MetricLabels,
        attempt_number: int,
        result: OperationResult,
        duration_seconds: float | None = None,
    ) -> None:
        """Record reconnection attempt.

        Args:
            labels: Metric labels
            attempt_number: Reconnection attempt number
            result: Result of the reconnection attempt
            duration_seconds: Time spent reconnecting
        """
        if not self.config.metrics_enabled:
            return

        attributes = self._get_metric_attributes(labels)
        attributes["reconnection.attempt"] = str(attempt_number)
        attributes["reconnection.success"] = "true" if result.is_successful else "false"

        self.reconnection_counter.add(1, attributes)

        if duration_seconds is not None:
            self.reconnection_duration.record(duration_seconds, attributes)

    def _get_metric_attributes(self, labels: MetricLabels) -> dict[str, str]:
        """Convert metric labels to attributes.

        Args:
            labels: Metric labels

        Returns:
            Attributes dictionary
        """
        attributes = {
            "exchange": labels.exchange,
        }

        if labels.connection_id:
            attributes["connection_id"] = labels.connection_id
        if labels.message_type:
            attributes["message_type"] = labels.message_type
        if labels.operation:
            attributes["operation"] = labels.operation
        if labels.status:
            attributes["status"] = labels.status
        if labels.error_type:
            attributes["error_type"] = labels.error_type

        return attributes

    def create_child_span(
        self, parent_span: Span, operation_name: str, **attributes: TelemetryAttributeValue
    ) -> Span | None:
        """Create a child span.

        Args:
            parent_span: Parent span
            operation_name: Name of the operation
            **attributes: Additional attributes

        Returns:
            Child span
        """
        if not self.config.trace_enabled or not parent_span:
            return None

        # Filter out None values from attributes and ensure all values are compatible types
        filtered_attributes = {k: v for k, v in attributes.items() if v is not None}
        return self.tracer.start_span(
            name=operation_name,
            context=trace.set_span_in_context(parent_span),
            attributes=filtered_attributes,
        )

    def inject_trace_context(self, span: Span) -> dict[str, str]:
        """Inject trace context into headers.

        Args:
            span: Current span

        Returns:
            Headers with trace context
        """
        if not self.config.trace_enabled or not span:
            return {}

        headers: dict[str, str] = {}
        propagator = TraceContextTextMapPropagator()
        propagator.inject(headers, context=trace.set_span_in_context(span))
        return headers

    def extract_trace_context(self, headers: TraceContextDict) -> object:
        """Extract trace context from headers.

        Args:
            headers: Headers containing trace context

        Returns:
            Extracted context
        """
        if not self.config.trace_enabled:
            return None

        propagator = TraceContextTextMapPropagator()
        return propagator.extract(headers)

    def get_telemetry_stats(self) -> dict[str, Any]:
        """Get telemetry statistics.

        Returns:
            Telemetry statistics
        """
        return {
            "service_name": self.config.service_name,
            "service_version": self.config.service_version,
            "trace_enabled": self.config.trace_enabled,
            "metrics_enabled": self.config.metrics_enabled,
            "export_endpoint": self.config.export_endpoint,
        }


class TelemetryMiddleware:
    """Middleware for automatic telemetry collection."""

    def __init__(self, telemetry: WebSocketTelemetry, exchange_name: str) -> None:
        """Initialize telemetry middleware.

        Args:
            telemetry: WebSocket telemetry instance
            exchange_name: Name of the exchange
        """
        self.telemetry = telemetry
        self.exchange_name = exchange_name
        self.logger = get_logger(f"TelemetryMiddleware.{exchange_name}")

        # Connection tracking
        self.connection_start_times: dict[str, float] = {}

    @contextmanager
    def trace_message_processing(
        self, connection_id: str, message_type: str, user_id: str | None = None
    ) -> Generator[Span | None]:
        """Trace message processing operation.

        Args:
            connection_id: Connection identifier
            message_type: Type of message
            user_id: Optional user identifier

        Yields:
            Span for the operation
        """
        context = SpanContext(
            operation_name="websocket.message.process",
            exchange=self.exchange_name,
            connection_id=connection_id,
            message_type=message_type,
            user_id=user_id,
        )

        with self.telemetry.trace_operation(context) as span:
            yield span

    @contextmanager
    def trace_connection_operation(
        self, connection_id: str, operation: str
    ) -> Generator[Span | None]:
        """Trace connection operation.

        Args:
            connection_id: Connection identifier
            operation: Operation name (connect, disconnect, etc.)

        Yields:
            Span for the operation
        """
        context = SpanContext(
            operation_name=f"websocket.connection.{operation}",
            exchange=self.exchange_name,
            connection_id=connection_id,
            additional_attributes={"websocket.operation": operation},
        )

        with self.telemetry.trace_operation(context) as span:
            yield span

    def record_connection_start(self, connection_id: str) -> None:
        """Record connection start.

        Args:
            connection_id: Connection identifier
        """
        self.connection_start_times[connection_id] = time.time()

        labels = MetricLabels(
            exchange=self.exchange_name,
            connection_id=connection_id,
            operation="connect",
            status="started",
        )
        self.telemetry.record_connection_start(labels)

    def record_connection_end(self, connection_id: str) -> None:
        """Record connection end.

        Args:
            connection_id: Connection identifier
        """
        start_time = self.connection_start_times.pop(connection_id, time.time())
        duration = time.time() - start_time

        labels = MetricLabels(
            exchange=self.exchange_name,
            connection_id=connection_id,
            operation="disconnect",
            status="completed",
        )
        self.telemetry.record_connection_end(labels, duration)

    def record_message_success(
        self, connection_id: str, message_type: str, size_bytes: int, processing_time_seconds: float
    ) -> None:
        """Record successful message processing.

        Args:
            connection_id: Connection identifier
            message_type: Type of message
            size_bytes: Message size
            processing_time_seconds: Processing time
        """
        labels = MetricLabels(
            exchange=self.exchange_name,
            connection_id=connection_id,
            message_type=message_type,
            operation="process",
            status="success",
        )
        self.telemetry.record_message_processed(labels, size_bytes, processing_time_seconds)

    def record_message_error(self, connection_id: str, message_type: str, error_type: str) -> None:
        """Record message processing error.

        Args:
            connection_id: Connection identifier
            message_type: Type of message
            error_type: Type of error
        """
        labels = MetricLabels(
            exchange=self.exchange_name,
            connection_id=connection_id,
            message_type=message_type,
            operation="process",
            status="error",
            error_type=error_type,
        )
        self.telemetry.record_error(labels, error_type)

    def record_rate_limit_hit(self, connection_id: str, limit_type: str) -> None:
        """Record rate limit violation.

        Args:
            connection_id: Connection identifier
            limit_type: Type of rate limit
        """
        labels = MetricLabels(
            exchange=self.exchange_name, connection_id=connection_id, operation="rate_limit"
        )
        self.telemetry.record_rate_limit_violation(labels, limit_type)

    def record_reconnection(
        self,
        connection_id: str,
        attempt_number: int,
        result: OperationResult,
        duration_seconds: float,
    ) -> None:
        """Record reconnection attempt.

        Args:
            connection_id: Connection identifier
            attempt_number: Attempt number
            result: Result of the reconnection attempt
            duration_seconds: Duration of attempt
        """
        labels = MetricLabels(
            exchange=self.exchange_name,
            connection_id=connection_id,
            operation="reconnect",
            status="success" if result.is_successful else "failure",
        )
        self.telemetry.record_reconnection_attempt(labels, attempt_number, result, duration_seconds)


class TelemetryManager:
    """Manager for WebSocket telemetry across multiple exchanges."""

    def __init__(self, config: TelemetryConfig) -> None:
        """Initialize telemetry manager.

        Args:
            config: Telemetry configuration
        """
        self.config = config
        self.telemetry = WebSocketTelemetry(config)
        self.middlewares: dict[str, TelemetryMiddleware] = {}
        self.logger = get_logger("TelemetryManager")

    def get_middleware(self, exchange_name: str) -> TelemetryMiddleware:
        """Get or create telemetry middleware for exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            Telemetry middleware instance
        """
        if exchange_name not in self.middlewares:
            self.middlewares[exchange_name] = TelemetryMiddleware(self.telemetry, exchange_name)

        return self.middlewares[exchange_name]

    def get_global_stats(self) -> dict[str, Any]:
        """Get global telemetry statistics.

        Returns:
            Global statistics
        """
        return {
            "telemetry": self.telemetry.get_telemetry_stats(),
            "exchanges": list(self.middlewares.keys()),
            "middleware_count": len(self.middlewares),
        }


# Default telemetry configuration
DEFAULT_TELEMETRY_CONFIG = TelemetryConfig()


class TelemetryManagerSingleton:
    """Singleton wrapper for telemetry manager."""

    _instance: TelemetryManager | None = None

    @classmethod
    def get_instance(cls, config: TelemetryConfig | None = None) -> TelemetryManager:
        """Get telemetry manager instance.

        Args:
            config: Optional telemetry configuration

        Returns:
            Telemetry manager instance
        """
        if cls._instance is None:
            cls._instance = TelemetryManager(config or DEFAULT_TELEMETRY_CONFIG)
        return cls._instance


def get_telemetry_manager(config: TelemetryConfig | None = None) -> TelemetryManager:
    """Get global telemetry manager instance.

    Args:
        config: Optional telemetry configuration

    Returns:
        Telemetry manager instance
    """
    return TelemetryManagerSingleton.get_instance(config)


def get_exchange_telemetry(exchange_name: str) -> TelemetryMiddleware:
    """Get telemetry middleware for specific exchange.

    Args:
        exchange_name: Name of the exchange

    Returns:
        Telemetry middleware instance
    """
    manager = get_telemetry_manager()
    return manager.get_middleware(exchange_name)
