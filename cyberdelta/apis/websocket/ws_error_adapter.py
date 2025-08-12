"""Compatibility adapter for WebSocket errors.

Converts WebSocket errors to APIError format for legacy systems
during the migration period. This adapter maintains backward compatibility
while the system migrates to the new type-safe error system.
"""

from __future__ import annotations

from datetime import UTC
from typing import TYPE_CHECKING

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


# Circuit breaker constants
MAX_RECONNECT_COUNT = 10


class WebSocketErrorAdapter:
    """Adapter to convert WebSocket errors to APIError for legacy compatibility."""

    # Mapping from WebSocket error codes to API error codes
    WS_TO_API_CODE_MAP: dict[WebSocketErrorCode, APIErrorCode] = {
        # Connection errors
        WebSocketErrorCode.CONNECTION_CLOSED: APIErrorCode.NETWORK_ISSUE,
        WebSocketErrorCode.CONNECTION_LOST: APIErrorCode.NETWORK_ISSUE,
        WebSocketErrorCode.CONNECTION_TIMEOUT: APIErrorCode.TIMEOUT,
        WebSocketErrorCode.CONNECTION_FAILED: APIErrorCode.NETWORK_ISSUE,
        WebSocketErrorCode.CONNECTION_REFUSED: APIErrorCode.SERVICE_UNAVAILABLE,
        WebSocketErrorCode.CONNECTION_RESET: APIErrorCode.NETWORK_ISSUE,
        WebSocketErrorCode.CONNECTION_ABORTED: APIErrorCode.NETWORK_ISSUE,
        # Authentication errors
        WebSocketErrorCode.AUTH_REQUIRED: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.AUTH_FAILED: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.AUTH_EXPIRED: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.AUTH_INVALID_TOKEN: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.AUTH_REVOKED: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.AUTH_INSUFFICIENT_PERMISSIONS: APIErrorCode.PERMISSION_DENIED,
        # Rate limiting
        WebSocketErrorCode.RATE_LIMITED: APIErrorCode.RATE_LIMITED,
        # Validation errors
        WebSocketErrorCode.VALIDATION_FAILED: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.INVALID_MESSAGE_FORMAT: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.INVALID_MESSAGE_TYPE: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.MISSING_REQUIRED_FIELD: APIErrorCode.INVALID_PARAMS,
        WebSocketErrorCode.INVALID_FIELD_VALUE: APIErrorCode.INVALID_PARAMS,
        # Stream errors
        WebSocketErrorCode.SEQUENCE_GAP: APIErrorCode.NETWORK_ISSUE,  # Always retryable
        WebSocketErrorCode.STREAM_INTERRUPTED: APIErrorCode.SERVER_ERROR,
        WebSocketErrorCode.STREAM_CORRUPTED: APIErrorCode.SERVER_ERROR,
        # Subscription errors
        WebSocketErrorCode.SUBSCRIPTION_FAILED: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED: APIErrorCode.RATE_LIMITED,
        WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL: APIErrorCode.INVALID_PARAMS,
        WebSocketErrorCode.SUBSCRIPTION_UNAUTHORIZED: APIErrorCode.PERMISSION_DENIED,
        # Protocol errors
        WebSocketErrorCode.PROTOCOL_ERROR: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.UNSUPPORTED_VERSION: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.HEARTBEAT_TIMEOUT: APIErrorCode.TIMEOUT,
        # Exchange errors
        WebSocketErrorCode.EXCHANGE_OVERLOADED: APIErrorCode.SERVICE_UNAVAILABLE,
        WebSocketErrorCode.EXCHANGE_MAINTENANCE: APIErrorCode.MAINTENANCE,
        WebSocketErrorCode.EXCHANGE_ERROR: APIErrorCode.SERVER_ERROR,
        # Security errors
        WebSocketErrorCode.SECURITY_VIOLATION: APIErrorCode.PERMISSION_DENIED,
        WebSocketErrorCode.INVALID_SIGNATURE: APIErrorCode.AUTHENTICATION_FAILED,
        WebSocketErrorCode.REPLAY_ATTACK_DETECTED: APIErrorCode.PERMISSION_DENIED,
        WebSocketErrorCode.INJECTION_DETECTED: APIErrorCode.INVALID_REQUEST,
        WebSocketErrorCode.IP_BANNED: APIErrorCode.IP_BAN_SUSPECTED,
        WebSocketErrorCode.ACCOUNT_SUSPENDED: APIErrorCode.ACCOUNT_SUSPENDED,
        WebSocketErrorCode.SUSPICIOUS_ACTIVITY: APIErrorCode.PERMISSION_DENIED,
        # Default mapping
        WebSocketErrorCode.UNKNOWN_ERROR: APIErrorCode.UNKNOWN,
    }

    # Recovery strategy to retryable mapping
    RETRYABLE_STRATEGIES = {
        WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
        WebSocketRecoveryStrategy.LINEAR_BACKOFF,
        WebSocketRecoveryStrategy.RECONNECT_SAME,
        WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
        WebSocketRecoveryStrategy.FULL_RECONNECT,
        WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
        WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE,
    }

    @classmethod
    def to_api_error(cls, ws_error: WebSocketStreamError) -> APIError:
        """Convert WebSocket error to APIError for legacy systems.

        Args:
            ws_error: WebSocket stream error to convert

        Returns:
            APIError instance with WebSocket error information
        """
        # Map WebSocket error code to API error code
        api_code = cls.WS_TO_API_CODE_MAP.get(ws_error.code, APIErrorCode.UNKNOWN)

        # Determine HTTP status based on error type and API code
        http_status = cls._get_http_status_from_code(api_code, ws_error.code)

        # Build metadata from WebSocket context
        metadata = cls._build_metadata(ws_error)

        # Calculate retry_after if available
        retry_after = None
        if ws_error.recovery_strategy != WebSocketRecoveryStrategy.NONE:
            retry_after_ms = ws_error.get_retry_delay_ms()
            if retry_after_ms > 0:
                retry_after = retry_after_ms / 1000.0  # Convert to seconds

        # Create APIError with mapped values
        return APIError(
            message=ws_error.message,
            code=api_code.value,
            http_status=http_status,
            exchange_code=ws_error.code.value,  # WebSocket code as exchange code
            exchange_message=ws_error.suggested_action,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=ws_error.cause,
        )

    @classmethod
    def _get_http_status_from_code(cls, api_code: APIErrorCode, ws_code: WebSocketErrorCode) -> int:
        """Map error code to appropriate HTTP status.

        Args:
            api_code: Mapped API error code
            ws_code: Original WebSocket error code

        Returns:
            Appropriate HTTP status code
        """
        # Map based on specific WebSocket error codes first for finer control
        ws_status_map = {
            WebSocketErrorCode.AUTH_REVOKED: 403,  # Revoked access
            WebSocketErrorCode.IP_BANNED: 403,  # Banned IP
            WebSocketErrorCode.ACCOUNT_SUSPENDED: 403,  # Suspended account
        }

        # Check for specific WebSocket code first
        if ws_code in ws_status_map:
            return ws_status_map[ws_code]

        # Fall back to API error code mapping
        status_map = {
            APIErrorCode.AUTHENTICATION_FAILED: 401,
            APIErrorCode.PERMISSION_DENIED: 403,
            APIErrorCode.ACCOUNT_SUSPENDED: 403,
            APIErrorCode.IP_BAN_SUSPECTED: 403,
            APIErrorCode.INVALID_REQUEST: 400,
            APIErrorCode.INVALID_PARAMS: 400,
            APIErrorCode.RATE_LIMITED: 503,
            APIErrorCode.SERVICE_UNAVAILABLE: 503,
            APIErrorCode.MAINTENANCE: 503,
            APIErrorCode.TIMEOUT: 503,
            APIErrorCode.NETWORK_ISSUE: 503,
            APIErrorCode.SERVER_ERROR: 500,
            APIErrorCode.UNKNOWN: 500,
        }

        return status_map.get(api_code, 500)

    @classmethod
    def _build_metadata(cls, ws_error: WebSocketStreamError) -> dict[str, object]:
        """Build metadata dictionary from WebSocket error.

        Args:
            ws_error: WebSocket stream error

        Returns:
            Metadata dictionary for APIError
        """
        metadata: dict[str, object] = {
            "error_domain": "websocket_stream",
            "ws_error_code": ws_error.code.name,
            "ws_error_code_value": ws_error.code.value,
            "ws_category": ws_error.category,
            "ws_severity": ws_error.severity.name,
            "ws_recovery_strategy": ws_error.recovery_strategy.name,
            "ws_is_critical": ws_error.is_critical,
            "ws_is_retryable": ws_error.is_retryable,
        }

        # Add context information
        context = ws_error.context
        metadata.update({
            "connection_id": context.connection_id,
            "exchange": context.exchange,
            "environment": context.environment,
        })

        # Add optional context fields
        if context.channel:
            metadata["channel"] = context.channel
        if context.topic:
            metadata["topic"] = context.topic
        if context.subscription_id:
            metadata["subscription_id"] = context.subscription_id

        # Add sequence information if available
        if context.sequence_number is not None:
            metadata["sequence_number"] = context.sequence_number
        if context.expected_sequence is not None:
            metadata["expected_sequence"] = context.expected_sequence
        if context.has_sequence_gap():
            metadata["sequence_gap"] = context.get_sequence_gap_size()

        # Add timing information
        metadata["error_timestamp_ms"] = context.error_timestamp_ms
        # Add ISO timestamp for easier reading
        from datetime import datetime

        timestamp_dt = datetime.fromtimestamp(context.error_timestamp_ms / 1000, UTC)
        metadata["timestamp_iso"] = timestamp_dt.isoformat()

        if context.get_connection_duration_ms():
            metadata["connection_duration_ms"] = context.get_connection_duration_ms()
        if context.get_time_since_last_message_ms():
            metadata["time_since_last_message_ms"] = context.get_time_since_last_message_ms()

        # Add connection state
        metadata["active_subscriptions"] = context.active_subscriptions
        metadata["pending_messages"] = context.pending_messages
        metadata["reconnect_count"] = context.reconnect_count

        # Add message size if available
        if context.raw_message_size is not None:
            metadata["raw_message_size"] = context.raw_message_size

        # Add any extra context
        if context.extra_context:
            metadata["extra_context"] = context.extra_context
            # Also add individual extra context fields to top level for easier access
            for key, value in context.extra_context.items():
                metadata[key] = value

        # Add cause information if present
        if ws_error.cause:
            metadata["cause_type"] = type(ws_error.cause).__name__
            metadata["cause_message"] = str(ws_error.cause)

        # Add error chain if present
        if context.error_chain:
            metadata["error_chain"] = [
                {
                    "class": err.error_class,
                    "message": err.error_message,
                    "code": err.error_code,
                    "timestamp_ms": err.timestamp_ms,
                }
                for err in context.error_chain
            ]

        return metadata

    @classmethod
    def get_legacy_monitoring_data(cls, ws_error: WebSocketStreamError) -> dict[str, object]:
        """Extract monitoring data for legacy dashboards.

        Args:
            ws_error: WebSocket stream error

        Returns:
            Dictionary with monitoring data in legacy format
        """
        monitoring_data: dict[str, object] = {
            # Error identification
            "error_type": "websocket",
            "error_code": ws_error.code.value,
            "error_name": ws_error.code.name,
            "error_message": ws_error.message,
            # Severity and criticality
            "severity": ws_error.severity.value,
            "severity_name": ws_error.severity.name,
            "is_critical": ws_error.is_critical,
            "is_retryable": ws_error.is_retryable,
            # Recovery information
            "recovery_strategy": ws_error.recovery_strategy.value,
            "recovery_strategy_name": ws_error.recovery_strategy.name,
            "retry_delay_ms": ws_error.get_retry_delay_ms(),
            # Connection information
            "connection_id": ws_error.context.connection_id,
            "exchange": ws_error.context.exchange,
            "channel": ws_error.context.channel,
            "reconnect_count": ws_error.context.reconnect_count,
            # Timing
            "timestamp_ms": ws_error.timestamp_ms,
            "error_age_seconds": ws_error.age_seconds(),
        }

        # Add sequence information if relevant
        if ws_error.context.has_sequence_gap():
            monitoring_data["has_sequence_gap"] = True
            monitoring_data["sequence_gap_size"] = ws_error.context.get_sequence_gap_size()

        # Add performance metrics
        if ws_error.context.get_time_since_last_message_ms():
            monitoring_data["time_since_last_message_ms"] = (
                ws_error.context.get_time_since_last_message_ms()
            )

        return monitoring_data

    @classmethod
    def is_retryable_ws_error(cls, ws_error: WebSocketStreamError) -> bool:
        """Check if WebSocket error is retryable according to legacy logic.

        Args:
            ws_error: WebSocket stream error

        Returns:
            True if error should be retried in legacy systems
        """
        return ws_error.recovery_strategy in cls.RETRYABLE_STRATEGIES

    @classmethod
    def get_retry_delay_seconds(cls, ws_error: WebSocketStreamError) -> float:
        """Get retry delay in seconds for legacy systems.

        Args:
            ws_error: WebSocket stream error

        Returns:
            Retry delay in seconds (0 if no retry)
        """
        if not cls.is_retryable_ws_error(ws_error):
            return 0.0

        retry_ms = ws_error.get_retry_delay_ms()
        return retry_ms / 1000.0

    @classmethod
    def should_circuit_break(cls, ws_error: WebSocketStreamError) -> bool:
        """Check if error should trigger circuit breaker in legacy systems.

        Args:
            ws_error: WebSocket stream error

        Returns:
            True if circuit breaker should be triggered
        """
        # Circuit break on specific strategies
        if ws_error.recovery_strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER:
            return True

        # Circuit break on critical errors
        if ws_error.is_critical:
            return True

        # Circuit break on excessive reconnects
        if ws_error.context.reconnect_count > MAX_RECONNECT_COUNT:
            return True

        # Circuit break on security violations
        if ws_error.category == "SECURITY":
            return True

        return False

    @classmethod
    def get_alert_level(cls, ws_error: WebSocketStreamError) -> str:
        """Get alert level for legacy monitoring systems.

        Args:
            ws_error: WebSocket stream error

        Returns:
            Alert level string (info, warning, error, critical)
        """
        if ws_error.severity <= ErrorSeverity.INFO:
            return "info"
        if ws_error.severity <= ErrorSeverity.WARNING:
            return "warning"
        if ws_error.severity <= ErrorSeverity.ERROR:
            return "error"
        return "critical"
