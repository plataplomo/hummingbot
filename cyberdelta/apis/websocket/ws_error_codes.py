"""WebSocket-specific error codes with clear semantics.

These error codes are designed specifically for WebSocket streaming errors,
separate from REST API error codes.
"""

from __future__ import annotations

from enum import IntEnum


class WebSocketErrorCode(IntEnum):
    """WebSocket-specific error codes."""

    # ========================================================================
    # Connection Level Errors (1000-1099)
    # ========================================================================
    CONNECTION_CLOSED = 1000
    CONNECTION_LOST = 1001
    CONNECTION_REFUSED = 1002
    CONNECTION_TIMEOUT = 1003
    CONNECTION_RESET = 1004
    CONNECTION_ABORTED = 1005
    CONNECTION_FAILED = 1006
    HANDSHAKE_FAILED = 1010
    PROTOCOL_ERROR = 1011
    INVALID_FRAME = 1012
    COMPRESSION_ERROR = 1013
    UNSUPPORTED_VERSION = 1014
    SSL_ERROR = 1015
    MESSAGE_TIMEOUT = 1016

    # ========================================================================
    # Authentication & Authorization (1100-1199)
    # ========================================================================
    AUTH_REQUIRED = 1100
    AUTH_FAILED = 1101
    AUTH_EXPIRED = 1102
    AUTH_REVOKED = 1103
    AUTH_INVALID_TOKEN = 1104
    AUTH_INSUFFICIENT_PERMISSIONS = 1110
    AUTH_UNAUTHORIZED = 1111
    INSUFFICIENT_PERMISSIONS = 1112
    INVALID_API_KEY = 1113
    PERMISSION_DENIED = 1114
    RATE_LIMITED = 1120
    IP_BANNED = 1121
    ACCOUNT_SUSPENDED = 1122

    # ========================================================================
    # Stream Level Errors (1200-1299)
    # ========================================================================
    STREAM_INTERRUPTED = 1200
    STREAM_CORRUPTED = 1201
    STREAM_OVERFLOW = 1202
    STREAM_UNDERFLOW = 1203
    STREAM_DESYNC = 1204
    SEQUENCE_GAP = 1210
    SEQUENCE_DUPLICATE = 1211
    SEQUENCE_OUT_OF_ORDER = 1212

    # ========================================================================
    # Subscription Errors (1300-1399)
    # ========================================================================
    SUBSCRIPTION_FAILED = 1300
    SUBSCRIPTION_REJECTED = 1301
    SUBSCRIPTION_LIMIT_EXCEEDED = 1302
    SUBSCRIPTION_NOT_FOUND = 1303
    SUBSCRIPTION_ALREADY_EXISTS = 1304
    SUBSCRIPTION_INVALID_CHANNEL = 1305
    SUBSCRIPTION_UNAUTHORIZED = 1306
    INVALID_CHANNEL = 1310
    INVALID_TOPIC = 1311
    INVALID_SYMBOL = 1312
    CHANNEL_CLOSED = 1320
    CHANNEL_FULL = 1321

    # ========================================================================
    # Message Processing Errors (1400-1499)
    # ========================================================================
    MESSAGE_TOO_LARGE = 1400
    MESSAGE_MALFORMED = 1401
    MESSAGE_UNSUPPORTED = 1402
    MESSAGE_VALIDATION_FAILED = 1403
    VALIDATION_FAILED = 1404
    INVALID_MESSAGE_FORMAT = 1405
    INVALID_MESSAGE_TYPE = 1406
    MISSING_REQUIRED_FIELD = 1407
    INVALID_FIELD_VALUE = 1408
    PAYLOAD_INVALID = 1410
    PAYLOAD_MISSING_REQUIRED = 1411
    PAYLOAD_TYPE_MISMATCH = 1412
    ENCODING_ERROR = 1420
    DECODING_ERROR = 1421
    SERIALIZATION_ERROR = 1422
    DESERIALIZATION_ERROR = 1423

    # ========================================================================
    # Heartbeat & Keep-Alive (1500-1599)
    # ========================================================================
    HEARTBEAT_TIMEOUT = 1500
    HEARTBEAT_FAILED = 1501
    PING_TIMEOUT = 1502
    PONG_TIMEOUT = 1503
    KEEP_ALIVE_FAILED = 1504

    # ========================================================================
    # Recovery & Reconnection (1600-1699)
    # ========================================================================
    RECOVERY_FAILED = 1600
    RECOVERY_IN_PROGRESS = 1601
    RECONNECT_FAILED = 1602
    RECONNECT_LIMIT_EXCEEDED = 1603
    RESUBSCRIBE_FAILED = 1604
    STATE_SYNC_FAILED = 1610
    SNAPSHOT_FAILED = 1611
    REPLAY_FAILED = 1612

    # ========================================================================
    # Exchange-Specific Errors (1700-1799)
    # ========================================================================
    EXCHANGE_UNAVAILABLE = 1700
    EXCHANGE_MAINTENANCE = 1701
    EXCHANGE_OVERLOADED = 1702
    EXCHANGE_ERROR = 1703
    SYMBOL_DELISTED = 1710
    SYMBOL_HALTED = 1711
    MARKET_CLOSED = 1712

    # ========================================================================
    # Internal System Errors (1800-1899)
    # ========================================================================
    INTERNAL_ERROR = 1800
    PROCESSOR_ERROR = 1801
    ROUTER_ERROR = 1802
    HANDLER_ERROR = 1803
    TRANSFORMER_ERROR = 1804
    MEMORY_LIMIT_EXCEEDED = 1810
    QUEUE_FULL = 1811
    BUFFER_OVERFLOW = 1812
    RESOURCE_EXHAUSTED = 1813

    # ========================================================================
    # Security & Validation (1900-1999)
    # ========================================================================
    SECURITY_VIOLATION = 1900
    INVALID_SIGNATURE = 1901
    INVALID_TIMESTAMP = 1902
    REPLAY_ATTACK_DETECTED = 1903
    SUSPICIOUS_ACTIVITY = 1904
    INJECTION_DETECTED = 1910
    XSS_DETECTED = 1911
    OVERFLOW_DETECTED = 1912
    UNDERFLOW_DETECTED = 1913

    # ========================================================================
    # General/Unknown Errors (1999)
    # ========================================================================
    UNKNOWN_ERROR = 1999

    def get_category(self) -> str:
        """Get the error category based on code range."""
        if 1000 <= self < 1100:
            return "CONNECTION"
        if 1100 <= self < 1200:
            return "AUTHENTICATION"
        if 1200 <= self < 1300:
            return "STREAM"
        if 1300 <= self < 1400:
            return "SUBSCRIPTION"
        if 1400 <= self < 1500:
            return "MESSAGE_PROCESSING"
        if 1500 <= self < 1600:
            return "HEARTBEAT"
        if 1600 <= self < 1700:
            return "RECOVERY"
        if 1700 <= self < 1800:
            return "EXCHANGE"
        if 1800 <= self < 1900:
            return "INTERNAL"
        if 1900 <= self < 2000:
            return "SECURITY"
        return "UNKNOWN"

    def is_retryable(self) -> bool:
        """Check if error is potentially retryable."""
        # Connection errors are often retryable
        if self in {
            self.CONNECTION_LOST,
            self.CONNECTION_TIMEOUT,
            self.CONNECTION_RESET,
        }:
            return True

        # Temporary stream issues
        if self in {
            self.STREAM_INTERRUPTED,
            self.SEQUENCE_GAP,
            self.SEQUENCE_OUT_OF_ORDER,
        }:
            return True

        # Rate limiting is retryable with backoff
        if self == self.RATE_LIMITED:
            return True

        # Recovery in progress means retry later
        if self == self.RECOVERY_IN_PROGRESS:
            return True

        # Exchange temporary issues
        if self in {
            self.EXCHANGE_OVERLOADED,
            self.EXCHANGE_MAINTENANCE,
        }:
            return True

        # Most other errors are not retryable
        return False

    def is_critical(self) -> bool:
        """Check if error is critical and requires immediate attention."""
        critical_errors = {
            # Security violations are always critical
            self.SECURITY_VIOLATION,
            self.INJECTION_DETECTED,
            self.XSS_DETECTED,
            self.REPLAY_ATTACK_DETECTED,
            # Authentication failures that prevent operation
            self.AUTH_REVOKED,
            self.IP_BANNED,
            self.ACCOUNT_SUSPENDED,
            # Data corruption
            self.STREAM_CORRUPTED,
            # System resource exhaustion
            self.MEMORY_LIMIT_EXCEEDED,
            self.RESOURCE_EXHAUSTED,
            # Unrecoverable connection issues
            self.HANDSHAKE_FAILED,
            self.PROTOCOL_ERROR,
        }

        return self in critical_errors

    def get_suggested_action(self) -> str:
        """Get suggested action for this error code."""
        if self == self.RATE_LIMITED:
            return "Implement exponential backoff and retry"
        if self == self.CONNECTION_LOST:
            return "Attempt reconnection with backoff"
        if self == self.AUTH_EXPIRED:
            return "Refresh authentication credentials"
        if self == self.SUBSCRIPTION_LIMIT_EXCEEDED:
            return "Reduce number of subscriptions"
        if self == self.MESSAGE_TOO_LARGE:
            return "Split message into smaller chunks"
        if self == self.HEARTBEAT_TIMEOUT:
            return "Check network connectivity and latency"
        if self == self.SEQUENCE_GAP:
            return "Request missing messages or snapshot"
        if self == self.MEMORY_LIMIT_EXCEEDED:
            return "Reduce memory usage or increase limits"
        if self.is_critical():
            return "Alert operations team immediately"
        if self.is_retryable():
            return "Retry with appropriate backoff strategy"
        return "Log error and investigate root cause"
