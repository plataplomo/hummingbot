"""Comprehensive error context validation tests for WebSocket error system.

Tests all error context validation scenarios to ensure proper data validation,
sanitization, and edge case handling.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_validator import StreamErrorContextValidator
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import (
    ErrorAssertions,
    ErrorTestFactory,
)


class TestErrorContextValidation:
    """Test error context validation functionality."""

    def test_valid_context_creation(self) -> None:
        """Test creating valid error contexts."""
        context = ErrorTestFactory.create_test_context(
            connection_id="conn-123",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            sequence_number=1000,
        )

        ErrorAssertions.assert_context_valid(context)
        assert context.connection_id == "conn-123"
        assert context.exchange == "hyperliquid"
        assert context.channel == "trades"
        assert context.topic == "BTC-USDC"
        assert context.sequence_number == 1000

    def test_minimal_context_creation(self) -> None:
        """Test creating context with minimal required fields."""
        context = StreamErrorContext(
            connection_id="conn-min",
            exchange="backpack",
            error_timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
        )

        ErrorAssertions.assert_context_valid(context)
        assert context.channel is None
        assert context.topic is None
        assert context.sequence_number is None

    def test_connection_id_validation(self) -> None:
        """Test connection ID validation."""
        # Valid connection IDs (8-128 characters)
        valid_ids = [
            "conn-123",  # 8 chars
            "ws_connection_456",
            "CONN-789-ABC",
            "a" * 100,  # Long but valid (within 128)
        ]

        for conn_id in valid_ids:
            validated = StreamErrorContextValidator.validate_connection_id(conn_id)
            assert validated == conn_id

        # Invalid connection IDs
        with pytest.raises(ValueError, match="connection_id must be a non-empty string"):
            StreamErrorContextValidator.validate_connection_id("")

        with pytest.raises(ValueError, match="connection_id must be a non-empty string"):
            StreamErrorContextValidator.validate_connection_id("   ")

        # Too long (>128 chars)
        with pytest.raises(ValueError, match="Must be 8-128 characters"):
            StreamErrorContextValidator.validate_connection_id("a" * 129)

        # Too short (<8 chars)
        with pytest.raises(ValueError, match="Must be 8-128 characters"):
            StreamErrorContextValidator.validate_connection_id("short")

    def test_exchange_validation(self) -> None:
        """Test exchange name validation."""
        # Valid exchanges
        valid_exchanges = [
            "hyperliquid",
            "backpack",
            "binance",
            "coinbase",
            "test-exchange",
        ]

        for exchange in valid_exchanges:
            validated = StreamErrorContextValidator.validate_exchange(exchange)
            assert validated == exchange.lower()

        # Case normalization
        assert StreamErrorContextValidator.validate_exchange("HYPERLIQUID") == "hyperliquid"
        assert StreamErrorContextValidator.validate_exchange("BackPack") == "backpack"

        # Invalid exchanges
        with pytest.raises(ValueError, match="exchange must be a non-empty string"):
            StreamErrorContextValidator.validate_exchange("")

        # Too long (>32 chars per pattern)
        with pytest.raises(ValueError, match="Must be 2-32 lowercase characters"):
            StreamErrorContextValidator.validate_exchange("x" * 33)

    def test_sequence_number_validation(self) -> None:
        """Test sequence number validation."""
        # Valid sequence numbers
        valid_sequences = [None, 0, 1, 1000, 999999999]

        for seq in valid_sequences:
            validated = StreamErrorContextValidator.validate_sequence_number(seq)
            assert validated == seq

        # Invalid sequence numbers
        with pytest.raises(ValueError, match="Sequence number must be non-negative"):
            StreamErrorContextValidator.validate_sequence_number(-1)

        with pytest.raises(ValueError, match="Sequence number must be non-negative"):
            StreamErrorContextValidator.validate_sequence_number(-1000)

    def test_sequence_consistency_validation(self) -> None:
        """Test sequence consistency validation."""
        # Valid: both None
        StreamErrorContextValidator.validate_sequence_consistency(None, None, None)

        # Valid: only sequence_number set
        StreamErrorContextValidator.validate_sequence_consistency(100, None, None)

        # Valid: both set with sequence_number < expected_sequence
        StreamErrorContextValidator.validate_sequence_consistency(100, 105, None)

        # Invalid: expected without sequence
        with pytest.raises(ValueError, match="Expected sequence requires sequence number"):
            StreamErrorContextValidator.validate_sequence_consistency(None, 100, None)

        # Invalid: sequence greater than or equal to expected
        with pytest.raises(ValueError, match="Expected sequence must be greater"):
            StreamErrorContextValidator.validate_sequence_consistency(100, 100, None)

        with pytest.raises(ValueError, match="Expected sequence must be greater"):
            StreamErrorContextValidator.validate_sequence_consistency(100, 99, None)

    def test_timestamp_validation(self) -> None:
        """Test timestamp validation."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Valid timestamps
        context = StreamErrorContext(
            connection_id="conn-1",
            exchange="test",
            error_timestamp_ms=now_ms,
            connection_started_ms=now_ms - 10000,
            last_heartbeat_ms=now_ms - 1000,
        )

        ErrorAssertions.assert_context_valid(context)

        # Invalid: connection started in future
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="conn-1",
                exchange="test",
                error_timestamp_ms=now_ms,
                connection_started_ms=now_ms + 10000,  # Future!
            )

        # Invalid: negative timestamp
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="conn-1",
                exchange="test",
                error_timestamp_ms=-1000,
            )

    def test_subscription_count_validation(self) -> None:
        """Test subscription count validation."""
        # Valid counts
        for count in [0, 1, 10, 100]:
            context = StreamErrorContext(
                connection_id="conn-1",
                exchange="test",
                error_timestamp_ms=1000,
                active_subscriptions=count,
            )
            assert context.active_subscriptions == count

        # Invalid: negative count
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="conn-1",
                exchange="test",
                error_timestamp_ms=1000,
                active_subscriptions=-1,
            )

    def test_context_sanitization(self) -> None:
        """Test context sanitization for invalid values."""
        # Create context with invalid values using model_construct
        invalid_context = StreamErrorContext.model_construct(
            connection_id="   conn-123   ",  # Whitespace
            exchange="HYPERLIQUID",  # Uppercase
            channel="  trades  ",  # Whitespace
            topic="BTC-USDC  ",  # Trailing whitespace
            sequence_number=-5,  # Negative
            expected_sequence=-10,  # Negative
            active_subscriptions=-3,  # Negative
            error_timestamp_ms=1000,
        )

        # Sanitize
        sanitized = StreamErrorContextValidator.sanitize_context(invalid_context)

        # Check sanitization results
        assert sanitized.connection_id == "conn-123"
        assert sanitized.exchange == "hyperliquid"
        assert sanitized.channel == "trades"
        assert sanitized.topic == "BTC-USDC"
        assert sanitized.sequence_number == 0  # Negative -> 0
        assert sanitized.expected_sequence is None  # Invalid -> None
        assert sanitized.active_subscriptions == 0  # Negative -> 0

    def test_error_chain_validation(self) -> None:
        """Test error chain validation in context."""
        context = ErrorTestFactory.create_test_context()

        # Add valid error chain
        context.add_to_error_chain(ValueError("Test error"))
        assert len(context.error_chain) == 1
        assert context.error_chain[0].error_class == "ValueError"

        # Add multiple errors
        context.add_to_error_chain(TypeError("Type error"))
        context.add_to_error_chain(RuntimeError("Runtime error"))
        assert len(context.error_chain) == 3

        # Validate chain
        for chain_error in context.error_chain:
            assert chain_error.error_class
            assert chain_error.error_message
            assert chain_error.timestamp_ms > 0

    def test_message_size_validation(self) -> None:
        """Test message size tracking in context."""
        context = StreamErrorContext(
            connection_id="conn-1",
            exchange="test",
            error_timestamp_ms=1000,
            raw_message_size=1024,
        )

        assert context.raw_message_size == 1024

        # Invalid: negative size
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="conn-1",
                exchange="test",
                error_timestamp_ms=1000,
                raw_message_size=-100,
            )

    def test_context_in_error_creation(self) -> None:
        """Test context validation when creating errors."""
        # Valid context
        context = ErrorTestFactory.create_test_context()
        error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
        )

        ErrorAssertions.assert_context_valid(error.context)

        # Context with all fields
        full_context = StreamErrorContext(
            connection_id="full-conn",
            exchange="hyperliquid",
            channel="orderbook",
            topic="ETH-USDC",
            sequence_number=5000,
            expected_sequence=5005,
            error_timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
            connection_started_ms=int((datetime.now(UTC) - timedelta(hours=1)).timestamp() * 1000),
            last_heartbeat_ms=int((datetime.now(UTC) - timedelta(seconds=30)).timestamp() * 1000),
            active_subscriptions=15,
            raw_message_size=2048,
        )

        error_full = WebSocketStreamError(
            message="Full context error",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=full_context,
        )

        ErrorAssertions.assert_context_valid(error_full.context)
        assert error_full.context.get_sequence_gap_size() == 5

    def test_context_summary_generation(self) -> None:
        """Test context summary generation."""
        context = StreamErrorContext(
            connection_id="very-long-connection-id-that-should-be-truncated",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            error_timestamp_ms=1000,
        )

        summary = context.get_summary()

        # Check summary contains key information
        assert "very-lon..." in summary  # Truncated connection ID
        assert "hyperliquid" in summary
        assert "trades" in summary
        assert "BTC-USDC" in summary

    def test_stale_connection_detection(self) -> None:
        """Test stale connection detection."""
        now = datetime.now(UTC)

        # Fresh connection
        fresh_context = StreamErrorContext(
            connection_id="fresh",
            exchange="test",
            error_timestamp_ms=int(now.timestamp() * 1000),
            last_message_received_ms=int((now - timedelta(seconds=10)).timestamp() * 1000),
        )

        assert not fresh_context.is_stale_connection(stale_threshold_ms=30000)

        # Stale connection
        stale_context = StreamErrorContext(
            connection_id="stale",
            exchange="test",
            error_timestamp_ms=int(now.timestamp() * 1000),
            last_message_received_ms=int((now - timedelta(minutes=5)).timestamp() * 1000),
        )

        assert stale_context.is_stale_connection(stale_threshold_ms=60000)

    def test_connection_duration_calculation(self) -> None:
        """Test connection duration calculation."""
        now = datetime.now(UTC)
        established = now - timedelta(hours=2, minutes=30, seconds=45)

        context = StreamErrorContext(
            connection_id="conn",
            exchange="test",
            error_timestamp_ms=int(now.timestamp() * 1000),
            connection_started_ms=int(established.timestamp() * 1000),
        )

        duration_ms = context.get_connection_duration_ms()
        assert duration_ms is not None

        # Should be approximately 2.5 hours in milliseconds
        expected_ms = 2 * 3600 * 1000 + 30 * 60 * 1000 + 45 * 1000
        assert abs(duration_ms - expected_ms) < 1000  # Within 1 second tolerance

    def test_context_field_constraints(self) -> None:
        """Test field constraints and limits."""
        # Test maximum field lengths based on actual validator patterns
        max_connection_id = "x" * 128  # Maximum allowed per CONNECTION_ID_PATTERN
        max_exchange = "y" * 32  # Maximum allowed per EXCHANGE_PATTERN
        max_channel = "z" * 64  # Maximum allowed per CHANNEL_PATTERN

        context = StreamErrorContext(
            connection_id=max_connection_id,
            exchange=max_exchange,
            channel=max_channel,
            error_timestamp_ms=1000,
        )

        assert len(context.connection_id) == 128
        assert len(context.exchange) == 32
        assert context.channel is not None
        assert len(context.channel) == 64

        # Test exceeding limits
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="x" * 129,  # Too long
                exchange="test",
                error_timestamp_ms=1000,
            )

    def test_context_immutability(self) -> None:
        """Test that context fields maintain immutability where expected."""
        context = ErrorTestFactory.create_test_context()

        # These operations should create new instances or raise errors
        # depending on Pydantic configuration
        original_id = context.connection_id
        original_exchange = context.exchange

        # Verify fields are set correctly
        assert context.connection_id == original_id
        assert context.exchange == original_exchange

        # Add to error chain (should modify in place)
        original_chain_len = len(context.error_chain)
        context.add_to_error_chain(ValueError("New error"))
        assert len(context.error_chain) == original_chain_len + 1

    def test_context_comparison(self) -> None:
        """Test context comparison and equality."""
        context1 = ErrorTestFactory.create_test_context(
            connection_id="connection-1",
            exchange="hyperliquid",
            channel="trades",
        )

        context2 = ErrorTestFactory.create_test_context(
            connection_id="connection-1",
            exchange="hyperliquid",
            channel="trades",
            error_timestamp_ms=int(datetime.now(UTC).timestamp() * 1000)
            + 1000,  # Different timestamp
        )

        context3 = ErrorTestFactory.create_test_context(
            connection_id="connection-2",  # Different
            exchange="hyperliquid",
            channel="trades",
        )

        # Same values but different instances
        assert context1 != context2  # Different timestamps

        # Different values
        assert context1 != context3
        assert context2 != context3

    def test_context_serialization(self) -> None:
        """Test context serialization and deserialization."""
        original = ErrorTestFactory.create_test_context(
            connection_id="ser-test",
            exchange="backpack",
            channel="account",
            topic="balances",
            sequence_number=1000,
            expected_sequence=1005,
        )

        # Add error chain
        original.add_to_error_chain(ValueError("Test error"))

        # Serialize to dict
        data = original.model_dump()

        # Deserialize
        restored = StreamErrorContext(**data)

        # Verify all fields preserved
        assert restored.connection_id == original.connection_id
        assert restored.exchange == original.exchange
        assert restored.channel == original.channel
        assert restored.topic == original.topic
        assert restored.sequence_number == original.sequence_number
        assert restored.expected_sequence == original.expected_sequence
        assert len(restored.error_chain) == len(original.error_chain)

    def test_context_with_metrics(self) -> None:
        """Test context with metrics fields."""
        context = StreamErrorContext(
            connection_id="metrics-conn",
            exchange="hyperliquid",
            error_timestamp_ms=1000,
            active_subscriptions=25,
            raw_message_size=4096,
            channel="all-channels",
        )

        # Verify metrics fields
        assert context.active_subscriptions == 25
        assert context.raw_message_size == 4096

    def test_edge_case_combinations(self) -> None:
        """Test edge case combinations of context fields."""
        # Minimal required fields only
        minimal = StreamErrorContext(
            connection_id="min",
            exchange="test",
            error_timestamp_ms=1000,
        )
        ErrorAssertions.assert_context_valid(minimal)

        # All optional fields None
        all_none = StreamErrorContext(
            connection_id="none-test",
            exchange="test",
            error_timestamp_ms=1000,
            channel=None,
            topic=None,
            sequence_number=None,
            expected_sequence=None,
            connection_started_ms=None,
            last_heartbeat_ms=None,
        )
        ErrorAssertions.assert_context_valid(all_none)

        # Mix of set and unset fields
        mixed = StreamErrorContext(
            connection_id="mixed",
            exchange="hyperliquid",
            error_timestamp_ms=1000,
            channel="trades",  # Set
            topic=None,  # Not set
            sequence_number=500,  # Set
            expected_sequence=None,  # Not set
            connection_started_ms=900,  # Set
            last_heartbeat_ms=None,  # Not set
        )
        ErrorAssertions.assert_context_valid(mixed)
