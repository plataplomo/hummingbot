"""Unit tests for audit logger module."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, SignalType, TimeInForce
from cyberdelta.logic.monitoring.audit_logger import (
    AuditEvent,
    AuditEventType,
    AuditSeverity,
)
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.trade_signal import TradeSignal


@pytest.fixture
def mock_config() -> MagicMock:
    """Create mock configuration for testing."""
    config = MagicMock(spec=AppSettings)

    # General config
    config.general.audit_log_enabled = True
    config.general.log_sensitive_data = False
    config.general.safe_mode = False
    config.general.shutdown_grace_period = 5

    # Monitoring config
    config.monitoring.audit_log_file = "/tmp/test_audit.log"
    config.monitoring.audit_retention_days = 30
    config.monitoring.audit_log_format = "json"
    config.monitoring.audit_buffer_size = 10
    config.monitoring.audit_flush_interval_seconds = 60

    return config


@pytest.fixture
def audit_logger(mock_config: MagicMock) -> AuditLogger:
    """Create audit logger instance."""
    return AuditLogger(mock_config)


@pytest.fixture
def sample_order() -> Order:
    """Create sample order for testing."""
    return Order(
        exchange_order_id="order_123",
        client_order_id="client_123",
        exchange=ExchangeName.HYPERLIQUID,
        symbol=exchanges.hyperliquid("BTC"),
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal(50000),
        quantity_requested=Decimal("0.1"),
        time_in_force=TimeInForce.GTC,
        status=OrderStatus.OPEN,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_trade() -> Trade:
    """Create sample trade for testing."""
    return Trade(
        id="trade_123",
        symbol=exchanges.hyperliquid("BTC"),
        executed_at=datetime.now(UTC),
        side=OrderSide.BUY,
        order_id="order_123",
        exchange=ExchangeName.HYPERLIQUID,
        price=Decimal(50000),
        quantity=Decimal("0.1"),
        fee=Decimal(5),
        fee_asset="USD",
    )


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Create sample trade signal for testing."""
    return TradeSignal(
        signal_id="signal_123",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        source_strategy="momentum",
        symbol=exchanges.hyperliquid("BTC"),
        exchange=ExchangeName.HYPERLIQUID,
        confidence=0.85,
        price=Decimal(50000),
        metadata={"indicator": "RSI"},
    )


class TestAuditLogger:
    """Test audit logger functionality."""

    @pytest.mark.asyncio
    async def test_init(self, mock_config: MagicMock) -> None:
        """Test audit logger initialization."""
        logger = AuditLogger(mock_config)

        assert logger.config == mock_config
        assert logger._enabled is True
        assert logger._log_sensitive_data is False
        assert logger._audit_log_file == Path("/tmp/test_audit.log")
        assert logger._retention_days == 30
        assert logger._log_format == "json"
        assert logger._buffer_size == 10
        assert logger._flush_interval == 60

    @pytest.mark.asyncio
    async def test_disabled_audit_logging(self, mock_config: MagicMock) -> None:
        """Test behavior when audit logging is disabled."""
        mock_config.general.audit_log_enabled = False
        logger = AuditLogger(mock_config)

        await logger.start()

        # Should not create flush task when disabled
        assert logger._flush_task is None

        # Should not log events when disabled
        event = AuditEvent(event_type=AuditEventType.ORDER_PLACED, description="Test order")
        await logger.log_event(event)

        assert len(logger._event_buffer) == 0
        assert logger._event_count == 0

    @pytest.mark.asyncio
    async def test_log_event_basic(self, audit_logger: AuditLogger) -> None:
        """Test basic event logging."""
        event = AuditEvent(
            event_type=AuditEventType.ORDER_PLACED,
            severity=AuditSeverity.INFO,
            description="Order placed successfully",
            entity_type="Order",
            entity_id="order_123",
        )

        await audit_logger.log_event(event)

        assert len(audit_logger._event_buffer) == 1
        assert audit_logger._event_count == 1

        logged_event = audit_logger._event_buffer[0]
        assert logged_event.event_type == AuditEventType.ORDER_PLACED
        assert logged_event.session_id == audit_logger._session_id
        assert logged_event.correlation_id is not None

    @pytest.mark.asyncio
    async def test_log_order_event(self, audit_logger: AuditLogger, sample_order: Order) -> None:
        """Test order event logging."""
        await audit_logger.log_order_event(
            sample_order, AuditEventType.ORDER_PLACED, "Order placed for BTC", strategy="momentum"
        )

        assert len(audit_logger._event_buffer) == 1
        event = audit_logger._event_buffer[0]

        assert event.event_type == AuditEventType.ORDER_PLACED
        assert event.entity_type == "Order"
        assert event.entity_id == "order_123"
        assert event.exchange == ExchangeName.HYPERLIQUID
        assert event.symbol == exchanges.hyperliquid("BTC")
        assert event.metadata["strategy"] == "momentum"
        assert event.metadata["side"] == "buy"
        assert event.metadata["quantity"] == 0.1
        assert event.metadata["price"] == 50000.0

    @pytest.mark.asyncio
    async def test_log_trade_event(self, audit_logger: AuditLogger, sample_trade: Trade) -> None:
        """Test trade event logging."""
        await audit_logger.log_trade_event(
            sample_trade, AuditEventType.ORDER_FILLED, "Trade executed", slippage=0.01
        )

        assert len(audit_logger._event_buffer) == 1
        event = audit_logger._event_buffer[0]

        assert event.event_type == AuditEventType.ORDER_FILLED
        assert event.entity_type == "Trade"
        assert event.entity_id == "trade_123"
        assert event.metadata["slippage"] == 0.01
        assert event.metadata["fee"] == 5.0

    @pytest.mark.asyncio
    async def test_log_signal_event(
        self, audit_logger: AuditLogger, sample_signal: TradeSignal
    ) -> None:
        """Test signal event logging."""
        await audit_logger.log_signal_event(
            sample_signal, AuditEventType.SIGNAL_GENERATED, "Signal generated by momentum strategy"
        )

        assert len(audit_logger._event_buffer) == 1
        event = audit_logger._event_buffer[0]

        assert event.event_type == AuditEventType.SIGNAL_GENERATED
        assert event.entity_type == "TradeSignal"
        assert event.entity_id == "signal_123"
        assert event.metadata["source_strategy"] == "momentum"
        assert event.metadata["confidence"] == 0.85

    @pytest.mark.asyncio
    async def test_log_risk_event(self, audit_logger: AuditLogger) -> None:
        """Test risk event logging."""
        await audit_logger.log_risk_event(
            AuditEventType.RISK_LIMIT_EXCEEDED,
            "Position size exceeds maximum",
            severity=AuditSeverity.WARNING,
            risk_score=0.85,
            violations=["max_position_size", "max_exposure"],
            position_size=10000,
            max_allowed=5000,
        )

        assert len(audit_logger._event_buffer) == 1
        event = audit_logger._event_buffer[0]

        assert event.event_type == AuditEventType.RISK_LIMIT_EXCEEDED
        assert event.severity == AuditSeverity.WARNING
        assert event.risk_score == 0.85
        assert event.compliance_flags == ["max_position_size", "max_exposure"]
        assert event.metadata["position_size"] == 10000
        assert event.metadata["max_allowed"] == 5000

    @pytest.mark.asyncio
    async def test_log_error_event(self, audit_logger: AuditLogger) -> None:
        """Test error event logging."""
        error = ValueError("Invalid order parameters")

        await audit_logger.log_error_event(error, "order_validation", order_id="order_123")

        await audit_logger.log_error_event(error, "order_validation", order_id="order_123")

        assert len(audit_logger._event_buffer) == 1
        event = audit_logger._event_buffer[0]

        assert event.event_type == AuditEventType.ERROR_OCCURRED
        assert event.severity == AuditSeverity.ERROR
        assert "Invalid order parameters" in event.description
        assert event.metadata["error_type"] == "ValueError"
        assert event.metadata["context"] == "order_validation"
        assert event.metadata["order_id"] == "order_123"

    @pytest.mark.asyncio
    async def test_sensitive_data_filtering(self, audit_logger: AuditLogger) -> None:
        """Test that sensitive data is filtered when configured."""
        # Log sensitive data disabled by default in fixture
        event = AuditEvent(
            event_type=AuditEventType.CONFIG_CHANGED,
            description="Configuration updated",
            metadata={
                "api_key": "secret_key_123",
                "password": "secret_pass",
                "normal_field": "normal_value",
            },
        )

        await audit_logger.log_event(event)

        filtered_event = audit_logger._event_buffer[0]
        assert filtered_event.metadata["api_key"] == "[REDACTED]"
        assert filtered_event.metadata["password"] == "[REDACTED]"
        assert filtered_event.metadata["normal_field"] == "normal_value"

    @pytest.mark.asyncio
    async def test_sensitive_data_not_filtered_when_enabled(self, mock_config: MagicMock) -> None:
        """Test that sensitive data is not filtered when logging is enabled."""
        mock_config.general.log_sensitive_data = True
        logger = AuditLogger(mock_config)

        event = AuditEvent(
            event_type=AuditEventType.CONFIG_CHANGED,
            description="Configuration updated",
            metadata={"api_key": "secret_key_123", "password": "secret_pass"},
        )

        await logger.log_event(event)

        logged_event = logger._event_buffer[0]
        assert logged_event.metadata["api_key"] == "secret_key_123"
        assert logged_event.metadata["password"] == "secret_pass"

    @pytest.mark.asyncio
    async def test_buffer_flush_on_size_limit(
        self, audit_logger: AuditLogger, mock_config: MagicMock
    ) -> None:
        """Test that buffer flushes when size limit is reached."""
        mock_config.monitoring.audit_buffer_size = 3
        audit_logger._buffer_size = 3

        # Mock the flush method
        audit_logger._flush_buffer = AsyncMock()

        # Add events up to buffer size
        for i in range(4):
            event = AuditEvent(event_type=AuditEventType.ORDER_PLACED, description=f"Order {i}")
            await audit_logger.log_event(event)

        # Should have flushed once when buffer was full
        audit_logger._flush_buffer.assert_called_once()
        # Buffer should have 1 event (the 4th one)
        assert len(audit_logger._event_buffer) == 1

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, audit_logger: AuditLogger) -> None:
        """Test audit logger start and stop lifecycle."""
        # Mock flush buffer
        audit_logger._flush_buffer = AsyncMock()

        # Start logger
        await audit_logger.start()

        # Should have logged system start event
        assert len(audit_logger._event_buffer) >= 1
        start_event = next(
            e for e in audit_logger._event_buffer if e.event_type == AuditEventType.SYSTEM_STARTED
        )
        assert start_event is not None
        assert start_event.metadata["safe_mode"] is False

        # Should have started flush task
        assert audit_logger._flush_task is not None
        assert not audit_logger._flush_task.done()

        # Stop logger
        await audit_logger.stop()

        # Should have logged system stop event
        stop_events = [
            e for e in audit_logger._event_buffer if e.event_type == AuditEventType.SYSTEM_STOPPED
        ]
        assert len(stop_events) > 0 or audit_logger._flush_buffer.called

        # Flush task should be cancelled
        assert audit_logger._flush_task.cancelled() or audit_logger._flush_task.done()

    @pytest.mark.asyncio
    async def test_write_json_format(self, audit_logger: AuditLogger) -> None:
        """Test writing events in JSON format."""
        events = [
            AuditEvent(event_type=AuditEventType.ORDER_PLACED, description="Order 1"),
            AuditEvent(event_type=AuditEventType.ORDER_FILLED, description="Order 2"),
        ]

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".log") as f:
            audit_logger._audit_log_file = Path(f.name)

        try:
            await audit_logger._write_json_format(events)

            # Read and verify
            with open(audit_logger._audit_log_file) as f:
                lines = f.readlines()

            assert len(lines) == 2

            # Parse JSON lines
            event1 = json.loads(lines[0])
            assert event1["event_type"] == "order_placed"
            assert event1["description"] == "Order 1"

            event2 = json.loads(lines[1])
            assert event2["event_type"] == "order_filled"
            assert event2["description"] == "Order 2"

        finally:
            # Cleanup
            if audit_logger._audit_log_file.exists():
                audit_logger._audit_log_file.unlink()

    @pytest.mark.asyncio
    async def test_write_text_format(
        self, audit_logger: AuditLogger, mock_config: MagicMock
    ) -> None:
        """Test writing events in text format."""
        mock_config.monitoring.audit_log_format = "text"
        audit_logger._log_format = "text"

        events = [
            AuditEvent(
                event_type=AuditEventType.ORDER_PLACED,
                severity=AuditSeverity.INFO,
                description="Order placed",
                entity_type="Order",
                entity_id="123",
                exchange=ExchangeName.HYPERLIQUID,
                symbol=Symbol("BTC_USD"),
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".log") as f:
            audit_logger._audit_log_file = Path(f.name)

        try:
            await audit_logger._write_text_format(events)

            # Read and verify
            with open(audit_logger._audit_log_file) as f:
                content = f.read()

            assert "[INFO]" in content
            assert "[order_placed]" in content
            assert "Order placed" in content
            assert "(Entity: Order:123)" in content
            assert "(Exchange: hyperliquid)" in content
            assert "(Symbol: BTC_USD)" in content

        finally:
            # Cleanup
            if audit_logger._audit_log_file.exists():
                audit_logger._audit_log_file.unlink()

    @pytest.mark.asyncio
    async def test_session_stats(self, audit_logger: AuditLogger) -> None:
        """Test getting session statistics."""
        # Log some events
        for i in range(5):
            event = AuditEvent(event_type=AuditEventType.ORDER_PLACED, description=f"Order {i}")
            await audit_logger.log_event(event)

        stats = audit_logger.get_session_stats()

        assert stats["session_id"] == audit_logger._session_id
        assert stats["total_events"] == 5
        assert stats["buffer_size"] == 5
        assert stats["enabled"] is True
        assert stats["log_sensitive_data"] is False
        assert "session_duration_seconds" in stats
        assert stats["session_duration_seconds"] >= 0

    @pytest.mark.asyncio
    async def test_multiple_start_calls(self, audit_logger: AuditLogger) -> None:
        """Test that multiple start calls are handled properly."""
        await audit_logger.start()

        # Create a mock task that's not done
        mock_task = MagicMock()
        mock_task.done.return_value = False
        audit_logger._flush_task = mock_task

        # Second start should not create new task
        await audit_logger.start()

        # Task should remain the same
        assert audit_logger._flush_task == mock_task
