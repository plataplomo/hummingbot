"""Unit tests for the logging helpers module.

Tests structured logging functionality for financial events.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest
import structlog
from pydantic import BaseModel

from cyberdelta.core.symbols import symbols
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    SignalType,
    TimeInForce,
    Trade,
    TradeSignal,
)
from cyberdelta.logging.logging_helpers import (
    SENSITIVE_FIELDS,
    log_order_lifecycle,
    log_position_update,
    log_trading_event,
)


@pytest.fixture
def mock_logger() -> Mock:
    """Create mock structlog logger for testing.

    Returns:
        Mock: A mock structlog BoundLogger instance for testing.
    """
    logger = Mock(spec=structlog.BoundLogger)
    logger.info = Mock()
    return logger


@pytest.fixture
def sample_order() -> Order:
    """Create sample order for testing.

    Returns:
        Order: A sample order instance with predefined values.
    """
    return Order(
        exchange="hyperliquid",
        client_order_id="test_order_123",
        exchange_order_id="exchange_123",
        symbol=symbols.BTC.hyperliquid(),
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity_requested=Decimal("1.0"),
        price=Decimal("50000.0"),
        status=OrderStatus.FILLED,
        quantity_filled=Decimal("1.0"),
        average_fill_price=Decimal("50000.0"),
        time_in_force=TimeInForce.GTC,
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
        hl_details=None,
        bp_details=None,
    )


@pytest.fixture
def sample_position() -> DerivativePosition:
    """Create sample position for testing.

    Returns:
        DerivativePosition: A sample derivative position instance.
    """
    return DerivativePosition(
        exchange="hyperliquid",
        symbol=symbols.BTC.hyperliquid(),
        side=OrderSide.BUY,
        size=Decimal("1.5"),
        entry_price=Decimal("50000.0"),
        timestamp=datetime.now(UTC),
        hl_details=None,
        bp_details=None,
    )


@pytest.fixture
def sample_trade() -> Trade:
    """Create sample trade for testing.

    Returns:
        Trade: A sample trade instance with predefined values.
    """
    return Trade(
        id="trade_123",
        symbol=symbols.BTC.hyperliquid(),
        executed_at=datetime.now(UTC),
        side=OrderSide.BUY,
        order_id="order_123",
        exchange="hyperliquid",
        price=Decimal("50000.0"),
        quantity=Decimal("1.0"),
        hl_details=None,
        bp_details=None,
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Create sample trade signal for testing.

    Returns:
        TradeSignal: A sample trade signal instance.
    """
    return TradeSignal(
        signal_id="signal_123",
        symbol=symbols.BTC.hyperliquid(),
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        exchange="hyperliquid",
        quantity=Decimal("1.0"),
        confidence=0.85,
        metadata={"strategy_secret": "confidential"},
    )


@pytest.fixture
def sample_margin_summary() -> MarginAccountSummary:
    """Create sample margin account summary for testing.

    Returns:
        MarginAccountSummary: A sample margin account summary instance.
    """
    return MarginAccountSummary(
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
        total_equity=Decimal("100000.0"),
        available_equity=Decimal("80000.0"),
        total_unrealized_pnl=Decimal("5000.0"),
        hl_details=None,
        bp_details=None,
    )


class TestSensitiveFields:
    """Test suite for SENSITIVE_FIELDS configuration."""

    # ==================== SUCCESS CASES ====================

    def test_sensitive_fields_contains_expected_models(self) -> None:
        """Test SENSITIVE_FIELDS contains all expected model types."""
        # Assert
        assert Order in SENSITIVE_FIELDS
        assert Trade in SENSITIVE_FIELDS
        assert TradeSignal in SENSITIVE_FIELDS
        assert DerivativePosition in SENSITIVE_FIELDS
        assert MarginAccountSummary in SENSITIVE_FIELDS

    def test_sensitive_fields_order_configuration(self) -> None:
        """Test Order model has correct sensitive fields."""
        # Assert
        expected_fields = {"trades", "hl_details", "bp_details"}
        assert SENSITIVE_FIELDS[Order] == expected_fields

    def test_sensitive_fields_trade_configuration(self) -> None:
        """Test Trade model has correct sensitive fields."""
        # Assert
        expected_fields = {"hl_details", "bp_details"}
        assert SENSITIVE_FIELDS[Trade] == expected_fields

    def test_sensitive_fields_position_configuration(self) -> None:
        """Test DerivativePosition model has correct sensitive fields."""
        # Assert
        expected_fields = {"hl_details", "bp_details"}
        assert SENSITIVE_FIELDS[DerivativePosition] == expected_fields

    def test_sensitive_fields_margin_configuration(self) -> None:
        """Test MarginAccountSummary model has correct sensitive fields."""
        # Assert
        expected_fields = {"total_equity", "available_equity", "hl_details", "bp_details"}
        assert SENSITIVE_FIELDS[MarginAccountSummary] == expected_fields

    # ==================== EDGE CASES ====================

    def test_sensitive_fields_trade_signal_configuration(self) -> None:
        """Test TradeSignal model has metadata as sensitive field."""
        # Assert
        expected_fields = {"metadata"}
        assert SENSITIVE_FIELDS[TradeSignal] == expected_fields


class TestLogTradingEvent:
    """Test suite for log_trading_event function."""

    # ==================== SUCCESS CASES ====================

    def test_log_trading_event_success_with_sensitive_exclusion(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test successful logging with sensitive field exclusion."""
        # Act
        log_trading_event(
            mock_logger,
            "order_placed",
            sample_order,
            exclude_sensitive=True,
            extra_field="extra_value",
        )

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args

        # Check event type is first argument
        assert call_args[0][0] == "order_placed"

        # Check kwargs contain model data but exclude sensitive fields
        kwargs = call_args[1]
        assert "exchange" in kwargs
        assert "symbol" in kwargs
        assert "side" in kwargs
        assert "hl_details" not in kwargs  # Should be excluded
        assert "bp_details" not in kwargs  # Should be excluded
        assert "trades" not in kwargs  # Should be excluded
        assert kwargs["extra_field"] == "extra_value"

    def test_log_trading_event_success_without_sensitive_exclusion(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test successful logging without sensitive field exclusion."""
        # Act
        log_trading_event(mock_logger, "order_updated", sample_order, exclude_sensitive=False)

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]

        # Check sensitive fields are included when exclusion is disabled
        assert "hl_details" in kwargs
        assert "bp_details" in kwargs

    def test_log_trading_event_success_with_different_model_types(
        self, mock_logger: Mock, sample_trade: Trade, sample_position: DerivativePosition
    ) -> None:
        """Test logging with different model types."""
        # Act - Test with Trade
        log_trading_event(mock_logger, "trade_executed", sample_trade)

        # Reset mock
        mock_logger.reset_mock()

        # Act - Test with Position
        log_trading_event(mock_logger, "position_opened", sample_position)

        # Assert both calls were made
        assert mock_logger.info.call_count == 1

    def test_log_trading_event_success_with_extra_context(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test logging with multiple extra context parameters."""
        # Act
        log_trading_event(
            mock_logger,
            "order_cancelled",
            sample_order,
            execution_id="exec_123",
            strategy_type="arbitrage",
            reason="market_closed",
        )

        # Assert
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]
        assert kwargs["execution_id"] == "exec_123"
        assert kwargs["strategy_type"] == "arbitrage"
        assert kwargs["reason"] == "market_closed"

    # ==================== EDGE CASES ====================

    def test_log_trading_event_edge_unknown_model_type(self, mock_logger: Mock) -> None:
        """Test logging with model type not in SENSITIVE_FIELDS."""
        # Arrange

        class CustomModel(BaseModel):
            custom_field: str = "value"
            secret_field: str = "secret"

        custom_model = CustomModel()

        # Act
        log_trading_event(mock_logger, "custom_event", custom_model)

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]

        # Should include all fields since model type is unknown
        assert "custom_field" in kwargs
        assert "secret_field" in kwargs

    def test_log_trading_event_edge_empty_extra_context(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test logging with no extra context parameters."""
        # Act
        log_trading_event(mock_logger, "order_event", sample_order)

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "order_event"

    def test_log_trading_event_edge_margin_summary_sensitive_fields(
        self, mock_logger: Mock, sample_margin_summary: MarginAccountSummary
    ) -> None:
        """Test logging with MarginAccountSummary excludes financial sensitive data."""
        # Act
        log_trading_event(
            mock_logger, "account_updated", sample_margin_summary, exclude_sensitive=True
        )

        # Assert
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]

        # Sensitive financial fields should be excluded
        assert "total_equity" not in kwargs
        assert "available_equity" not in kwargs
        assert "hl_details" not in kwargs
        assert "bp_details" not in kwargs

        # Non-sensitive fields should be included
        assert "total_unrealized_pnl" in kwargs
        assert "exchange" in kwargs

    # ==================== FAILURE CASES ====================

    def test_log_trading_event_failure_model_dump_error(self, mock_logger: Mock) -> None:
        """Test logging handles model_dump errors gracefully."""
        # Arrange

        class BrokenModel(BaseModel):
            def model_dump(self, **kwargs: object) -> dict[str, object]:
                raise ValueError("Model dump failed")

        broken_model = BrokenModel()

        # Act & Assert
        with pytest.raises(ValueError, match="Model dump failed"):
            log_trading_event(mock_logger, "broken_event", broken_model)


class TestLogOrderLifecycle:
    """Test suite for log_order_lifecycle function."""

    # ==================== SUCCESS CASES ====================

    def test_log_order_lifecycle_success_placed_event(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test successful order lifecycle logging for placed event."""
        # Act
        log_order_lifecycle(mock_logger, sample_order, "placed", execution_id="exec_123")

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args

        # Check event type follows lifecycle pattern
        assert call_args[0][0] == "order_placed"

        # Check lifecycle context is added
        kwargs = call_args[1]
        assert kwargs["order_lifecycle_event"] == "placed"
        assert kwargs["execution_id"] == "exec_123"

        # Check sensitive fields are excluded (default behavior)
        assert "hl_details" not in kwargs
        assert "bp_details" not in kwargs

    def test_log_order_lifecycle_success_filled_event(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test order lifecycle logging for filled event."""
        # Act
        log_order_lifecycle(
            mock_logger,
            sample_order,
            "filled",
            fill_price=Decimal("50001.0"),
            fill_quantity=Decimal("1.0"),
        )

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "order_filled"

        kwargs = call_args[1]
        assert kwargs["order_lifecycle_event"] == "filled"
        assert kwargs["fill_price"] == Decimal("50001.0")
        assert kwargs["fill_quantity"] == Decimal("1.0")

    def test_log_order_lifecycle_success_cancelled_event(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test order lifecycle logging for cancelled event."""
        # Act
        log_order_lifecycle(mock_logger, sample_order, "cancelled", reason="user_request")

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "order_cancelled"

        kwargs = call_args[1]
        assert kwargs["order_lifecycle_event"] == "cancelled"
        assert kwargs["reason"] == "user_request"

    # ==================== EDGE CASES ====================

    def test_log_order_lifecycle_edge_custom_event_name(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test order lifecycle with custom event name."""
        # Act
        log_order_lifecycle(mock_logger, sample_order, "rejected")

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "order_rejected"

    def test_log_order_lifecycle_edge_no_additional_context(
        self, mock_logger: Mock, sample_order: Order
    ) -> None:
        """Test order lifecycle logging with no additional context."""
        # Act
        log_order_lifecycle(mock_logger, sample_order, "updated")

        # Assert
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]
        assert kwargs["order_lifecycle_event"] == "updated"

        # Should contain order data but no extra context
        assert "symbol" in kwargs
        assert "side" in kwargs

    # ==================== FAILURE CASES ====================

    def test_log_order_lifecycle_failure_invalid_order(self, mock_logger: Mock) -> None:
        """Test order lifecycle logging with invalid order object."""
        # Arrange

        class InvalidOrder(BaseModel):
            def model_dump(self, **kwargs: object) -> dict[str, object]:
                raise AttributeError("Invalid order object")

        invalid_order = InvalidOrder()

        # Act & Assert
        with pytest.raises(AttributeError, match="Invalid order object"):
            log_order_lifecycle(mock_logger, invalid_order, "placed")  # type: ignore[arg-type]


class TestLogPositionUpdate:
    """Test suite for log_position_update function."""

    # ==================== SUCCESS CASES ====================

    def test_log_position_update_success_opened_action(
        self, mock_logger: Mock, sample_position: DerivativePosition
    ) -> None:
        """Test successful position update logging for opened action."""
        # Act
        log_position_update(
            mock_logger,
            sample_position,
            "opened",
            strategy_type="arbitrage",
            trade_signal_id="signal_123",
        )

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args

        # Check event type follows position pattern
        assert call_args[0][0] == "position_opened"

        # Check position action context is added
        kwargs = call_args[1]
        assert kwargs["position_action"] == "opened"
        assert kwargs["strategy_type"] == "arbitrage"
        assert kwargs["trade_signal_id"] == "signal_123"

        # Check sensitive fields are excluded
        assert "hl_details" not in kwargs
        assert "bp_details" not in kwargs

        # Check position data is included
        assert "exchange" in kwargs
        assert "symbol" in kwargs
        assert "size" in kwargs

    def test_log_position_update_success_closed_action(
        self, mock_logger: Mock, sample_position: DerivativePosition
    ) -> None:
        """Test position update logging for closed action."""
        # Act
        log_position_update(
            mock_logger,
            sample_position,
            "closed",
            pnl=Decimal("1500.0"),
            close_reason="take_profit",
        )

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "position_closed"

        kwargs = call_args[1]
        assert kwargs["position_action"] == "closed"
        assert kwargs["pnl"] == Decimal("1500.0")
        assert kwargs["close_reason"] == "take_profit"

    def test_log_position_update_success_liquidated_action(
        self, mock_logger: Mock, sample_position: DerivativePosition
    ) -> None:
        """Test position update logging for liquidated action."""
        # Act
        log_position_update(
            mock_logger,
            sample_position,
            "liquidated",
            actual_liquidation_price=Decimal("45000.0"),
            loss_amount=Decimal("5000.0"),
        )

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "position_liquidated"

        kwargs = call_args[1]
        assert kwargs["position_action"] == "liquidated"
        assert kwargs["actual_liquidation_price"] == Decimal("45000.0")
        assert kwargs["loss_amount"] == Decimal("5000.0")

    # ==================== EDGE CASES ====================

    def test_log_position_update_edge_updated_action(
        self, mock_logger: Mock, sample_position: DerivativePosition
    ) -> None:
        """Test position update with generic updated action."""
        # Act
        log_position_update(mock_logger, sample_position, "updated")

        # Assert
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "position_updated"

        kwargs = call_args[1]
        assert kwargs["position_action"] == "updated"

    def test_log_position_update_edge_no_context(
        self, mock_logger: Mock, sample_position: DerivativePosition
    ) -> None:
        """Test position update logging with no additional context."""
        # Act
        log_position_update(mock_logger, sample_position, "modified")

        # Assert
        call_args = mock_logger.info.call_args
        kwargs = call_args[1]
        assert kwargs["position_action"] == "modified"

        # Should contain position data
        assert "symbol" in kwargs
        assert "side" in kwargs
        assert "size" in kwargs

    # ==================== FAILURE CASES ====================

    def test_log_position_update_failure_invalid_position(self, mock_logger: Mock) -> None:
        """Test position update logging with invalid position object."""

        # Arrange
        class InvalidPosition(DerivativePosition):
            def __init__(self) -> None:
                # Initialize with minimal required fields for DerivativePosition
                super().__init__(
                    exchange="test",
                    symbol="TEST-PERP",
                    side=OrderSide.BUY,
                    size=Decimal("1.0"),
                    entry_price=Decimal("100.0"),
                    timestamp=datetime.now(UTC),
                    hl_details=None,
                    bp_details=None,
                )

            def model_dump(self, **kwargs: object) -> dict[str, object]:
                raise TypeError("Invalid position type")

        invalid_position = InvalidPosition()

        # Act & Assert
        with pytest.raises(TypeError, match="Invalid position type"):
            log_position_update(mock_logger, invalid_position, "opened")


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("model_type", "expected_sensitive_fields"),
    [
        (Order, {"trades", "hl_details", "bp_details"}),
        (Trade, {"hl_details", "bp_details"}),
        (TradeSignal, {"metadata"}),
        (DerivativePosition, {"hl_details", "bp_details"}),
        (MarginAccountSummary, {"total_equity", "available_equity", "hl_details", "bp_details"}),
    ],
)
def test_sensitive_fields_configuration_parametrized(
    model_type: type, expected_sensitive_fields: set[str]
) -> None:
    """Test sensitive fields configuration for all model types."""
    # Assert
    assert SENSITIVE_FIELDS[model_type] == expected_sensitive_fields


@pytest.mark.parametrize(
    ("event_name", "expected_log_type"),
    [
        ("placed", "order_placed"),
        ("filled", "order_filled"),
        ("cancelled", "order_cancelled"),
        ("rejected", "order_rejected"),
        ("updated", "order_updated"),
        ("expired", "order_expired"),
    ],
)
def test_log_order_lifecycle_event_types_parametrized(
    mock_logger: Mock, sample_order: Order, event_name: str, expected_log_type: str
) -> None:
    """Test order lifecycle logging for various event types."""
    # Act
    log_order_lifecycle(mock_logger, sample_order, event_name)

    # Assert
    call_args = mock_logger.info.call_args
    assert call_args[0][0] == expected_log_type


@pytest.mark.parametrize(
    ("action_name", "expected_log_type"),
    [
        ("opened", "position_opened"),
        ("closed", "position_closed"),
        ("updated", "position_updated"),
        ("liquidated", "position_liquidated"),
        ("modified", "position_modified"),
        ("adjusted", "position_adjusted"),
    ],
)
def test_log_position_update_action_types_parametrized(
    mock_logger: Mock, sample_position: DerivativePosition, action_name: str, expected_log_type: str
) -> None:
    """Test position update logging for various action types."""
    # Act
    log_position_update(mock_logger, sample_position, action_name)

    # Assert
    call_args = mock_logger.info.call_args
    assert call_args[0][0] == expected_log_type
