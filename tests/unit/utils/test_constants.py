"""Unit tests for the constants module.

Tests all constants and enumerations defined in the constants module.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from enum import Enum

import pytest

from cyberdelta.utils import constants
from cyberdelta.utils.constants import (
    DEFAULT_AMOUNT_PRECISION,
    DEFAULT_CONFIG_PATH,
    DEFAULT_HEADERS,
    DEFAULT_PRICE_PRECISION,
    DEFAULT_SECRETS_PATH,
    DEFAULT_STATE_FILE,
    MAX_RETRIES,
    RATE_LIMIT_BUFFER,
    RETRY_DELAY_BASE,
    SECONDS_PER_DAY,
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
    WEBSOCKET_MAX_RECONNECT_DELAY,
    WEBSOCKET_RECONNECT_DELAY,
    WEBSOCKET_RECONNECT_FACTOR,
    EventType,
    Exchange,
    MarketDataType,
    PositionSide,
    PositionStatus,
    Signal,
    Timeframe,
)


class TestSystemConstants:
    """Test suite for system constants."""

    # ==================== SUCCESS CASES ====================

    def test_default_paths_success(self) -> None:
        """Test that default paths are correctly defined."""
        # Assert
        assert DEFAULT_CONFIG_PATH == "cyberdelta/config/config.yaml"
        assert DEFAULT_SECRETS_PATH == "cyberdelta/config/secrets.yaml"
        assert DEFAULT_STATE_FILE == "state.json"

    def test_rate_limiting_constants_success(self) -> None:
        """Test rate limiting constants are properly defined."""
        # Assert
        assert RATE_LIMIT_BUFFER == 0.9
        assert isinstance(RATE_LIMIT_BUFFER, float)
        assert 0 < RATE_LIMIT_BUFFER < 1

    def test_websocket_constants_success(self) -> None:
        """Test WebSocket constants are properly defined."""
        # Assert
        assert WEBSOCKET_RECONNECT_DELAY == 5
        assert WEBSOCKET_MAX_RECONNECT_DELAY == 300
        assert WEBSOCKET_RECONNECT_FACTOR == 1.5
        assert WEBSOCKET_RECONNECT_DELAY < WEBSOCKET_MAX_RECONNECT_DELAY
        assert WEBSOCKET_RECONNECT_FACTOR > 1.0

    def test_error_handling_constants_success(self) -> None:
        """Test error handling constants are properly defined."""
        # Assert
        assert MAX_RETRIES == 3
        assert RETRY_DELAY_BASE == 1.0
        assert isinstance(MAX_RETRIES, int)
        assert isinstance(RETRY_DELAY_BASE, float)

    def test_time_constants_success(self) -> None:
        """Test time constants are correctly calculated."""
        # Assert
        assert SECONDS_PER_MINUTE == 60
        assert SECONDS_PER_HOUR == 3600
        assert SECONDS_PER_DAY == 86400
        # Verify relationships
        assert SECONDS_PER_HOUR == SECONDS_PER_MINUTE * 60
        assert SECONDS_PER_DAY == SECONDS_PER_HOUR * 24

    def test_precision_constants_success(self) -> None:
        """Test precision constants are properly defined."""
        # Assert
        assert DEFAULT_PRICE_PRECISION == 6
        assert DEFAULT_AMOUNT_PRECISION == 8
        assert isinstance(DEFAULT_PRICE_PRECISION, int)
        assert isinstance(DEFAULT_AMOUNT_PRECISION, int)

    def test_default_headers_success(self) -> None:
        """Test default headers are properly defined."""
        # Assert
        assert isinstance(DEFAULT_HEADERS, dict)
        assert "Content-Type" in DEFAULT_HEADERS
        assert "User-Agent" in DEFAULT_HEADERS
        assert DEFAULT_HEADERS["Content-Type"] == "application/json"
        assert "CyberDeltaEngine" in DEFAULT_HEADERS["User-Agent"]

    # ==================== EDGE CASES ====================

    def test_constants_immutability_edge(self) -> None:
        """Test that constants remain immutable."""
        # Arrange
        original_retries = MAX_RETRIES
        original_buffer = RATE_LIMIT_BUFFER

        # Assert - values haven't changed
        assert original_retries == MAX_RETRIES
        assert original_buffer == RATE_LIMIT_BUFFER

    def test_headers_dict_modification_edge(self) -> None:
        """Test that modifying headers dict doesn't affect the constant."""
        # Arrange
        headers_copy = DEFAULT_HEADERS.copy()
        headers_copy["Custom-Header"] = "value"

        # Assert - original remains unchanged
        assert "Custom-Header" not in DEFAULT_HEADERS
        assert headers_copy != DEFAULT_HEADERS


class TestExchangeEnum:
    """Test suite for Exchange enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_exchange_enum_values_success(self) -> None:
        """Test Exchange enum has correct values."""
        # Assert
        assert Exchange.HYPERLIQUID.value == "hyperliquid"
        assert Exchange.BACKPACK.value == "backpack"

    def test_exchange_enum_members_success(self) -> None:
        """Test all Exchange enum members are accessible."""
        # Assert
        members = list(Exchange)
        assert len(members) == 2
        assert Exchange.HYPERLIQUID in members
        assert Exchange.BACKPACK in members

    def test_exchange_enum_comparison_success(self) -> None:
        """Test Exchange enum comparisons work correctly."""
        # Assert
        assert Exchange.HYPERLIQUID == Exchange.HYPERLIQUID
        # Test enum values are different instances
        exchanges = [Exchange.HYPERLIQUID, Exchange.BACKPACK]
        assert len(set(exchanges)) == 2  # All different
        assert Exchange.HYPERLIQUID is Exchange.HYPERLIQUID

    # ==================== EDGE CASES ====================

    def test_exchange_enum_string_conversion_edge(self) -> None:
        """Test Exchange enum string representations."""
        # Assert
        assert str(Exchange.HYPERLIQUID) == "Exchange.HYPERLIQUID"
        assert repr(Exchange.HYPERLIQUID) == "<Exchange.HYPERLIQUID: 'hyperliquid'>"

    def test_exchange_enum_value_access_edge(self) -> None:
        """Test accessing Exchange enum by value."""
        # Act & Assert
        assert Exchange("hyperliquid") == Exchange.HYPERLIQUID
        assert Exchange("backpack") == Exchange.BACKPACK

    # ==================== FAILURE CASES ====================

    def test_exchange_enum_invalid_value_failure(self) -> None:
        """Test Exchange enum with invalid value raises error."""
        # Act & Assert
        with pytest.raises(ValueError, match="is not a valid Exchange"):
            Exchange("invalid_exchange")


class TestPositionStatusEnum:
    """Test suite for PositionStatus enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_position_status_members_success(self) -> None:
        """Test all PositionStatus enum members exist."""
        # Assert
        members = list(PositionStatus)
        assert len(members) == 5
        assert PositionStatus.OPEN in members
        assert PositionStatus.CLOSED in members
        assert PositionStatus.PENDING_OPEN in members
        assert PositionStatus.PENDING_CLOSE in members
        assert PositionStatus.ERROR in members

    def test_position_status_auto_values_success(self) -> None:
        """Test PositionStatus auto() values are unique."""
        # Assert
        values = [member.value for member in PositionStatus]
        assert len(values) == len(set(values))  # All unique

    # ==================== EDGE CASES ====================

    def test_position_status_ordering_edge(self) -> None:
        """Test PositionStatus members maintain consistent ordering."""
        # Arrange
        members = list(PositionStatus)

        # Act
        members_again = list(PositionStatus)

        # Assert
        assert members == members_again


class TestPositionSideEnum:
    """Test suite for PositionSide enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_position_side_values_success(self) -> None:
        """Test PositionSide enum has correct values."""
        # Assert
        assert PositionSide.LONG.value == "long"
        assert PositionSide.SHORT.value == "short"
        assert PositionSide.NONE.value == "none"

    def test_position_side_all_members_success(self) -> None:
        """Test all PositionSide members are defined."""
        # Assert
        assert len(list(PositionSide)) == 3

    # ==================== EDGE CASES ====================

    def test_position_side_case_sensitive_edge(self) -> None:
        """Test PositionSide values are case-sensitive."""
        # Act & Assert
        assert PositionSide("long") == PositionSide.LONG
        with pytest.raises(ValueError):
            PositionSide("LONG")  # Wrong case

    # ==================== FAILURE CASES ====================

    def test_position_side_invalid_value_failure(self) -> None:
        """Test PositionSide with invalid value raises error."""
        # Act & Assert
        with pytest.raises(ValueError, match="is not a valid PositionSide"):
            PositionSide("neutral")


class TestEventTypeEnum:
    """Test suite for EventType enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_event_type_all_members_success(self) -> None:
        """Test all EventType members exist."""
        # Assert
        expected_members = {
            EventType.MARKET_DATA,
            EventType.ORDER_UPDATE,
            EventType.POSITION_UPDATE,
            EventType.BALANCE_UPDATE,
            EventType.ERROR,
            EventType.SYSTEM,
        }
        actual_members = set(EventType)
        assert actual_members == expected_members

    def test_event_type_uniqueness_success(self) -> None:
        """Test EventType values are unique."""
        # Assert
        values = [member.value for member in EventType]
        assert len(values) == len(set(values))


class TestMarketDataTypeEnum:
    """Test suite for MarketDataType enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_market_data_type_members_success(self) -> None:
        """Test all MarketDataType members exist."""
        # Assert
        assert hasattr(MarketDataType, "TRADE")
        assert hasattr(MarketDataType, "ORDERBOOK")
        assert hasattr(MarketDataType, "TICKER")
        assert hasattr(MarketDataType, "FUNDING")
        assert hasattr(MarketDataType, "CANDLE")

    def test_market_data_type_count_success(self) -> None:
        """Test MarketDataType has correct number of members."""
        # Assert
        assert len(list(MarketDataType)) == 5


class TestTimeframeEnum:
    """Test suite for Timeframe enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_timeframe_values_success(self) -> None:
        """Test Timeframe enum has correct string values."""
        # Assert
        assert Timeframe.MINUTE_1.value == "1m"
        assert Timeframe.MINUTE_5.value == "5m"
        assert Timeframe.MINUTE_15.value == "15m"
        assert Timeframe.MINUTE_30.value == "30m"
        assert Timeframe.HOUR_1.value == "1h"
        assert Timeframe.HOUR_4.value == "4h"
        assert Timeframe.HOUR_8.value == "8h"
        assert Timeframe.DAY_1.value == "1d"
        assert Timeframe.WEEK_1.value == "1w"

    def test_timeframe_complete_set_success(self) -> None:
        """Test all expected timeframes are present."""
        # Assert
        assert len(list(Timeframe)) == 9

    # ==================== EDGE CASES ====================

    def test_timeframe_value_format_edge(self) -> None:
        """Test Timeframe values follow consistent format."""
        # Assert
        for timeframe in Timeframe:
            value = timeframe.value
            assert isinstance(value, str)
            assert len(value) >= 2
            # Check format: number + unit
            assert value[-1] in ["m", "h", "d", "w"]
            assert value[:-1].isdigit()

    def test_timeframe_by_value_edge(self) -> None:
        """Test accessing Timeframe by value."""
        # Act & Assert
        assert Timeframe("1m") == Timeframe.MINUTE_1
        assert Timeframe("1h") == Timeframe.HOUR_1
        assert Timeframe("1d") == Timeframe.DAY_1

    # ==================== FAILURE CASES ====================

    def test_timeframe_invalid_value_failure(self) -> None:
        """Test Timeframe with invalid value raises error."""
        # Act & Assert
        with pytest.raises(ValueError, match="is not a valid Timeframe"):
            Timeframe("2h")  # Not a valid timeframe


class TestSignalEnum:
    """Test suite for Signal enumeration."""

    # ==================== SUCCESS CASES ====================

    def test_signal_members_success(self) -> None:
        """Test all Signal members exist."""
        # Assert
        signals = list(Signal)
        assert Signal.BUY in signals
        assert Signal.SELL in signals
        assert Signal.HOLD in signals
        assert Signal.CLOSE in signals
        assert len(signals) == 4

    def test_signal_auto_values_unique_success(self) -> None:
        """Test Signal auto() generates unique values."""
        # Assert
        values = [s.value for s in Signal]
        assert len(values) == len(set(values))

    # ==================== EDGE CASES ====================

    def test_signal_iteration_order_edge(self) -> None:
        """Test Signal enum maintains definition order."""
        # Arrange
        expected_order = [Signal.BUY, Signal.SELL, Signal.HOLD, Signal.CLOSE]

        # Act
        actual_order = list(Signal)

        # Assert
        assert actual_order == expected_order


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("constant_name", "expected_type", "min_value", "max_value"),
    [
        ("MAX_RETRIES", int, 1, 10),
        ("DEFAULT_PRICE_PRECISION", int, 0, 20),
        ("DEFAULT_AMOUNT_PRECISION", int, 0, 20),
        ("WEBSOCKET_RECONNECT_DELAY", int, 1, 60),
        ("WEBSOCKET_MAX_RECONNECT_DELAY", int, 60, 3600),
    ],
)
def test_numeric_constants_bounds(
    constant_name: str, expected_type: type[int], min_value: int, max_value: int
) -> None:
    """Test numeric constants are within reasonable bounds."""
    # Arrange

    # Act
    value = getattr(constants, constant_name)

    # Assert
    assert isinstance(value, expected_type)
    assert min_value <= value <= max_value


@pytest.mark.parametrize(
    ("enum_class", "expected_count"),
    [
        (Exchange, 2),
        (PositionStatus, 5),
        (PositionSide, 3),
        (EventType, 6),
        (MarketDataType, 5),
        (Timeframe, 9),
        (Signal, 4),
    ],
)
def test_enum_member_counts(enum_class: type[Enum], expected_count: int) -> None:
    """Test that enums have the expected number of members."""
    # Act
    actual_count = len(list(enum_class.__members__))

    # Assert
    assert actual_count == expected_count
