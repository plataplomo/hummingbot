"""Unit tests for WebSocket payload validators."""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.base.ws_validators import (
    ExchangeSpecificValidators,
    WebSocketPayloadValidators,
)


class TestWebSocketPayloadValidators:
    """Test WebSocketPayloadValidators functionality."""

    def test_validate_dict_payload_success(self) -> None:
        """Test successful dict payload validation."""
        payload = {"key1": "value1", "key2": "value2"}
        result = WebSocketPayloadValidators.validate_dict_payload(payload)
        assert result == payload

    def test_validate_dict_payload_with_constraints(self) -> None:
        """Test dict payload validation with size constraints."""
        payload = {"key1": "value1", "key2": "value2"}

        # Test min_keys constraint
        result = WebSocketPayloadValidators.validate_dict_payload(payload, min_keys=2)
        assert result == payload

        # Test max_keys constraint
        result = WebSocketPayloadValidators.validate_dict_payload(payload, max_keys=5)
        assert result == payload

    def test_validate_dict_payload_failures(self) -> None:
        """Test dict payload validation failures."""
        # Wrong type
        with pytest.raises(TypeError, match=r"Invalid message payload type: list\. Expected dict"):
            WebSocketPayloadValidators.validate_dict_payload([1, 2, 3])

        # Too few keys
        with pytest.raises(ValueError, match="message payload must have at least 3 keys, got 1"):
            WebSocketPayloadValidators.validate_dict_payload({"a": 1}, min_keys=3)

        # Too many keys
        with pytest.raises(ValueError, match="message payload must have at most 1 keys, got 2"):
            WebSocketPayloadValidators.validate_dict_payload({"a": 1, "b": 2}, max_keys=1)

    def test_validate_list_payload_success(self) -> None:
        """Test successful list payload validation."""
        payload = [1, 2, 3, 4]
        result = WebSocketPayloadValidators.validate_list_payload(payload)
        assert result == payload

    def test_validate_list_payload_with_constraints(self) -> None:
        """Test list payload validation with constraints."""
        payload = ["a", "b", "c"]

        # Test length constraints
        result = WebSocketPayloadValidators.validate_list_payload(
            payload, min_length=2, max_length=5
        )
        assert result == payload

        # Test item type constraint
        result = WebSocketPayloadValidators.validate_list_payload(payload, item_type=str)
        assert result == payload

    def test_validate_list_payload_failures(self) -> None:
        """Test list payload validation failures."""
        # Wrong type
        with pytest.raises(TypeError, match=r"Invalid message payload type: dict\. Expected list"):
            WebSocketPayloadValidators.validate_list_payload({"a": 1})

        # Too short
        with pytest.raises(ValueError, match="message payload must have at least 5 items, got 2"):
            WebSocketPayloadValidators.validate_list_payload([1, 2], min_length=5)

        # Too long
        with pytest.raises(ValueError, match="message payload must have at most 2 items, got 3"):
            WebSocketPayloadValidators.validate_list_payload([1, 2, 3], max_length=2)

        # Wrong item type
        with pytest.raises(
            TypeError, match=r"message payload item 1 has invalid type: str\. Expected int"
        ):
            WebSocketPayloadValidators.validate_list_payload([1, "2", 3], item_type=int)

    def test_validate_required_fields_success(self) -> None:
        """Test successful required fields validation."""
        payload: dict[str, Any] = {"name": "test", "value": 42, "data": {}}
        required = ["name", "value"]

        result = WebSocketPayloadValidators.validate_required_fields(payload, required)
        assert result == payload

    def test_validate_required_fields_failure(self) -> None:
        """Test required fields validation failure."""
        payload = {"name": "test"}
        required = ["name", "value", "data"]

        with pytest.raises(ValueError, match=r"Missing required fields.*value.*data"):
            WebSocketPayloadValidators.validate_required_fields(payload, required)

    def test_validate_optional_fields_success(self) -> None:
        """Test successful optional fields validation."""
        payload = {"name": "test", "value": 42}
        allowed = ["name", "value", "data", "extra"]

        result = WebSocketPayloadValidators.validate_optional_fields(payload, allowed)
        assert result == payload

    def test_validate_optional_fields_failure(self) -> None:
        """Test optional fields validation failure."""
        payload = {"name": "test", "forbidden": "value"}
        allowed = ["name", "value"]

        with pytest.raises(ValueError, match=r"Unexpected fields.*forbidden"):
            WebSocketPayloadValidators.validate_optional_fields(payload, allowed)

    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("BTC_USDC", "BTC_USDC"),
            ("SOL-PERP", "SOL-PERP"),
            ("ETH_USD", "ETH_USD"),
            ("AVAX123", "AVAX123"),
        ],
    )
    def test_validate_symbol_success(self, symbol: str, expected: str) -> None:
        """Test successful symbol validation."""
        result = WebSocketPayloadValidators.validate_symbol(symbol)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_symbol",
        [
            123,  # Wrong type
            "",  # Empty
            "btc_usdc",  # Lowercase
            "BTC USDC",  # Space
            "A" * 25,  # Too long
            "BTC@USDC",  # Invalid character
        ],
    )
    def test_validate_symbol_failure(self, invalid_symbol: str) -> None:
        """Test symbol validation failures."""
        with pytest.raises((TypeError, ValueError)):
            WebSocketPayloadValidators.validate_symbol(invalid_symbol)

    @pytest.mark.parametrize(
        ("topic", "expected"),
        [
            ("depth", "depth"),
            ("ticker.BTC", "ticker.BTC"),
            ("trades_v2", "trades_v2"),
            ("user-events", "user-events"),
        ],
    )
    def test_validate_topic_success(self, topic: str, expected: str) -> None:
        """Test successful topic validation."""
        result = WebSocketPayloadValidators.validate_topic(topic)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_topic",
        [
            123,  # Wrong type
            "",  # Empty
            "topic with spaces",  # Spaces
            "A" * 60,  # Too long
            "topic@invalid",  # Invalid character
        ],
    )
    def test_validate_topic_failure(self, invalid_topic: str) -> None:
        """Test topic validation failures."""
        with pytest.raises((TypeError, ValueError)):
            WebSocketPayloadValidators.validate_topic(invalid_topic)

    @pytest.mark.parametrize(
        ("numeric_string", "expected"),
        [
            ("123.45", "123.45"),
            ("0.001", "0.001"),
            ("1000000", "1000000"),
            ("-50.5", "-50.5"),
        ],
    )
    def test_validate_numeric_string_success(self, numeric_string: str, expected: str) -> None:
        """Test successful numeric string validation."""
        result = WebSocketPayloadValidators.validate_numeric_string(numeric_string)
        assert result == expected

    def test_validate_numeric_string_with_range(self) -> None:
        """Test numeric string validation with range constraints."""
        # Valid range
        result = WebSocketPayloadValidators.validate_numeric_string(
            "50.5", min_value=0, max_value=100
        )
        assert result == "50.5"

        # Below minimum
        with pytest.raises(ValueError, match=r"Invalid numeric value: -10\.0 must be at least 0"):
            WebSocketPayloadValidators.validate_numeric_string("-10", min_value=0)

        # Above maximum
        with pytest.raises(ValueError, match=r"Invalid numeric value: 150\.0 must be at most 100"):
            WebSocketPayloadValidators.validate_numeric_string("150", max_value=100)

    @pytest.mark.parametrize(
        "invalid_numeric",
        [
            123,  # Wrong type
            "not-a-number",  # Invalid format
            "123.45.67",  # Multiple decimals
            "",  # Empty
        ],
    )
    def test_validate_numeric_string_failure(self, invalid_numeric: str) -> None:
        """Test numeric string validation failures."""
        with pytest.raises((TypeError, ValueError)):
            WebSocketPayloadValidators.validate_numeric_string(invalid_numeric)

    @pytest.mark.parametrize(
        ("timestamp", "expected"),
        [
            (1640995200, 1640995200),  # Valid timestamp
            (946684800, 946684800),  # Year 2000
            (2000000000, 2000000000),  # Future timestamp
        ],
    )
    def test_validate_timestamp_success(self, timestamp: int, expected: int) -> None:
        """Test successful timestamp validation."""
        result = WebSocketPayloadValidators.validate_timestamp(timestamp)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_timestamp",
        [
            "1640995200",  # Wrong type (string)
            -1,  # Negative
            100,  # Too old (before year 2000)
        ],
    )
    def test_validate_timestamp_failure(self, invalid_timestamp: int) -> None:
        """Test timestamp validation failures."""
        with pytest.raises((TypeError, ValueError)):
            WebSocketPayloadValidators.validate_timestamp(invalid_timestamp)


class TestExchangeSpecificValidators:
    """Test ExchangeSpecificValidators functionality."""

    @pytest.mark.parametrize(
        ("topic", "expected_type", "expected_symbol"),
        [
            ("depth.BTC_USDC", "depth", "BTC_USDC"),
            ("ticker.SOL-PERP", "ticker", "SOL-PERP"),
            ("trades.ETH_USD", "trades", "ETH_USD"),
        ],
    )
    def test_validate_backpack_topic_success(
        self, topic: str, expected_type: str, expected_symbol: str
    ) -> None:
        """Test successful Backpack topic validation."""
        topic_type, symbol = ExchangeSpecificValidators.validate_backpack_topic(topic)
        assert topic_type == expected_type
        assert symbol == expected_symbol

    @pytest.mark.parametrize(
        "invalid_topic",
        [
            "depth",  # Missing symbol
            "depth.btc_usdc",  # Invalid symbol (lowercase)
            "invalid_type.BTC_USDC",  # Invalid topic type
            "depth.BTC_USDC.extra",  # Too many parts
            "",  # Empty
        ],
    )
    def test_validate_backpack_topic_failure(self, invalid_topic: str) -> None:
        """Test Backpack topic validation failures."""
        with pytest.raises(ValueError):
            ExchangeSpecificValidators.validate_backpack_topic(invalid_topic)

    @pytest.mark.parametrize(
        ("channel", "expected"),
        [
            ("l2Book", "l2Book"),
            ("trades", "trades"),
            ("userEvents", "userEvents"),
            ("allMids", "allMids"),
        ],
    )
    def test_validate_hyperliquid_channel_success(self, channel: str, expected: str) -> None:
        """Test successful Hyperliquid channel validation."""
        result = ExchangeSpecificValidators.validate_hyperliquid_channel(channel)
        assert result == expected

    def test_validate_hyperliquid_channel_unknown(self) -> None:
        """Test Hyperliquid channel validation with unknown channel."""
        # Should not raise exception, just log warning
        result = ExchangeSpecificValidators.validate_hyperliquid_channel("unknown_channel")
        assert result == "unknown_channel"

    @pytest.mark.parametrize(
        ("level", "expected"),
        [
            (["100.5", "50.25"], ["100.5", "50.25"]),
            (["0.001", "1000.0"], ["0.001", "1000.0"]),
        ],
    )
    def test_validate_order_book_level_success(self, level: list[str], expected: list[str]) -> None:
        """Test successful order book level validation."""
        result = ExchangeSpecificValidators.validate_order_book_level(level)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_level",
        [
            ["100.5"],  # Too short
            ["100.5", "50.25", "extra"],  # Too long
            [100.5, 50.25],  # Wrong type (not strings)
            ["invalid", "50.25"],  # Invalid price
            ["100.5", "invalid"],  # Invalid quantity
            ["-10.5", "50.25"],  # Negative price
            ["100.5", "-10.0"],  # Negative quantity
        ],
    )
    def test_validate_order_book_level_failure(self, invalid_level: list[str]) -> None:
        """Test order book level validation failures."""
        with pytest.raises((TypeError, ValueError)):
            ExchangeSpecificValidators.validate_order_book_level(invalid_level)
