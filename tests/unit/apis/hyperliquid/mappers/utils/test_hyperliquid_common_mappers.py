"""Tests for HyperliquidCommonMappers utility class.

This module tests all utility methods in HyperliquidCommonMappers to ensure
they follow the Backpack pattern and provide consistent behavior.
"""

from datetime import UTC
from decimal import Decimal
from typing import Any, cast

import pytest

from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
    NonPositiveValueError,
    NumberValidationPolicy,
)


class TestHyperliquidCommonMappers:
    """Test HyperliquidCommonMappers utility methods."""

    def test_parse_decimal_safely_valid_inputs(self) -> None:
        """Test parse_decimal_safely with valid inputs."""
        # String input
        assert HyperliquidCommonMappers.parse_decimal_safely("123.45") == Decimal("123.45")

        # Float input
        assert HyperliquidCommonMappers.parse_decimal_safely(123.45) == Decimal("123.45")

        # Integer input
        assert HyperliquidCommonMappers.parse_decimal_safely(100) == Decimal(100)

        # Decimal input (passthrough)
        assert HyperliquidCommonMappers.parse_decimal_safely(Decimal("99.99")) == Decimal("99.99")

        # None input with default
        assert HyperliquidCommonMappers.parse_decimal_safely(None) == Decimal(0)
        assert HyperliquidCommonMappers.parse_decimal_safely(None, Decimal(10)) == Decimal(10)

    def test_parse_decimal_safely_invalid_inputs(self) -> None:
        """Test parse_decimal_safely with invalid inputs."""
        # Invalid string
        assert HyperliquidCommonMappers.parse_decimal_safely("not_a_number") == Decimal(0)

        # Invalid type
        assert HyperliquidCommonMappers.parse_decimal_safely(cast(Any, [1, 2, 3])) == Decimal(0)

        # With custom default
        assert HyperliquidCommonMappers.parse_decimal_safely("invalid", Decimal(99)) == Decimal(99)

    def test_normalize_symbol(self) -> None:
        """Test symbol normalization."""
        # Basic normalization
        assert HyperliquidCommonMappers.normalize_symbol("btc") == "BTC"
        assert HyperliquidCommonMappers.normalize_symbol("ETH") == "ETH"
        assert HyperliquidCommonMappers.normalize_symbol(" btc ") == "BTC"

        # Perpetual normalization
        assert HyperliquidCommonMappers.normalize_symbol("BTC-PERP") == "BTC"
        assert HyperliquidCommonMappers.normalize_symbol("ETH-PERP") == "ETH"
        assert HyperliquidCommonMappers.normalize_symbol("USDC-PERP") == "USDC"

        # Empty string
        assert not HyperliquidCommonMappers.normalize_symbol("")

    def test_denormalize_symbol(self) -> None:
        """Test symbol denormalization."""
        # Currently just returns as-is
        assert HyperliquidCommonMappers.denormalize_symbol("BTC") == "BTC"
        assert not HyperliquidCommonMappers.denormalize_symbol("")

    def test_timestamp_ms_to_datetime(self) -> None:
        """Test timestamp conversion."""
        # Valid timestamp
        timestamp_ms = 1609459200000  # 2021-01-01 00:00:00 UTC
        dt = HyperliquidCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
        assert dt is not None
        assert dt.year == 2021
        assert dt.month == 1
        assert dt.day == 1
        assert dt.tzinfo == UTC

        # None input
        assert HyperliquidCommonMappers.timestamp_ms_to_datetime(None) is None

        # Invalid timestamp
        assert HyperliquidCommonMappers.timestamp_ms_to_datetime(1e20) is None

    def test_safe_get_nested_type_checking(self) -> None:
        """Test safe_get_nested handles different types correctly (tests internal type guards)."""
        # Test with non-dict values (implicitly tests _is_dict_with_key)
        # Using cast to test the method's handling of invalid types
        assert HyperliquidCommonMappers.safe_get_nested(cast(Any, "not_a_dict"), "key") is None
        assert HyperliquidCommonMappers.safe_get_nested(cast(Any, []), "key") is None
        assert HyperliquidCommonMappers.safe_get_nested(cast(Any, 123), "key") is None

        # Test with dict missing keys
        assert HyperliquidCommonMappers.safe_get_nested({"other": "value"}, "key") is None

        # Test nested traversal with mixed types
        data = {"level1": "not_a_dict"}
        assert HyperliquidCommonMappers.safe_get_nested(data, "level1", "level2") is None

    def test_ensure_list_type_checking(self) -> None:
        """Test ensure_list handles different types correctly (tests internal _is_list)."""
        # Test with list inputs (already lists)
        assert HyperliquidCommonMappers.ensure_list([1, 2, 3]) == [1, 2, 3]
        assert HyperliquidCommonMappers.ensure_list([]) == []

        # Test with non-list inputs (should be wrapped in list)
        assert HyperliquidCommonMappers.ensure_list("not_list") == ["not_list"]
        assert HyperliquidCommonMappers.ensure_list({"dict": True}) == [{"dict": True}]
        assert HyperliquidCommonMappers.ensure_list(42) == [42]

        # Test edge cases
        assert HyperliquidCommonMappers.ensure_list(None) is None
        assert HyperliquidCommonMappers.ensure_list(None, default=[1, 2]) == [1, 2]

    def test_safe_get_nested(self) -> None:
        """Test safe nested dictionary access."""
        data = {"level1": {"level2": {"level3": "value"}}}

        # Valid path
        assert (
            HyperliquidCommonMappers.safe_get_nested(data, "level1", "level2", "level3") == "value"
        )

        # Partial path
        result = HyperliquidCommonMappers.safe_get_nested(data, "level1", "level2")
        assert result == {"level3": "value"}

        # Invalid path
        assert HyperliquidCommonMappers.safe_get_nested(data, "level1", "invalid") is None
        assert HyperliquidCommonMappers.safe_get_nested(data, "invalid") is None

        # With default
        assert (
            HyperliquidCommonMappers.safe_get_nested(data, "invalid", default="default")
            == "default"
        )

    def test_ensure_list(self) -> None:
        """Test ensure_list utility."""
        # Already a list
        assert HyperliquidCommonMappers.ensure_list([1, 2, 3]) == [1, 2, 3]

        # Single value
        assert HyperliquidCommonMappers.ensure_list("value") == ["value"]
        assert HyperliquidCommonMappers.ensure_list(123) == [123]

        # None with default
        assert HyperliquidCommonMappers.ensure_list(None) is None
        assert HyperliquidCommonMappers.ensure_list(None, default=[]) == []
        assert HyperliquidCommonMappers.ensure_list(None, default=[1, 2]) == [1, 2]

    def test_validate_positive_decimal(self) -> None:
        """Test positive decimal validation."""
        # Valid positive values
        assert HyperliquidCommonMappers.validate_positive_decimal(Decimal(10), "field") == Decimal(
            10
        )
        assert HyperliquidCommonMappers.validate_positive_decimal(
            Decimal("0.01"), "field"
        ) == Decimal("0.01")

        # Zero with NON_NEGATIVE policy
        assert HyperliquidCommonMappers.validate_positive_decimal(
            Decimal(0), "field", NumberValidationPolicy.NON_NEGATIVE
        ) == Decimal(0)

        # Invalid values
        with pytest.raises(NonPositiveValueError) as exc_info:
            HyperliquidCommonMappers.validate_positive_decimal(Decimal(0), "field")
        assert exc_info.value.field_name == "field"

        with pytest.raises(NonPositiveValueError):
            HyperliquidCommonMappers.validate_positive_decimal(Decimal(-10), "field")

        with pytest.raises(NonPositiveValueError):
            HyperliquidCommonMappers.validate_positive_decimal(
                Decimal(-10), "field", NumberValidationPolicy.NON_NEGATIVE
            )

    def test_is_valid_symbol(self) -> None:
        """Test symbol validation."""
        # Valid symbols
        assert HyperliquidCommonMappers.is_valid_symbol("BTC") is True
        assert HyperliquidCommonMappers.is_valid_symbol("ETH-PERP") is True
        assert HyperliquidCommonMappers.is_valid_symbol("BTC_USDC") is True
        assert HyperliquidCommonMappers.is_valid_symbol("BTC/USDC") is True

        # Invalid symbols
        assert HyperliquidCommonMappers.is_valid_symbol("") is False
        assert HyperliquidCommonMappers.is_valid_symbol("A" * 25) is False  # Too long
        assert HyperliquidCommonMappers.is_valid_symbol("BTC$USD") is False  # Invalid char
        assert HyperliquidCommonMappers.is_valid_symbol(cast(Any, None)) is False

    def test_calculate_percentage(self) -> None:
        """Test percentage calculation."""
        # Valid calculations
        assert HyperliquidCommonMappers.calculate_percentage(Decimal(25), Decimal(100)) == Decimal(
            "0.2500"
        )

        assert HyperliquidCommonMappers.calculate_percentage(
            Decimal(1), Decimal(3), decimal_places=2
        ) == Decimal("0.33")

        # Division by zero
        assert HyperliquidCommonMappers.calculate_percentage(Decimal(10), Decimal(0)) is None

    def test_format_order_id(self) -> None:
        """Test order ID formatting."""
        # Both IDs
        assert (
            HyperliquidCommonMappers.format_order_id("12345", "CLIENT123")
            == "12345 (client: CLIENT123)"
        )

        # Exchange ID only
        assert HyperliquidCommonMappers.format_order_id("12345") == "12345"

        # Client ID only
        assert (
            HyperliquidCommonMappers.format_order_id(client_order_id="CLIENT123")
            == "client: CLIENT123"
        )

        # Neither ID
        assert HyperliquidCommonMappers.format_order_id() == "unknown"

    def test_extract_base_quote_from_symbol(self) -> None:
        """Test base/quote extraction from symbols."""
        # Perpetual format
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("BTC-PERP") == (
            "BTC",
            "USDC",
        )

        # Slash format
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("BTC/USDC") == (
            "BTC",
            "USDC",
        )

        # Underscore format
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("ETH_USDT") == (
            "ETH",
            "USDT",
        )

        # Single symbol (assumes USDC quote)
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("BTC") == ("BTC", "USDC")

        # Invalid formats
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("") is None
        assert HyperliquidCommonMappers.extract_base_quote_from_symbol("INVALID/TOO/MANY") is None

    def test_create_internal_symbol(self) -> None:
        """Test internal symbol creation."""
        assert HyperliquidCommonMappers.create_internal_symbol("BTC", "USDC") == "BTC/USDC"
        assert HyperliquidCommonMappers.create_internal_symbol("eth", "usdt") == "ETH/USDT"

    def test_safe_divide(self) -> None:
        """Test safe division."""
        # Valid division
        assert HyperliquidCommonMappers.safe_divide(Decimal(10), Decimal(2)) == Decimal(5)

        # Division by zero
        assert HyperliquidCommonMappers.safe_divide(Decimal(10), Decimal(0)) == Decimal(0)

        # With custom default
        assert HyperliquidCommonMappers.safe_divide(
            Decimal(10), Decimal(0), Decimal(99)
        ) == Decimal(99)

    def test_round_to_tick_size(self) -> None:
        """Test tick size rounding."""
        # Normal rounding
        assert HyperliquidCommonMappers.round_to_tick_size(
            Decimal("123.456"), Decimal("0.01")
        ) == Decimal("123.46")

        assert HyperliquidCommonMappers.round_to_tick_size(
            Decimal("123.456"), Decimal("0.1")
        ) == Decimal("123.5")

        # Invalid tick size
        assert HyperliquidCommonMappers.round_to_tick_size(
            Decimal("123.456"), Decimal(0)
        ) == Decimal("123.456")

    def test_clamp_value(self) -> None:
        """Test value clamping."""
        # Within bounds
        assert HyperliquidCommonMappers.clamp_value(Decimal(5), Decimal(0), Decimal(10)) == Decimal(
            5
        )

        # Below minimum
        assert HyperliquidCommonMappers.clamp_value(
            Decimal(-5), Decimal(0), Decimal(10)
        ) == Decimal(0)

        # Above maximum
        assert HyperliquidCommonMappers.clamp_value(
            Decimal(15), Decimal(0), Decimal(10)
        ) == Decimal(10)

        # No bounds
        assert HyperliquidCommonMappers.clamp_value(Decimal(100)) == Decimal(100)

        # Only min bound
        assert HyperliquidCommonMappers.clamp_value(Decimal(5), min_value=Decimal(10)) == Decimal(
            10
        )

        # Only max bound
        assert HyperliquidCommonMappers.clamp_value(Decimal(15), max_value=Decimal(10)) == Decimal(
            10
        )

    def test_map_order_status(self) -> None:
        """Test order status mapping."""
        assert HyperliquidCommonMappers.map_order_status("open") == "OPEN"
        assert HyperliquidCommonMappers.map_order_status("filled") == "FILLED"
        assert HyperliquidCommonMappers.map_order_status("cancelled") == "CANCELLED"
        assert HyperliquidCommonMappers.map_order_status("partial") == "PARTIALLY_FILLED"
        assert HyperliquidCommonMappers.map_order_status("unknown") == "UNKNOWN"

    def test_map_order_side(self) -> None:
        """Test order side mapping."""
        # Hyperliquid A/B format
        assert HyperliquidCommonMappers.map_order_side("A") == "SELL"
        assert HyperliquidCommonMappers.map_order_side("B") == "BUY"

        # Standard format
        assert HyperliquidCommonMappers.map_order_side("buy") == "BUY"
        assert HyperliquidCommonMappers.map_order_side("sell") == "SELL"

        # Unknown
        assert HyperliquidCommonMappers.map_order_side("unknown") == "UNKNOWN"

    def test_map_order_type(self) -> None:
        """Test order type mapping."""
        assert HyperliquidCommonMappers.map_order_type("limit") == "LIMIT"
        assert HyperliquidCommonMappers.map_order_type("market") == "MARKET"
        assert HyperliquidCommonMappers.map_order_type("trigger") == "STOP"
        assert HyperliquidCommonMappers.map_order_type("triggered_tp") == "TAKE_PROFIT"
        assert HyperliquidCommonMappers.map_order_type("unknown") == "UNKNOWN"

    def test_parse_leverage(self) -> None:
        """Test leverage parsing."""
        # Valid leverage
        assert HyperliquidCommonMappers.parse_leverage(10) == Decimal(10)
        assert HyperliquidCommonMappers.parse_leverage("5.5") == Decimal("5.5")

        # Minimum leverage
        assert HyperliquidCommonMappers.parse_leverage(0) == Decimal(1)
        assert HyperliquidCommonMappers.parse_leverage(-5) == Decimal(1)

        # Invalid input
        assert HyperliquidCommonMappers.parse_leverage("invalid") == Decimal(1)
        assert HyperliquidCommonMappers.parse_leverage(None) == Decimal(1)
