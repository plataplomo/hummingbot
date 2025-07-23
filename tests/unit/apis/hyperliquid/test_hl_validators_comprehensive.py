"""Comprehensive unit tests for HyperliquidValidators.

Tests validator functionality for Hyperliquid-specific channel validation,
focusing only on public API behavior with comprehensive success, edge, and failure cases.
"""

import pytest

from cyberdelta.apis.common.base_types import InvalidChannelError
from cyberdelta.apis.hyperliquid.hl_validators import HyperliquidValidators


class TestHyperliquidValidators:
    """Test HyperliquidValidators public API functionality."""

    @pytest.mark.parametrize(
        "valid_channel",
        [
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "notification",
            "webData2",
            "subscriptionResponse",
            "fills",
            "orders",
            "candle",
        ],
    )
    def test_validate_hyperliquid_channel_success(self, valid_channel: str) -> None:
        """Test successful validation of all valid Hyperliquid channels."""
        result = HyperliquidValidators.validate_hyperliquid_channel(valid_channel)

        assert result == valid_channel
        assert isinstance(result, str)

    @pytest.mark.parametrize(
        "invalid_channel",
        [
            "invalid_channel",
            "l2book",  # wrong case
            "TRADES",  # wrong case
            "user_events",  # wrong format
            "",  # empty string
            " ",  # whitespace
            "l2Book ",  # trailing space
            " l2Book",  # leading space
            "unknown",
            "ticker",  # might exist on other exchanges
            "orderbook",  # different naming
            "user-events",  # different separator
            "userEvents2",  # modified valid channel
            "123",  # numeric
            "null",  # string null
            "None",  # string None
        ],
    )
    def test_validate_hyperliquid_channel_failure(self, invalid_channel: str) -> None:
        """Test validation failures for invalid channels."""
        with pytest.raises(InvalidChannelError) as exc_info:
            HyperliquidValidators.validate_hyperliquid_channel(invalid_channel)

        # Verify the exception contains the invalid channel
        error = exc_info.value
        assert invalid_channel in str(error)

    def test_validate_hyperliquid_channel_error_details(self) -> None:
        """Test that InvalidChannelError provides detailed error information."""
        invalid_channel = "invalid_test_channel"

        with pytest.raises(InvalidChannelError) as exc_info:
            HyperliquidValidators.validate_hyperliquid_channel(invalid_channel)

        error = exc_info.value
        error_str = str(error)

        # Verify error message contains useful information
        assert invalid_channel in error_str
        assert "l2Book" in error_str  # Should show valid channels
        assert "trades" in error_str
        assert "userEvents" in error_str

    def test_validate_hyperliquid_channel_case_sensitivity(self) -> None:
        """Test that channel validation is case-sensitive."""
        # Valid channel
        valid_result = HyperliquidValidators.validate_hyperliquid_channel("l2Book")
        assert valid_result == "l2Book"

        # Invalid case variations should fail
        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel("l2book")

        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel("L2BOOK")

        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel("L2Book")

    def test_validate_hyperliquid_channel_whitespace_handling(self) -> None:
        """Test that whitespace is not trimmed and causes validation failure."""
        # Channels with whitespace should fail
        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel(" l2Book")

        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel("l2Book ")

        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel(" l2Book ")

    def test_validate_hyperliquid_channel_static_method(self) -> None:
        """Test that validate_hyperliquid_channel is a static method."""
        # Can be called without instance
        result = HyperliquidValidators.validate_hyperliquid_channel("trades")
        assert result == "trades"

        # Can also be called with instance
        validator = HyperliquidValidators()
        result = validator.validate_hyperliquid_channel("trades")
        assert result == "trades"

    def test_hyperliquid_validators_instantiation(self) -> None:
        """Test that HyperliquidValidators can be instantiated."""
        validator = HyperliquidValidators()
        assert validator is not None
        assert isinstance(validator, HyperliquidValidators)

    @pytest.mark.parametrize(
        ("channel_type", "expected_channels"),
        [
            ("market_data", ["l2Book", "trades", "allMids", "candle"]),
            ("user_data", ["userEvents", "fills", "orders"]),
            ("system", ["notification", "webData2", "subscriptionResponse"]),
        ],
    )
    def test_validate_channel_categories(
        self, channel_type: str, expected_channels: list[str]
    ) -> None:
        """Test validation works for different categories of channels."""
        for channel in expected_channels:
            result = HyperliquidValidators.validate_hyperliquid_channel(channel)
            assert result == channel

    def test_comprehensive_channel_validation_edge_cases(self) -> None:
        """Test comprehensive edge cases for channel validation."""
        # Test all valid channels work
        valid_channels = {
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "notification",
            "webData2",
            "subscriptionResponse",
            "fills",
            "orders",
            "candle",
        }

        for channel in valid_channels:
            result = HyperliquidValidators.validate_hyperliquid_channel(channel)
            assert result == channel

        # Test that the set of valid channels is exactly what we expect
        assert len(valid_channels) == 10  # Update if channels are added

        # Test that similar but invalid channels fail
        similar_invalid = [
            "l2book",
            "l2Book2",
            "tradesData",
            "userEvent",
            "allMid",
            "notifications",
            "webData",
            "response",
            "fill",
            "order",
            "candles",
        ]

        for invalid_channel in similar_invalid:
            with pytest.raises(InvalidChannelError):
                HyperliquidValidators.validate_hyperliquid_channel(invalid_channel)

    def test_validator_error_message_quality(self) -> None:
        """Test that error messages are helpful for debugging."""
        with pytest.raises(InvalidChannelError) as exc_info:
            HyperliquidValidators.validate_hyperliquid_channel("invalid")

        error_message = str(exc_info.value)

        # Error should be descriptive
        assert "invalid" in error_message
        assert len(error_message) > 20  # Should be descriptive

        # Should contain at least some valid channels for reference
        valid_channels_in_message = [
            channel
            for channel in ["l2Book", "trades", "userEvents", "allMids"]
            if channel in error_message
        ]
        assert len(valid_channels_in_message) > 0  # At least one valid channel shown
