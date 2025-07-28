"""Unit tests for the MidPrices model.

Tests mid price management functionality including symbol lookup,
validation, and utility methods.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.mid_prices import MidPrices


class TestMidPricesInitialization:
    """Test suite for MidPrices model initialization."""

    # ==================== SUCCESS CASES ====================

    def test_mid_prices_init_success_with_required_fields(self) -> None:
        """Test successful initialization with required fields."""
        # Arrange
        prices = {
            "BTC-PERP": Decimal("50000.0"),
            "ETH-PERP": Decimal("3000.0"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="hyperliquid",
        )

        # Assert
        assert mid_prices.prices == prices
        assert mid_prices.exchange == "hyperliquid"
        assert mid_prices.timestamp is None  # Default value

    def test_mid_prices_init_success_with_timestamp(self) -> None:
        """Test successful initialization with timestamp."""
        # Arrange
        prices = {"SOL-PERP": Decimal("100.0")}
        timestamp = datetime.now(UTC)

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="backpack",
            timestamp=timestamp,
        )

        # Assert
        assert mid_prices.prices == prices
        assert mid_prices.exchange == "backpack"
        assert mid_prices.timestamp == timestamp

    def test_mid_prices_init_success_empty_prices(self) -> None:
        """Test successful initialization with empty prices dict."""
        # Act
        mid_prices = MidPrices(
            prices={},
            exchange="test_exchange",
        )

        # Assert
        assert mid_prices.prices == {}
        assert mid_prices.exchange == "test_exchange"
        assert len(mid_prices) == 0

    # ==================== EDGE CASES ====================

    def test_mid_prices_init_edge_very_high_precision_prices(self) -> None:
        """Test initialization with high precision decimal prices."""
        # Arrange
        prices = {
            "BTC-PERP": Decimal("50000.123456789123456789"),
            "ETH-PERP": Decimal("3000.987654321987654321"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="precision_exchange",
        )

        # Assert
        assert mid_prices.prices["BTC-PERP"] == Decimal("50000.123456789123456789")
        assert mid_prices.prices["ETH-PERP"] == Decimal("3000.987654321987654321")

    def test_mid_prices_init_edge_zero_prices(self) -> None:
        """Test initialization with zero prices."""
        # Arrange
        prices = {
            "ZERO-PERP": Decimal("0.0"),
            "TINY-PERP": Decimal("0.000001"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="test_exchange",
        )

        # Assert
        assert mid_prices.prices["ZERO-PERP"] == Decimal("0.0")
        assert mid_prices.prices["TINY-PERP"] == Decimal("0.000001")

    # ==================== FAILURE CASES ====================

    def test_mid_prices_init_failure_missing_prices(self) -> None:
        """Test initialization fails without prices field."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            MidPrices(exchange="test_exchange")  # type: ignore

        # Verify the validation error is about missing prices
        error_msg = str(exc_info.value)
        assert "prices" in error_msg.lower()
        assert "missing" in error_msg.lower()

    def test_mid_prices_init_failure_missing_exchange(self) -> None:
        """Test initialization fails without exchange field."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            MidPrices(prices={"BTC-PERP": Decimal("50000.0")})  # type: ignore

        # Verify the validation error is about missing exchange
        error_msg = str(exc_info.value)
        assert "exchange" in error_msg.lower()
        assert "missing" in error_msg.lower()


class TestMidPricesSymbolLookup:
    """Test suite for MidPrices symbol lookup methods."""

    @pytest.fixture
    def sample_mid_prices(self) -> MidPrices:
        """Create sample MidPrices instance for testing.
        
        Returns:
            MidPrices: A sample mid prices instance for testing.
        """
        return MidPrices(
            prices={
                "BTC-PERP": Decimal("50000.0"),
                "ETH-PERP": Decimal("3000.0"),
                "SOL-PERP": Decimal("100.0"),
            },
            exchange="test_exchange",
            timestamp=datetime.now(UTC),
        )

    # ==================== SUCCESS CASES ====================

    def test_get_success_existing_symbol(self, sample_mid_prices: MidPrices) -> None:
        """Test getting price for existing symbol."""
        # Act
        btc_price = sample_mid_prices.get("BTC-PERP")
        eth_price = sample_mid_prices.get("ETH-PERP")

        # Assert
        assert btc_price == Decimal("50000.0")
        assert eth_price == Decimal("3000.0")

    def test_has_symbol_success_existing_symbols(self, sample_mid_prices: MidPrices) -> None:
        """Test checking existence of symbols."""
        # Act & Assert
        assert sample_mid_prices.has_symbol("BTC-PERP") is True
        assert sample_mid_prices.has_symbol("ETH-PERP") is True
        assert sample_mid_prices.has_symbol("SOL-PERP") is True

    def test_symbols_success_returns_all_symbols(self, sample_mid_prices: MidPrices) -> None:
        """Test getting list of all symbols."""
        # Act
        symbols = sample_mid_prices.symbols()

        # Assert
        assert isinstance(symbols, list)
        assert len(symbols) == 3
        assert "BTC-PERP" in symbols
        assert "ETH-PERP" in symbols
        assert "SOL-PERP" in symbols

    def test_len_success_returns_correct_count(self, sample_mid_prices: MidPrices) -> None:
        """Test getting count of symbols."""
        # Act
        count = len(sample_mid_prices)

        # Assert
        assert count == 3

    # ==================== EDGE CASES ====================

    def test_get_edge_nonexistent_symbol_returns_none(self, sample_mid_prices: MidPrices) -> None:
        """Test getting price for non-existent symbol returns None."""
        # Act
        result = sample_mid_prices.get("NONEXISTENT-PERP")

        # Assert
        assert result is None

    def test_has_symbol_edge_nonexistent_symbol_returns_false(
        self, sample_mid_prices: MidPrices
    ) -> None:
        """Test checking non-existent symbol returns False."""
        # Act
        result = sample_mid_prices.has_symbol("NONEXISTENT-PERP")

        # Assert
        assert result is False

    def test_get_edge_case_sensitive_symbol_lookup(self, sample_mid_prices: MidPrices) -> None:
        """Test that symbol lookup is case sensitive."""
        # Act
        lowercase_result = sample_mid_prices.get("btc-perp")
        uppercase_result = sample_mid_prices.get("BTC-perp")

        # Assert
        assert lowercase_result is None
        assert uppercase_result is None  # Only exact match "BTC-PERP" exists

    def test_has_symbol_edge_empty_string_symbol(self, sample_mid_prices: MidPrices) -> None:
        """Test checking empty string symbol."""
        # Act
        result = sample_mid_prices.has_symbol("")

        # Assert
        assert result is False

    def test_symbols_edge_empty_prices_returns_empty_list(self) -> None:
        """Test symbols() method with empty prices."""
        # Arrange
        empty_mid_prices = MidPrices(prices={}, exchange="test_exchange")

        # Act
        symbols = empty_mid_prices.symbols()

        # Assert
        assert symbols == []

    def test_len_edge_empty_prices_returns_zero(self) -> None:
        """Test len() with empty prices."""
        # Arrange
        empty_mid_prices = MidPrices(prices={}, exchange="test_exchange")

        # Act
        count = len(empty_mid_prices)

        # Assert
        assert count == 0


class TestMidPricesUtilityMethods:
    """Test suite for MidPrices utility and helper methods."""

    def test_mid_prices_is_immutable_after_creation(self) -> None:
        """Test that MidPrices behaves as expected for data integrity."""
        # Arrange
        original_prices = {
            "BTC-PERP": Decimal("50000.0"),
            "ETH-PERP": Decimal("3000.0"),
        }
        mid_prices = MidPrices(
            prices=original_prices.copy(),
            exchange="test_exchange",
        )

        # Act - Modify the original dict (should not affect MidPrices)
        original_prices["DOGE-PERP"] = Decimal("0.1")

        # Assert - MidPrices should be unaffected
        assert "DOGE-PERP" not in mid_prices.symbols()
        assert len(mid_prices) == 2

    def test_mid_prices_string_representation_includes_key_info(self) -> None:
        """Test that string representation contains useful information."""
        # Arrange
        mid_prices = MidPrices(
            prices={"BTC-PERP": Decimal("50000.0")},
            exchange="test_exchange",
        )

        # Act
        str_repr = str(mid_prices)

        # Assert
        assert "test_exchange" in str_repr
        assert "BTC-PERP" in str_repr

    def test_mid_prices_equality_comparison(self) -> None:
        """Test equality comparison between MidPrices instances."""
        # Arrange
        timestamp = datetime.now(UTC)
        prices = {"BTC-PERP": Decimal("50000.0")}

        mid_prices1 = MidPrices(
            prices=prices,
            exchange="test_exchange",
            timestamp=timestamp,
        )
        mid_prices2 = MidPrices(
            prices=prices,
            exchange="test_exchange",
            timestamp=timestamp,
        )
        mid_prices3 = MidPrices(
            prices={"ETH-PERP": Decimal("3000.0")},
            exchange="test_exchange",
            timestamp=timestamp,
        )

        # Act & Assert
        assert mid_prices1 == mid_prices2
        assert mid_prices1 != mid_prices3

    def test_mid_prices_dict_conversion_preserves_data(self) -> None:
        """Test converting MidPrices to dict preserves all data."""
        # Arrange
        timestamp = datetime.now(UTC)
        mid_prices = MidPrices(
            prices={"BTC-PERP": Decimal("50000.0")},
            exchange="test_exchange",
            timestamp=timestamp,
        )

        # Act
        as_dict = mid_prices.model_dump()

        # Assert
        assert as_dict["prices"]["BTC-PERP"] == Decimal("50000.0")
        assert as_dict["exchange"] == "test_exchange"
        assert as_dict["timestamp"] == timestamp


class TestMidPricesEdgeCasesAndValidation:
    """Test suite for MidPrices edge cases and validation scenarios."""

    def test_mid_prices_with_special_character_symbols(self) -> None:
        """Test MidPrices with symbols containing special characters."""
        # Arrange
        prices = {
            "BTC/USD": Decimal("50000.0"),
            "ETH_USDC": Decimal("3000.0"),
            "SOL-PERP-Q24": Decimal("100.0"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="special_exchange",
        )

        # Assert
        assert mid_prices.get("BTC/USD") == Decimal("50000.0")
        assert mid_prices.get("ETH_USDC") == Decimal("3000.0")
        assert mid_prices.get("SOL-PERP-Q24") == Decimal("100.0")
        assert mid_prices.has_symbol("BTC/USD") is True

    def test_mid_prices_with_very_long_symbol_names(self) -> None:
        """Test MidPrices with very long symbol names."""
        # Arrange
        long_symbol = "VERY_LONG_SYMBOL_NAME_THAT_EXCEEDS_NORMAL_LENGTH_PERP"
        prices = {long_symbol: Decimal("123.456")}

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="test_exchange",
        )

        # Assert
        assert mid_prices.get(long_symbol) == Decimal("123.456")
        assert mid_prices.has_symbol(long_symbol) is True
        assert long_symbol in mid_prices.symbols()

    def test_mid_prices_with_negative_prices(self) -> None:
        """Test MidPrices with negative prices (edge case for some markets)."""
        # Arrange
        prices = {
            "OIL-FUT": Decimal("-10.50"),  # Could happen in commodity futures
            "NORMAL-PERP": Decimal("100.0"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="commodities_exchange",
        )

        # Assert
        assert mid_prices.get("OIL-FUT") == Decimal("-10.50")
        assert mid_prices.get("NORMAL-PERP") == Decimal("100.0")
        assert len(mid_prices) == 2

    def test_mid_prices_with_unicode_exchange_name(self) -> None:
        """Test MidPrices with unicode characters in exchange name."""
        # Arrange
        prices = {"BTC-PERP": Decimal("50000.0")}
        unicode_exchange = "测试交易所"  # Chinese characters

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange=unicode_exchange,
        )

        # Assert
        assert mid_prices.exchange == unicode_exchange
        assert mid_prices.get("BTC-PERP") == Decimal("50000.0")
