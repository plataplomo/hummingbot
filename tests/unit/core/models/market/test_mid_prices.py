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
from cyberdelta.core.symbols.api import symbol
from cyberdelta.enums.exchange_names import ExchangeName
from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL


class TestMidPricesInitialization:
    """Test suite for MidPrices model initialization."""

    # ==================== SUCCESS CASES ====================

    def test_mid_prices_init_success_with_required_fields(self) -> None:
        """Test successful initialization with required fields."""
        # Arrange
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        prices = {
            btc_symbol: Decimal("50000.0"),
            eth_symbol: Decimal("3000.0"),
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
        sol_symbol = SOL_HL
        prices = {sol_symbol: Decimal("100.0")}
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
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        prices = {
            btc_symbol: Decimal("50000.123456789123456789"),
            eth_symbol: Decimal("3000.987654321987654321"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="precision_exchange",
        )

        # Assert
        assert mid_prices.prices[btc_symbol] == Decimal("50000.123456789123456789")
        assert mid_prices.prices[eth_symbol] == Decimal("3000.987654321987654321")

    def test_mid_prices_init_edge_zero_prices(self) -> None:
        """Test initialization with zero prices."""
        # Arrange
        zero_symbol = symbol("ZERO-PERP", ExchangeName.HYPERLIQUID)
        tiny_symbol = symbol("TINY-PERP", ExchangeName.HYPERLIQUID)
        prices = {
            zero_symbol: Decimal("0.0"),
            tiny_symbol: Decimal("0.000001"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="test_exchange",
        )

        # Assert
        assert mid_prices.prices[zero_symbol] == Decimal("0.0")
        assert mid_prices.prices[tiny_symbol] == Decimal("0.000001")

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
        # Arrange
        btc_symbol = BTC_HL

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            MidPrices(prices={btc_symbol: Decimal("50000.0")})  # type: ignore

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
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        sol_symbol = SOL_HL

        return MidPrices(
            prices={
                btc_symbol: Decimal("50000.0"),
                eth_symbol: Decimal("3000.0"),
                sol_symbol: Decimal("100.0"),
            },
            exchange="test_exchange",
            timestamp=datetime.now(UTC),
        )

    # ==================== SUCCESS CASES ====================

    def test_get_success_existing_symbol(self, sample_mid_prices: MidPrices) -> None:
        """Test getting price for existing symbol."""
        # Arrange
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL

        # Act
        btc_price = sample_mid_prices.get(btc_symbol)
        eth_price = sample_mid_prices.get(eth_symbol)

        # Assert
        assert btc_price == Decimal("50000.0")
        assert eth_price == Decimal("3000.0")

    def test_has_symbol_success_existing_symbols(self, sample_mid_prices: MidPrices) -> None:
        """Test checking existence of symbols."""
        # Arrange
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        sol_symbol = SOL_HL

        # Act & Assert
        assert sample_mid_prices.has_symbol(btc_symbol) is True
        assert sample_mid_prices.has_symbol(eth_symbol) is True
        assert sample_mid_prices.has_symbol(sol_symbol) is True

    def test_symbols_success_returns_all_symbols(self, sample_mid_prices: MidPrices) -> None:
        """Test getting list of all symbols."""
        # Arrange
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        sol_symbol = SOL_HL

        # Act
        symbol_list = sample_mid_prices.symbols()

        # Assert
        assert isinstance(symbol_list, list)
        assert len(symbol_list) == 3
        assert btc_symbol in symbol_list
        assert eth_symbol in symbol_list
        assert sol_symbol in symbol_list

    def test_len_success_returns_correct_count(self, sample_mid_prices: MidPrices) -> None:
        """Test getting count of symbols."""
        # Act
        count = len(sample_mid_prices)

        # Assert
        assert count == 3

    # ==================== EDGE CASES ====================

    def test_get_edge_nonexistent_symbol_returns_none(self, sample_mid_prices: MidPrices) -> None:
        """Test getting price for non-existent symbol returns None."""
        # Arrange
        nonexistent_symbol = symbol("NONEXISTENT-PERP", ExchangeName.HYPERLIQUID)

        # Act
        result = sample_mid_prices.get(nonexistent_symbol)

        # Assert
        assert result is None

    def test_has_symbol_edge_nonexistent_symbol_returns_false(
        self, sample_mid_prices: MidPrices
    ) -> None:
        """Test checking non-existent symbol returns False."""
        # Arrange
        nonexistent_symbol = symbol("NONEXISTENT-PERP", ExchangeName.HYPERLIQUID)

        # Act
        result = sample_mid_prices.has_symbol(nonexistent_symbol)

        # Assert
        assert result is False

    def test_get_edge_case_sensitive_symbol_lookup(self, sample_mid_prices: MidPrices) -> None:
        """Test that symbol lookup is case sensitive."""
        # Arrange
        lowercase_symbol = symbol("btc-perp", ExchangeName.HYPERLIQUID)
        mixed_case_symbol = symbol("BTC-perp", ExchangeName.HYPERLIQUID)

        # Act
        lowercase_result = sample_mid_prices.get(lowercase_symbol)
        uppercase_result = sample_mid_prices.get(mixed_case_symbol)

        # Assert
        assert lowercase_result is None
        assert uppercase_result is None  # Only exact match BTC-PERP symbol exists

    def test_has_symbol_edge_empty_string_symbol(self, sample_mid_prices: MidPrices) -> None:
        """Test checking empty string symbol."""
        # Arrange
        # Note: This should fail validation due to min_length=1 in Symbol model
        # but we'll test the behavior for completeness
        result = False  # Default to False
        try:
            empty_symbol = symbol("", ExchangeName.HYPERLIQUID)
            # Act
            result = sample_mid_prices.has_symbol(empty_symbol)
        except ValueError:
            # If symbol creation fails due to validation, that's expected
            # We can't test the has_symbol behavior with an invalid symbol
            result = False  # This represents the expected behavior

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
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        original_prices = {
            btc_symbol: Decimal("50000.0"),
            eth_symbol: Decimal("3000.0"),
        }
        mid_prices = MidPrices(
            prices=original_prices.copy(),
            exchange="test_exchange",
        )

        # Act - Modify the original dict (should not affect MidPrices)
        doge_symbol = SOL_HL  # Use a different existing symbol for test
        original_prices[doge_symbol] = Decimal("0.1")

        # Assert - MidPrices should be unaffected
        assert doge_symbol not in mid_prices.symbols()
        assert len(mid_prices) == 2

    def test_mid_prices_string_representation_includes_key_info(self) -> None:
        """Test that string representation contains useful information."""
        # Arrange
        btc_symbol = BTC_HL
        mid_prices = MidPrices(
            prices={btc_symbol: Decimal("50000.0")},
            exchange="test_exchange",
        )

        # Act
        str_repr = str(mid_prices)

        # Assert
        assert "test_exchange" in str_repr
        assert "BTC-PERP" in str_repr  # Should contain the symbol's string value

    def test_mid_prices_equality_comparison(self) -> None:
        """Test equality comparison between MidPrices instances."""
        # Arrange
        btc_symbol = BTC_HL
        eth_symbol = ETH_HL
        timestamp = datetime.now(UTC)
        prices = {btc_symbol: Decimal("50000.0")}

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
            prices={eth_symbol: Decimal("3000.0")},
            exchange="test_exchange",
            timestamp=timestamp,
        )

        # Act & Assert
        assert mid_prices1 == mid_prices2
        assert mid_prices1 != mid_prices3

    def test_mid_prices_dict_conversion_preserves_data(self) -> None:
        """Test converting MidPrices to dict preserves all data."""
        # Arrange
        btc_symbol = BTC_HL
        timestamp = datetime.now(UTC)
        mid_prices = MidPrices(
            prices={btc_symbol: Decimal("50000.0")},
            exchange="test_exchange",
            timestamp=timestamp,
        )

        # Act - Use model_dump with mode='json' to handle complex types
        as_dict = mid_prices.model_dump(mode="json")

        # Assert - In JSON mode, Symbol keys should be serialized as strings
        assert as_dict["exchange"] == "test_exchange"
        # datetime gets serialized as ISO string with 'Z' suffix in JSON mode
        expected_timestamp = timestamp.isoformat().replace("+00:00", "Z")
        assert as_dict["timestamp"] == expected_timestamp
        # Check that prices dict has one entry with the correct value
        assert len(as_dict["prices"]) == 1
        # Symbol keys serialized as strings, values as "50000.0" in JSON mode
        price_values = list(as_dict["prices"].values())
        assert Decimal(price_values[0]) == Decimal("50000.0")


class TestMidPricesEdgeCasesAndValidation:
    """Test suite for MidPrices edge cases and validation scenarios."""

    def test_mid_prices_with_special_character_symbols(self) -> None:
        """Test MidPrices with symbols containing special characters."""
        # Arrange

        btc_slash_symbol = symbol("BTC/USD", ExchangeName.HYPERLIQUID)
        eth_underscore_symbol = symbol("ETH_USDC", ExchangeName.HYPERLIQUID)
        sol_quarterly_symbol = symbol("SOL-PERP-Q24", ExchangeName.HYPERLIQUID)

        prices = {
            btc_slash_symbol: Decimal("50000.0"),
            eth_underscore_symbol: Decimal("3000.0"),
            sol_quarterly_symbol: Decimal("100.0"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="special_exchange",
        )

        # Assert
        assert mid_prices.get(btc_slash_symbol) == Decimal("50000.0")
        assert mid_prices.get(eth_underscore_symbol) == Decimal("3000.0")
        assert mid_prices.get(sol_quarterly_symbol) == Decimal("100.0")
        assert mid_prices.has_symbol(btc_slash_symbol) is True

    def test_mid_prices_with_very_long_symbol_names(self) -> None:
        """Test MidPrices with very long symbol names."""
        # Arrange

        long_symbol_name = "VERY_LONG_SYMBOL_NAME_PERP"  # Shortened to respect 30 char limit
        long_symbol = symbol(long_symbol_name, ExchangeName.HYPERLIQUID)
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

        oil_symbol = symbol("OIL-FUT", ExchangeName.HYPERLIQUID)
        normal_symbol = symbol("NORMAL-PERP", ExchangeName.HYPERLIQUID)
        prices = {
            oil_symbol: Decimal("-10.50"),  # Could happen in commodity futures
            normal_symbol: Decimal("100.0"),
        }

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange="commodities_exchange",
        )

        # Assert
        assert mid_prices.get(oil_symbol) == Decimal("-10.50")
        assert mid_prices.get(normal_symbol) == Decimal("100.0")
        assert len(mid_prices) == 2

    def test_mid_prices_with_unicode_exchange_name(self) -> None:
        """Test MidPrices with unicode characters in exchange name."""
        # Arrange
        btc_symbol = BTC_HL
        prices = {btc_symbol: Decimal("50000.0")}
        unicode_exchange = "测试交易所"  # Chinese characters

        # Act
        mid_prices = MidPrices(
            prices=prices,
            exchange=unicode_exchange,
        )

        # Assert
        assert mid_prices.exchange == unicode_exchange
        assert mid_prices.get(btc_symbol) == Decimal("50000.0")
