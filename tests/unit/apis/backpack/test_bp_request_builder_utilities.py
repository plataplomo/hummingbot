"""Unit tests for BackpackRequestBuilder utility functions."""

import pytest
from tests.common_symbols import BTC_BP, ETH_BP

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers


class TestFormatSymbol:
    """Tests for symbol formatting utility function."""

    def test_format_symbol_dash_to_underscore(self) -> None:
        """Test _format_symbol correctly converts dash to underscore."""
        assert BackpackCommonMappers.normalize_symbol("SOL-USDC") == "SOL_USDC"

    def test_format_symbol_lowercase_to_uppercase(self) -> None:
        """Test _format_symbol correctly converts lowercase to uppercase."""
        assert BackpackCommonMappers.normalize_symbol("sol_usdc") == "SOL_USDC"

    def test_format_symbol_perp_suffix_preserved(self) -> None:
        """Test _format_symbol preserves PERP suffix correctly."""
        assert BackpackCommonMappers.normalize_symbol(ETH_BP.value.replace("-", "_")) == ETH_BP.value.replace("-", "_")

    def test_format_symbol_mixed_case_and_separators(self) -> None:
        """Test _format_symbol handles mixed case and separators."""
        assert BackpackCommonMappers.normalize_symbol("btc-usdt") == "BTC_USDT"
        assert BackpackCommonMappers.normalize_symbol("Eth-Perp") == ETH_BP.value.replace("-", "_")

    def test_format_symbol_already_formatted(self) -> None:
        """Test _format_symbol handles already formatted symbols."""
        assert BackpackCommonMappers.normalize_symbol("BTC_USDT") == "BTC_USDT"
        assert BackpackCommonMappers.normalize_symbol("SOL_USDC") == "SOL_USDC"

    def test_format_symbol_empty_string(self) -> None:
        """Test _format_symbol handles edge cases."""
        assert not BackpackCommonMappers.normalize_symbol("")

    @pytest.mark.parametrize(
        ("input_symbol", "expected_output"),
        [
            ("sol-usdc", "SOL_USDC"),
            (BTC_BP.value, BTC_BP.value.replace("-", "_")),
            ("eth_usdt", "ETH_USDT"),
            ("DOGE-USDC", "DOGE_USDC"),
            ("avax_perp", "AVAX_PERP"),
        ],
    )
    def test_format_symbol_parametrized(self, input_symbol: str, expected_output: str) -> None:
        """Test format_symbol with various input combinations."""
        assert BackpackCommonMappers.normalize_symbol(input_symbol) == expected_output
