"""Unit tests for BackpackRequestBuilder utility functions."""

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder


class TestFormatSymbol:
    """Tests for symbol formatting utility function."""

    def test_format_symbol_dash_to_underscore(self) -> None:
        """Test _format_symbol correctly converts dash to underscore."""
        assert BackpackRequestBuilder.format_symbol("SOL-USDC") == "SOL_USDC"

    def test_format_symbol_lowercase_to_uppercase(self) -> None:
        """Test _format_symbol correctly converts lowercase to uppercase."""
        assert BackpackRequestBuilder.format_symbol("sol_usdc") == "SOL_USDC"

    def test_format_symbol_perp_suffix_preserved(self) -> None:
        """Test _format_symbol preserves PERP suffix correctly."""
        assert BackpackRequestBuilder.format_symbol("ETH_PERP") == "ETH_PERP"

    def test_format_symbol_mixed_case_and_separators(self) -> None:
        """Test _format_symbol handles mixed case and separators."""
        assert BackpackRequestBuilder.format_symbol("btc-usdt") == "BTC_USDT"
        assert BackpackRequestBuilder.format_symbol("Eth-Perp") == "ETH_PERP"

    def test_format_symbol_already_formatted(self) -> None:
        """Test _format_symbol handles already formatted symbols."""
        assert BackpackRequestBuilder.format_symbol("BTC_USDT") == "BTC_USDT"
        assert BackpackRequestBuilder.format_symbol("SOL_USDC") == "SOL_USDC"

    def test_format_symbol_empty_string(self) -> None:
        """Test _format_symbol handles edge cases."""
        assert BackpackRequestBuilder.format_symbol("") == ""

    @pytest.mark.parametrize(
        "input_symbol, expected_output",
        [
            ("sol-usdc", "SOL_USDC"),
            ("BTC-PERP", "BTC_PERP"),
            ("eth_usdt", "ETH_USDT"),
            ("DOGE-USDC", "DOGE_USDC"),
            ("avax_perp", "AVAX_PERP"),
        ],
    )
    def test_format_symbol_parametrized(self, input_symbol: str, expected_output: str) -> None:
        """Test format_symbol with various input combinations."""
        assert BackpackRequestBuilder.format_symbol(input_symbol) == expected_output
