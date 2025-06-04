"""Unit tests for BackpackRequestBuilder account and position methods."""

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetPositionsParams,
)


class TestBuildGetBalancesParams:
    """Tests for build_get_balances_params method."""

    def test_build_get_balances_params_returns_none(self) -> None:
        """Test build_get_balances_params returns empty model instance."""
        params = BackpackRequestBuilder.build_get_balances_params()
        assert isinstance(params, BackpackRawGetBalancesParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetPositionsParams:
    """Tests for build_get_positions_params method."""

    def test_build_get_positions_params_no_symbol(self) -> None:
        """Test build_get_positions_params without symbol."""
        params = BackpackRequestBuilder.build_get_positions_params(None)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    def test_build_get_positions_params_with_symbol(self, symbol_spot: str) -> None:
        """Test build_get_positions_params with symbol."""
        # Symbol is in path, not params for Backpack positions endpoint
        params = BackpackRequestBuilder.build_get_positions_params(symbol_spot)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    def test_build_get_positions_params_with_formatted_symbol(self) -> None:
        """Test build_get_positions_params with symbol requiring formatting."""
        # Even with symbol formatting needed, params should still be empty
        params = BackpackRequestBuilder.build_get_positions_params("SOL-USDC")
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    @pytest.mark.parametrize(
        "symbol",
        [
            None,
            "SOL_USDC",
            "BTC_USDT",
            "ETH-PERP",
            "sol-usdc",
        ],
    )
    def test_build_get_positions_params_parametrized(self, symbol: str | None) -> None:
        """Test build_get_positions_params with various symbol inputs."""
        params = BackpackRequestBuilder.build_get_positions_params(symbol)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetAccountInfoParams:
    """Tests for build_get_account_info_params method."""

    def test_build_get_account_info_params_returns_none(self) -> None:
        """Test build_get_account_info_params returns empty model instance."""
        params = BackpackRequestBuilder.build_get_account_info_params()
        assert isinstance(params, BackpackRawGetAccountInfoParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetFundingRateParams:
    """Tests for build_get_funding_rate_params method."""

    def test_build_get_funding_rate_params_perp_symbol(self, symbol_perp: str) -> None:
        """Test build_get_funding_rate_params with perp symbol."""
        params = BackpackRequestBuilder.build_get_funding_rate_params(symbol_perp)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        # The symbol should be formatted (dash to underscore conversion)
        expected_symbol = symbol_perp.replace("-", "_").upper()
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": expected_symbol}

    def test_build_get_funding_rate_params_formats_symbol(self) -> None:
        """Test build_get_funding_rate_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_funding_rate_params("SOL-PERP")
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": "SOL_PERP"}

    def test_build_get_funding_rate_params_btc_perp(self) -> None:
        """Test build_get_funding_rate_params with BTC perp."""
        params = BackpackRequestBuilder.build_get_funding_rate_params("BTC-PERP")
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": "BTC_PERP"}

    @pytest.mark.parametrize(
        "input_symbol, expected_symbol",
        [
            ("SOL-PERP", "SOL_PERP"),
            ("BTC-PERP", "BTC_PERP"),
            ("ETH-PERP", "ETH_PERP"),
            ("sol-perp", "SOL_PERP"),
            ("btc_perp", "BTC_PERP"),
        ],
    )
    def test_build_get_funding_rate_params_parametrized(
        self, input_symbol: str, expected_symbol: str,
    ) -> None:
        """Test build_get_funding_rate_params with various perp symbols."""
        params = BackpackRequestBuilder.build_get_funding_rate_params(input_symbol)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": expected_symbol}
