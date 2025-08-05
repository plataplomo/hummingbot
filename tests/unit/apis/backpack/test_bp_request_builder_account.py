"""Unit tests for BackpackRequestBuilder account and position methods."""

import pytest

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetPositionsParams,
)
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.core.symbols.models import Symbol
from tests.common_symbols import BTC_BP, BTC_USDT_BP, ETH_BP, SOL_BP, SOL_USDC_BP


class TestBuildGetBalancesParams:
    """Tests for build_get_balances_params method."""

    def test_build_get_balances_params_returns_none(self) -> None:
        """Test build_get_balances_params returns empty model instance."""
        params = BackpackAccountRequestBuilder.build_get_balances_params()
        assert isinstance(params, BackpackRawGetBalancesParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetPositionsParams:
    """Tests for build_get_positions_params method."""

    def test_build_get_positions_params_no_symbol(self) -> None:
        """Test build_get_positions_params without symbol."""
        params = BackpackAccountRequestBuilder.build_get_positions_params(None)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    def test_build_get_positions_params_with_symbol(self, symbol_spot: Symbol) -> None:
        """Test build_get_positions_params with symbol."""
        # Symbol is in path, not params for Backpack positions endpoint
        params = BackpackAccountRequestBuilder.build_get_positions_params(symbol_spot)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    def test_build_get_positions_params_with_formatted_symbol(self) -> None:
        """Test build_get_positions_params with symbol requiring formatting."""
        # Even with symbol formatting needed, params should still be empty
        params = BackpackAccountRequestBuilder.build_get_positions_params(SOL_USDC_BP)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}

    @pytest.mark.parametrize(
        "symbol",
        [
            None,
            SOL_USDC_BP,
            BTC_USDT_BP,
            ETH_BP,
            SOL_USDC_BP,
        ],
    )
    def test_build_get_positions_params_parametrized(self, symbol: Symbol | None) -> None:
        """Test build_get_positions_params with various symbol inputs."""
        params = BackpackAccountRequestBuilder.build_get_positions_params(symbol)
        assert isinstance(params, BackpackRawGetPositionsParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetAccountInfoParams:
    """Tests for build_get_account_info_params method."""

    def test_build_get_account_info_params_returns_none(self) -> None:
        """Test build_get_account_info_params returns empty model instance."""
        params = BackpackAccountRequestBuilder.build_get_account_info_params()
        assert isinstance(params, BackpackRawGetAccountInfoParams)
        # Empty model should serialize to empty dict
        assert params.model_dump(by_alias=True, exclude_none=True) == {}


class TestBuildGetFundingRateParams:
    """Tests for build_get_funding_rate_params method."""

    def test_build_get_funding_rate_params_perp_symbol(self, symbol_perp: Symbol) -> None:
        """Test build_get_funding_rate_params with perp symbol."""
        params = BackpackMarketDataRequestBuilder.build_get_funding_rate_params(symbol_perp)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        # The symbol should be formatted (dash to underscore conversion)
        expected_symbol = symbol_perp.value.replace("-", "_").upper()
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": expected_symbol}

    def test_build_get_funding_rate_params_formats_symbol(self) -> None:
        """Test build_get_funding_rate_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_funding_rate_params(SOL_BP)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {
            "symbol": SOL_BP.value.replace("-", "_")
        }

    def test_build_get_funding_rate_params_btc_perp(self) -> None:
        """Test build_get_funding_rate_params with BTC perp."""
        params = BackpackMarketDataRequestBuilder.build_get_funding_rate_params(BTC_BP)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {
            "symbol": BTC_BP.value.replace("-", "_")
        }

    @pytest.mark.parametrize(
        ("input_symbol", "expected_symbol"),
        [
            (SOL_BP, SOL_BP.value.replace("-", "_")),
            (BTC_BP, BTC_BP.value.replace("-", "_")),
            (ETH_BP, ETH_BP.value.replace("-", "_")),
        ],
    )
    def test_build_get_funding_rate_params_parametrized(
        self,
        input_symbol: Symbol,
        expected_symbol: str,
    ) -> None:
        """Test build_get_funding_rate_params with various perp symbols."""
        params = BackpackMarketDataRequestBuilder.build_get_funding_rate_params(input_symbol)
        assert isinstance(params, BackpackRawGetFundingRateParams)
        assert params.model_dump(by_alias=True, exclude_none=True) == {"symbol": expected_symbol}
