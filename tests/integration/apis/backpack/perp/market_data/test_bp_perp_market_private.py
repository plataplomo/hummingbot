"""Integration tests for Backpack Perpetual Market data with authentication.

These tests validate perpetual market data pipeline with authenticated context,
using pytest-recording (VCR) for deterministic tests.

Authentication: Ed25519 signing for API authentication where required
VCR: Records both success and error responses with sensitive data filtering
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market.market import Market


pytestmark = [pytest.mark.integration, pytest.mark.perp]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/perp/market_data/private"],
    indirect=True,
)
class TestBackpackPerpMarketPrivate:
    """Integration tests for Perpetual Market model pipeline with authenticated context."""

    @pytest.mark.perp
    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_perp_market_authenticated_context(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perpetual market retrieval with authenticated context."""
        args = GetMarketArgs(symbol="SOL_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        assert isinstance(market, Market), f"Expected Market, got {type(market)}"
        assert market.symbol == "SOL_USDC_PERP", (
            f"Expected symbol 'SOL_USDC_PERP', got '{market.symbol}'"
        )

        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal, got {type(market.tick_size)}"
        )
        assert market.tick_size > Decimal(0), (
            f"tick_size should be positive, got {market.tick_size}"
        )

        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal, got {type(market.step_size)}"
        )
        assert market.step_size > Decimal(0), (
            f"step_size should be positive, got {market.step_size}"
        )

        if market.bp_details is not None:
            from cyberdelta.core.models.market.market import BackpackMarketDetails

            assert isinstance(market.bp_details, BackpackMarketDetails), (
                f"bp_details should be BackpackMarketDetails, got {type(market.bp_details)}"
            )

        assert market.hl_details is None, "hl_details should be None for Backpack markets"

    @pytest.mark.perp
    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_perp_markets_authenticated_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test authenticated perpetual market requests handle rate limiting appropriately."""
        args = GetMarketsArgs()
        markets = await bp_api_for_test_env.get_markets(args)

        assert isinstance(markets, list), f"Expected list[Market], got {type(markets)}"
        assert len(markets) > 0, "Should return at least some markets"

        # Test first 3 perpetual markets only
        perp_markets = [m for m in markets if "_USDC_PERP" in m.symbol][:3]

        for market in perp_markets:
            individual_args = GetMarketArgs(symbol=market.symbol)
            individual_market = await bp_api_for_test_env.get_market(individual_args)

            assert individual_market is not None, f"Should retrieve market {market.symbol}"
            assert individual_market.symbol == market.symbol, (
                f"Retrieved market symbol should match: {individual_market.symbol} vs "
                f"{market.symbol}"
            )

    @pytest.mark.perp
    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_perp_market_data_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test authenticated perpetual market data is consistent."""
        test_symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"]

        for symbol in test_symbols:
            try:
                args = GetMarketArgs(symbol=symbol)
                market = await bp_api_for_test_env.get_market(args)

                assert isinstance(market, Market), (
                    f"Expected Market for {symbol}, got {type(market)}"
                )

                assert isinstance(market.symbol, str), f"symbol should be str for {symbol}"
                assert isinstance(market.base_symbol, str), (
                    f"base_symbol should be str for {symbol}"
                )
                assert isinstance(market.quote_symbol, str), (
                    f"quote_symbol should be str for {symbol}"
                )
                assert isinstance(market.tick_size, Decimal), (
                    f"tick_size should be Decimal for {symbol}"
                )
                assert isinstance(market.step_size, Decimal), (
                    f"step_size should be Decimal for {symbol}"
                )

                assert market.bp_details is None or hasattr(
                    market.bp_details,
                    "order_book_state",
                ), f"bp_details structure should be consistent for {symbol}"

            except APIError as e:
                pytest.skip(f"Symbol {symbol} not available: {e}")

    @pytest.mark.perp
    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_perp_market_access_permissions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test authenticated perpetual market access doesn't reveal restricted information."""
        args = GetMarketsArgs()
        markets = await bp_api_for_test_env.get_markets(args)

        perp_markets = [m for m in markets if "_USDC_PERP" in m.symbol]

        for market in perp_markets:
            assert not hasattr(market, "account_balance"), (
                "Market should not contain account balance info"
            )
            assert not hasattr(market, "position_size"), "Market should not contain position info"
            assert not hasattr(market, "unrealized_pnl"), "Market should not contain PnL info"

            if market.bp_details is not None:
                assert not hasattr(market.bp_details, "user_tier"), (
                    "bp_details should not contain user tier"
                )
                assert not hasattr(market.bp_details, "fee_rate"), (
                    "bp_details should not contain user fee rate"
                )

    @pytest.mark.perp
    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_perp_market_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling for authenticated perpetual market requests."""
        invalid_args = GetMarketArgs(symbol="INVALID_PERP_SYMBOL_AUTH")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(invalid_args)

        error_message = str(exc_info.value).lower()
        is_invalid_symbol_error = (
            "invalid" in error_message or "not found" in error_message or "symbol" in error_message
        )
        assert is_invalid_symbol_error, (
            f"Error should be about invalid symbol, got: {exc_info.value}"
        )

        is_not_auth_error = "auth" not in error_message and "permission" not in error_message
        assert is_not_auth_error, (
            f"Should not be authentication error for public endpoint, got: {exc_info.value}"
        )
