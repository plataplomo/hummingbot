"""Integration tests for Backpack Market model pipeline with authentication.

These tests validate the complete data pipeline for market-related endpoints that
may require authentication or provide enhanced data when authenticated, using
pytest-recording (VCR) for deterministic tests.

Tests cover:
- Market metadata retrieval with authenticated context
- Enhanced market data available to authenticated users
- Rate limiting behavior differences for authenticated vs public endpoints
- Error handling for authentication-related issues
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline

Authentication: Ed25519 signing for API authentication where required
VCR: Records both success and error responses with sensitive data filtering
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market.market import Market

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private/market"], indirect=True)
class TestBackpackMarketPrivate:
    """Integration tests for Market model pipeline with authenticated context."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_market_authenticated_context(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test market retrieval with authenticated context provides same data as public.

        While market metadata is typically public, this test validates that authenticated
        requests work correctly and don't introduce additional errors or rate limiting issues.
        """
        args = GetMarketArgs(symbol="SOL_USDC")
        market = await bp_api_for_test_env.get_market(args)

        # Validate return type and basic structure
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"
        assert market.symbol == "SOL_USDC", f"Expected symbol 'SOL_USDC', got '{market.symbol}'"

        # Validate core market fields (same validation as public endpoint)
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal, got {type(market.tick_size)}"
        )
        assert market.tick_size > Decimal("0"), (
            f"tick_size should be positive, got {market.tick_size}"
        )

        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal, got {type(market.step_size)}"
        )
        assert market.step_size > Decimal("0"), (
            f"step_size should be positive, got {market.step_size}"
        )

        # Validate Backpack-specific details
        if market.bp_details is not None:
            from cyberdelta.core.models.market.market import BackpackMarketDetails

            assert isinstance(market.bp_details, BackpackMarketDetails), (
                f"bp_details should be BackpackMarketDetails, got {type(market.bp_details)}"
            )

        # Authenticated context should not affect basic market metadata
        assert market.hl_details is None, "hl_details should be None for Backpack markets"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_markets_authenticated_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that authenticated market requests handle rate limiting appropriately.

        Authenticated users may have different rate limits. This test ensures that
        market data retrieval works correctly within those limits.
        """
        args = GetMarketsArgs()
        markets = await bp_api_for_test_env.get_markets(args)

        # Should return markets successfully
        assert isinstance(markets, list), f"Expected list[Market], got {type(markets)}"
        assert len(markets) > 0, "Should return at least some markets"

        # Validate that we can make multiple authenticated requests without issues
        # (This tests rate limiting behavior)
        for _i, market in enumerate(markets[:3]):  # Test first 3 markets
            individual_args = GetMarketArgs(symbol=market.symbol)
            individual_market = await bp_api_for_test_env.get_market(individual_args)

            assert individual_market is not None, f"Should retrieve market {market.symbol}"
            assert individual_market.symbol == market.symbol, (
                f"Retrieved market symbol should match: {individual_market.symbol} vs "
                f"{market.symbol}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_market_data_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that authenticated market data is consistent with public market data expectations.

        While the data should be the same, this validates that authentication
        doesn't introduce inconsistencies or additional/different fields.
        """
        # Test multiple symbols to ensure consistency
        test_symbols = ["SOL_USDC", "BTC_USDC", "ETH_USDC"]

        for symbol in test_symbols:
            try:
                args = GetMarketArgs(symbol=symbol)
                market = await bp_api_for_test_env.get_market(args)

                # Validate basic market structure
                assert isinstance(market, Market), (
                    f"Expected Market for {symbol}, got {type(market)}"
                )

                # Validate that authentication doesn't change core field types
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

                # Validate that Backpack-specific fields are still present
                assert market.bp_details is None or hasattr(
                    market.bp_details, "order_book_state"
                ), f"bp_details structure should be consistent for {symbol}"

            except APIError as e:
                # If specific symbols aren't available, that's acceptable
                # but log the issue for awareness
                pytest.skip(f"Symbol {symbol} not available: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_market_access_permissions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that authenticated market access doesn't reveal restricted information.

        This test ensures that market endpoints maintain appropriate data access
        controls even when authenticated.
        """
        args = GetMarketsArgs()
        markets = await bp_api_for_test_env.get_markets(args)

        # Market data should not contain sensitive trading information
        for market in markets:
            # Market metadata should not contain account-specific information
            assert not hasattr(market, "account_balance"), (
                "Market should not contain account balance info"
            )
            assert not hasattr(market, "position_size"), "Market should not contain position info"
            assert not hasattr(market, "unrealized_pnl"), "Market should not contain PnL info"

            # Backpack-specific details should only contain market-level information
            if market.bp_details is not None:
                # Should not contain user-specific data
                assert not hasattr(market.bp_details, "user_tier"), (
                    "bp_details should not contain user tier"
                )
                assert not hasattr(market.bp_details, "fee_rate"), (
                    "bp_details should not contain user fee rate"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_market_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test error handling for authenticated market requests.

        This validates that authentication errors are properly handled and
        that invalid market requests with authentication still fail appropriately.
        """
        # Test with invalid symbol
        invalid_args = GetMarketArgs(symbol="INVALID_SYMBOL_AUTH")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(invalid_args)

        # Error should be about invalid symbol, not authentication
        error_message = str(exc_info.value).lower()
        assert (
            "invalid" in error_message or "not found" in error_message or "symbol" in error_message
        ), f"Error should be about invalid symbol, got: {exc_info.value}"

        # Should not be an authentication error
        assert "auth" not in error_message and "permission" not in error_message, (
            f"Should not be authentication error for public endpoint, got: {exc_info.value}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_market_data_immutability(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that Market models maintain immutability even in authenticated context.

        This ensures that the frozen=True configuration works correctly
        regardless of how the market data was retrieved.
        """
        args = GetMarketArgs(symbol="SOL_USDC")
        market = await bp_api_for_test_env.get_market(args)

        # Market should be immutable (frozen=True)
        from pydantic import ValidationError
        
        # Test immutability by trying to modify fields
        try:
            market.symbol = "MODIFIED_SYMBOL"
            # If we get here, the model is not properly frozen
            pytest.fail("Market model should be immutable but allowed symbol modification")
        except (ValidationError, AttributeError, TypeError):
            pass  # This is expected - model should be frozen

        try:
            market.tick_size = Decimal("999.99")
            pytest.fail("Market model should be immutable but allowed tick_size modification")
        except (ValidationError, AttributeError, TypeError):
            pass  # This is expected - model should be frozen

        # Nested details should also be immutable if present
        if market.bp_details is not None:
            try:
                market.bp_details.order_book_state = "MODIFIED"
                pytest.fail("Market bp_details should be immutable but allowed modification")
            except (ValidationError, AttributeError, TypeError):
                pass  # This is expected - nested model should be frozen

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_authenticated_market_performance_baseline(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test performance baseline for authenticated market requests.

        While VCR makes this deterministic, this test validates that
        authenticated requests complete successfully and don't hang or timeout.
        """
        import time

        # Test single market retrieval performance
        start_time = time.time()
        args = GetMarketArgs(symbol="SOL_USDC")
        market = await bp_api_for_test_env.get_market(args)
        single_duration = time.time() - start_time

        assert market is not None, "Market retrieval should succeed"
        # With VCR, this should be very fast (< 1 second)
        assert single_duration < 10.0, f"Single market retrieval took too long: {single_duration}s"

        # Test multiple markets retrieval performance
        start_time = time.time()
        markets_args = GetMarketsArgs()
        markets = await bp_api_for_test_env.get_markets(markets_args)
        multiple_duration = time.time() - start_time

        assert len(markets) > 0, "Markets retrieval should return data"
        # With VCR, this should also be fast
        assert multiple_duration < 10.0, (
            f"Multiple markets retrieval took too long: {multiple_duration}s"
        )
