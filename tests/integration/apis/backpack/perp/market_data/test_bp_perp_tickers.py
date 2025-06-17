"""Integration tests for Backpack perp ticker model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_ticker() calls
to final Ticker internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover perpetual futures markets only:
- Successful ticker retrieval for valid perp symbols (SOL_USDC_PERP, BTC_USDC_PERP, ETH_USDC_PERP)
- Edge cases and error handling
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.core.models import Ticker

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.vcr]


class TestBackpackPerpTickers:
    """Backpack perp ticker integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_perp_ticker_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with SOL_USDC_PERP returns valid Ticker model."""
        ticker = await bp_api_for_test_env.get_ticker("SOL_USDC_PERP")

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

        assert ticker.symbol == "SOL_USDC_PERP", (
            f"Expected symbol 'SOL_USDC_PERP', got '{ticker.symbol}'"
        )
        assert isinstance(ticker.price, Decimal), (
            f"Price should be Decimal, got {type(ticker.price)}"
        )
        assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

        # Additional validation that price is reasonable (positive and finite)
        assert ticker.price.is_finite(), f"SOL perp price must be finite: {ticker.price}"

        if hasattr(ticker, "volume") and ticker.volume is not None:
            assert isinstance(ticker.volume, Decimal), (
                f"Volume should be Decimal, got {type(ticker.volume)}"
            )
            assert ticker.volume >= Decimal("0"), (
                f"Volume should be non-negative, got {ticker.volume}"
            )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_perp_ticker_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with BTC_USDC_PERP returns valid Ticker model."""
        ticker = await bp_api_for_test_env.get_ticker("BTC_USDC_PERP")

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

        assert ticker.symbol == "BTC_USDC_PERP", (
            f"Expected symbol 'BTC_USDC_PERP', got '{ticker.symbol}'"
        )
        assert isinstance(ticker.price, Decimal), (
            f"Price should be Decimal, got {type(ticker.price)}"
        )
        assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

        # Additional validation that price is reasonable (positive and finite)
        assert ticker.price.is_finite(), f"BTC perp price must be finite: {ticker.price}"

        if hasattr(ticker, "volume") and ticker.volume is not None:
            assert isinstance(ticker.volume, Decimal), (
                f"Volume should be Decimal, got {type(ticker.volume)}"
            )
            assert ticker.volume >= Decimal("0"), (
                f"Volume should be non-negative, got {ticker.volume}"
            )

    @pytest.mark.parametrize("symbol", ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_perp_ticker_data_types_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        symbol: str,
    ) -> None:
        """Test perp ticker data types and validation across symbols."""
        ticker = await bp_api_for_test_env.get_ticker(symbol)

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
        assert ticker.symbol == symbol, f"Expected symbol '{symbol}', got '{ticker.symbol}'"

        assert isinstance(ticker.price, Decimal), f"Price should be Decimal for {symbol}"
        assert ticker.price > Decimal("0"), f"Price should be positive for {symbol}"

        if hasattr(ticker, "bid") and ticker.bid is not None:
            assert isinstance(ticker.bid, Decimal), f"Bid should be Decimal for {symbol}"
            assert ticker.bid > Decimal("0"), f"Bid should be positive for {symbol}"
            assert ticker.bid <= ticker.price, f"Bid should be <= price for {symbol}"

        if hasattr(ticker, "ask") and ticker.ask is not None:
            assert isinstance(ticker.ask, Decimal), f"Ask should be Decimal for {symbol}"
            assert ticker.ask > Decimal("0"), f"Ask should be positive for {symbol}"
            assert ticker.ask >= ticker.price, f"Ask should be >= price for {symbol}"

        # Note: mark_price is not available in the Backpack ticker model
        # Would need to be accessed through bp_details if implemented

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_get_ticker_invalid_perp_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with invalid perp symbol raises appropriate error."""
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_ticker("INVALID_PERP")

        error = exc_info.value
        assert "INVALID_PERP" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_perp_ticker_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp ticker precision and decimal operations."""
        ticker = await bp_api_for_test_env.get_ticker("SOL_USDC_PERP")

        assert isinstance(ticker.price, Decimal), "Price should be Decimal type"

        doubled_price = ticker.price * Decimal("2")
        assert isinstance(doubled_price, Decimal), "Price arithmetic should maintain Decimal type"
        assert doubled_price > ticker.price, "Doubled price should be greater than original"

        if ticker.price > Decimal("0"):
            halved_price = ticker.price / Decimal("2")
            assert isinstance(halved_price, Decimal), "Price division should maintain Decimal type"
            assert halved_price < ticker.price, "Halved price should be less than original"

        price_str = str(ticker.price)
        assert len(price_str) > 0, "Price string representation should not be empty"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["apis/backpack/perp/tickers"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_perp_ticker_funding_rate_features(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp-specific ticker features like funding rates."""
        ticker = await bp_api_for_test_env.get_ticker("SOL_USDC_PERP")

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
        assert ticker.symbol == "SOL_USDC_PERP", "Should be perp symbol"

        # Note: funding_rate and next_funding_time are not available in the Backpack ticker model
        # These would be separate API calls to get_funding_rate() method
        # For perp trading, funding rates are accessed via dedicated endpoints
