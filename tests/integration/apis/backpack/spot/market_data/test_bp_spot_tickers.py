"""Integration tests for Backpack spot ticker model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_ticker() calls
to final Ticker internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover spot markets only:
- Successful ticker retrieval for valid spot symbols (SOL_USDC, BTC_USDC, ETH_USDC)
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
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.vcr]


class TestBackpackSpotTickers:
    """Backpack spot ticker integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/tickers"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_ticker_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with SOL_USDC returns valid Ticker model."""
        ticker = await bp_api_for_test_env.get_ticker("SOL_USDC")

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

        assert ticker.symbol == "SOL_USDC", f"Expected symbol 'SOL_USDC', got '{ticker.symbol}'"
        assert isinstance(ticker.price, Decimal), (
            f"Price should be Decimal, got {type(ticker.price)}"
        )
        assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

        # Price validation without hardcoded bounds - market prices can fluctuate widely
        assert ticker.price > Decimal(0), f"SOL price should be positive: {ticker.price}"

        if hasattr(ticker, "volume") and ticker.volume is not None:
            assert isinstance(ticker.volume, Decimal), (
                f"Volume should be Decimal, got {type(ticker.volume)}"
            )
            assert ticker.volume >= Decimal(0), (
                f"Volume should be non-negative, got {ticker.volume}"
            )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/tickers"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_ticker_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with BTC_USDC returns valid Ticker model."""
        ticker = await bp_api_for_test_env.get_ticker("BTC_USDC")

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

        assert ticker.symbol == "BTC_USDC", f"Expected symbol 'BTC_USDC', got '{ticker.symbol}'"
        assert isinstance(ticker.price, Decimal), (
            f"Price should be Decimal, got {type(ticker.price)}"
        )
        assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

        # Price validation without hardcoded bounds - market prices can fluctuate widely
        assert ticker.price > Decimal(0), f"BTC price should be positive: {ticker.price}"

        if hasattr(ticker, "volume") and ticker.volume is not None:
            assert isinstance(ticker.volume, Decimal), (
                f"Volume should be Decimal, got {type(ticker.volume)}"
            )
            assert ticker.volume >= Decimal(0), (
                f"Volume should be non-negative, got {ticker.volume}"
            )

    @pytest.mark.parametrize("symbol", ["SOL_USDC", "BTC_USDC", "ETH_USDC"])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/tickers"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_ticker_data_types_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        symbol: str,
    ) -> None:
        """Test spot ticker data types and validation across symbols."""
        ticker = await bp_api_for_test_env.get_ticker(symbol)

        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
        assert ticker.symbol == symbol, f"Expected symbol '{symbol}', got '{ticker.symbol}'"

        assert isinstance(ticker.price, Decimal), f"Price should be Decimal for {symbol}"
        assert ticker.price > Decimal(0), f"Price should be positive for {symbol}"

        if hasattr(ticker, "bid") and ticker.bid is not None:
            assert isinstance(ticker.bid, Decimal), f"Bid should be Decimal for {symbol}"
            assert ticker.bid > Decimal(0), f"Bid should be positive for {symbol}"
            assert ticker.bid <= ticker.price, f"Bid should be <= price for {symbol}"

        if hasattr(ticker, "ask") and ticker.ask is not None:
            assert isinstance(ticker.ask, Decimal), f"Ask should be Decimal for {symbol}"
            assert ticker.ask > Decimal(0), f"Ask should be positive for {symbol}"
            assert ticker.ask >= ticker.price, f"Ask should be >= price for {symbol}"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/tickers"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_ticker_invalid_spot_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_ticker() with invalid spot symbol raises appropriate error."""
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_ticker("INVALID_SPOT_SYMBOL")

        error = exc_info.value
        assert "INVALID_SPOT_SYMBOL" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/tickers"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_ticker_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot ticker precision and decimal operations."""
        ticker = await bp_api_for_test_env.get_ticker("SOL_USDC")

        assert isinstance(ticker.price, Decimal), "Price should be Decimal type"

        doubled_price = ticker.price * Decimal(2)
        assert isinstance(doubled_price, Decimal), "Price arithmetic should maintain Decimal type"
        assert doubled_price > ticker.price, "Doubled price should be greater than original"

        if ticker.price > Decimal(0):
            halved_price = ticker.price / Decimal(2)
            assert isinstance(halved_price, Decimal), "Price division should maintain Decimal type"
            assert halved_price < ticker.price, "Halved price should be less than original"

        price_str = str(ticker.price)
        assert len(price_str) > 0, "Price string representation should not be empty"
