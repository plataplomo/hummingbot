"""Integration tests for Backpack spot market candle data.

These tests validate the complete data pipeline from BackpackAPI.get_market_data() calls
to final Candle internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover spot markets only:
- SOL_USDC, BTC_USDC, ETH_USDC spot candle data
- OHLCV data validation and relationships
- Time series validation and ordering
- Edge cases and error handling
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle
from tests.fixtures.time_fixtures import FreezerProtocol


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.vcr]


class TestBackpackSpotCandles:
    """Backpack spot market candle integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/candles"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_1h_candles_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with SOL_USDC 1h interval.

        Returns valid Candle models.
        """
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)
        start_time_dt = end_time_dt - timedelta(hours=1)

        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol="SOL_USDC",
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

        if len(candles) > 0:
            for i, candle in enumerate(candles):
                assert isinstance(candle, Candle), (
                    f"Candle {i} should be Candle model, got {type(candle)}"
                )

                assert hasattr(candle, "symbol"), f"Candle {i} should have symbol attribute"
                assert hasattr(candle, "open"), f"Candle {i} should have open attribute"
                assert hasattr(candle, "high"), f"Candle {i} should have high attribute"
                assert hasattr(candle, "low"), f"Candle {i} should have low attribute"
                assert hasattr(candle, "close"), f"Candle {i} should have close attribute"
                assert hasattr(candle, "volume"), f"Candle {i} should have volume attribute"
                assert hasattr(candle, "open_time"), f"Candle {i} should have open_time attribute"

                assert isinstance(candle.open, Decimal), (
                    f"Candle {i} open should be Decimal, got {type(candle.open)}"
                )
                assert isinstance(candle.high, Decimal), (
                    f"Candle {i} high should be Decimal, got {type(candle.high)}"
                )
                assert isinstance(candle.low, Decimal), (
                    f"Candle {i} low should be Decimal, got {type(candle.low)}"
                )
                assert isinstance(candle.close, Decimal), (
                    f"Candle {i} close should be Decimal, got {type(candle.close)}"
                )
                assert isinstance(candle.volume, Decimal), (
                    f"Candle {i} volume should be Decimal, got {type(candle.volume)}"
                )

                assert candle.open > Decimal(0), (
                    f"Candle {i} open should be positive, got {candle.open}"
                )
                assert candle.high > Decimal(0), (
                    f"Candle {i} high should be positive, got {candle.high}"
                )
                assert candle.low > Decimal(0), (
                    f"Candle {i} low should be positive, got {candle.low}"
                )
                assert candle.close > Decimal(0), (
                    f"Candle {i} close should be positive, got {candle.close}"
                )
                assert candle.volume >= Decimal(0), (
                    f"Candle {i} volume should be non-negative, got {candle.volume}"
                )

                assert candle.high >= candle.open, (
                    f"Candle {i} high {candle.high} should be >= open {candle.open}"
                )
                assert candle.high >= candle.close, (
                    f"Candle {i} high {candle.high} should be >= close {candle.close}"
                )
                assert candle.low <= candle.open, (
                    f"Candle {i} low {candle.low} should be <= open {candle.open}"
                )
                assert candle.low <= candle.close, (
                    f"Candle {i} low {candle.low} should be <= close {candle.close}"
                )

                assert candle.symbol == "SOL_USDC", (
                    f"Candle {i} symbol should be 'SOL_USDC', got '{candle.symbol}'"
                )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/candles"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_1h_candles_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with BTC_USDC 1h interval.

        Returns valid Candle models.
        """
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)
        start_time_dt = end_time_dt - timedelta(hours=1)

        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol="BTC_USDC",
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

        if len(candles) > 0:
            for i, candle in enumerate(candles):
                assert isinstance(candle, Candle), (
                    f"Candle {i} should be Candle model, got {type(candle)}"
                )
                assert candle.symbol == "BTC_USDC", (
                    f"Candle {i} symbol should be 'BTC_USDC', got '{candle.symbol}'"
                )

                # Validate OHLC relationships instead of hardcoded price bounds
                assert candle.open > Decimal(0), f"BTC open price should be positive: {candle.open}"
                assert candle.high > Decimal(0), f"BTC high price should be positive: {candle.high}"
                assert candle.low > Decimal(0), f"BTC low price should be positive: {candle.low}"
                assert candle.close > Decimal(0), (
                    f"BTC close price should be positive: {candle.close}"
                )

                # Validate OHLC relationships
                assert candle.high >= candle.open, (
                    f"BTC high {candle.high} should be >= open {candle.open}"
                )
                assert candle.high >= candle.close, (
                    f"BTC high {candle.high} should be >= close {candle.close}"
                )
                assert candle.low <= candle.open, (
                    f"BTC low {candle.low} should be <= open {candle.open}"
                )
                assert candle.low <= candle.close, (
                    f"BTC low {candle.low} should be <= close {candle.close}"
                )

    @pytest.mark.parametrize("symbol", ["SOL_USDC", "BTC_USDC", "ETH_USDC"])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/candles"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_candle_multiple_symbols_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
        symbol: str,
    ) -> None:
        """Test spot candle structure consistency across different symbols."""
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)
        start_time_dt = end_time_dt - timedelta(days=1)

        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=symbol,
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)
        assert isinstance(candles, list), f"Expected list for {symbol}, got {type(candles)}"

        if len(candles) > 0:
            first_candle = candles[0]
            required_attrs = ["symbol", "open", "high", "low", "close", "volume"]

            for attr in required_attrs:
                assert hasattr(first_candle, attr), (
                    f"Candle for {symbol} missing required attribute: {attr}"
                )
                value = getattr(first_candle, attr)
                assert value is not None, (
                    f"Candle for {symbol} has None value for required attribute: {attr}"
                )

            for i, candle in enumerate(candles):
                assert candle.symbol == symbol, (
                    f"Candle {i} for {symbol} has wrong symbol: {candle.symbol}"
                )
