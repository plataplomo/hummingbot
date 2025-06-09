"""Integration tests for Hyperliquid Candle model pipeline."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_data_btc_1h_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market_data() with BTC 1h interval returns valid Candle models."""
    # Use fixed timestamps for VCR consistency
    end_time = 1640995200  # Fixed timestamp
    start_time = end_time - 3600  # 1 hour earlier

    args = GetMarketDataArgs(
        symbol="BTC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await hl_api_for_test_env.get_market_data(args)

    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    if len(candles) > 0:
        for i, candle in enumerate(candles):
            assert isinstance(candle, Candle), (
                f"Candle {i} should be Candle model, got {type(candle)}"
            )

            # Validate OHLCV fields exist and are proper Decimals
            assert hasattr(candle, "open") and isinstance(candle.open, Decimal)
            assert hasattr(candle, "high") and isinstance(candle.high, Decimal)
            assert hasattr(candle, "low") and isinstance(candle.low, Decimal)
            assert hasattr(candle, "close") and isinstance(candle.close, Decimal)
            assert hasattr(candle, "volume") and isinstance(candle.volume, Decimal)

            # Validate OHLC relationships
            assert candle.high >= candle.open and candle.high >= candle.close
            assert candle.low <= candle.open and candle.low <= candle.close
            assert candle.high >= candle.low

            # Validate positive values
            assert candle.open > Decimal("0") and candle.high > Decimal("0")
            assert candle.low > Decimal("0") and candle.close > Decimal("0")
            assert candle.volume >= Decimal("0")

            assert candle.symbol == "BTC", f"Wrong symbol: {candle.symbol}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_data_eth_1h_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market_data() with ETH 1h interval returns valid Candle models."""
    end_time = 1640995200
    start_time = end_time - 3600

    args = GetMarketDataArgs(
        symbol="ETH",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await hl_api_for_test_env.get_market_data(args)

    assert isinstance(candles, list), f"Expected list, got {type(candles)}"

    if len(candles) > 0:
        for candle in candles:
            assert isinstance(candle, Candle), "Should be Candle model"
            assert candle.symbol == "ETH", f"Wrong symbol: {candle.symbol}"
            # ETH prices should be reasonable
            assert candle.close > Decimal("100"), f"ETH price too low: {candle.close}"
