"""Integration tests for Hyperliquid Perpetual Candle model pipeline."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import HyperliquidTestHelpers


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/candle"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_market_data_btc_1h_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market_data() with BTC 1h interval.

    Returns valid perpetual Candle models.
    """
    end_time = 1640995200
    start_time = end_time - 3600

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

            assert hasattr(candle, "open")
            assert isinstance(candle.open, Decimal)
            assert hasattr(candle, "high")
            assert isinstance(candle.high, Decimal)
            assert hasattr(candle, "low")
            assert isinstance(candle.low, Decimal)
            assert hasattr(candle, "close")
            assert isinstance(candle.close, Decimal)
            assert hasattr(candle, "volume")
            assert isinstance(candle.volume, Decimal)

            assert candle.high >= candle.open
            assert candle.high >= candle.close
            assert candle.low <= candle.open
            assert candle.low <= candle.close
            assert candle.high >= candle.low

            assert candle.open > Decimal(0)
            assert candle.high > Decimal(0)
            assert candle.low > Decimal(0)
            assert candle.close > Decimal(0)
            assert candle.volume >= Decimal(0)

            assert candle.symbol == "BTC", f"Wrong symbol: {candle.symbol}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/candle"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_market_data_eth_1h_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market_data() with ETH 1h interval.

    Returns valid perpetual Candle models.
    """
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

            # Validate ETH price using current market data instead of hardcoded value
            # Get current market price to validate historical candle is reasonable
            try:
                current_price = await HyperliquidTestHelpers.get_current_market_price(
                    hl_api_for_test_env,
                    "ETH",
                )
                # Allow historical prices to be within 50% of current price (reasonable range)
                min_reasonable = current_price * Decimal("0.5")
                max_reasonable = current_price * Decimal("2.0")

                assert min_reasonable <= candle.close <= max_reasonable, (
                    f"ETH historical price {candle.close} outside reasonable range "
                    f"[{min_reasonable}, {max_reasonable}] vs current {current_price}"
                )
            except (APIError, ValueError, TypeError, KeyError):
                # If we can't get current price, just validate positive
                assert candle.close > Decimal(0), f"ETH price should be positive: {candle.close}"
