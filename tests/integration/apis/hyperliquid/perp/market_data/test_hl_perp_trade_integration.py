"""Integration tests for Hyperliquid Perpetual Trade model pipeline."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.models import Trade


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/market_data/trade"], indirect=True
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_recent_trades_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_recent_trades() with BTC returns valid perpetual Trade models."""
    trades = await hl_api_for_test_env.get_recent_trades("BTC", limit=10)

    assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

    if len(trades) > 0:
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), f"Trade {i} should be Trade model, got {type(trade)}"
            assert (
                hasattr(trade, "symbol") and hasattr(trade, "price") and hasattr(trade, "quantity")
            )
            assert isinstance(trade.price, Decimal) and trade.price > Decimal("0")
            assert isinstance(trade.quantity, Decimal) and trade.quantity > Decimal("0")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/market_data/trade"], indirect=True
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_recent_trades_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_recent_trades() with ETH returns valid perpetual Trade models."""
    trades = await hl_api_for_test_env.get_recent_trades("ETH", limit=5)

    assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

    if len(trades) > 0:
        for trade in trades:
            assert isinstance(trade, Trade), f"Should be Trade model, got {type(trade)}"
            assert trade.symbol == "ETH", f"Wrong symbol, got {trade.symbol}"
