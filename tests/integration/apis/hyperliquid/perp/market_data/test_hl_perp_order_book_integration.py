"""Integration tests for Hyperliquid Perpetual OrderBook model pipeline."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.models import OrderBook


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/order_book"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_order_book_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() with BTC returns valid perpetual OrderBook model."""
    order_book = await hl_api_for_test_env.get_order_book("BTC")

    assert order_book is not None, "OrderBook should not be None for BTC"
    assert isinstance(order_book, OrderBook), f"Expected OrderBook, got {type(order_book)}"
    assert order_book.symbol == "BTC", f"Expected symbol 'BTC', got '{order_book.symbol}'"

    assert isinstance(order_book.bids, list)
    assert len(order_book.bids) > 0
    assert isinstance(order_book.asks, list)
    assert len(order_book.asks) > 0

    bid_price, _bid_size = order_book.bids[0]
    ask_price, _ask_size = order_book.asks[0]

    assert isinstance(bid_price, Decimal)
    assert bid_price > Decimal(0)
    assert isinstance(ask_price, Decimal)
    assert ask_price > bid_price


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/order_book"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_order_book_nonexistent_symbol_raises_api_error(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() with non-existent symbol raises APIError."""
    from cyberdelta.apis.common import APIError, APIErrorCode

    with pytest.raises(APIError) as exc_info:
        await hl_api_for_test_env.get_order_book("NONEXISTENT")

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "No content received from HTTP client for l2Book" in exc_info.value.message
