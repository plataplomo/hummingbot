"""Integration tests for Hyperliquid OrderBook model pipeline."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.models import OrderBook


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/order_book"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_order_book_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() with BTC returns valid OrderBook model."""
    order_book = await hl_api_for_test_env.get_order_book("BTC")

    assert order_book is not None, "OrderBook should not be None for BTC"
    assert isinstance(order_book, OrderBook), f"Expected OrderBook, got {type(order_book)}"
    assert order_book.symbol == "BTC", f"Expected symbol 'BTC', got '{order_book.symbol}'"

    # Validate bids/asks structure and OHLC relationships
    assert isinstance(order_book.bids, list) and len(order_book.bids) > 0
    assert isinstance(order_book.asks, list) and len(order_book.asks) > 0

    bid_price, _bid_size = order_book.bids[0]
    ask_price, _ask_size = order_book.asks[0]

    assert isinstance(bid_price, Decimal) and bid_price > Decimal("0")
    assert isinstance(ask_price, Decimal) and ask_price > bid_price


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/order_book"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_order_book_nonexistent_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() with non-existent symbol returns None."""
    order_book = await hl_api_for_test_env.get_order_book("NONEXISTENT")
    assert order_book is None, f"Expected None for non-existent symbol, got {order_book}"
