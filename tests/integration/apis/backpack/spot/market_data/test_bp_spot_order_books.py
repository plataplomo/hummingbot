"""Integration tests for Backpack spot order book model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_order_book() calls
to final OrderBook internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover spot markets only:
- Successful order book retrieval for valid spot symbols (SOL_USDC, BTC_USDC, ETH_USDC)
- Order book structure validation (bids/asks)
- Decimal precision and ordering validation
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.core.models import OrderBook


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.vcr]


class TestBackpackSpotOrderBooks:
    """Backpack spot order book integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_order_book_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_order_book() with SOL_USDC returns valid OrderBook model."""
        order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")

        assert isinstance(order_book, OrderBook), f"Expected OrderBook, got {type(order_book)}"

        assert order_book.symbol == "SOL_USDC", (
            f"Expected symbol 'SOL_USDC', got '{order_book.symbol}'"
        )

        assert isinstance(order_book.bids, list), (
            f"Bids should be list, got {type(order_book.bids)}"
        )
        assert len(order_book.bids) > 0, "Should have at least one bid for liquid SOL_USDC market"

        first_bid = order_book.bids[0]
        assert isinstance(first_bid, tuple), f"Bid should be tuple, got {type(first_bid)}"
        assert len(first_bid) == 2, f"Bid tuple should have 2 elements, got {len(first_bid)}"

        bid_price, bid_size = first_bid
        assert isinstance(bid_price, Decimal), f"Bid price should be Decimal, got {type(bid_price)}"
        assert isinstance(bid_size, Decimal), f"Bid size should be Decimal, got {type(bid_size)}"
        assert bid_price > Decimal(0), f"Bid price should be positive, got {bid_price}"
        assert bid_size > Decimal(0), f"Bid size should be positive, got {bid_size}"

        assert isinstance(order_book.asks, list), (
            f"Asks should be list, got {type(order_book.asks)}"
        )
        assert len(order_book.asks) > 0, "Should have at least one ask for liquid SOL_USDC market"

        first_ask = order_book.asks[0]
        assert isinstance(first_ask, tuple), f"Ask should be tuple, got {type(first_ask)}"
        assert len(first_ask) == 2, f"Ask tuple should have 2 elements, got {len(first_ask)}"

        ask_price, ask_size = first_ask
        assert isinstance(ask_price, Decimal), f"Ask price should be Decimal, got {type(ask_price)}"
        assert isinstance(ask_size, Decimal), f"Ask size should be Decimal, got {type(ask_size)}"
        assert ask_price > Decimal(0), f"Ask price should be positive, got {ask_price}"
        assert ask_size > Decimal(0), f"Ask size should be positive, got {ask_size}"

        assert ask_price > bid_price, (
            f"Ask price {ask_price} should be higher than bid price {bid_price}"
        )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_order_book_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_order_book() with BTC_USDC returns valid OrderBook model."""
        order_book = await bp_api_for_test_env.get_order_book("BTC_USDC")

        assert isinstance(order_book, OrderBook), f"Expected OrderBook, got {type(order_book)}"
        assert order_book.symbol == "BTC_USDC", (
            f"Expected symbol 'BTC_USDC', got '{order_book.symbol}'"
        )

        # BTC should have reasonable price levels
        if len(order_book.bids) > 0:
            bid_price, _ = order_book.bids[0]
            assert bid_price > Decimal(1000), f"BTC bid price seems too low: {bid_price}"

        if len(order_book.asks) > 0:
            ask_price, _ = order_book.asks[0]
            assert ask_price > Decimal(1000), f"BTC ask price seems too low: {ask_price}"

    @pytest.mark.parametrize("symbol", ["SOL_USDC", "BTC_USDC", "ETH_USDC"])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_order_book_structure_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        symbol: str,
    ) -> None:
        """Test spot order book structure consistency across symbols."""
        order_book = await bp_api_for_test_env.get_order_book(symbol)

        assert isinstance(order_book, OrderBook), f"Expected OrderBook for {symbol}"
        assert order_book.symbol == symbol, f"Expected symbol '{symbol}', got '{order_book.symbol}'"

        # Validate bids ordering (highest to lowest)
        if len(order_book.bids) > 1:
            for i in range(len(order_book.bids) - 1):
                current_price = order_book.bids[i][0]
                next_price = order_book.bids[i + 1][0]
                assert current_price >= next_price, f"Bids not sorted correctly in {symbol}"

        # Validate asks ordering (lowest to highest)
        if len(order_book.asks) > 1:
            for i in range(len(order_book.asks) - 1):
                current_price = order_book.asks[i][0]
                next_price = order_book.asks[i + 1][0]
                assert current_price <= next_price, f"Asks not sorted correctly in {symbol}"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_order_book_depth_levels(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot order book depth and liquidity."""
        order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")

        assert isinstance(order_book, OrderBook), "Expected OrderBook"
        assert len(order_book.bids) > 0, "Should have bids"
        assert len(order_book.asks) > 0, "Should have asks"

        # Test depth levels
        total_bid_volume = sum(size for _, size in order_book.bids)
        total_ask_volume = sum(size for _, size in order_book.asks)

        assert total_bid_volume > Decimal(0), "Total bid volume should be positive"
        assert total_ask_volume > Decimal(0), "Total ask volume should be positive"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_order_book_invalid_spot_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_order_book() with invalid spot symbol raises appropriate error."""
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_order_book("INVALID_SPOT_SYMBOL")

        error = exc_info.value
        assert "INVALID_SPOT_SYMBOL" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/order_books"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_order_book_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot order book decimal precision handling."""
        order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")

        # Test precision on all bids
        for bid_price, bid_size in order_book.bids:
            assert isinstance(bid_price, Decimal), "Bid price should be Decimal"
            assert isinstance(bid_size, Decimal), "Bid size should be Decimal"

            # Test arithmetic operations
            doubled_price = bid_price * Decimal(2)
            assert isinstance(doubled_price, Decimal), (
                "Price arithmetic should maintain Decimal type"
            )

        # Test precision on all asks
        for ask_price, ask_size in order_book.asks:
            assert isinstance(ask_price, Decimal), "Ask price should be Decimal"
            assert isinstance(ask_size, Decimal), "Ask size should be Decimal"

            # Test arithmetic operations
            doubled_price = ask_price * Decimal(2)
            assert isinstance(doubled_price, Decimal), (
                "Price arithmetic should maintain Decimal type"
            )
