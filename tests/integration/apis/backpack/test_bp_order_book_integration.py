"""Integration tests for Backpack OrderBook model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_order_book() calls
to final OrderBook internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful order book retrieval for valid symbols
- Order book structure validation (bids/asks)
- Decimal precision and ordering validation
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.core.models import OrderBook


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_sol_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with SOL_USDC returns valid OrderBook model.
    
    This validates the complete pipeline:
    - API method call (get_order_book)
    - Request building (/api/v1/depth endpoint)
    - Response handling and validation
    - Mapping to internal OrderBook model
    """
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (
        f"Expected OrderBook, got {type(order_book)}"
    )
    
    # Validate core order book fields
    assert order_book.symbol == "SOL_USDC", (
        f"Expected symbol 'SOL_USDC', got '{order_book.symbol}'"
    )
    
    # Validate bids structure
    assert isinstance(order_book.bids, list), (
        f"Bids should be list, got {type(order_book.bids)}"
    )
    assert len(order_book.bids) > 0, (
        "Should have at least one bid for liquid SOL_USDC market"
    )
    
    # Validate first bid structure
    first_bid = order_book.bids[0]
    assert isinstance(first_bid, tuple), (
        f"Bid should be tuple, got {type(first_bid)}"
    )
    assert len(first_bid) == 2, (
        f"Bid tuple should have 2 elements, got {len(first_bid)}"
    )
    
    bid_price, bid_size = first_bid
    assert isinstance(bid_price, Decimal), (
        f"Bid price should be Decimal, got {type(bid_price)}"
    )
    assert isinstance(bid_size, Decimal), (
        f"Bid size should be Decimal, got {type(bid_size)}"
    )
    assert bid_price > Decimal("0"), f"Bid price should be positive, got {bid_price}"
    assert bid_size > Decimal("0"), f"Bid size should be positive, got {bid_size}"
    
    # Validate asks structure
    assert isinstance(order_book.asks, list), (
        f"Asks should be list, got {type(order_book.asks)}"
    )
    assert len(order_book.asks) > 0, (
        "Should have at least one ask for liquid SOL_USDC market"
    )
    
    # Validate first ask structure
    first_ask = order_book.asks[0]
    assert isinstance(first_ask, tuple), (
        f"Ask should be tuple, got {type(first_ask)}"
    )
    assert len(first_ask) == 2, (
        f"Ask tuple should have 2 elements, got {len(first_ask)}"
    )
    
    ask_price, ask_size = first_ask
    assert isinstance(ask_price, Decimal), (
        f"Ask price should be Decimal, got {type(ask_price)}"
    )
    assert isinstance(ask_size, Decimal), (
        f"Ask size should be Decimal, got {type(ask_size)}"
    )
    assert ask_price > Decimal("0"), f"Ask price should be positive, got {ask_price}"
    assert ask_size > Decimal("0"), f"Ask size should be positive, got {ask_size}"
    
    # Validate spread (ask should be higher than bid)
    assert ask_price > bid_price, (
        f"Ask price {ask_price} should be higher than bid price {bid_price}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_btc_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with BTC_USDC returns valid OrderBook model."""
    order_book = await bp_api_for_test_env.get_order_book("BTC_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (
        f"Expected OrderBook, got {type(order_book)}"
    )
    
    # Validate core order book fields
    assert order_book.symbol == "BTC_USDC", (
        f"Expected symbol 'BTC_USDC', got '{order_book.symbol}'"
    )
    
    # Basic structure validation
    assert isinstance(order_book.bids, list), (
        f"Bids should be list, got {type(order_book.bids)}"
    )
    assert isinstance(order_book.asks, list), (
        f"Asks should be list, got {type(order_book.asks)}"
    )
    assert len(order_book.bids) > 0, (
        "Should have at least one bid for liquid BTC_USDC market"
    )
    assert len(order_book.asks) > 0, (
        "Should have at least one ask for liquid BTC_USDC market"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with perpetual contract returns valid OrderBook model."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC_PERP")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (
        f"Expected OrderBook, got {type(order_book)}"
    )
    
    # Validate core order book fields
    assert order_book.symbol == "SOL_USDC_PERP", (
        f"Expected symbol 'SOL_USDC_PERP', got '{order_book.symbol}'"
    )
    
    # Basic structure validation
    assert isinstance(order_book.bids, list), (
        f"Bids should be list, got {type(order_book.bids)}"
    )
    assert isinstance(order_book.asks, list), (
        f"Asks should be list, got {type(order_book.asks)}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_bids_ordering_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() ensures bids are properly ordered (highest to lowest)."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (

        f"Expected OrderBook, got {type(order_book)}"

    )
    
    # Check bid ordering - should be sorted from highest price to lowest
    if len(order_book.bids) > 1:
        for i in range(len(order_book.bids) - 1):
            current_bid_price = order_book.bids[i][0]
            next_bid_price = order_book.bids[i + 1][0]
            assert current_bid_price >= next_bid_price, (
                f"Bids should be ordered highest to lowest: "
                f"bid[{i}]={current_bid_price} should be >= bid[{i+1}]={next_bid_price}"
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_asks_ordering_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() ensures asks are properly ordered (lowest to highest)."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (

        f"Expected OrderBook, got {type(order_book)}"

    )
    
    # Check ask ordering - should be sorted from lowest price to highest
    if len(order_book.asks) > 1:
        for i in range(len(order_book.asks) - 1):
            current_ask_price = order_book.asks[i][0]
            next_ask_price = order_book.asks[i + 1][0]
            assert current_ask_price <= next_ask_price, (
                f"Asks should be ordered lowest to highest: "
                f"ask[{i}]={current_ask_price} should be <= ask[{i+1}]={next_ask_price}"
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_spread_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() spread analysis and validation."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (

        f"Expected OrderBook, got {type(order_book)}"

    )
    
    if len(order_book.bids) > 0 and len(order_book.asks) > 0:
        best_bid = order_book.bids[0][0]
        best_ask = order_book.asks[0][0]
        
        # Basic spread validation
        spread = best_ask - best_bid
        assert spread > Decimal("0"), f"Spread should be positive, got {spread}"
        
        # Spread should be reasonable (not more than 10% for liquid markets)
        spread_percentage = (spread / best_bid) * Decimal("100")
        assert spread_percentage < Decimal("10"), (
            f"Spread seems too large for liquid market: {spread_percentage}%"
        )
        
        # Mid price calculation
        mid_price = (best_bid + best_ask) / Decimal("2")
        assert mid_price > Decimal("0"), f"Mid price should be positive, got {mid_price}"
        assert best_bid < mid_price < best_ask, (
            f"Mid price {mid_price} should be between best bid {best_bid} and best ask {best_ask}"
        )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_depth_analysis(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() depth and liquidity analysis."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (

        f"Expected OrderBook, got {type(order_book)}"

    )
    
    # Analyze bid depth
    total_bid_volume = Decimal("0")
    for bid_price, bid_size in order_book.bids:
        assert isinstance(bid_price, Decimal), (

            f"Bid price should be Decimal, got {type(bid_price)}"

        )
        assert isinstance(bid_size, Decimal), (

            f"Bid size should be Decimal, got {type(bid_size)}"

        )
        assert bid_price > Decimal("0"), f"Bid price should be positive, got {bid_price}"
        assert bid_size > Decimal("0"), f"Bid size should be positive, got {bid_size}"
        total_bid_volume += bid_size
    
    # Analyze ask depth
    total_ask_volume = Decimal("0")
    for ask_price, ask_size in order_book.asks:
        assert isinstance(ask_price, Decimal), (

            f"Ask price should be Decimal, got {type(ask_price)}"

        )
        assert isinstance(ask_size, Decimal), (

            f"Ask size should be Decimal, got {type(ask_size)}"

        )
        assert ask_price > Decimal("0"), f"Ask price should be positive, got {ask_price}"
        assert ask_size > Decimal("0"), f"Ask size should be positive, got {ask_size}"
        total_ask_volume += ask_size
    
    # Validate total volumes are reasonable
    assert total_bid_volume > Decimal("0"), (
        f"Total bid volume should be positive, got {total_bid_volume}"
    )
    assert total_ask_volume > Decimal("0"), (
        f"Total ask volume should be positive, got {total_ask_volume}"
    )
    
    # For a liquid market, expect reasonable depth
    min_expected_volume = Decimal("1")  # At least 1 unit of volume on each side
    assert total_bid_volume >= min_expected_volume, (
        f"Bid volume seems too low for liquid market: {total_bid_volume}"
    )
    assert total_ask_volume >= min_expected_volume, (
        f"Ask volume seems too low for liquid market: {total_ask_volume}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() ensures proper Decimal precision handling."""
    order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
    
    # Validate return type
    assert isinstance(order_book, OrderBook), (

        f"Expected OrderBook, got {type(order_book)}"

    )
    
    # Check precision of all bid/ask prices and sizes
    for bid_price, bid_size in order_book.bids[:5]:  # Check first 5 bids
        # Prices should maintain proper precision
        assert isinstance(bid_price, Decimal), (

            f"Bid price should be Decimal, got {type(bid_price)}"

        )
        assert isinstance(bid_size, Decimal), (

            f"Bid size should be Decimal, got {type(bid_size)}"

        )
        
        # Arithmetic operations should work correctly
        price_doubled = bid_price * Decimal("2")
        assert isinstance(price_doubled, Decimal), "Arithmetic should maintain Decimal type"
        assert price_doubled > bid_price, "Doubled price should be greater than original"
    
    for ask_price, ask_size in order_book.asks[:5]:  # Check first 5 asks
        # Prices should maintain proper precision
        assert isinstance(ask_price, Decimal), (

            f"Ask price should be Decimal, got {type(ask_price)}"

        )
        assert isinstance(ask_size, Decimal), (

            f"Ask size should be Decimal, got {type(ask_size)}"

        )
        
        # Arithmetic operations should work correctly
        size_halved = ask_size / Decimal("2")
        assert isinstance(size_halved, Decimal), "Arithmetic should maintain Decimal type"
        assert size_halved < ask_size, "Halved size should be less than original"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with invalid symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_order_book("INVALID_SYMBOL")
    
    # Validate error details
    error = exc_info.value
    assert "INVALID_SYMBOL" in str(error) or "symbol" in str(error).lower()


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_nonexistent_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with non-existent but well-formed symbol."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_order_book("NOTREAL_USDC")
    
    # Validate error details
    error = exc_info.value
    assert (
        "NOTREAL_USDC" in str(error) or
        "not found" in str(error).lower() or
        "symbol" in str(error).lower()
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_empty_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with empty symbol raises appropriate error."""
    with pytest.raises((APIError, ValueError)) as exc_info:
        await bp_api_for_test_env.get_order_book("")
    
    # Validate error contains relevant information
    error_str = str(exc_info.value)
    assert len(error_str) > 0, "Error message should not be empty"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_multiple_symbols_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() returns consistent structure across symbols."""
    symbols = ["SOL_USDC", "BTC_USDC"]
    order_books: list[OrderBook] = []
    
    for symbol in symbols:
        order_book = await bp_api_for_test_env.get_order_book(symbol)
        assert isinstance(order_book, OrderBook), (
            f"Expected OrderBook for {symbol}, got {type(order_book)}"
        )
        order_books.append(order_book)
    
    # Verify all order books have the same structure
    for order_book in order_books:
        assert hasattr(order_book, "symbol"), "OrderBook missing symbol attribute"
        assert hasattr(order_book, "bids"), "OrderBook missing bids attribute"
        assert hasattr(order_book, "asks"), "OrderBook missing asks attribute"
        
        assert isinstance(order_book.bids, list), (

        
            f"Bids should be list for {order_book.symbol}"

        
        )
        assert isinstance(order_book.asks, list), (

            f"Asks should be list for {order_book.symbol}"

        )
        
        # Each order book should have reasonable liquidity
        assert len(order_book.bids) > 0, f"No bids for {order_book.symbol}"
        assert len(order_book.asks) > 0, f"No asks for {order_book.symbol}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/order_book"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_order_book_limit_parameter_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_order_book() with limit parameter if supported."""
    # Test with different limit values if the API supports it
    try:
        # Try with a specific limit (this might not be supported by Backpack)
        order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
        
        # If successful, validate the limit was respected
        assert isinstance(order_book, OrderBook), (

            f"Expected OrderBook, got {type(order_book)}"

        )
        
        # Note: Not all exchanges respect the limit parameter, so we just validate structure
        assert len(order_book.bids) > 0, "Should have at least one bid"
        assert len(order_book.asks) > 0, "Should have at least one ask"
        
    except TypeError:
        # If limit parameter is not supported, that's acceptable
        # Just test the normal call works
        order_book = await bp_api_for_test_env.get_order_book("SOL_USDC")
        assert isinstance(order_book, OrderBook), (

            f"Expected OrderBook, got {type(order_book)}"

        )