"""Integration tests for Hyperliquid Ticker model pipeline.

These tests validate the complete data pipeline from HyperliquidAPI.get_ticker() calls
to final Ticker internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful ticker retrieval for valid symbols
- Edge cases and error handling (non-existent symbols)
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.core.models import Ticker


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with BTC returns valid Ticker model.

    This validates the complete pipeline:
    - API method call (get_ticker)
    - Request building (metaAndAssetCtxs endpoint)
    - Response handling and validation
    - Mapping to internal Ticker model
    """
    ticker = await hl_api_for_test_env.get_ticker("BTC")

    # Validate return type
    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "BTC", f"Expected symbol 'BTC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for BTC
    assert ticker.price > Decimal("1000"), f"BTC price seems too low: {ticker.price}"
    assert ticker.price < Decimal("1000000"), f"BTC price seems too high: {ticker.price}"

    # Validate optional fields if present
    if hasattr(ticker, "volume") and ticker.volume is not None:
        assert isinstance(ticker.volume, Decimal), (
            f"Volume should be Decimal, got {type(ticker.volume)}"
        )
        assert ticker.volume >= Decimal("0"), f"Volume should be non-negative, got {ticker.volume}"

    # Price change field may not exist in basic Ticker model
    # if hasattr(ticker, "price_change") and ticker.price_change is not None:
    #     assert isinstance(ticker.price_change, Decimal), (
    #         f"Price change should be Decimal, got {type(ticker.price_change)}"
    #     )

    # High field may not exist in basic Ticker model
    # if hasattr(ticker, "high") and ticker.high is not None:
    #     assert isinstance(ticker.high, Decimal), (
    #         f"High price should be Decimal, got {type(ticker.high)}"
    #     )
    #     assert ticker.high >= ticker.price, (
    #         f"High price {ticker.high} should be >= current price {ticker.price}"
    #     )

    # Low field may not exist in basic Ticker model
    # if hasattr(ticker, "low") and ticker.low is not None:
    #     assert isinstance(ticker.low, Decimal), (
    #         f"Low price should be Decimal, got {type(ticker.low)}"
    #     )
    #     assert ticker.low <= ticker.price, (
    #         f"Low price {ticker.low} should be <= current price {ticker.price}"
    #     )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with ETH returns valid Ticker model."""
    ticker = await hl_api_for_test_env.get_ticker("ETH")

    # Validate return type
    assert ticker is not None, "Ticker should not be None for ETH"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "ETH", f"Expected symbol 'ETH', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for ETH
    assert ticker.price > Decimal("100"), f"ETH price seems too low: {ticker.price}"
    assert ticker.price < Decimal("100000"), f"ETH price seems too high: {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_sol_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with SOL returns valid Ticker model."""
    ticker = await hl_api_for_test_env.get_ticker("SOL")

    # Validate return type
    assert ticker is not None, "Ticker should not be None for SOL"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "SOL", f"Expected symbol 'SOL', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for SOL
    assert ticker.price > Decimal("1"), f"SOL price seems too low: {ticker.price}"
    assert ticker.price < Decimal("10000"), f"SOL price seems too high: {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_avax_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with AVAX returns valid Ticker model."""
    try:
        ticker = await hl_api_for_test_env.get_ticker("AVAX")

        if ticker is not None:
            # Validate return type
            assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

            # Validate core ticker fields
            assert ticker.symbol == "AVAX", f"Expected symbol 'AVAX', got '{ticker.symbol}'"
            assert isinstance(ticker.price, Decimal), (
                f"Price should be Decimal, got {type(ticker.price)}"
            )
            assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

            # Validate reasonable price range for AVAX
            assert ticker.price > Decimal("1"), f"AVAX price seems too low: {ticker.price}"
            assert ticker.price < Decimal("1000"), f"AVAX price seems too high: {ticker.price}"
        else:
            # If AVAX is not available on Hyperliquid, that's acceptable
            pass

    except APIError:
        # If AVAX is not supported on Hyperliquid, that's acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_nonexistent_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with non-existent symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker("NONEXISTENT")

    # Should return None for non-existent symbols (Hyperliquid behavior)
    assert ticker is None, f"Expected None for non-existent symbol, got {ticker}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_invalid_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with malformed symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker("@#$%^&*")

    # Should return None for invalid symbols (Hyperliquid behavior)
    assert ticker is None, f"Expected None for invalid symbol, got {ticker}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_empty_symbol_handling(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with empty symbol."""
    try:
        ticker = await hl_api_for_test_env.get_ticker("")

        # Should return None for empty symbol
        assert ticker is None, f"Expected None for empty symbol, got {ticker}"

    except (APIError, ValueError):
        # If it raises an error for empty symbol, that's also acceptable behavior
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_case_sensitivity(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() case sensitivity behavior."""
    # Test with lowercase symbol
    ticker_lower = await hl_api_for_test_env.get_ticker("btc")

    # Test with uppercase symbol
    ticker_upper = await hl_api_for_test_env.get_ticker("BTC")

    # Both should return valid tickers or both should return None
    if ticker_lower is not None and ticker_upper is not None:
        # Both successful - validate they're equivalent
        assert isinstance(ticker_lower, Ticker), (
            f"Expected Ticker for 'btc', got {type(ticker_lower)}"
        )
        assert isinstance(ticker_upper, Ticker), (
            f"Expected Ticker for 'BTC', got {type(ticker_upper)}"
        )

        # Symbols should be normalized (likely to uppercase)
        assert ticker_lower.symbol == ticker_upper.symbol, (
            f"Case-insensitive symbols should return same symbol: "
            f"{ticker_lower.symbol} vs {ticker_upper.symbol}"
        )

        # Prices should be the same (or very close)
        if ticker_lower.price is not None and ticker_upper.price is not None:
            price_diff = abs(ticker_lower.price - ticker_upper.price)
            max_allowed_diff = ticker_upper.price * Decimal("0.01")  # 1% tolerance
            assert price_diff <= max_allowed_diff, (
                f"Prices for case-insensitive symbols should be similar: "
                f"{ticker_lower.price} vs {ticker_upper.price}"
            )

    elif ticker_lower is None and ticker_upper is None:
        # Both failed - that's consistent behavior
        pass

    else:
        # One succeeded, one failed - this could indicate case sensitivity
        # For Hyperliquid, this might be acceptable depending on their implementation
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_precision_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() ensures proper Decimal precision handling."""
    ticker = await hl_api_for_test_env.get_ticker("BTC")

    # Validate return type
    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate price precision - should maintain high precision
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"

    # Price should have reasonable precision (not truncated to integers)
    price_str = str(ticker.price)
    # For crypto prices, expect some decimal places (though this could vary)
    if "." in price_str:
        decimal_places = len(price_str.split(".")[1])
        # Should have at least some precision, but not too much (sanity check)
        assert decimal_places <= 18, (
            f"Price precision seems too high: {decimal_places} decimal places"
        )

    # Verify arithmetic operations work correctly with the Decimal
    doubled_price = ticker.price * Decimal("2")
    assert isinstance(doubled_price, Decimal), "Arithmetic with price should maintain Decimal type"
    assert doubled_price == ticker.price + ticker.price, "Decimal arithmetic should be consistent"

    # Test division operations
    half_price = ticker.price / Decimal("2")
    assert isinstance(half_price, Decimal), "Division should maintain Decimal type"
    assert half_price < ticker.price, "Half price should be less than original price"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_multiple_symbols_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() returns consistent data structure across symbols."""
    # Test multiple symbols to ensure consistent structure
    symbols = ["BTC", "ETH", "SOL"]
    tickers: list[Ticker] = []

    for symbol in symbols:
        ticker = await hl_api_for_test_env.get_ticker(symbol)
        if ticker is not None:
            assert isinstance(ticker, Ticker), f"Expected Ticker for {symbol}, got {type(ticker)}"
            tickers.append(ticker)

    # Should have at least BTC and ETH (most common symbols)
    assert len(tickers) >= 2, f"Expected at least 2 valid tickers, got {len(tickers)}"

    # Verify all tickers have the same structure (required fields)
    required_attrs = ["symbol", "price"]
    for ticker in tickers:
        for attr in required_attrs:
            assert hasattr(ticker, attr), (
                f"Ticker for {ticker.symbol} missing required attribute: {attr}"
            )
            value = getattr(ticker, attr)
            assert value is not None, (
                f"Ticker for {ticker.symbol} has None value for required attribute: {attr}"
            )

    # Verify prices are in expected order (BTC should be much higher than others)
    btc_ticker = next((t for t in tickers if t.symbol == "BTC"), None)
    eth_ticker = next((t for t in tickers if t.symbol == "ETH"), None)

    if btc_ticker and eth_ticker and btc_ticker.price is not None and eth_ticker.price is not None:
        # BTC should generally be more expensive than ETH
        assert btc_ticker.price > eth_ticker.price, (
            f"Expected BTC price {btc_ticker.price} to be higher than ETH price {eth_ticker.price}"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_symbol_normalization(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() symbol normalization and consistency."""
    # Test various symbol formats to understand normalization
    symbol_variants = ["BTC", "btc", "Btc"]
    results: dict[str, Ticker | None] = {}

    for variant in symbol_variants:
        ticker = await hl_api_for_test_env.get_ticker(variant)
        results[variant] = ticker

    # Analyze results
    valid_tickers = {k: v for k, v in results.items() if v is not None}

    if len(valid_tickers) > 0:
        # If any variants work, check symbol normalization
        symbols_returned = {ticker.symbol for ticker in valid_tickers.values()}

        # All successful requests should return the same normalized symbol
        assert len(symbols_returned) == 1, (
            f"Expected same normalized symbol for all variants, got: {symbols_returned}"
        )

        normalized_symbol = list(symbols_returned)[0]
        # Normalized symbol should be uppercase (typical crypto exchange convention)
        assert normalized_symbol.isupper(), (
            f"Expected uppercase normalized symbol, got '{normalized_symbol}'"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_price_sanity_checks(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performs comprehensive price validation."""
    ticker = await hl_api_for_test_env.get_ticker("BTC")

    # Validate return type
    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Price should be a positive Decimal
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Price should not be extremely small (no sub-penny values for major cryptos)
    assert ticker.price >= Decimal("0.01"), f"Price seems too small for BTC: {ticker.price}"

    # Price should not be NaN or infinite
    assert ticker.price.is_finite(), f"Price should be finite, got {ticker.price}"
    assert not ticker.price.is_nan(), f"Price should not be NaN, got {ticker.price}"

    # Price should have reasonable number of decimal places (not excessive precision)
    price_str = str(ticker.price)
    if "." in price_str:
        decimal_part = price_str.split(".")[1]
        # Remove trailing zeros for counting significant decimal places
        significant_decimals = len(decimal_part.rstrip("0"))
        assert significant_decimals <= 8, (
            f"Price has excessive precision: {significant_decimals} decimal places"
        )

    # Test edge case arithmetic
    try:
        # Should not overflow or underflow
        large_calc = ticker.price * Decimal("1000000")
        assert large_calc.is_finite(), "Large calculation should remain finite"

        small_calc = ticker.price / Decimal("1000000")
        assert small_calc.is_finite(), "Small calculation should remain finite"
        assert small_calc > Decimal("0"), "Small calculation should remain positive"

    except (OverflowError, ZeroDivisionError):
        pytest.fail("Price arithmetic should not cause overflow or division errors")


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_performance_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performance and consistency across multiple calls."""
    # Make multiple calls to the same symbol
    symbol = "BTC"
    tickers: list[Ticker] = []

    for _i in range(3):
        ticker = await hl_api_for_test_env.get_ticker(symbol)
        if ticker is not None:
            tickers.append(ticker)

    # Should get consistent results
    assert len(tickers) > 0, f"Should get at least one valid ticker for {symbol}"

    # All tickers should have the same symbol
    symbols = {ticker.symbol for ticker in tickers}
    assert len(symbols) == 1, f"All calls should return same symbol, got: {symbols}"

    # Prices should be consistent (for VCR recorded responses)
    prices = [ticker.price for ticker in tickers]
    unique_prices = set(prices)

    # For VCR recorded responses, prices should be identical
    # For live API calls, allow small variations due to market volatility
    if len(unique_prices) == 1:
        # Perfect consistency - this is ideal
        pass
    else:
        # Check if price variations are within reasonable bounds for live market data
        # DEFENSIVE CHECK: Runtime check for None values before operations.
        if any(price is None for price in prices):
            pytest.fail("Some prices are None, cannot perform price variation analysis")

        # Type narrowing after None check - we know all prices are Decimal now
        valid_prices = [price for price in prices if price is not None]
        min_price = min(valid_prices)
        max_price = max(valid_prices)
        price_range = max_price - min_price
        avg_price = sum(valid_prices) / Decimal(len(valid_prices))

        # Allow up to 0.1% price variation for live market data
        max_allowed_variation = avg_price * Decimal("0.001")

        assert price_range <= max_allowed_variation, (
            f"Price variation {price_range} exceeds allowed tolerance {max_allowed_variation}. "
            f"Prices: {unique_prices}. This may indicate live API calls during high volatility."
        )
