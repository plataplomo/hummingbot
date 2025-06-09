"""Integration tests for Backpack Ticker model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_ticker() calls
to final Ticker internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful ticker retrieval for valid symbols
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_sol_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with SOL_USDC returns valid Ticker model.

    This validates the complete pipeline:
    - API method call (get_ticker)
    - Request building (/api/v1/ticker endpoint)
    - Response handling and validation
    - Mapping to internal Ticker model
    """
    ticker = await bp_api_for_test_env.get_ticker("SOL_USDC")

    # Validate return type
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "SOL_USDC", f"Expected symbol 'SOL_USDC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for SOL
    assert ticker.price > Decimal("1"), f"SOL price seems too low: {ticker.price}"
    assert ticker.price < Decimal("10000"), f"SOL price seems too high: {ticker.price}"

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

    # High/low fields may not exist in basic Ticker model
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_btc_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with BTC_USDC returns valid Ticker model."""
    ticker = await bp_api_for_test_env.get_ticker("BTC_USDC")

    # Validate return type
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "BTC_USDC", f"Expected symbol 'BTC_USDC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for BTC
    assert ticker.price > Decimal("1000"), f"BTC price seems too low: {ticker.price}"
    assert ticker.price < Decimal("1000000"), f"BTC price seems too high: {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_eth_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with ETH_USDC returns valid Ticker model."""
    ticker = await bp_api_for_test_env.get_ticker("ETH_USDC")

    # Validate return type
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "ETH_USDC", f"Expected symbol 'ETH_USDC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"

    # Validate reasonable price range for ETH
    assert ticker.price > Decimal("100"), f"ETH price seems too low: {ticker.price}"
    assert ticker.price < Decimal("100000"), f"ETH price seems too high: {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_perp_symbol_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with perpetual contract symbol returns valid Ticker model."""
    ticker = await bp_api_for_test_env.get_ticker("SOL_USDC_PERP")

    # Validate return type
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    # Validate core ticker fields
    assert ticker.symbol == "SOL_USDC_PERP", (
        f"Expected symbol 'SOL_USDC_PERP', got '{ticker.symbol}'"
    )
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with invalid symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_ticker("INVALID_SYMBOL")

    # Validate error details
    error = exc_info.value
    assert "INVALID_SYMBOL" in str(error) or "symbol" in str(error).lower()


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_nonexistent_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with non-existent but well-formed symbol."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_ticker("NOTREAL_USDC")

    # Validate error details
    error = exc_info.value
    assert (
        "NOTREAL_USDC" in str(error)
        or "not found" in str(error).lower()
        or "symbol" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_empty_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with empty symbol raises appropriate error."""
    with pytest.raises((APIError, ValueError)) as exc_info:
        await bp_api_for_test_env.get_ticker("")

    # Validate error contains relevant information
    error_str = str(exc_info.value)
    assert len(error_str) > 0, "Error message should not be empty"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_malformed_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() with malformed symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_ticker("@#$%^&*")

    # Validate error details
    error = exc_info.value
    assert (
        "@#$%^&*" in str(error) or "symbol" in str(error).lower() or "invalid" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_case_sensitivity(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() case sensitivity behavior."""
    # Test with lowercase symbol (should work if Backpack normalizes case)
    try:
        ticker = await bp_api_for_test_env.get_ticker("sol_usdc")
        # If successful, validate the returned symbol is normalized
        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
        # Symbol should be normalized to uppercase format
        assert ticker.symbol in ["SOL_USDC", "sol_usdc"], (
            f"Unexpected symbol format: {ticker.symbol}"
        )
    except APIError:
        # If it fails, that's also acceptable behavior - case sensitivity is exchange-specific
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_data_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() ensures proper Decimal precision handling."""
    ticker = await bp_api_for_test_env.get_ticker("SOL_USDC")

    # Validate return type
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/ticker"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_ticker_multiple_symbols_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_ticker() returns consistent data structure across symbols."""
    # Test multiple symbols to ensure consistent structure
    symbols = ["SOL_USDC", "BTC_USDC"]
    tickers: list[Ticker] = []

    for symbol in symbols:
        ticker = await bp_api_for_test_env.get_ticker(symbol)
        assert isinstance(ticker, Ticker), f"Expected Ticker for {symbol}, got {type(ticker)}"
        tickers.append(ticker)

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

    # Verify prices are in expected order (BTC should be much higher than SOL)
    sol_ticker = next(t for t in tickers if t.symbol == "SOL_USDC")
    btc_ticker = next(t for t in tickers if t.symbol == "BTC_USDC")

    # BTC should generally be much more expensive than SOL
    if btc_ticker.price is not None and sol_ticker.price is not None:
        assert btc_ticker.price > sol_ticker.price * Decimal("10"), (
            f"Expected BTC price {btc_ticker.price} to be much higher than "
            f"SOL price {sol_ticker.price}"
        )
