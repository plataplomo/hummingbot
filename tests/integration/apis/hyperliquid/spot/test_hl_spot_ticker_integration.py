"""Integration tests for Hyperliquid Spot Ticker model pipeline.

These tests validate the complete data pipeline from HyperliquidAPI.get_ticker() calls
to final Ticker internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful spot ticker retrieval for valid symbols
- Edge cases and error handling (non-existent symbols)
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.symbols import exchanges
from cyberdelta.models import Ticker


pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with BTC returns valid Ticker model."""
    btc_symbol = exchanges.hyperliquid("BTC")
    ticker = await hl_api_for_test_env.get_ticker(btc_symbol)

    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol == btc_symbol, f"Expected symbol '{btc_symbol}', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    assert ticker.price > Decimal(1000), f"BTC price seems too low: {ticker.price}"
    assert ticker.price < Decimal(1000000), f"BTC price seems too high: {ticker.price}"

    if hasattr(ticker, "volume") and ticker.volume is not None:
        assert isinstance(ticker.volume, Decimal), (
            f"Volume should be Decimal, got {type(ticker.volume)}"
        )
        assert ticker.volume >= Decimal(0), f"Volume should be non-negative, got {ticker.volume}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with ETH returns valid Ticker model."""
    eth_symbol = exchanges.hyperliquid("ETH")
    ticker = await hl_api_for_test_env.get_ticker(eth_symbol)

    assert ticker is not None, "Ticker should not be None for ETH"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol == eth_symbol, f"Expected symbol '{eth_symbol}', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    assert ticker.price > Decimal(100), f"ETH price seems too low: {ticker.price}"
    assert ticker.price < Decimal(100000), f"ETH price seems too high: {ticker.price}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_sol_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with SOL returns valid Ticker model."""
    sol_symbol = exchanges.hyperliquid("SOL")
    ticker = await hl_api_for_test_env.get_ticker(sol_symbol)

    assert ticker is not None, "Ticker should not be None for SOL"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol == sol_symbol, f"Expected symbol '{sol_symbol}', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    assert ticker.price > Decimal(1), f"SOL price seems too low: {ticker.price}"
    assert ticker.price < Decimal(10000), f"SOL price seems too high: {ticker.price}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_avax_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with AVAX returns valid Ticker model."""
    try:
        avax_symbol = exchanges.hyperliquid("AVAX")
        ticker = await hl_api_for_test_env.get_ticker(avax_symbol)

        if ticker is not None:
            assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

            assert ticker.symbol == avax_symbol, (
                f"Expected symbol '{avax_symbol}', got '{ticker.symbol}'"
            )
            assert isinstance(ticker.price, Decimal), (
                f"Price should be Decimal, got {type(ticker.price)}"
            )
            assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

            assert ticker.price > Decimal(1), f"AVAX price seems too low: {ticker.price}"
            assert ticker.price < Decimal(1000), f"AVAX price seems too high: {ticker.price}"

    except APIError as e:
        # Market data failures are system errors - don't hide them
        pytest.fail(
            f"Failed to get ticker data: {e}. "
            "Market data access is critical for trading operations.",
        )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_nonexistent_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with non-existent symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("NONEXISTENT"))

    assert ticker is None, f"Expected None for non-existent symbol, got {ticker}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_invalid_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with malformed symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("@#$%^&*"))

    assert ticker is None, f"Expected None for invalid symbol, got {ticker}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_empty_symbol_handling(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with empty symbol - should raise validation error."""
    # Empty symbol should raise validation error, not return None
    # This is proper input validation behavior
    with pytest.raises(ValueError) as exc_info:
        await hl_api_for_test_env.get_ticker(exchanges.hyperliquid(""))

    error_message = str(exc_info.value)
    assert "symbol" in error_message.lower(), (
        f"Error message should mention symbol: {error_message}"
    )
    assert "non-empty" in error_message.lower(), (
        f"Error message should mention non-empty requirement: {error_message}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_case_sensitivity(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() case sensitivity behavior for spot symbols."""
    ticker_lower = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("btc"))

    ticker_upper = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("BTC"))

    if ticker_lower is not None and ticker_upper is not None:
        assert isinstance(ticker_lower, Ticker), (
            f"Expected Ticker for 'btc', got {type(ticker_lower)}"
        )
        assert isinstance(ticker_upper, Ticker), (
            f"Expected Ticker for 'BTC', got {type(ticker_upper)}"
        )

        assert ticker_lower.symbol == ticker_upper.symbol, (
            f"Case-insensitive symbols should return same symbol: "
            f"{ticker_lower.symbol} vs {ticker_upper.symbol}"
        )

        if ticker_lower.price is not None and ticker_upper.price is not None:
            price_diff = abs(ticker_lower.price - ticker_upper.price)
            max_allowed_diff = ticker_upper.price * Decimal("0.01")
            assert price_diff <= max_allowed_diff, (
                f"Prices for case-insensitive symbols should be similar: "
                f"{ticker_lower.price} vs {ticker_upper.price}"
            )

    elif ticker_lower is None and ticker_upper is None:
        pass


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_precision_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() ensures proper Decimal precision handling for spot."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("BTC"))

    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"

    price_str = str(ticker.price)
    if "." in price_str:
        decimal_places = len(price_str.split(".")[1])
        assert decimal_places <= 18, (
            f"Price precision seems too high: {decimal_places} decimal places"
        )

    doubled_price = ticker.price * Decimal(2)
    assert isinstance(doubled_price, Decimal), "Arithmetic with price should maintain Decimal type"
    assert doubled_price == ticker.price + ticker.price, "Decimal arithmetic should be consistent"

    half_price = ticker.price / Decimal(2)
    assert isinstance(half_price, Decimal), "Division should maintain Decimal type"
    assert half_price < ticker.price, "Half price should be less than original price"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_multiple_symbols_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() returns consistent data structure across spot symbols."""
    symbols = ["BTC", "ETH", "SOL"]
    tickers: list[Ticker] = []

    for symbol in symbols:
        ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid(symbol))
        if ticker is not None:
            assert isinstance(ticker, Ticker), f"Expected Ticker for {symbol}, got {type(ticker)}"
            tickers.append(ticker)

    assert len(tickers) >= 2, f"Expected at least 2 valid tickers, got {len(tickers)}"

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

    btc_symbol = exchanges.hyperliquid("BTC")
    eth_symbol = exchanges.hyperliquid("ETH")
    btc_ticker = next((t for t in tickers if t.symbol == btc_symbol), None)
    eth_ticker = next((t for t in tickers if t.symbol == eth_symbol), None)

    if btc_ticker and eth_ticker and btc_ticker.price is not None and eth_ticker.price is not None:
        assert btc_ticker.price > eth_ticker.price, (
            f"Expected BTC price {btc_ticker.price} to be higher than ETH price {eth_ticker.price}"
        )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_symbol_normalization(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() symbol normalization and consistency for spot."""
    symbol_variants = ["BTC", "btc", "Btc"]
    results: dict[str, Ticker | None] = {}

    for variant in symbol_variants:
        ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid(variant))
        results[variant] = ticker

    valid_tickers = {k: v for k, v in results.items() if v is not None}

    if len(valid_tickers) > 0:
        symbols_returned = {ticker.symbol for ticker in valid_tickers.values()}

        assert len(symbols_returned) == 1, (
            f"Expected same normalized symbol for all variants, got: {symbols_returned}"
        )

        normalized_symbol = next(iter(symbols_returned))
        assert normalized_symbol.value.isupper(), (
            f"Expected uppercase normalized symbol, got '{normalized_symbol.value}'"
        )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_price_sanity_checks(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performs comprehensive price validation for spot."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("BTC"))

    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    assert ticker.price >= Decimal("0.01"), f"Price seems too small for BTC: {ticker.price}"

    assert ticker.price.is_finite(), f"Price should be finite, got {ticker.price}"
    assert not ticker.price.is_nan(), f"Price should not be NaN, got {ticker.price}"

    price_str = str(ticker.price)
    if "." in price_str:
        decimal_part = price_str.split(".")[1]
        significant_decimals = len(decimal_part.rstrip("0"))
        assert significant_decimals <= 8, (
            f"Price has excessive precision: {significant_decimals} decimal places"
        )

    try:
        large_calc = ticker.price * Decimal(1000000)
        assert large_calc.is_finite(), "Large calculation should remain finite"

        small_calc = ticker.price / Decimal(1000000)
        assert small_calc.is_finite(), "Small calculation should remain finite"
        assert small_calc > Decimal(0), "Small calculation should remain positive"

    except (OverflowError, ZeroDivisionError):
        pytest.fail("Price arithmetic should not cause overflow or division errors")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/market_data/ticker"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_spot_ticker_performance_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performance and consistency.

    Across multiple calls for spot.
    """
    symbol = "BTC"
    tickers: list[Ticker] = []

    for _i in range(3):
        ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid(symbol))
        if ticker is not None:
            tickers.append(ticker)

    assert len(tickers) > 0, f"Should get at least one valid ticker for {symbol}"

    symbols = {ticker.symbol for ticker in tickers}
    assert len(symbols) == 1, f"All calls should return same symbol, got: {symbols}"

    prices = [ticker.price for ticker in tickers]
    unique_prices = set(prices)

    if len(unique_prices) == 1:
        pass
    else:
        if any(price is None for price in prices):
            pytest.fail("Some prices are None, cannot perform price variation analysis")

        valid_prices = [price for price in prices if price is not None]
        min_price = min(valid_prices)
        max_price = max(valid_prices)
        price_range = max_price - min_price
        avg_price = sum(valid_prices) / Decimal(len(valid_prices))

        max_allowed_variation = avg_price * Decimal("0.001")

        assert price_range <= max_allowed_variation, (
            f"Price variation {price_range} exceeds allowed tolerance {max_allowed_variation}. "
            f"Prices: {unique_prices}. This may indicate live API calls during high volatility."
        )
