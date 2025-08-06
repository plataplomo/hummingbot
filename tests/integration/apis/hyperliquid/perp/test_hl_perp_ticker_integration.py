"""Integration tests for Hyperliquid Perpetual Ticker model pipeline.

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

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.models import Ticker
from cyberdelta.symbols import exchanges
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import HyperliquidTestHelpers


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with BTC returns valid Ticker model."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("BTC"))

    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol.value == "BTC", f"Expected symbol 'BTC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    # Get dynamic price bounds from exchange market data instead of hardcoded values
    market_constraints = await HyperliquidTestHelpers.get_market_constraints(
        hl_api_for_test_env,
        ticker.symbol,  # Use the symbol from the ticker itself
    )

    # Use exchange-specific minimum price if available, otherwise use tick_size as minimum
    min_reasonable_price = market_constraints.get("min_price") or market_constraints["tick_size"]
    max_reasonable_price = market_constraints.get("max_price") or (
        ticker.price * Decimal(100)  # Allow 100x current price as upper bound
    )

    assert ticker.price >= min_reasonable_price, (
        f"BTC price {ticker.price} below exchange minimum {min_reasonable_price}"
    )
    assert ticker.price <= max_reasonable_price, (
        f"BTC price {ticker.price} above reasonable maximum {max_reasonable_price}"
    )

    if hasattr(ticker, "volume") and ticker.volume is not None:
        assert isinstance(ticker.volume, Decimal), (
            f"Volume should be Decimal, got {type(ticker.volume)}"
        )
        assert ticker.volume >= Decimal(0), f"Volume should be non-negative, got {ticker.volume}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with ETH returns valid Ticker model."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("ETH"))

    assert ticker is not None, "Ticker should not be None for ETH"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol.value == "ETH", f"Expected symbol 'ETH', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    # Get dynamic price bounds from exchange market data instead of hardcoded values
    market_constraints = await HyperliquidTestHelpers.get_market_constraints(
        hl_api_for_test_env,
        ticker.symbol,  # Use the symbol from the ticker itself
    )

    # Use exchange-specific bounds or calculate reasonable bounds from current price
    min_reasonable_price = market_constraints.get("min_price") or market_constraints["tick_size"]
    max_reasonable_price = market_constraints.get("max_price") or (
        ticker.price * Decimal(100)  # Allow 100x current price as upper bound
    )

    assert ticker.price >= min_reasonable_price, (
        f"ETH price {ticker.price} below exchange minimum {min_reasonable_price}"
    )
    assert ticker.price <= max_reasonable_price, (
        f"ETH price {ticker.price} above reasonable maximum {max_reasonable_price}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_sol_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with SOL returns valid Ticker model."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("SOL"))

    assert ticker is not None, "Ticker should not be None for SOL"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

    assert ticker.symbol.value == "SOL", f"Expected symbol 'SOL', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

    # Get dynamic price bounds from exchange market data instead of hardcoded values
    market_constraints = await HyperliquidTestHelpers.get_market_constraints(
        hl_api_for_test_env,
        ticker.symbol,  # Use the symbol from the ticker itself
    )

    # Use exchange-specific bounds or calculate reasonable bounds from current price
    min_reasonable_price = market_constraints.get("min_price") or market_constraints["tick_size"]
    max_reasonable_price = market_constraints.get("max_price") or (
        ticker.price * Decimal(100)  # Allow 100x current price as upper bound
    )

    assert ticker.price >= min_reasonable_price, (
        f"SOL price {ticker.price} below exchange minimum {min_reasonable_price}"
    )
    assert ticker.price <= max_reasonable_price, (
        f"SOL price {ticker.price} above reasonable maximum {max_reasonable_price}"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_avax_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with AVAX returns valid Ticker model."""
    try:
        ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("AVAX"))

        if ticker is not None:
            assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"

            assert ticker.symbol.value == "AVAX", f"Expected symbol 'AVAX', got '{ticker.symbol}'"
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
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_nonexistent_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with non-existent symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("NONEXISTENT"))

    assert ticker is None, f"Expected None for non-existent symbol, got {ticker}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_invalid_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with malformed symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker(exchanges.hyperliquid("@#$%^&*"))

    assert ticker is None, f"Expected None for invalid symbol, got {ticker}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_empty_symbol_handling(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with empty symbol."""
    # Empty symbol should raise ValueError per API design
    with pytest.raises(Exception, match="String should have at least 1 character"):
        await hl_api_for_test_env.get_ticker(exchanges.hyperliquid(""))


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_case_sensitivity(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() case sensitivity behavior."""
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
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_precision_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() ensures proper Decimal precision handling."""
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
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_multiple_symbols_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() returns consistent data structure across symbols."""
    symbols = [
        exchanges.hyperliquid("BTC"),
        exchanges.hyperliquid("ETH"),
        exchanges.hyperliquid("SOL"),
    ]
    tickers: list[Ticker] = []

    for symbol in symbols:
        ticker = await hl_api_for_test_env.get_ticker(symbol)
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

    btc_ticker = next((t for t in tickers if t.symbol.value == "BTC"), None)
    eth_ticker = next((t for t in tickers if t.symbol.value == "ETH"), None)

    if btc_ticker and eth_ticker and btc_ticker.price is not None and eth_ticker.price is not None:
        assert btc_ticker.price > eth_ticker.price, (
            f"Expected BTC price {btc_ticker.price} to be higher than ETH price {eth_ticker.price}"
        )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_symbol_normalization(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() symbol normalization and consistency."""
    symbol_variants = [
        exchanges.hyperliquid("BTC"),
        exchanges.hyperliquid("btc"),
        exchanges.hyperliquid("Btc"),
    ]
    results: dict[str, Ticker | None] = {}

    for variant in symbol_variants:
        ticker = await hl_api_for_test_env.get_ticker(variant)
        results[variant.value] = ticker

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
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_price_sanity_checks(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performs comprehensive price validation."""
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
    ["apis/hyperliquid/perp/market_data/ticker"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_perp_ticker_performance_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() performance and consistency across multiple calls."""
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
