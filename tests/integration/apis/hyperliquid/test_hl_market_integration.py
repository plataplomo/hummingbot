"""Integration tests for Hyperliquid Market model pipeline.

These tests validate the complete data pipeline from HyperliquidAPI.get_market() and
HyperliquidAPI.get_markets() calls to final Market internal domain models using
pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful market metadata retrieval for valid symbols
- Multiple markets retrieval and validation
- Edge cases and error handling (non-existent symbols)
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
- Hyperliquid-specific market details (hl_details extension slots)
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market.market import Market

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market() with BTC returns valid Market model.

    This validates the complete pipeline:
    - API method call (get_market)
    - Request building (metaAndAssetCtxs endpoint)
    - Response handling and validation
    - Mapping to internal Market model with Hyperliquid-specific details
    """
    args = GetMarketArgs(symbol="BTC")
    market = await hl_api_for_test_env.get_market(args)

    # Validate return type
    assert market is not None, "Market should not be None for BTC"
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "BTC", f"Expected symbol 'BTC', got '{market.symbol}'"
    assert market.base_symbol == "BTC", f"Expected base_symbol 'BTC', got '{market.base_symbol}'"
    assert market.quote_symbol == "USD", f"Expected quote_symbol 'USD', got '{market.quote_symbol}'"

    # Validate market type for Hyperliquid (perpetual futures)
    assert isinstance(market.market_type, str), (
        f"market_type should be str, got {type(market.market_type)}"
    )
    assert market.market_type in ["Perpetual", "Future", "Perp"], (
        f"Expected perpetual market type for Hyperliquid, got '{market.market_type}'"
    )

    # Validate tick size and step size
    assert isinstance(market.tick_size, Decimal), (
        f"tick_size should be Decimal, got {type(market.tick_size)}"
    )
    assert market.tick_size > Decimal("0"), f"tick_size should be positive, got {market.tick_size}"

    assert isinstance(market.step_size, Decimal), (
        f"step_size should be Decimal, got {type(market.step_size)}"
    )
    assert market.step_size > Decimal("0"), f"step_size should be positive, got {market.step_size}"

    # For BTC, tick size should be reasonable
    assert market.tick_size <= Decimal("100"), f"BTC tick_size seems too large: {market.tick_size}"
    assert market.tick_size >= Decimal("0.01"), f"BTC tick_size seems too small: {market.tick_size}"

    # Validate optional price limits
    if market.min_price is not None:
        assert isinstance(market.min_price, Decimal), (
            f"min_price should be Decimal, got {type(market.min_price)}"
        )
        assert market.min_price >= Decimal("0"), (
            f"min_price should be non-negative, got {market.min_price}"
        )

    if market.max_price is not None:
        assert isinstance(market.max_price, Decimal), (
            f"max_price should be Decimal, got {type(market.max_price)}"
        )
        assert market.max_price >= Decimal("0"), (
            f"max_price should be non-negative, got {market.max_price}"
        )

    # Validate status
    assert isinstance(market.status, str), f"status should be str, got {type(market.status)}"
    assert len(market.status) > 0, "status should not be empty"

    # Validate Hyperliquid-specific details
    if market.hl_details is not None:
        from cyberdelta.core.models.market.market import HyperliquidMarketDetails

        assert isinstance(market.hl_details, HyperliquidMarketDetails), (
            f"hl_details should be HyperliquidMarketDetails, got {type(market.hl_details)}"
        )

        # Validate Hyperliquid-specific fields
        assert isinstance(market.hl_details.max_leverage, int), (
            f"max_leverage should be int, got {type(market.hl_details.max_leverage)}"
        )
        assert 1 <= market.hl_details.max_leverage <= 1000, (
            f"max_leverage should be 1-1000, got {market.hl_details.max_leverage}"
        )

        assert isinstance(market.hl_details.only_isolated, bool), (
            f"only_isolated should be bool, got {type(market.hl_details.only_isolated)}"
        )

        assert isinstance(market.hl_details.sz_decimals, int), (
            f"sz_decimals should be int, got {type(market.hl_details.sz_decimals)}"
        )
        assert 0 <= market.hl_details.sz_decimals <= 18, (
            f"sz_decimals should be 0-18, got {market.hl_details.sz_decimals}"
        )

        # Optional fields validation
        if market.hl_details.mark_price is not None:
            assert isinstance(market.hl_details.mark_price, Decimal), (
                f"mark_price should be Decimal, got {type(market.hl_details.mark_price)}"
            )
            assert market.hl_details.mark_price >= Decimal("0"), (
                f"mark_price should be non-negative, got {market.hl_details.mark_price}"
            )

        if market.hl_details.funding_rate is not None:
            assert isinstance(market.hl_details.funding_rate, Decimal), (
                f"funding_rate should be Decimal, got {type(market.hl_details.funding_rate)}"
            )

    # Validate that bp_details is None for Hyperliquid markets
    assert market.bp_details is None, "bp_details should be None for Hyperliquid markets"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market() with ETH returns valid Market model."""
    args = GetMarketArgs(symbol="ETH")
    market = await hl_api_for_test_env.get_market(args)

    # Validate return type
    assert market is not None, "Market should not be None for ETH"
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "ETH", f"Expected symbol 'ETH', got '{market.symbol}'"
    assert market.base_symbol == "ETH", f"Expected base_symbol 'ETH', got '{market.base_symbol}'"
    assert market.quote_symbol == "USD", f"Expected quote_symbol 'USD', got '{market.quote_symbol}'"

    # ETH should have reasonable tick and step sizes
    assert market.tick_size <= Decimal("10"), f"ETH tick_size seems too large: {market.tick_size}"
    assert market.step_size <= Decimal("1"), f"ETH step_size seems too large: {market.step_size}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_sol_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market() with SOL returns valid Market model."""
    args = GetMarketArgs(symbol="SOL")
    market = await hl_api_for_test_env.get_market(args)

    # Validate return type
    assert market is not None, "Market should not be None for SOL"
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "SOL", f"Expected symbol 'SOL', got '{market.symbol}'"
    assert market.base_symbol == "SOL", f"Expected base_symbol 'SOL', got '{market.base_symbol}'"
    assert market.quote_symbol == "USD", f"Expected quote_symbol 'USD', got '{market.quote_symbol}'"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_nonexistent_symbol_returns_none(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market() with non-existent symbol returns None or raises APIError."""
    args = GetMarketArgs(symbol="NONEXISTENT")

    try:
        market = await hl_api_for_test_env.get_market(args)
        # Should return None for non-existent symbols (Hyperliquid behavior)
        assert market is None, f"Expected None for non-existent symbol, got {market}"
    except APIError:
        # If it raises APIError instead of returning None, that's also acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_invalid_symbol_handling(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market() with malformed symbol."""
    args = GetMarketArgs(symbol="@#$%^&*")

    try:
        market = await hl_api_for_test_env.get_market(args)
        # Should return None for invalid symbols (Hyperliquid behavior)
        assert market is None, f"Expected None for invalid symbol, got {market}"
    except APIError:
        # If it raises APIError instead of returning None, that's also acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_markets_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_markets() returns valid list of Market models.

    This validates the complete pipeline:
    - API method call (get_markets)
    - Request building (metaAndAssetCtxs endpoint)
    - Response handling and validation
    - Mapping to internal Market models
    """
    args = GetMarketsArgs()
    markets = await hl_api_for_test_env.get_markets(args)

    # Validate return type
    assert isinstance(markets, list), f"Expected list[Market], got {type(markets)}"
    assert len(markets) > 0, "Should return at least some markets"

    # Validate each market
    symbols_seen = set()
    for market in markets:
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"

        # Validate unique symbols
        assert market.symbol not in symbols_seen, f"Duplicate symbol found: {market.symbol}"
        symbols_seen.add(market.symbol)

        # Validate core fields are present and valid
        assert isinstance(market.symbol, str), f"symbol should be str, got {type(market.symbol)}"
        assert len(market.symbol) > 0, "symbol should not be empty"

        assert isinstance(market.base_symbol, str), (
            f"base_symbol should be str, got {type(market.base_symbol)}"
        )
        assert len(market.base_symbol) > 0, "base_symbol should not be empty"

        assert isinstance(market.quote_symbol, str), (
            f"quote_symbol should be str, got {type(market.quote_symbol)}"
        )
        assert len(market.quote_symbol) > 0, "quote_symbol should not be empty"

        # For Hyperliquid, quote should typically be USD
        assert market.quote_symbol == "USD", (
            f"Expected USD quote for Hyperliquid market {market.symbol}"
        )

        # Validate financial constraints
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal for {market.symbol}"
        )
        assert market.tick_size > Decimal("0"), f"tick_size should be positive for {market.symbol}"

        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal for {market.symbol}"
        )
        assert market.step_size > Decimal("0"), f"step_size should be positive for {market.symbol}"

        # Validate status
        assert isinstance(market.status, str), f"status should be str for {market.symbol}"
        assert len(market.status) > 0, f"status should not be empty for {market.symbol}"

        # Validate exchange-specific details
        assert market.bp_details is None, (
            f"bp_details should be None for Hyperliquid market {market.symbol}"
        )

        # Hyperliquid markets should have hl_details
        if market.hl_details is not None:
            from cyberdelta.core.models.market.market import HyperliquidMarketDetails

            assert isinstance(market.hl_details, HyperliquidMarketDetails), (
                f"hl_details should be HyperliquidMarketDetails for {market.symbol}"
            )

    # Should include common trading symbols
    market_symbols = {market.symbol for market in markets}
    # At least some common symbols should be available
    common_symbols = {"BTC", "ETH", "SOL"}
    found_symbols = common_symbols.intersection(market_symbols)
    assert len(found_symbols) > 0, (
        f"Expected to find at least one common symbol from {common_symbols}, "
        f"got symbols: {sorted(market_symbols)}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_markets_data_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_markets() returns consistent data structure across markets."""
    args = GetMarketsArgs()
    markets = await hl_api_for_test_env.get_markets(args)

    assert len(markets) > 1, "Need multiple markets for consistency testing"

    # Verify all markets have the same structure (required fields)
    required_attrs = [
        "symbol",
        "base_symbol",
        "quote_symbol",
        "market_type",
        "tick_size",
        "step_size",
        "status",
    ]

    for market in markets:
        for attr in required_attrs:
            assert hasattr(market, attr), (
                f"Market {market.symbol} missing required attribute: {attr}"
            )
            value = getattr(market, attr)
            assert value is not None, (
                f"Market {market.symbol} has None value for required attribute: {attr}"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_markets_hyperliquid_specific_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid-specific market characteristics and validation."""
    args = GetMarketsArgs()
    markets = await hl_api_for_test_env.get_markets(args)

    for market in markets:
        # All Hyperliquid markets should be perpetual futures with USD quote
        assert market.quote_symbol == "USD", (
            f"Expected USD quote for all Hyperliquid markets, got {market.quote_symbol} "
            f"for {market.symbol}"
        )

        # Market type should indicate perpetual/futures
        assert market.market_type in ["Perpetual", "Future", "Perp"], (
            f"Expected perpetual market type for Hyperliquid, got '{market.market_type}' "
            f"for {market.symbol}"
        )

        # Base symbol should match the symbol for Hyperliquid's naming convention
        assert market.base_symbol == market.symbol, (
            f"For Hyperliquid, base_symbol should equal symbol, got {market.base_symbol} "
            f"vs {market.symbol}"
        )

        # Validate Hyperliquid-specific details if present
        if market.hl_details is not None:
            # Max leverage should be reasonable for crypto derivatives
            assert market.hl_details.max_leverage >= 1, (
                f"max_leverage should be at least 1 for {market.symbol}"
            )
            assert market.hl_details.max_leverage <= 1000, (
                f"max_leverage should be at most 1000 for {market.symbol}"
            )

            # Size decimals should be reasonable
            assert 0 <= market.hl_details.sz_decimals <= 18, (
                f"sz_decimals should be 0-18 for {market.symbol}"
            )

            # If mark price is provided, it should be positive
            if market.hl_details.mark_price is not None:
                assert market.hl_details.mark_price > Decimal("0"), (
                    f"mark_price should be positive for {market.symbol}"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_markets_precision_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_markets() ensures proper Decimal precision handling."""
    args = GetMarketsArgs()
    markets = await hl_api_for_test_env.get_markets(args)

    for market in markets:
        # Validate tick_size precision
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal for {market.symbol}"
        )
        assert market.tick_size.is_finite(), f"tick_size should be finite for {market.symbol}"
        assert not market.tick_size.is_nan(), f"tick_size should not be NaN for {market.symbol}"

        # Validate step_size precision
        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal for {market.symbol}"
        )
        assert market.step_size.is_finite(), f"step_size should be finite for {market.symbol}"
        assert not market.step_size.is_nan(), f"step_size should not be NaN for {market.symbol}"

        # Verify arithmetic operations work correctly with the Decimals
        doubled_tick = market.tick_size * Decimal("2")
        assert isinstance(doubled_tick, Decimal), (
            f"Arithmetic with tick_size should maintain Decimal type for {market.symbol}"
        )

        # Test division operations
        half_step = market.step_size / Decimal("2")
        assert isinstance(half_step, Decimal), (
            f"Division should maintain Decimal type for {market.symbol}"
        )
        assert half_step > Decimal("0"), f"Half step should be positive for {market.symbol}"

        # Test Hyperliquid-specific decimal fields
        if market.hl_details is not None:
            if market.hl_details.mark_price is not None:
                assert market.hl_details.mark_price.is_finite(), (
                    f"mark_price should be finite for {market.symbol}"
                )
                assert not market.hl_details.mark_price.is_nan(), (
                    f"mark_price should not be NaN for {market.symbol}"
                )

            if market.hl_details.funding_rate is not None:
                assert market.hl_details.funding_rate.is_finite(), (
                    f"funding_rate should be finite for {market.symbol}"
                )
                assert not market.hl_details.funding_rate.is_nan(), (
                    f"funding_rate should not be NaN for {market.symbol}"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_vs_get_markets_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test that get_market() and get_markets() return consistent data for the same symbol."""
    # Get all markets first
    markets_args = GetMarketsArgs()
    all_markets = await hl_api_for_test_env.get_markets(markets_args)

    assert len(all_markets) > 0, "Should have at least one market for consistency testing"

    # Pick a market to test individual retrieval
    test_symbol = all_markets[0].symbol

    # Get the same market individually
    market_args = GetMarketArgs(symbol=test_symbol)
    individual_market = await hl_api_for_test_env.get_market(market_args)

    # Should not be None
    assert individual_market is not None, f"get_market should return a Market for {test_symbol}"

    # Find the matching market from the list
    matching_market = next((m for m in all_markets if m.symbol == test_symbol), None)
    assert matching_market is not None, f"Could not find market {test_symbol} in all_markets list"

    # Compare core fields for consistency
    assert individual_market.symbol == matching_market.symbol
    assert individual_market.base_symbol == matching_market.base_symbol
    assert individual_market.quote_symbol == matching_market.quote_symbol
    assert individual_market.market_type == matching_market.market_type
    assert individual_market.tick_size == matching_market.tick_size
    assert individual_market.step_size == matching_market.step_size
    assert individual_market.status == matching_market.status

    # Optional fields should also match
    assert individual_market.min_price == matching_market.min_price
    assert individual_market.max_price == matching_market.max_price
    assert individual_market.min_quantity == matching_market.min_quantity
    assert individual_market.max_quantity == matching_market.max_quantity

    # Hyperliquid-specific details should match
    if individual_market.hl_details is not None and matching_market.hl_details is not None:
        assert individual_market.hl_details.max_leverage == matching_market.hl_details.max_leverage
        assert (
            individual_market.hl_details.only_isolated == matching_market.hl_details.only_isolated
        )
        assert individual_market.hl_details.sz_decimals == matching_market.hl_details.sz_decimals
        assert individual_market.hl_details.mark_price == matching_market.hl_details.mark_price
        assert individual_market.hl_details.funding_rate == matching_market.hl_details.funding_rate


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_market_business_logic_validation(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test that Market models from Hyperliquid satisfy business logic constraints."""
    # Test with a well-known symbol
    args = GetMarketArgs(symbol="BTC")
    market = await hl_api_for_test_env.get_market(args)

    assert market is not None, "BTC market should not be None"

    # Validate Hyperliquid-specific symbol conventions
    # For Hyperliquid, the symbol is typically just the base asset name
    assert market.symbol == market.base_symbol, (
        f"For Hyperliquid, symbol should equal base_symbol, got {market.symbol} "
        f"vs {market.base_symbol}"
    )

    # Quote should always be USD for Hyperliquid
    assert market.quote_symbol == "USD", (
        f"Hyperliquid markets should always quote in USD, got {market.quote_symbol}"
    )

    # Validate trading constraints make sense
    if market.min_price is not None and market.max_price is not None:
        assert market.max_price > market.min_price, (
            f"max_price ({market.max_price}) should be greater than min_price ({market.min_price})"
        )

    if market.min_quantity is not None and market.max_quantity is not None:
        assert market.max_quantity > market.min_quantity, (
            f"max_quantity ({market.max_quantity}) should be greater than "
            f"min_quantity ({market.min_quantity})"
        )

    # Validate tick_size is reasonable for USD-denominated trading
    assert market.tick_size <= Decimal("1000"), (
        f"tick_size seems too large for USD pair: {market.tick_size}"
    )
    assert market.tick_size >= Decimal("0.000001"), f"tick_size seems too small: {market.tick_size}"

    # Step size should be reasonable for crypto derivatives
    assert market.step_size <= Decimal("1000"), f"step_size seems too large: {market.step_size}"
    assert market.step_size >= Decimal("0.000001"), f"step_size seems too small: {market.step_size}"

    # Validate Hyperliquid-specific constraints
    if market.hl_details is not None:
        # Max leverage should be within reasonable bounds for derivatives
        assert market.hl_details.max_leverage >= 1, "max_leverage should be at least 1"
        assert market.hl_details.max_leverage <= 1000, "max_leverage should not exceed 1000"

        # Size decimals should be reasonable for crypto precision
        assert market.hl_details.sz_decimals <= 18, "sz_decimals should not exceed 18"

        # If mark price is available, it should be reasonable
        if market.hl_details.mark_price is not None:
            # For BTC, mark price should be in a reasonable range
            if market.symbol == "BTC":
                assert market.hl_details.mark_price > Decimal("1000"), (
                    f"BTC mark price seems too low: {market.hl_details.mark_price}"
                )
                assert market.hl_details.mark_price < Decimal("1000000"), (
                    f"BTC mark price seems too high: {market.hl_details.mark_price}"
                )

        # Funding rate should be within reasonable bounds if provided
        if market.hl_details.funding_rate is not None:
            # Funding rates are typically small percentages
            assert abs(market.hl_details.funding_rate) <= Decimal("0.1"), (
                f"funding_rate seems extreme: {market.hl_details.funding_rate}"
            )
