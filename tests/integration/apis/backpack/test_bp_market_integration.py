"""Integration tests for Backpack Market model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_market() and
BackpackAPI.get_markets() calls to final Market internal domain models using
pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful market metadata retrieval for valid symbols
- Multiple markets retrieval and validation
- Edge cases and error handling
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
- Backpack-specific market details (bp_details extension slots)
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market.market import Market

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_sol_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with SOL_USDC returns valid Market model.

    This validates the complete pipeline:
    - API method call (get_market)
    - Request building (/api/v1/markets/{symbol} endpoint)
    - Response handling and validation
    - Mapping to internal Market model with Backpack-specific details
    """
    args = GetMarketArgs(symbol="SOL_USDC")
    market = await bp_api_for_test_env.get_market(args)

    # Validate return type
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "SOL_USDC", f"Expected symbol 'SOL_USDC', got '{market.symbol}'"
    assert market.base_symbol == "SOL", f"Expected base_symbol 'SOL', got '{market.base_symbol}'"
    assert market.quote_symbol == "USDC", (
        f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
    )

    # Validate market type
    assert isinstance(market.market_type, str), (
        f"market_type should be str, got {type(market.market_type)}"
    )
    assert len(market.market_type) > 0, "market_type should not be empty"

    # Validate tick size and step size
    assert isinstance(market.tick_size, Decimal), (
        f"tick_size should be Decimal, got {type(market.tick_size)}"
    )
    assert market.tick_size > Decimal("0"), f"tick_size should be positive, got {market.tick_size}"

    assert isinstance(market.step_size, Decimal), (
        f"step_size should be Decimal, got {type(market.step_size)}"
    )
    assert market.step_size > Decimal("0"), f"step_size should be positive, got {market.step_size}"

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
        if market.min_price is not None:
            assert market.max_price >= market.min_price, (
                f"max_price ({market.max_price}) should be >= min_price ({market.min_price})"
            )

    # Validate optional quantity limits
    if market.min_quantity is not None:
        assert isinstance(market.min_quantity, Decimal), (
            f"min_quantity should be Decimal, got {type(market.min_quantity)}"
        )
        assert market.min_quantity >= Decimal("0"), (
            f"min_quantity should be non-negative, got {market.min_quantity}"
        )

    if market.max_quantity is not None:
        assert isinstance(market.max_quantity, Decimal), (
            f"max_quantity should be Decimal, got {type(market.max_quantity)}"
        )
        assert market.max_quantity >= Decimal("0"), (
            f"max_quantity should be non-negative, got {market.max_quantity}"
        )
        if market.min_quantity is not None:
            assert market.max_quantity >= market.min_quantity, (
                f"max_quantity ({market.max_quantity}) should be >= min_quantity ({market.min_quantity})"
            )

    # Validate status
    assert isinstance(market.status, str), f"status should be str, got {type(market.status)}"
    assert len(market.status) > 0, "status should not be empty"

    # Validate optional timestamp
    if market.created_at is not None:
        from datetime import datetime

        assert isinstance(market.created_at, datetime), (
            f"created_at should be datetime, got {type(market.created_at)}"
        )

    # Validate Backpack-specific details
    if market.bp_details is not None:
        from cyberdelta.core.models.market.market import BackpackMarketDetails

        assert isinstance(market.bp_details, BackpackMarketDetails), (
            f"bp_details should be BackpackMarketDetails, got {type(market.bp_details)}"
        )

    # Validate that hl_details is None for Backpack markets
    assert market.hl_details is None, "hl_details should be None for Backpack markets"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_btc_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with BTC_USDC returns valid Market model."""
    args = GetMarketArgs(symbol="BTC_USDC")
    market = await bp_api_for_test_env.get_market(args)

    # Validate return type
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "BTC_USDC", f"Expected symbol 'BTC_USDC', got '{market.symbol}'"
    assert market.base_symbol == "BTC", f"Expected base_symbol 'BTC', got '{market.base_symbol}'"
    assert market.quote_symbol == "USDC", (
        f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
    )

    # BTC should have reasonable tick and step sizes
    assert market.tick_size <= Decimal("100"), f"BTC tick_size seems too large: {market.tick_size}"
    assert market.step_size <= Decimal("1"), f"BTC step_size seems too large: {market.step_size}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_eth_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with ETH_USDC returns valid Market model."""
    args = GetMarketArgs(symbol="ETH_USDC")
    market = await bp_api_for_test_env.get_market(args)

    # Validate return type
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "ETH_USDC", f"Expected symbol 'ETH_USDC', got '{market.symbol}'"
    assert market.base_symbol == "ETH", f"Expected base_symbol 'ETH', got '{market.base_symbol}'"
    assert market.quote_symbol == "USDC", (
        f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_perp_symbol_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with perpetual contract symbol returns valid Market model."""
    args = GetMarketArgs(symbol="SOL_USDC_PERP")
    market = await bp_api_for_test_env.get_market(args)

    # Validate return type
    assert isinstance(market, Market), f"Expected Market, got {type(market)}"

    # Validate core market fields
    assert market.symbol == "SOL_USDC_PERP", (
        f"Expected symbol 'SOL_USDC_PERP', got '{market.symbol}'"
    )
    assert market.base_symbol == "SOL", f"Expected base_symbol 'SOL', got '{market.base_symbol}'"
    assert market.quote_symbol == "USDC", (
        f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
    )

    # Perpetual contracts should have appropriate market type
    assert "perp" in market.market_type.lower() or market.market_type in ["Perpetual", "Future"], (
        f"Expected perpetual market type, got '{market.market_type}'"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with invalid symbol raises appropriate error."""
    args = GetMarketArgs(symbol="INVALID_SYMBOL")

    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_market(args)

    # Validate error details
    error = exc_info.value
    assert (
        "INVALID_SYMBOL" in str(error)
        or "symbol" in str(error).lower()
        or "not found" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_nonexistent_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market() with non-existent but well-formed symbol."""
    args = GetMarketArgs(symbol="NOTREAL_USDC")

    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_market(args)

    # Validate error details
    error = exc_info.value
    assert (
        "NOTREAL_USDC" in str(error)
        or "not found" in str(error).lower()
        or "symbol" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_markets_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_markets() returns valid list of Market models.

    This validates the complete pipeline:
    - API method call (get_markets)
    - Request building (/api/v1/markets endpoint)
    - Response handling and validation
    - Mapping to internal Market models
    """
    args = GetMarketsArgs()
    markets = await bp_api_for_test_env.get_markets(args)

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
        assert market.hl_details is None, (
            f"hl_details should be None for Backpack market {market.symbol}"
        )

    # Should include common trading pairs
    market_symbols = {market.symbol for market in markets}
    # At least some common pairs should be available (adjust based on actual Backpack offerings)
    common_pairs = {"SOL_USDC", "BTC_USDC", "ETH_USDC"}
    found_pairs = common_pairs.intersection(market_symbols)
    assert len(found_pairs) > 0, (
        f"Expected to find at least one common pair from {common_pairs}, got symbols: {sorted(market_symbols)}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_markets_data_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_markets() returns consistent data structure across markets."""
    args = GetMarketsArgs()
    markets = await bp_api_for_test_env.get_markets(args)

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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_markets_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_markets() ensures proper Decimal precision handling."""
    args = GetMarketsArgs()
    markets = await bp_api_for_test_env.get_markets(args)

    for market in markets:
        # Validate tick_size precision
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal for {market.symbol}"
        )
        assert market.tick_size.is_finite(), f"tick_size should be finite for {market.symbol}"

        # Validate step_size precision
        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal for {market.symbol}"
        )
        assert market.step_size.is_finite(), f"step_size should be finite for {market.symbol}"

        # Verify arithmetic operations work correctly with the Decimals
        doubled_tick = market.tick_size * Decimal("2")
        assert isinstance(doubled_tick, Decimal), (
            f"Arithmetic with tick_size should maintain Decimal type for {market.symbol}"
        )
        assert doubled_tick == market.tick_size + market.tick_size, (
            f"Decimal arithmetic should be consistent for {market.symbol}"
        )

        # Test optional decimal fields if present
        if market.min_price is not None:
            assert isinstance(market.min_price, Decimal), (
                f"min_price should be Decimal for {market.symbol}"
            )
            assert market.min_price.is_finite(), f"min_price should be finite for {market.symbol}"

        if market.max_price is not None:
            assert isinstance(market.max_price, Decimal), (
                f"max_price should be Decimal for {market.symbol}"
            )
            assert market.max_price.is_finite(), f"max_price should be finite for {market.symbol}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_vs_get_markets_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test that get_market() and get_markets() return consistent data for the same symbol."""
    # Get all markets first
    markets_args = GetMarketsArgs()
    all_markets = await bp_api_for_test_env.get_markets(markets_args)

    assert len(all_markets) > 0, "Should have at least one market for consistency testing"

    # Pick a market to test individual retrieval
    test_symbol = all_markets[0].symbol

    # Get the same market individually
    market_args = GetMarketArgs(symbol=test_symbol)
    individual_market = await bp_api_for_test_env.get_market(market_args)

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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/market"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_market_business_logic_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test that Market models from Backpack satisfy business logic constraints."""
    # Test with a well-known symbol
    args = GetMarketArgs(symbol="SOL_USDC")
    market = await bp_api_for_test_env.get_market(args)

    # Validate symbol parsing consistency
    if "_" in market.symbol:
        parts = market.symbol.split("_")
        if len(parts) >= 2:
            # For simple base_quote format, base should match first part
            assert market.base_symbol == parts[0], (
                f"base_symbol '{market.base_symbol}' should match first part of symbol '{parts[0]}'"
            )
            # Quote should match second part (excluding PERP suffix)
            expected_quote = parts[1] if not market.symbol.endswith("_PERP") else parts[1]
            assert market.quote_symbol == expected_quote, (
                f"quote_symbol '{market.quote_symbol}' should match expected quote '{expected_quote}'"
            )

    # Validate trading constraints make sense
    if market.min_price is not None and market.max_price is not None:
        assert market.max_price > market.min_price, (
            f"max_price ({market.max_price}) should be greater than min_price ({market.min_price})"
        )

    if market.min_quantity is not None and market.max_quantity is not None:
        assert market.max_quantity > market.min_quantity, (
            f"max_quantity ({market.max_quantity}) should be greater than min_quantity ({market.min_quantity})"
        )

    # Validate tick_size is reasonable relative to potential prices
    # For USDC pairs, tick size should typically be reasonable for dollar values
    if market.quote_symbol == "USDC":
        # Tick size should be reasonable for USD-denominated trading
        assert market.tick_size <= Decimal("1000"), (
            f"tick_size seems too large for USDC pair: {market.tick_size}"
        )
        assert market.tick_size >= Decimal("0.000001"), (
            f"tick_size seems too small: {market.tick_size}"
        )

    # Step size should be reasonable for the asset
    assert market.step_size <= Decimal("1000"), f"step_size seems too large: {market.step_size}"
    assert market.step_size >= Decimal("0.000001"), f"step_size seems too small: {market.step_size}"
