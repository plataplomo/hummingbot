"""Integration tests for Backpack spot market model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_market() and
BackpackAPI.get_markets() calls to final Market internal domain models using
pytest-recording (VCR) for deterministic tests.

Tests cover spot markets only:
- Successful market metadata retrieval for valid spot symbols
- Multiple markets retrieval and validation for spot pairs
- Edge cases and error handling for spot markets
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
- Backpack-specific market details (bp_details extension slots)
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market.market import BackpackMarketDetails, Market


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.vcr]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/spot/markets"], indirect=True)
class TestBackpackSpotMarkets:
    """Backpack spot market integration tests."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_sol_usdc_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with SOL_USDC returns valid Market model."""
        args = GetMarketArgs(symbol="SOL_USDC")
        market = await bp_api_for_test_env.get_market(args)

        # Validate return type
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"

        # Validate core market fields
        assert market.symbol == "SOL_USDC", f"Expected symbol 'SOL_USDC', got '{market.symbol}'"
        assert market.base_symbol == "SOL", (
            f"Expected base_symbol 'SOL', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # Validate market type (should be spot)
        assert isinstance(market.market_type, str), (
            f"market_type should be str, got {type(market.market_type)}"
        )
        assert len(market.market_type) > 0, "market_type should not be empty"
        assert "spot" in market.market_type.lower() or market.market_type in ["Spot", "SPOT"], (
            f"Expected spot market type, got '{market.market_type}'"
        )

        # Validate tick size and step size
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal, got {type(market.tick_size)}"
        )
        assert market.tick_size > Decimal(0), (
            f"tick_size should be positive, got {market.tick_size}"
        )

        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal, got {type(market.step_size)}"
        )
        assert market.step_size > Decimal(0), (
            f"step_size should be positive, got {market.step_size}"
        )

        # Validate optional price limits
        if market.min_price is not None:
            assert isinstance(market.min_price, Decimal), (
                f"min_price should be Decimal, got {type(market.min_price)}"
            )
            assert market.min_price >= Decimal(0), (
                f"min_price should be non-negative, got {market.min_price}"
            )

        if market.max_price is not None:
            assert isinstance(market.max_price, Decimal), (
                f"max_price should be Decimal, got {type(market.max_price)}"
            )
            assert market.max_price >= Decimal(0), (
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
            assert market.min_quantity >= Decimal(0), (
                f"min_quantity should be non-negative, got {market.min_quantity}"
            )

        if market.max_quantity is not None:
            assert isinstance(market.max_quantity, Decimal), (
                f"max_quantity should be Decimal, got {type(market.max_quantity)}"
            )
            assert market.max_quantity >= Decimal(0), (
                f"max_quantity should be non-negative, got {market.max_quantity}"
            )
            if market.min_quantity is not None:
                assert market.max_quantity >= market.min_quantity, (
                    f"max_quantity ({market.max_quantity}) should be >= "
                    f"min_quantity ({market.min_quantity})"
                )

        # Validate status
        assert isinstance(market.status, str), f"status should be str, got {type(market.status)}"
        assert len(market.status) > 0, "status should not be empty"

        # Validate optional timestamp
        if market.created_at is not None:
            assert isinstance(market.created_at, datetime), (
                f"created_at should be datetime, got {type(market.created_at)}"
            )

        # Validate Backpack-specific details
        if market.bp_details is not None:
            assert isinstance(market.bp_details, BackpackMarketDetails), (
                f"bp_details should be BackpackMarketDetails, got {type(market.bp_details)}"
            )

        # Validate that hl_details is None for Backpack markets
        assert market.hl_details is None, "hl_details should be None for Backpack markets"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_btc_usdc_success(
        self,
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
        assert market.base_symbol == "BTC", (
            f"Expected base_symbol 'BTC', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # BTC should have reasonable tick and step sizes for spot trading
        assert market.tick_size <= Decimal(100), (
            f"BTC spot tick_size seems too large: {market.tick_size}"
        )
        assert market.step_size <= Decimal(1), (
            f"BTC spot step_size seems too large: {market.step_size}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_eth_usdc_success(
        self,
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
        assert market.base_symbol == "ETH", (
            f"Expected base_symbol 'ETH', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # ETH spot market validation
        assert "spot" in market.market_type.lower() or market.market_type in ["Spot", "SPOT"], (
            f"Expected spot market type for ETH_USDC, got '{market.market_type}'"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_invalid_spot_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with invalid spot symbol raises appropriate error."""
        args = GetMarketArgs(symbol="INVALID_SPOT_SYMBOL")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(args)

        # Validate error details
        error = exc_info.value
        assert (
            "INVALID_SPOT_SYMBOL" in str(error)
            or "symbol" in str(error).lower()
            or "market" in str(error).lower()
            or "not found" in str(error).lower()
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_nonexistent_spot_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with non-existent but well-formed spot symbol."""
        args = GetMarketArgs(symbol="NOTREAL_USDC")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(args)

        # Validate error details
        error = exc_info.value
        assert (
            "NOTREAL_USDC" in str(error)
            or "not found" in str(error).lower()
            or "symbol" in str(error).lower()
            or "market" in str(error).lower()
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_spot_markets_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() returns valid list of spot Market models."""
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        # Filter for spot markets only
        spot_markets = [
            market
            for market in all_markets
            if not market.symbol.endswith("_PERP") and "perp" not in market.market_type.lower()
        ]

        assert len(spot_markets) > 0, "Should return at least some spot markets"

        # Validate each spot market
        symbols_seen: set[str] = set()
        for market in spot_markets:
            assert isinstance(market, Market), f"Expected Market, got {type(market)}"

            # Validate unique symbols
            assert market.symbol not in symbols_seen, f"Duplicate symbol found: {market.symbol}"
            symbols_seen.add(market.symbol)

            # Validate it's actually a spot market
            assert not market.symbol.endswith("_PERP"), (
                f"Should not include perp symbols: {market.symbol}"
            )

            # Validate core fields are present and valid
            assert isinstance(market.symbol, str), (
                f"symbol should be str, got {type(market.symbol)}"
            )
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
            assert market.tick_size > Decimal(0), (
                f"tick_size should be positive for {market.symbol}"
            )

            assert isinstance(market.step_size, Decimal), (
                f"step_size should be Decimal for {market.symbol}"
            )
            assert market.step_size > Decimal(0), (
                f"step_size should be positive for {market.symbol}"
            )

            # Validate status
            assert isinstance(market.status, str), f"status should be str for {market.symbol}"
            assert len(market.status) > 0, f"status should not be empty for {market.symbol}"

            # Validate exchange-specific details
            assert market.hl_details is None, (
                f"hl_details should be None for Backpack market {market.symbol}"
            )

        # Should include common spot trading pairs
        market_symbols = {market.symbol for market in spot_markets}
        common_spot_pairs = {"SOL_USDC", "BTC_USDC", "ETH_USDC"}
        found_pairs = common_spot_pairs.intersection(market_symbols)
        assert len(found_pairs) > 0, (
            f"Expected to find at least one common spot pair from {common_spot_pairs}, "
            f"got symbols: {sorted(market_symbols)}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_spot_markets_data_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() returns consistent data structure across spot markets."""
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        spot_markets = [market for market in all_markets if not market.symbol.endswith("_PERP")]

        assert len(spot_markets) > 1, "Need multiple spot markets for consistency testing"

        # Verify all spot markets have the same structure (required fields)
        required_attrs = [
            "symbol",
            "base_symbol",
            "quote_symbol",
            "market_type",
            "tick_size",
            "step_size",
            "status",
        ]

        for market in spot_markets:
            for attr in required_attrs:
                assert hasattr(market, attr), (
                    f"Spot market {market.symbol} missing required attribute: {attr}"
                )
                value = getattr(market, attr)
                assert value is not None, (
                    f"Spot market {market.symbol} has None value for required attribute: {attr}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_spot_markets_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() ensures proper Decimal precision handling.

        For spot markets.
        """
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        spot_markets = [market for market in all_markets if not market.symbol.endswith("_PERP")]

        for market in spot_markets:
            # Validate tick_size precision
            assert isinstance(market.tick_size, Decimal), (
                f"tick_size should be Decimal for spot {market.symbol}"
            )
            assert market.tick_size.is_finite(), (
                f"tick_size should be finite for spot {market.symbol}"
            )

            # Validate step_size precision
            assert isinstance(market.step_size, Decimal), (
                f"step_size should be Decimal for spot {market.symbol}"
            )
            assert market.step_size.is_finite(), (
                f"step_size should be finite for spot {market.symbol}"
            )

            # Verify arithmetic operations work correctly with the Decimals
            doubled_tick = market.tick_size * Decimal(2)
            assert isinstance(doubled_tick, Decimal), (
                f"Arithmetic with tick_size should maintain Decimal type for spot {market.symbol}"
            )
            assert doubled_tick == market.tick_size + market.tick_size, (
                f"Decimal arithmetic should be consistent for spot {market.symbol}"
            )

            # Test optional decimal fields if present
            if market.min_price is not None:
                assert isinstance(market.min_price, Decimal), (
                    f"min_price should be Decimal for spot {market.symbol}"
                )
                assert market.min_price.is_finite(), (
                    f"min_price should be finite for spot {market.symbol}"
                )

            if market.max_price is not None:
                assert isinstance(market.max_price, Decimal), (
                    f"max_price should be Decimal for spot {market.symbol}"
                )
                assert market.max_price.is_finite(), (
                    f"max_price should be finite for spot {market.symbol}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_vs_get_markets_consistency_spot(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that get_market() and get_markets() return consistent data for spot symbols."""
        # Get all markets first
        markets_args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(markets_args)

        spot_markets = [market for market in all_markets if not market.symbol.endswith("_PERP")]

        assert len(spot_markets) > 0, "Should have at least one spot market for consistency testing"

        # Pick a spot market to test individual retrieval
        test_symbol = spot_markets[0].symbol

        # Get the same market individually
        market_args = GetMarketArgs(symbol=test_symbol)
        individual_market = await bp_api_for_test_env.get_market(market_args)

        # Find the matching market from the list
        matching_market = next((m for m in spot_markets if m.symbol == test_symbol), None)
        assert matching_market is not None, (
            f"Could not find spot market {test_symbol} in all_markets list"
        )

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

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_spot_market_business_logic_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that spot Market models from Backpack satisfy business logic constraints."""
        # Test with a well-known spot symbol
        args = GetMarketArgs(symbol="SOL_USDC")
        market = await bp_api_for_test_env.get_market(args)

        # Validate symbol parsing consistency for spot markets
        if "_" in market.symbol and not market.symbol.endswith("_PERP"):
            parts = market.symbol.split("_")
            if len(parts) == 2:  # Simple base_quote format for spot
                assert market.base_symbol == parts[0], (
                    f"base_symbol '{market.base_symbol}' should match first part of "
                    f"symbol '{parts[0]}'"
                )
                assert market.quote_symbol == parts[1], (
                    f"quote_symbol '{market.quote_symbol}' should match second part '{parts[1]}'"
                )

        # Validate trading constraints make sense
        if market.min_price is not None and market.max_price is not None:
            assert market.max_price > market.min_price, (
                f"max_price ({market.max_price}) should be greater than "
                f"min_price ({market.min_price})"
            )

        if market.min_quantity is not None and market.max_quantity is not None:
            assert market.max_quantity > market.min_quantity, (
                f"max_quantity ({market.max_quantity}) should be greater than "
                f"min_quantity ({market.min_quantity})"
            )

        # Validate tick_size is reasonable relative to potential prices for spot
        if market.quote_symbol == "USDC":
            # Tick size should be reasonable for USD-denominated spot trading
            assert market.tick_size <= Decimal(1000), (
                f"tick_size seems too large for USDC spot pair: {market.tick_size}"
            )
            assert market.tick_size >= Decimal("0.000001"), (
                f"tick_size seems too small: {market.tick_size}"
            )

        # Step size should be reasonable for spot asset trading
        assert market.step_size <= Decimal(1000), (
            f"step_size seems too large for spot: {market.step_size}"
        )
        assert market.step_size >= Decimal("0.000001"), (
            f"step_size seems too small for spot: {market.step_size}"
        )
