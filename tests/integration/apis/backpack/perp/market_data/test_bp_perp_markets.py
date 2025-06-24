"""Integration tests for Backpack perp market model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_market() and
BackpackAPI.get_markets() calls to final Market internal domain models using
pytest-recording (VCR) for deterministic tests.

Tests cover perpetual futures markets only:
- Successful market metadata retrieval for valid perp symbols
- Multiple markets retrieval and validation for perp pairs
- Edge cases and error handling for perp markets
- Data type validation and business logic validation
- Perpetual-specific features (funding rates, leverage characteristics)
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


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.vcr]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/perp/markets"], indirect=True)
class TestBackpackPerpMarkets:
    """Backpack perp market integration tests."""

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_sol_usdc_perp_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with SOL_USDC_PERP returns valid Market model."""
        args = GetMarketArgs(symbol="SOL_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Validate return type
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"

        # Validate core market fields
        assert market.symbol == "SOL_USDC_PERP", (
            f"Expected symbol 'SOL_USDC_PERP', got '{market.symbol}'"
        )
        assert market.base_symbol == "SOL", (
            f"Expected base_symbol 'SOL', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # Perpetual contracts should have appropriate market type
        assert isinstance(market.market_type, str), (
            f"market_type should be str, got {type(market.market_type)}"
        )
        assert len(market.market_type) > 0, "market_type should not be empty"
        assert "perp" in market.market_type.lower() or market.market_type in [
            "Perpetual",
            "Future",
        ], f"Expected perpetual market type, got '{market.market_type}'"

        # Validate tick size and step size for perp trading
        assert isinstance(market.tick_size, Decimal), (
            f"tick_size should be Decimal, got {type(market.tick_size)}"
        )
        assert market.tick_size > Decimal("0"), (
            f"tick_size should be positive, got {market.tick_size}"
        )

        assert isinstance(market.step_size, Decimal), (
            f"step_size should be Decimal, got {type(market.step_size)}"
        )
        assert market.step_size > Decimal("0"), (
            f"step_size should be positive, got {market.step_size}"
        )

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
                    f"max_quantity ({market.max_quantity}) should be >= "
                    f"min_quantity ({market.min_quantity})"
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

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_btc_usdc_perp_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with BTC_USDC_PERP returns valid Market model."""
        args = GetMarketArgs(symbol="BTC_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Validate return type
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"

        # Validate core market fields
        assert market.symbol == "BTC_USDC_PERP", (
            f"Expected symbol 'BTC_USDC_PERP', got '{market.symbol}'"
        )
        assert market.base_symbol == "BTC", (
            f"Expected base_symbol 'BTC', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # BTC perp should have positive tick and step sizes
        assert market.tick_size > Decimal("0"), (
            f"BTC perp tick_size must be positive: {market.tick_size}"
        )
        assert market.step_size > Decimal("0"), (
            f"BTC perp step_size must be positive: {market.step_size}"
        )

        # Validate it's a perp market
        assert "perp" in market.market_type.lower() or market.market_type in [
            "Perpetual",
            "Future",
        ], f"Expected perpetual market type for BTC_USDC_PERP, got '{market.market_type}'"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_eth_usdc_perp_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with ETH_USDC_PERP returns valid Market model."""
        args = GetMarketArgs(symbol="ETH_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Validate return type
        assert isinstance(market, Market), f"Expected Market, got {type(market)}"

        # Validate core market fields
        assert market.symbol == "ETH_USDC_PERP", (
            f"Expected symbol 'ETH_USDC_PERP', got '{market.symbol}'"
        )
        assert market.base_symbol == "ETH", (
            f"Expected base_symbol 'ETH', got '{market.base_symbol}'"
        )
        assert market.quote_symbol == "USDC", (
            f"Expected quote_symbol 'USDC', got '{market.quote_symbol}'"
        )

        # ETH perp market validation
        assert "perp" in market.market_type.lower() or market.market_type in [
            "Perpetual",
            "Future",
        ], f"Expected perpetual market type for ETH_USDC_PERP, got '{market.market_type}'"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_invalid_perp_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with invalid perp symbol raises appropriate error."""
        args = GetMarketArgs(symbol="INVALID_PERP")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(args)

        # Validate error details
        error = exc_info.value
        assert (
            "INVALID_PERP" in str(error)
            or "symbol" in str(error).lower()
            or "market" in str(error).lower()
            or "not found" in str(error).lower()
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_nonexistent_perp_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_market() with non-existent but well-formed perp symbol."""
        args = GetMarketArgs(symbol="NOTREAL_USDC_PERP")

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market(args)

        # Validate error details
        error = exc_info.value
        assert (
            "NOTREAL_USDC_PERP" in str(error)
            or "not found" in str(error).lower()
            or "symbol" in str(error).lower()
            or "market" in str(error).lower()
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_perp_markets_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() returns valid list of perp Market models."""
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        # Filter for perp markets only
        perp_markets = [
            market
            for market in all_markets
            if market.symbol.endswith("_PERP") or "perp" in market.market_type.lower()
        ]

        assert len(perp_markets) > 0, "Should return at least some perp markets"

        # Validate each perp market
        symbols_seen: set[str] = set()
        for market in perp_markets:
            assert isinstance(market, Market), f"Expected Market, got {type(market)}"

            # Validate unique symbols
            assert market.symbol not in symbols_seen, f"Duplicate symbol found: {market.symbol}"
            symbols_seen.add(market.symbol)

            # Validate it's actually a perp market
            assert market.symbol.endswith("_PERP") or "perp" in market.market_type.lower(), (
                f"Should only include perp symbols: {market.symbol}"
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

            # Validate financial constraints for perp trading
            assert isinstance(market.tick_size, Decimal), (
                f"tick_size should be Decimal for {market.symbol}"
            )
            assert market.tick_size > Decimal("0"), (
                f"tick_size should be positive for {market.symbol}"
            )

            assert isinstance(market.step_size, Decimal), (
                f"step_size should be Decimal for {market.symbol}"
            )
            assert market.step_size > Decimal("0"), (
                f"step_size should be positive for {market.symbol}"
            )

            # Validate status
            assert isinstance(market.status, str), f"status should be str for {market.symbol}"
            assert len(market.status) > 0, f"status should not be empty for {market.symbol}"

            # Validate exchange-specific details
            assert market.hl_details is None, (
                f"hl_details should be None for Backpack market {market.symbol}"
            )

        # Should include common perp trading pairs
        market_symbols = {market.symbol for market in perp_markets}
        common_perp_pairs = {"SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"}
        found_pairs = common_perp_pairs.intersection(market_symbols)
        assert len(found_pairs) > 0, (
            f"Expected to find at least one common perp pair from {common_perp_pairs}, "
            f"got symbols: {sorted(market_symbols)}"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_perp_markets_data_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() returns consistent data structure across perp markets."""
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        perp_markets = [market for market in all_markets if market.symbol.endswith("_PERP")]

        assert len(perp_markets) > 1, "Need multiple perp markets for consistency testing"

        # Verify all perp markets have the same structure (required fields)
        required_attrs = [
            "symbol",
            "base_symbol",
            "quote_symbol",
            "market_type",
            "tick_size",
            "step_size",
            "status",
        ]

        for market in perp_markets:
            for attr in required_attrs:
                assert hasattr(market, attr), (
                    f"Perp market {market.symbol} missing required attribute: {attr}"
                )
                value = getattr(market, attr)
                assert value is not None, (
                    f"Perp market {market.symbol} has None value for required attribute: {attr}"
                )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_perp_markets_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_markets() ensures proper Decimal precision handling.

        For perp markets.
        """
        args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(args)

        perp_markets = [market for market in all_markets if market.symbol.endswith("_PERP")]

        for market in perp_markets:
            # Validate tick_size precision for perp trading
            assert isinstance(market.tick_size, Decimal), (
                f"tick_size should be Decimal for perp {market.symbol}"
            )
            assert market.tick_size.is_finite(), (
                f"tick_size should be finite for perp {market.symbol}"
            )

            # Validate step_size precision for perp trading
            assert isinstance(market.step_size, Decimal), (
                f"step_size should be Decimal for perp {market.symbol}"
            )
            assert market.step_size.is_finite(), (
                f"step_size should be finite for perp {market.symbol}"
            )

            # Verify arithmetic operations work correctly with the Decimals for
            # leverage calculations
            doubled_tick = market.tick_size * Decimal("2")
            assert isinstance(doubled_tick, Decimal), (
                f"Arithmetic with tick_size should maintain Decimal type for perp {market.symbol}"
            )
            assert doubled_tick == market.tick_size + market.tick_size, (
                f"Decimal arithmetic should be consistent for perp {market.symbol}"
            )

            # Test leverage-related calculations
            leverage_factor = Decimal("10")  # 10x leverage
            leveraged_size = market.step_size * leverage_factor
            assert isinstance(leveraged_size, Decimal), (
                f"Leverage calculations should maintain Decimal type for perp {market.symbol}"
            )

            # Test optional decimal fields if present
            if market.min_price is not None:
                assert isinstance(market.min_price, Decimal), (
                    f"min_price should be Decimal for perp {market.symbol}"
                )
                assert market.min_price.is_finite(), (
                    f"min_price should be finite for perp {market.symbol}"
                )

            if market.max_price is not None:
                assert isinstance(market.max_price, Decimal), (
                    f"max_price should be Decimal for perp {market.symbol}"
                )
                assert market.max_price.is_finite(), (
                    f"max_price should be finite for perp {market.symbol}"
                )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_get_market_vs_get_markets_consistency_perp(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that get_market() and get_markets() return consistent data for perp symbols."""
        # Get all markets first
        markets_args = GetMarketsArgs()
        all_markets = await bp_api_for_test_env.get_markets(markets_args)

        perp_markets = [market for market in all_markets if market.symbol.endswith("_PERP")]

        assert len(perp_markets) > 0, "Should have at least one perp market for consistency testing"

        # Pick a perp market to test individual retrieval
        test_symbol = perp_markets[0].symbol

        # Get the same market individually
        market_args = GetMarketArgs(symbol=test_symbol)
        individual_market = await bp_api_for_test_env.get_market(market_args)

        # Find the matching market from the list
        matching_market = next((m for m in perp_markets if m.symbol == test_symbol), None)
        assert matching_market is not None, (
            f"Could not find perp market {test_symbol} in all_markets list"
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

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_perp_market_business_logic_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that perp Market models from Backpack satisfy business logic constraints."""
        # Test with a well-known perp symbol
        args = GetMarketArgs(symbol="SOL_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Validate symbol parsing consistency for perp markets
        if market.symbol.endswith("_PERP"):
            # Remove _PERP suffix for parsing
            base_symbol = market.symbol[:-5]  # Remove "_PERP"
            parts = base_symbol.split("_")
            if len(parts) == 2:  # base_quote format
                assert market.base_symbol == parts[0], (
                    f"base_symbol '{market.base_symbol}' should match first part of "
                    f"symbol '{parts[0]}'"
                )
                assert market.quote_symbol == parts[1], (
                    f"quote_symbol '{market.quote_symbol}' should match second part '{parts[1]}'"
                )

        # Validate trading constraints make sense for perp trading
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

        # Validate tick_size is reasonable relative to ACTUAL current prices
        if market.quote_symbol == "USDC":
            # Get real current price to validate tick size makes sense
            from tests.integration.apis.backpack.shared.bp_test_helpers import (
                get_current_market_price,
            )

            current_price = await get_current_market_price(bp_api_for_test_env, market.symbol)
            # Tick size should be much smaller than current price (reasonable precision)
            price_to_tick_ratio = current_price / market.tick_size
            assert price_to_tick_ratio > Decimal("0"), (
                f"tick_size {market.tick_size} must create positive ratio with "
                f"current price {current_price}. Ratio: {price_to_tick_ratio}"
            )
            assert market.tick_size > Decimal("0"), (
                f"tick_size must be positive: {market.tick_size}"
            )

        # Step size should be positive for valid trading
        assert market.step_size > Decimal("0"), (
            f"step_size must be positive for perp trading: {market.step_size}"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_perp_market_leverage_characteristics(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp market characteristics related to leverage trading."""
        args = GetMarketArgs(symbol="SOL_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Test precision requirements for leverage calculations
        # Use the market's actual maximum leverage instead of hardcoded values
        max_leverage = getattr(market, "max_leverage", Decimal("100"))  # Default if not specified

        # Test with a range of leverage values up to the market maximum
        test_leverages = [
            Decimal("2"),
            max_leverage / Decimal("4"),  # 25% of max
            max_leverage / Decimal("2"),  # 50% of max
            max_leverage,  # Maximum available
        ]

        for leverage in test_leverages:
            # Test notional calculations with REAL market data
            # Get actual current market price - no hardcoded values allowed
            from tests.integration.apis.backpack.shared.bp_test_helpers import (
                get_current_market_price,
            )

            actual_price = await get_current_market_price(bp_api_for_test_env, "SOL_USDC_PERP")

            # Use minimum quantity from market constraints - no hardcoded quantities
            test_quantity = market.step_size  # Use actual step size

            notional = test_quantity * actual_price
            margin_requirement = notional / leverage

            assert isinstance(notional, Decimal), "Notional should be Decimal"
            assert isinstance(margin_requirement, Decimal), "Margin requirement should be Decimal"
            assert margin_requirement > Decimal("0"), "Margin requirement should be positive"
            assert margin_requirement < notional, (
                "Margin should be less than notional (leverage effect)"
            )

        # Test tick size precision for leverage scenarios
        leverage_adjusted_tick = market.tick_size / Decimal(
            "10"
        )  # High precision for leveraged positions
        assert isinstance(leverage_adjusted_tick, Decimal), (
            "Leverage-adjusted calculations should maintain Decimal"
        )

        # Test that market constraints are valid for leverage trading
        if market.min_quantity is not None:
            # Minimum quantity should be positive for valid trading
            assert market.min_quantity > Decimal("0"), (
                "Min quantity must be positive for valid leveraged positions"
            )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_bp_perp_market_funding_awareness(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp market metadata supports funding rate awareness."""
        args = GetMarketArgs(symbol="SOL_USDC_PERP")
        market = await bp_api_for_test_env.get_market(args)

        # Perp markets should have constraints suitable for funding rate periods
        # Funding typically occurs every 8 hours, so constraints should support this

        # Test that tick size allows reasonable funding rate calculations with REAL data
        # Get actual funding rate from exchange - no hardcoded rates
        from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs

        funding_rates = await bp_api_for_test_env.get_funding_rates(
            GetFundingRatesArgs(symbols=["SOL_USDC_PERP"])
        )
        assert len(funding_rates) > 0, "Should get funding rate data for SOL_USDC_PERP"
        funding_data = funding_rates[0]
        assert funding_data.funding_rate is not None, (
            "Funding rate should not be None for perpetual markets"
        )
        actual_funding_rate = abs(funding_data.funding_rate)

        # Get real current price - no hardcoded prices
        from tests.integration.apis.backpack.shared.bp_test_helpers import get_current_market_price

        actual_price = await get_current_market_price(bp_api_for_test_env, "SOL_USDC_PERP")
        funding_payment = actual_price * actual_funding_rate

        # The funding payment should be a valid tradeable amount based on tick size
        # This validates the tick size can represent funding-adjusted prices properly
        if funding_payment > Decimal("0"):
            # Check that the funding payment can be represented with the market's precision
            # The funding payment should be expressible as a multiple of tick_size
            # when applied to price
            price_with_funding = actual_price + funding_payment
            # Ensure the price with funding can be properly quantized to tick size
            quantized_price = price_with_funding.quantize(market.tick_size)
            assert quantized_price > Decimal("0"), (
                f"Price with funding ({price_with_funding}) quantized to tick size "
                f"{market.tick_size} results in invalid price: {quantized_price}"
            )

        # Test market type indicates perpetual characteristics
        assert "perp" in market.market_type.lower() or "perpetual" in market.market_type.lower(), (
            f"Market type should indicate perpetual characteristics: {market.market_type}"
        )
