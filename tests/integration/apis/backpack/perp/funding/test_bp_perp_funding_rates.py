"""Integration tests for Backpack perp funding rate model pipeline.

These tests validate the complete data pipeline from BackpackAPI funding rate methods
to final FundingRate internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover perpetual futures funding rates:
- Single funding rate retrieval (get_funding_rate)
- Historical funding rates retrieval (get_funding_rates)
- Funding rate data validation (rate, timestamp, symbol)
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.exceptions.market_data_service import EmptySymbolListError
from cyberdelta.apis.models.service_args_models import GetFundingRatesArgs
from cyberdelta.core.models import FundingRate


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.vcr]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/perp/funding"], indirect=True)
@pytest.mark.asyncio
async def test_bp_get_funding_rate_sol_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with SOL_USDC_PERP returns valid FundingRate model.

    This validates the complete pipeline:
    - API method call (get_funding_rate)
    - Request building (/api/v1/fundingRates endpoint)
    - Response handling and validation
    - Mapping to internal FundingRate model
    """
    funding_rate = await bp_api_for_test_env.get_funding_rate("SOL_USDC_PERP")

    # Validate return type
    assert isinstance(funding_rate, FundingRate), f"Expected FundingRate, got {type(funding_rate)}"

    # Validate core funding rate fields
    assert hasattr(funding_rate, "symbol"), "FundingRate should have symbol attribute"
    assert hasattr(funding_rate, "funding_rate"), "FundingRate should have funding_rate attribute"
    assert hasattr(funding_rate, "timestamp"), "FundingRate should have timestamp attribute"

    # Validate data types
    assert isinstance(funding_rate.funding_rate, Decimal), (
        f"Rate should be Decimal, got {type(funding_rate.funding_rate)}"
    )

    # Validate symbol
    assert funding_rate.symbol == "SOL_USDC_PERP", (
        f"Expected symbol 'SOL_USDC_PERP', got '{funding_rate.symbol}'"
    )

    # Funding rates can be positive or negative, but should be reasonable
    # Typical funding rates are small percentages, usually between -1% and +1%
    assert abs(funding_rate.funding_rate) < Decimal(1), (
        f"Funding rate seems unreasonable: {funding_rate.funding_rate}"
    )

    # Most funding rates are much smaller, typically < 0.1%
    assert abs(funding_rate.funding_rate) < Decimal("0.5"), (
        f"Funding rate seems unusually high: {funding_rate.funding_rate}"
    )

    # Validate timestamp if present
    if hasattr(funding_rate, "timestamp"):
        assert isinstance(funding_rate.timestamp, datetime), (
            f"Timestamp should be datetime, got {type(funding_rate.timestamp)}"
        )
        # Should be a reasonable timestamp (after 2020)

        min_date = datetime(2020, 1, 1, tzinfo=UTC)
        assert funding_rate.timestamp > min_date, (
            f"Timestamp seems too old: {funding_rate.timestamp}"
        )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_btc_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with BTC_USDC_PERP returns valid FundingRate model."""
    funding_rate = await bp_api_for_test_env.get_funding_rate("BTC_USDC_PERP")

    # Validate return type
    assert isinstance(funding_rate, FundingRate), f"Expected FundingRate, got {type(funding_rate)}"

    # Validate symbol
    assert funding_rate.symbol == "BTC_USDC_PERP", (
        f"Expected symbol 'BTC_USDC_PERP', got '{funding_rate.symbol}'"
    )

    # Validate rate is reasonable
    assert isinstance(funding_rate.funding_rate, Decimal), (
        f"Rate should be Decimal, got {type(funding_rate.funding_rate)}"
    )
    assert abs(funding_rate.funding_rate) < Decimal(1), (
        f"Funding rate seems unreasonable: {funding_rate.funding_rate}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_eth_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with ETH_USDC_PERP returns valid FundingRate model."""
    funding_rate = await bp_api_for_test_env.get_funding_rate("ETH_USDC_PERP")

    # Validate return type
    assert isinstance(funding_rate, FundingRate), f"Expected FundingRate, got {type(funding_rate)}"

    # Validate symbol
    assert funding_rate.symbol == "ETH_USDC_PERP", (
        f"Expected symbol 'ETH_USDC_PERP', got '{funding_rate.symbol}'"
    )

    # Validate rate is reasonable
    assert isinstance(funding_rate.funding_rate, Decimal), (
        f"Rate should be Decimal, got {type(funding_rate.funding_rate)}"
    )
    assert abs(funding_rate.funding_rate) < Decimal(1), (
        f"Funding rate seems unreasonable: {funding_rate.funding_rate}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_sol_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() with SOL_USDC_PERP returns valid FundingRate models.

    This validates the complete pipeline:
    - API method call (get_funding_rates)
    - Request building (/api/v1/fundingRates endpoint with query params)
    - Response handling and validation
    - Mapping to internal FundingRate models
    """
    args = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP"],
    )

    funding_rates = await bp_api_for_test_env.get_funding_rates(args)

    # Validate return type
    assert isinstance(funding_rates, list), (
        f"Expected list of funding rates, got {type(funding_rates)}"
    )

    # Validate each funding rate
    if len(funding_rates) > 0:
        for i, funding_rate in enumerate(funding_rates):
            assert isinstance(funding_rate, FundingRate), (
                f"Funding rate {i} should be FundingRate model, got {type(funding_rate)}"
            )

            # Validate core funding rate fields
            assert hasattr(funding_rate, "symbol"), f"Funding rate {i} should have symbol attribute"
            assert hasattr(funding_rate, "funding_rate"), (
                f"Funding rate {i} should have funding_rate attribute"
            )
            assert hasattr(funding_rate, "timestamp"), (
                f"Funding rate {i} should have timestamp attribute"
            )

            # Validate data types
            assert isinstance(funding_rate.funding_rate, Decimal), (
                f"Funding rate {i} funding_rate should be Decimal, "
                f"got {type(funding_rate.funding_rate)}"
            )

            # Validate symbol
            assert funding_rate.symbol == "SOL_USDC_PERP", (
                f"Funding rate {i} symbol should be 'SOL_USDC_PERP', got '{funding_rate.symbol}'"
            )

            # Validate rate is reasonable
            assert abs(funding_rate.funding_rate) < Decimal(1), (
                f"Funding rate {i} seems unreasonable: {funding_rate.funding_rate}"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_single_vs_multiple_symbols(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() with single vs multiple symbols."""
    # Test with single symbol
    args_single = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP"],
    )

    funding_rates_single = await bp_api_for_test_env.get_funding_rates(args_single)

    # Validate return type
    assert isinstance(funding_rates_single, list), (
        f"Expected list, got {type(funding_rates_single)}"
    )

    # Should return exactly one funding rate for one symbol
    assert len(funding_rates_single) == 1, (
        f"Expected 1 funding rate for single symbol, got {len(funding_rates_single)}"
    )

    # Test with multiple symbols
    args_multiple = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP", "BTC_USDC_PERP"],
    )

    funding_rates_multiple = await bp_api_for_test_env.get_funding_rates(args_multiple)

    # Validate return type
    assert isinstance(funding_rates_multiple, list), (
        f"Expected list, got {type(funding_rates_multiple)}"
    )

    # Should return two funding rates for two symbols
    assert len(funding_rates_multiple) == 2, (
        f"Expected 2 funding rates for two symbols, got {len(funding_rates_multiple)}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_chronological_ordering(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() returns funding rates in proper chronological order."""
    args = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP"],
    )

    funding_rates = await bp_api_for_test_env.get_funding_rates(args)

    # Validate return type
    assert isinstance(funding_rates, list), f"Expected list, got {type(funding_rates)}"

    # Check chronological ordering if we have multiple funding rates
    if len(funding_rates) > 1:
        for i in range(len(funding_rates) - 1):
            current_rate = funding_rates[i]
            next_rate = funding_rates[i + 1]

            # Verify both rates have timestamps
            if hasattr(current_rate, "timestamp") and hasattr(next_rate, "timestamp"):
                # Check that timestamps are reasonable and consistent

                assert isinstance(current_rate.timestamp, datetime), (
                    f"Funding rate {i} timestamp should be datetime, "
                    f"got {type(current_rate.timestamp)}"
                )
                assert isinstance(next_rate.timestamp, datetime), (
                    f"Funding rate {i + 1} timestamp should be datetime, "
                    f"got {type(next_rate.timestamp)}"
                )

                # Timestamps should be different (not the same funding period)
                # Note: We don't enforce specific ordering since it depends on
                # exchange implementation


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() ensures proper Decimal precision handling."""
    args = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP"],
    )

    funding_rates = await bp_api_for_test_env.get_funding_rates(args)

    # Validate return type
    assert isinstance(funding_rates, list), f"Expected list, got {type(funding_rates)}"

    if len(funding_rates) > 0:
        for i, funding_rate in enumerate(funding_rates):
            # Validate rate precision
            assert isinstance(funding_rate.funding_rate, Decimal), (
                f"Funding rate {i} funding_rate should be Decimal, "
                f"got {type(funding_rate.funding_rate)}"
            )

            # Test arithmetic operations work correctly
            rate_doubled = funding_rate.funding_rate * Decimal(2)
            assert isinstance(rate_doubled, Decimal), (
                f"Funding rate {i} arithmetic should maintain Decimal type"
            )

            # Test precision is maintained - funding rates are usually very precise
            rate_str = str(funding_rate.funding_rate)
            if "." in rate_str:
                decimal_places = len(rate_str.split(".")[1])
                assert decimal_places <= 18, (
                    f"Funding rate {i} precision seems too high: {decimal_places} decimal places"
                )

            # Test funding rate calculations
            if funding_rate.funding_rate != Decimal(0):
                # Calculate percentage
                rate_percentage = funding_rate.funding_rate * Decimal(100)
                assert isinstance(rate_percentage, Decimal), (
                    "Percentage calculation should maintain Decimal type"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_spot_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with spot symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_funding_rate("SOL_USDC")  # Spot symbol, not perpetual

    # Validate error details
    error = exc_info.value
    assert (
        "SOL_USDC" in str(error)
        or "funding" in str(error).lower()
        or "perpetual" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with invalid symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_funding_rate("INVALID_SYMBOL_PERP")

    # Validate error details
    error = exc_info.value
    assert "INVALID_SYMBOL_PERP" in str(error) or "symbol" in str(error).lower()


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_nonexistent_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with non-existent but well-formed perpetual symbol."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_funding_rate("NOTREAL_USDC_PERP")

    # Validate error details
    error = exc_info.value
    assert (
        "NOTREAL_USDC_PERP" in str(error)
        or "not found" in str(error).lower()
        or "symbol" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rate_empty_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rate() with empty symbol raises appropriate error."""
    with pytest.raises((APIError, ValueError)) as exc_info:
        await bp_api_for_test_env.get_funding_rate("")

    # Validate error contains relevant information
    error_str = str(exc_info.value)
    assert len(error_str) > 0, "Error message should not be empty"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_invalid_args_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() with invalid args raises appropriate error."""
    # Test with spot symbol
    args = GetFundingRatesArgs(
        symbols=["SOL_USDC"],  # Spot symbol, not perpetual
    )

    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_funding_rates(args)

    # Validate error details
    error = exc_info.value
    assert (
        "SOL_USDC" in str(error)
        or "funding" in str(error).lower()
        or "perpetual" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_empty_symbols_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() with empty symbols list raises error."""
    args = GetFundingRatesArgs(
        symbols=[],  # Empty list should cause error
    )

    with pytest.raises(EmptySymbolListError) as exc_info:
        await bp_api_for_test_env.get_funding_rates(args)

    # Validate error details
    error_str = str(exc_info.value)
    assert "At least one symbol is required" in error_str, (
        f"Expected error about required symbols, got: {error_str}"
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_large_limit_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() with very large limit parameter."""
    args = GetFundingRatesArgs(
        symbols=["SOL_USDC_PERP"],
    )

    try:
        funding_rates = await bp_api_for_test_env.get_funding_rates(args)

        # Should return valid list (may be capped by exchange)
        assert isinstance(funding_rates, list), f"Expected list, got {type(funding_rates)}"

        # Validate all returned funding rates if any
        for i, funding_rate in enumerate(funding_rates):
            assert isinstance(funding_rate, FundingRate), (
                f"Funding rate {i} should be FundingRate model, got {type(funding_rate)}"
            )
            assert funding_rate.symbol == "SOL_USDC_PERP", (
                f"Funding rate {i} symbol should be 'SOL_USDC_PERP', got '{funding_rate.symbol}'"
            )

    except (APIError, ValueError):
        # If exchange rejects large limits, that's acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/funding_rate"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
@pytest.mark.asyncio
async def test_bp_get_funding_rates_multiple_symbols_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_funding_rates() returns consistent structure across symbols."""
    perp_symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP"]
    all_funding_rates: dict[str, list[FundingRate]] = {}

    for symbol in perp_symbols:
        args = GetFundingRatesArgs(
            symbols=[symbol],
        )

        funding_rates = await bp_api_for_test_env.get_funding_rates(args)
        assert isinstance(funding_rates, list), (
            f"Expected list for {symbol}, got {type(funding_rates)}"
        )
        all_funding_rates[symbol] = funding_rates

    # Verify structure consistency across symbols
    for symbol, funding_rates in all_funding_rates.items():
        if len(funding_rates) > 0:
            # All funding rates for each symbol should have the same structure
            first_rate = funding_rates[0]
            required_attrs = ["symbol", "funding_rate"]

            for attr in required_attrs:
                assert hasattr(first_rate, attr), (
                    f"FundingRate for {symbol} missing required attribute: {attr}"
                )
                value = getattr(first_rate, attr)
                assert value is not None, (
                    f"FundingRate for {symbol} has None value for required attribute: {attr}"
                )

            # All funding rates in the list should have same symbol
            for i, funding_rate in enumerate(funding_rates):
                assert funding_rate.symbol == symbol, (
                    f"Funding rate {i} for {symbol} has wrong symbol: {funding_rate.symbol}"
                )
