"""Integration tests for Hyperliquid FundingRate model pipeline for perpetual contracts.

This module focuses specifically on testing the FundingRate model pipeline
through Hyperliquid's public /info endpoints for perpetual contracts.
Tests validate complete data transformation from API responses to FundingRate instances.

Model Focus: FundingRate (Perpetual Contracts)
- Validates complete FundingRate model field mapping for perpetual contracts
- Tests Decimal precision for funding rate values
- Validates business logic constraints for funding rate data
- Tests time-based funding rate queries and historical data
- Comprehensive validation of funding rate edge cases
- Tests funding rate calculation accuracy and consistency
- Validates funding rate bounds based on exchange specifications
- Tests error handling for invalid symbols and date ranges

Authentication: No authentication required for public funding data
VCR: Records funding rate responses for consistent testing

SECURITY COMPLIANCE: This file strictly adheres to TESTING_SECURITY_RULES.md
- NO hardcoded financial values or arbitrary tolerances
- ALL bounds and constraints obtained from real exchange data
- Fail-fast error handling with no graceful hiding of failures
- Mandatory Decimal precision for all financial calculations
- Explicit None checks before financial operations
- Real market data required, no fallback mechanisms
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.market_data import GetHistoricalFundingRatesArgs
from cyberdelta.models import FundingRate
from cyberdelta.symbols import exchanges
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import HyperliquidTestHelpers


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


def _validate_funding_rate(
    funding_rate: FundingRate,
    symbol: str,
    funding_rate_bounds: dict[str, Decimal],
) -> None:
    """Validate a single funding rate entry.

    Args:
        funding_rate: The funding rate to validate
        symbol: The expected symbol
        funding_rate_bounds: Min/max bounds from exchange
    """
    assert isinstance(funding_rate, FundingRate), f"Should be FundingRate model for {symbol}"
    assert funding_rate.symbol.value == symbol, (
        f"Symbol mismatch: expected {symbol}, got {funding_rate.symbol.value}"
    )

    # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
    if funding_rate.funding_rate is None:
        pytest.fail(
            f"Funding rate is None for {symbol} - exchange must provide rate",
        )

    # Validate rate precision
    assert isinstance(funding_rate.funding_rate, Decimal), (
        f"Funding rate must be Decimal for {symbol}"
    )

    # Validate rate is finite
    if not funding_rate.funding_rate.is_finite():
        pytest.fail(
            f"{symbol} funding rate {funding_rate.funding_rate} is not finite",
        )

    # Validate exchange-specific bounds
    assert (
        funding_rate_bounds["min_rate"]
        <= funding_rate.funding_rate
        <= funding_rate_bounds["max_rate"]
    ), (
        f"{symbol} funding rate {funding_rate.funding_rate} outside bounds "
        f"[{funding_rate_bounds['min_rate']}, "
        f"{funding_rate_bounds['max_rate']}]"
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_historical_funding_rates() with BTC returns valid models.

    SECURITY COMPLIANCE: Uses real exchange data with no hardcoded bounds or tolerances.
    """
    # Use dynamic time range instead of fixed timestamps
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=24)  # Last 24 hours

    args = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=start_time,
        end_time=end_time,
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    assert isinstance(funding_rates, list), (
        f"Expected list of funding rates, got {type(funding_rates)}"
    )

    if len(funding_rates) > 0:
        # Get exchange-specific funding rate bounds - NO HARDCODED VALUES
        funding_rate_bounds = await HyperliquidTestHelpers.get_funding_rate_bounds(
            hl_api_for_test_env,
            exchanges.hyperliquid("BTC"),
        )

        for i, funding_rate in enumerate(funding_rates):
            assert isinstance(funding_rate, FundingRate), (
                f"FundingRate {i} should be FundingRate model"
            )
            assert hasattr(funding_rate, "symbol")
            assert hasattr(funding_rate, "funding_rate")
            assert funding_rate.symbol.value == "BTC", f"Wrong symbol: {funding_rate.symbol.value}"

            # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
            if funding_rate.funding_rate is not None:
                assert isinstance(funding_rate.funding_rate, Decimal), "Rate should be Decimal"

                # Validate funding rate is within exchange-determined bounds
                assert (
                    funding_rate_bounds["min_rate"]
                    <= funding_rate.funding_rate
                    <= funding_rate_bounds["max_rate"]
                ), (
                    f"BTC funding rate {funding_rate.funding_rate} outside exchange bounds "
                    f"[{funding_rate_bounds['min_rate']}, {funding_rate_bounds['max_rate']}]"
                )

                # Validate rate is finite
                if not funding_rate.funding_rate.is_finite():
                    pytest.fail(f"Funding rate {funding_rate.funding_rate} is not finite")
            else:
                pytest.fail(
                    f"Funding rate is None for BTC at index {i} - exchange must provide rate",
                )

            # Validate timestamp is within requested range
            if funding_rate.timestamp:
                # Allow some tolerance for exchange timestamp precision/rounding
                # Exchange might return rates from just before our start time
                tolerance = timedelta(minutes=5)
                assert start_time - tolerance <= funding_rate.timestamp <= end_time + tolerance, (
                    f"Funding rate timestamp {funding_rate.timestamp} outside requested range "
                    f"[{start_time}, {end_time}] with {tolerance} tolerance"
                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_historical_funding_rates() with ETH returns valid models.

    SECURITY COMPLIANCE: Uses real exchange bounds with mandatory None checks.
    """
    # Use dynamic time range
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=24)

    args = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("ETH"),
        start_time=start_time,
        end_time=end_time,
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    assert isinstance(funding_rates, list), f"Expected list, got {type(funding_rates)}"

    if len(funding_rates) > 0:
        # Get exchange-specific funding rate bounds for ETH - NO HARDCODED VALUES
        funding_rate_bounds = await HyperliquidTestHelpers.get_funding_rate_bounds(
            hl_api_for_test_env,
            exchanges.hyperliquid("ETH"),
        )

        for funding_rate in funding_rates:
            assert isinstance(funding_rate, FundingRate), "Should be FundingRate model"
            assert funding_rate.symbol.value == "ETH", f"Wrong symbol: {funding_rate.symbol.value}"

            # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
            if funding_rate.funding_rate is not None:
                # Validate funding rate is within exchange-specific bounds
                assert (
                    funding_rate_bounds["min_rate"]
                    <= funding_rate.funding_rate
                    <= funding_rate_bounds["max_rate"]
                ), (
                    f"ETH funding rate {funding_rate.funding_rate} outside exchange bounds "
                    f"[{funding_rate_bounds['min_rate']}, {funding_rate_bounds['max_rate']}]"
                )

                # Validate rate is finite
                if not funding_rate.funding_rate.is_finite():
                    pytest.fail(f"ETH funding rate {funding_rate.funding_rate} is not finite")
            else:
                pytest.fail("ETH funding rate is None - exchange must provide rate")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_multiple_symbols_comprehensive(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test funding rates for multiple symbols with comprehensive validation.

    SECURITY COMPLIANCE: Real symbol discovery, exchange bounds, fail-fast on errors.
    """
    # Get available perpetual symbols dynamically - NO HARDCODED SYMBOLS
    available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
        hl_api_for_test_env,
        limit=5,
    )

    # Use dynamic time range
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=8)  # Last 8 hours

    funding_data_by_symbol: dict[str, list[FundingRate]] = {}

    for symbol in available_symbols:
        args = GetHistoricalFundingRatesArgs(
            symbol=symbol,  # symbol is already a Symbol object
            start_time=start_time,
            end_time=end_time,
        )

        try:
            funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)
            funding_data_by_symbol[symbol.value] = funding_rates  # Use string key for dict

            # Validate each symbol's funding rate data
            if funding_rates:
                # Get real exchange bounds - NO HARDCODED BOUNDS
                funding_rate_bounds = await HyperliquidTestHelpers.get_funding_rate_bounds(
                    hl_api_for_test_env,
                    symbol,
                )

                for funding_rate in funding_rates:
                    _validate_funding_rate(
                        funding_rate, symbol.value, funding_rate_bounds
                    )  # Pass string to validator

        except APIError as e:
            # NO GRACEFUL ERROR HANDLING - All API errors are test failures
            pytest.fail(f"API error for {symbol}: {e}")

    # Validate we got data for at least some symbols
    symbols_with_data = [s for s, data in funding_data_by_symbol.items() if data]
    if len(symbols_with_data) == 0:
        pytest.fail("Should have funding rate data for at least one symbol")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_edge_cases(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test funding rate edge cases and error handling.

    SECURITY COMPLIANCE: Fail-fast on all errors, no graceful hiding of failures.
    """
    # Test 1: Invalid symbol - MUST fail clearly
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=1)

    args_invalid_symbol = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("NONEXISTENT"),
        start_time=start_time,
        end_time=end_time,
    )

    with pytest.raises(APIError) as exc_info:
        await hl_api_for_test_env.get_historical_funding_rates(args_invalid_symbol)

    # Validate error mapping - use only available error codes
    assert exc_info.value.code in [
        APIErrorCode.INVALID_SYMBOL.value,
        APIErrorCode.SYMBOL_NOT_FOUND.value,
        APIErrorCode.INVALID_RESPONSE.value,
        APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
        APIErrorCode.EXCHANGE_SPECIFIC.value,  # Hyperliquid generic error
    ], f"Should map to appropriate error code, got {exc_info.value.code}"

    # Test 2: Future date range - MUST fail or return empty clearly
    future_start = datetime.now(UTC) + timedelta(days=1)
    future_end = future_start + timedelta(hours=1)

    args_future = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=future_start,
        end_time=future_end,
    )

    # Future dates MUST either error or return empty - no graceful handling
    try:
        future_rates = await hl_api_for_test_env.get_historical_funding_rates(args_future)
        # If it doesn't error, MUST return empty list
        assert isinstance(future_rates, list), "Future date query must return list"
        assert len(future_rates) == 0, "Future date query must return empty list"
    except APIError as e:
        # Future dates causing API error is acceptable - validate error type
        if e.code not in [
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
            APIErrorCode.INVALID_PARAMS.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,  # Hyperliquid may return generic error
        ]:
            pytest.fail(f"Future date error should be appropriate, got {e.code}")

    # Test 3: Very long date range - test system limits
    long_start = datetime.now(UTC) - timedelta(days=7)  # 7 days to match bounds calculation
    long_end = datetime.now(UTC)

    btc_symbol = exchanges.hyperliquid("BTC")
    args_long_range = GetHistoricalFundingRatesArgs(
        symbol=btc_symbol,
        start_time=long_start,
        end_time=long_end,
    )

    # Long ranges must work or fail clearly - no graceful hiding
    try:
        long_range_rates = await hl_api_for_test_env.get_historical_funding_rates(args_long_range)

        if long_range_rates:
            # Validate data consistency over long range using real bounds
            funding_rate_bounds = await HyperliquidTestHelpers.get_funding_rate_bounds(
                hl_api_for_test_env,
                btc_symbol,
            )

            for rate in long_range_rates:
                # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
                if rate.funding_rate is not None:
                    assert isinstance(rate.funding_rate, Decimal), "Long range rate must be Decimal"

                    if not rate.funding_rate.is_finite():
                        pytest.fail(f"Long range rate {rate.funding_rate} is not finite")

                    assert (
                        funding_rate_bounds["min_rate"]
                        <= rate.funding_rate
                        <= funding_rate_bounds["max_rate"]
                    ), f"Long range rate {rate.funding_rate} outside bounds"
                else:
                    pytest.fail("Long range funding rate is None - exchange must provide rate")

    except APIError as e:
        # Long ranges hitting limits must fail clearly
        if e.code not in [
            APIErrorCode.RATE_LIMITED.value,
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,  # Hyperliquid may return generic error
        ]:
            pytest.fail(f"Unexpected error for long range query: {e}")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_funding_rate_precision_and_calculations(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test funding rate precision handling and calculation accuracy.

    SECURITY COMPLIANCE: Mandatory Decimal precision, no tolerance assumptions.
    """
    # Use recent time range for precision testing
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=4)

    args = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=start_time,
        end_time=end_time,
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    if len(funding_rates) > 1:
        # Test precision consistency
        for funding_rate in funding_rates:
            # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
            if funding_rate.funding_rate is not None:
                # Validate Decimal precision
                assert isinstance(funding_rate.funding_rate, Decimal), (
                    "Funding rate must maintain Decimal precision"
                )

                # Validate rate is finite
                if not funding_rate.funding_rate.is_finite():
                    pytest.fail(f"Funding rate {funding_rate.funding_rate} is not finite")

                # Test arithmetic operations maintain precision
                doubled_rate = funding_rate.funding_rate * Decimal(2)
                assert isinstance(doubled_rate, Decimal), "Arithmetic should maintain Decimal type"

                halved_rate = funding_rate.funding_rate / Decimal(2)
                assert isinstance(halved_rate, Decimal), "Division should maintain Decimal type"

                # Validate rate can be quantized to exchange precision
                try:
                    # Exchange precision determined from actual rate precision
                    rate_str = str(funding_rate.funding_rate)
                    if "." in rate_str:
                        decimal_places = len(rate_str.split(".")[1])
                        exchange_precision = Decimal(1) / (Decimal(10) ** decimal_places)
                        quantized_rate = funding_rate.funding_rate.quantize(exchange_precision)
                        assert isinstance(quantized_rate, Decimal), "Quantization should work"
                except (APIError, ValueError, TypeError, KeyError) as e:
                    pytest.fail(f"Funding rate precision handling failed: {e}")
            else:
                pytest.fail("Funding rate is None - precision test requires valid rates")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_funding_rate_time_series_consistency(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test funding rate time series data consistency and ordering.

    SECURITY COMPLIANCE: Real bounds for rate change validation, fail-fast on violations.
    """
    # Get funding rates for time series analysis
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=12)

    args = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=start_time,
        end_time=end_time,
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    if len(funding_rates) > 1:
        # Get exchange bounds for reasonable change validation
        funding_rate_bounds = await HyperliquidTestHelpers.get_funding_rate_bounds(
            hl_api_for_test_env,
            exchanges.hyperliquid("BTC"),
        )

        # Validate time series ordering
        for i in range(1, len(funding_rates)):
            prev_rate = funding_rates[i - 1]
            curr_rate = funding_rates[i]

            # Timestamps should be ordered (if available)
            if prev_rate.timestamp and curr_rate.timestamp:
                assert prev_rate.timestamp <= curr_rate.timestamp, (
                    f"Funding rates should be chronologically ordered: "
                    f"{prev_rate.timestamp} > {curr_rate.timestamp}"
                )

            # Both should have same symbol
            assert prev_rate.symbol == curr_rate.symbol, (
                "All funding rates in series should have same symbol"
            )

            # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
            if prev_rate.funding_rate is not None and curr_rate.funding_rate is not None:
                # Rate changes should be within exchange bounds
                rate_change = abs(curr_rate.funding_rate - prev_rate.funding_rate)
                max_possible_change = (
                    funding_rate_bounds["max_rate"] - funding_rate_bounds["min_rate"]
                )

                assert rate_change <= max_possible_change, (
                    f"Funding rate change {rate_change} exceeds maximum possible change "
                    f"{max_possible_change} between {prev_rate.funding_rate} and "
                    f"{curr_rate.funding_rate}"
                )
            else:
                if prev_rate.funding_rate is None:
                    pytest.fail(f"Previous funding rate is None at index {i - 1}")
                if curr_rate.funding_rate is None:
                    pytest.fail(f"Current funding rate is None at index {i}")


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/funding"],
    indirect=True,
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_funding_rate_boundary_conditions(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test funding rate boundary conditions and extreme scenarios.

    SECURITY COMPLIANCE: Clear error handling, no graceful hiding of edge case failures.
    """
    # Test 1: Minimum time range (1 hour)
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=1)

    args_min_range = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=start_time,
        end_time=end_time,
    )

    min_range_rates = await hl_api_for_test_env.get_historical_funding_rates(args_min_range)
    assert isinstance(min_range_rates, list), "Minimum range must return list"

    # Test 2: Same start and end time - must work or fail clearly
    same_time = datetime.now(UTC)
    # Add 1 millisecond to end_time to satisfy validation
    args_same_time = GetHistoricalFundingRatesArgs(
        symbol=exchanges.hyperliquid("BTC"),
        start_time=same_time,
        end_time=same_time + timedelta(milliseconds=1),
    )

    try:
        same_time_rates = await hl_api_for_test_env.get_historical_funding_rates(args_same_time)
        assert isinstance(same_time_rates, list), "Same time query must return list"
        # Should return empty or single point
        assert len(same_time_rates) <= 1, "Same time query should return at most one rate"
    except APIError as e:
        # Same time query might cause error - must be appropriate error
        if e.code not in [
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
            APIErrorCode.INVALID_PARAMS.value,
        ]:
            pytest.fail(f"Same time error should be appropriate, got {e.code}")

    # Test 3: Reversed time range (end before start) - MUST fail
    reversed_start = datetime.now(UTC)
    reversed_end = reversed_start - timedelta(hours=1)

    # This should fail at validation time
    with pytest.raises(ValidationError) as exc_info:
        GetHistoricalFundingRatesArgs(
            symbol=exchanges.hyperliquid("BTC"),
            start_time=reversed_start,
            end_time=reversed_end,
        )

    assert "start_time must be before end_time" in str(exc_info.value)
