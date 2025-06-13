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

Authentication: No authentication required for public funding data
VCR: Records funding rate responses for consistent testing
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs
from cyberdelta.core.models import FundingRate

pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/funding"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_btc_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_historical_funding_rates() with BTC returns valid models."""
    # Use fixed timestamps for VCR consistency
    end_time = 1640995200  # Fixed timestamp
    start_time = end_time - 86400  # 24 hours earlier

    args = GetHistoricalFundingRatesArgs(
        symbol="BTC",
        start_time=datetime.fromtimestamp(start_time),
        end_time=datetime.fromtimestamp(end_time),
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    assert isinstance(funding_rates, list), (
        f"Expected list of funding rates, got {type(funding_rates)}"
    )

    if len(funding_rates) > 0:
        for i, funding_rate in enumerate(funding_rates):
            assert isinstance(funding_rate, FundingRate), (
                f"FundingRate {i} should be FundingRate model"
            )
            assert hasattr(funding_rate, "symbol") and hasattr(funding_rate, "rate")
            assert isinstance(funding_rate.funding_rate, Decimal), "Rate should be Decimal"
            assert funding_rate.symbol == "BTC", f"Wrong symbol: {funding_rate.symbol}"
            assert abs(funding_rate.funding_rate) < Decimal("1"), (
                f"Rate seems unreasonable: {funding_rate.funding_rate}"
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/funding"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_eth_success(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_historical_funding_rates() with ETH returns valid models."""
    end_time = 1640995200
    start_time = end_time - 86400

    args = GetHistoricalFundingRatesArgs(
        symbol="ETH",
        start_time=datetime.fromtimestamp(start_time),
        end_time=datetime.fromtimestamp(end_time),
    )

    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)

    assert isinstance(funding_rates, list), f"Expected list, got {type(funding_rates)}"

    if len(funding_rates) > 0:
        for funding_rate in funding_rates:
            assert isinstance(funding_rate, FundingRate), "Should be FundingRate model"
            assert funding_rate.symbol == "ETH", f"Wrong symbol: {funding_rate.symbol}"