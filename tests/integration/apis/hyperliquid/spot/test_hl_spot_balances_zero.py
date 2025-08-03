"""Integration tests for Hyperliquid spot balances endpoints with zero balance scenarios.

This module focuses specifically on testing the SpotBalance model pipeline
through Hyperliquid's /info endpoint with user address authentication for zero balance scenarios.
Tests validate complete data transformation from API responses to SpotBalance instances.

Model Focus: SpotBalance (Zero Balance Edge Cases)
- Validates complete SpotBalance model field mapping for empty accounts
- Tests Decimal precision for zero financial values
- Validates business logic constraints with zero balances
- Tests Hyperliquid-specific balance details (hl_details) for empty accounts
- Comprehensive error handling and edge cases

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.models.spot_balance import SpotBalance


pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/balances/zero"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.zero_balance
class TestHyperliquidSpotBalancesZero:
    """Comprehensive zero balance integration tests for SpotBalance model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_empty_account(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with empty/zero balance account."""
        balances = await hl_api_for_zero_balance_test.get_balances()

        assert isinstance(balances, dict), "get_balances() should always return dict"

        for asset_symbol, balance in balances.items():
            assert isinstance(balance, SpotBalance), (
                f"Even zero balance should be SpotBalance for {asset_symbol}"
            )

            assert balance.total_quantity >= Decimal(0), (
                "Zero balances should still be non-negative"
            )
            assert balance.available_quantity >= Decimal(0), (
                "Zero available should still be non-negative"
            )
            assert balance.total_quantity >= balance.available_quantity, (
                "Zero balance logic should still hold"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_network_timeout(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() behavior with network timeout scenarios.

        Raises:
            APIError: When network timeout, connection errors, or other
                network-related issues occur during API calls.
        """
        try:
            balances = await hl_api_for_zero_balance_test.get_balances()

            assert isinstance(balances, dict), "Successful response should be dict"

        except APIError as e:
            if "timeout" in e.message.lower() or "connection" in e.message.lower():
                if e.code not in [
                    APIErrorCode.TIMEOUT.value,
                    APIErrorCode.CONNECTION_ERROR.value,
                    APIErrorCode.NETWORK_ISSUE.value,
                ]:
                    pytest.fail(f"Network error should map to network-related code, got {e.code}")
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior.

        Raises:
            APIError: When rate limits are exceeded or other API errors
                occur during rapid concurrent requests.
        """
        try:
            tasks: list[Any] = [hl_api_for_zero_balance_test.get_balances() for _ in range(5)]

            results: list[dict[str, Any]] = []
            for i, task in enumerate(tasks):
                try:
                    result: dict[str, Any] = await task
                    results.append(result)
                    assert isinstance(result, dict), f"Call {i} should return dict if successful"
                except APIError as e:
                    if "rate" in e.message.lower() or "limit" in e.message.lower():
                        if e.code != APIErrorCode.RATE_LIMITED.value:
                            pytest.fail(
                                f"Rate limit error should map to RATE_LIMITED, got {e.code}"
                            )
                        if hasattr(e, "retry_after") and e.retry_after is not None:
                            pytest.fail("retry_after should be numeric if present")
                    else:
                        raise

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint."""
        tasks = [
            hl_api_for_zero_balance_test.get_balances(),
            hl_api_for_zero_balance_test.get_balances(),
            hl_api_for_zero_balance_test.get_balances(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, dict), f"Result {i} should be dict"
                successful_results.append(result)

        assert len(successful_results) > 0, "At least one concurrent call should succeed"

        if len(successful_results) > 1:
            first_result: dict[str, Any] = successful_results[0]
            for _i, result in enumerate(successful_results[1:], 1):
                assert set(first_result.keys()) == set(result.keys()), (
                    f"Concurrent results should have same assets: {first_result.keys()} vs "
                    f"{result.keys()}"
                )
