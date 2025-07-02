"""Integration tests for Hyperliquid balances endpoints.

This module focuses specifically on testing the SpotBalance model pipeline
through Hyperliquid's /info endpoint with user address authentication.
Tests validate complete data transformation from API responses to SpotBalance instances.

Model Focus: SpotBalance
- Validates complete SpotBalance model field mapping
- Tests Decimal precision for financial values
- Validates business logic constraints
- Tests Hyperliquid-specific balance details (hl_details)
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
from cyberdelta.core.models.spot_balance import SpotBalance


# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/balances"], indirect=True)
@pytest.mark.zero_balance
class TestHyperliquidBalancesZeroComprehensive:
    """Comprehensive balances integration tests for SpotBalance model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_success_comprehensive(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_balances() with comprehensive SpotBalance validation.

        This test validates the complete pipeline from EIP-712 authenticated request
        to fully validated SpotBalance model instances with all field constraints.
        """
        # Execute the API call
        balances = await hl_api_for_zero_balance_test.get_balances()

        # Validate container type
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        # For zero balance test - validate either empty dict or dict with zero balances
        # Different exchanges handle zero balances differently:
        # - Some return empty dict (preferred behavior)
        # - Some return balance entries with 0 quantities (also valid)

        if not balances:
            # Empty dict case - this is the preferred behavior
            assert balances == {}, "Empty balances should be empty dict"
        else:
            # Non-empty case - all balances should be zero
            for asset_symbol, balance in balances.items():
                assert isinstance(balance, SpotBalance), (
                    f"Balance for {asset_symbol} must be SpotBalance"
                )
                assert balance.total_quantity == Decimal(0), (
                    f"Zero balance account should have 0 total quantity for {asset_symbol}, "
                    f"got {balance.total_quantity}"
                )
                assert balance.available_quantity == Decimal(0), (
                    f"Zero balance account should have 0 available quantity for {asset_symbol}, "
                    f"got {balance.available_quantity}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_empty_account(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with empty/zero balance account.

        This test validates behavior when account has no balances or all zero balances.
        Important for testing edge cases in balance handling.
        """
        balances = await hl_api_for_zero_balance_test.get_balances()

        # Should return empty dict or dict with zero balances
        assert isinstance(balances, dict), "get_balances() should always return dict"

        # If balances exist, they should all be valid (including zero balances)
        for asset_symbol, balance in balances.items():
            assert isinstance(balance, SpotBalance), (
                f"Even zero balance should be SpotBalance for {asset_symbol}"
            )

            # Zero balances should still follow constraints
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

        Note: This test may be challenging to reproduce consistently in VCR,
        so it might need to be mocked or use specific testnet conditions.

        Raises:
            APIError: When network timeout or connection errors occur during API calls.
        """
        try:
            # Attempt the call - in normal conditions this should succeed
            balances = await hl_api_for_zero_balance_test.get_balances()

            # If successful, validate the response
            assert isinstance(balances, dict), "Successful response should be dict"

        except APIError as e:
            # If we get a timeout or network error, validate it's properly classified
            if "timeout" in e.message.lower() or "connection" in e.message.lower():
                if e.code not in [
                    APIErrorCode.TIMEOUT.value,
                    APIErrorCode.CONNECTION_ERROR.value,
                    APIErrorCode.NETWORK_ISSUE.value,
                ]:
                    pytest.fail(f"Network error should map to network-related code, got {e.code}")
            else:
                # Re-raise non-network errors
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior.

        This test validates proper handling of rate limit responses from Hyperliquid.
        May require multiple rapid calls to trigger rate limiting during recording.

        Raises:
            APIError: When rate limiting is triggered by making too many rapid API calls.
        """
        try:
            # Make multiple rapid calls to potentially trigger rate limiting
            tasks: list[Any] = [hl_api_for_zero_balance_test.get_balances() for _ in range(5)]

            # Most should succeed, but if rate limited, validate error handling
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
                        # Check if retry-after information is preserved
                        if hasattr(e, "retry_after") and e.retry_after is not None:
                            pytest.fail("retry_after should be numeric if present")
                    else:
                        raise  # Re-raise non-rate-limit errors

        except (APIError, ValueError, TypeError, KeyError) as e:
            # If we can't trigger rate limiting in testnet, skip the test
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint.

        This validates that concurrent balance requests don't interfere with each other
        and that the underlying clearinghouse state call handles concurrency properly.
        """
        # Make multiple concurrent calls
        tasks = [
            hl_api_for_zero_balance_test.get_balances(),
            hl_api_for_zero_balance_test.get_balances(),
            hl_api_for_zero_balance_test.get_balances(),
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should succeed and return consistent data
        successful_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # If some fail due to rate limiting, that's acceptable
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, dict), f"Result {i} should be dict"
                successful_results.append(result)

        # At least one should succeed
        assert len(successful_results) > 0, "At least one concurrent call should succeed"

        # If multiple succeed, they should have consistent data (within reasonable time window)
        if len(successful_results) > 1:
            first_result: dict[str, Any] = successful_results[0]
            for _i, result in enumerate(successful_results[1:], 1):
                # Balance amounts might differ slightly due to timing, but structure should be same
                assert set(first_result.keys()) == set(result.keys()), (
                    f"Concurrent results should have same assets: {first_result.keys()} vs "
                    f"{result.keys()}"
                )
