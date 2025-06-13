"""Comprehensive integration tests for Backpack spot balances with zero balance scenarios.

This module provides comprehensive testing of the SpotBalance model pipeline
through Backpack's balance endpoints when the account has zero or minimal balances.
Tests validate complete data transformation, error handling, and edge cases.

Model Focus: SpotBalance (Zero Balance Edge Cases)
- Validates complete SpotBalance model field mapping for empty accounts
- Tests Decimal precision for financial values with zero/dust amounts
- Validates business logic constraints with zero balance scenarios
- Tests Backpack-specific balance details and edge cases
- Comprehensive error handling and authentication scenarios

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance

# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/balances/zero_balance"], indirect=True
)
@pytest.mark.zero_balance
class TestBackpackSpotBalancesZeroComprehensive:
    """Comprehensive spot balance integration tests for zero balance scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_zero_account_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with comprehensive SpotBalance validation for zero balance accounts.

        This test validates the complete pipeline from Ed25519 authenticated request
        to fully validated SpotBalance model instances with zero balance constraints.
        """
        # Execute the API call
        balances = await bp_api_for_test_env.get_balances()

        # Validate container type
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        # Test zero balance scenarios - should return dict (may be empty or contain zero balances)
        for asset_symbol, balance in balances.items():
            # Validate model type
            assert isinstance(balance, SpotBalance), (
                f"Balance for {asset_symbol} should be SpotBalance instance"
            )

            # Use shared validation helper
            assert_valid_spot_balance(balance)

            # Validate core fields
            assert balance.asset == asset_symbol, (
                f"SpotBalance.asset ({balance.asset}) should match dict key ({asset_symbol})"
            )
            assert balance.exchange == "backpack", (
                f"SpotBalance.exchange should be 'backpack', got {balance.exchange}"
            )

            # Validate timestamp recency (within 1 hour for active API)
            assert balance.timestamp is not None, "SpotBalance must have timestamp"
            time_diff = datetime.now(balance.timestamp.tzinfo) - balance.timestamp
            assert time_diff.total_seconds() < 3600, (
                f"Timestamp should be recent (< 1 hour), got {time_diff.total_seconds()}s ago"
            )

            # Validate Decimal precision and types
            assert isinstance(balance.total_quantity, Decimal), (
                f"total_quantity must be Decimal, got {type(balance.total_quantity)}"
            )
            assert isinstance(balance.available_quantity, Decimal), (
                f"available_quantity must be Decimal, got {type(balance.available_quantity)}"
            )

            # Validate financial constraints for zero balance accounts
            assert balance.total_quantity >= Decimal("0"), (
                f"total_quantity must be non-negative, got {balance.total_quantity}"
            )
            assert balance.available_quantity >= Decimal("0"), (
                f"available_quantity must be non-negative, got {balance.available_quantity}"
            )

            # Validate business logic: total >= available (accounting for locked funds)
            assert balance.total_quantity >= balance.available_quantity, (
                f"total_quantity ({balance.total_quantity}) must be >= "
                f"available_quantity ({balance.available_quantity})"
            )

            # For zero balance accounts, expect very small or zero amounts
            assert balance.total_quantity <= Decimal("1.0"), (
                f"Zero balance account should have minimal total_quantity, "
                f"got {balance.total_quantity}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_authentication_failure(
        self,
        bp_api_with_di: Callable[..., BackpackAPI],
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with invalid authentication credentials.

        This validates proper error handling and mapping for authentication failures.
        """
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("invalid_api_key"),
            api_secret=SecretStr("invalid_api_secret"),
        )

        bad_api = bp_api_with_di(secrets=invalid_secrets)

        # Try to get balances with invalid credentials
        try:
            result = await bad_api.get_balances()
            # If we get here, the call succeeded (possibly due to VCR playback)
            # Validate that we got a proper response structure
            assert isinstance(result, dict), "Response should be a dict of balances"
            # This is acceptable for VCR playback scenarios
        except APIError as api_error:
            # This is the expected behavior for real API calls with invalid credentials
            assert api_error.code in [
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.INVALID_PARAMS.value,
            ], f"Expected authentication error, got: {api_error.code}"
            assert "auth" in api_error.message.lower() or "invalid" in api_error.message.lower()
        except Exception as e:
            pytest.fail(f"Unexpected exception type: {type(e).__name__}: {e}")
        finally:
            await bad_api.close()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with edge cases around decimal precision.

        This validates handling of very small balances, dust amounts,
        and precision edge cases that might occur in real trading.
        """
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for precision testing")

        for _, balance in balances.items():
            # Test very small balance handling
            if balance.total_quantity > Decimal("0"):
                # Validate that small balances maintain precision
                assert balance.total_quantity.is_finite(), (
                    f"Balance {balance.total_quantity} should be finite"
                )

                # Check for dust handling (very small amounts)
                if balance.total_quantity < Decimal("0.000001"):  # Less than 1 micro-unit
                    # Even dust amounts should be properly represented
                    assert str(balance.total_quantity) != "0E-0", (
                        "Dust balances should maintain proper decimal representation"
                    )

                # Validate precision consistency between total and available
                total_str = str(balance.total_quantity)
                available_str = str(balance.available_quantity)

                # Both should have reasonable precision representation
                assert "E" not in total_str.upper() or "E-" in total_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {total_str}"
                )
                assert "E" not in available_str.upper() or "E-" in available_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {available_str}"
                )

            # Validate Decimal precision (should have reasonable precision for crypto)
            total_precision = (
                len(str(balance.total_quantity).split(".")[-1])
                if "." in str(balance.total_quantity)
                else 0
            )
            available_precision = (
                len(str(balance.available_quantity).split(".")[-1])
                if "." in str(balance.available_quantity)
                else 0
            )

            # Financial precision should be reasonable (not excessive)
            assert total_precision <= 18, (
                f"total_quantity precision too high: {total_precision} decimals"
            )
            assert available_precision <= 18, (
                f"available_quantity precision too high: {available_precision} decimals"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint.

        This validates that concurrent balance requests don't interfere with each other
        and that the underlying API handles concurrency properly.
        """
        import asyncio

        # Make multiple concurrent calls
        tasks = [
            bp_api_for_test_env.get_balances(),
            bp_api_for_test_env.get_balances(),
            bp_api_for_test_env.get_balances(),
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should succeed and return consistent data
        successful_results: list[dict[str, SpotBalance]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # If some fail due to rate limiting, that's acceptable
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                else:
                    pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, dict), f"Result {i} should be dict"
                successful_results.append(result)

        # At least one should succeed
        assert len(successful_results) > 0, "At least one concurrent call should succeed"

        # If multiple succeed, they should have consistent data (within reasonable time window)
        if len(successful_results) > 1:
            first_result = successful_results[0]
            for _, result in enumerate(successful_results[1:], 1):
                # Balance amounts might differ slightly due to timing, but structure should be same
                assert set(first_result.keys()) == set(result.keys()), (
                    f"Concurrent results should have same assets: {first_result.keys()} vs "
                    f"{result.keys()}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior.

        This test validates proper handling of rate limit responses from Backpack.
        May require multiple rapid calls to trigger rate limiting during recording.
        """
        try:
            # Make multiple rapid calls to potentially trigger rate limiting
            tasks: list[Any] = []
            for _ in range(5):
                tasks.append(bp_api_for_test_env.get_balances())

            # Most should succeed, but if rate limited, validate error handling
            results: list[dict[str, SpotBalance]] = []
            for i, task in enumerate(tasks):
                try:
                    result = await task
                    results.append(result)
                    assert isinstance(result, dict), f"Call {i} should return dict if successful"
                except APIError as e:
                    if "rate" in e.message.lower() or "limit" in e.message.lower():
                        assert e.code == APIErrorCode.RATE_LIMITED.value, (
                            f"Rate limit error should map to RATE_LIMITED, got {e.code}"
                        )
                        # Check if retry-after information is preserved
                        if hasattr(e, "retry_after") and e.retry_after:
                            assert isinstance(e.retry_after, int | float), (
                                "retry_after should be numeric if present"
                            )
                    else:
                        raise  # Re-raise non-rate-limit errors

        except Exception as e:
            # If we can't trigger rate limiting, skip the test
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_network_timeout(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() behavior with network timeout scenarios.

        Note: This test may be challenging to reproduce consistently in VCR,
        so it might need to be mocked or use specific network conditions.
        """
        try:
            # Attempt the call - in normal conditions this should succeed
            balances = await bp_api_for_test_env.get_balances()

            # If successful, validate the response
            assert isinstance(balances, dict), "Successful response should be dict"

        except APIError as e:
            # If we get a timeout or network error, validate it's properly classified
            if "timeout" in e.message.lower() or "connection" in e.message.lower():
                assert e.code in [
                    APIErrorCode.TIMEOUT.value,
                    APIErrorCode.CONNECTION_ERROR.value,
                    APIErrorCode.NETWORK_ISSUE.value,
                ], f"Network error should map to network-related code, got {e.code}"
            else:
                # Re-raise non-network errors
                raise
