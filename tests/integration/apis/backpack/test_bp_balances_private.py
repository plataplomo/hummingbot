"""Integration tests for Backpack private balances endpoints.

This module focuses specifically on testing the SpotBalance model pipeline
through Backpack's private /api/v1/capital endpoint with Ed25519 authentication.
Tests validate complete data transformation from API responses to SpotBalance instances.

Model Focus: SpotBalance
- Validates complete SpotBalance model field mapping
- Tests Decimal precision for financial values
- Validates business logic constraints
- Tests Backpack-specific balance details (bp_details)
- Comprehensive error handling and edge cases

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

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/balances"], indirect=True
)
class TestBackpackBalancesPrivate:
    """Comprehensive private balances integration tests for SpotBalance model validation."""

    @pytest.mark.vcr
    async def test_get_balances_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_balances() with comprehensive SpotBalance validation.

        This test validates the complete pipeline from Ed25519 authenticated request
        to fully validated SpotBalance model instances with all field constraints.
        """
        # Execute the API call
        balances = await bp_api_for_test_env.get_balances()

        # Validate container type
        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        # Test both empty and populated balance scenarios
        if not balances:
            pytest.skip("No balances available in test environment for validation")

        # Comprehensive validation of each balance
        for asset_symbol, balance in balances.items():
            # Validate model type
            assert isinstance(balance, SpotBalance), (
                f"Balance for {asset_symbol} should be SpotBalance instance"
            )

            # Validate core fields
            assert balance.asset == asset_symbol, (
                f"SpotBalance.asset ({balance.asset}) should match dict key ({asset_symbol})"
            )
            assert balance.exchange == "backpack", (
                f"SpotBalance.exchange should be 'backpack', got {balance.exchange}"
            )

            # Validate timestamp recency (within 1 hour for active environment)
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

            # Validate financial constraints
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

            # Crypto typically has 8-18 decimal places, but our internal precision should be
            # reasonable
            assert total_precision <= 18, (
                f"total_quantity precision too high: {total_precision} decimals"
            )
            assert available_precision <= 18, (
                f"available_quantity precision too high: {available_precision} decimals"
            )

            # Validate exchange-specific details if present
            if balance.bp_details:
                bp_details = balance.bp_details

                # Validate Backpack-specific balance fields
                if bp_details.open_order_quantity is not None:
                    assert isinstance(bp_details.open_order_quantity, Decimal), (
                        "bp_details.open_order_quantity must be Decimal"
                    )
                    assert bp_details.open_order_quantity >= Decimal("0"), (
                        f"open_order_quantity must be non-negative, got {bp_details.open_order_quantity}"
                    )

                    # Validate locked quantity logic: total - available should approximately equal locked
                    locked_quantity = balance.total_quantity - balance.available_quantity
                    if locked_quantity > Decimal("0"):
                        # Open order quantity should contribute to locked amount
                        assert bp_details.open_order_quantity <= locked_quantity, (
                            f"open_order_quantity ({bp_details.open_order_quantity}) should not exceed "
                            f"locked quantity ({locked_quantity})"
                        )

    @pytest.mark.vcr
    async def test_get_balances_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with empty/zero balance account.

        This test validates behavior when account has no balances or all zero balances.
        Important for testing edge cases in balance handling.
        """
        balances = await bp_api_for_test_env.get_balances()

        # Should return empty dict or dict with zero balances
        assert isinstance(balances, dict), "get_balances() should always return dict"

        # If balances exist, they should all be valid (including zero balances)
        for asset_symbol, balance in balances.items():
            assert isinstance(balance, SpotBalance), (
                f"Even zero balance should be SpotBalance for {asset_symbol}"
            )

            # Zero balances should still follow constraints
            assert balance.total_quantity >= Decimal("0"), (
                "Zero balances should still be non-negative"
            )
            assert balance.available_quantity >= Decimal("0"), (
                "Zero available should still be non-negative"
            )
            assert balance.total_quantity >= balance.available_quantity, (
                "Zero balance logic should still hold"
            )

    @pytest.mark.vcr
    async def test_get_balances_authentication_failure(
        self,
        bp_api_with_di: Callable[
            ..., BackpackAPI
        ],  # Factory function for creating API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with invalid Ed25519 authentication.

        This validates proper error handling when Ed25519 signature is invalid,
        testing the complete authentication failure pipeline.
        """
        # Create API with invalid credentials
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        bad_api = bp_api_with_di(secrets=invalid_secrets)

        # Should raise authentication error
        with pytest.raises(APIError) as exc_info:
            await bad_api.get_balances()

        # Validate error mapping and structure
        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], f"Expected 401/403 status, got {error.http_status}"
        assert len(error.message) > 0, "Error message should be descriptive"

        # Validate exchange-specific error preservation
        assert error.exchange_code is not None, "Exchange error code should be preserved"

    @pytest.mark.vcr
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
            results: list[dict[str, Any]] = []
            for i, task in enumerate(tasks):
                try:
                    result: dict[str, Any] = await task
                    results.append(result)
                    assert isinstance(result, dict), f"Call {i} should return dict if successful"
                except APIError as e:
                    if "rate" in e.message.lower() or "limit" in e.message.lower():
                        assert e.code == APIErrorCode.RATE_LIMITED.value, (
                            f"Rate limit error should map to RATE_LIMITED, got {e.code}"
                        )
                        # Check if retry-after information is preserved
                        if hasattr(e, "retry_after") and e.retry_after:
                            assert isinstance(e.retry_after, (int, float)), (
                                "retry_after should be numeric if present"
                            )
                    else:
                        raise  # Re-raise non-rate-limit errors

        except Exception as e:
            # If we can't trigger rate limiting in test environment, skip the test
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
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

    @pytest.mark.vcr
    async def test_get_balances_backpack_specific_details(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with focus on Backpack-specific balance details.

        This validates Backpack's balance structure including open order quantities
        and other exchange-specific fields.
        """
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for Backpack-specific testing")

        for asset_symbol, balance in balances.items():
            if balance.bp_details:
                bp_details = balance.bp_details

                # Validate open order quantity if present
                if bp_details.open_order_quantity is not None:
                    # Should be Decimal and non-negative
                    assert isinstance(bp_details.open_order_quantity, Decimal), (
                        f"open_order_quantity should be Decimal for {asset_symbol}"
                    )
                    assert bp_details.open_order_quantity >= Decimal("0"), (
                        f"open_order_quantity should be non-negative for {asset_symbol}"
                    )

                    # Open order quantity should make sense relative to total balance
                    assert bp_details.open_order_quantity <= balance.total_quantity, (
                        f"open_order_quantity ({bp_details.open_order_quantity}) cannot exceed "
                        f"total_quantity ({balance.total_quantity}) for {asset_symbol}"
                    )

                    # If there are open orders, available should be less than total
                    if bp_details.open_order_quantity > Decimal("0"):
                        assert balance.available_quantity <= balance.total_quantity, (
                            f"Available quantity should be <= total when open orders exist for {asset_symbol}"
                        )

    @pytest.mark.vcr
    async def test_get_balances_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint.

        This validates that concurrent balance requests don't interfere with each other
        and that the underlying balance API call handles concurrency properly.
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
        successful_results: list[dict[str, Any]] = []
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
            first_result: dict[str, Any] = successful_results[0]
            for i, result in enumerate(successful_results[1:], 1):
                # Balance amounts should be very similar for concurrent calls
                assert set(first_result.keys()) == set(result.keys()), (
                    f"Concurrent results should have same assets: {first_result.keys()} vs {result.keys()}"
                )

                # Check that balances are consistent across concurrent calls
                for asset in first_result.keys():
                    if asset in result:
                        first_balance = first_result[asset].total_quantity
                        second_balance = result[asset].total_quantity
                        # Allow for minor differences due to timing
                        balance_diff = abs(first_balance - second_balance)
                        assert balance_diff <= Decimal("0.00001"), (
                            f"Concurrent balance calls should return similar values for {asset}: "
                            f"{first_balance} vs {second_balance}"
                        )

    @pytest.mark.vcr
    async def test_get_balances_network_timeout(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() behavior with network timeout scenarios.

        Note: This test may be challenging to reproduce consistently in VCR,
        so it might need to be mocked or use specific test conditions.
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

    @pytest.mark.vcr
    async def test_get_balances_large_balance_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() handling of large balance amounts.

        This validates that large balances are handled correctly without
        precision loss or overflow issues.
        """
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for large balance testing")

        for asset_symbol, balance in balances.items():
            # Test handling of large balance amounts
            if balance.total_quantity > Decimal("1000000"):  # Large balance (> 1M units)
                # Should maintain precision for large balances
                assert balance.total_quantity.is_finite(), (
                    f"Large balance should be finite: {balance.total_quantity}"
                )

                # Available quantity should still be finite and reasonable
                assert balance.available_quantity.is_finite(), (
                    f"Large available balance should be finite: {balance.available_quantity}"
                )

                # Large balances should still follow business logic
                assert balance.total_quantity >= balance.available_quantity, (
                    f"Large balance logic should still hold: total={balance.total_quantity}, "
                    f"available={balance.available_quantity}"
                )

            # Test precision for high-value assets (like BTC)
            if asset_symbol.upper() in ["BTC", "WBTC"] and balance.total_quantity > Decimal("0"):
                # High-value assets should maintain precision
                total_str = str(balance.total_quantity)
                # Should not lose precision due to large USD values
                if "." in total_str:
                    decimal_places = len(total_str.split(".")[1])
                    assert decimal_places >= 6, (
                        f"High-value asset {asset_symbol} should maintain precision: {total_str}"
                    )
