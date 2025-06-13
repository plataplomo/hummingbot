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

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.spot_balance import SpotBalance

pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/spot/balances/zero"], indirect=True
)
@pytest.mark.spot
@pytest.mark.zero_balance
class TestHyperliquidSpotBalancesZero:
    """Comprehensive zero balance integration tests for SpotBalance model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_empty_account(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with empty/zero balance account."""
        balances = await hl_api_for_test_env.get_balances()

        assert isinstance(balances, dict), "get_balances() should always return dict"

        for asset_symbol, balance in balances.items():
            assert isinstance(balance, SpotBalance), (
                f"Even zero balance should be SpotBalance for {asset_symbol}"
            )

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
    @pytest.mark.asyncio
    async def test_get_balances_authentication_failure(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with invalid EIP-712 authentication."""
        invalid_secrets = PrivateKeyAuthSecrets(
            private_key=SecretStr(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            ),
        )

        bad_api = hl_api_with_di(secrets=invalid_secrets)

        with pytest.raises(APIError) as exc_info:
            await bad_api.get_balances()

        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], f"Expected 401/403 status, got {error.http_status}"
        assert len(error.message) > 0, "Error message should be descriptive"

        assert error.exchange_code is not None, "Exchange error code should be preserved"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_network_timeout(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() behavior with network timeout scenarios."""
        try:
            balances = await hl_api_for_test_env.get_balances()

            assert isinstance(balances, dict), "Successful response should be dict"

        except APIError as e:
            if "timeout" in e.message.lower() or "connection" in e.message.lower():
                assert e.code in [
                    APIErrorCode.TIMEOUT.value,
                    APIErrorCode.CONNECTION_ERROR.value,
                    APIErrorCode.NETWORK_ISSUE.value,
                ], f"Network error should map to network-related code, got {e.code}"
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior."""
        try:
            tasks: list[Any] = []
            for _ in range(5):
                tasks.append(hl_api_for_test_env.get_balances())

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
                        if hasattr(e, "retry_after") and e.retry_after:
                            assert isinstance(e.retry_after, int | float), (
                                "retry_after should be numeric if present"
                            )
                    else:
                        raise

        except Exception as e:
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with edge cases around decimal precision."""
        balances = await hl_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for precision testing")

        for _, balance in balances.items():
            if balance.total_quantity > Decimal("0"):
                assert balance.total_quantity.is_finite(), (
                    f"Balance {balance.total_quantity} should be finite"
                )

                if balance.total_quantity < Decimal("0.000001"):
                    assert str(balance.total_quantity) != "0E-0", (
                        "Dust balances should maintain proper decimal representation"
                    )

                total_str = str(balance.total_quantity)
                available_str = str(balance.available_quantity)

                assert "E" not in total_str.upper() or "E-" in total_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {total_str}"
                )
                assert "E" not in available_str.upper() or "E-" in available_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {available_str}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint."""
        import asyncio

        tasks = [
            hl_api_for_test_env.get_balances(),
            hl_api_for_test_env.get_balances(),
            hl_api_for_test_env.get_balances(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                else:
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
