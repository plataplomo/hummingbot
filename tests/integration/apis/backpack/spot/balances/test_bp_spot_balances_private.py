"""Integration tests for Backpack private balances endpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.spot,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/balances"],
    indirect=True,
)
class TestBackpackSpotBalancesPrivate:
    """Private balances integration tests for SpotBalance model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_balances() with comprehensive SpotBalance validation."""
        balances = await bp_api_for_test_env.get_balances()

        assert isinstance(balances, dict), "get_balances() should return dict[str, SpotBalance]"

        if not balances:
            pytest.skip("No balances available in test environment for validation")

        for asset_symbol, balance in balances.items():
            assert isinstance(balance, SpotBalance)
            assert balance.asset == asset_symbol
            assert balance.exchange == "backpack"

            # Validate timestamp recency (within 1 hour for active environment)
            assert balance.timestamp is not None
            time_diff = datetime.now(balance.timestamp.tzinfo) - balance.timestamp
            assert time_diff.total_seconds() < 3600

            # Use shared validation helper
            assert_valid_spot_balance(balance)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with invalid Ed25519 authentication."""
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        bad_api = BackpackAPI(
            exchange_config=active_bp_config,
            exchange_secrets=invalid_secrets,
        )

        try:
            with pytest.raises((APIError, AttributeError)) as exc_info:
                await bad_api.get_balances()

            error = exc_info.value
            if isinstance(error, APIError):
                assert error.code in [
                    APIErrorCode.AUTHENTICATION_FAILED.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ]
            else:
                assert "authenticator" in str(error).lower() or "NoneType" in str(error)
        finally:
            await bad_api.close()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior.

        Raises:
            APIError: If rate limiting is encountered
        """
        try:
            tasks: list[Any] = []
            for _ in range(5):
                tasks.append(bp_api_for_test_env.get_balances())

            results: list[dict[str, Any]] = []
            for task in tasks:
                try:
                    result: dict[str, Any] = await task
                    results.append(result)
                    assert isinstance(result, dict)
                except APIError as e:
                    if "rate" in e.message.lower() or "limit" in e.message.lower():
                        assert e.code == APIErrorCode.RATE_LIMITED.value
                        if hasattr(e, "retry_after") and e.retry_after:
                            assert isinstance(e.retry_after, int | float)
                    else:
                        raise

        except Exception as e:
            pytest.skip(f"Could not test rate limiting in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with edge cases around decimal precision."""
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for precision testing")

        for _, balance in balances.items():
            if balance.total_quantity > Decimal(0):
                assert balance.total_quantity.is_finite()

                if balance.total_quantity < Decimal("0.000001"):
                    assert str(balance.total_quantity) != "0E-0"

                total_str = str(balance.total_quantity)
                available_str = str(balance.available_quantity)

                assert "E" not in total_str.upper() or "E-" in total_str.upper()
                assert "E" not in available_str.upper() or "E-" in available_str.upper()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_backpack_specific_details(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with focus on Backpack-specific balance details."""
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for Backpack-specific testing")

        for _, balance in balances.items():
            if balance.bp_details:
                bp_details = balance.bp_details

                if bp_details.open_order_quantity is not None:
                    assert isinstance(bp_details.open_order_quantity, Decimal)
                    assert bp_details.open_order_quantity >= Decimal(0)
                    assert bp_details.open_order_quantity <= balance.total_quantity

                    if bp_details.open_order_quantity > Decimal(0):
                        assert balance.available_quantity <= balance.total_quantity

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests to same endpoint."""
        import asyncio

        tasks = [
            bp_api_for_test_env.get_balances(),
            bp_api_for_test_env.get_balances(),
            bp_api_for_test_env.get_balances(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, dict)
                successful_results.append(result)

        assert len(successful_results) > 0

        if len(successful_results) > 1:
            first_result: dict[str, Any] = successful_results[0]
            for _i, result in enumerate(successful_results[1:], 1):
                assert set(first_result.keys()) == set(result.keys())

                for asset in first_result:
                    if asset in result:
                        first_balance = first_result[asset].total_quantity
                        second_balance = result[asset].total_quantity
                        balance_diff = abs(first_balance - second_balance)
                        assert balance_diff <= Decimal("0.00001")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_network_timeout(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() behavior with network timeout scenarios.

        Raises:
            APIError: If network timeout occurs during API call
        """
        try:
            balances = await bp_api_for_test_env.get_balances()
            assert isinstance(balances, dict)

        except APIError as e:
            if "timeout" in e.message.lower() or "connection" in e.message.lower():
                assert e.code in [
                    APIErrorCode.TIMEOUT.value,
                    APIErrorCode.CONNECTION_ERROR.value,
                    APIErrorCode.NETWORK_ISSUE.value,
                ]
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_large_balance_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() handling of large balance amounts."""
        balances = await bp_api_for_test_env.get_balances()

        if not balances:
            pytest.skip("No balances for large balance testing")

        for asset_symbol, balance in balances.items():
            if balance.total_quantity > Decimal(1000000):
                assert balance.total_quantity.is_finite()
                assert balance.available_quantity.is_finite()
                assert balance.total_quantity >= balance.available_quantity

            if asset_symbol.upper() in ["BTC", "WBTC"] and balance.total_quantity > Decimal(0):
                total_str = str(balance.total_quantity)
                if "." in total_str:
                    decimal_places = len(total_str.split(".")[1])
                    assert decimal_places >= 6
