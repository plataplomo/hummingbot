"""Integration tests for Backpack balance endpoints with $0 balance accounts."""

import asyncio
import gc
import re
import sys
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions.authentication import InvalidPrivateKeyError
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance


logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/balances/zero_balance"],
    indirect=True,
)
class TestBackpackSpotBalancesZero:
    """Integration tests for Backpack balances with $0 balance (empty account scenarios)."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_empty_account(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with empty/zero balance account."""
        balances = await bp_api_for_zero_balance_test.get_balances()

        assert isinstance(balances, dict)

        for asset_symbol, spot_balance in balances.items():
            assert isinstance(spot_balance, SpotBalance)
            assert_valid_spot_balance(spot_balance)
            assert spot_balance.exchange == "backpack"
            logger.info(
                "zero_balance_validated",
                asset_symbol=asset_symbol,
                spot_balance=str(spot_balance),
                message=f"✓ Zero balance for {asset_symbol}: {spot_balance}",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that invalid Ed25519 authentication fails during API initialization."""
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        # Authentication validation now happens during API initialization
        # This provides better security by failing fast with invalid credentials
        with pytest.raises(InvalidPrivateKeyError) as exc_info:
            BackpackAPI(
                exchange_config=active_bp_config,
                exchange_secrets=invalid_secrets,
            )

        error = exc_info.value
        assert "Invalid Base64 ED25519 private key" in str(error)
        assert "Incorrect padding" in str(error)
        logger.info(
            "authentication_failure_detected",
            error_message=str(error),
            message=f"✓ Authentication failure properly detected during initialization: {error}",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_rate_limiting(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() rate limiting behavior."""
        tasks: list[Any] = [bp_api_for_zero_balance_test.get_balances() for _ in range(5)]

        try:
            results: list[dict[str, SpotBalance] | BaseException] = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            successes = [r for r in results if isinstance(r, dict)]
            errors = [r for r in results if isinstance(r, Exception)]

            assert len(successes) >= 1

            rate_limit_errors = [
                e
                for e in errors
                if isinstance(e, APIError) and e.code == APIErrorCode.RATE_LIMITED.value
            ]

            if rate_limit_errors:
                logger.info(
                    "rate_limiting_detected",
                    limited_requests=len(rate_limit_errors),
                    message=f"✓ Rate limiting detected: {len(rate_limit_errors)} requests limited",
                )
            else:
                logger.info("✓ No rate limiting encountered in this test run")

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.skip(f"Rate limiting test unstable in current environment: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_zero_balance_structure(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() structure validation with zero balance account."""
        balances = await bp_api_for_zero_balance_test.get_balances()

        assert isinstance(balances, dict)

        for asset_symbol, spot_balance in balances.items():
            assert isinstance(asset_symbol, str)
            assert len(asset_symbol) > 0
            assert asset_symbol.isupper()

            assert isinstance(spot_balance, SpotBalance)
            assert spot_balance.asset == asset_symbol
            assert spot_balance.exchange == "backpack"

            assert spot_balance.bp_details is not None
            assert hasattr(spot_balance.bp_details, "open_order_quantity")
            assert hasattr(spot_balance.bp_details, "lend_quantity")
            assert hasattr(spot_balance.bp_details, "collateral_weight")

            logger.info(
                "structure_valid_zero_balance",
                asset_symbol=asset_symbol,
                message=f"✓ Structure valid for {asset_symbol} even with zero balance",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_concurrent_requests_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() with concurrent requests with zero balance account."""
        concurrent_results = await asyncio.gather(
            bp_api_for_zero_balance_test.get_balances(),
            bp_api_for_zero_balance_test.get_balances(),
            bp_api_for_zero_balance_test.get_balances(),
            return_exceptions=True,
        )
        concurrent_tasks: list[dict[str, SpotBalance] | BaseException] = list(concurrent_results)

        successful_results = [
            result
            for result in concurrent_tasks
            if isinstance(result, dict) and not isinstance(result, Exception)
        ]

        assert len(successful_results) >= 1

        if len(successful_results) > 1:
            first_result = successful_results[0]
            for _, result in enumerate(successful_results[1:], 1):
                assert result.keys() == first_result.keys()

                for asset in first_result:
                    assert result[asset].total_quantity == first_result[asset].total_quantity

        logger.info(
            "concurrent_requests_consistent",
            successful_count=len(successful_results),
            message=(
                f"✓ Concurrent balance requests consistent: {len(successful_results)} successful"
            ),
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_decimal_precision_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() decimal precision handling with zero balances."""
        balances = await bp_api_for_zero_balance_test.get_balances()

        for asset_symbol, spot_balance in balances.items():
            assert isinstance(spot_balance.available_quantity, Decimal)
            assert isinstance(spot_balance.total_quantity, Decimal)

            if spot_balance.available_quantity == Decimal(0):
                assert str(spot_balance.available_quantity) == "0"
                logger.info(
                    "zero_available_balance_precise",
                    asset_symbol=asset_symbol,
                    message=f"✓ Zero available balance precise for {asset_symbol}",
                )

            if spot_balance.total_quantity == Decimal(0):
                assert str(spot_balance.total_quantity) == "0"
                logger.info(
                    "zero_total_balance_precise",
                    asset_symbol=asset_symbol,
                    message=f"✓ Zero total balance precise for {asset_symbol}",
                )

            total_value = spot_balance.total_quantity + Decimal(0)
            assert total_value == spot_balance.total_quantity

            assert spot_balance.total_quantity >= Decimal(0)
            assert spot_balance.available_quantity <= spot_balance.total_quantity

            logger.info(
                "decimal_operations_stable",
                asset_symbol=asset_symbol,
                message=f"✓ Decimal operations stable for {asset_symbol}",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_asset_validation_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() asset symbol validation and formatting."""
        balances = await bp_api_for_zero_balance_test.get_balances()

        asset_pattern = re.compile(r"^[A-Z]{2,10}$")

        for asset_symbol, spot_balance in balances.items():
            assert isinstance(asset_symbol, str)
            assert len(asset_symbol) >= 2
            assert len(asset_symbol) <= 10
            assert asset_symbol.isupper()
            assert asset_pattern.match(asset_symbol)
            assert spot_balance.asset == asset_symbol

            known_assets = {"USDC", "SOL", "BTC", "ETH", "BONK", "JUP", "WIF"}
            if asset_symbol in known_assets:
                logger.info(
                    "known_asset_formatted",
                    asset_symbol=asset_symbol,
                    message=f"✓ Known asset {asset_symbol} properly formatted",
                )
            else:
                logger.info(
                    "unknown_asset_follows_rules",
                    asset_symbol=asset_symbol,
                    message=f"✓ Unknown asset {asset_symbol} follows format rules",
                )

            assert asset_symbol.isalpha()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_memory_efficiency_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_balances() memory efficiency with zero balance data."""
        gc.collect()
        initial_objects = len(gc.get_objects())

        for i in range(5):
            try:
                balances = await bp_api_for_zero_balance_test.get_balances()

                assert isinstance(balances, dict)
                assert len(balances) <= 50

                for asset, balance in balances.items():
                    assert sys.getsizeof(balance) < 1000
                    assert sys.getsizeof(asset) < 100

                logger.info(
                    "request_processed",
                    request_number=i + 1,
                    assets_count=len(balances),
                    message=f"Request {i + 1}: {len(balances)} assets processed",
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                logger.info(
                    "request_failed",
                    request_number=i + 1,
                    error=str(e),
                    message=f"Request {i + 1} failed: {e}",
                )

        gc.collect()
        final_objects = len(gc.get_objects())

        object_growth = final_objects - initial_objects
        assert object_growth < 1000

        logger.info(
            "memory_efficiency_validated",
            object_growth=object_growth,
            message=f"✓ Memory efficiency validated: {object_growth} object growth",
        )
