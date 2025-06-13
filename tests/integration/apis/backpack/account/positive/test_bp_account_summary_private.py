"""Integration tests for Backpack private account summary endpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.margin_account import MarginAccountSummary

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/account_summary"], indirect=True
)
class TestBackpackAccountSummaryPrivate:
    """Private account summary integration tests for MarginAccountSummary."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_account_summary() with comprehensive MarginAccountSummary validation."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        assert account_summary.timestamp is not None
        time_diff = datetime.now(account_summary.timestamp.tzinfo) - account_summary.timestamp
        assert time_diff.total_seconds() < 3600

        assert isinstance(account_summary.total_equity, Decimal)
        assert isinstance(account_summary.initial_margin_requirement, Decimal)
        assert isinstance(account_summary.maintenance_margin_requirement, Decimal)

        assert account_summary.total_equity >= Decimal("0")
        assert account_summary.initial_margin_requirement >= Decimal("0")
        assert account_summary.maintenance_margin_requirement >= Decimal("0")
        assert account_summary.initial_margin_requirement >= account_summary.maintenance_margin_requirement

        if account_summary.bp_details:
            bp_details = account_summary.bp_details
            if bp_details.available_balance is not None:
                assert isinstance(bp_details.available_balance, Decimal)
                assert bp_details.available_balance >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with invalid Ed25519 authentication."""
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        bad_api = BackpackAPI(
            exchange_config=active_bp_config,
            exchange_secrets=invalid_secrets,
        )

        with pytest.raises((APIError, AttributeError)) as exc_info:
            await bad_api.get_account_summary()

        error = exc_info.value
        if isinstance(error, APIError):
            assert error.code in [
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.INVALID_REQUEST.value,
            ]
        else:
            assert "authenticator" in str(error).lower() or "NoneType" in str(error)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_margin_calculations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() margin calculation validation."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)

        equity = account_summary.total_equity
        initial_margin = account_summary.initial_margin_requirement
        maintenance_margin = account_summary.maintenance_margin_requirement

        if equity > Decimal("0"):
            initial_ratio = initial_margin / equity if initial_margin > Decimal("0") else Decimal("0")
            maintenance_ratio = maintenance_margin / equity if maintenance_margin > Decimal("0") else Decimal("0")

            assert initial_ratio <= Decimal("1")
            assert maintenance_ratio <= Decimal("1")
            assert maintenance_ratio <= initial_ratio

        available_margin = equity - initial_margin
        if available_margin > Decimal("0"):
            assert account_summary.bp_details is not None
            if account_summary.bp_details and account_summary.bp_details.available_balance:
                assert account_summary.bp_details.available_balance <= available_margin