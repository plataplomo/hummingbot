"""Integration tests for Backpack private account summary endpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/account_summary"],
    indirect=True,
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
        """Test successful get_account_summary() with comprehensive validation."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        assert account_summary.timestamp is not None
        time_diff = datetime.now(account_summary.timestamp.tzinfo) - account_summary.timestamp
        assert time_diff.total_seconds() < 3600

        # Validate margin requirements - they may be None for zero balance accounts
        assert account_summary.total_initial_margin_required is None or isinstance(
            account_summary.total_initial_margin_required,
            Decimal,
        )
        assert account_summary.total_maintenance_margin_required is None or isinstance(
            account_summary.total_maintenance_margin_required,
            Decimal,
        )
        if account_summary.total_initial_margin_required is not None:
            assert account_summary.total_initial_margin_required >= Decimal(0)
        if account_summary.total_maintenance_margin_required is not None:
            assert account_summary.total_maintenance_margin_required >= Decimal(0)

        # Validate equity is present - may be zero for zero balance accounts
        assert isinstance(account_summary.total_equity, Decimal)
        assert account_summary.total_equity >= Decimal(0)

        # Validate backpack-specific details
        assert account_summary.bp_details is not None
        assert isinstance(account_summary.bp_details, BackpackMarginDetails)

        # Validate string representations for non-zero values
        if account_summary.total_equity > Decimal(0):
            assert len(str(account_summary.total_equity)) > 0
            assert "." in str(account_summary.total_equity) or account_summary.total_equity == int(
                account_summary.total_equity,
            )

        if (
            account_summary.total_initial_margin_required is not None
            and account_summary.total_initial_margin_required > Decimal(0)
        ):
            assert len(str(account_summary.total_initial_margin_required)) > 0

        if (
            account_summary.total_maintenance_margin_required is not None
            and account_summary.total_maintenance_margin_required > Decimal(0)
        ):
            assert len(str(account_summary.total_maintenance_margin_required)) > 0

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

        try:
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
        finally:
            await bad_api.close()

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
        initial_margin = account_summary.total_initial_margin_required
        maintenance_margin = account_summary.total_maintenance_margin_required

        if equity > Decimal(0):
            initial_ratio = (
                initial_margin / equity
                if initial_margin and initial_margin > Decimal(0)
                else Decimal(0)
            )
            maintenance_ratio = (
                maintenance_margin / equity
                if maintenance_margin and maintenance_margin > Decimal(0)
                else Decimal(0)
            )

            assert initial_ratio <= Decimal(1)
            assert maintenance_ratio <= Decimal(1)
            assert maintenance_ratio <= initial_ratio

        available_margin = equity - (initial_margin or Decimal(0))
        if available_margin > Decimal(0):
            assert account_summary.bp_details is not None
            if account_summary.bp_details and account_summary.bp_details.assets_value:
                # Assets value might be slightly higher than equity due to timing differences
                # between when collateral and main account endpoints are called
                # Allow for small precision differences
                from tests.integration.apis.backpack.shared.bp_test_helpers import (
                    SMALL_VALUE_TOLERANCE,
                )

                assert account_summary.bp_details.assets_value <= equity + SMALL_VALUE_TOLERANCE
