"""Integration tests for Backpack private account summary endpoints with zero balance."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.margin_account import MarginAccountSummary

logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.zero_balance
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/account_summary"], indirect=True
)
class TestBackpackAccountSummaryZero:
    """Account summary integration tests specifically for zero balance scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with empty account (zero balance)."""
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

        if account_summary.total_equity == Decimal("0"):
            assert account_summary.initial_margin_requirement == Decimal("0")
            assert account_summary.maintenance_margin_requirement == Decimal("0")

        logger.info(f"Zero balance account summary: equity={account_summary.total_equity}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_zero_precision(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() decimal precision with zero values."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary.total_equity, Decimal)
        assert isinstance(account_summary.initial_margin_requirement, Decimal)
        assert isinstance(account_summary.maintenance_margin_requirement, Decimal)

        if account_summary.total_equity == Decimal("0"):
            assert str(account_summary.total_equity) == "0"

        if account_summary.initial_margin_requirement == Decimal("0"):
            assert str(account_summary.initial_margin_requirement) == "0"

        if account_summary.maintenance_margin_requirement == Decimal("0"):
            assert str(account_summary.maintenance_margin_requirement) == "0"

        equity_sum = (
            account_summary.total_equity +
            account_summary.initial_margin_requirement +
            account_summary.maintenance_margin_requirement
        )
        assert equity_sum.is_finite()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_structure_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() structure validation with zero balance."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert hasattr(account_summary, "total_equity")
        assert hasattr(account_summary, "initial_margin_requirement")
        assert hasattr(account_summary, "maintenance_margin_requirement")
        assert hasattr(account_summary, "exchange")
        assert hasattr(account_summary, "timestamp")
        assert hasattr(account_summary, "bp_details")

        if account_summary.bp_details:
            assert hasattr(account_summary.bp_details, "available_balance")

        assert account_summary.exchange == "backpack"

        now = datetime.now(UTC)
        time_delta = now - account_summary.timestamp.replace(tzinfo=UTC)
        assert time_delta < timedelta(hours=1)