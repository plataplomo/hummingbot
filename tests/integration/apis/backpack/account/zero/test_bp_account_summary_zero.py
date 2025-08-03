"""Integration tests for Backpack private account summary endpoints with zero balance."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.margin_account import MarginAccountSummary


logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/account_summary"],
    indirect=True,
)
class TestBackpackAccountSummaryZero:
    """Account summary integration tests specifically for zero balance scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_empty_account(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with empty account (zero balance)."""
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        assert account_summary.timestamp is not None
        time_diff = datetime.now(account_summary.timestamp.tzinfo) - account_summary.timestamp
        assert time_diff.total_seconds() < 3600

        # For zero balance accounts, these fields may be None from the API
        assert isinstance(account_summary.total_equity, Decimal)
        assert account_summary.total_initial_margin_required is None or isinstance(
            account_summary.total_initial_margin_required,
            Decimal,
        )
        assert account_summary.total_maintenance_margin_required is None or isinstance(
            account_summary.total_maintenance_margin_required,
            Decimal,
        )

        assert account_summary.total_equity >= Decimal(0)
        if account_summary.total_initial_margin_required is not None:
            assert account_summary.total_initial_margin_required >= Decimal(0)
        if account_summary.total_maintenance_margin_required is not None:
            assert account_summary.total_maintenance_margin_required >= Decimal(0)

        # With zero balance, Backpack may still return base margin requirements
        # These represent the minimum margin factors that would apply if positions were opened
        if account_summary.total_equity == Decimal(0):
            # Margin requirements can be non-zero even with zero equity as they represent
            # base margin factors from the exchange
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required >= Decimal(0)
            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required >= Decimal(0)

        logger.info(
            "zero_balance_account_summary",
            total_equity=account_summary.total_equity,
            message="Zero balance account summary",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_zero_precision(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() decimal precision with zero values."""
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        assert isinstance(account_summary.total_equity, Decimal)
        # These fields may be None for zero balance accounts
        assert account_summary.total_initial_margin_required is None or isinstance(
            account_summary.total_initial_margin_required,
            Decimal,
        )
        assert account_summary.total_maintenance_margin_required is None or isinstance(
            account_summary.total_maintenance_margin_required,
            Decimal,
        )

        if account_summary.total_equity == Decimal(0):
            assert str(account_summary.total_equity) in ["0", "0.0"]

        if (
            account_summary.total_initial_margin_required is not None
            and account_summary.total_initial_margin_required == Decimal(0)
        ):
            assert str(account_summary.total_initial_margin_required) in ["0", "0.0"]

        if (
            account_summary.total_maintenance_margin_required is not None
            and account_summary.total_maintenance_margin_required == Decimal(0)
        ):
            assert str(account_summary.total_maintenance_margin_required) in ["0", "0.0"]

        # Calculate sum only for non-None values
        equity_sum = account_summary.total_equity
        if account_summary.total_initial_margin_required is not None:
            equity_sum += account_summary.total_initial_margin_required
        if account_summary.total_maintenance_margin_required is not None:
            equity_sum += account_summary.total_maintenance_margin_required

        assert equity_sum.is_finite()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_structure_validation(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() structure validation with zero balance."""
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        assert hasattr(account_summary, "total_equity")
        assert hasattr(account_summary, "total_initial_margin_required")
        assert hasattr(account_summary, "total_maintenance_margin_required")
        assert hasattr(account_summary, "exchange")
        assert hasattr(account_summary, "timestamp")
        assert hasattr(account_summary, "bp_details")

        # BackpackMarginDetails doesn't have available_balance - this was the wrong assumption
        if account_summary.bp_details:
            # Check for attributes that actually exist in BackpackMarginDetails
            assert hasattr(account_summary.bp_details, "assets_value")
            assert hasattr(account_summary.bp_details, "borrow_liability")
            assert hasattr(account_summary.bp_details, "liabilities_value")
            assert hasattr(account_summary.bp_details, "imf_raw")
            assert hasattr(account_summary.bp_details, "mmf_raw")

        assert account_summary.exchange == "backpack"

        now = datetime.now(UTC)
        time_delta = now - account_summary.timestamp.replace(tzinfo=UTC)
        assert time_delta < timedelta(hours=1)
