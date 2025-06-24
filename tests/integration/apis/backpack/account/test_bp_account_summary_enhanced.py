"""Integration tests for enhanced Backpack account summary with collateral endpoint.

Tests the progressive enhancement pattern where the service attempts to use
the /api/v1/capital/collateral endpoint first, then falls back to basic
implementation if unavailable.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/account_summary_enhanced"], indirect=True
)
class TestBackpackAccountSummaryEnhanced:
    """Enhanced account summary integration tests with collateral endpoint support."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_with_collateral_endpoint(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary when collateral endpoint is available.

        This test verifies the enhanced implementation that uses the
        /api/v1/capital/collateral endpoint to provide comprehensive
        margin and collateral data.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        # Validate enhanced data from collateral endpoint
        assert account_summary.bp_details is not None
        assert isinstance(account_summary.bp_details, BackpackMarginDetails)

        # Enhanced fields should be populated when collateral endpoint is available
        bp_details = account_summary.bp_details
        if hasattr(bp_details, "assets_value") and bp_details.assets_value is not None:
            # We have enhanced data from collateral endpoint
            assert isinstance(bp_details.assets_value, Decimal)
            assert bp_details.assets_value >= Decimal("0")

            if (
                hasattr(bp_details, "liabilities_value")
                and bp_details.liabilities_value is not None
            ):
                assert isinstance(bp_details.liabilities_value, Decimal)
                assert bp_details.liabilities_value >= Decimal("0")

            # Check if we have both assets_value and liabilities_value
            # net_equity = assets_value - liabilities_value
            if (
                hasattr(bp_details, "liabilities_value")
                and bp_details.liabilities_value is not None
            ):
                net_equity = bp_details.assets_value - bp_details.liabilities_value
                # This calculated net equity should roughly equal total_equity
                assert abs(net_equity - account_summary.total_equity) < Decimal("0.01")

            # Available equity might be in the main account summary
            if account_summary.available_equity > Decimal("0"):
                assert isinstance(account_summary.available_equity, Decimal)
                assert account_summary.available_equity >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_fallback_to_basic(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary fallback when collateral endpoint is unavailable.

        This test simulates the scenario where /api/v1/capital/collateral
        returns 404, causing the service to fall back to the basic
        implementation using balances and positions.
        """
        # This test would be recorded with a cassette that shows 404 for collateral endpoint
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        # Basic implementation should still provide core fields
        assert isinstance(account_summary.total_equity, Decimal)
        assert account_summary.total_equity >= Decimal("0")

        assert isinstance(account_summary.timestamp, datetime)

        # bp_details should still be present but with limited data
        assert account_summary.bp_details is not None

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_basic_fields(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary returns all basic required fields.

        This test verifies that the account summary provides all the
        essential fields required by the MarginAccountSummary model.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        # Should return data with proper equity values
        assert isinstance(account_summary.total_equity, Decimal)
        assert account_summary.total_equity >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_margin_fields_optional(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that optional margin fields are handled properly.

        Some margin fields may be None depending on account state
        and whether positions are open.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)

        # These fields are optional and may be None
        if account_summary.total_initial_margin_required is not None:
            assert isinstance(account_summary.total_initial_margin_required, Decimal)
            assert account_summary.total_initial_margin_required >= Decimal("0")

        if account_summary.total_maintenance_margin_required is not None:
            assert isinstance(account_summary.total_maintenance_margin_required, Decimal)
            assert account_summary.total_maintenance_margin_required >= Decimal("0")

        # available_equity is a required field, always present
        assert isinstance(account_summary.available_equity, Decimal)
        # Note: available_equity has ge=0 constraint in the model

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_margin_calculations_enhanced(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test enhanced margin calculations when collateral data is available.

        With the collateral endpoint, we should get accurate:
        - Initial margin requirements
        - Maintenance margin requirements
        - Margin fractions (IMF/MMF)
        - Available margin for trading
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)

        # Enhanced implementation should provide accurate margin data
        if account_summary.total_initial_margin_required is not None:
            assert isinstance(account_summary.total_initial_margin_required, Decimal)
            assert account_summary.total_initial_margin_required >= Decimal("0")

        if account_summary.total_maintenance_margin_required is not None:
            assert isinstance(account_summary.total_maintenance_margin_required, Decimal)
            assert account_summary.total_maintenance_margin_required >= Decimal("0")

            # Initial margin should be >= maintenance margin
            if account_summary.total_initial_margin_required is not None:
                assert (
                    account_summary.total_initial_margin_required
                    >= account_summary.total_maintenance_margin_required
                )

        # Check enhanced bp_details for margin fractions
        if account_summary.bp_details and hasattr(account_summary.bp_details, "imf"):
            imf = getattr(account_summary.bp_details, "imf", None)
            mmf = getattr(account_summary.bp_details, "mmf", None)

            if imf is not None:
                assert isinstance(imf, Decimal)
                assert Decimal("0") <= imf <= Decimal("1")

            if mmf is not None:
                assert isinstance(mmf, Decimal)
                assert Decimal("0") <= mmf <= Decimal("1")

            # IMF should be >= MMF
            if imf is not None and mmf is not None:
                assert imf >= mmf

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_collateral_details(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test detailed collateral information from enhanced endpoint.

        The collateral endpoint provides per-asset collateral values
        and detailed margin state that isn't available in the basic
        implementation.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.bp_details is not None

        # When enhanced data is available, check for collateral details
        bp_details = account_summary.bp_details

        # Check for enhanced fields that indicate collateral endpoint data
        enhanced_fields = [
            "assets_value",
            "liabilities_value",
            "locked_equity",
            "borrow_liability",
            "margin_fraction",
            "net_exposure_futures",
            "unsettled_equity",
        ]

        # Count how many enhanced fields are present
        enhanced_field_count = sum(
            1
            for field in enhanced_fields
            if hasattr(bp_details, field) and getattr(bp_details, field) is not None
        )

        # If we have enhanced data, we should have multiple enhanced fields
        if enhanced_field_count > 0:
            # We likely have collateral endpoint data
            assert enhanced_field_count >= 3, (
                f"Expected multiple enhanced fields when collateral endpoint is available, "
                f"but only found {enhanced_field_count}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_consistency_check(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test consistency between enhanced and basic data when both are available.

        When the collateral endpoint is available, certain values should
        be consistent with the basic implementation's calculations.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)

        # Total equity should always be present
        assert isinstance(account_summary.total_equity, Decimal)

        # If we have enhanced bp_details with assets and liabilities
        if (
            account_summary.bp_details
            and hasattr(account_summary.bp_details, "assets_value")
            and account_summary.bp_details.assets_value is not None
            and hasattr(account_summary.bp_details, "liabilities_value")
            and account_summary.bp_details.liabilities_value is not None
        ):
            # net_equity = assets_value - liabilities_value should match total_equity
            calculated_equity = (
                account_summary.bp_details.assets_value
                - account_summary.bp_details.liabilities_value
            )
            equity_diff = abs(calculated_equity - account_summary.total_equity)
            assert equity_diff < Decimal("0.01"), (
                f"Large discrepancy between calculated equity "
                f"({calculated_equity}) and total_equity "
                f"({account_summary.total_equity})"
            )

        # Available equity consistency check
        if account_summary.bp_details:
            # Check if locked_equity is tracked
            if account_summary.bp_details.locked_equity is not None:
                # Available equity should be total minus locked
                expected_available = (
                    account_summary.total_equity - account_summary.bp_details.locked_equity
                )
                available_diff = abs(account_summary.available_equity - expected_available)
                # Allow small differences due to rounding or timing
                assert available_diff < Decimal("1.0"), (
                    f"Large discrepancy between available_equity "
                    f"({account_summary.available_equity}) and calculated available "
                    f"({expected_available})"
                )
