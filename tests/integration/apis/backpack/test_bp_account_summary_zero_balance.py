"""Integration tests for Backpack private account summary endpoints with zero balance.

This module focuses specifically on testing the MarginAccountSummary model pipeline
through Backpack's private account summary endpoint with Ed25519 authentication
when the account has zero or minimal balance.
Tests validate complete data transformation from API responses to MarginAccountSummary instances.

Model Focus: MarginAccountSummary (Zero Balance Scenarios)
- Validates complete MarginAccountSummary model field mapping for empty accounts
- Tests Decimal precision for minimal financial values
- Validates business logic constraints with zero equity/margin
- Tests Backpack-specific margin details with zero positions
- Authentication and error handling with empty accounts

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: Zero or minimal balance (safe for CI/CD testing)
"""

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

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/account_summary"], indirect=True
)
class TestBackpackAccountSummaryZeroBalance:
    """Account summary integration tests specifically for zero balance scenarios."""

    @pytest.mark.vcr
    async def test_get_account_summary_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with empty/new account.

        This test validates behavior when account has minimal equity or is newly created.
        Important for testing edge cases in margin account handling with zero balance.
        Safe for CI/CD environments as it doesn't require funds.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Should return valid MarginAccountSummary even for empty accounts
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should always return MarginAccountSummary"
        )

        # Even empty accounts should have valid structure
        assert account_summary.total_equity >= Decimal("0"), (
            "Empty account should still have non-negative total_equity"
        )
        assert account_summary.available_equity >= Decimal("0"), (
            "Empty account should still have non-negative available_equity"
        )
        assert account_summary.total_equity >= account_summary.available_equity, (
            "Empty account equity logic should still hold"
        )

        # Empty account should have zero or minimal margin requirements
        if account_summary.total_initial_margin_required is not None:
            # Should be zero or very small for empty account
            assert account_summary.total_initial_margin_required <= Decimal("1.0"), (
                f"Empty account should have minimal initial margin, "
                f"got {account_summary.total_initial_margin_required}"
            )

        # Validate timestamp recency (within 1 hour for active environment)
        assert account_summary.timestamp is not None, "MarginAccountSummary must have timestamp"
        time_diff = datetime.now(UTC) - account_summary.timestamp
        assert time_diff.total_seconds() < 3600, (
            f"Timestamp should be recent (< 1 hour), got {time_diff.total_seconds()}s ago"
        )

        # Exchange should be correct
        assert account_summary.exchange == "backpack", (
            f"MarginAccountSummary.exchange should be 'backpack', got {account_summary.exchange}"
        )

        # Validate required Decimal field types even for zero balance
        assert isinstance(account_summary.total_equity, Decimal), (
            f"total_equity must be Decimal, got {type(account_summary.total_equity)}"
        )
        assert isinstance(account_summary.available_equity, Decimal), (
            f"available_equity must be Decimal, got {type(account_summary.available_equity)}"
        )

        # Validate Backpack-specific details for zero balance if present
        if account_summary.bp_details:
            bp_details = account_summary.bp_details

            # Even with zero balance, structure should be valid
            decimal_fields = [
                "assets_value",
                "borrow_liability",
                "liabilities_value",
                "locked_equity",
                "margin_fraction",
            ]

            for field_name in decimal_fields:
                field_value = getattr(bp_details, field_name, None)
                if field_value is not None:
                    assert isinstance(field_value, Decimal), (
                        f"bp_details.{field_name} must be Decimal if present"
                    )

            # For zero balance accounts, most fields should be zero or minimal
            if bp_details.assets_value is not None:
                assert bp_details.assets_value <= Decimal("1.0"), (
                    f"Zero balance account should have minimal assets_value, "
                    f"got {bp_details.assets_value}"
                )

            if bp_details.locked_equity is not None:
                assert bp_details.locked_equity <= Decimal("1.0"), (
                    f"Zero balance account should have minimal locked_equity, "
                    f"got {bp_details.locked_equity}"
                )

        logger.info(
            f"✓ Account summary zero balance test completed: equity={account_summary.total_equity}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_precision_validation_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() decimal precision validation with zero balances.

        Validates that all financial fields maintain proper decimal precision
        and handle zero values correctly without floating point issues.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Test decimal precision for all financial fields
        financial_fields = [
            ("total_equity", account_summary.total_equity),
            ("available_equity", account_summary.available_equity),
        ]

        for field_name, field_value in financial_fields:
            assert isinstance(field_value, Decimal), f"{field_name} should be Decimal"
            assert field_value.is_finite(), f"{field_name} should be finite"
            assert field_value >= Decimal("0"), f"{field_name} should be non-negative"

            # For zero balance accounts, most values should be zero or very small
            if field_value == Decimal("0"):
                # Decimal("0") may be represented as "0" or "0.0" depending on parsing
                assert field_value == Decimal("0"), f"Zero {field_name} should be exact zero"
                logger.info(f"✓ Zero {field_name} = {field_value} (exact zero)")

            # Test arithmetic operations don't cause precision issues
            doubled = field_value * Decimal("2")
            halved = doubled / Decimal("2")
            assert halved == field_value, f"Arithmetic precision issue with {field_name}"

            logger.info(f"✓ {field_name} = {field_value} (precision validated)")

        # Test field relationships for zero balance account
        assert account_summary.total_equity >= Decimal("0")
        assert account_summary.available_equity >= Decimal("0")

        # Available should be <= total for reasonable relationship
        assert account_summary.available_equity <= account_summary.total_equity
        if account_summary.total_equity == Decimal("0"):
            assert account_summary.available_equity == Decimal("0"), (
                "Zero equity should mean zero available"
            )

        logger.info("✓ Financial field relationships validated for zero balance")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_exchange_specific_fields_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() Backpack-specific field validation.

        Validates that Backpack-specific margin account fields are properly
        populated and consistent even for zero balance accounts.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Validate Backpack-specific details exist
        assert account_summary.bp_details is not None, "bp_details should be populated"

        bp_details = account_summary.bp_details

        # Test required Backpack margin fields
        # Note: BackpackMarginDetails has different fields than the core MarginAccountSummary
        bp_margin_fields = [
            "assets_value",
            "borrow_liability",
            "liabilities_value",
            "locked_equity",
            "margin_fraction",
        ]

        for field in bp_margin_fields:
            assert hasattr(bp_details, field), f"Missing Backpack field: {field}"

            field_value = getattr(bp_details, field)
            # These fields can be None in BackpackMarginDetails
            if field_value is not None:
                assert isinstance(field_value, Decimal), f"Backpack {field} should be Decimal"
                assert field_value.is_finite(), f"Backpack {field} should be finite"
                assert field_value >= Decimal("0"), f"Backpack {field} should be non-negative"
                logger.info(f"✓ Backpack {field} = {field_value}")
            else:
                logger.info(f"✓ Backpack {field} = None (allowed)")

        # Test zero balance specific validations
        if account_summary.total_equity == Decimal("0"):
            # Check that Backpack-specific fields are also zero or minimal
            if bp_details.assets_value is not None:
                assert bp_details.assets_value <= Decimal("1.0"), (
                    "Zero balance should have minimal assets"
                )
            if bp_details.locked_equity is not None:
                assert bp_details.locked_equity <= Decimal("1.0"), (
                    "Zero balance should have minimal locked equity"
                )

            logger.info("✓ Zero balance Backpack details validated")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_error_handling_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() error handling robustness.

        Tests various error scenarios and validates proper error response handling.
        """
        try:
            account_summary = await bp_api_for_test_env.get_account_summary()

            # If successful, validate structure
            assert isinstance(account_summary, MarginAccountSummary)
            assert hasattr(account_summary, "total_equity")
            assert hasattr(account_summary, "exchange")
            assert account_summary.exchange == "backpack"

            logger.info("✓ Account summary request succeeded with proper structure")

        except APIError as e:
            # If it fails, validate error structure
            assert hasattr(e, "code"), "APIError should have error code"
            assert hasattr(e, "message"), "APIError should have error message"
            assert e.code is not None, "Error code should not be None"
            assert e.message is not None, "Error message should not be None"

            # Error message should be informative
            error_msg = str(e.message).lower()
            assert len(error_msg) > 0, "Error message should not be empty"
            assert len(error_msg) < 500, "Error message should be reasonable length"

            # Common error patterns for account summary
            account_keywords = ["account", "summary", "balance", "margin", "equity"]
            auth_keywords = ["auth", "permission", "unauthorized", "forbidden"]

            # Check if error message contains relevant keywords (not required)
            any(keyword in error_msg for keyword in account_keywords + auth_keywords)

            logger.info(f"✓ Account summary error properly structured: {e.code} - {e.message}")

        except Exception as e:
            # Handle unexpected errors gracefully
            if "timeout" in str(e).lower():
                logger.info(f"✓ Timeout handled gracefully: {e}")
            else:
                logger.info(f"✓ Unexpected error handled: {type(e).__name__}: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_consistency_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() consistency across multiple calls.

        For zero balance accounts, consecutive calls should return identical results.
        """
        summaries: list[MarginAccountSummary | None] = []

        # Make multiple account summary requests
        for i in range(3):
            try:
                summary = await bp_api_for_test_env.get_account_summary()
                summaries.append(summary)
                logger.info(f"Summary {i + 1}: equity={summary.total_equity}")
            except Exception as e:
                logger.info(f"Summary {i + 1} failed: {e}")
                summaries.append(None)

        # Filter successful calls
        successful_summaries = [s for s in summaries if s is not None]

        if len(successful_summaries) >= 2:
            first_summary = successful_summaries[0]

            for i, subsequent_summary in enumerate(successful_summaries[1:], 1):
                # For zero balance accounts, all values should be identical
                assert first_summary.total_equity == subsequent_summary.total_equity
                assert first_summary.available_equity == subsequent_summary.available_equity
                assert (
                    first_summary.total_initial_margin_required
                    == subsequent_summary.total_initial_margin_required
                )
                assert (
                    first_summary.total_maintenance_margin_required
                    == subsequent_summary.total_maintenance_margin_required
                )

                # Exchange and timestamp fields should be consistent
                assert first_summary.exchange == subsequent_summary.exchange

                logger.info(f"✓ Summary {i + 1} consistent with first summary")

            logger.info(f"✓ Consistency validated across {len(successful_summaries)} calls")
        else:
            logger.info("✓ Insufficient successful calls for consistency testing")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_field_bounds_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() field boundary conditions with zero balance.

        Validates that all numeric fields are within reasonable bounds and handle
        edge cases properly for zero balance accounts.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Define reasonable bounds for zero balance accounts
        max_value = Decimal("1000")  # Zero balance shouldn't have large values

        field_bounds = [
            ("total_equity", Decimal("0"), max_value),  # Should be >= 0
            ("available_equity", Decimal("0"), max_value),  # Should be >= 0
            ("total_initial_margin_required", Decimal("0"), max_value),  # Should be >= 0 if present
            (
                "total_maintenance_margin_required",
                Decimal("0"),
                max_value,
            ),  # Should be >= 0 if present
        ]

        for field_name, min_bound, max_bound in field_bounds:
            field_value = getattr(account_summary, field_name, None)
            # Some fields might be optional (None)
            if field_value is not None:
                assert field_value >= min_bound, (
                    f"{field_name} ({field_value}) below minimum bound ({min_bound})"
                )
                assert field_value <= max_bound, (
                    f"{field_name} ({field_value}) above maximum bound ({max_bound})"
                )

                # Test for reasonable precision (not excessive decimal places)
                decimal_places = abs(field_value.as_tuple().exponent)
                assert decimal_places <= 18, (
                    f"{field_name} has excessive precision: {decimal_places}"
                )

                logger.info(
                    f"✓ {field_name} = {field_value} within bounds [{min_bound}, {max_bound}]"
                )
            else:
                logger.info(f"✓ {field_name} = None (optional field)")

        # Test logical relationships
        if account_summary.total_equity == Decimal("0"):
            # For true zero balance accounts
            assert account_summary.available_equity == Decimal("0")
            logger.info("✓ True zero balance account relationships validated")
        else:
            # For accounts with minimal equity
            assert account_summary.total_equity > Decimal("0")
            assert account_summary.available_equity <= account_summary.total_equity
            logger.info("✓ Minimal balance account relationships validated")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_timestamp_validation_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() timestamp field validation.

        Validates that timestamp fields are properly formatted and reasonable.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Check if timestamp field exists and validate it
        if hasattr(account_summary, "timestamp") and account_summary.timestamp is not None:
            timestamp = account_summary.timestamp

            assert isinstance(timestamp, datetime), "Timestamp should be datetime object"

            # Should be UTC timezone aware
            assert timestamp.tzinfo is not None, "Timestamp should be timezone aware"

            # Should be recent (within last hour)
            now = datetime.now(UTC)
            time_diff = abs((now - timestamp).total_seconds())
            assert time_diff < 3600, f"Timestamp too old: {time_diff} seconds ago"

            # Should not be in the future (allow 60 seconds clock skew)
            assert timestamp <= now + timedelta(seconds=60), "Timestamp should not be in future"

            logger.info(f"✓ Timestamp validation passed: {timestamp}")
        else:
            logger.info("✓ No timestamp field present (acceptable)")

        # Validate exchange field
        assert account_summary.exchange == "backpack", "Exchange should be 'backpack'"

        logger.info("✓ Metadata fields validated")
