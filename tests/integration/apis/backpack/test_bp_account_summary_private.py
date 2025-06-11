"""Integration tests for Backpack private account summary endpoints.

This module focuses specifically on testing the MarginAccountSummary model pipeline
through Backpack's private account summary endpoint with Ed25519 authentication.
Tests validate complete data transformation from API responses to MarginAccountSummary instances.

Model Focus: MarginAccountSummary
- Validates complete MarginAccountSummary model field mapping
- Tests Decimal precision for financial values (equity, margin requirements)
- Validates business logic constraints and margin calculations
- Tests Backpack-specific margin details (bp_details with margin breakdown)
- Comprehensive error handling and margin edge cases

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
from cyberdelta.core.models.margin_account import MarginAccountSummary

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", 
    ["apis/backpack/private/account_summary"], 
    indirect=True
)
class TestBackpackAccountSummaryPrivate:
    """Comprehensive private account summary integration tests for MarginAccountSummary."""

    @pytest.mark.vcr
    async def test_get_account_summary_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_account_summary() with comprehensive MarginAccountSummary validation.

        This test validates the complete pipeline from Ed25519 authenticated request
        to fully validated MarginAccountSummary model instances with all field constraints.
        """
        # Execute the API call
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Validate return type
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should return MarginAccountSummary instance"
        )

        # Validate core fields
        assert account_summary.exchange == "backpack", (
            f"MarginAccountSummary.exchange should be 'backpack', got {account_summary.exchange}"
        )

        # Validate timestamp recency (within 1 hour for active environment)
        assert account_summary.timestamp is not None, "MarginAccountSummary must have timestamp"
        time_diff = datetime.now(account_summary.timestamp.tzinfo) - account_summary.timestamp
        assert time_diff.total_seconds() < 3600, (
            f"Timestamp should be recent (< 1 hour), got {time_diff.total_seconds()}s ago"
        )

        # Validate required Decimal fields with precision
        assert isinstance(account_summary.total_equity, Decimal), (
            f"total_equity must be Decimal, got {type(account_summary.total_equity)}"
        )
        assert isinstance(account_summary.available_equity, Decimal), (
            f"available_equity must be Decimal, got {type(account_summary.available_equity)}"
        )

        # Validate business logic constraints
        assert account_summary.total_equity >= Decimal("0"), (
            f"total_equity must be non-negative, got {account_summary.total_equity}"
        )
        assert account_summary.available_equity >= Decimal("0"), (
            f"available_equity must be non-negative, got {account_summary.available_equity}"
        )

        # Validate equity relationship (total >= available)
        assert account_summary.total_equity >= account_summary.available_equity, (
            f"total_equity ({account_summary.total_equity}) should be >= "
            f"available_equity ({account_summary.available_equity})"
        )

        # Validate optional margin fields if present
        if account_summary.total_initial_margin_required is not None:
            assert isinstance(account_summary.total_initial_margin_required, Decimal), (
                "total_initial_margin_required must be Decimal if present"
            )
            assert account_summary.total_initial_margin_required >= Decimal("0"), (
                f"total_initial_margin_required must be non-negative, "
                f"got {account_summary.total_initial_margin_required}"
            )

        if account_summary.total_maintenance_margin_required is not None:
            assert isinstance(account_summary.total_maintenance_margin_required, Decimal), (
                "total_maintenance_margin_required must be Decimal if present"
            )
            assert account_summary.total_maintenance_margin_required >= Decimal("0"), (
                f"total_maintenance_margin_required must be non-negative, "
                f"got {account_summary.total_maintenance_margin_required}"
            )

        # Validate exchange-specific details if present
        if account_summary.bp_details:
            bp_details = account_summary.bp_details
            
            # Validate Backpack-specific margin fields
            decimal_fields = ["assets_value", "borrow_liability", "liabilities_value", 
                            "locked_equity", "margin_fraction"]
            
            for field_name in decimal_fields:
                field_value = getattr(bp_details, field_name, None)
                if field_value is not None:
                    assert isinstance(field_value, Decimal), (
                        f"bp_details.{field_name} must be Decimal if present"
                    )
                    # Most fields should be non-negative (except liabilities which could be debt)
                    if field_name not in ["borrow_liability", "liabilities_value"]:
                        assert field_value >= Decimal("0"), (
                            f"bp_details.{field_name} must be non-negative, got {field_value}"
                        )

            # Validate margin fraction if present
            if bp_details.margin_fraction is not None:
                assert bp_details.margin_fraction >= Decimal("0"), (
                    f"margin_fraction should be non-negative, got {bp_details.margin_fraction}"
                )
                # Margin fraction is typically between 0 and 1 (or 0% to 100%)
                assert bp_details.margin_fraction <= Decimal("10.0"), (  # Allow >100% in extremes
                    f"margin_fraction seems unreasonably high: {bp_details.margin_fraction}"
                )

            # Validate locked equity if present
            if bp_details.locked_equity is not None:
                # Locked equity should not exceed total equity
                assert bp_details.locked_equity <= account_summary.total_equity, (
                    f"locked_equity ({bp_details.locked_equity}) should not exceed "
                    f"total_equity ({account_summary.total_equity})"
                )

    @pytest.mark.vcr
    async def test_get_account_summary_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with empty/new account.

        This test validates behavior when account has minimal equity or is newly created.
        Important for testing edge cases in margin account handling.
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

    @pytest.mark.vcr
    async def test_get_account_summary_authentication_failure(
        self,
        bp_api_with_di: Callable[..., BackpackAPI],  # Factory for API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with invalid Ed25519 authentication."""
        # Create API with invalid credentials
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )
        
        bad_api = bp_api_with_di(secrets=invalid_secrets)

        # Should raise authentication error
        with pytest.raises(APIError) as exc_info:
            await bad_api.get_account_summary()

        # Validate error mapping and structure
        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], (
            f"Expected 401/403 status, got {error.http_status}"
        )

    @pytest.mark.vcr
    async def test_get_account_summary_margin_calculation_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() margin calculation consistency.

        This validates that margin calculations are internally consistent
        and that Backpack-specific margin values add up correctly.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()
        
        if not account_summary.bp_details:
            pytest.skip("No Backpack margin details for calculation testing")

        bp_details = account_summary.bp_details
        
        # Validate margin calculations are finite and consistent
        if bp_details.assets_value is not None:
            assert bp_details.assets_value.is_finite(), (
                f"Assets value should be finite: {bp_details.assets_value}"
            )
            
            # Assets value should be reasonable relative to total equity
            if account_summary.total_equity > Decimal("0"):
                value_ratio = bp_details.assets_value / account_summary.total_equity
                assert value_ratio >= Decimal("0.8"), (  # Allow for some variation
                    f"Assets value ratio seems low: {value_ratio:.4f}"
                )
                assert value_ratio <= Decimal("1.2"), (  # Allow for some leverage
                    f"Assets value ratio seems high: {value_ratio:.4f}"
                )

        # Validate liability calculations if present
        if bp_details.liabilities_value is not None and bp_details.borrow_liability is not None:
            assert bp_details.liabilities_value.is_finite(), (
                f"Liabilities value should be finite: {bp_details.liabilities_value}"
            )
            assert bp_details.borrow_liability.is_finite(), (
                f"Borrow liability should be finite: {bp_details.borrow_liability}"
            )
            
            # Borrow liability should be part of total liabilities
            if bp_details.liabilities_value > Decimal("0"):
                assert bp_details.borrow_liability <= bp_details.liabilities_value, (
                    f"Borrow liability ({bp_details.borrow_liability}) should be <= "
                    f"total liabilities ({bp_details.liabilities_value})"
                )

        # Validate equity calculations: total_equity = assets - liabilities (approximately)
        if (bp_details.assets_value is not None and 
            bp_details.liabilities_value is not None and
            account_summary.total_equity > Decimal("1")):  # Only check for significant amounts
            
            expected_equity = bp_details.assets_value - bp_details.liabilities_value
            equity_diff = abs(expected_equity - account_summary.total_equity)
            # 1% tolerance
            equity_tolerance = max(Decimal("1.0"), account_summary.total_equity * Decimal("0.01"))
            
            assert equity_diff <= equity_tolerance, (
                f"Equity calculation inconsistency: assets-liabilities={expected_equity}, "
                f"total_equity={account_summary.total_equity}, diff={equity_diff}"
            )

    @pytest.mark.vcr
    async def test_get_account_summary_margin_fraction_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() margin fraction validation.

        This validates Backpack's margin fraction calculation and ensures it
        makes sense relative to position risk and account equity.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()
        
        if not account_summary.bp_details or account_summary.bp_details.margin_fraction is None:
            pytest.skip("No margin fraction for validation testing")

        bp_details = account_summary.bp_details
        margin_fraction = bp_details.margin_fraction
        
        assert margin_fraction is not None, "Margin fraction should not be None for this test"
        
        # Validate margin fraction is finite and reasonable
        assert margin_fraction.is_finite(), f"Margin fraction should be finite: {margin_fraction}"
        assert margin_fraction >= Decimal("0"), (
            f"Margin fraction should be non-negative: {margin_fraction}"
        )
        
        # Validate margin fraction relationships
        if account_summary.total_equity > Decimal("0"):
            # High margin fraction should correlate with low available equity
            if margin_fraction > Decimal("0.8"):  # High margin usage (>80%)
                equity_ratio = account_summary.available_equity / account_summary.total_equity
                assert equity_ratio <= Decimal("0.5"), (
                    f"High margin fraction ({margin_fraction}) should result in "
                    f"low available equity ratio ({equity_ratio})"
                )
            
            # Low margin fraction should allow for more available equity
            elif margin_fraction < Decimal("0.2"):  # Low margin usage (<20%)
                equity_ratio = account_summary.available_equity / account_summary.total_equity
                assert equity_ratio >= Decimal("0.5"), (
                    f"Low margin fraction ({margin_fraction}) should allow "
                    f"higher available equity ratio ({equity_ratio})"
                )

        # Validate margin fraction against margin requirements if present
        if (account_summary.total_maintenance_margin_required is not None and 
            account_summary.total_equity > Decimal("0")):
            
            calculated_margin_fraction = (
                account_summary.total_maintenance_margin_required / account_summary.total_equity
            )
            margin_diff = abs(margin_fraction - calculated_margin_fraction)
            
            # Allow for some difference due to different calculation methods
            assert margin_diff <= Decimal("0.1"), (
                f"Margin fraction mismatch: reported={margin_fraction}, "
                f"calculated={calculated_margin_fraction}, diff={margin_diff}"
            )

    @pytest.mark.vcr
    async def test_get_account_summary_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with edge cases around decimal precision.

        This validates handling of very small equity amounts, dust margin requirements,
        and precision edge cases that might occur in real trading.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()
        
        # Test very small equity handling
        if (account_summary.total_equity > Decimal("0") and 
            account_summary.total_equity < Decimal("1.0")):
            # Very small equity should maintain precision
            assert account_summary.total_equity.is_finite(), (
                f"Small equity should be finite: {account_summary.total_equity}"
            )
            
            # Should not have scientific notation issues
            equity_str = str(account_summary.total_equity)
            if "E" in equity_str.upper():
                assert "E-" in equity_str.upper(), (
                    f"Scientific notation should be negative exponent: {equity_str}"
                )

        # Test small margin amounts
        if account_summary.total_initial_margin_required is not None:
            if (account_summary.total_initial_margin_required > Decimal("0") and 
                account_summary.total_initial_margin_required < Decimal("0.01")):
                # Small margin should be properly represented
                assert account_summary.total_initial_margin_required.is_finite(), (
                    f"Small margin should be finite: "
                    f"{account_summary.total_initial_margin_required}"
                )

        # Test precision consistency across fields
        equity_precision = len(str(account_summary.total_equity).split(".")[-1]) if "." in str(account_summary.total_equity) else 0
        available_precision = len(str(account_summary.available_equity).split(".")[-1]) if "." in str(account_summary.available_equity) else 0
        
        # Financial precision should be reasonable (not excessive)
        assert equity_precision <= 18, f"total_equity precision too high: {equity_precision} decimals"
        assert available_precision <= 18, f"available_equity precision too high: {available_precision} decimals"

    @pytest.mark.vcr
    async def test_get_account_summary_backpack_specific_fields(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with focus on Backpack-specific fields.

        This validates all Backpack-specific account summary fields and their
        relationships to ensure they make sense together.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()
        
        if not account_summary.bp_details:
            pytest.skip("No Backpack details for field testing")

        bp_details = account_summary.bp_details
        
        # Validate assets value field
        if bp_details.assets_value is not None:
            assert isinstance(bp_details.assets_value, Decimal), "assets_value should be Decimal"
            assert bp_details.assets_value >= Decimal("0"), "assets_value should be non-negative"
            assert bp_details.assets_value.is_finite(), "assets_value should be finite"

        # Validate liability fields
        if bp_details.liabilities_value is not None:
            assert isinstance(bp_details.liabilities_value, Decimal), "liabilities_value should be Decimal"
            assert bp_details.liabilities_value >= Decimal("0"), "liabilities_value should be non-negative"
            assert bp_details.liabilities_value.is_finite(), "liabilities_value should be finite"

        if bp_details.borrow_liability is not None:
            assert isinstance(bp_details.borrow_liability, Decimal), "borrow_liability should be Decimal"
            assert bp_details.borrow_liability >= Decimal("0"), "borrow_liability should be non-negative"
            assert bp_details.borrow_liability.is_finite(), "borrow_liability should be finite"

        # Validate locked equity field
        if bp_details.locked_equity is not None:
            assert isinstance(bp_details.locked_equity, Decimal), "locked_equity should be Decimal"
            assert bp_details.locked_equity >= Decimal("0"), "locked_equity should be non-negative"
            assert bp_details.locked_equity.is_finite(), "locked_equity should be finite"
            
            # Locked equity relationship with available equity
            expected_available = account_summary.total_equity - bp_details.locked_equity
            if expected_available >= Decimal("0"):
                available_diff = abs(account_summary.available_equity - expected_available)
                tolerance = max(Decimal("0.01"), account_summary.total_equity * Decimal("0.001"))
                assert available_diff <= tolerance, (
                    f"Available equity calculation mismatch: "
                    f"expected={expected_available}, actual={account_summary.available_equity}"
                )

        # Validate all fields are reasonable together
        if (bp_details.assets_value is not None and 
            bp_details.liabilities_value is not None and
            bp_details.locked_equity is not None):
            
            # All values should be reasonable relative to each other
            total_values = bp_details.assets_value + bp_details.liabilities_value + bp_details.locked_equity
            assert total_values < Decimal("1000000000"), (  # 1 billion limit for sanity
                f"Combined values seem unreasonably high: {total_values}"
            )

    @pytest.mark.vcr
    async def test_get_account_summary_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with concurrent requests to same endpoint.

        This validates that concurrent account summary requests don't interfere with each other
        and that the underlying account summary API call handles concurrency properly.
        """
        import asyncio
        
        # Make multiple concurrent calls
        tasks = [
            bp_api_for_test_env.get_account_summary(),
            bp_api_for_test_env.get_account_summary(),
            bp_api_for_test_env.get_account_summary(),
        ]
        
        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should succeed and return consistent data
        successful_results: list[MarginAccountSummary] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # If some fail due to rate limiting, that's acceptable
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                else:
                    pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, MarginAccountSummary), f"Result {i} should be MarginAccountSummary"
                successful_results.append(result)
        
        # At least one should succeed
        assert len(successful_results) > 0, "At least one concurrent call should succeed"
        
        # If multiple succeed, they should have consistent data (within reasonable time window)
        if len(successful_results) > 1:
            first_result: MarginAccountSummary = successful_results[0]
            for i, result in enumerate(successful_results[1:], 1):
                # Equity values might differ slightly due to timing, but should be very close
                equity_diff: Decimal = abs(first_result.total_equity - result.total_equity)
                assert equity_diff <= Decimal("0.01"), (
                    f"Concurrent results should have similar equity: {first_result.total_equity} vs {result.total_equity}"
                )
                
                available_diff: Decimal = abs(first_result.available_equity - result.available_equity)
                assert available_diff <= Decimal("0.01"), (
                    f"Concurrent results should have similar available equity: "
                    f"{first_result.available_equity} vs {result.available_equity}"
                )