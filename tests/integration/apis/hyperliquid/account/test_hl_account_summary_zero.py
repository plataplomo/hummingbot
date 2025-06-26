"""Integration tests for Hyperliquid account summary endpoints for zero balance scenarios.

This module focuses specifically on testing the MarginAccountSummary model pipeline
through Hyperliquid's /info endpoint with user address authentication for zero balance scenarios.
Tests validate complete data transformation from API responses to MarginAccountSummary instances.

Model Focus: MarginAccountSummary (Zero Balance Edge Cases)
- Validates complete MarginAccountSummary model field mapping for empty accounts
- Tests Decimal precision for financial values (equity, margin requirements)
- Validates business logic constraints and margin calculations with zero balances
- Tests Hyperliquid-specific margin details (hl_details with cross/isolated margin)
- Comprehensive error handling and margin edge cases for empty accounts

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.margin_account import MarginAccountSummary


pytestmark = [pytest.mark.integration, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/account/zero"],
    indirect=True,
)
class TestHyperliquidAccountSummaryZero:
    """Comprehensive account summary integration tests for MarginAccountSummary model.

    With zero balance scenarios.

    Tests the integration between Hyperliquid API and our internal MarginAccountSummary model
    focusing on zero balance edge cases and empty account scenarios.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_success_comprehensive(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_account_summary() with comprehensive MarginAccountSummary validation.

        This test validates the complete pipeline from EIP-712 authenticated request
        to fully validated MarginAccountSummary model instances with all field constraints.
        """
        # Execute the API call
        account_summary = await hl_api_for_zero_balance_test.get_account_summary()

        # Validate return type
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should return MarginAccountSummary instance"
        )

        # Validate core fields
        assert account_summary.exchange == "hyperliquid", (
            f"MarginAccountSummary.exchange should be 'hyperliquid', got {account_summary.exchange}"
        )

        # Validate timestamp recency (within 1 hour for active testnet)
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
        assert account_summary.total_equity >= Decimal(0), (
            f"total_equity must be non-negative, got {account_summary.total_equity}"
        )
        assert account_summary.available_equity >= Decimal(0), (
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
            assert account_summary.total_initial_margin_required >= Decimal(0), (
                f"total_initial_margin_required must be non-negative, got "
                f"{account_summary.total_initial_margin_required}"
            )

        if account_summary.total_maintenance_margin_required is not None:
            assert isinstance(account_summary.total_maintenance_margin_required, Decimal), (
                "total_maintenance_margin_required must be Decimal if present"
            )
            assert account_summary.total_maintenance_margin_required >= Decimal(0), (
                f"total_maintenance_margin_required must be non-negative, got "
                f"{account_summary.total_maintenance_margin_required}"
            )

        # Validate exchange-specific details if present
        if account_summary.hl_details:
            hl_details = account_summary.hl_details

            # Validate Decimal fields in hl_details (HyperliquidMarginDetails)
            assert isinstance(hl_details.cross_maintenance_margin_used, Decimal), (
                "hl_details.cross_maintenance_margin_used must be Decimal"
            )
            assert isinstance(hl_details.isolated_maintenance_margin_used, Decimal), (
                "hl_details.isolated_maintenance_margin_used must be Decimal"
            )

            # Validate non-negative constraints
            assert hl_details.cross_maintenance_margin_used >= Decimal(0), (
                f"cross_maintenance_margin_used must be non-negative, got "
                f"{hl_details.cross_maintenance_margin_used}"
            )
            assert hl_details.isolated_maintenance_margin_used >= Decimal(0), (
                f"isolated_maintenance_margin_used must be non-negative, got "
                f"{hl_details.isolated_maintenance_margin_used}"
            )

            # Validate total margin usage makes sense
            total_margin_used = (
                hl_details.cross_maintenance_margin_used
                + hl_details.isolated_maintenance_margin_used
            )
            if account_summary.total_maintenance_margin_required is not None:
                # Total margin used should be <= total equity (can't use more than you have)
                assert total_margin_used <= account_summary.total_equity, (
                    f"Total margin used ({total_margin_used}) should not exceed total equity "
                    f"({account_summary.total_equity})"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_empty_account(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with empty/new account.

        This test validates behavior when account has minimal equity or is newly created.
        Important for testing edge cases in margin account handling.
        """
        account_summary = await hl_api_for_zero_balance_test.get_account_summary()

        # Should return valid MarginAccountSummary even for empty accounts
        assert isinstance(account_summary, MarginAccountSummary), (
            "get_account_summary() should always return MarginAccountSummary"
        )

        # Even empty accounts should have valid structure
        assert account_summary.total_equity >= Decimal(0), (
            "Empty account should still have non-negative total_equity"
        )
        assert account_summary.available_equity >= Decimal(0), (
            "Empty account should still have non-negative available_equity"
        )
        assert account_summary.total_equity >= account_summary.available_equity, (
            "Empty account equity logic should still hold"
        )

        # Empty account should have zero or minimal margin requirements
        if account_summary.total_initial_margin_required is not None:
            # Should be zero or very small for empty account
            assert account_summary.total_initial_margin_required <= Decimal("1.0"), (
                f"Empty account should have minimal initial margin, got "
                f"{account_summary.total_initial_margin_required}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_margin_calculation_consistency(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() margin calculation consistency.

        This validates that margin calculations are internally consistent
        and that cross/isolated margin values add up correctly.
        """
        account_summary = await hl_api_for_zero_balance_test.get_account_summary()

        assert account_summary is not None, "Account summary should not be None"
        if not account_summary.hl_details:
            pytest.skip("No Hyperliquid margin details for calculation testing")

        hl_details = account_summary.hl_details

        # Validate margin calculations are finite and consistent
        assert hl_details.cross_maintenance_margin_used.is_finite(), (
            f"Cross margin should be finite: {hl_details.cross_maintenance_margin_used}"
        )
        assert hl_details.isolated_maintenance_margin_used.is_finite(), (
            f"Isolated margin should be finite: {hl_details.isolated_maintenance_margin_used}"
        )

        # Total maintenance margin used should be sum of cross + isolated
        cross_margin = hl_details.cross_maintenance_margin_used or Decimal(0)
        isolated_margin = hl_details.isolated_maintenance_margin_used or Decimal(0)
        total_maintenance_used = cross_margin + isolated_margin

        # If total_maintenance_margin_required is provided, it should be consistent
        if account_summary.total_maintenance_margin_required is not None:
            # Allow for small rounding differences
            margin_diff = abs(
                total_maintenance_used - account_summary.total_maintenance_margin_required,
            )
            assert margin_diff <= Decimal("0.01"), (
                f"Margin calculation inconsistency: cross+isolated={total_maintenance_used}, "
                f"total_required={account_summary.total_maintenance_margin_required}, "
                f"diff={margin_diff}"
            )

        # Available equity should be total equity minus used margin (approximately)
        # Note: This is a simplified check - real calculation may include other factors
        if account_summary.total_equity > Decimal(0) and total_maintenance_used > Decimal(0):
            # Available should be less than total if margin is being used
            if total_maintenance_used > Decimal("0.01"):  # Only check if significant margin usage
                assert account_summary.available_equity <= account_summary.total_equity, (
                    "Available equity should be <= total equity when margin is being used"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_precision_edge_cases(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with edge cases around decimal precision.

        This validates handling of very small equity amounts, dust margin requirements,
        and precision edge cases that might occur in real trading.
        """
        account_summary = await hl_api_for_zero_balance_test.get_account_summary()

        assert account_summary is not None, "Account summary should not be None"

        # Test very small equity handling
        if account_summary.total_equity > Decimal(0) and account_summary.total_equity < Decimal(
            "1.0",
        ):
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
            if account_summary.total_initial_margin_required > Decimal(
                0,
            ) and account_summary.total_initial_margin_required < Decimal("0.01"):
                # Small margin should be properly represented
                assert account_summary.total_initial_margin_required.is_finite(), (
                    f"Small margin should be finite: "
                    f"{account_summary.total_initial_margin_required}"
                )

        # Test precision consistency across fields
        equity_precision = (
            len(str(account_summary.total_equity).split(".")[-1])
            if "." in str(account_summary.total_equity)
            else 0
        )
        available_precision = (
            len(str(account_summary.available_equity).split(".")[-1])
            if "." in str(account_summary.available_equity)
            else 0
        )

        # Financial precision should be reasonable (not excessive)
        assert equity_precision <= 18, (
            f"total_equity precision too high: {equity_precision} decimals"
        )
        assert available_precision <= 18, (
            f"available_equity precision too high: {available_precision} decimals"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_leverage_scenarios(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with different leverage scenarios.

        This validates margin calculations across different leverage types
        (cross vs isolated) and edge cases around high leverage usage.
        """
        account_summary = await hl_api_for_zero_balance_test.get_account_summary()

        assert account_summary is not None, "Account summary should not be None"
        if not account_summary.hl_details:
            pytest.skip("No Hyperliquid margin details for leverage testing")

        hl_details = account_summary.hl_details

        # Test cross margin scenario
        if hl_details.cross_maintenance_margin_used > Decimal(0):
            # Cross margin usage should be reasonable relative to total equity
            cross_ratio = hl_details.cross_maintenance_margin_used / account_summary.total_equity
            assert cross_ratio <= Decimal("1.0"), (
                f"Cross margin ratio should be <= 100%: {cross_ratio:.4f}"
            )

            # Cross margin should affect available equity
            assert account_summary.available_equity <= account_summary.total_equity, (
                "Available equity should be reduced when cross margin is used"
            )

        # Test isolated margin scenario
        if hl_details.isolated_maintenance_margin_used > Decimal(0):
            # Isolated margin usage should be reasonable
            isolated_ratio = (
                hl_details.isolated_maintenance_margin_used / account_summary.total_equity
            )
            assert isolated_ratio <= Decimal("1.0"), (
                f"Isolated margin ratio should be <= 100%: {isolated_ratio:.4f}"
            )

        # Test combined margin usage
        total_margin_used = (
            hl_details.cross_maintenance_margin_used + hl_details.isolated_maintenance_margin_used
        )

        if total_margin_used > Decimal(0):
            # Total margin usage should not exceed total equity
            total_margin_ratio = total_margin_used / account_summary.total_equity
            assert total_margin_ratio <= Decimal(
                "1.2",
            ), (  # Allow slight buffer for calculation differences
                f"Total margin ratio should be reasonable: {total_margin_ratio:.4f}"
            )

            # High margin usage should leave minimal available equity
            if total_margin_ratio > Decimal("0.8"):  # High leverage scenario
                available_ratio = account_summary.available_equity / account_summary.total_equity
                assert available_ratio <= Decimal("0.5"), (
                    f"High leverage should reduce available equity significantly: "
                    f"{available_ratio:.4f}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_account_summary_concurrent_requests(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_account_summary() with concurrent requests to same endpoint.

        This validates that concurrent account summary requests don't interfere with each other
        and that the underlying clearinghouse state call handles concurrency properly.
        """
        import asyncio

        # Make multiple concurrent calls
        tasks = [
            hl_api_for_zero_balance_test.get_account_summary(),
            hl_api_for_zero_balance_test.get_account_summary(),
            hl_api_for_zero_balance_test.get_account_summary(),
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
                pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, MarginAccountSummary), (
                    f"Result {i} should be MarginAccountSummary"
                )
                successful_results.append(result)

        # At least one should succeed
        assert len(successful_results) > 0, "At least one concurrent call should succeed"

        # If multiple succeed, they should have consistent data (within reasonable time window)
        if len(successful_results) > 1:
            first_result: MarginAccountSummary = successful_results[0]
            for _i, result in enumerate(successful_results[1:], 1):
                # Equity values might differ slightly due to timing, but should be very close
                equity_diff: Decimal = abs(first_result.total_equity - result.total_equity)
                assert equity_diff <= Decimal("0.01"), (
                    f"Concurrent results should have similar equity: "
                    f"{first_result.total_equity} vs {result.total_equity}"
                )
