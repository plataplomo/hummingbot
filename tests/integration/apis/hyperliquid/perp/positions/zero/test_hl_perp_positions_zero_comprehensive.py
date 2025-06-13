"""Integration tests for Hyperliquid positions endpoints.

This module focuses specifically on testing the DerivativePosition model pipeline
through Hyperliquid's /info endpoint with user address authentication.
Tests validate complete data transformation from API responses to DerivativePosition instances.

Model Focus: DerivativePosition
- Validates complete DerivativePosition model field mapping
- Tests Decimal precision for financial values (size, prices, PnL)
- Validates business logic constraints and position calculations
- Tests Hyperliquid-specific position details (hl_details with leverage info)
- Comprehensive error handling and position edge cases

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition

# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/positions"], indirect=True)
@pytest.mark.zero_balance
class TestHyperliquidPositionsZeroComprehensive:
    """Comprehensive positions integration tests for DerivativePosition model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_positions() with comprehensive DerivativePosition validation.

        This test validates the complete pipeline from EIP-712 authenticated request
        to fully validated DerivativePosition model instances with all field constraints.
        """
        # Execute the API call
        positions = await hl_api_for_test_env.get_positions()

        # Validate container type
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        # Test both empty and populated position scenarios
        if not positions:
            # Empty positions is valid for accounts with no open positions
            return

        # Comprehensive validation of each position
        for i, position in enumerate(positions):
            # Validate model type
            assert isinstance(position, DerivativePosition), (
                f"Position {i} should be DerivativePosition instance, got {type(position)}"
            )

            # Validate core fields
            assert position.exchange == "hyperliquid", (
                f"Position.exchange should be 'hyperliquid', got {position.exchange}"
            )

            # Validate symbol format (Hyperliquid uses asset names like "BTC", "ETH", "PURP")
            assert isinstance(position.symbol, str), f"Position {i} symbol must be string"
            assert len(position.symbol) > 0, f"Position {i} symbol cannot be empty"
            assert len(position.symbol) <= 10, f"Position {i} symbol should be reasonable length"

            # Validate timestamp recency
            assert position.timestamp is not None, f"Position {i} must have timestamp"
            time_diff = datetime.now(position.timestamp.tzinfo) - position.timestamp
            assert time_diff.total_seconds() < 3600, (
                f"Position {i} timestamp should be recent (< 1 hour), got "
                f"{time_diff.total_seconds()}s ago"
            )

            # Validate Decimal precision and types
            assert isinstance(position.size, Decimal), (
                f"Position {i} size must be Decimal, got {type(position.size)}"
            )
            assert isinstance(position.entry_price, Decimal), (
                f"Position {i} entry_price must be Decimal, got {type(position.entry_price)}"
            )
            assert isinstance(position.mark_price, Decimal), (
                f"Position {i} mark_price must be Decimal, got {type(position.mark_price)}"
            )
            assert isinstance(position.unrealized_pnl, Decimal), (
                f"Position {i} unrealized_pnl must be Decimal, got {type(position.unrealized_pnl)}"
            )
            assert isinstance(position.realized_pnl, Decimal), (
                f"Position {i} realized_pnl must be Decimal, got {type(position.realized_pnl)}"
            )

            # Validate position size (can be positive, negative, but not zero for active positions)
            if position.size != Decimal("0"):
                # Non-zero positions should have valid entry and mark prices
                assert position.entry_price > Decimal("0"), (
                    f"Position {i} with non-zero size should have positive entry_price, got "
                    f"{position.entry_price}"
                )
                assert position.mark_price > Decimal("0"), (
                    f"Position {i} with non-zero size should have positive mark_price, got "
                    f"{position.mark_price}"
                )

            # Validate price relationships and reasonableness
            if position.entry_price > Decimal("0") and position.mark_price > Decimal("0"):
                # Prices should be in reasonable range (not negative, not astronomically high)
                assert position.entry_price < Decimal("1000000"), (
                    f"Position {i} entry_price seems unreasonably high: {position.entry_price}"
                )
                assert position.mark_price < Decimal("1000000"), (
                    f"Position {i} mark_price seems unreasonably high: {position.mark_price}"
                )

            # Validate PnL calculations make sense
            if (
                position.size != Decimal("0")
                and position.entry_price > Decimal("0")
                and position.mark_price > Decimal("0")
            ):
                # Calculate expected unrealized PnL and validate it's reasonable
                expected_pnl_direction = (
                    position.mark_price - position.entry_price
                ) * position.size

                # PnL direction should match calculation (allowing for fees and other factors)
                if abs(expected_pnl_direction) > Decimal("0.01"):  # Only check if significant
                    pnl_direction_matches = (
                        expected_pnl_direction > 0 and position.unrealized_pnl >= Decimal("0")
                    ) or (expected_pnl_direction < 0 and position.unrealized_pnl <= Decimal("0"))
                    assert pnl_direction_matches, (
                        f"Position {i} PnL direction mismatch: expected "
                        f"{expected_pnl_direction > 0}, got "
                        f"unrealized_pnl={position.unrealized_pnl}"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with account that has no open positions.

        This test validates behavior when account has no positions or all closed positions.
        Important for testing edge cases in position handling.
        """
        positions = await hl_api_for_test_env.get_positions()

        # Should return empty list for account with no positions
        assert isinstance(positions, list), "get_positions() should always return list"

        # If positions exist, they should all be valid (including zero-size positions)
        for i, position in enumerate(positions):
            assert isinstance(position, DerivativePosition), (
                f"Even zero position should be DerivativePosition for {i}"
            )

            # Zero positions should still follow basic constraints
            assert isinstance(position.size, Decimal), "size should be Decimal"
            assert isinstance(position.unrealized_pnl, Decimal), "unrealized_pnl should be Decimal"
            assert isinstance(position.realized_pnl, Decimal), "realized_pnl should be Decimal"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_authentication_failure(
        self,
        hl_api_with_di: Callable[
            ..., HyperliquidAPI
        ],  # Factory function for creating API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid EIP-712 authentication.

        This validates proper error handling when EIP-712 signature is invalid,
        testing the complete authentication failure pipeline.
        """
        # Create API with invalid EIP-712 private key
        invalid_secrets = PrivateKeyAuthSecrets(
            private_key=SecretStr(
                "0x0000000000000000000000000000000000000000000000000000000000000002"
            ),
        )

        bad_api = hl_api_with_di(secrets=invalid_secrets)

        # Should raise authentication error
        with pytest.raises(APIError) as exc_info:
            await bad_api.get_positions()

        # Validate error mapping and structure
        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], f"Expected 401/403 status, got {error.http_status}"
        assert len(error.message) > 0, "Error message should be descriptive"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with edge cases around decimal precision.

        This validates handling of very small position sizes, dust PnL amounts,
        and precision edge cases that might occur in real trading.
        """
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for precision testing")

        for i, position in enumerate(positions):
            # Test very small position handling
            if position.size != Decimal("0"):
                # Validate that small positions maintain precision
                assert position.size.is_finite(), (
                    f"Position {i} size should be finite: {position.size}"
                )

                # Check for dust handling (very small amounts)
                if abs(position.size) < Decimal("0.001"):  # Less than 0.001 units
                    # Even dust positions should be properly represented
                    assert str(position.size) != "0E-0", (
                        f"Dust position {i} should maintain proper decimal representation"
                    )

            # Test PnL precision
            if position.unrealized_pnl != Decimal("0"):
                assert position.unrealized_pnl.is_finite(), (
                    f"Position {i} unrealized_pnl should be finite: {position.unrealized_pnl}"
                )

            # Validate precision consistency across financial fields
            for field_name, field_value in [
                ("size", position.size),
                ("entry_price", position.entry_price),
                ("mark_price", position.mark_price),
                ("unrealized_pnl", position.unrealized_pnl),
                ("realized_pnl", position.realized_pnl),
            ]:
                if field_value != Decimal("0"):
                    precision = (
                        len(str(field_value).split(".")[-1])
                        if "." in str(field_value)
                        else 0
                    )
                    assert precision <= 18, (
                        f"Position {i} {field_name} precision too high: {precision} decimals"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_concurrent_requests(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with concurrent requests to same endpoint.

        This validates that concurrent position requests don't interfere with each other
        and that the underlying clearinghouse state call handles concurrency properly.
        """
        import asyncio

        # Make multiple concurrent calls
        tasks = [
            hl_api_for_test_env.get_positions(),
            hl_api_for_test_env.get_positions(),
            hl_api_for_test_env.get_positions(),
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should succeed and return consistent data
        successful_results: list[list[DerivativePosition]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # If some fail due to rate limiting, that's acceptable
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                else:
                    pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, list), f"Result {i} should be list"
                successful_results.append(result)

        # At least one should succeed
        assert len(successful_results) > 0, "At least one concurrent call should succeed"

        # If multiple succeed, they should have consistent data (within reasonable time window)
        if len(successful_results) > 1:
            first_result = successful_results[0]
            for j, result in enumerate(successful_results[1:], 1):
                # Position counts might differ slightly due to timing, but should be close
                assert len(first_result) == len(result), (
                    f"Concurrent results should have same position count: "
                    f"{len(first_result)} vs {len(result)}"
                )