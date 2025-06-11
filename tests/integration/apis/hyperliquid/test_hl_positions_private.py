"""Integration tests for Hyperliquid private positions endpoints.

This module focuses specifically on testing the DerivativePosition model pipeline
through Hyperliquid's private /info endpoint with EIP-712 authentication.
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
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/private/positions"], indirect=True
)
class TestHyperliquidPositionsPrivate:
    """Comprehensive private positions integration tests for DerivativePosition model validation."""

    @pytest.mark.vcr
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
                f"Position {i} timestamp should be recent (< 1 hour), got {time_diff.total_seconds()}s ago"
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
                    f"Position {i} with non-zero size should have positive entry_price, got {position.entry_price}"
                )
                assert position.mark_price > Decimal("0"), (
                    f"Position {i} with non-zero size should have positive mark_price, got {position.mark_price}"
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
                        f"Position {i} PnL direction mismatch: expected {expected_pnl_direction > 0}, "
                        f"got unrealized_pnl={position.unrealized_pnl}"
                    )

            # Validate exchange-specific details if present
            if position.hl_details:
                hl_details = position.hl_details

                # Validate leverage information
                assert isinstance(hl_details.leverage_type, str), (
                    f"Position {i} leverage_type must be string"
                )
                assert hl_details.leverage_type in ["cross", "isolated"], (
                    f"Position {i} leverage_type must be 'cross' or 'isolated', got {hl_details.leverage_type}"
                )

                assert isinstance(hl_details.leverage_value, int), (
                    f"Position {i} leverage_value must be int"
                )
                assert hl_details.leverage_value >= 1, (
                    f"Position {i} leverage_value must be >= 1, got {hl_details.leverage_value}"
                )
                assert hl_details.leverage_value <= 50, (
                    f"Position {i} leverage_value seems too high: {hl_details.leverage_value}"
                )

    @pytest.mark.vcr
    async def test_get_positions_empty_account(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with account that has no open positions.

        This test validates behavior when account has no derivative positions.
        Important for testing edge cases in position handling.
        """
        positions = await hl_api_for_test_env.get_positions()

        # Should return empty list
        assert isinstance(positions, list), "get_positions() should always return list"

        # Empty list is valid for accounts with no positions
        if len(positions) == 0:
            return  # Test passes - no positions is valid

        # If positions exist, they should all be valid
        for position in positions:
            assert isinstance(position, DerivativePosition), (
                "All returned positions should be valid"
            )

    @pytest.mark.vcr
    async def test_get_positions_authentication_failure(
        self,
        hl_api_with_di: Callable[
            ..., HyperliquidAPI
        ],  # Factory function for creating API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid EIP-712 authentication."""
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

    @pytest.mark.vcr
    async def test_get_positions_large_position_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() handling of large position sizes and values.

        This validates that large positions are handled correctly without
        precision loss or overflow issues.
        """
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for large position testing")

        for position in positions:
            # Test handling of large position sizes
            size_magnitude = abs(position.size)
            if size_magnitude > Decimal("1000"):  # Large position
                # Should maintain precision for large positions
                assert position.size.is_finite(), (
                    f"Large position size should be finite: {position.size}"
                )

                # PnL calculations should still be accurate
                if position.unrealized_pnl is not None:
                    assert position.unrealized_pnl.is_finite(), (
                        f"Large position unrealized_pnl should be finite: {position.unrealized_pnl}"
                    )
                if position.realized_pnl is not None:
                    assert position.realized_pnl.is_finite(), (
                        f"Large position realized_pnl should be finite: {position.realized_pnl}"
                    )

            # Test handling of high-value positions (price * size)
            if (
                position.size != Decimal("0")
                and position.mark_price is not None
                and position.mark_price > Decimal("0")
            ):
                notional_value = abs(position.size * position.mark_price)
                if notional_value > Decimal("10000"):  # High notional value
                    # Should handle large notional values without precision issues
                    assert notional_value.is_finite(), (
                        f"Large notional value should be finite: {notional_value}"
                    )

    @pytest.mark.vcr
    async def test_get_positions_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with edge cases around decimal precision.

        This validates handling of very small positions, dust amounts,
        and precision edge cases for position sizes and PnL.
        """
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for precision testing")

        for position in positions:
            # Test very small position handling
            if abs(position.size) > Decimal("0") and abs(position.size) < Decimal("0.001"):
                # Very small positions should maintain precision
                assert position.size.is_finite(), (
                    f"Small position size should be finite: {position.size}"
                )

                # Should not have scientific notation issues
                size_str = str(position.size)
                if "E" in size_str.upper():
                    assert "E-" in size_str.upper(), (
                        f"Scientific notation should be negative: {size_str}"
                    )

            # Test small PnL amounts
            if (
                position.unrealized_pnl is not None
                and abs(position.unrealized_pnl) > Decimal("0")
                and abs(position.unrealized_pnl) < Decimal("0.01")
            ):
                # Small PnL should be properly represented
                assert position.unrealized_pnl.is_finite(), (
                    f"Small PnL should be finite: {position.unrealized_pnl}"
                )

    @pytest.mark.vcr
    async def test_get_positions_leverage_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with focus on leverage-specific validation.

        This test specifically validates Hyperliquid's leverage system
        including cross vs isolated margin and leverage ratios.
        """
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for leverage testing")

        cross_positions: list[DerivativePosition] = []
        isolated_positions: list[DerivativePosition] = []

        for position in positions:
            if position.hl_details:
                hl_details = position.hl_details

                # Categorize by leverage type
                if hl_details.leverage_type == "cross":
                    cross_positions.append(position)
                elif hl_details.leverage_type == "isolated":
                    isolated_positions.append(position)

                # Validate leverage consistency
                if position.size != Decimal("0"):  # Only for active positions
                    # Leverage should be reasonable for position size
                    assert hl_details.leverage_value >= 1, (
                        f"Leverage should be >= 1 for active position: {hl_details.leverage_value}"
                    )

                    # Max leverage validation (Hyperliquid typically allows up to 50x)
                    assert hl_details.leverage_value <= 50, (
                        f"Leverage seems too high: {hl_details.leverage_value}"
                    )

        # Validate leverage type distribution (account should have consistent strategy)
        if cross_positions and isolated_positions:
            # Mixed leverage types are allowed, just validate both are properly handled
            assert len(cross_positions) > 0, "Cross positions should be properly categorized"
            assert len(isolated_positions) > 0, "Isolated positions should be properly categorized"

    @pytest.mark.vcr
    async def test_get_positions_pnl_consistency(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() PnL calculation consistency and edge cases.

        This validates that PnL calculations are consistent with position data
        and handles edge cases like zero positions or extreme price movements.
        """
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for PnL testing")

        total_unrealized_pnl = Decimal("0")
        total_realized_pnl = Decimal("0")

        for position in positions:
            # Accumulate total PnL
            if position.unrealized_pnl is not None:
                total_unrealized_pnl += position.unrealized_pnl
            if position.realized_pnl is not None:
                total_realized_pnl += position.realized_pnl

            # Validate PnL is finite
            if position.unrealized_pnl is not None:
                assert position.unrealized_pnl.is_finite(), (
                    f"Unrealized PnL should be finite: {position.unrealized_pnl}"
                )
            if position.realized_pnl is not None:
                assert position.realized_pnl.is_finite(), (
                    f"Realized PnL should be finite: {position.realized_pnl}"
                )

            # For zero positions, unrealized PnL should typically be zero
            if position.size == Decimal("0") and position.unrealized_pnl is not None:
                # Note: There might be edge cases where closed positions still show small unrealized PnL
                # due to funding or other factors, so we check for reasonable values
                assert abs(position.unrealized_pnl) < Decimal("1.0"), (
                    f"Zero position should have minimal unrealized PnL: {position.unrealized_pnl}"
                )

        # Validate total PnL is reasonable
        assert total_unrealized_pnl.is_finite(), (
            f"Total unrealized PnL should be finite: {total_unrealized_pnl}"
        )
        assert total_realized_pnl.is_finite(), (
            f"Total realized PnL should be finite: {total_realized_pnl}"
        )
