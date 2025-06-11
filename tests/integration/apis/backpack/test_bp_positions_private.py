"""Integration tests for Backpack private positions endpoints.

This module focuses specifically on testing the DerivativePosition model pipeline
through Backpack's private positions endpoint with Ed25519 authentication.
Tests validate complete data transformation from API responses to DerivativePosition instances.

Model Focus: DerivativePosition
- Validates complete DerivativePosition model field mapping
- Tests Decimal precision for financial values (size, prices, PnL)
- Validates business logic constraints and position calculations
- Tests Backpack-specific position details (bp_details with margin info)
- Comprehensive error handling and position edge cases

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
from cyberdelta.core.models.derivative_position import DerivativePosition

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positions"], indirect=True
)
class TestBackpackPositionsPrivate:
    """Comprehensive private positions integration tests for DerivativePosition model validation."""

    def _validate_position_core_fields(self, position: DerivativePosition, index: int) -> None:
        """Validate core fields of a DerivativePosition."""
        # Validate model type
        assert isinstance(position, DerivativePosition), (
            f"Position {index} should be DerivativePosition instance, got {type(position)}"
        )

        # Validate core fields
        assert position.exchange == "backpack", (
            f"Position.exchange should be 'backpack', got {position.exchange}"
        )

        # Validate symbol format (Backpack uses symbols like "SOL-PERP", "BTC-PERP")
        assert isinstance(position.symbol, str), f"Position {index} symbol must be string"
        assert len(position.symbol) > 0, f"Position {index} symbol cannot be empty"
        assert len(position.symbol) <= 20, f"Position {index} symbol should be reasonable length"

        # Backpack typically uses PERP suffix for perpetual contracts
        if "PERP" in position.symbol.upper():
            assert "-" in position.symbol, (
                f"Position {index} PERP symbol should have dash separator"
            )

        # Validate timestamp recency
        assert position.timestamp is not None, f"Position {index} must have timestamp"
        time_diff = datetime.now(position.timestamp.tzinfo) - position.timestamp
        assert time_diff.total_seconds() < 3600, (
            f"Position {index} timestamp should be recent (< 1 hour), got "
            f"{time_diff.total_seconds()}s ago"
        )

    def _validate_position_decimal_fields(self, position: DerivativePosition, index: int) -> None:
        """Validate Decimal fields of a DerivativePosition."""
        # Validate Decimal precision and types
        assert isinstance(position.size, Decimal), (
            f"Position {index} size must be Decimal, got {type(position.size)}"
        )
        assert isinstance(position.entry_price, Decimal), (
            f"Position {index} entry_price must be Decimal, got {type(position.entry_price)}"
        )
        assert isinstance(position.mark_price, Decimal), (
            f"Position {index} mark_price must be Decimal, got {type(position.mark_price)}"
        )
        assert isinstance(position.unrealized_pnl, Decimal), (
            f"Position {index} unrealized_pnl must be Decimal, got {type(position.unrealized_pnl)}"
        )
        assert isinstance(position.realized_pnl, Decimal), (
            f"Position {index} realized_pnl must be Decimal, got {type(position.realized_pnl)}"
        )

    def _validate_position_prices(self, position: DerivativePosition, index: int) -> None:
        """Validate price fields and relationships of a DerivativePosition."""
        # Validate position size (can be positive, negative, but not zero for active positions)
        if position.size != Decimal("0"):
            # Non-zero positions should have valid entry and mark prices
            if position.entry_price is not None:
                assert position.entry_price > Decimal("0"), (
                    f"Position {index} with non-zero size should have positive entry_price, got "
                    f"{position.entry_price}"
                )
            if position.mark_price is not None:
                assert position.mark_price > Decimal("0"), (
                    f"Position {index} with non-zero size should have positive mark_price, got "
                    f"{position.mark_price}"
                )

        # Validate price relationships and reasonableness
        if (
            position.entry_price is not None
            and position.entry_price > Decimal("0")
            and position.mark_price is not None
            and position.mark_price > Decimal("0")
        ):
            # Prices should be in reasonable range (not negative, not astronomically high)
            assert position.entry_price < Decimal("1000000"), (
                f"Position {index} entry_price seems unreasonably high: {position.entry_price}"
            )
            assert position.mark_price < Decimal("1000000"), (
                f"Position {index} mark_price seems unreasonably high: {position.mark_price}"
            )

    def _validate_position_pnl(self, position: DerivativePosition, index: int) -> None:
        """Validate PnL calculations of a DerivativePosition."""
        # Validate PnL calculations make sense
        if (
            position.size != Decimal("0")
            and position.entry_price is not None
            and position.entry_price > Decimal("0")
            and position.mark_price is not None
            and position.mark_price > Decimal("0")
        ):
            # Calculate expected unrealized PnL and validate it's reasonable
            expected_pnl_direction = (position.mark_price - position.entry_price) * position.size

            # PnL direction should match calculation (allowing for fees and other factors)
            if (
                abs(expected_pnl_direction) > Decimal("0.01")
                and position.unrealized_pnl is not None
            ):  # Only check if significant
                pnl_direction_matches = (
                    expected_pnl_direction > 0 and position.unrealized_pnl >= Decimal("0")
                ) or (expected_pnl_direction < 0 and position.unrealized_pnl <= Decimal("0"))
                assert pnl_direction_matches, (
                    f"Position {index} PnL direction mismatch: expected "
                    f"{expected_pnl_direction > 0}, got "
                    f"unrealized_pnl={position.unrealized_pnl}"
                )

    def _validate_backpack_specific_details(self, position: DerivativePosition, index: int) -> None:
        """Validate Backpack-specific details of a DerivativePosition."""
        # Validate exchange-specific details if present
        if position.bp_details:
            bp_details = position.bp_details

            # Validate Backpack-specific position fields
            initial_margin_req = getattr(bp_details, "initial_margin_requirement", None)
            if initial_margin_req is not None:
                assert isinstance(initial_margin_req, Decimal), (
                    f"Position {index} initial_margin_requirement must be Decimal"
                )
                assert initial_margin_req >= Decimal("0"), (
                    f"Position {index} initial_margin_requirement must be non-negative"
                )

            maintenance_margin_req = getattr(bp_details, "maintenance_margin_requirement", None)
            if maintenance_margin_req is not None:
                assert isinstance(maintenance_margin_req, Decimal), (
                    f"Position {index} maintenance_margin_requirement must be Decimal"
                )
                assert maintenance_margin_req >= Decimal("0"), (
                    f"Position {index} maintenance_margin_requirement must be non-negative"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_positions() with comprehensive DerivativePosition validation.

        This test validates the complete pipeline from Ed25519 authenticated request
        to fully validated DerivativePosition model instances with all field constraints.
        """
        # Execute the API call
        positions = await bp_api_for_test_env.get_positions()

        # Validate container type
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        # Test both empty and populated position scenarios
        if not positions:
            # Empty positions is valid for accounts with no open positions
            return

        # Comprehensive validation of each position
        for i, position in enumerate(positions):
            self._validate_position_core_fields(position, i)
            self._validate_position_decimal_fields(position, i)
            self._validate_position_prices(position, i)
            self._validate_position_pnl(position, i)
            self._validate_backpack_specific_details(position, i)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with account that has no open positions.

        This test validates behavior when account has no derivative positions.
        Important for testing edge cases in position handling.
        """
        positions = await bp_api_for_test_env.get_positions()

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
    @pytest.mark.asyncio
    async def test_get_positions_authentication_failure(
        self,
        bp_api_with_di: Callable[
            ..., BackpackAPI
        ],  # Factory function for creating API with custom secrets
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid Ed25519 authentication."""
        # Create API with invalid credentials
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        bad_api = bp_api_with_di(secrets=invalid_secrets)

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
    @pytest.mark.asyncio
    async def test_get_positions_large_position_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() handling of large position sizes and values.

        This validates that large positions are handled correctly without
        precision loss or overflow issues.
        """
        positions = await bp_api_for_test_env.get_positions()

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
    @pytest.mark.asyncio
    async def test_get_positions_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with edge cases around decimal precision.

        This validates handling of very small positions, dust amounts,
        and precision edge cases for position sizes and PnL.
        """
        positions = await bp_api_for_test_env.get_positions()

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
    @pytest.mark.asyncio
    async def test_get_positions_backpack_specific_details(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with focus on Backpack-specific position details.

        This test specifically validates Backpack's position structure including
        margin requirements and exchange-specific fields.
        """
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for Backpack-specific testing")

        for position in positions:
            if position.bp_details:
                bp_details = position.bp_details

                # Validate Backpack-specific margin fields if present
                initial_margin_req = getattr(bp_details, "initial_margin_requirement", None)
                if initial_margin_req is not None:
                    assert isinstance(initial_margin_req, Decimal), (
                        "initial_margin_requirement should be Decimal"
                    )
                    assert initial_margin_req >= Decimal("0"), (
                        "initial_margin_requirement should be non-negative"
                    )

                    # Initial margin should be reasonable relative to position value
                    if (
                        position.size != Decimal("0")
                        and position.mark_price is not None
                        and position.mark_price > Decimal("0")
                    ):
                        notional_value = abs(position.size * position.mark_price)
                        margin_ratio = initial_margin_req / notional_value
                        assert margin_ratio <= Decimal("1.0"), (
                            f"Initial margin ratio should be <= 100%: {margin_ratio:.4f}"
                        )

                maintenance_margin_req = getattr(bp_details, "maintenance_margin_requirement", None)
                if maintenance_margin_req is not None:
                    assert isinstance(maintenance_margin_req, Decimal), (
                        "maintenance_margin_requirement should be Decimal"
                    )
                    assert maintenance_margin_req >= Decimal("0"), (
                        "maintenance_margin_requirement should be non-negative"
                    )

                    # Maintenance margin should be less than or equal to initial margin
                    if initial_margin_req is not None:
                        assert maintenance_margin_req <= initial_margin_req, (
                            "Maintenance margin should be <= initial margin"
                        )

                # Validate any Backpack-specific cumulative funding if present
                cumulative_funding = getattr(bp_details, "cumulative_funding", None)
                if cumulative_funding is not None:
                    assert isinstance(cumulative_funding, Decimal), (
                        "cumulative_funding should be Decimal"
                    )
                    assert cumulative_funding.is_finite(), "cumulative_funding should be finite"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_pnl_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() PnL calculation consistency and edge cases.

        This validates that PnL calculations are consistent with position data
        and handles edge cases like zero positions or extreme price movements.
        """
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for PnL testing")

        total_unrealized_pnl = Decimal("0")
        total_realized_pnl = Decimal("0")

        for position in positions:
            # Accumulate total PnL (with None checks)
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
                # Note: There might be edge cases where closed positions still show small
                # unrealized PnL
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

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_symbol_format_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() symbol format validation for Backpack.

        This validates that position symbols follow Backpack's naming conventions
        and are properly formatted.
        """
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for symbol validation testing")

        for position in positions:
            symbol = position.symbol

            # Validate symbol format
            assert isinstance(symbol, str), "Symbol should be string"
            assert len(symbol) > 0, "Symbol should not be empty"
            assert symbol == symbol.strip(), "Symbol should not have leading/trailing whitespace"

            # Backpack typically uses patterns like "SOL-PERP", "BTC-PERP"
            if "PERP" in symbol.upper():
                # Perpetual contracts should have proper format
                assert "-" in symbol, f"PERP symbol should have dash separator: {symbol}"
                parts = symbol.split("-")
                assert len(parts) == 2, f"PERP symbol should have exactly one dash: {symbol}"
                assert parts[1].upper() == "PERP", f"Second part should be PERP: {symbol}"
                assert len(parts[0]) >= 2, f"Asset part should be at least 2 characters: {symbol}"

            # Symbol should not contain invalid characters
            invalid_chars = ["<", ">", "&", "'", '"', "%"]
            for char in invalid_chars:
                assert char not in symbol, f"Symbol should not contain {char}: {symbol}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with concurrent requests to same endpoint.

        This validates that concurrent position requests don't interfere with each other
        and that the underlying positions API call handles concurrency properly.
        """
        import asyncio

        # Make multiple concurrent calls
        tasks = [
            bp_api_for_test_env.get_positions(),
            bp_api_for_test_env.get_positions(),
            bp_api_for_test_env.get_positions(),
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
            first_result: list[DerivativePosition] = successful_results[0]
            for _i, result in enumerate(successful_results[1:], 1):
                # Position counts should be the same for concurrent calls
                assert len(first_result) == len(result), (
                    f"Concurrent results should have same position count: {len(first_result)} vs "
                    f"{len(result)}"
                )

                # If there are positions, validate consistency
                if first_result:
                    # Create symbol to position mapping for comparison
                    first_positions = {pos.symbol: pos for pos in first_result}
                    second_positions = {pos.symbol: pos for pos in result}

                    assert set(first_positions.keys()) == set(second_positions.keys()), (
                        "Concurrent results should have same symbols"
                    )

                    # Check position sizes are consistent (allowing for minor timing differences)
                    for symbol in first_positions.keys():
                        first_size = first_positions[symbol].size
                        second_size = second_positions[symbol].size
                        size_diff = abs(first_size - second_size)
                        assert size_diff <= Decimal("0.0001"), (
                            f"Position sizes should be consistent for {symbol}: {first_size} vs "
                            f"{second_size}"
                        )
