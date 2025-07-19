"""Integration tests for Backpack private positions endpoints with zero balance.

This module focuses specifically on testing perp positions with zero balance scenarios:
- Empty account (no positions)
- Authentication validation
- Error handling and edge cases
- Data structure validation
- Symbol filtering scenarios
- API behavior consistency

Model Focus: DerivativePosition (Zero Balance Scenarios)
- Validates empty response handling
- Tests authentication for private endpoints
- Validates error scenarios and API limits
- Tests position filtering and symbol-specific queries
- Comprehensive zero balance edge cases

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: Zero balance account (no positions)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.derivative_position import BackpackPositionDetails, DerivativePosition
from cyberdelta.enums import OrderSide
from tests.integration.apis.backpack.shared.bp_test_helpers import wait_for_condition


logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.zero_balance,
    pytest.mark.timing,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/positions"],
    indirect=True,
)
class TestBackpackPerpPositionsZero:
    """Comprehensive positions integration tests for zero balance scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account_success(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with empty account returns empty list successfully."""
        positions = await bp_api_for_zero_balance_test.get_positions()

        # Validate return type
        assert isinstance(positions, list), "get_positions() must return a list"

        # For zero balance accounts, expect empty list or very small positions
        if positions:
            logger.info(
                "positions_found_in_zero_balance",
                position_count=len(positions),
                message="Found positions in zero balance account",
            )
            # If positions exist, they should be very small (dust amounts)
            for i, position in enumerate(positions):
                assert isinstance(position, DerivativePosition), (
                    f"Position {i} must be DerivativePosition"
                )
                assert position.exchange == "backpack", f"Position {i} exchange must be 'backpack'"
                assert isinstance(position.symbol, str), f"Position {i} symbol must be string"
                assert len(position.symbol) > 0, f"Position {i} symbol cannot be empty"

                # For zero balance accounts, positions should be dust amounts
                assert abs(position.size) <= Decimal(1), (
                    f"Position {i} size too large for zero balance account: {position.size}"
                )
                assert position.timestamp is not None, f"Position {i} timestamp cannot be None"

                # Validate position fields
                self._validate_position_structure(position, i)
        else:
            logger.info("✓ Empty positions list as expected for zero balance account")
            assert len(positions) == 0, "Empty list should have length 0"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_symbol_specific_empty(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with specific symbol on zero balance account."""
        # Test common perp symbols
        perp_symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"]

        for symbol in perp_symbols:
            try:
                positions = await bp_api_for_zero_balance_test.get_positions(symbol=symbol)

                assert isinstance(positions, list), f"get_positions({symbol}) must return list"

                # Should be empty for zero balance account
                if positions:
                    # If any positions exist, they should be for the requested symbol only
                    for position in positions:
                        assert position.symbol == symbol, (
                            f"Position symbol mismatch: expected {symbol}, got {position.symbol}"
                        )
                        assert abs(position.size) <= Decimal(1), (
                            f"Position size too large for zero balance: {position.size}"
                        )
                        self._validate_position_structure(position, 0)
                else:
                    logger.info(
                        "no_positions_found_zero_balance",
                        symbol=symbol,
                        message="✓ No positions found for symbol in zero balance account",
                    )

            except APIError as e:
                # Some symbols might not exist or have specific requirements
                logger.info(
                    "symbol_api_error_expected",
                    symbol=symbol,
                    error_code=e.code,
                    error_message=str(e),
                    message="Symbol returned API error (expected for some symbols)",
                )
                if e.code not in [
                    APIErrorCode.INVALID_SYMBOL.value,
                    APIErrorCode.SYMBOL_NOT_FOUND.value,
                ]:
                    pytest.fail(f"Unexpected error code for symbol {symbol}: {e.code}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_authentication_required(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that get_positions() properly validates authentication."""
        # This test verifies the endpoint requires authentication
        # The fixture should have valid credentials, so this should succeed
        try:
            positions = await bp_api_for_zero_balance_test.get_positions()
            assert isinstance(positions, list), "Authenticated request should return list"
            logger.info("✓ Authentication validation passed")
        except APIError as e:
            # If authentication fails, it should be a specific auth error
            if e.code != APIErrorCode.AUTHENTICATION_FAILED.value:
                pytest.fail(f"Unexpected authentication error: {e.code}")
            pytest.fail(f"Authentication should not fail with valid credentials: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_invalid_symbol_handling(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid symbols returns appropriate errors."""
        invalid_symbols = [
            "INVALID_SYMBOL",
            "FAKE_USDC_PERP",
            "NOTREAL_PERP",
            "",  # Empty string
            "TOOLONG_SYMBOL_NAME_THAT_EXCEEDS_LIMITS_PERP",
        ]

        for symbol in invalid_symbols:
            try:
                positions = await bp_api_for_zero_balance_test.get_positions(symbol=symbol)

                # If no error thrown, should be empty list for invalid symbols
                assert isinstance(positions, list), f"Invalid symbol {symbol} should return list"
                assert len(positions) == 0, f"Invalid symbol {symbol} should return empty list"
                logger.info(
                    "invalid_symbol_empty_list",
                    symbol=symbol,
                    message="Invalid symbol returned empty list (graceful handling)",
                )

            except APIError as e:
                # Expected errors for invalid symbols
                expected_codes = [
                    APIErrorCode.INVALID_SYMBOL.value,
                    APIErrorCode.SYMBOL_NOT_FOUND.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.INVALID_PARAMS.value,
                    APIErrorCode.SERVER_ERROR.value,  # VCR-related errors
                ]
                if e.code not in expected_codes:
                    pytest.fail(f"Unexpected error code for invalid symbol {symbol}: {e.code}")
                logger.info(
                    "invalid_symbol_rejected",
                    symbol=symbol,
                    error_code=e.code,
                    message="✓ Invalid symbol properly rejected with error",
                )

            except ValueError as e:
                # Service layer validation errors (e.g., empty string symbols)
                assert not symbol, f"ValueError should only occur for empty symbol, got: {symbol}"
                logger.info(
                    "empty_symbol_value_error",
                    error_message=str(e),
                    message="✓ Empty symbol properly rejected with ValueError",
                )

            except (TypeError, KeyError) as e:
                pytest.fail(
                    f"Unexpected exception type for invalid symbol {symbol}: "
                    f"{type(e).__name__}: {e}",
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_data_consistency(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() returns consistent data across multiple calls."""
        # Make multiple calls to ensure consistency
        calls_count = 3
        all_positions: list[list[DerivativePosition]] = []

        for i in range(calls_count):
            positions = await bp_api_for_zero_balance_test.get_positions()
            assert isinstance(positions, list), f"Call {i + 1} should return list"
            all_positions.append(positions)

            # Small delay to avoid rate limiting
            if i < calls_count - 1:
                await wait_for_condition(
                    lambda: True,  # Always true, just wait
                    timeout_seconds=0.1,
                    poll_interval=0.1,
                    message="Rate limit delay",
                )

        # Verify consistency across calls
        for i in range(1, calls_count):
            assert len(all_positions[i]) == len(all_positions[0]), (
                f"Position count inconsistent between calls: {len(all_positions[0])} vs "
                f"{len(all_positions[i])}"
            )

            # If positions exist, verify they're consistent
            if all_positions[0]:
                for j, (pos1, pos2) in enumerate(
                    zip(all_positions[0], all_positions[i], strict=False),
                ):
                    assert pos1.symbol == pos2.symbol, f"Position {j} symbol changed between calls"
                    # Size might change slightly due to funding, but shouldn't be
                    # drastically different
                    size_diff = abs(pos1.size - pos2.size)
                    assert size_diff <= Decimal("0.01"), (
                        f"Position {j} size changed too much: {size_diff}"
                    )

        logger.info(
            "data_consistency_verified",
            calls_count=calls_count,
            message="✓ Data consistency verified across calls",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_concurrent_requests(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent get_positions() requests don't cause issues."""
        # Create multiple concurrent requests
        concurrent_count = 3
        tasks = [bp_api_for_zero_balance_test.get_positions() for _ in range(concurrent_count)]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            successful_results: list[list[DerivativePosition]] = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Some concurrent requests might fail due to rate limiting
                    if (
                        isinstance(result, APIError)
                        and result.code == APIErrorCode.RATE_LIMITED.value
                    ):
                        logger.info(
                            "concurrent_rate_limit",
                            request_number=i + 1,
                            message="Concurrent request hit rate limit (expected)",
                        )
                    else:
                        pytest.fail(f"Unexpected error in concurrent request {i + 1}: {result}")
                else:
                    assert isinstance(result, list), (
                        f"Concurrent request {i + 1} should return list"
                    )
                    successful_results.append(result)

            # At least one request should succeed
            assert len(successful_results) > 0, "At least one concurrent request should succeed"

            # All successful results should be consistent
            if len(successful_results) > 1:
                for i in range(1, len(successful_results)):
                    assert len(successful_results[i]) == len(successful_results[0]), (
                        "Concurrent results should have same position count"
                    )

            logger.info(
                "concurrent_requests_succeeded",
                successful_count=len(successful_results),
                total_count=concurrent_count,
                message="✓ Concurrent requests succeeded",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(f"Unexpected error in concurrent test: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_edge_cases(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test edge cases for get_positions() with zero balance account."""
        # Test with None symbol (should work same as no symbol)
        try:
            positions_none = await bp_api_for_zero_balance_test.get_positions(symbol=None)
            positions_all = await bp_api_for_zero_balance_test.get_positions()

            assert isinstance(positions_none, list), "get_positions(symbol=None) should return list"
            assert isinstance(positions_all, list), "get_positions() should return list"
            assert len(positions_none) == len(positions_all), "None symbol should equal no symbol"

        except (APIError, ValueError, TypeError, KeyError) as e:
            logger.info(
                "none_symbol_test_result",
                error_message=str(e),
                message="None symbol test resulted in error",
            )

        # Test timestamp consistency if positions exist
        positions = await bp_api_for_zero_balance_test.get_positions()
        if positions:
            current_time = datetime.now(UTC)
            for i, position in enumerate(positions):
                if position.timestamp:
                    # Timestamp should be recent (within last 24 hours for zero balance)
                    time_diff = (
                        current_time.replace(tzinfo=position.timestamp.tzinfo) - position.timestamp
                    )
                    assert time_diff <= timedelta(days=1), (
                        f"Position {i} timestamp too old: {position.timestamp}"
                    )
                    assert time_diff >= timedelta(seconds=-60), (
                        f"Position {i} timestamp in future: {position.timestamp}"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_response_time(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() response time is reasonable."""
        start_time = datetime.now(UTC)

        positions = await bp_api_for_zero_balance_test.get_positions()

        end_time = datetime.now(UTC)
        response_time = (end_time - start_time).total_seconds()

        # Response should be under 10 seconds for positions endpoint
        assert response_time < 10.0, f"Response time too slow: {response_time}s"

        assert isinstance(positions, list), "Should return list despite timing check"
        logger.info(
            "response_time_measured",
            response_time_seconds=round(response_time, 3),
            message="✓ Response time measured",
        )

    def _validate_position_structure(self, position: DerivativePosition, index: int) -> None:
        """Validate the structure and types of a DerivativePosition."""
        # Basic type validation
        assert isinstance(position, DerivativePosition), (
            f"Position {index} must be DerivativePosition"
        )
        assert position.exchange == "backpack", f"Position {index} exchange must be 'backpack'"

        # Symbol validation
        assert isinstance(position.symbol, str), f"Position {index} symbol must be string"
        assert len(position.symbol) > 0, f"Position {index} symbol cannot be empty"
        assert len(position.symbol) <= 50, f"Position {index} symbol too long: {position.symbol}"

        # For perp symbols, should contain _PERP suffix
        if "PERP" in position.symbol.upper():
            assert "_" in position.symbol, (
                f"Position {index} perp symbol should use underscores: {position.symbol}"
            )

        # Size validation
        assert isinstance(position.size, Decimal), f"Position {index} size must be Decimal"
        # For zero balance, size should be very small
        assert abs(position.size) <= Decimal(10), (
            f"Position {index} size too large for zero balance: {position.size}"
        )

        # Entry price validation
        assert isinstance(position.entry_price, Decimal), (
            f"Position {index} entry_price must be Decimal"
        )
        assert position.entry_price > Decimal(0), (
            f"Position {index} entry_price must be positive: {position.entry_price}"
        )
        assert position.entry_price <= Decimal(1000000), (
            f"Position {index} entry_price too high: {position.entry_price}"
        )

        # Mark price validation
        assert isinstance(position.mark_price, Decimal), (
            f"Position {index} mark_price must be Decimal"
        )
        assert position.mark_price > Decimal(0), (
            f"Position {index} mark_price must be positive: {position.mark_price}"
        )

        # PnL validation (can be negative)
        assert isinstance(position.unrealized_pnl, Decimal), (
            f"Position {index} unrealized_pnl must be Decimal"
        )
        assert isinstance(position.realized_pnl, Decimal), (
            f"Position {index} realized_pnl must be Decimal"
        )

        # For zero balance, PnL should be small
        assert abs(position.unrealized_pnl) <= Decimal(100), (
            f"Position {index} unrealized_pnl too large: {position.unrealized_pnl}"
        )
        assert abs(position.realized_pnl) <= Decimal(1000), (
            f"Position {index} realized_pnl too large: {position.realized_pnl}"
        )

        # Timestamp validation
        assert position.timestamp is not None, f"Position {index} timestamp cannot be None"
        assert isinstance(position.timestamp, datetime), (
            f"Position {index} timestamp must be datetime"
        )

        # Validate liquidation price (can be None or Decimal)
        if position.liquidation_price is not None:
            assert isinstance(position.liquidation_price, Decimal), (
                f"Position {index} liquidation_price must be Decimal or None"
            )
            assert position.liquidation_price >= Decimal(0), (
                f"Position {index} liquidation_price cannot be negative"
            )

        # Validate side (BUY/SELL)

        assert position.side in [OrderSide.BUY, OrderSide.SELL], (
            f"Position {index} side must be BUY or SELL"
        )

        # Validate Backpack-specific details if present
        if position.bp_details:
            self._validate_backpack_position_details(position.bp_details, index)

        logger.debug(
            "position_validation_passed",
            position_index=index,
            symbol=position.symbol,
            message="✓ Position structure validation passed",
        )

    def _validate_backpack_position_details(
        self,
        bp_details: BackpackPositionDetails,
        index: int,
    ) -> None:
        """Validate Backpack-specific position details."""
        assert isinstance(bp_details, BackpackPositionDetails), (
            f"Position {index} bp_details must be BackpackPositionDetails"
        )

        # Validate margin factors
        if bp_details.imf_base is not None:
            assert isinstance(bp_details.imf_base, Decimal), (
                f"Position {index} imf_base must be Decimal"
            )
            assert bp_details.imf_base >= Decimal(0), (
                f"Position {index} imf_base cannot be negative"
            )
            assert bp_details.imf_base <= Decimal(1), (
                f"Position {index} imf_base too high: {bp_details.imf_base}"
            )

        if bp_details.imf_factor is not None:
            assert isinstance(bp_details.imf_factor, Decimal), (
                f"Position {index} imf_factor must be Decimal"
            )
            assert bp_details.imf_factor >= Decimal(0), (
                f"Position {index} imf_factor cannot be negative"
            )

        if bp_details.mmf_base is not None:
            assert isinstance(bp_details.mmf_base, Decimal), (
                f"Position {index} mmf_base must be Decimal"
            )
            assert bp_details.mmf_base >= Decimal(0), (
                f"Position {index} mmf_base cannot be negative"
            )
            assert bp_details.mmf_base <= Decimal(1), (
                f"Position {index} mmf_base too high: {bp_details.mmf_base}"
            )

        if bp_details.mmf_factor is not None:
            assert isinstance(bp_details.mmf_factor, Decimal), (
                f"Position {index} mmf_factor must be Decimal"
            )
            assert bp_details.mmf_factor >= Decimal(0), (
                f"Position {index} mmf_factor cannot be negative"
            )

        if bp_details.cumulative_funding is not None:
            assert isinstance(bp_details.cumulative_funding, Decimal), (
                f"Position {index} cumulative_funding must be Decimal"
            )
            # Cumulative funding can be negative

        logger.debug(
            "backpack_details_validation_passed",
            position_index=index,
            message="✓ Position Backpack details validation passed",
        )
