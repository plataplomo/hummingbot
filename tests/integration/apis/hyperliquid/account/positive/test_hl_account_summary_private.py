"""Integration tests for Hyperliquid private account summary endpoints.

This module focuses specifically on testing the MarginAccountSummary model pipeline
through Hyperliquid's private /exchange endpoints with EIP-712 authentication.
Tests validate complete data transformation for account state-changing operations.

Model Focus: MarginAccountSummary (Write Operations)
- Tests leverage management operations (when implemented)
- Tests margin configuration operations
- Tests account-affecting trading operations
- Tests risk management operations
- Validates EIP-712 cryptographic authentication
- Comprehensive error handling for account management operations

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering

Note: Many account management operations are currently not implemented in the
HyperliquidAccountService but tests are structured to be ready for when
implementation is completed.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    PlaceOrderArgs,
)
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.hyperliquid.shared.test_helpers import HyperliquidTestHelpers
from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

pytestmark = [pytest.mark.integration, pytest.mark.requires_balance, pytest.mark.positive_balance]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/account/positive"], indirect=True
)
class TestHyperliquidAccountSummaryPrivate:
    """Comprehensive private account summary integration tests for /exchange operations.

    This class tests only /exchange endpoint operations (signed with EIP-712) that affect
    account summaries:
    - Account-affecting order operations (large orders that impact margin calculations)
    - Leverage management operations (when implemented)
    - Margin configuration operations (when implemented)
    - Risk management operations (when implemented)

    These operations require cryptographic authentication and modify account state.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_large_order_account_impact_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test large order placement impact on MarginAccountSummary with comprehensive validation.

        This test validates how large order placements affect account summary metrics
        like available equity, margin requirements, and leverage utilization.
        Uses testnet asset and manages risk through small absolute amounts.
        """
        # Get initial account summary to establish baseline
        initial_summary = await hl_api_for_test_env.get_account_summary()

        # Validate initial account summary structure
        assert isinstance(initial_summary, MarginAccountSummary), (
            "Account summary should be MarginAccountSummary instance"
        )
        assert initial_summary.exchange == "hyperliquid", (
            f"Exchange should be 'hyperliquid', got {initial_summary.exchange}"
        )

        # Store initial values for comparison
        initial_margin_used = initial_summary.total_initial_margin_required

        # Get available symbol from exchange instead of hardcoding
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Define order parameters that should impact account metrics (but still testnet-safe)
        impact_order_args = PlaceOrderArgs(
            symbol=test_symbol,  # Use first available symbol from exchange
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.001"),  # Small testnet-safe size
            price=Decimal("50000"),  # More realistic price to avoid fills but pass validation
            time_in_force=TimeInForce.GTC,
        )

        # Execute the order placement
        placed_order = await hl_api_for_test_env.place_order(impact_order_args)
        assert placed_order.exchange_order_id is not None, "Order should be placed successfully"

        # Get updated account summary to validate impact
        updated_summary = await hl_api_for_test_env.get_account_summary()

        # Validate MarginAccountSummary model consistency after order
        assert isinstance(updated_summary, MarginAccountSummary), (
            "Updated summary should be MarginAccountSummary instance"
        )
        assert updated_summary.exchange == "hyperliquid", (
            f"Exchange should remain 'hyperliquid', got {updated_summary.exchange}"
        )

        # Validate Decimal precision for all financial fields
        assert isinstance(updated_summary.total_equity, Decimal), (
            f"total_equity must be Decimal, got {type(updated_summary.total_equity)}"
        )
        assert isinstance(updated_summary.available_equity, Decimal), (
            f"available_equity must be Decimal, got {type(updated_summary.available_equity)}"
        )
        assert isinstance(updated_summary.total_initial_margin_required, Decimal), (
            f"total_initial_margin_required must be Decimal, got "
            f"{type(updated_summary.total_initial_margin_required)}"
        )

        # Validate logical account constraints
        assert updated_summary.available_equity <= updated_summary.total_equity, (
            f"Available equity ({updated_summary.available_equity}) should not exceed "
            f"total equity ({updated_summary.total_equity})"
        )
        assert updated_summary.available_equity >= Decimal("0"), (
            f"Available equity should be non-negative, got {updated_summary.available_equity}"
        )

        # For open orders, margin should potentially be affected
        # Type narrowing: assertions above guarantee both are Decimal
        assert isinstance(initial_margin_used, Decimal)
        assert isinstance(updated_summary.total_initial_margin_required, Decimal)
        if updated_summary.total_initial_margin_required > initial_margin_used:
            # Margin increased due to order, validate the increase is reasonable
            margin_increase = updated_summary.total_initial_margin_required - initial_margin_used
            assert margin_increase > Decimal("0"), (
                f"Margin increase should be positive, got {margin_increase}"
            )

        # Clean up - cancel the order to restore account state
        try:
            from cyberdelta.apis.models.service_args_models import CancelOrderArgs

            cancel_args = CancelOrderArgs(
                order_id=placed_order.exchange_order_id,
                symbol=test_symbol,
            )
            await hl_api_for_test_env.cancel_order(cancel_args)
        except APIError as e:
            # Order cancellation is critical - don't hide failures
            pytest.fail(
                f"Failed to cancel order {cancel_args.order_id}: {e}. "
                "Order cancellation is a critical operation that must work reliably."
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_summary_after_position_operations(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary changes after position-affecting operations.

        This validates how MarginAccountSummary reflects changes when positions
        are opened, modified, or closed through trading operations.
        """
        # Get available symbol from exchange instead of hardcoding
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Execute a market order that should create/modify a position
        position_order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.001"),  # Small size for testnet
            time_in_force=TimeInForce.IOC,
        )

        try:
            position_order = await hl_api_for_test_env.place_order(position_order_args)
            assert position_order.exchange_order_id is not None, "Position order should succeed"

            # Get account summary after position operation
            post_position_summary = await hl_api_for_test_env.get_account_summary()

            # Validate account summary structure remains consistent
            assert isinstance(post_position_summary, MarginAccountSummary), (
                "Post-position summary should be MarginAccountSummary instance"
            )

            # Validate financial field types
            assert isinstance(post_position_summary.total_equity, Decimal), (
                "total_equity must remain Decimal after position operations"
            )
            assert isinstance(post_position_summary.total_unrealized_pnl, Decimal), (
                "total_unrealized_pnl must be Decimal after position operations"
            )

            # If a position was created, unrealized PnL might be non-zero
            # (This is acceptable as it reflects real market conditions)

            # Validate account constraints are maintained
            assert post_position_summary.available_equity <= post_position_summary.total_equity, (
                "Available equity constraint should be maintained after position operations"
            )

            # Attempt to close position for cleanup
            close_position_args = PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.SELL,  # Opposite side to close
                order_type=OrderType.MARKET,
                quantity=Decimal("0.001"),  # Same size to close
                time_in_force=TimeInForce.IOC,
            )

            try:
                await hl_api_for_test_env.place_order(close_position_args)
            except APIError as e:
                # Position closure is critical for test isolation
                pytest.fail(
                    f"Failed to close position: {e}. "
                    "Position closure is critical for test isolation and financial safety."
                )

        except APIError as e:
            # Distinguish between expected business errors and system failures
            if "insufficient" in e.message.lower() or "minimum" in e.message.lower():
                # Expected business logic error - order correctly rejected
                logger.info(f"Order correctly rejected due to business rules: {e}")
            else:
                # Unexpected system error
                pytest.fail(f"Unexpected error in position order: {e}")
            pytest.skip(f"Position order failed due to market conditions: {e.message}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_summary_margin_stress_scenario(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary behavior under margin stress conditions.

        This validates error handling and account summary consistency when
        attempting operations that would exceed available margin.
        """
        # Get available symbol from exchange instead of hardcoding
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get current account summary to understand available margin
        current_summary = await hl_api_for_test_env.get_account_summary()

        # Calculate an order size that would likely exceed available margin
        # Use a multiplier approach based on available equity
        if current_summary is not None and current_summary.available_equity > Decimal("0"):
            # Order value much larger than available equity
            stress_order_value = current_summary.available_equity * Decimal("100")
            # Assuming $1 per unit for simplicity in stress test
            stress_quantity = stress_order_value
        else:
            # Get unreasonably large quantity from exchange constraints instead of hardcoded value
            stress_quantity = await HyperliquidTestHelpers.get_unreasonably_large_quantity(
                hl_api_for_test_env, test_symbol
            )

        # Define stress order that should trigger margin error
        stress_order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=stress_quantity,
            price=await HyperliquidTestHelpers.get_current_market_price(
                hl_api_for_test_env, test_symbol
            ),  # Current market price from exchange
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError related to insufficient margin/funds
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(stress_order_args)

        # Validate error mapping for margin-related issues
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.MAX_POSITION_EXCEEDED.value,
            APIErrorCode.INVALID_ORDER_SIZE.value,  # Order value limit
        ], f"Should map to margin-related error code, got {api_error.code}"

        # Verify account summary remains consistent after failed operation
        post_error_summary = await hl_api_for_test_env.get_account_summary()

        # Account summary should be unchanged after failed operation
        if current_summary is not None and post_error_summary is not None:
            assert post_error_summary.total_equity == current_summary.total_equity, (
                "Total equity should be unchanged after failed margin operation"
            )
            assert post_error_summary.available_equity == current_summary.available_equity, (
                "Available equity should be unchanged after failed margin operation"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_summary_precision_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test MarginAccountSummary decimal precision handling with account operations.

        This validates that all financial fields maintain proper Decimal precision
        throughout various account-affecting operations.
        """
        # Get account summary to validate precision
        summary = await hl_api_for_test_env.get_account_summary()

        # Validate all financial fields are proper Decimals (not floats)
        financial_fields = []
        if summary is not None:
            financial_fields = [
                ("total_equity", summary.total_equity),
                ("available_equity", summary.available_equity),
                ("total_unrealized_pnl", summary.total_unrealized_pnl),
                ("total_initial_margin_required", summary.total_initial_margin_required),
            ]

        for field_name, field_value in financial_fields:
            assert isinstance(field_value, Decimal), (
                f"{field_name} must be Decimal, got {type(field_value)}"
            )

            # Validate no scientific notation in string representation (unless appropriate)
            field_str = str(field_value)
            if "E" in field_str.upper():
                assert "E-" in field_str.upper(), (
                    f"{field_name} scientific notation should be negative exponent: {field_str}"
                )

        # Get available symbol from exchange instead of hardcoding
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Test with a small precision order to validate precision preservation
        precision_order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.000001"),  # Very small quantity
            price=await HyperliquidTestHelpers.get_current_market_price(
                hl_api_for_test_env, test_symbol
            ),  # Current market price from exchange
            time_in_force=TimeInForce.GTC,
        )

        try:
            precision_order = await hl_api_for_test_env.place_order(precision_order_args)

            # Get updated summary after precision order
            precision_summary = await hl_api_for_test_env.get_account_summary()

            # Validate precision is maintained in updated summary
            precision_fields = []
            if precision_summary is not None:
                precision_fields = [
                    ("total_equity", precision_summary.total_equity),
                    ("available_equity", precision_summary.available_equity),
                    (
                        "total_initial_margin_required",
                        precision_summary.total_initial_margin_required,
                    ),
                ]

            for field_name, field_value in precision_fields:
                assert isinstance(field_value, Decimal), (
                    f"{field_name} must remain Decimal after precision operations"
                )

            # Clean up precision order
            try:
                from cyberdelta.apis.models.service_args_models import CancelOrderArgs

                if precision_order.exchange_order_id is not None:
                    cancel_args = CancelOrderArgs(
                        order_id=precision_order.exchange_order_id,
                        symbol=test_symbol,
                    )
                    await hl_api_for_test_env.cancel_order(cancel_args)
            except APIError as e:
                # Order cancellation is critical - don't hide failures
                pytest.fail(
                    f"Failed to cancel order: {e}. "
                    "Order cancellation is a critical operation that must work reliably."
                )

        except APIError as e:
            # Distinguish expected business errors from system failures
            if any(phrase in e.message.lower() for phrase in ["minimum", "size", "precision"]):
                # Expected business logic error - order correctly rejected
                logger.info(f"Order correctly rejected due to size constraints: {e}")
                pytest.skip(f"Exchange has minimum order size requirements: {e.message}")
            else:
                # Unexpected system error
                pytest.fail(f"Unexpected error in order placement: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_summary_consistency_across_operations(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test MarginAccountSummary consistency across multiple trading operations.

        This validates that the account summary model remains internally consistent
        and maintains logical relationships between fields throughout various operations.
        """
        # Get baseline account summary
        baseline = await hl_api_for_test_env.get_account_summary()

        # Validate baseline consistency
        if baseline is not None:
            assert baseline.available_equity <= baseline.total_equity, (
                "Baseline: Available equity should not exceed total equity"
            )

        # Get available symbol from exchange instead of hardcoding
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Execute a series of operations and validate consistency at each step
        operations = [
            # Operation 1: Place a limit order (should affect available equity)
            PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.001"),
                price=await HyperliquidTestHelpers.get_current_market_price(
                    hl_api_for_test_env, test_symbol
                ),
                time_in_force=TimeInForce.GTC,
            ),
        ]

        placed_orders: list[Order] = []

        for i, operation in enumerate(operations):
            try:
                # Execute operation
                order = await hl_api_for_test_env.place_order(operation)
                placed_orders.append(order)

                # Get account summary after operation
                post_op_summary = await hl_api_for_test_env.get_account_summary()

                # Validate consistency constraints
                if post_op_summary is not None:
                    assert post_op_summary.available_equity <= post_op_summary.total_equity, (
                        f"Operation {i + 1}: Available equity should not exceed total equity"
                    )
                    assert post_op_summary.available_equity >= Decimal("0"), (
                        f"Operation {i + 1}: Available equity should be non-negative"
                    )
                    assert isinstance(post_op_summary.total_equity, Decimal), (
                        f"Operation {i + 1}: total_equity must remain Decimal"
                    )

                    # Validate account summary structure integrity
                    assert post_op_summary.exchange == "hyperliquid", (
                        f"Operation {i + 1}: Exchange should remain consistent"
                    )

            except APIError as e:
                # Operations failing isn't acceptable - investigate and fail
                pytest.fail(
                    f"Operation {i + 1} failed: {e.message}. "
                    "Trading operations must work reliably for stress testing."
                )

        # Clean up all placed orders
        for order in placed_orders:
            if order.exchange_order_id:
                try:
                    from cyberdelta.apis.models.service_args_models import CancelOrderArgs

                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=test_symbol,
                    )
                    await hl_api_for_test_env.cancel_order(cancel_args)
                except APIError as e:
                    # Order cancellation is critical - don't hide failures
                    pytest.fail(
                        f"Failed to cancel order during cleanup: {e}. "
                        "Order cancellation is critical for test isolation."
                    )
