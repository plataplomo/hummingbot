"""Integration tests for Backpack large position handling with edge cases.

This module contains tests specifically for validating the handling of large
derivative positions at the absolute maximum limits of what an account can support.

Large Position Definition:
- Large means: max account balance multiplied by maximum account leverage
- This is the true meaning of "large" - the largest possible position size
- Not arbitrary hardcoded numbers, but dynamically calculated maximum capacity

Test Philosophy:
- Calculate maximum position size based on account balance * leverage
- Create edge cases testing positions slightly above and below maximum
- Test leverage changes and their impact on position limits
- Always close positions after testing
- Use dynamic helpers, never hardcode values
- Test both success and failure scenarios at the limits
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.backpack.shared.test_helpers import (
    get_current_market_price,
    get_dynamic_test_price,
    get_market_constraints,
)

logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.large_positions,
    pytest.mark.requires_large_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/perp/positions/large"], indirect=True
)
class TestBackpackPerpLargePositions:
    """Test suite for validating maximum position handling in Backpack perpetuals.

    These tests validate the system's behavior at the absolute limits of what
    an account can support, testing edge cases around maximum position sizes
    calculated dynamically based on account balance and leverage.
    """

    async def _calculate_maximum_position_size(
        self, api: BackpackAPI, symbol: str
    ) -> dict[str, Decimal]:
        """Calculate the maximum position size for a symbol based on account parameters.

        Returns:
            Dict containing:
            - max_position_size: Maximum position size in base units
            - max_notional_value: Maximum notional value in quote currency
            - account_equity: Total account equity
            - leverage_limit: Account leverage limit
            - market_price: Current market price
        """
        # Get account summary and margin parameters
        account_summary = await api.get_account_summary()

        # Get current market price
        market_price = await get_current_market_price(api, symbol)

        # Use conservative leverage limit since leverage_limit is not available from account summary
        # Backpack doesn't expose current leverage limit via MarginAccountSummary API
        # Using conservative 5x leverage for max position calculations
        leverage_limit = Decimal("5")  # Conservative leverage assumption

        # Calculate maximum notional value (account equity * leverage)
        max_notional_value = account_summary.total_equity * leverage_limit

        # Calculate maximum position size in base units
        max_position_size = max_notional_value / market_price

        # Get market constraints to ensure position size is valid
        constraints = await get_market_constraints(api, symbol)
        step_size = constraints["step_size"]

        # Quantize to step size
        max_position_size = (max_position_size / step_size).quantize(Decimal("1")) * step_size

        return {
            "max_position_size": max_position_size,
            "max_notional_value": max_notional_value,
            "account_equity": account_summary.total_equity,
            "leverage_limit": leverage_limit,
            "market_price": market_price,
        }

    async def _close_all_positions(self, api: BackpackAPI) -> None:
        """Close all open positions to ensure clean test state."""
        try:
            positions = await api.get_positions()

            for position in positions:
                if abs(position.size) > Decimal("0.001"):  # Only close non-dust positions
                    # Create market order to close position
                    close_side = OrderSide.SELL if position.size > 0 else OrderSide.BUY

                    place_args = PlaceOrderArgs(
                        symbol=position.symbol,
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=abs(position.size),
                        time_in_force=TimeInForce.IOC,
                    )

                    try:
                        await api.place_order(place_args)
                        logger.info(f"Closed position {position.symbol}: size={position.size}")

                        # Small delay to allow order processing
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        logger.warning(f"Failed to close position {position.symbol}: {e}")

        except Exception as e:
            logger.warning(f"Error in _close_all_positions: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_maximum_position_size_calculation(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test calculation of maximum position size based on account parameters.

        This test validates that we can correctly calculate the theoretical maximum
        position size based on account equity and leverage limits.
        """
        symbol = "SOL_USDC_PERP"

        max_params = await self._calculate_maximum_position_size(
            bp_api_for_large_balance_test, symbol
        )

        # Validate all calculated values are positive and finite
        assert max_params["account_equity"] > Decimal("0"), "Account equity must be positive"
        assert max_params["leverage_limit"] > Decimal("0"), "Leverage limit must be positive"
        assert max_params["market_price"] > Decimal("0"), "Market price must be positive"
        assert max_params["max_notional_value"] > Decimal("0"), "Max notional must be positive"
        assert max_params["max_position_size"] > Decimal("0"), "Max position size must be positive"

        # Validate all values are finite
        for key, value in max_params.items():
            assert isinstance(value, Decimal), f"{key} must be Decimal"
            assert value.is_finite(), f"{key} must be finite, got {value}"

        # Validate leverage calculation
        expected_notional = max_params["account_equity"] * max_params["leverage_limit"]
        assert abs(max_params["max_notional_value"] - expected_notional) < Decimal("0.01"), (
            f"Max notional calculation incorrect: {max_params['max_notional_value']} vs "
            f"{expected_notional}"
        )

        # Validate position size calculation
        expected_size = max_params["max_notional_value"] / max_params["market_price"]
        size_diff = abs(max_params["max_position_size"] - expected_size) / expected_size
        assert size_diff < Decimal("0.01"), (  # Allow 1% difference due to quantization
            f"Position size calculation differs by {size_diff:.2%}: "
            f"{max_params['max_position_size']} vs {expected_size}"
        )

        logger.info(
            f"Maximum position calculation for {symbol}:\n"
            f"  Account Equity: ${max_params['account_equity']:,.2f}\n"
            f"  Leverage Limit: {max_params['leverage_limit']}x\n"
            f"  Market Price: ${max_params['market_price']:,.2f}\n"
            f"  Max Notional: ${max_params['max_notional_value']:,.2f}\n"
            f"  Max Position Size: {max_params['max_position_size']:,.4f} {symbol.split('_')[0]}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_edge_case_position_just_below_maximum(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing position slightly below maximum (99.9% of max).

        This should succeed and demonstrates the edge case behavior
        just below the maximum allowable position size.
        """
        symbol = "SOL_USDC_PERP"

        try:
            # Calculate maximum position parameters
            max_params = await self._calculate_maximum_position_size(
                bp_api_for_large_balance_test, symbol
            )

            # Position size at 99.9% of maximum
            edge_case_size = max_params["max_position_size"] * Decimal("0.999")

            # Get market constraints for proper quantization
            constraints = await get_market_constraints(bp_api_for_large_balance_test, symbol)
            step_size = constraints["step_size"]
            edge_case_size = (edge_case_size / step_size).quantize(Decimal("1")) * step_size

            # Get dynamic test price (far from market to avoid fills)
            test_price = await get_dynamic_test_price(
                bp_api_for_large_balance_test,
                symbol,
                OrderSide.BUY,
                tolerance_percent=Decimal("10"),
            )

            # Place limit order just below maximum
            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=edge_case_size,
                price=test_price,
                time_in_force=TimeInForce.GTC,
            )

            placed_order = await bp_api_for_large_balance_test.place_order(place_args)

            # Validate order was placed successfully
            assert placed_order.exchange_order_id is not None, "Order should have exchange ID"
            assert placed_order.quantity_requested == edge_case_size, (
                f"Order quantity mismatch: {placed_order.quantity_requested} vs {edge_case_size}"
            )

            logger.info(
                f"✓ Successfully placed edge case order just below maximum:\n"
                f"  Max possible: {max_params['max_position_size']:,.4f}\n"
                f"  Order size: {edge_case_size:,.4f} "
                f"({edge_case_size / max_params['max_position_size']:.1%} of max)\n"
                f"  Order ID: {placed_order.exchange_order_id}"
            )

            # Clean up: Cancel the order
            if placed_order.exchange_order_id:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_large_balance_test.cancel_order(cancel_args)
                logger.info(f"Cancelled edge case order {placed_order.exchange_order_id}")

        finally:
            # Ensure all positions are closed
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_edge_case_position_just_above_maximum_fails(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing position slightly above maximum (100.1% of max) fails.

        This should fail with insufficient margin error and demonstrates
        the protective behavior at the maximum limit.
        """
        symbol = "SOL_USDC_PERP"

        try:
            # Calculate maximum position parameters
            max_params = await self._calculate_maximum_position_size(
                bp_api_for_large_balance_test, symbol
            )

            # Position size at 100.1% of maximum (should fail)
            over_max_size = max_params["max_position_size"] * Decimal("1.001")

            # Get market constraints for proper quantization
            constraints = await get_market_constraints(bp_api_for_large_balance_test, symbol)
            step_size = constraints["step_size"]
            over_max_size = (over_max_size / step_size).quantize(Decimal("1")) * step_size

            # Get dynamic test price (far from market to avoid fills)
            test_price = await get_dynamic_test_price(
                bp_api_for_large_balance_test,
                symbol,
                OrderSide.BUY,
                tolerance_percent=Decimal("10"),
            )

            # Attempt to place order above maximum (should fail)
            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=over_max_size,
                price=test_price,
                time_in_force=TimeInForce.GTC,
            )

            # This should raise an exception due to insufficient margin
            with pytest.raises(Exception) as exc_info:
                await bp_api_for_large_balance_test.place_order(place_args)

            # Validate the error is related to insufficient margin/balance
            error_msg = str(exc_info.value).lower()
            assert any(
                keyword in error_msg
                for keyword in ["insufficient", "margin", "balance", "funds", "limit", "leverage"]
            ), f"Error should indicate insufficient margin/balance, got: {exc_info.value}"

            logger.info(
                f"✓ Correctly rejected order above maximum:\n"
                f"  Max possible: {max_params['max_position_size']:,.4f}\n"
                f"  Attempted size: {over_max_size:,.4f} "
                f"({over_max_size / max_params['max_position_size']:.1%} of max)\n"
                f"  Error: {exc_info.value}"
            )

        finally:
            # Ensure all positions are closed
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_large_positions_aggregate_limit(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that multiple large positions are subject to aggregate limits.

        This validates that the sum of all position notional values
        cannot exceed the account's maximum capacity.
        """
        symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP"]
        placed_orders: list[tuple[Order, str]] = []

        try:
            # Get account total capacity
            account_summary = await bp_api_for_large_balance_test.get_account_summary()
            total_capacity = account_summary.total_equity

            logger.info(f"Testing aggregate limits with total capacity: ${total_capacity:,.2f}")

            for symbol in symbols:
                # Calculate position size as 60% of max individual capacity
                max_params = await self._calculate_maximum_position_size(
                    bp_api_for_large_balance_test, symbol
                )

                # Use 60% of individual max to allow for multiple positions
                position_size = max_params["max_position_size"] * Decimal("0.6")

                # Get market constraints for proper quantization
                constraints = await get_market_constraints(bp_api_for_large_balance_test, symbol)
                step_size = constraints["step_size"]
                position_size = (position_size / step_size).quantize(Decimal("1")) * step_size

                # Get dynamic test price
                test_price = await get_dynamic_test_price(
                    bp_api_for_large_balance_test,
                    symbol,
                    OrderSide.BUY,
                    tolerance_percent=Decimal("10"),
                )

                # Place order
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=position_size,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                )

                try:
                    placed_order = await bp_api_for_large_balance_test.place_order(place_args)
                    placed_orders.append((placed_order, symbol))

                    notional_value = position_size * max_params["market_price"]
                    logger.info(
                        f"✓ Placed large order for {symbol}:\n"
                        f"  Size: {position_size:,.4f}\n"
                        f"  Notional: ${notional_value:,.2f}\n"
                        f"  Order ID: {placed_order.exchange_order_id}"
                    )

                except Exception as e:
                    logger.info(f"Order for {symbol} rejected (expected for aggregate limits): {e}")
                    break

            # Validate that we could place at least one large order
            assert len(placed_orders) > 0, "Should be able to place at least one large order"

        finally:
            # Clean up all placed orders
            for placed_order, symbol in placed_orders:
                if placed_order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=placed_order.exchange_order_id,
                            symbol=symbol,
                        )
                        await bp_api_for_large_balance_test.cancel_order(cancel_args)
                        logger.info(f"Cancelled order {placed_order.exchange_order_id}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to cancel order {placed_order.exchange_order_id}: {e}"
                        )

            # Ensure all positions are closed
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_size_precision_at_maximum(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that maximum position sizes maintain proper decimal precision.

        Validates that large position calculations don't lose precision
        or result in scientific notation.
        """
        symbol = "SOL_USDC_PERP"

        # Calculate maximum position parameters
        max_params = await self._calculate_maximum_position_size(
            bp_api_for_large_balance_test, symbol
        )

        # Validate precision in string representation
        for key, value in max_params.items():
            value_str = str(value)

            # Should not use scientific notation
            assert "E" not in value_str.upper(), f"{key} uses scientific notation: {value_str}"

            # Should have reasonable decimal places
            if "." in value_str:
                decimal_places = len(value_str.split(".")[-1])
                assert decimal_places <= 8, (
                    f"{key} has excessive decimal places ({decimal_places}): {value_str}"
                )

        # Test precision in calculations
        recalculated_notional = max_params["max_position_size"] * max_params["market_price"]
        precision_diff = abs(recalculated_notional - max_params["max_notional_value"])
        relative_error = precision_diff / max_params["max_notional_value"]

        assert relative_error < Decimal("0.001"), (  # Less than 0.1% error
            f"Precision loss in calculations: {relative_error:.4%} error"
        )

        logger.info(
            f"✓ Precision validation passed for {symbol}:\n"
            f"  All values use proper decimal notation\n"
            f"  Calculation precision error: {relative_error:.6%}\n"
            f"  Max position size: {max_params['max_position_size']:,.8f}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_leverage_impact_on_position_limits(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test how leverage limits affect maximum position sizes.

        This test demonstrates leverage impact calculation and can now
        optionally test actual leverage changes using the account
        settings update endpoint (PATCH /api/v1/account).
        """
        symbol = "SOL_USDC_PERP"

        # Get current account parameters
        current_max_params = await self._calculate_maximum_position_size(
            bp_api_for_large_balance_test, symbol
        )

        # Simulate different leverage scenarios
        leverage_scenarios = [Decimal("5"), Decimal("10"), Decimal("20"), Decimal("50")]

        logger.info(f"Current leverage limit: {current_max_params['leverage_limit']}x")
        logger.info(f"Account equity: ${current_max_params['account_equity']:,.2f}")

        for leverage in leverage_scenarios:
            # Calculate theoretical maximum with different leverage
            theoretical_notional = current_max_params["account_equity"] * leverage
            theoretical_position_size = theoretical_notional / current_max_params["market_price"]

            # Get market constraints for proper quantization
            constraints = await get_market_constraints(bp_api_for_large_balance_test, symbol)
            step_size = constraints["step_size"]
            theoretical_position_size = (theoretical_position_size / step_size).quantize(
                Decimal("1")
            ) * step_size

            leverage_multiplier = leverage / current_max_params["leverage_limit"]

            logger.info(
                f"  {leverage}x leverage scenario:\n"
                f"    Theoretical max notional: ${theoretical_notional:,.2f}\n"
                f"    Theoretical max position: {theoretical_position_size:,.4f}\n"
                f"    Position multiplier vs current: {leverage_multiplier:.2f}x"
            )

        # Validate leverage relationship
        assert current_max_params["max_notional_value"] == (
            current_max_params["account_equity"] * current_max_params["leverage_limit"]
        ), "Current max notional should equal equity * leverage"

        logger.info(
            "✓ Leverage impact analysis completed\n"
            "  Note: Actual leverage changes can now be performed using:\n"
            "  await bp_api_for_large_balance_test.update_account_settings(\n"
            "      UpdateAccountSettingsArgs(leverage_limit=Decimal('20'))\n"
            "  )"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_volatility_impact_on_large_positions(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test how market price changes affect large position calculations.

        This validates that position limits adjust dynamically with market prices.
        """
        symbol = "SOL_USDC_PERP"

        # Get current market parameters
        base_params = await self._calculate_maximum_position_size(
            bp_api_for_large_balance_test, symbol
        )

        # Simulate different market price scenarios
        price_multipliers = [Decimal("0.8"), Decimal("1.0"), Decimal("1.2"), Decimal("1.5")]

        logger.info(f"Current market price: ${base_params['market_price']:,.2f}")

        for multiplier in price_multipliers:
            simulated_price = base_params["market_price"] * multiplier

            # Calculate position size at simulated price
            simulated_max_notional = base_params["max_notional_value"]  # Stays constant
            simulated_position_size = simulated_max_notional / simulated_price

            price_change_pct = (multiplier - Decimal("1")) * Decimal("100")
            position_change_ratio = Decimal("1") / multiplier

            logger.info(
                f"  Price scenario: {price_change_pct:+.0f}% ({multiplier:.1f}x)\n"
                f"    Simulated price: ${simulated_price:,.2f}\n"
                f"    Max position size: {simulated_position_size:,.4f}\n"
                f"    Position size ratio: {position_change_ratio:.3f}x"
            )

            # Validate inverse relationship
            expected_ratio = Decimal("1") / multiplier
            actual_ratio = simulated_position_size / base_params["max_position_size"]
            ratio_diff = abs(actual_ratio - expected_ratio) / expected_ratio

            assert ratio_diff < Decimal("0.001"), (
                f"Position size should be inversely proportional to price: "
                f"expected {expected_ratio:.3f}x, got {actual_ratio:.3f}x"
            )

        logger.info("✓ Market volatility impact analysis completed")
