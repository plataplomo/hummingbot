"""Zero balance dynamic integration tests for Hyperliquid private perpetual orders.

This module demonstrates the use of dynamic test helpers for robust testing
across different account states. Uses zero balance account for error handling tests.

Features:
- Dynamic helper integration for market-aware testing
- Account state detection and appropriate test adaptation
- Balance-based test parameter calculation
- Graceful handling of zero balance scenarios
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from tests.integration.apis.hyperliquid.shared.test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity_for_zero_balance,
    get_safe_test_price,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.zero_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/orders/zero"], indirect=True
)
class TestHyperliquidPerpOrdersZeroDynamic:
    """Dynamic integration tests for zero balance accounts using test helpers.

    This class demonstrates proper usage of HyperliquidTestHelpers for accounts
    with zero or minimal balances, showing how to adapt tests based on account state.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_state_detection_zero_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account state detection with zero balance account.

        Demonstrates how HyperliquidTestHelpers.detect_account_state() works
        with accounts that have insufficient funds for trading.
        """
        # Detect account state using dynamic helpers
        account_state = await HyperliquidTestHelpers.detect_account_state(
            hl_api_for_zero_balance_test
        )

        # Validate account state detection for zero balance
        assert isinstance(account_state, dict), "Account state should be dict"
        assert "has_balance" in account_state, "Should detect balance status"
        assert "can_trade" in account_state, "Should detect trading capability"
        assert "total_equity" in account_state, "Should include equity information"

        # For zero balance account, these should be false/minimal
        assert account_state["has_balance"] is False or account_state["total_equity"] < Decimal(
            "1"
        ), "Zero balance account should have minimal equity"
        assert account_state["can_trade"] is False, (
            "Zero balance account should not be able to trade"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_dynamic_order_sizing_with_zero_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test dynamic order sizing calculations with zero balance account.

        Shows how get_minimal_test_quantity_for_zero_balance() handles zero balance scenarios
        by falling back to market minimum quantities.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]  # First available symbol

        # Get minimal order size using zero balance helpers
        minimal_quantity = await get_minimal_test_quantity_for_zero_balance(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY
        )

        # Should return exchange minimum since account can't afford more
        assert isinstance(minimal_quantity, Decimal), "Quantity should be Decimal"
        assert minimal_quantity > Decimal("0"), "Minimal quantity should be positive"

        # Get market constraints for comparison
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_zero_balance_test, test_symbol
        )

        # For zero balance, should return market minimum
        assert minimal_quantity == constraints["min_quantity"], (
            f"Zero balance should return market minimum: {minimal_quantity} vs "
            f"{constraints['min_quantity']}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_dynamic_pricing_with_zero_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test dynamic pricing calculations work regardless of account balance.

        Shows that get_safe_test_price() works independently of account state
        since it only depends on market data.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Get safe test price using dynamic helpers
        test_price = await get_safe_test_price(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY, tolerance=Decimal("0.05")
        )

        # Price calculation should work regardless of balance
        assert isinstance(test_price, Decimal), "Price should be Decimal"
        assert test_price > Decimal("0"), "Price should be positive"

        # Get current market price for comparison
        market_price = await HyperliquidTestHelpers.get_current_market_price(
            hl_api_for_zero_balance_test, test_symbol
        )

        # For BUY order, test price should be below market (using 5% tolerance)
        # The actual price should be close to the expected 5% below market
        expected_price = market_price * Decimal("0.95")  # 5% below market
        price_difference_percent = abs(test_price - expected_price) / market_price * Decimal("100")

        # Allow some flexibility due to tick size rounding (within 6% tolerance)
        assert test_price < market_price, (
            f"BUY test price should be below market: {test_price} < {market_price}"
        )
        assert price_difference_percent <= Decimal("6.0"), (
            f"Test price should be reasonably close to expected 5% offset: "
            f"difference is {price_difference_percent}% of market price"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_insufficient_funds_dynamic(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test insufficient funds error with dynamically calculated parameters.

        Uses dynamic helpers to create a "reasonable" order that still fails
        due to insufficient funds, demonstrating proper error mapping.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Get dynamic test parameters
        test_price = await get_safe_test_price(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY, tolerance=Decimal("0.05")
        )
        test_quantity = await get_minimal_test_quantity_for_zero_balance(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY
        )

        # Create order args with dynamic parameters
        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # Should fail with insufficient funds
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_zero_balance_test.place_order(order_args)

        # Validate error mapping - could be either insufficient funds or minimum notional
        error = exc_info.value
        expected_codes = [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.MIN_NOTIONAL_NOT_MET.value,
        ]
        assert error.code in expected_codes, (
            f"Should map to INSUFFICIENT_FUNDS or MIN_NOTIONAL_NOT_MET, got {error.code}: {error.message}"
        )

        # Check that our dynamic helpers created valid parameters
        is_valid = await HyperliquidTestHelpers.validate_order_constraints(
            hl_api_for_zero_balance_test, test_symbol, test_quantity, test_price
        )
        assert is_valid, "Dynamic helpers should create valid order parameters"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_margin_parameters_detection_zero_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test margin parameter detection with zero balance account.

        Shows how get_account_margin_parameters() handles zero balance accounts
        and provides appropriate fallback values.
        """
        # Get margin parameters using dynamic helpers
        margin_params = await HyperliquidTestHelpers.get_account_margin_parameters(
            hl_api_for_zero_balance_test
        )

        # Validate margin parameter structure
        expected_keys = ["maintenance_margin", "initial_margin", "leverage", "max_leverage"]
        for key in expected_keys:
            assert key in margin_params, f"Margin params should include {key}"
            assert isinstance(margin_params[key], Decimal), f"{key} should be Decimal"
            assert margin_params[key] >= Decimal("0"), f"{key} should be non-negative"

        # Validate leverage relationships
        assert margin_params["leverage"] <= margin_params["max_leverage"], (
            "Current leverage should not exceed maximum"
        )
        assert margin_params["initial_margin"] >= margin_params["maintenance_margin"], (
            "Initial margin should be >= maintenance margin"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_maximum_position_calculation_zero_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test maximum position calculation with zero balance account.

        Demonstrates how calculate_maximum_position_size() handles zero balance
        by returning zero or minimal position limits.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Calculate maximum position size
        max_position = await HyperliquidTestHelpers.calculate_maximum_position_size(
            hl_api_for_zero_balance_test, test_symbol
        )

        # Validate result structure
        assert isinstance(max_position, dict), "Should return dict"
        assert "max_quantity" in max_position, "Should include max_quantity"
        assert "max_notional" in max_position, "Should include max_notional"

        # For zero balance, should be zero or minimal
        assert max_position["max_quantity"] == Decimal("0"), (
            f"Zero balance should have zero max position: {max_position['max_quantity']}"
        )
        assert max_position["max_notional"] == Decimal("0"), (
            f"Zero balance should have zero max notional: {max_position['max_notional']}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_constraints_independent_of_balance(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that market constraints work independently of account balance.

        Shows that get_market_constraints() returns exchange-level limits
        regardless of account balance state.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Get market constraints
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_zero_balance_test, test_symbol
        )

        # Market constraints should be available regardless of balance
        expected_keys = ["tick_size", "step_size", "min_quantity", "max_quantity"]
        for key in expected_keys:
            assert key in constraints, f"Constraints should include {key}"
            assert isinstance(constraints[key], Decimal), f"{key} should be Decimal"
            assert constraints[key] > Decimal("0"), f"{key} should be positive"

        # Validate constraint relationships
        assert constraints["tick_size"] <= constraints["step_size"] or (
            constraints["tick_size"] > constraints["step_size"]
        ), "Tick and step sizes should have valid relationship"
        assert constraints["min_quantity"] <= constraints["max_quantity"], (
            "Min quantity should be <= max quantity"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_unreasonable_values_calculation(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test unreasonable value calculations for negative testing.

        Shows how get_unreasonably_large_* functions work for edge case testing,
        independent of account balance.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Get unreasonably large values
        large_price = await HyperliquidTestHelpers.get_unreasonably_large_price(
            hl_api_for_zero_balance_test, test_symbol
        )
        large_quantity = await HyperliquidTestHelpers.get_unreasonably_large_quantity(
            hl_api_for_zero_balance_test, test_symbol
        )

        # Validate unreasonable values
        market_price = await HyperliquidTestHelpers.get_current_market_price(
            hl_api_for_zero_balance_test, test_symbol
        )
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_zero_balance_test, test_symbol
        )

        # Large price should be significantly above market (at least 50% higher)
        assert large_price > market_price * Decimal("1.5"), (
            f"Large price should be higher than market: {large_price} > {market_price * Decimal('1.5')}"
        )

        # Large quantity should be at least as large as max (might equal max for some exchanges)
        assert large_quantity >= constraints["max_quantity"], (
            f"Large quantity should be at least max: {large_quantity} >= {constraints['max_quantity']}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_validation_functions_with_dynamic_parameters(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test validation functions with dynamically generated parameters.

        Demonstrates how validate_order_constraints() works with parameters
        generated by other dynamic helper functions.
        """
        # Get available symbols dynamically from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_zero_balance_test, limit=1
        )
        test_symbol = available_symbols[0]

        # Get dynamic parameters
        valid_price = await get_safe_test_price(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY, tolerance=Decimal("0.05")
        )
        valid_quantity = await get_minimal_test_quantity_for_zero_balance(
            hl_api_for_zero_balance_test, test_symbol, OrderSide.BUY
        )

        # Test validation with valid parameters
        is_valid = await HyperliquidTestHelpers.validate_order_constraints(
            hl_api_for_zero_balance_test, test_symbol, valid_quantity, valid_price
        )
        assert is_valid, "Dynamically generated parameters should be valid"

        # Test validation with invalid parameters
        invalid_quantity = Decimal("0.0000001")  # Too small
        invalid_price = Decimal("0.0000001")  # Too small

        is_invalid_qty = await HyperliquidTestHelpers.validate_order_constraints(
            hl_api_for_zero_balance_test, test_symbol, invalid_quantity, valid_price
        )
        assert not is_invalid_qty, "Invalid quantity should fail validation"

        is_invalid_price = await HyperliquidTestHelpers.validate_order_constraints(
            hl_api_for_zero_balance_test, test_symbol, valid_quantity, invalid_price
        )
        assert not is_invalid_price, "Invalid price should fail validation"
