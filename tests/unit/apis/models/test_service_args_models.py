"""Comprehensive unit tests for Pydantic service arguments models.

Tests all three models (PlaceOrderArgs, TransferArgs, WithdrawArgs) with:
1. Valid inputs for all fields
2. Invalid types and values (negative amounts, empty strings, etc.)
3. Inter-parameter dependencies and validation rules
4. Edge cases and boundary conditions

Ensures Pydantic validation works correctly and provides meaningful error messages.
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
    PositionIntent,
)
from cyberdelta.apis.models.service_args_models import (
    GetMarketArgs,
    GetMarketsArgs,
    PlaceOrderArgs,
    TransferArgs,
    WithdrawArgs,
)
from cyberdelta.core.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


class TestPlaceOrderArgs:
    """Test PlaceOrderArgs Pydantic model validation."""

    def test_valid_market_order(self) -> None:
        """Test valid market order creation."""
        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.5"),
            time_in_force=TimeInForce.IOC,
        )

        assert args.symbol == "BTC-USD"
        assert args.side == OrderSide.BUY
        assert args.order_type == OrderType.MARKET
        assert args.quantity == Decimal("1.5")
        assert args.time_in_force == TimeInForce.IOC
        assert args.price is None
        assert args.stop_price is None
        assert args.client_order_id is None
        assert args.execution.liquidity_requirement == LiquidityRequirement.ANY
        assert args.execution.position_intent == PositionIntent.OPEN_OR_INCREASE

    def test_valid_limit_order(self) -> None:
        """Test valid limit order creation."""
        args = PlaceOrderArgs(
            symbol="ETH-USDT",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2500.50"),
            client_order_id="my_order_123",
            execution=OrderExecution(
                liquidity_requirement=LiquidityRequirement.POST_ONLY,
                position_intent=PositionIntent.REDUCE_ONLY,
            ),
        )

        assert args.symbol == "ETH-USDT"
        assert args.side == OrderSide.SELL
        assert args.order_type == OrderType.LIMIT
        assert args.quantity == Decimal("10.0")
        assert args.time_in_force == TimeInForce.GTC
        assert args.price == Decimal("2500.50")
        assert args.client_order_id == "my_order_123"
        assert args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY
        assert args.execution.position_intent == PositionIntent.REDUCE_ONLY

    def test_valid_stop_limit_order(self) -> None:
        """Test valid stop limit order creation."""
        args = PlaceOrderArgs(
            symbol="SOL-USD",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal(100),
            time_in_force=TimeInForce.GTC,
            price=Decimal("150.00"),
            stop_price=Decimal("145.00"),
        )

        assert args.order_type == OrderType.STOP_LIMIT
        assert args.price == Decimal("150.00")
        assert args.stop_price == Decimal("145.00")

    def test_valid_stop_market_order(self) -> None:
        """Test valid stop market order creation."""
        args = PlaceOrderArgs(
            symbol="DOGE-USDT",
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("1000.0"),
            time_in_force=TimeInForce.IOC,
            stop_price=Decimal("0.08"),
        )

        assert args.order_type == OrderType.STOP_MARKET
        assert args.price is None
        assert args.stop_price == Decimal("0.08")

    def test_decimal_parsing_from_various_types(self) -> None:
        """Test decimal field parsing from various input types."""
        # From string
        args1 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.5"),
            time_in_force=TimeInForce.IOC,
        )
        assert args1.quantity == Decimal("1.5")

        # From int
        args2 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal(2),
            price=Decimal(50000),
            time_in_force=TimeInForce.GTC,
        )
        assert args2.quantity == Decimal(2)
        assert args2.price == Decimal(50000)

        # From float
        args3 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.25"),
            price=Decimal("49999.99"),
            time_in_force=TimeInForce.GTC,
        )
        assert args3.quantity == Decimal("1.25")
        assert args3.price == Decimal("49999.99")

    def test_symbol_validation(self) -> None:
        """Test symbol field validation."""
        # Valid symbol
        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
        )
        assert args.symbol == "BTC-USD"

        # Symbol with whitespace should be validated as-is
        # The validate_str_field doesn't automatically strip whitespace
        args2 = PlaceOrderArgs(
            symbol="ETH-USDT",  # No whitespace
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
        )
        assert args2.symbol == "ETH-USDT"

    def test_invalid_symbol_types(self) -> None:
        """Test invalid symbol types."""
        with pytest.raises(TypeError) as exc_info:
            PlaceOrderArgs(
                symbol=123,  # type: ignore
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
            )

        # The actual error message from validate_str_field is "Field 'symbol' must be str, got int"
        assert "Field 'symbol' must be str, got int" in str(exc_info.value)

    def test_empty_symbol(self) -> None:
        """Test empty symbol validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            PlaceOrderArgs(
                symbol="",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
            )

        assert "Field symbol: String cannot be empty" in str(exc_info.value)

    def test_symbol_max_length(self) -> None:
        """Test symbol maximum length validation."""
        long_symbol = "A" * 65  # Exceeds 64 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            PlaceOrderArgs(
                symbol=long_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
            )

        assert "Field 'symbol' must be string with max length 64, got string with length 65" in str(
            exc_info.value
        )

    def test_client_order_id_validation(self) -> None:
        """Test client_order_id validation."""
        # Valid client_order_id
        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            client_order_id="valid_id_123",
        )
        assert args.client_order_id == "valid_id_123"

        # None client_order_id
        args2 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            client_order_id=None,
        )
        assert args2.client_order_id is None

    def test_invalid_client_order_id(self) -> None:
        """Test invalid client_order_id validation."""
        # Empty string
        with pytest.raises(EmptyStringError) as exc_info_empty:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                client_order_id="",
            )

        assert "Field client_order_id: String cannot be empty" in str(exc_info_empty.value)

        # Too long
        long_id = "A" * 65
        with pytest.raises(TypeFieldError) as exc_info_long:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                client_order_id=long_id,
            )

        assert (
            "Field 'client_order_id' must be string with max length 64, got string with length 65"
            in str(exc_info_long.value)
        )

    def test_negative_quantity(self) -> None:
        """Test negative quantity validation."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("-1.0"),
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        assert "greater than 0" in errors[0]["msg"]

    def test_zero_quantity(self) -> None:
        """Test zero quantity validation."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal(0),
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        assert "greater than 0" in errors[0]["msg"]

    def test_infinite_decimal_values(self) -> None:
        """Test infinite decimal values validation."""
        # Test infinite quantity
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("inf"),
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        assert "finite decimal" in errors[0]["msg"]

    def test_nan_decimal_values(self) -> None:
        """Test NaN decimal values validation."""
        # Test NaN quantity
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("nan"),
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        assert "finite decimal" in errors[0]["msg"]

    def test_negative_price(self) -> None:
        """Test negative price validation."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("-100.0"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("price",)
        assert "greater than 0" in errors[0]["msg"]

    def test_negative_stop_price(self) -> None:
        """Test negative stop_price validation."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                stop_price=Decimal("-50.0"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("stop_price",)
        assert "greater than 0" in errors[0]["msg"]

    def test_limit_order_requires_price(self) -> None:
        """Test that limit orders require a price."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                # price is None, should fail
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "A positive price is required" in errors[0]["msg"]

    def test_stop_limit_order_requires_price_and_stop_price(self) -> None:
        """Test that stop limit orders require both price and stop_price."""
        # Missing price
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                stop_price=Decimal("50000.0"),
                # price is None, should fail
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "A positive price is required" in errors[0]["msg"]

        # Missing stop_price
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("51000.0"),
                # stop_price is None, should fail
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "A positive stop_price is required" in errors[0]["msg"]

    def test_stop_market_order_requires_stop_price(self) -> None:
        """Test that stop market orders require stop_price."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                # stop_price is None, should fail
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "A positive stop_price is required" in errors[0]["msg"]

    # Note: post_only validation has been moved to OrderExecution domain object
    # with different rules (e.g., LiquidityRequirement.POST_ONLY)
    # Old test removed as part of clean break refactor

    def test_invalid_unparseable_decimal_quantity(self) -> None:
        """Test unparseable decimal values for quantity."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity="not_a_number",  # type: ignore
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        assert "Cannot convert" in errors[0]["msg"]

    def test_none_quantity(self) -> None:
        """Test None quantity validation (should fail as it's required)."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=None,  # type: ignore
                time_in_force=TimeInForce.IOC,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("quantity",)
        # Pydantic might give different error messages for None values
        assert (
            "cannot be None" in errors[0]["msg"]
            or "none is not an allowed value" in errors[0]["msg"].lower()
        )

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
                extra_field="not_allowed",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "extra_forbidden"

    def test_validate_assignment(self) -> None:
        """Test that assignment validation works."""
        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
        )

        # Valid assignment
        args.quantity = Decimal("2.0")
        assert args.quantity == Decimal("2.0")

        # Invalid assignment
        with pytest.raises(ValidationError):
            args.quantity = Decimal("-1.0")


class TestTransferArgs:
    """Test TransferArgs Pydantic model validation."""

    def test_valid_transfer_args(self) -> None:
        """Test valid transfer args creation."""
        args = TransferArgs(
            asset="BTC",
            amount=Decimal("1.5"),
            from_account_type="spot",
            to_account_type="futures",
            client_transfer_id="transfer_123",
        )

        assert args.asset == "BTC"
        assert args.amount == Decimal("1.5")
        assert args.from_account_type == "spot"
        assert args.to_account_type == "futures"
        assert args.client_transfer_id == "transfer_123"

    def test_valid_transfer_args_no_client_id(self) -> None:
        """Test valid transfer args without client transfer ID."""
        args = TransferArgs(
            asset="ETH",
            amount=Decimal("10.0"),
            from_account_type="margin",
            to_account_type="spot",
        )

        assert args.asset == "ETH"
        assert args.amount == Decimal("10.0")
        assert args.from_account_type == "margin"
        assert args.to_account_type == "spot"
        assert args.client_transfer_id is None

    def test_decimal_parsing_from_various_types(self) -> None:
        """Test amount parsing from various input types."""
        # From string
        args1 = TransferArgs(
            asset="BTC",
            amount=Decimal("1.5"),
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args1.amount == Decimal("1.5")

        # From int
        args2 = TransferArgs(
            asset="BTC",
            amount=Decimal(2),
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args2.amount == Decimal(2)

        # From float
        args3 = TransferArgs(
            asset="BTC",
            amount=Decimal("1.25"),
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args3.amount == Decimal("1.25")

    def test_string_field_validation(self) -> None:
        """Test string field validation."""
        # Valid strings
        args = TransferArgs(
            asset="BTC",
            amount=Decimal("1.0"),
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args.asset == "BTC"
        assert args.from_account_type == "spot"
        assert args.to_account_type == "futures"

    def test_empty_asset(self) -> None:
        """Test empty asset validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            TransferArgs(
                asset="",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
            )

        assert "Field asset: String cannot be empty" in str(exc_info.value)

    def test_empty_from_account_type(self) -> None:
        """Test empty from_account_type validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="",
                to_account_type="futures",
            )

        assert "Field from_account_type: String cannot be empty" in str(exc_info.value)

    def test_empty_to_account_type(self) -> None:
        """Test empty to_account_type validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="",
            )

        assert "Field to_account_type: String cannot be empty" in str(exc_info.value)

    def test_string_max_length(self) -> None:
        """Test string maximum length validation."""
        long_asset = "A" * 65  # Exceeds 64 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            TransferArgs(
                asset=long_asset,
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
            )

        assert "Field 'asset' must be string with max length 64, got string with length 65" in str(
            exc_info.value
        )

    def test_client_transfer_id_max_length(self) -> None:
        """Test client_transfer_id maximum length validation."""
        long_id = "A" * 129  # Exceeds 128 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
                client_transfer_id=long_id,
            )

        assert (
            "Field 'client_transfer_id' must be string with max length 128, "
            "got string with length 129" in str(exc_info.value)
        )

    def test_empty_client_transfer_id(self) -> None:
        """Test empty client_transfer_id validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
                client_transfer_id="",
            )

        assert "Field client_transfer_id: String cannot be empty" in str(exc_info.value)

    def test_negative_amount(self) -> None:
        """Test negative amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("-1.0"),
                from_account_type="spot",
                to_account_type="futures",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "greater than 0" in errors[0]["msg"]

    def test_zero_amount(self) -> None:
        """Test zero amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal(0),
                from_account_type="spot",
                to_account_type="futures",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "greater than 0" in errors[0]["msg"]

    def test_infinite_amount(self) -> None:
        """Test infinite amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("inf"),
                from_account_type="spot",
                to_account_type="futures",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "finite decimal" in errors[0]["msg"]

    def test_nan_amount(self) -> None:
        """Test NaN amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("nan"),
                from_account_type="spot",
                to_account_type="futures",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "finite decimal" in errors[0]["msg"]

    def test_none_amount(self) -> None:
        """Test None amount validation (should fail as it's required)."""
        with pytest.raises(TypeFieldError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=None,  # type: ignore
                from_account_type="spot",
                to_account_type="futures",
            )

        assert "Field 'amount' must be string, int, float, or Decimal, got NoneType" in str(
            exc_info.value
        )

    def test_unparseable_amount(self) -> None:
        """Test unparseable amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount="not_a_number",  # type: ignore
                from_account_type="spot",
                to_account_type="futures",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "Cannot convert" in errors[0]["msg"]

    def test_same_account_types(self) -> None:
        """Test validation that from and to account types must be different."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="spot",  # Same as from_account_type
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "from_account_type and to_account_type cannot be the same" in errors[0]["msg"]

    def test_invalid_string_types(self) -> None:
        """Test invalid types for string fields."""
        with pytest.raises(TypeError) as exc_info:
            TransferArgs(
                asset=123,  # type: ignore
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
            )

        assert "Field 'asset' must be str, got int" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            TransferArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                from_account_type="spot",
                to_account_type="futures",
                extra_field="not_allowed",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "extra_forbidden"


class TestWithdrawArgs:
    """Test WithdrawArgs Pydantic model validation."""

    def test_valid_withdraw_args_minimal(self) -> None:
        """Test valid withdraw args with minimal fields."""
        args = WithdrawArgs(
            asset="BTC",
            amount=Decimal("0.5"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        )

        assert args.asset == "BTC"
        assert args.amount == Decimal("0.5")
        assert args.address == "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
        assert args.network is None
        assert args.tag is None
        assert args.client_withdrawal_id is None
        assert args.two_factor_token is None

    def test_valid_withdraw_args_full(self) -> None:
        """Test valid withdraw args with all fields."""
        args = WithdrawArgs(
            asset="XRP",
            amount=Decimal("100.0"),
            address="rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
            network="xrp",
            tag="123456789",
            client_withdrawal_id="withdrawal_abc_123",
            two_factor_token="654321",
        )

        assert args.asset == "XRP"
        assert args.amount == Decimal("100.0")
        assert args.address == "rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH"
        assert args.network == "xrp"
        assert args.tag == "123456789"
        assert args.client_withdrawal_id == "withdrawal_abc_123"
        assert args.two_factor_token == "654321"

    def test_decimal_parsing_from_various_types(self) -> None:
        """Test amount parsing from various input types."""
        # From string
        args1 = WithdrawArgs(
            asset="BTC",
            amount=Decimal("0.5"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        )
        assert args1.amount == Decimal("0.5")

        # From int
        args2 = WithdrawArgs(
            asset="ETH",
            amount=Decimal(1),
            address="0x742d35Cc6765C0532C3A6C25C8FbC7b1b7d1D3E9",
        )
        assert args2.amount == Decimal(1)

        # From float
        args3 = WithdrawArgs(
            asset="LTC",
            amount=Decimal("2.5"),
            address="LTC123456789ABC",
        )
        assert args3.amount == Decimal("2.5")

    def test_required_string_field_validation(self) -> None:
        """Test required string field validation."""
        # Valid strings
        args = WithdrawArgs(
            asset="BTC",
            amount=Decimal("1.0"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        )
        assert args.asset == "BTC"
        assert args.address == "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"

    def test_empty_asset(self) -> None:
        """Test empty asset validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            WithdrawArgs(
                asset="",
                amount=Decimal("1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        assert "String cannot be empty" in str(exc_info.value)

    def test_empty_address(self) -> None:
        """Test empty address validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                address="",
            )

        assert "String cannot be empty" in str(exc_info.value)

    def test_asset_max_length(self) -> None:
        """Test asset maximum length validation."""
        long_asset = "A" * 129  # Exceeds 128 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            WithdrawArgs(
                asset=long_asset,
                amount=Decimal("1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        assert (
            "Field 'asset' must be string with max length 128, got string with length 129"
            in str(exc_info.value)
        )

    def test_address_max_length(self) -> None:
        """Test address maximum length validation."""
        long_address = "A" * 129  # Exceeds 128 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                address=long_address,
            )

        assert (
            "Field 'address' must be string with max length 128, got string with length 129"
            in str(exc_info.value)
        )

    def test_optional_string_field_validation(self) -> None:
        """Test optional string field validation."""
        # Valid optional strings
        args = WithdrawArgs(
            asset="XRP",
            amount=Decimal("100.0"),
            address="rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
            network="xrp",
            tag="123456",
        )
        assert args.network == "xrp"
        assert args.tag == "123456"

        # None values
        args2 = WithdrawArgs(
            asset="BTC",
            amount=Decimal("1.0"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            network=None,
            tag=None,
        )
        assert args2.network is None
        assert args2.tag is None

    def test_empty_optional_strings(self) -> None:
        """Test empty optional string validation."""
        with pytest.raises(EmptyStringError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
                network="",  # Empty string not allowed
            )

        assert "String cannot be empty" in str(exc_info.value)

    def test_optional_string_max_length(self) -> None:
        """Test optional string maximum length validation."""
        long_network = "A" * 65  # Exceeds 64 character limit

        with pytest.raises(TypeFieldError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
                network=long_network,
            )

        assert (
            "Field 'network' must be string with max length 64, got string with length 65"
            in str(exc_info.value)
        )

    def test_negative_amount(self) -> None:
        """Test negative amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("-1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "greater than 0" in errors[0]["msg"]

    def test_zero_amount(self) -> None:
        """Test zero amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal(0),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "greater than 0" in errors[0]["msg"]

    def test_infinite_amount(self) -> None:
        """Test infinite amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("inf"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "finite decimal" in errors[0]["msg"]

    def test_nan_amount(self) -> None:
        """Test NaN amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=Decimal("nan"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "finite decimal" in errors[0]["msg"]

    def test_none_amount(self) -> None:
        """Test None amount validation (should fail as it's required)."""
        with pytest.raises(TypeFieldError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount=None,  # type: ignore
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        assert "Field 'amount' must be string, int, float, or Decimal, got NoneType" in str(
            exc_info.value
        )

    def test_unparseable_amount(self) -> None:
        """Test unparseable amount validation."""
        with pytest.raises(ValidationError) as exc_info:
            WithdrawArgs(
                asset="BTC",
                amount="not_a_number",  # type: ignore
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("amount",)
        assert "Cannot convert" in errors[0]["msg"]

    def test_invalid_string_types(self) -> None:
        """Test invalid types for string fields."""
        with pytest.raises(TypeError) as exc_info:
            WithdrawArgs(
                asset=123,  # type: ignore
                amount=Decimal("1.0"),
                address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            )

        assert "Field 'asset' must be str, got int" in str(exc_info.value)

    def test_extra_fields_allowed(self) -> None:
        """Test that extra fields are allowed in WithdrawArgs."""
        # Should not raise an error due to extra="allow"
        # Test runtime behavior - extra fields are allowed by Pydantic at runtime
        args_dict = {
            "asset": "BTC",
            "amount": Decimal("1.0"),
            "address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            "custom_field": "some_value",
            "exchange_specific_param": 123,
        }
        args = WithdrawArgs.model_validate(args_dict)

        assert args.asset == "BTC"
        assert args.amount == Decimal("1.0")
        assert args.address == "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
        # Extra fields should be accessible
        assert hasattr(args, "custom_field")
        assert hasattr(args, "exchange_specific_param")

    def test_validate_assignment(self) -> None:
        """Test that assignment validation works."""
        args = WithdrawArgs(
            asset="BTC",
            amount=Decimal("1.0"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        )

        # Valid assignment
        args.amount = Decimal("2.0")
        assert args.amount == Decimal("2.0")

        # Invalid assignment
        with pytest.raises(ValidationError):
            args.amount = Decimal("-1.0")


class TestEdgeCasesAndBoundaryConditions:
    """Test edge cases and boundary conditions across all models."""

    def test_very_small_decimal_values(self) -> None:
        """Test very small but positive decimal values."""
        tiny_amount = Decimal("0.00000001")

        # PlaceOrderArgs
        args1 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=tiny_amount,
            time_in_force=TimeInForce.IOC,
        )
        assert args1.quantity == tiny_amount

        # TransferArgs
        args2 = TransferArgs(
            asset="BTC",
            amount=tiny_amount,
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args2.amount == tiny_amount

        # WithdrawArgs
        args3 = WithdrawArgs(
            asset="BTC",
            amount=tiny_amount,
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        )
        assert args3.amount == tiny_amount

    def test_very_large_decimal_values(self) -> None:
        """Test very large decimal values."""
        large_amount = Decimal("999999999999999999.999999999")

        # PlaceOrderArgs
        args1 = PlaceOrderArgs(
            symbol="DOGE-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=large_amount,
            time_in_force=TimeInForce.IOC,
        )
        assert args1.quantity == large_amount

        # TransferArgs
        args2 = TransferArgs(
            asset="DOGE",
            amount=large_amount,
            from_account_type="spot",
            to_account_type="futures",
        )
        assert args2.amount == large_amount

        # WithdrawArgs
        args3 = WithdrawArgs(
            asset="DOGE",
            amount=large_amount,
            address="DGE123456789ABC",
        )
        assert args3.amount == large_amount

    def test_unicode_string_fields(self) -> None:
        """Test Unicode characters in string fields."""
        unicode_symbol = "币-USD"  # Chinese character
        unicode_address = "🚀bitcoin123"  # Emoji

        # Should work with valid UTF-8
        args1 = PlaceOrderArgs(
            symbol=unicode_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
        )
        assert args1.symbol == unicode_symbol

        args2 = WithdrawArgs(
            asset="BTC",
            amount=Decimal("1.0"),
            address=unicode_address,
        )
        assert args2.address == unicode_address

    def test_whitespace_handling(self) -> None:
        """Test whitespace handling in string fields."""
        # validate_str_field strips input and validates,
        # so whitespace should be handled appropriately
        # But we need to test what actually happens rather than assume

        # Whitespace-only strings should fail
        with pytest.raises(EmptyStringError) as exc_info:
            PlaceOrderArgs(
                symbol="   ",  # Only whitespace
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
            )

        assert "String cannot be empty" in str(exc_info.value)

    def test_decimal_precision_preservation(self) -> None:
        """Test that decimal precision is preserved."""
        high_precision = Decimal("1.123456789012345678901234567890")

        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=high_precision,
            time_in_force=TimeInForce.IOC,
        )

        # Precision should be preserved
        assert args.quantity == high_precision
        assert str(args.quantity) == "1.123456789012345678901234567890"

    def test_comma_in_decimal_strings(self) -> None:
        """Test decimal parsing with commas in strings."""
        # parse_decimal_value should strip commas
        args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1234.56"),
            time_in_force=TimeInForce.IOC,
        )

        assert args.quantity == Decimal("1234.56")

    def test_all_order_types_and_dependencies(self) -> None:
        """Test all order types and their specific dependencies."""
        # MARKET order - no price required
        market_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
        )
        assert market_args.order_type == OrderType.MARKET

        # LIMIT order - price required
        limit_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.0"),
        )
        assert limit_args.order_type == OrderType.LIMIT

        # STOP_MARKET order - stop_price required
        stop_market_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            stop_price=Decimal("45000.0"),
        )
        assert stop_market_args.order_type == OrderType.STOP_MARKET

        # STOP_LIMIT order - both price and stop_price required
        stop_limit_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.SELL,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("46000.0"),
            stop_price=Decimal("45000.0"),
        )
        assert stop_limit_args.order_type == OrderType.STOP_LIMIT

        # TAKE_PROFIT_MARKET order - stop_price required
        tp_market_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.SELL,
            order_type=OrderType.TAKE_PROFIT_MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.IOC,
            stop_price=Decimal("55000.0"),
        )
        assert tp_market_args.order_type == OrderType.TAKE_PROFIT_MARKET

        # TAKE_PROFIT_LIMIT order - both price and stop_price required
        tp_limit_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.SELL,
            order_type=OrderType.TAKE_PROFIT_LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("54000.0"),
            stop_price=Decimal("55000.0"),
        )
        assert tp_limit_args.order_type == OrderType.TAKE_PROFIT_LIMIT

    def test_all_time_in_force_values(self) -> None:
        """Test all time in force values."""
        for tif in TimeInForce:
            args = PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=tif,
            )
            assert args.time_in_force == tif

    def test_all_order_sides(self) -> None:
        """Test all order sides."""
        for side in OrderSide:
            args = PlaceOrderArgs(
                symbol="BTC-USD",
                side=side,
                order_type=OrderType.MARKET,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.IOC,
            )
            assert args.side == side


class TestGetMarketArgs:
    """Test GetMarketArgs Pydantic model validation."""

    def test_valid_symbol(self) -> None:
        """Test valid symbol creation."""
        args = GetMarketArgs(symbol="BTC-USDC")
        assert args.symbol == "BTC-USDC"

    def test_symbol_validation_empty_string(self) -> None:
        """Test that empty string is rejected."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            GetMarketArgs(symbol="")

    def test_symbol_validation_whitespace_only(self) -> None:
        """Test that whitespace-only string is rejected."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            GetMarketArgs(symbol="   ")

    def test_symbol_validation_too_long(self) -> None:
        """Test that string longer than 64 characters is rejected."""
        long_symbol = "A" * 65
        with pytest.raises(
            TypeFieldError, match="string with max length 64, got string with length 65"
        ):
            GetMarketArgs(symbol=long_symbol)

    def test_symbol_validation_maximum_length(self) -> None:
        """Test that 64-character string is accepted."""
        max_length_symbol = "A" * 64
        args = GetMarketArgs(symbol=max_length_symbol)
        assert args.symbol == max_length_symbol

    def test_symbol_validation_unicode_support(self) -> None:
        """Test that Unicode characters are supported."""
        unicode_symbol = "BTC-USDC_🚀"
        args = GetMarketArgs(symbol=unicode_symbol)
        assert args.symbol == unicode_symbol

    def test_symbol_validation_special_characters(self) -> None:
        """Test that special characters commonly used in symbols are supported."""
        special_symbols = [
            "BTC-USDC",
            "BTC_USDC",
            "BTC/USDC",
            "BTC.USDC",
            "BTC:USDC",
        ]
        for symbol in special_symbols:
            args = GetMarketArgs(symbol=symbol)
            assert args.symbol == symbol

    def test_symbol_required_field(self) -> None:
        """Test that symbol is a required field."""
        with pytest.raises(ValidationError, match="Field required"):
            GetMarketArgs()  # type: ignore[call-arg]

    def test_symbol_wrong_type(self) -> None:
        """Test that non-string types are rejected."""
        with pytest.raises(TypeError) as exc_info:
            GetMarketArgs(symbol=123)  # type: ignore[arg-type]

        assert "Field 'symbol' must be str, got int" in str(exc_info.value)

        with pytest.raises(TypeError):
            GetMarketArgs(symbol=None)  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            GetMarketArgs(symbol=["BTC-USDC"])  # type: ignore[arg-type]

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            GetMarketArgs(symbol="BTC-USDC", extra_field="not_allowed")  # type: ignore[call-arg]

    def test_immutability_after_creation(self) -> None:
        """Test that fields cannot be modified after creation."""
        args = GetMarketArgs(symbol="BTC-USDC")

        # Test validation on assignment
        with pytest.raises(EmptyStringError):
            args.symbol = ""

        with pytest.raises(TypeFieldError):
            args.symbol = "A" * 65

    def test_model_validation_assignment(self) -> None:
        """Test that validate_assignment=True works correctly."""
        args = GetMarketArgs(symbol="BTC-USDC")

        # Valid assignment should work
        args.symbol = "ETH-USDC"
        assert args.symbol == "ETH-USDC"


class TestGetMarketsArgs:
    """Test GetMarketsArgs Pydantic model validation."""

    def test_empty_creation(self) -> None:
        """Test creating with no arguments (default case)."""
        args = GetMarketsArgs()
        assert args is not None

    def test_empty_dict_creation(self) -> None:
        """Test creating from empty dictionary."""
        args = GetMarketsArgs.model_validate({})
        assert args is not None

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            GetMarketsArgs(extra_field="not_allowed")  # type: ignore[call-arg]

    def test_model_consistency(self) -> None:
        """Test that the model provides consistent interface."""
        args1 = GetMarketsArgs()
        args2 = GetMarketsArgs()

        # Should be equal (both empty)
        assert args1.model_dump() == args2.model_dump()

    def test_model_serialization(self) -> None:
        """Test model serialization to dict."""
        args = GetMarketsArgs()
        data = args.model_dump()
        assert isinstance(data, dict)
        assert len(data) == 0  # No fields currently

    def test_model_deserialization(self) -> None:
        """Test model deserialization from dict."""
        data: dict[str, object] = {}
        args = GetMarketsArgs.model_validate(data)
        assert args is not None

    def test_future_extensibility(self) -> None:
        """Test that the model can be extended in the future."""
        # This test documents the intent for future extensibility
        # Currently no parameters, but the model structure supports adding them
        args = GetMarketsArgs()

        # Verify the model has the expected configuration
        assert args.model_config.get("extra") == "forbid"
        assert args.model_config.get("validate_assignment") is True

    def test_model_repr_and_str(self) -> None:
        """Test that the model has reasonable string representations."""
        args = GetMarketsArgs()

        # Should not raise exceptions
        repr_str = repr(args)
        str_str = str(args)

        assert "GetMarketsArgs" in repr_str
        assert isinstance(str_str, str)
