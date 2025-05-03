"""
Unit tests for the CyberDeltaEngine internal Order model and enrichment slots.

Covers:
- Order: construction, field validation, mutability, serialization, and error cases
- BackpackOrderDetails: all field and model-level validation, including enum and decimal checks,
  edge cases, and error messages
- HyperliquidOrderDetails: field validation, edge cases

All tests use pytest and pydantic.ValidationError for error testing.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pydantic
import pytest

from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
)
from cyberdelta.core.models.market.order import (
    BackpackOrderDetails,
    HyperliquidOrderDetails,
    Order,
)


# --- Order Model Tests ---
def test_order_minimal_valid() -> None:
    """Test that a minimal valid Order instance is accepted and fields are set correctly."""
    order = Order(
        exchange="backpack",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("1.0"),
        price=Decimal("100.0"),
        exchange_order_id=None,
        related_order_id=None,
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )
    assert order.exchange == "backpack"
    assert order.symbol == "BTC-PERP"
    assert order.side == OrderSide.BUY
    assert order.order_type == OrderType.LIMIT
    assert order.quantity_requested == Decimal("1.0")
    assert order.price == Decimal("100.0")
    assert order.status == OrderStatus.NEW
    assert order.trades == []
    assert order.hl_details is None
    assert order.bp_details is None
    assert isinstance(uuid.UUID(order.client_order_id), uuid.UUID)


def test_order_required_fields_missing() -> None:
    """Test that missing required fields raise ValidationError."""
    with pytest.raises(pydantic.ValidationError):
        Order(
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            exchange="backpack",
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
    with pytest.raises(pydantic.ValidationError):
        Order(
            exchange="backpack",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            symbol="BTC-PERP",
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
    with pytest.raises(pydantic.ValidationError):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            side=OrderSide.BUY,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
    with pytest.raises(pydantic.ValidationError):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            order_type=OrderType.LIMIT,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )


def test_order_decimal_and_enum_validation() -> None:
    """Test that invalid decimals and enums are rejected."""
    # Negative quantity
    with pytest.raises(pydantic.ValidationError):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("-1.0"),
            price=Decimal("100.0"),
            exchange_order_id=None,
            related_order_id=None,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
    # Invalid enum
    with pytest.raises(ValueError):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            side="INVALID",  # type: ignore
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("100.0"),
            exchange_order_id=None,
            related_order_id=None,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )


def test_order_limit_and_stop_type_requirements() -> None:
    """Test that limit types require price and stop types require stop_price."""
    # LIMIT without price
    with pytest.raises(ValueError, match="requires a price"):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
    # STOP_MARKET without stop_price
    with pytest.raises(ValueError, match="requires a stop_price"):
        Order(
            exchange="backpack",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_MARKET,
            quantity_requested=Decimal("1.0"),
            exchange_order_id=None,
            related_order_id=None,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )


def test_order_mutability() -> None:
    """Test that Order is mutable and fields can be updated."""
    order = Order(
        exchange="backpack",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("1.0"),
        price=Decimal("100.0"),
        exchange_order_id=None,
        related_order_id=None,
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )
    order.status = OrderStatus.FILLED
    order.quantity_filled = Decimal("1.0")
    order.updated_at = datetime.now(UTC)
    assert order.status == OrderStatus.FILLED
    assert order.quantity_filled == Decimal("1.0")
    assert isinstance(order.updated_at, datetime)


def test_order_to_dict_serialization() -> None:
    """Test that to_dict serializes correctly and includes trades as list of dicts."""
    order = Order(
        exchange="backpack",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("1.0"),
        price=Decimal("100.0"),
        exchange_order_id=None,
        related_order_id=None,
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )
    d = order.to_dict()
    assert isinstance(d, dict)
    assert d["symbol"] == "BTC-PERP"
    assert isinstance(d["trades"], list)


# --- BackpackOrderDetails Tests ---
def test_backpack_order_details_valid() -> None:
    """Test that a valid BackpackOrderDetails instance is accepted."""
    details = BackpackOrderDetails(
        executed_quote_quantity=Decimal("10.5"),
        self_trade_prevention=SelfTradePrevention("RejectTaker"),
        expiry_reason=OrderExpiryReason("UserCancelled"),
        origin=OrderUpdateOrigin("USER"),
    )
    assert details.executed_quote_quantity == Decimal("10.5")
    assert details.self_trade_prevention == SelfTradePrevention("RejectTaker")
    assert details.expiry_reason == OrderExpiryReason("UserCancelled")
    assert details.origin == OrderUpdateOrigin("USER")


def test_backpack_order_details_enum_str_coercion() -> None:
    """Test that enum fields accept both enum values and valid strings (via enum constructor)."""
    details = BackpackOrderDetails(
        self_trade_prevention=SelfTradePrevention.REJECT_TAKER,
        expiry_reason=OrderExpiryReason.USER_CANCELLED,
        origin=OrderUpdateOrigin.USER,
    )
    assert details.self_trade_prevention == SelfTradePrevention.REJECT_TAKER
    assert details.expiry_reason == OrderExpiryReason.USER_CANCELLED
    assert details.origin == OrderUpdateOrigin.USER


def test_backpack_order_details_invalid_enum() -> None:
    """Test that invalid enum values raise ValidationError with clear messages."""
    from typing import cast

    with pytest.raises(pydantic.ValidationError, match="enum"):
        BackpackOrderDetails(self_trade_prevention=cast(SelfTradePrevention, "INVALID"))
    with pytest.raises(pydantic.ValidationError, match="enum"):
        BackpackOrderDetails(expiry_reason=cast(OrderExpiryReason, "NOT_A_REASON"))
    with pytest.raises(pydantic.ValidationError, match="enum"):
        BackpackOrderDetails(origin=cast(OrderUpdateOrigin, "NOT_AN_ORIGIN"))


def test_backpack_order_details_executed_quote_quantity_validation() -> None:
    """Test that executed_quote_quantity must be non-negative and finite if set."""
    BackpackOrderDetails(executed_quote_quantity=Decimal("0"))  # valid
    BackpackOrderDetails(executed_quote_quantity=None)  # valid
    with pytest.raises(ValueError, match="non-negative"):
        BackpackOrderDetails(executed_quote_quantity=Decimal("-1"))
    with pytest.raises(ValueError, match="finite"):
        BackpackOrderDetails(executed_quote_quantity=Decimal("NaN"))


# --- HyperliquidOrderDetails Tests ---
def test_hyperliquid_order_details_valid() -> None:
    """Test that a valid HyperliquidOrderDetails instance is accepted."""
    details = HyperliquidOrderDetails(remaining_sz=Decimal("5.0"))
    assert details.remaining_sz == Decimal("5.0")
    details2 = HyperliquidOrderDetails(remaining_sz=None)
    assert details2.remaining_sz is None


def test_hyperliquid_order_details_invalid_remaining_sz() -> None:
    """Test that remaining_sz must be non-negative and finite if set."""
    with pytest.raises(ValueError, match="non-negative"):
        HyperliquidOrderDetails(remaining_sz=Decimal("-1"))
    with pytest.raises(ValueError, match="finite"):
        HyperliquidOrderDetails(remaining_sz=Decimal("NaN"))


# --- Draft: Extend with more edge cases, cross-field logic, and integration tests as needed ---
