"""Property-based tests for the core Order model using Hypothesis.

This module provides comprehensive property-based testing of the Order Pydantic model,
which serves as the unified internal representation for trading orders across all
supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Complex cross-field validation logic with generated combinations
- Order type-specific validation (LIMIT, MARKET, STOP_MARKET, STOP_LIMIT)
- Decimal precision handling for financial calculations (comprehensive value ranges)
- Exchange-specific detail model integration with validation
- Order lifecycle state management and validation
- Mutability and assignment validation
- Business rule enforcement across all possible combinations

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete order data flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.core.enums import (
    OrderExpiryReason,
    OrderStatus,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TriggerType,
)
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.order import (
    BackpackOrderDetails,
    HyperliquidOrderDetails,
    Order,
)
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    BTC_BP,
    BTC_HL,
    BTC_USDC_BP,
    DOGE_HL,
    ETH_BP,
    ETH_HL,
    ETH_USDC_BP,
    SOL_BP,
    SOL_HL,
    SOL_USDC_BP,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER DATA
# =============================================================================


@st.composite
def financial_decimal_strategy(
    draw: st.DrawFn,
    min_value: float = 0.000001,
    max_value: float = 1000000.0,
    allow_zero: bool = True,
    allow_negative: bool = False,
) -> Decimal:
    """Generate realistic Decimal values for financial calculations.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value (exclusive unless allow_zero)
        max_value: Maximum value (inclusive)
        allow_zero: Whether to allow zero values
        allow_negative: Whether to allow negative values

    Returns:
        Decimal: A valid decimal for financial calculations
    """
    if allow_zero and draw(st.booleans()):
        return Decimal(0)

    if allow_negative and draw(st.booleans()):
        # Generate negative values
        negative_value = draw(
            st.floats(
                min_value=-max_value,
                max_value=-min_value,
                allow_infinity=False,
                allow_nan=False,
                exclude_max=True,
            )
        )
        return Decimal(str(negative_value))

    # Generate positive values
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
            exclude_min=True,
        )
    )
    return Decimal(str(value))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values for order data.
    
    Returns:
        Decimal price value for order testing.
    """
    return draw(financial_decimal_strategy(min_value=0.01, max_value=100000.0, allow_zero=False))


@st.composite
def quantity_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic quantity values for order data.
    
    Returns:
        Decimal quantity value for order testing.
    """
    return draw(financial_decimal_strategy(min_value=0.000001, max_value=10000.0, allow_zero=False))


@st.composite
def filled_quantity_strategy(draw: st.DrawFn, max_quantity: Decimal) -> Decimal:
    """Generate realistic filled quantity values (between 0 and max_quantity).

    Args:
        draw: Hypothesis draw function
        max_quantity: Maximum allowed quantity (quantity_requested)

    Returns:
        Decimal: A valid filled quantity
    """
    return draw(
        financial_decimal_strategy(
            min_value=0.0, max_value=float(max_quantity), allow_zero=True, allow_negative=False
        )
    )


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for order testing.
    
    Returns:
        Valid Symbol object for testing.
    """
    return draw(
        st.sampled_from([
            BTC_HL,
            ETH_HL,
            SOL_HL,
            DOGE_HL,
            BTC_BP,
            ETH_BP,
            SOL_BP,
            BTC_USDC_BP,
            ETH_USDC_BP,
            SOL_USDC_BP,
        ])
    )


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for order data.
    
    Returns:
        UTC datetime object for order testing.
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def valid_id_strategy(draw: st.DrawFn) -> str:
    """Generate valid ID strings for order testing.
    
    Returns:
        Valid ID string for testing.
    """
    return draw(
        st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=128,
        ).filter(lambda x: x.strip())
    )


@st.composite
def hyperliquid_order_details_strategy(draw: st.DrawFn) -> HyperliquidOrderDetails:
    """Generate valid HyperliquidOrderDetails for testing.
    
    Returns:
        Valid HyperliquidOrderDetails object for testing.
    """
    remaining_sz = draw(
        st.one_of(
            st.none(), financial_decimal_strategy(min_value=0.0, max_value=10000.0, allow_zero=True)
        )
    )

    return HyperliquidOrderDetails(remaining_sz=remaining_sz)


@st.composite
def backpack_order_details_strategy(draw: st.DrawFn) -> BackpackOrderDetails:
    """Generate valid BackpackOrderDetails for testing.
    
    Returns:
        Valid BackpackOrderDetails object for testing.
    """
    executed_quote_quantity = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=100000.0, allow_zero=True),
        )
    )

    self_trade_prevention = draw(
        st.one_of(
            st.none(),
            st.sampled_from([SelfTradePrevention.REJECT_TAKER, SelfTradePrevention.REJECT_MAKER]),
        )
    )

    expiry_reason = draw(
        st.one_of(
            st.none(),
            st.sampled_from([OrderExpiryReason.USER_CANCELLED, OrderExpiryReason.FILL_OR_KILL]),
        )
    )

    origin = draw(
        st.one_of(
            st.none(),
            st.sampled_from([OrderUpdateOrigin.USER, OrderUpdateOrigin.LIQUIDATION_AUTOCLOSE]),
        )
    )

    # Optional price fields
    sl_trigger_price = draw(st.one_of(st.none(), price_strategy()))
    sl_limit_price = draw(st.one_of(st.none(), price_strategy()))
    tp_trigger_price = draw(st.one_of(st.none(), price_strategy()))
    tp_limit_price = draw(st.one_of(st.none(), price_strategy()))

    sl_trigger_by = draw(
        st.one_of(st.none(), st.sampled_from([TriggerType.MARK_PRICE, TriggerType.LAST_PRICE]))
    )
    tp_trigger_by = draw(
        st.one_of(st.none(), st.sampled_from([TriggerType.MARK_PRICE, TriggerType.LAST_PRICE]))
    )

    trigger_quantity = draw(st.one_of(st.none(), quantity_strategy()))

    return BackpackOrderDetails(
        executed_quote_quantity=executed_quote_quantity,
        self_trade_prevention=self_trade_prevention,
        expiry_reason=expiry_reason,
        origin=origin,
        sl_trigger_price=sl_trigger_price,
        sl_limit_price=sl_limit_price,
        sl_trigger_by=sl_trigger_by,
        tp_trigger_price=tp_trigger_price,
        tp_limit_price=tp_limit_price,
        tp_trigger_by=tp_trigger_by,
        trigger_quantity=trigger_quantity,
    )


@st.composite
def valid_order_type_with_price_strategy(
    draw: st.DrawFn,
) -> tuple[OrderType, Decimal | None, Decimal | None]:
    """Generate valid order type with appropriate price combinations.

    Returns:
        tuple: (order_type, price, stop_price) that satisfy business rules
    """
    order_type = draw(
        st.sampled_from([
            OrderType.LIMIT,
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
        ])
    )

    if order_type == OrderType.LIMIT:
        price = draw(price_strategy())
        stop_price = None
    elif order_type == OrderType.MARKET:
        price = None
        stop_price = None
    elif order_type == OrderType.STOP_MARKET:
        price = None
        stop_price = draw(price_strategy())
    else:  # STOP_LIMIT
        price = draw(price_strategy())
        stop_price = draw(price_strategy())

    return order_type, price, stop_price


# =============================================================================
# PROPERTY TESTS FOR ORDER MODEL
# =============================================================================


class TestOrderModelProperties:
    """Property-based tests for the Order model."""

    @given(
        order_symbol=valid_symbol_strategy(),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
        order_type_price=valid_order_type_with_price_strategy(),
        quantity_requested=quantity_strategy(),
        time_in_force=st.sampled_from([TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        created_at=valid_timestamp_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_order_creation_properties(
        self,
        order_symbol: Symbol,
        side: OrderSide,
        order_type_price: tuple[OrderType, Decimal | None, Decimal | None],
        quantity_requested: Decimal,
        time_in_force: TimeInForce,
        exchange: ExchangeName,
        created_at: datetime,
    ) -> None:
        """Property: Minimal order with only required fields should always be valid."""
        order_type, price, stop_price = order_type_price

        order = Order(
            symbol=order_symbol,
            side=side,
            order_type=order_type,
            quantity_requested=quantity_requested,
            price=price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            exchange=exchange,
            created_at=created_at,
            updated_at=created_at,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Properties: Required fields should be set correctly
        assert order.symbol == order_symbol
        assert order.side == side
        assert order.order_type == order_type
        assert order.quantity_requested == quantity_requested
        assert order.price == price
        assert order.stop_price == stop_price
        assert order.time_in_force == time_in_force
        assert order.exchange == exchange
        assert order.created_at == created_at

        # Properties: Optional fields should have correct defaults
        assert order.status == OrderStatus.NEW
        assert order.quantity_filled == Decimal(0)
        assert order.reduce_only is False
        assert order.post_only is False
        assert order.trades == []
        assert order.hl_details is None
        assert order.bp_details is None
        assert order.exchange_order_id is None
        assert order.related_order_id is None
        assert order.quote_quantity_requested is None
        assert order.average_fill_price is None
        assert order.updated_at is None
        assert order.triggered_at is None
        assert order.strategy_name is None
        assert order.signal_id is None

        # Properties: Client order ID should be valid UUID
        assert isinstance(uuid.UUID(order.client_order_id), uuid.UUID)

    @given(
        order_symbol=valid_symbol_strategy(),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
        order_type_price=valid_order_type_with_price_strategy(),
        quantity_requested=quantity_strategy(),
        time_in_force=st.sampled_from([TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        created_at=valid_timestamp_strategy(),
        data=st.data(),
    )
    @settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_full_order_creation_properties(
        self,
        order_symbol: Symbol,
        side: OrderSide,
        order_type_price: tuple[OrderType, Decimal | None, Decimal | None],
        quantity_requested: Decimal,
        time_in_force: TimeInForce,
        exchange: ExchangeName,
        created_at: datetime,
        data: st.DataObject,
    ) -> None:
        """Property: Full order with all fields should maintain data integrity."""
        order_type, price, stop_price = order_type_price

        # Generate optional fields
        exchange_order_id = data.draw(st.one_of(st.none(), valid_id_strategy()))
        related_order_id = data.draw(st.one_of(st.none(), valid_id_strategy()))
        status = data.draw(
            st.sampled_from([
                OrderStatus.NEW,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
            ])
        )

        # Generate quantity_filled that respects business rules
        quantity_filled = data.draw(filled_quantity_strategy(quantity_requested))

        # Generate average_fill_price based on quantity_filled
        average_fill_price = data.draw(price_strategy()) if quantity_filled > 0 else None

        quote_quantity_requested = data.draw(
            st.one_of(
                st.none(),
                financial_decimal_strategy(min_value=1.0, max_value=1000000.0, allow_zero=False),
            )
        )

        reduce_only = data.draw(st.booleans())
        post_only = data.draw(st.booleans())

        updated_at = data.draw(st.one_of(st.none(), valid_timestamp_strategy()))
        triggered_at = data.draw(st.one_of(st.none(), valid_timestamp_strategy()))

        strategy_name = data.draw(
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
                    ),
                    min_size=1,
                    max_size=50,
                ).filter(lambda x: x.strip() and x == x.strip()),
            )
        )
        signal_id = data.draw(st.one_of(st.none(), valid_id_strategy()))

        # Generate trigger_by that's compatible with order_type
        if order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]:
            trigger_by = data.draw(
                st.sampled_from([TriggerType.MARK_PRICE, TriggerType.LAST_PRICE])
            )
        else:
            trigger_by = None

        # Generate exchange-specific details based on exchange
        if exchange == ExchangeName.HYPERLIQUID:
            hl_details = data.draw(st.one_of(st.none(), hyperliquid_order_details_strategy()))
            bp_details = None
        else:
            hl_details = None
            bp_details = data.draw(st.one_of(st.none(), backpack_order_details_strategy()))

        order = Order(
            symbol=order_symbol,
            side=side,
            order_type=order_type,
            quantity_requested=quantity_requested,
            price=price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            exchange=exchange,
            created_at=created_at,
            exchange_order_id=exchange_order_id,
            related_order_id=related_order_id,
            status=status,
            quantity_filled=quantity_filled,
            quote_quantity_requested=quote_quantity_requested,
            average_fill_price=average_fill_price,
            trigger_by=trigger_by,
            reduce_only=reduce_only,
            post_only=post_only,
            updated_at=updated_at,
            triggered_at=triggered_at,
            strategy_name=strategy_name,
            signal_id=signal_id,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: All fields should be preserved exactly
        assert order.symbol == order_symbol
        assert order.side == side
        assert order.order_type == order_type
        assert order.quantity_requested == quantity_requested
        assert order.price == price
        assert order.stop_price == stop_price
        assert order.time_in_force == time_in_force
        assert order.exchange == exchange
        assert order.created_at == created_at
        assert order.exchange_order_id == exchange_order_id
        assert order.related_order_id == related_order_id
        assert order.status == status
        assert order.quantity_filled == quantity_filled
        assert order.quote_quantity_requested == quote_quantity_requested
        assert order.average_fill_price == average_fill_price
        assert order.trigger_by == trigger_by
        assert order.reduce_only == reduce_only
        assert order.post_only == post_only
        assert order.updated_at == updated_at
        assert order.triggered_at == triggered_at
        assert order.strategy_name == strategy_name
        assert order.signal_id == signal_id
        assert order.hl_details == hl_details
        assert order.bp_details == bp_details

    @given(
        order_type=st.sampled_from([
            OrderType.LIMIT,
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
        ]),
        price=st.one_of(st.none(), price_strategy()),
        stop_price=st.one_of(st.none(), price_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_order_type_price_validation_properties(
        self, order_type: OrderType, price: Decimal | None, stop_price: Decimal | None
    ) -> None:
        """Property: Order type and price combinations should follow business rules."""
        base_order_data: dict[str, Any] = {
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "order_type": order_type,
            "quantity_requested": Decimal("1.0"),
            "price": price,
            "stop_price": stop_price,
            "time_in_force": TimeInForce.GTC,
            "exchange": ExchangeName.HYPERLIQUID,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
        }

        # Property: Business rule validation for order types
        if order_type == OrderType.LIMIT and (price is None or price <= 0):
            with pytest.raises(ValidationError, match="Order type LIMIT requires a positive price"):
                Order(**base_order_data)
        elif order_type == OrderType.STOP_MARKET and (stop_price is None or stop_price <= 0):
            with pytest.raises(
                ValidationError, match="Order type STOP_MARKET requires a positive stop_price"
            ):
                Order(**base_order_data)
        elif order_type == OrderType.STOP_LIMIT and (
            price is None or price <= 0 or stop_price is None or stop_price <= 0
        ):
            # Should fail if either price or stop_price is missing/invalid
            with pytest.raises(ValidationError):
                Order(**base_order_data)
        else:
            # Should be valid for all other combinations
            order = Order(**base_order_data)
            assert order.order_type == order_type
            assert order.price == price
            assert order.stop_price == stop_price

    @given(
        quantity_requested=quantity_strategy(),
        quantity_filled=st.data(),
        average_fill_price=st.one_of(st.none(), price_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_quantity_fill_validation_properties(
        self,
        quantity_requested: Decimal,
        quantity_filled: st.DataObject,
        average_fill_price: Decimal | None,
    ) -> None:
        """Property: Quantity filled validation should enforce business rules."""
        # Generate quantity_filled that may or may not be valid
        filled = quantity_filled.draw(
            financial_decimal_strategy(
                min_value=0.0,
                max_value=float(quantity_requested) * 2,
                allow_zero=True,  # Allow over-fill for testing
            )
        )

        base_order_data: dict[str, Any] = {
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "order_type": OrderType.LIMIT,
            "quantity_requested": quantity_requested,
            "price": Decimal("50000.0"),
            "time_in_force": TimeInForce.GTC,
            "exchange": ExchangeName.HYPERLIQUID,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "quantity_filled": filled,
            "average_fill_price": average_fill_price,
        }

        # Property: Validate business rules
        # Test quantity_filled > quantity_requested case first
        if filled > quantity_requested:
            # For this test, set a valid average_fill_price to avoid the other validation
            base_order_data["average_fill_price"] = Decimal("50000.0")
            with pytest.raises(
                ValidationError, match=r"quantity_filled .* cannot exceed quantity_requested"
            ):
                Order(**base_order_data)
        # Test average_fill_price validation when quantity_filled > 0
        elif filled > 0 and (average_fill_price is None or average_fill_price <= 0):
            with pytest.raises(
                ValidationError, match="average_fill_price must be positive if quantity_filled > 0"
            ):
                Order(**base_order_data)
        else:
            # Should be valid
            order = Order(**base_order_data)
            assert order.quantity_filled == filled
            assert order.average_fill_price == average_fill_price

    @given(
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        hl_details=st.one_of(st.none(), hyperliquid_order_details_strategy()),
        bp_details=st.one_of(st.none(), backpack_order_details_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_exchange_details_validation_properties(
        self,
        exchange: ExchangeName,
        hl_details: HyperliquidOrderDetails | None,
        bp_details: BackpackOrderDetails | None,
    ) -> None:
        """Property: Exchange-specific details should only be valid for correct exchange."""
        base_order_data: dict[str, Any] = {
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "order_type": OrderType.LIMIT,
            "quantity_requested": Decimal("1.0"),
            "price": Decimal("50000.0"),
            "time_in_force": TimeInForce.GTC,
            "exchange": exchange,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "hl_details": hl_details,
            "bp_details": bp_details,
        }

        # Property: Exchange details validation
        if exchange == ExchangeName.HYPERLIQUID and bp_details is not None:
            with pytest.raises(
                ValidationError, match=r"Backpack details .* must be None for a Hyperliquid order"
            ):
                Order(**base_order_data)
        elif exchange == ExchangeName.BACKPACK and hl_details is not None:
            with pytest.raises(
                ValidationError, match=r"Hyperliquid details .* must be None for a Backpack order"
            ):
                Order(**base_order_data)
        else:
            # Should be valid
            order = Order(**base_order_data)
            assert order.exchange == exchange
            assert order.hl_details == hl_details
            assert order.bp_details == bp_details

    @given(
        field_name=st.sampled_from([
            "price",
            "stop_price",
            "quantity_requested",
            "quote_quantity_requested",
            "average_fill_price",
        ]),
        invalid_value=st.one_of(
            st.just(Decimal(0)),
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_financial_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Financial fields should reject invalid values."""
        base_order_data: dict[str, Any] = {
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "order_type": OrderType.LIMIT,
            "quantity_requested": Decimal("1.0"),
            "price": Decimal("50000.0"),
            "time_in_force": TimeInForce.GTC,
            "exchange": ExchangeName.HYPERLIQUID,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
        }

        # Special handling for certain fields
        if field_name == "average_fill_price":
            base_order_data["quantity_filled"] = Decimal(
                "0.5"
            )  # Required for average_fill_price validation

        kwargs: dict[str, Any] = base_order_data.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            Order(**kwargs)

    @given(
        quantity_requested=quantity_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_order_mutability_properties(self, quantity_requested: Decimal) -> None:
        """Property: Order instances should be mutable with validation on assignment."""
        order = Order(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=quantity_requested,
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Valid mutations should work
        order.status = OrderStatus.PARTIALLY_FILLED
        order.average_fill_price = Decimal("50001.0")  # Set this before quantity_filled
        order.quantity_filled = quantity_requested / 2
        order.updated_at = datetime.now(UTC)

        assert order.status == OrderStatus.PARTIALLY_FILLED
        assert order.quantity_filled == quantity_requested / 2
        assert order.average_fill_price == Decimal("50001.0")
        assert order.updated_at is not None

        # Property: Invalid mutations should be rejected
        with pytest.raises(ValidationError):
            order.quantity_filled = quantity_requested + Decimal("0.01")  # Exceeds requested

        with pytest.raises(ValidationError):
            order.price = Decimal("-100.0")  # Negative price

    @given(
        hl_details=hyperliquid_order_details_strategy(),
        bp_details=backpack_order_details_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_exchange_details_immutability_properties(
        self, hl_details: HyperliquidOrderDetails, bp_details: BackpackOrderDetails
    ) -> None:
        """Property: Exchange-specific details should be immutable."""
        order_hl = Order(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            hl_details=hl_details,
        )

        order_bp = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=bp_details,
        )

        # Properties: Details should be preserved
        assert order_hl.hl_details == hl_details
        assert order_bp.bp_details == bp_details

        # Properties: Details should be immutable
        if order_hl.hl_details is not None and order_hl.hl_details.remaining_sz is not None:
            with pytest.raises(ValidationError, match="Instance is frozen"):
                order_hl.hl_details.remaining_sz = Decimal(999)

        if (
            order_bp.bp_details is not None
            and order_bp.bp_details.executed_quote_quantity is not None
        ):
            with pytest.raises(ValidationError, match="Instance is frozen"):
                order_bp.bp_details.executed_quote_quantity = Decimal(999999)


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC DETAIL MODELS
# =============================================================================


class TestHyperliquidOrderDetailsProperties:
    """Property-based tests for HyperliquidOrderDetails model."""

    @given(
        remaining_sz=st.one_of(
            st.none(), financial_decimal_strategy(min_value=0.0, max_value=10000.0, allow_zero=True)
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_hyperliquid_details_creation_properties(self, remaining_sz: Decimal | None) -> None:
        """Property: HyperliquidOrderDetails should handle all field combinations correctly."""
        details = HyperliquidOrderDetails(remaining_sz=remaining_sz)

        # Property: Field should be preserved
        assert details.remaining_sz == remaining_sz

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.remaining_sz = Decimal(999)

    @given(
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_hyperliquid_details_validation_properties(self, invalid_value: Decimal) -> None:
        """Property: HyperliquidOrderDetails should validate field values correctly."""
        with pytest.raises(ValidationError):
            HyperliquidOrderDetails(remaining_sz=invalid_value)


class TestBackpackOrderDetailsProperties:
    """Property-based tests for BackpackOrderDetails model."""

    @given(
        executed_quote_quantity=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=100000.0, allow_zero=True),
        ),
        self_trade_prevention=st.one_of(
            st.none(),
            st.sampled_from([SelfTradePrevention.REJECT_TAKER, SelfTradePrevention.REJECT_MAKER]),
        ),
        trigger_quantity=st.one_of(st.none(), quantity_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_backpack_details_creation_properties(
        self,
        executed_quote_quantity: Decimal | None,
        self_trade_prevention: SelfTradePrevention | None,
        trigger_quantity: Decimal | None,
    ) -> None:
        """Property: BackpackOrderDetails should handle all field combinations correctly."""
        details = BackpackOrderDetails(
            executed_quote_quantity=executed_quote_quantity,
            self_trade_prevention=self_trade_prevention,
            trigger_quantity=trigger_quantity,
        )

        # Properties: Fields should be preserved
        assert details.executed_quote_quantity == executed_quote_quantity
        assert details.self_trade_prevention == self_trade_prevention
        assert details.trigger_quantity == trigger_quantity

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.executed_quote_quantity = Decimal(999999)

    @given(
        field_name=st.sampled_from([
            "executed_quote_quantity",
            "sl_trigger_price",
            "sl_limit_price",
            "tp_trigger_price",
            "tp_limit_price",
            "trigger_quantity",
        ]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_backpack_details_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: BackpackOrderDetails should validate decimal field values correctly."""
        kwargs: dict[str, Any] = {field_name: invalid_value}

        with pytest.raises(ValidationError):
            BackpackOrderDetails(**kwargs)
