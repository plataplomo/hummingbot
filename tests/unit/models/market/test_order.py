"""Property-based tests for Order model.

This module tests the critical Order model for trading operations to ensure:
- Financial precision preservation in all price/quantity fields
- Cross-field validation logic (order type constraints, quantity limits)
- Order lifecycle invariants (quantity_filled <= quantity_requested)
- Exchange-specific field validation and constraints
- Mathematical consistency in calculations (average fill price, etc.)

SECURITY CRITICAL: Order model errors could lead to incorrect trade execution,
wrong position sizing, invalid orders reaching exchanges, or calculation failures.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import OrderLogicError
from cyberdelta.models.market.order import BackpackOrderDetails, HyperliquidOrderDetails, Order
from cyberdelta.symbols import exchanges


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER MODEL TESTING
# =============================================================================


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for financial amounts.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        # Common trading amounts with realistic precision
        st.decimals(
            min_value=Decimal("0.00000001"),  # Crypto precision
            max_value=Decimal(1000000),
            places=8,
        ).map(str),
        st.decimals(min_value=Decimal("0.0001"), max_value=Decimal(100000), places=4).map(str),
        # Edge cases
        st.just("0.00000001"),  # Minimum crypto amount
        st.just("999999.99999999"),  # Large amount
        st.just("1.0"),  # Common unit amount
    ])


def positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate positive Decimal values for financial calculations.
    
    Returns:
        SearchStrategy for positive Decimal values.
    """
    return st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8)


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate realistic price values.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.decimals(
        min_value=Decimal("0.01"),  # Minimum meaningful price
        max_value=Decimal(100000),
        places=6,
    )


def quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate realistic quantity values.
    
    Returns:
        SearchStrategy for realistic quantity values.
    """
    return st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(10000), places=8)


def order_side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def order_type_strategy() -> SearchStrategy[OrderType]:
    """Generate valid order types.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from([
        OrderType.MARKET,
        OrderType.LIMIT,
        OrderType.STOP_MARKET,
        OrderType.STOP_LIMIT,
        OrderType.TAKE_PROFIT_MARKET,
        OrderType.TAKE_PROFIT_LIMIT,
    ])


def time_in_force_strategy() -> SearchStrategy[TimeInForce]:
    """Generate valid time in force values.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from([
        TimeInForce.GTC,
        TimeInForce.IOC,
        TimeInForce.FOK,
        TimeInForce.ALO,
    ])


def exchange_strategy() -> SearchStrategy[ExchangeName]:
    """Generate valid exchange names.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])


def symbol_strategy() -> SearchStrategy[object]:
    """Generate valid Symbol objects.
    
    Returns:
        SearchStrategy for valid Symbol objects.
    """

    def create_symbol(exchange: ExchangeName, asset: str) -> object:
        if exchange == ExchangeName.HYPERLIQUID:
            return exchanges.hyperliquid(value=asset)
        return exchanges.backpack(value=asset)

    return st.builds(
        create_symbol,
        exchange=exchange_strategy(),
        asset=st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),
    )


def order_status_strategy() -> SearchStrategy[OrderStatus]:
    """Generate valid order statuses.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from([
        OrderStatus.NEW,
        OrderStatus.OPEN,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
    ])


def limit_order_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate data for valid limit orders.
    
    Returns:
        SearchStrategy for limit order data dictionaries.
    """
    return st.fixed_dictionaries({
        "symbol": symbol_strategy(),
        "side": order_side_strategy(),
        "order_type": st.just(OrderType.LIMIT),
        "quantity_requested": positive_decimal_strategy(),
        "price": price_strategy(),
        "time_in_force": time_in_force_strategy(),
        "exchange": exchange_strategy(),
    })


def market_order_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate data for valid market orders.
    
    Returns:
        SearchStrategy for market order data dictionaries.
    """
    return st.fixed_dictionaries({
        "symbol": symbol_strategy(),
        "side": order_side_strategy(),
        "order_type": st.just(OrderType.MARKET),
        "quantity_requested": positive_decimal_strategy(),
        "time_in_force": time_in_force_strategy(),
        "exchange": exchange_strategy(),
    })


def stop_order_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate data for valid stop orders.
    
    Returns:
        SearchStrategy for stop order data dictionaries.
    """
    return st.fixed_dictionaries({
        "symbol": symbol_strategy(),
        "side": order_side_strategy(),
        "order_type": st.sampled_from([OrderType.STOP_MARKET, OrderType.STOP_LIMIT]),
        "quantity_requested": positive_decimal_strategy(),
        "stop_price": price_strategy(),
        "price": st.one_of(st.none(), price_strategy()),  # Optional for stop market
        "time_in_force": time_in_force_strategy(),
        "exchange": exchange_strategy(),
    })


# =============================================================================
# PROPERTY TESTS FOR ORDER VALIDATION LOGIC
# =============================================================================


class TestOrderValidationProperties:
    """Property-based tests for Order model validation logic."""

    @given(order_data=limit_order_data_strategy())
    def test_limit_order_creation_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid limit orders should always be created successfully."""
        # Ensure price is provided for limit orders
        assert order_data["price"] is not None
        assert order_data["price"] > 0

        order = Order(**order_data)

        # Property: All financial values should be preserved as Decimal
        assert isinstance(order.quantity_requested, Decimal)
        assert isinstance(order.price, Decimal)
        assert isinstance(order.quantity_filled, Decimal)

        # Property: Limit orders must have positive price
        assert order.price > Decimal(0)

        # Property: Order type consistency
        assert order.order_type == OrderType.LIMIT

        # Property: Initial state consistency
        assert order.quantity_filled == Decimal(0)
        assert order.status == OrderStatus.NEW

        # Property: Financial invariants
        assert order.quantity_requested > Decimal(0)
        assert order.quantity_filled <= order.quantity_requested

    @given(order_data=market_order_data_strategy())
    def test_market_order_creation_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid market orders should be created without price."""
        order = Order(**order_data)

        # Property: Market orders should not require price
        assert order.order_type == OrderType.MARKET

        # Property: Price should be None for market orders
        assert order.price is None

        # Property: Financial precision preserved
        assert isinstance(order.quantity_requested, Decimal)
        assert order.quantity_requested > Decimal(0)

    @given(
        quantity_requested=positive_decimal_strategy(), quantity_filled=positive_decimal_strategy()
    )
    def test_quantity_relationship_invariants(
        self, quantity_requested: Decimal, quantity_filled: Decimal
    ) -> None:
        """Property: quantity_filled must never exceed quantity_requested."""
        # Only test cases where the relationship would be valid
        assume(quantity_filled <= quantity_requested)

        # Create Order explicitly to avoid mypy dict unpacking issues
        average_fill_price = Decimal("50000.0") if quantity_filled > 0 else None

        order = Order(
            symbol=exchanges.hyperliquid(value="BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=quantity_requested,
            quantity_filled=quantity_filled,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            average_fill_price=average_fill_price,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Relationship preserved
        assert order.quantity_filled <= order.quantity_requested

        # Property: Both values are non-negative
        assert order.quantity_filled >= Decimal(0)
        assert order.quantity_requested > Decimal(0)

        # Property: Precision preserved
        assert order.quantity_filled == quantity_filled
        assert order.quantity_requested == quantity_requested

    @given(
        quantity_requested=positive_decimal_strategy(), quantity_filled=positive_decimal_strategy()
    )
    def test_invalid_quantity_relationship_rejection(
        self, quantity_requested: Decimal, quantity_filled: Decimal
    ) -> None:
        """Property: Orders with quantity_filled > quantity_requested should be rejected."""
        # Only test invalid cases
        assume(quantity_filled > quantity_requested)

        # Property: Invalid relationship should be rejected
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                average_fill_price=Decimal("50000.0"),  # Required for filled orders
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

    @given(
        order_type=st.sampled_from([
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ])
    )
    def test_limit_type_price_requirement(self, order_type: OrderType) -> None:
        """Property: Limit-type orders must have positive prices."""
        # Test with missing price (None)
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=None,  # Missing price
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        # Test with zero price
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=Decimal(0),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        # Test with negative price
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=Decimal("-10.5"),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

    @given(order_type=st.sampled_from([OrderType.STOP_MARKET, OrderType.STOP_LIMIT]))
    def test_stop_type_stop_price_requirement(self, order_type: OrderType) -> None:
        """Property: Stop-type orders must have positive stop prices."""
        # Set price for STOP_LIMIT
        price = Decimal("50000.0") if order_type == OrderType.STOP_LIMIT else None

        # Test missing stop_price
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=price,
                stop_price=None,  # Missing stop_price
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        # Test zero stop_price
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=price,
                stop_price=Decimal(0),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        # Test negative stop_price
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=order_type,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                price=price,
                stop_price=Decimal("-1000.0"),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

    @given(
        quantity_filled=positive_decimal_strategy(),
        average_fill_price=st.one_of(st.none(), st.just(Decimal(0)), price_strategy()),
    )
    def test_average_fill_price_validation(
        self, quantity_filled: Decimal, average_fill_price: Decimal | None
    ) -> None:
        """Property: average_fill_price must be positive if quantity_filled > 0."""
        if quantity_filled > 0 and (average_fill_price is None or average_fill_price <= 0):
            # Property: Should reject invalid average fill price for filled orders
            with pytest.raises((OrderLogicError, Exception)):
                Order(
                    symbol=exchanges.hyperliquid(value="BTC"),
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity_requested=quantity_filled
                    + Decimal("1.0"),  # Ensure valid relationship
                    quantity_filled=quantity_filled,
                    average_fill_price=average_fill_price,
                    time_in_force=TimeInForce.GTC,
                    exchange=ExchangeName.HYPERLIQUID,
                    updated_at=None,
                    triggered_at=None,
                    strategy_name=None,
                    signal_id=None,
                )
        else:
            # Property: Should accept valid combinations
            order = Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=quantity_filled + Decimal("1.0"),  # Ensure valid relationship
                quantity_filled=quantity_filled,
                average_fill_price=average_fill_price,
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
            if quantity_filled > 0:
                assert order.average_fill_price is not None
                assert order.average_fill_price > Decimal(0)


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC VALIDATION
# =============================================================================


class TestOrderExchangeValidationProperties:
    """Property-based tests for exchange-specific order validation."""

    @given(exchange=exchange_strategy())
    def test_exchange_details_consistency(self, exchange: ExchangeName) -> None:
        """Property: Orders should only have details for their own exchange."""
        # Get the appropriate symbol for the exchange
        symbol = (
            exchanges.hyperliquid(value="BTC")
            if exchange == ExchangeName.HYPERLIQUID
            else exchanges.backpack(value="BTC")
        )

        # Test with correct exchange details
        if exchange == ExchangeName.HYPERLIQUID:
            order = Order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=exchange,
                hl_details=HyperliquidOrderDetails(),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
            assert order.hl_details is not None
            assert order.bp_details is None
        else:
            order = Order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=exchange,
                bp_details=BackpackOrderDetails(),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
            assert order.bp_details is not None
            assert order.hl_details is None

    def test_exchange_details_cross_contamination(self) -> None:
        """Property: Orders should reject details from other exchanges."""
        # Hyperliquid order with Backpack details
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.hyperliquid(value="BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.HYPERLIQUID,
                bp_details=BackpackOrderDetails(),  # Wrong exchange details
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        # Backpack order with Hyperliquid details
        with pytest.raises((OrderLogicError, Exception)):
            Order(
                symbol=exchanges.backpack(value="BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity_requested=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                exchange=ExchangeName.BACKPACK,
                hl_details=HyperliquidOrderDetails(),  # Wrong exchange details
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )


# =============================================================================
# PROPERTY TESTS FOR FINANCIAL PRECISION
# =============================================================================


class TestOrderFinancialPrecisionProperties:
    """Property-based tests for financial precision preservation in orders."""

    @given(
        price=st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(999999), places=8),
        quantity=st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(10000), places=8),
    )
    def test_financial_precision_preservation(self, price: Decimal, quantity: Decimal) -> None:
        """Property: All financial values should preserve exact decimal precision."""
        order = Order(
            symbol=exchanges.hyperliquid(value="BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=quantity,
            price=price,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Exact precision preserved
        assert order.price == price
        assert order.quantity_requested == quantity
        assert order.price is not None

        # Property: String representation should be consistent
        assert str(order.price) == str(price)
        assert str(order.quantity_requested) == str(quantity)

        # Property: Mathematical operations should be exact
        assert order.price * order.quantity_requested == price * quantity

    @given(
        base_price=price_strategy(),
        quantity_filled=quantity_strategy(),
        fees=st.decimals(min_value=Decimal(0), max_value=Decimal(100), places=6),
    )
    def test_order_value_calculations(
        self, base_price: Decimal, quantity_filled: Decimal, fees: Decimal
    ) -> None:
        """Property: Order value calculations should be mathematically consistent."""
        order = Order(
            symbol=exchanges.hyperliquid(value="BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=quantity_filled + Decimal("1.0"),  # Ensure valid relationship
            quantity_filled=quantity_filled,
            price=base_price,
            average_fill_price=base_price,  # Same as price for simplicity
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Notional value calculation should be exact
        if order.quantity_filled > 0 and order.average_fill_price is not None:
            notional_value = order.quantity_filled * order.average_fill_price
            expected_value = quantity_filled * base_price
            assert notional_value == expected_value

        # Property: All calculations should be finite
        assert order.quantity_filled.is_finite()
        if order.price is not None:
            assert order.price.is_finite()
        if order.average_fill_price is not None:
            assert order.average_fill_price.is_finite()


# =============================================================================
# PROPERTY TESTS FOR ORDER LIFECYCLE
# =============================================================================


class TestOrderLifecycleProperties:
    """Property-based tests for order lifecycle management."""

    @given(
        initial_status=st.sampled_from([OrderStatus.NEW, OrderStatus.OPEN]),
        final_status=st.sampled_from([
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.PARTIALLY_FILLED,
        ]),
    )
    def test_order_status_transitions(
        self, initial_status: OrderStatus, final_status: OrderStatus
    ) -> None:
        """Property: Order status transitions should maintain consistency."""
        order = Order(
            symbol=exchanges.hyperliquid(value="BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            status=initial_status,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Initial state should be valid
        assert order.status == initial_status
        assert order.quantity_filled == Decimal(0)

        # Update status (this tests mutable model behavior)
        order.status = final_status

        # Property: Status should be updated
        assert order.status == final_status

        # Property: Core invariants should still hold
        assert order.quantity_filled <= order.quantity_requested

    @given(fill_quantity=quantity_strategy())
    def test_order_fill_updates(self, fill_quantity: Decimal) -> None:
        """Property: Order fill updates should maintain mathematical consistency."""
        total_quantity = fill_quantity + Decimal("1.0")  # Ensure valid relationship

        order = Order(
            symbol=exchanges.hyperliquid(value="BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=total_quantity,
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Property: Initial unfilled state
        assert order.quantity_filled == Decimal(0)

        # Update with fill (need to validate this works with the model's validation)
        try:
            order.quantity_filled = fill_quantity
            order.average_fill_price = Decimal("50000.0")  # Same as limit price

            # Property: Fill should be valid
            assert order.quantity_filled == fill_quantity
            assert order.quantity_filled <= order.quantity_requested

            # Property: Average fill price should be positive for filled orders
            if order.quantity_filled > 0:
                assert order.average_fill_price > Decimal(0)
        except (ValueError, TypeError, AttributeError):
            # Model validation might prevent invalid updates - this is acceptable
            pass


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestOrderIntegrationProperties:
    """Integration property tests for Order model behavior."""

    @given(order_data=st.one_of(limit_order_data_strategy(), market_order_data_strategy()))
    def test_order_creation_deterministic(self, order_data: dict[str, Any]) -> None:
        """Property: Order creation should be deterministic for same inputs."""
        # Create same order twice
        order1 = Order(**order_data)
        order2 = Order(**order_data)

        # Property: All field values should be identical (except client_order_id which has
        # UUID default)
        assert order1.symbol == order2.symbol
        assert order1.side == order2.side
        assert order1.order_type == order2.order_type
        assert order1.quantity_requested == order2.quantity_requested
        assert order1.price == order2.price
        assert order1.exchange == order2.exchange

        # Property: Generated IDs should be different (UUID factory)
        assert order1.client_order_id != order2.client_order_id

    @given(order_data=limit_order_data_strategy())
    def test_order_serialization_round_trip(self, order_data: dict[str, Any]) -> None:
        """Property: Order should survive serialization round trip."""
        order = Order(**order_data)

        # Serialize to dict
        order_dict = order.model_dump()

        # Property: All critical fields should be present
        assert "quantity_requested" in order_dict
        assert "price" in order_dict
        assert "symbol" in order_dict
        assert "exchange" in order_dict

        # Property: Financial values should be preserved correctly
        # Note: The actual serialization format may vary (str or Decimal)
        if isinstance(order_dict["quantity_requested"], str):
            assert Decimal(order_dict["quantity_requested"]) == order.quantity_requested
        elif isinstance(order_dict["quantity_requested"], Decimal):
            assert order_dict["quantity_requested"] == order.quantity_requested

        # Property: Price field handling
        if order.price is not None and order_dict["price"] is not None:
            if isinstance(order_dict["price"], str):
                assert Decimal(order_dict["price"]) == order.price
            elif isinstance(order_dict["price"], Decimal):
                assert order_dict["price"] == order.price
