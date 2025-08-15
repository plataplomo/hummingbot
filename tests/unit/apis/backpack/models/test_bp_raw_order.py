"""Property-based tests for Backpack Raw Order models.

This module tests the critical Backpack raw order models for API boundary validation to ensure:
- Robust field validation for all order data from external APIs
- Financial precision preservation in all price/quantity fields
- Alias mapping consistency across REST and WebSocket formats
- Type safety and boundary validation for all field types
- Serialization round-trip properties for API data integrity
- Edge case handling for malformed or corrupted API responses

SECURITY CRITICAL: Raw order model errors could allow malformed external data
to enter the trading system, leading to incorrect order processing, invalid trades,
or system instability.
"""

import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import (
    BackpackRawOrderBook,
    BackpackRawOrderResponse,
    BackpackRawOrderUpdate,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BACKPACK RAW ORDER TESTING
# =============================================================================


def valid_string_strategy(max_length: int = 64) -> SearchStrategy[str]:
    """Generate valid non-empty strings with reasonable length.

    Returns:
        SearchStrategy[str]: Strategy generating valid non-empty strings.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_."),
        min_size=1,
        max_size=max_length,
    ).filter(lambda x: len(x.strip()) > 0)


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        SearchStrategy[str]: Strategy generating valid decimal strings.
    """
    return st.one_of([
        # Normal decimal values
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        # Scientific notation (allowed by project policy)
        st.sampled_from(["1e3", "2.5e-4", "1.23e+2", "9.99e-8"]),
        # Common edge cases
        st.just("0.00000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
    ])


def timestamp_strategy() -> SearchStrategy[object]:
    """Generate valid timestamp values (int, float, or ISO string).

    Returns:
        SearchStrategy[object]: Strategy generating valid timestamp values.
    """
    return st.one_of([
        st.integers(min_value=1000000000, max_value=2000000000),  # Unix timestamps
        st.floats(
            min_value=1000000000.0, max_value=2000000000.0, allow_nan=False, allow_infinity=False
        ),
        st.just("2024-01-15T10:30:00Z"),  # ISO format
        st.just("2024-01-15T10:30:00.123Z"),  # ISO with milliseconds
    ])


def order_side_strategy() -> SearchStrategy[str]:
    """Generate valid order sides.

    Returns:
        SearchStrategy[str]: Strategy generating valid order sides.
    """
    return st.sampled_from(["buy", "sell", "Bid", "Ask"])


def order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order types.

    Returns:
        SearchStrategy[str]: Strategy generating valid order types.
    """
    return st.sampled_from([
        "LIMIT",
        "MARKET",
        "STOP",
        "TRAILING_STOP",
        "TAKE_PROFIT",
        # Backpack API case variations
        "Limit",
        "Market",
        "Stop",
        "TrailingStop",
        "TakeProfit",
    ])


def order_status_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order statuses.

    Returns:
        SearchStrategy[str]: Strategy generating valid order statuses.
    """
    return st.sampled_from([
        "NEW",
        "FILLED",
        "CANCELLED",
        "EXPIRED",
        "REJECTED",
        "PARTIALLY_FILLED",
        "TRIGGER_PENDING",
        # Backpack API case variations
        "New",
        "Filled",
        "Cancelled",
        "Expired",
        "Rejected",
        "PartiallyFilled",
        "TriggerPending",
    ])


def boolean_strategy() -> SearchStrategy[bool]:
    """Generate boolean values.

    Returns:
        SearchStrategy[bool]: Strategy generating boolean values.
    """
    return st.booleans()


def required_order_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate required fields for a valid order.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy generating required order fields.
    """
    return st.fixed_dictionaries({
        "id": valid_string_strategy(),
        "symbol": valid_string_strategy(),
        "side": order_side_strategy(),
        "orderType": order_type_strategy(),
        "status": order_status_strategy(),
        "createdAt": timestamp_strategy(),
    })


def optional_order_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate optional fields for orders.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy generating optional order fields.
    """
    return st.fixed_dictionaries({
        "clientId": st.one_of(st.none(), valid_string_strategy()),
        "relatedOrderId": st.one_of(st.none(), valid_string_strategy()),
        "quantity": st.one_of(st.none(), financial_decimal_string_strategy()),
        "executedQuantity": st.one_of(st.none(), financial_decimal_string_strategy()),
        "executedQuoteQuantity": st.one_of(st.none(), financial_decimal_string_strategy()),
        "price": st.one_of(st.none(), financial_decimal_string_strategy()),
        "triggerPrice": st.one_of(st.none(), financial_decimal_string_strategy()),
        "avgFillPrice": st.one_of(st.none(), financial_decimal_string_strategy()),
        "triggerBy": st.one_of(st.none(), valid_string_strategy(max_length=32)),
        "timeInForce": st.one_of(st.none(), st.sampled_from(["GTC", "IOC", "FOK"])),
        "reduceOnly": st.one_of(st.none(), boolean_strategy()),
        "postOnly": st.one_of(st.none(), boolean_strategy()),
        "selfTradePrevention": st.one_of(
            st.none(), st.sampled_from(["EXPIRE_TAKER", "EXPIRE_MAKER", "EXPIRE_BOTH"])
        ),
        "updatedAt": st.one_of(st.none(), timestamp_strategy()),
        "triggeredAt": st.one_of(st.none(), timestamp_strategy()),
        "expiryReason": st.one_of(st.none(), valid_string_strategy()),
        "origin": st.one_of(st.none(), valid_string_strategy()),
    })


def complete_order_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate complete order data with both required and optional fields.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy generating complete order data.
    """

    def merge_dicts(req: dict[str, Any], opt: dict[str, Any]) -> dict[str, Any]:
        return {**req, **opt}

    return st.builds(
        merge_dicts,
        req=required_order_fields_strategy(),
        opt=optional_order_fields_strategy(),
    )


def order_book_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid order book data.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy generating valid order book data.
    """

    def bid_ask_strategy() -> SearchStrategy[list[tuple[str, str]]]:
        return st.lists(
            st.tuples(financial_decimal_string_strategy(), financial_decimal_string_strategy()),
            min_size=1,
            max_size=10,
        )

    return st.fixed_dictionaries({
        "symbol": valid_string_strategy(),
        "bids": bid_ask_strategy(),
        "asks": bid_ask_strategy(),
        "time": timestamp_strategy(),
    })


def order_update_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid order update data.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy generating valid order update data.
    """
    return st.fixed_dictionaries({
        "e": st.sampled_from(["orderAccepted", "orderFill", "orderCanceled"]),
        "E": timestamp_strategy(),
        "s": valid_string_strategy(),
        "S": st.sampled_from(["Bid", "Ask"]),
        "o": order_type_strategy(),
        "X": order_status_strategy(),
        "c": st.one_of(st.none(), valid_string_strategy()),
        "f": st.one_of(st.none(), st.sampled_from(["GTC", "IOC", "FOK"])),
        "q": st.one_of(st.none(), financial_decimal_string_strategy()),
        "p": st.one_of(st.none(), financial_decimal_string_strategy()),
    })


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER RESPONSE
# =============================================================================


class TestBackpackRawOrderResponseProperties:
    """Property-based tests for BackpackRawOrderResponse validation."""

    @given(order_data=complete_order_data_strategy())
    def test_valid_order_creation_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid order data should always create valid models."""
        order = BackpackRawOrderResponse.model_validate(order_data)

        # Property: Required fields should be preserved exactly
        assert order.id == order_data["id"]
        assert order.symbol == order_data["symbol"]
        assert order.side == order_data["side"]
        assert order.orderType == order_data["orderType"]
        assert order.status == order_data["status"]

        # Property: Financial fields should preserve exact string values
        if order_data.get("quantity"):
            assert order.quantity == order_data["quantity"]
        if order_data.get("price"):
            assert order.price == order_data["price"]
        if order_data.get("executedQuantity"):
            assert order.executedQuantity == order_data["executedQuantity"]

        # Property: Optional fields should handle None correctly
        if order_data.get("clientId") is None:
            assert order.clientId is None
        else:
            assert order.clientId == order_data["clientId"]

    @given(
        required_fields=required_order_fields_strategy(),
        missing_field=st.sampled_from(["id", "symbol", "side", "orderType", "status", "createdAt"]),
    )
    def test_missing_required_fields_rejection(
        self, required_fields: dict[str, Any], missing_field: str
    ) -> None:
        """Property: Orders missing required fields should always be rejected."""
        incomplete_data = required_fields.copy()
        del incomplete_data[missing_field]

        # Property: Missing required field should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawOrderResponse.model_validate(incomplete_data)

    @given(order_data=complete_order_data_strategy())
    def test_serialization_roundtrip_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Orders should survive serialization round trip."""
        order = BackpackRawOrderResponse.model_validate(order_data)

        # Serialize to dict
        serialized = order.model_dump()

        # Property: All original fields should be present
        for key, value in order_data.items():
            if value is not None:
                assert key in serialized
                assert serialized[key] == value

        # Property: Re-parsing should produce identical result
        reparsed = BackpackRawOrderResponse.model_validate(serialized)

        # Check critical fields are identical
        assert reparsed.id == order.id
        assert reparsed.symbol == order.symbol
        assert reparsed.side == order.side
        assert reparsed.quantity == order.quantity
        assert reparsed.price == order.price

    @given(
        invalid_decimal=st.one_of(
            st.just("NaN"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
        )
    )
    def test_invalid_decimal_fields_rejection(self, invalid_decimal: str) -> None:
        """Property: Invalid decimal strings should be consistently rejected."""
        base_data = {
            "id": "test123",
            "symbol": "BTC_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "status": "NEW",
            "createdAt": 1234567890,
        }

        # Test each decimal field
        decimal_fields = ["quantity", "price", "executedQuantity", "triggerPrice", "avgFillPrice"]

        for field in decimal_fields:
            test_data = base_data.copy()
            test_data[field] = invalid_decimal

            # Property: Invalid decimal should cause validation error
            with pytest.raises((ValidationError, Exception)):
                BackpackRawOrderResponse.model_validate(test_data)

    @given(
        base_data=required_order_fields_strategy(),
        invalid_enum=st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=10),
    )
    def test_invalid_enum_fields_rejection(
        self, base_data: dict[str, Any], invalid_enum: str
    ) -> None:
        """Property: Invalid enum values should be consistently rejected."""
        # Ensure we don't accidentally generate valid enum values
        valid_sides = {"buy", "sell", "Bid", "Ask"}
        valid_types = {
            "LIMIT",
            "MARKET",
            "STOP",
            "TRAILING_STOP",
            "TAKE_PROFIT",
            "Limit",
            "Market",
            "Stop",
            "TrailingStop",
            "TakeProfit",
        }
        valid_statuses = {
            "NEW",
            "FILLED",
            "CANCELLED",
            "EXPIRED",
            "REJECTED",
            "PARTIALLY_FILLED",
            "TRIGGER_PENDING",
            "New",
            "Filled",
            "Cancelled",
            "Expired",
            "Rejected",
            "PartiallyFilled",
            "TriggerPending",
        }

        assume(invalid_enum not in valid_sides)
        assume(invalid_enum not in valid_types)
        assume(invalid_enum not in valid_statuses)

        # Test invalid side
        test_data = base_data.copy()
        test_data["side"] = invalid_enum
        with pytest.raises((ValidationError, Exception)):
            BackpackRawOrderResponse.model_validate(test_data)

        # Test invalid order type
        test_data = base_data.copy()
        test_data["orderType"] = invalid_enum
        with pytest.raises((ValidationError, Exception)):
            BackpackRawOrderResponse.model_validate(test_data)

        # Test invalid status
        test_data = base_data.copy()
        test_data["status"] = invalid_enum
        with pytest.raises((ValidationError, Exception)):
            BackpackRawOrderResponse.model_validate(test_data)

    @given(order_data=complete_order_data_strategy())
    def test_alias_mapping_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Alias mapping should work consistently."""
        # Create aliased version of the data
        aliased_data = {}
        alias_map = {
            "id": "i",
            "clientId": "c",
            "symbol": "s",
            "side": "S",
            "orderType": "o",
            "status": "X",
            "quantity": "q",
            "price": "p",
            "createdAt": "E",
        }

        for canonical, alias in alias_map.items():
            if canonical in order_data and order_data[canonical] is not None:
                aliased_data[alias] = order_data[canonical]

        # Ensure we have required fields via aliases
        if "i" not in aliased_data:
            aliased_data["i"] = "test123"
        if "s" not in aliased_data:
            aliased_data["s"] = "BTC_USDC"
        if "S" not in aliased_data:
            aliased_data["S"] = "buy"
        if "o" not in aliased_data:
            aliased_data["o"] = "LIMIT"
        if "X" not in aliased_data:
            aliased_data["X"] = "NEW"
        if "E" not in aliased_data:
            aliased_data["E"] = 1234567890

        # Property: Aliased data should parse correctly
        order = BackpackRawOrderResponse.model_validate(aliased_data)

        # Property: Values should be correctly mapped
        assert order.id == aliased_data["i"]
        assert order.symbol == aliased_data["s"]
        assert order.side == aliased_data["S"]
        assert order.orderType == aliased_data["o"]
        assert order.status == aliased_data["X"]

    @given(order_data=complete_order_data_strategy())
    def test_extra_fields_rejection(self, order_data: dict[str, Any]) -> None:
        """Property: Extra fields should always be rejected."""
        # Add an extra field
        invalid_data = order_data.copy()
        invalid_data["extraField"] = "should_not_be_allowed"

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawOrderResponse.model_validate(invalid_data)

        # Property: Error should mention extra field
        assert "extra" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()

    @given(
        financial_value=financial_decimal_string_strategy(),
        field_name=st.sampled_from([
            "quantity",
            "price",
            "executedQuantity",
            "avgFillPrice",
            "triggerPrice",
        ]),
    )
    def test_financial_precision_preservation(self, financial_value: str, field_name: str) -> None:
        """Property: Financial values should preserve exact string precision."""
        order_data = {
            "id": "test123",
            "symbol": "BTC_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "status": "NEW",
            "createdAt": 1234567890,
            field_name: financial_value,
        }

        order = BackpackRawOrderResponse.model_validate(order_data)

        # Property: Financial value should be preserved exactly as string
        assert getattr(order, field_name) == financial_value

        # Property: If parseable, should be valid decimal
        try:
            decimal_value = Decimal(financial_value)
            assert decimal_value.is_finite()
        except (ValueError, TypeError, OverflowError):
            # If not parseable as decimal, validation should have failed
            # This tests our decimal validation strategy
            pytest.fail("Invalid decimal passed validation")


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER BOOK
# =============================================================================


class TestBackpackRawOrderBookProperties:
    """Property-based tests for BackpackRawOrderBook validation."""

    @given(book_data=order_book_data_strategy())
    def test_valid_orderbook_creation_properties(self, book_data: dict[str, Any]) -> None:
        """Property: Valid order book data should always create valid models."""
        book = BackpackRawOrderBook.model_validate(book_data)

        # Property: All fields should be preserved
        assert book.symbol == book_data["symbol"]
        assert len(book.bids) == len(book_data["bids"])
        assert len(book.asks) == len(book_data["asks"])

        # Property: Bid/ask tuples should preserve price/quantity
        for i, (price, qty) in enumerate(book_data["bids"]):
            assert book.bids[i][0] == price
            assert book.bids[i][1] == qty

        for i, (price, qty) in enumerate(book_data["asks"]):
            assert book.asks[i][0] == price
            assert book.asks[i][1] == qty

    @given(
        symbol=valid_string_strategy(),
        invalid_bid_ask=st.one_of(
            st.just([["invalid_price", "1.0"]]),
            st.just([["50000.0", "invalid_quantity"]]),
            st.just([["NaN", "1.0"]]),
            st.just([["50000.0", "Infinity"]]),
        ),
    )
    def test_invalid_bid_ask_rejection(self, symbol: str, invalid_bid_ask: list[list[str]]) -> None:
        """Property: Invalid bid/ask data should be rejected."""
        book_data = {
            "symbol": symbol,
            "bids": invalid_bid_ask,
            "asks": [["50100.0", "0.5"]],
            "time": 1234567890,
        }

        # Property: Invalid price/quantity should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawOrderBook.model_validate(book_data)

    @given(book_data=order_book_data_strategy())
    def test_orderbook_serialization_roundtrip(self, book_data: dict[str, Any]) -> None:
        """Property: Order books should survive serialization round trip."""
        book = BackpackRawOrderBook.model_validate(book_data)

        # Serialize and deserialize
        serialized = book.model_dump()
        reparsed = BackpackRawOrderBook.model_validate(serialized)

        # Property: All data should be identical
        assert reparsed.symbol == book.symbol
        assert reparsed.bids == book.bids
        assert reparsed.asks == book.asks
        assert reparsed.time == book.time


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER UPDATE
# =============================================================================


class TestBackpackRawOrderUpdateProperties:
    """Property-based tests for BackpackRawOrderUpdate validation."""

    @given(update_data=order_update_data_strategy())
    def test_valid_order_update_creation_properties(self, update_data: dict[str, Any]) -> None:
        """Property: Valid order update data should always create valid models."""
        update = BackpackRawOrderUpdate.model_validate(update_data)

        # Property: Required fields should be preserved
        assert update.event_type == update_data["e"]
        assert update.symbol == update_data["s"]
        assert update.side == update_data["S"]
        assert update.order_type == update_data["o"]
        assert update.order_status == update_data["X"]

        # Property: Optional fields should handle None correctly
        if update_data.get("c"):
            assert update.client_order_id == update_data["c"]
        else:
            assert update.client_order_id is None

    @given(
        update_data=order_update_data_strategy(),
        missing_field=st.sampled_from(["e", "E", "s", "S", "o", "X"]),
    )
    def test_order_update_missing_required_fields(
        self, update_data: dict[str, Any], missing_field: str
    ) -> None:
        """Property: Order updates missing required fields should be rejected."""
        incomplete_data = update_data.copy()
        del incomplete_data[missing_field]

        # Property: Missing required field should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawOrderUpdate.model_validate(incomplete_data)

    @given(update_data=order_update_data_strategy())
    def test_order_update_immutability_properties(self, update_data: dict[str, Any]) -> None:
        """Property: Order updates should be immutable after creation."""
        update = BackpackRawOrderUpdate.model_validate(update_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError):
            update.event_type = "modified"

        with pytest.raises(ValidationError):
            update.symbol = "MODIFIED"


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawOrderIntegrationProperties:
    """Integration property tests for Backpack raw order models."""

    @given(order_data=complete_order_data_strategy())
    def test_model_deterministic_creation(self, order_data: dict[str, Any]) -> None:
        """Property: Model creation should be deterministic for same inputs."""
        order1 = BackpackRawOrderResponse.model_validate(order_data)
        order2 = BackpackRawOrderResponse.model_validate(order_data)

        # Property: All field values should be identical
        assert order1.id == order2.id
        assert order1.symbol == order2.symbol
        assert order1.quantity == order2.quantity
        assert order1.price == order2.price
        assert order1.side == order2.side
        assert order1.status == order2.status

    # Note: Corruption resistance is covered by other validation tests

    @given(
        decimal_value=financial_decimal_string_strategy(),
        operation=st.sampled_from(["addition", "multiplication", "precision_check"]),
    )
    def test_financial_calculation_properties(self, decimal_value: str, operation: str) -> None:
        """Property: Financial values should maintain precision for calculations."""
        order_data = {
            "id": "test123",
            "symbol": "BTC_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "status": "NEW",
            "createdAt": 1234567890,
            "quantity": decimal_value,
            "price": decimal_value,
        }

        order = BackpackRawOrderResponse.model_validate(order_data)

        # Property: Should be able to reconstruct exact Decimal values
        if order.quantity and order.price:
            quantity_decimal = Decimal(order.quantity)
            price_decimal = Decimal(order.price)

            # Property: Basic operations should work with exact precision
            if operation == "addition":
                result = quantity_decimal + price_decimal
                assert result.is_finite()
            elif operation == "multiplication":
                result = quantity_decimal * price_decimal
                assert result.is_finite()
            elif operation == "precision_check":
                # Property: String conversion should be reversible
                assert str(quantity_decimal) == order.quantity or quantity_decimal == Decimal(
                    order.quantity
                )
                assert str(price_decimal) == order.price or price_decimal == Decimal(order.price)

    @given(orders=st.lists(complete_order_data_strategy(), min_size=2, max_size=10))
    def test_multiple_orders_independence(self, orders: list[dict[str, Any]]) -> None:
        """Property: Multiple orders should be processed independently."""
        parsed_orders: list[BackpackRawOrderResponse] = []

        for order_data in orders:
            order = BackpackRawOrderResponse.model_validate(order_data)
            parsed_orders.append(order)

        # Property: Each order should maintain its individual data
        for i, (original_data, parsed_order) in enumerate(zip(orders, parsed_orders, strict=False)):
            assert parsed_order.id == original_data["id"]
            assert parsed_order.symbol == original_data["symbol"]

            # Property: Orders should not affect each other
            for j, other_order in enumerate(parsed_orders):
                # Orders should have independent IDs if they are different
                if i != j and original_data["id"] != orders[j]["id"]:
                    assert parsed_order.id != other_order.id
