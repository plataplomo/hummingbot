"""Property-based tests for Backpack Raw Fill models.

This module tests the critical Backpack raw fill models for API boundary validation to ensure:
- Robust field validation for all fill (trade execution) data from external APIs
- Financial precision preservation in all price/quantity/fee fields
- Alias mapping consistency for camelCase API fields
- Type safety and boundary validation for all field types
- Serialization round-trip properties for API data integrity
- Edge case handling for malformed or corrupted API responses

SECURITY CRITICAL: Raw fill model errors could allow malformed external data
to enter the trading system, leading to incorrect trade accounting, invalid PnL
calculations, or financial losses.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import (
    BackpackRawFillResponse,
    BackpackRawFillsList,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BACKPACK RAW FILL TESTING
# =============================================================================


def valid_string_strategy(max_length: int = 64) -> SearchStrategy[str]:
    """Generate valid non-empty strings with reasonable length.

    Args:
        max_length: Maximum length of generated strings

    Returns:
        Hypothesis strategy that generates valid string values
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_."),
        min_size=1,
        max_size=max_length,
    ).filter(lambda x: len(x.strip()) > 0)


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        Hypothesis strategy that generates valid financial decimal strings
    """
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(1000000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Scientific notation (allowed by project policy)
        st.sampled_from(["1e3", "2.5e-4", "1.23e+2", "9.99e-8"]),
        # Common edge cases
        st.just("0.00000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
        st.just("50000.0"),
    ])


def fee_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid fee amounts (typically smaller values).

    Returns:
        Hypothesis strategy that generates valid fee decimal strings
    """
    return st.one_of([
        st.decimals(
            min_value=Decimal(0),
            max_value=Decimal(100),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        st.just("0"),
        st.just("0.001"),
        st.just("0.1"),
        st.just("1.0"),
    ])


def iso_timestamp_strategy() -> SearchStrategy[str]:
    """Generate valid ISO 8601 timestamp strings.

    Returns:
        Hypothesis strategy that generates valid ISO 8601 timestamp strings
    """
    return st.datetimes(
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2030, 1, 1),
        timezones=st.just(UTC),
    ).map(lambda dt: dt.isoformat().replace("+00:00", "Z"))


def fill_side_strategy() -> SearchStrategy[str]:
    """Generate valid fill sides (Backpack format).

    Returns:
        Hypothesis strategy that generates valid Backpack fill side strings
    """
    return st.sampled_from(["Bid", "Ask"])


def symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbols.

    Returns:
        Hypothesis strategy that generates valid trading symbol strings
    """
    return st.sampled_from([
        "BTC_USDC",
        "SOL_USDC",
        "ETH_USDC",
        "PYTH_USDC",
        "BONK_USDC",
        "WIF_USDC",
    ])


def fee_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid fee symbols.

    Returns:
        Hypothesis strategy that generates valid fee symbol strings
    """
    return st.sampled_from(["USDC", "BTC", "SOL", "ETH"])


def trade_id_strategy() -> SearchStrategy[int]:
    """Generate valid trade IDs.

    Returns:
        Hypothesis strategy that generates valid trade ID integers
    """
    return st.integers(min_value=0, max_value=2**31 - 1)


def required_fill_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate required fields for a valid fill.

    Returns:
        Hypothesis strategy that generates dictionaries with required fill fields
    """
    return st.fixed_dictionaries({
        "fee": fee_decimal_string_strategy(),
        "feeSymbol": fee_symbol_strategy(),
        "isMaker": st.booleans(),
        "orderId": valid_string_strategy(max_length=128),
        "price": financial_decimal_string_strategy(),
        "quantity": financial_decimal_string_strategy(),
        "side": fill_side_strategy(),
        "symbol": symbol_strategy(),
        "timestamp": iso_timestamp_strategy(),
        "tradeId": trade_id_strategy(),
    })


def optional_fill_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate optional fields for fills.

    Returns:
        Hypothesis strategy that generates dictionaries with optional fill fields
    """
    return st.fixed_dictionaries({
        "clientId": st.one_of(st.none(), valid_string_strategy(max_length=128)),
        "systemOrderType": st.one_of(st.none(), valid_string_strategy(max_length=128)),
    })


def _merge_fill_dicts(req: dict[str, Any], opt: dict[str, Any]) -> dict[str, Any]:
    """Merge required and optional fill fields into complete dictionary.

    Args:
        req: Required fill field dictionary
        opt: Optional fill field dictionary

    Returns:
        Merged dictionary containing all fill fields
    """
    return {**req, **opt}


def complete_fill_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate complete fill data with both required and optional fields.

    Returns:
        Hypothesis strategy that generates complete fill data dictionaries
    """
    return st.builds(
        _merge_fill_dicts,
        req=required_fill_fields_strategy(),
        opt=optional_fill_fields_strategy(),
    )


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW FILL RESPONSE
# =============================================================================


class TestBackpackRawFillResponseProperties:
    """Property-based tests for BackpackRawFillResponse validation."""

    @given(fill_data=complete_fill_data_strategy())
    def test_valid_fill_creation_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Valid fill data should always create valid models."""
        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Required fields should be preserved exactly
        assert fill.fee == fill_data["fee"]
        assert fill.fee_symbol == fill_data["feeSymbol"]
        assert fill.is_maker == fill_data["isMaker"]
        assert fill.order_id == fill_data["orderId"]
        assert fill.price == fill_data["price"]
        assert fill.quantity == fill_data["quantity"]
        assert fill.side == fill_data["side"]
        assert fill.symbol == fill_data["symbol"]
        assert fill.timestamp == fill_data["timestamp"]
        assert fill.trade_id == fill_data["tradeId"]

        # Property: Optional fields should handle None correctly
        if fill_data.get("clientId") is None:
            assert fill.client_id is None
        else:
            assert fill.client_id == fill_data["clientId"]

        if fill_data.get("systemOrderType") is None:
            assert fill.system_order_type is None
        else:
            assert fill.system_order_type == fill_data["systemOrderType"]

    @given(
        required_fields=required_fill_fields_strategy(),
        missing_field=st.sampled_from([
            "fee",
            "feeSymbol",
            "isMaker",
            "orderId",
            "price",
            "quantity",
            "side",
            "symbol",
            "timestamp",
            "tradeId",
        ]),
    )
    def test_missing_required_fields_rejection(
        self, required_fields: dict[str, Any], missing_field: str
    ) -> None:
        """Property: Fills missing required fields should always be rejected."""
        incomplete_data = required_fields.copy()
        del incomplete_data[missing_field]

        # Property: Missing required field should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawFillResponse.model_validate(incomplete_data)

    @given(fill_data=complete_fill_data_strategy())
    def test_serialization_roundtrip_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Fills should survive serialization round trip."""
        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Serialize to dict
        serialized = fill.model_dump()

        # Property: All original fields should be present
        assert "fee" in serialized
        assert "fee_symbol" in serialized  # Note: converted to snake_case
        assert "is_maker" in serialized
        assert "order_id" in serialized
        assert "price" in serialized
        assert "quantity" in serialized
        assert "side" in serialized
        assert "symbol" in serialized
        assert "timestamp" in serialized
        assert "trade_id" in serialized

        # Property: Re-parsing should produce identical result
        reparsed = BackpackRawFillResponse.model_validate(serialized)

        # Check critical fields are identical
        assert reparsed.fee == fill.fee
        assert reparsed.price == fill.price
        assert reparsed.quantity == fill.quantity
        assert reparsed.trade_id == fill.trade_id

    @given(
        invalid_decimal=st.one_of(
            st.just("NaN"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
        )
    )
    def test_invalid_decimal_fields_rejection(self, invalid_decimal: str) -> None:
        """Property: Invalid decimal strings should be consistently rejected."""
        base_data = {
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "test123",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-15T10:30:00Z",
            "tradeId": 12345,
        }

        # Test each decimal field
        decimal_fields = ["fee", "price", "quantity"]

        for field in decimal_fields:
            test_data = base_data.copy()
            # Add valid values for other decimal fields
            for other_field in decimal_fields:
                if other_field != field:
                    test_data[other_field] = "1.0"
            test_data[field] = invalid_decimal

            # Property: Invalid decimal should cause validation error
            with pytest.raises((ValidationError, Exception)):
                BackpackRawFillResponse.model_validate(test_data)

    @given(
        base_data=required_fill_fields_strategy(),
        invalid_side=st.text(alphabet="xyz", min_size=1, max_size=10),
    )
    def test_invalid_side_rejection(self, base_data: dict[str, Any], invalid_side: str) -> None:
        """Property: Invalid side values should be consistently rejected."""
        # Ensure we don't accidentally generate valid sides
        valid_sides = {"Bid", "Ask", "bid", "ask", "Buy", "Sell", "buy", "sell"}
        assume(invalid_side not in valid_sides)

        test_data = base_data.copy()
        test_data["side"] = invalid_side

        # Property: Invalid side should cause validation error
        with pytest.raises((ValidationError, Exception)):
            BackpackRawFillResponse.model_validate(test_data)

    @given(fill_data=complete_fill_data_strategy())
    def test_immutability_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Fill models should be immutable after creation."""
        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises((AttributeError, ValidationError)):
            fill.fee = "999.99"

        with pytest.raises((AttributeError, ValidationError)):
            fill.trade_id = 99999

    @given(fill_data=complete_fill_data_strategy())
    def test_alias_mapping_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Alias mapping should work consistently."""
        # The model should accept camelCase aliases
        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Field access should use snake_case internally
        assert hasattr(fill, "fee_symbol")
        assert hasattr(fill, "is_maker")
        assert hasattr(fill, "order_id")
        assert hasattr(fill, "trade_id")
        assert hasattr(fill, "client_id")
        assert hasattr(fill, "system_order_type")

        # Property: Values should be correctly mapped
        assert fill.fee_symbol == fill_data["feeSymbol"]
        assert fill.is_maker == fill_data["isMaker"]
        assert fill.order_id == fill_data["orderId"]
        assert fill.trade_id == fill_data["tradeId"]

    @given(fill_data=complete_fill_data_strategy())
    def test_extra_fields_rejection(self, fill_data: dict[str, Any]) -> None:
        """Property: Extra fields should always be rejected."""
        # Add an extra field
        invalid_data = fill_data.copy()
        invalid_data["extraField"] = "should_not_be_allowed"

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawFillResponse.model_validate(invalid_data)

        # Property: Error should mention extra field
        assert "extra" in str(exc_info.value).lower()

    @given(
        fee=fee_decimal_string_strategy(),
        price=financial_decimal_string_strategy(),
        quantity=financial_decimal_string_strategy(),
    )
    def test_financial_precision_preservation(self, fee: str, price: str, quantity: str) -> None:
        """Property: Financial values should preserve exact string precision."""
        fill_data = {
            "fee": fee,
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "test123",
            "price": price,
            "quantity": quantity,
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-15T10:30:00Z",
            "tradeId": 12345,
        }

        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Financial values should be preserved exactly as strings
        assert fill.fee == fee
        assert fill.price == price
        assert fill.quantity == quantity

        # Property: If parseable, should be valid decimals
        fee_decimal = Decimal(fee)
        price_decimal = Decimal(price)
        quantity_decimal = Decimal(quantity)
        assert fee_decimal.is_finite()
        assert price_decimal.is_finite()
        assert quantity_decimal.is_finite()

        # Property: Notional calculation should be possible
        notional = price_decimal * quantity_decimal
        assert notional.is_finite()
        total_cost = notional + fee_decimal
        assert total_cost.is_finite()

    @given(negative_trade_id=st.integers(min_value=-1000, max_value=-1))
    def test_negative_trade_id_rejection(self, negative_trade_id: int) -> None:
        """Property: Negative trade IDs should be rejected."""
        fill_data = {
            "fee": "0.1",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "test123",
            "price": "100.0",
            "quantity": "1.0",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-15T10:30:00Z",
            "tradeId": negative_trade_id,
        }

        # Property: Negative trade ID should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawFillResponse.model_validate(fill_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW FILLS LIST
# =============================================================================


class TestBackpackRawFillsListProperties:
    """Property-based tests for BackpackRawFillsList validation."""

    @given(fills_data=st.lists(complete_fill_data_strategy(), min_size=0, max_size=10))
    def test_valid_fills_list_creation(self, fills_data: list[dict[str, Any]]) -> None:
        """Property: Valid fills list should always create valid models."""
        fills_list = BackpackRawFillsList.model_validate(fills_data)

        # Property: List length should be preserved
        assert len(fills_list) == len(fills_data)

        # Property: Each fill should be correctly parsed
        for i, fill_data in enumerate(fills_data):
            fill = fills_list[i]
            assert fill.fee == fill_data["fee"]
            assert fill.price == fill_data["price"]
            assert fill.quantity == fill_data["quantity"]
            assert fill.trade_id == fill_data["tradeId"]

    @given(fills_data=st.lists(complete_fill_data_strategy(), min_size=1, max_size=5))
    def test_fills_list_indexing_properties(self, fills_data: list[dict[str, Any]]) -> None:
        """Property: Fills list should support proper indexing."""
        fills_list = BackpackRawFillsList.model_validate(fills_data)

        # Property: Individual indexing should work
        for i in range(len(fills_data)):
            fill = fills_list[i]
            assert isinstance(fill, BackpackRawFillResponse)
            assert fill.trade_id == fills_data[i]["tradeId"]

        # Property: Negative indexing should work
        if len(fills_data) > 0:
            last_fill = fills_list[-1]
            assert last_fill.trade_id == fills_data[-1]["tradeId"]

        # Property: Slicing should work
        if len(fills_data) >= 2:
            slice_result = fills_list[0:2]
            assert isinstance(slice_result, list)
            assert len(slice_result) == 2
            assert all(isinstance(f, BackpackRawFillResponse) for f in slice_result)

    @given(fills_data=st.lists(complete_fill_data_strategy(), min_size=0, max_size=10))
    def test_fills_list_immutability(self, fills_data: list[dict[str, Any]]) -> None:
        """Property: Fills list should be immutable after creation."""
        fills_list = BackpackRawFillsList.model_validate(fills_data)

        # Property: List should be frozen
        with pytest.raises((AttributeError, TypeError)):
            fills_list.root = []

        # Property: Individual fills should also be immutable
        if len(fills_list) > 0:
            with pytest.raises((AttributeError, ValidationError)):
                fills_list[0].fee = "999.99"

    @given(
        fills_data=st.lists(complete_fill_data_strategy(), min_size=1, max_size=5),
        invalid_index=st.integers(min_value=0, max_value=2),
    )
    def test_fills_list_with_invalid_fill(
        self, fills_data: list[dict[str, Any]], invalid_index: int
    ) -> None:
        """Property: List with any invalid fill should be rejected."""
        # Ensure we have an index to corrupt
        assume(invalid_index < len(fills_data))

        # Corrupt one fill
        fills_data[invalid_index]["price"] = "not_a_number"

        # Property: Invalid fill in list should cause validation error
        with pytest.raises(ValidationError):
            BackpackRawFillsList.model_validate(fills_data)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawFillIntegrationProperties:
    """Integration property tests for Backpack raw fill models."""

    @given(fill_data=complete_fill_data_strategy())
    def test_model_deterministic_creation(self, fill_data: dict[str, Any]) -> None:
        """Property: Model creation should be deterministic for same inputs."""
        fill1 = BackpackRawFillResponse.model_validate(fill_data)
        fill2 = BackpackRawFillResponse.model_validate(fill_data)

        # Property: All field values should be identical
        assert fill1.fee == fill2.fee
        assert fill1.price == fill2.price
        assert fill1.quantity == fill2.quantity
        assert fill1.trade_id == fill2.trade_id
        assert fill1.timestamp == fill2.timestamp
        assert fill1.is_maker == fill2.is_maker

    @given(
        price=financial_decimal_string_strategy(),
        quantity=financial_decimal_string_strategy(),
        fee=fee_decimal_string_strategy(),
        is_maker=st.booleans(),
    )
    def test_fill_financial_calculations(
        self, price: str, quantity: str, fee: str, is_maker: bool
    ) -> None:
        """Property: Fill financial values should support accurate calculations."""
        fill_data = {
            "fee": fee,
            "feeSymbol": "USDC",
            "isMaker": is_maker,
            "orderId": "test123",
            "price": price,
            "quantity": quantity,
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-15T10:30:00Z",
            "tradeId": 12345,
        }

        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Should be able to calculate notional value
        price_decimal = Decimal(fill.price)
        quantity_decimal = Decimal(fill.quantity)
        fee_decimal = Decimal(fill.fee)

        notional = price_decimal * quantity_decimal
        assert notional.is_finite()

        # Property: Fee should be reasonable (less than notional in most cases)
        # Note: This is a soft property - fees could theoretically exceed notional
        # in some edge cases, but it's worth checking for sanity
        if notional > 0:
            fee_percentage = (fee_decimal / notional) * 100
            assert fee_percentage.is_finite()

        # Property: Total cost calculation should work
        total_cost = notional + fee_decimal if fill.side == "Bid" else notional - fee_decimal
        assert total_cost.is_finite()

    @given(fills=st.lists(complete_fill_data_strategy(), min_size=2, max_size=10))
    def test_multiple_fills_independence(self, fills: list[dict[str, Any]]) -> None:
        """Property: Multiple fills should be processed independently."""
        parsed_fills: list[BackpackRawFillResponse] = []

        for fill_data in fills:
            fill = BackpackRawFillResponse.model_validate(fill_data)
            parsed_fills.append(fill)

        # Property: Each fill should maintain its individual data
        for i, (original_data, parsed_fill) in enumerate(zip(fills, parsed_fills, strict=False)):
            assert parsed_fill.trade_id == original_data["tradeId"]
            assert parsed_fill.order_id == original_data["orderId"]
            assert parsed_fill.price == original_data["price"]
            assert parsed_fill.quantity == original_data["quantity"]

            # Property: Fills should not affect each other
            for j, other_fill in enumerate(parsed_fills):
                if i != j and original_data["tradeId"] != fills[j]["tradeId"]:
                    # Trade IDs should be independent
                    assert parsed_fill.trade_id != other_fill.trade_id
                    # Order IDs might be the same (multiple fills per order)
                    # but other fields should vary independently

    @given(
        fills_data=st.lists(complete_fill_data_strategy(), min_size=1, max_size=5),
        operation=st.sampled_from(["sum_quantities", "sum_fees", "avg_price"]),
    )
    def test_fills_aggregation_properties(
        self, fills_data: list[dict[str, Any]], operation: str
    ) -> None:
        """Property: Fills should support aggregation operations."""
        fills_list = BackpackRawFillsList.model_validate(fills_data)

        if operation == "sum_quantities":
            # Property: Should be able to sum quantities
            total_quantity = Decimal(0)
            for fill in fills_list.root:
                total_quantity += Decimal(fill.quantity)
            assert total_quantity.is_finite()
            assert total_quantity >= 0

        elif operation == "sum_fees":
            # Property: Should be able to sum fees
            total_fees = Decimal(0)
            for fill in fills_list.root:
                total_fees += Decimal(fill.fee)
            assert total_fees.is_finite()
            assert total_fees >= 0

        elif operation == "avg_price":
            # Property: Should be able to calculate weighted average price
            total_notional = Decimal(0)
            total_quantity = Decimal(0)
            for fill in fills_list.root:
                price = Decimal(fill.price)
                quantity = Decimal(fill.quantity)
                total_notional += price * quantity
                total_quantity += quantity

            if total_quantity > 0:
                avg_price = total_notional / total_quantity
                assert avg_price.is_finite()
                assert avg_price > 0

    @settings(max_examples=50)
    @given(timestamp=iso_timestamp_strategy(), trade_id=trade_id_strategy())
    def test_fill_uniqueness_properties(self, timestamp: str, trade_id: int) -> None:
        """Property: Fills should have unique identifiers."""
        fill_data = {
            "fee": "0.1",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "test123",
            "price": "100.0",
            "quantity": "1.0",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": timestamp,
            "tradeId": trade_id,
        }

        fill = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Trade ID should uniquely identify the fill
        assert fill.trade_id == trade_id

        # Property: Timestamp + trade_id should form a unique key
        unique_key = f"{fill.timestamp}_{fill.trade_id}"
        assert unique_key == f"{timestamp}_{trade_id}"

        # Property: Trade ID should be immutable
        with pytest.raises((AttributeError, ValidationError)):
            fill.trade_id = trade_id + 1
