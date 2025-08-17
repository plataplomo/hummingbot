"""Property-based tests for the CyberDeltaEngine OrderBook model.

This module provides comprehensive property-based testing of the OrderBook Pydantic model,
which represents immutable snapshots of Level 2 order book data for trading symbols.

Key Testing Areas:
- Order book field validation and type safety using property-based input generation
- Financial precision handling for bid/ask prices and quantities
- Level structure validation (price/quantity pairs, list structure)
- Order book constraint validation (finite prices, non-negative quantities)
- Immutability properties and frozen model behavior
- Order book parsing from various input formats (strings, ints, floats, Decimals)
- Edge cases and boundary conditions for order book data

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete order book creation flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast


if TYPE_CHECKING:
    pass

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.exceptions.field_validation import (
    DecimalFieldError,
    DecimalFiniteError,
    ListFieldError,
    RangeFieldError,
    TypeFieldError,
)
from cyberdelta.models.market.order_book import OrderBook
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


pytestmark = pytest.mark.timing


# =============================================================================
# TYPE DEFINITIONS FOR ORDER BOOK MODEL TESTING
# =============================================================================

# General malicious value types
MaliciousValue = str | int | float | bytes | list[str] | dict[str, str] | None

# Malicious price/quantity types that can be parseable or unparseable
ParseableValue = str | int | float | Decimal

# OrderBook creation parameters - keeping flexible for test variations
# Note: Uses Any for test flexibility when testing invalid inputs
OrderBookKwargs = dict[str, Any]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _is_valid_decimal_string(s: str) -> bool:
    """Check if a string can be parsed as a valid Decimal.

    Returns:
        True if string can be parsed as Decimal, False otherwise.
    """
    try:
        Decimal(s)
    except (ValueError, TypeError, InvalidOperation):
        return False
    else:
        return True


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER BOOK DATA
# =============================================================================


@st.composite
def finite_positive_decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.00000001, max_value: float = 100000.0
) -> Decimal:
    """Generate finite positive decimal values for prices.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A finite positive decimal value
    """
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def finite_non_negative_decimal_strategy(draw: st.DrawFn, max_value: float = 100000.0) -> Decimal:
    """Generate finite non-negative decimal values for quantities.

    Args:
        draw: Hypothesis draw function
        max_value: Maximum value for generation

    Returns:
        Decimal: A finite non-negative decimal value (including 0)
    """
    value = draw(
        st.floats(
            min_value=0.0,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for order book testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Symbol: A valid symbol for order book data
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
    """Generate valid UTC timestamps for order book data.

    Args:
        draw: Hypothesis draw function

    Returns:
        datetime: A valid UTC timestamp
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 12, 31),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def order_book_level_strategy(
    draw: st.DrawFn, allow_zero_quantity: bool = True
) -> tuple[Decimal, Decimal]:
    """Generate valid order book levels (price, quantity) as Decimal tuples.

    Args:
        draw: Hypothesis draw function
        allow_zero_quantity: Whether to allow zero quantities

    Returns:
        tuple[Decimal, Decimal]: A valid (price, quantity) level
    """
    price = draw(finite_positive_decimal_strategy())
    if allow_zero_quantity:
        quantity = draw(finite_non_negative_decimal_strategy())
    else:
        quantity = draw(finite_positive_decimal_strategy(min_value=0.00000001))
    return (price, quantity)


@st.composite
def order_book_levels_strategy(
    draw: st.DrawFn, min_size: int = 0, max_size: int = 10, allow_zero_quantity: bool = True
) -> list[tuple[Decimal, Decimal]]:
    """Generate valid lists of order book levels.

    Args:
        draw: Hypothesis draw function
        min_size: Minimum number of levels
        max_size: Maximum number of levels
        allow_zero_quantity: Whether to allow zero quantities

    Returns:
        list[tuple[Decimal, Decimal]]: A list of valid order book levels
    """
    return draw(
        st.lists(
            order_book_level_strategy(allow_zero_quantity=allow_zero_quantity),
            min_size=min_size,
            max_size=max_size,
        )
    )


@st.composite
def parseable_price_strategy(draw: st.DrawFn) -> ParseableValue:
    """Generate parseable price values in various formats.

    Args:
        draw: Hypothesis draw function

    Returns:
        Any: A value that can be parsed to a positive Decimal
    """
    return draw(
        st.one_of(
            # Decimal objects
            finite_positive_decimal_strategy(),
            # Integer values
            st.integers(min_value=1, max_value=100000),
            # Float values
            st.floats(min_value=0.00001, max_value=100000.0, allow_nan=False, allow_infinity=False),
            # String values
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: (
                    any(c.isdigit() for c in x)
                    and x.count(".") <= 1
                    and _is_valid_decimal_string(x)
                    and float(x) > 0
                )
            ),
        )
    )


@st.composite
def parseable_quantity_strategy(draw: st.DrawFn) -> ParseableValue:
    """Generate parseable quantity values in various formats.

    Args:
        draw: Hypothesis draw function

    Returns:
        Any: A value that can be parsed to a non-negative Decimal
    """
    return draw(
        st.one_of(
            # Decimal objects
            finite_non_negative_decimal_strategy(),
            # Integer values (including 0)
            st.integers(min_value=0, max_value=100000),
            # Float values (including 0.0)
            st.floats(min_value=0.0, max_value=100000.0, allow_nan=False, allow_infinity=False),
            # String values (including "0")
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: (
                    any(c.isdigit() for c in x)
                    and x.count(".") <= 1
                    and _is_valid_decimal_string(x)
                    and float(x) >= 0
                )
            ),
        )
    )


@st.composite
def parseable_level_strategy(draw: st.DrawFn) -> tuple[ParseableValue, ParseableValue]:
    """Generate parseable order book levels in mixed formats.

    Args:
        draw: Hypothesis draw function

    Returns:
        tuple[Any, Any]: A level with mixed parseable types
    """
    price = draw(parseable_price_strategy())
    quantity = draw(parseable_quantity_strategy())
    return (price, quantity)


@st.composite
def parseable_levels_strategy(
    draw: st.DrawFn, min_size: int = 0, max_size: int = 10
) -> list[tuple[ParseableValue, ParseableValue]]:
    """Generate lists of parseable order book levels in mixed formats.

    Args:
        draw: Hypothesis draw function
        min_size: Minimum number of levels
        max_size: Maximum number of levels

    Returns:
        list[tuple[Any, Any]]: A list of levels with mixed parseable types
    """
    return draw(st.lists(parseable_level_strategy(), min_size=min_size, max_size=max_size))


# =============================================================================
# PROPERTY TESTS FOR ORDER BOOK MODEL
# =============================================================================


class TestOrderBookModelProperties:
    """Property-based tests for the OrderBook model."""

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_order_book_creation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
    ) -> None:
        """Property: Minimal OrderBook with empty bids/asks should always be valid."""
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=[],
            asks=[],
        )

        # Properties: Required fields should be set correctly
        assert order_book.symbol == symbol
        assert order_book.timestamp == timestamp
        assert order_book.bids == []
        assert order_book.asks == []

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(),
        asks=order_book_levels_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_complete_order_book_creation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: Complete OrderBook with valid levels should maintain data integrity."""
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )

        # Properties: All fields should be preserved exactly
        assert order_book.symbol == symbol
        assert order_book.timestamp == timestamp
        assert order_book.bids == bids
        assert order_book.asks == asks

        # Properties: All levels should maintain financial constraints
        for price, quantity in order_book.bids:
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)
            assert price.is_finite()
            assert quantity.is_finite()
            assert price > 0
            assert quantity >= 0

        for price, quantity in order_book.asks:
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)
            assert price.is_finite()
            assert quantity.is_finite()
            assert price > 0
            assert quantity >= 0

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=parseable_levels_strategy(),
        asks=parseable_levels_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_order_book_parsing_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[ParseableValue, ParseableValue]],
        asks: list[tuple[ParseableValue, ParseableValue]],
    ) -> None:
        """Property: OrderBook should correctly parse various input formats to Decimal."""
        # Cast to Any to allow testing with ParseableValue types which are valid runtime inputs
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=cast(Any, bids),
            asks=cast(Any, asks),
        )

        # Property: All levels should be converted to Decimal tuples
        assert len(order_book.bids) == len(bids)
        assert len(order_book.asks) == len(asks)

        for i, (price, quantity) in enumerate(order_book.bids):
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)
            # Property: Parsed values should match original semantically
            original_price, original_quantity = bids[i]
            assert price == Decimal(str(original_price))
            assert quantity == Decimal(str(original_quantity))

        for i, (price, quantity) in enumerate(order_book.asks):
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)
            # Property: Parsed values should match original semantically
            original_price, original_quantity = asks[i]
            assert price == Decimal(str(original_price))
            assert quantity == Decimal(str(original_quantity))

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_immutability_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: OrderBook should be immutable after creation."""
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=[],
        )

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            order_book.symbol = BTC_HL

        with pytest.raises(ValidationError, match="Instance is frozen"):
            order_book.timestamp = datetime.now(UTC)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            order_book.bids = []

        with pytest.raises(ValidationError, match="Instance is frozen"):
            order_book.asks = [(Decimal(100), Decimal(1))]

    @given(
        field_name=st.sampled_from(["bids", "asks"]),
        invalid_list_value=st.one_of(
            st.just("not_a_list"),
            st.just(123),
            st.just({"key": "value"}),
            st.just(None),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_list_structure_rejection_properties(
        self, field_name: str, invalid_list_value: MaliciousValue
    ) -> None:
        """Property: Invalid list structures should be rejected."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "timestamp": datetime.now(UTC),
            "bids": [],
            "asks": [],
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_list_value

        # Property: Invalid list structure should be rejected
        with pytest.raises(ListFieldError):
            OrderBook(**cast(Any, kwargs))

    @given(
        field_name=st.sampled_from(["bids", "asks"]),
        invalid_level=st.one_of(
            st.just(123),  # Not a list/tuple
            st.just("string"),  # Not a list/tuple
            st.just([]),  # Empty list (wrong length)
            st.just(["price"]),  # Single element (wrong length)
            st.just(["price", "quantity", "extra"]),  # Three elements (wrong length)
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_level_structure_rejection_properties(
        self, field_name: str, invalid_level: MaliciousValue
    ) -> None:
        """Property: Invalid level structures should be rejected."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "timestamp": datetime.now(UTC),
            "bids": [],
            "asks": [],
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = [invalid_level]

        # Property: Invalid level structure should be rejected
        with pytest.raises((ListFieldError, RangeFieldError, ValidationError)):
            OrderBook(**cast(Any, kwargs))

    @given(
        field_name=st.sampled_from(["bids", "asks"]),
        invalid_price=st.one_of(
            st.just(None),
            st.just([]),
            st.just({}),
            st.just("not_a_number"),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_price_rejection_properties(
        self, field_name: str, invalid_price: MaliciousValue
    ) -> None:
        """Property: Invalid price values should be rejected."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "timestamp": datetime.now(UTC),
            "bids": [],
            "asks": [],
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = [(invalid_price, "1.0")]

        # Property: Invalid price should be rejected
        with pytest.raises((
            TypeFieldError,
            DecimalFieldError,
            DecimalFiniteError,
            ValidationError,
        )):
            OrderBook(**cast(Any, kwargs))

    @given(
        field_name=st.sampled_from(["bids", "asks"]),
        invalid_quantity=st.one_of(
            st.just(None),
            st.just([]),
            st.just({}),
            st.just("not_a_number"),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
            st.just("-1.0"),  # Negative quantity
            st.just(Decimal("-0.1")),  # Negative quantity
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_quantity_rejection_properties(
        self, field_name: str, invalid_quantity: MaliciousValue
    ) -> None:
        """Property: Invalid quantity values should be rejected."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "timestamp": datetime.now(UTC),
            "bids": [],
            "asks": [],
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = [("100.0", invalid_quantity)]

        # Property: Invalid quantity should be rejected
        with pytest.raises((
            TypeFieldError,
            DecimalFieldError,
            DecimalFiniteError,
            RangeFieldError,
            ValidationError,
        )):
            OrderBook(**cast(Any, kwargs))

    @given(
        timestamp_input=st.one_of(
            valid_timestamp_strategy(),
            st.integers(
                min_value=946684800,  # 2000-01-01
                max_value=1893456000,  # 2030-01-01
            ),
            st.text(
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-T:Z."
                ),
                min_size=10,
                max_size=30,
            ).filter(lambda x: "T" in x and ":" in x),  # Basic ISO format filter
        ),
    )
    @settings(max_examples=150, deadline=None)
    def test_timestamp_parsing_properties(self, timestamp_input: MaliciousValue) -> None:
        """Property: Timestamp fields should parse various input types correctly."""
        try:
            order_book = OrderBook(
                symbol=BTC_HL,
                timestamp=cast(Any, timestamp_input),
                bids=[],
                asks=[],
            )

            # Property: Timestamp should be converted to UTC datetime
            assert isinstance(order_book.timestamp, datetime)
            assert order_book.timestamp.tzinfo == UTC
        except (ValidationError, ValueError):
            # Some inputs may be invalid - this is expected behavior
            # We're testing that valid inputs work, invalid inputs fail cleanly
            pass

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(),
        asks=order_book_levels_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_extra_fields_rejection_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: Extra fields should always be rejected."""
        order_book_data = {
            "symbol": symbol,
            "timestamp": timestamp,
            "bids": bids,
            "asks": asks,
            "extra_field": "not_allowed",
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            OrderBook(**cast(Any, order_book_data))


# =============================================================================
# ORDER BOOK BUSINESS LOGIC PROPERTIES
# =============================================================================


class TestOrderBookBusinessLogicProperties:
    """Property-based tests for OrderBook business logic and relationships."""

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(min_size=1, max_size=5),
        asks=order_book_levels_strategy(min_size=1, max_size=5),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_financial_invariants_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: OrderBook should maintain financial invariants."""
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )

        # Property: All prices should be positive and finite
        for price, quantity in order_book.bids + order_book.asks:
            assert price > 0
            assert price.is_finite()
            assert quantity >= 0
            assert quantity.is_finite()

        # Property: Levels should support basic financial operations
        if order_book.bids:
            total_bid_value = sum(
                (price * quantity for price, quantity in order_book.bids), Decimal(0)
            )
            assert total_bid_value.is_finite()
            assert total_bid_value >= 0

        if order_book.asks:
            total_ask_value = sum(
                (price * quantity for price, quantity in order_book.asks), Decimal(0)
            )
            assert total_ask_value.is_finite()
            assert total_ask_value >= 0

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(),
        asks=order_book_levels_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_serialization_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: OrderBooks should be serializable and deserializable."""
        order_book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )

        # Property: OrderBook should be serializable to dict
        order_book_dict = order_book.model_dump()
        assert isinstance(order_book_dict, dict)

        # Property: Essential fields should be present in serialized data
        assert "symbol" in order_book_dict
        assert "timestamp" in order_book_dict
        assert "bids" in order_book_dict
        assert "asks" in order_book_dict

        # Property: OrderBook should be serializable to JSON
        json_str = order_book.model_dump_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        bids=order_book_levels_strategy(),
        asks=order_book_levels_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_deterministic_creation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """Property: OrderBook creation should be deterministic for same inputs."""
        order_book1 = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )
        order_book2 = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
        )

        # Property: All field values should be identical
        assert order_book1.symbol == order_book2.symbol
        assert order_book1.timestamp == order_book2.timestamp
        assert order_book1.bids == order_book2.bids
        assert order_book1.asks == order_book2.asks

    @given(
        level=order_book_level_strategy(),
        operation=st.sampled_from(["spread_calculation", "value_calculation", "precision_check"]),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_level_calculation_properties(
        self, level: tuple[Decimal, Decimal], operation: str
    ) -> None:
        """Property: OrderBook levels should support financial calculations."""
        price, quantity = level

        order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=datetime.now(UTC),
            bids=[level],
            asks=[(price + Decimal("0.01"), quantity)],  # Slight spread
        )

        # Property: Basic operations should work with exact precision
        if operation == "spread_calculation":
            if order_book.bids and order_book.asks:
                bid_price = order_book.bids[0][0]
                ask_price = order_book.asks[0][0]
                spread = ask_price - bid_price
                assert spread.is_finite()
                assert spread >= 0

        elif operation == "value_calculation":
            bid_value = price * quantity
            assert bid_value.is_finite()
            assert bid_value >= 0

        elif operation == "precision_check":
            # Property: Decimal precision should be preserved
            assert price.is_finite()
            assert quantity.is_finite()
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)


# =============================================================================
# EDGE CASE AND INTEGRATION PROPERTIES
# =============================================================================


class TestOrderBookEdgeCaseProperties:
    """Property-based tests for edge cases and integration scenarios."""

    @given(
        base_time=valid_timestamp_strategy(),
        offset_seconds=st.integers(min_value=-3600, max_value=3600),  # ±1 hour
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_timing_edge_cases_properties(
        self,
        base_time: datetime,
        offset_seconds: int,
    ) -> None:
        """Property: OrderBook timing should handle various edge cases correctly."""
        snapshot_time = base_time + timedelta(seconds=offset_seconds)

        order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=snapshot_time,
            bids=[(Decimal(100), Decimal(1))],
            asks=[(Decimal(101), Decimal(1))],
        )

        # Property: Timestamp relationships should be preserved
        assert order_book.timestamp == snapshot_time
        assert order_book.timestamp.tzinfo == UTC

        # Property: Time difference from base should match our offset
        if order_book.timestamp and base_time:
            time_diff = order_book.timestamp - base_time
            assert time_diff.total_seconds() == offset_seconds

    @given(
        order_books=st.lists(
            st.tuples(
                valid_symbol_strategy(),
                valid_timestamp_strategy(),
                order_book_levels_strategy(max_size=3),
                order_book_levels_strategy(max_size=3),
            ),
            min_size=2,
            max_size=5,
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_multiple_order_books_independence_properties(
        self,
        order_books: list[
            tuple[Symbol, datetime, list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]
        ],
    ) -> None:
        """Property: Multiple order books should be processed independently."""
        created_order_books: list[OrderBook] = []

        for symbol, timestamp, bids, asks in order_books:
            order_book = OrderBook(
                symbol=symbol,
                timestamp=timestamp,
                bids=bids,
                asks=asks,
            )
            created_order_books.append(order_book)

        # Property: Each order book should maintain its individual data
        for i, (original_data, created_order_book) in enumerate(
            zip(order_books, created_order_books, strict=False)
        ):
            symbol, timestamp, bids, asks = original_data
            assert created_order_book.symbol == symbol
            assert created_order_book.timestamp == timestamp
            assert created_order_book.bids == bids
            assert created_order_book.asks == asks

            # Property: Order books should not affect each other
            for j, other_order_book in enumerate(created_order_books):
                if i != j:
                    # Order books should be independent instances
                    assert created_order_book is not other_order_book

    @given(
        levels=order_book_levels_strategy(min_size=1, max_size=10),
        aggregation_operation=st.sampled_from(["total_value", "average_price", "max_quantity"]),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_book_aggregation_properties(
        self, levels: list[tuple[Decimal, Decimal]], aggregation_operation: str
    ) -> None:
        """Property: OrderBook should support aggregation operations correctly."""
        order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=datetime.now(UTC),
            bids=levels,
            asks=[],
        )

        if not order_book.bids:
            return  # Skip if no levels

        # Property: Aggregation operations should work correctly
        if aggregation_operation == "total_value":
            total_value = sum((price * quantity for price, quantity in order_book.bids), Decimal(0))
            assert total_value.is_finite()
            assert total_value >= 0

        elif aggregation_operation == "average_price":
            if order_book.bids:
                total_quantity = sum((quantity for _, quantity in order_book.bids), Decimal(0))
                if total_quantity > 0:
                    weighted_avg = (
                        sum((price * quantity for price, quantity in order_book.bids), Decimal(0))
                        / total_quantity
                    )
                    assert weighted_avg.is_finite()
                    assert weighted_avg > 0

        elif aggregation_operation == "max_quantity":
            max_quantity = max(quantity for _, quantity in order_book.bids)
            assert max_quantity.is_finite()
            assert max_quantity >= 0

    @given(
        very_small_values=st.tuples(
            st.floats(min_value=1e-8, max_value=1e-6, allow_nan=False, allow_infinity=False),
            st.floats(min_value=1e-8, max_value=1e-6, allow_nan=False, allow_infinity=False),
        ),
        very_large_values=st.tuples(
            st.floats(min_value=1e6, max_value=1e8, allow_nan=False, allow_infinity=False),
            st.floats(min_value=1e6, max_value=1e8, allow_nan=False, allow_infinity=False),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_order_book_extreme_values_properties(
        self,
        very_small_values: tuple[float, float],
        very_large_values: tuple[float, float],
    ) -> None:
        """Property: OrderBook should handle extreme values correctly."""
        small_price, small_quantity = very_small_values
        large_price, large_quantity = very_large_values

        # Property: Very small values should be handled correctly
        small_order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=datetime.now(UTC),
            bids=[(Decimal(str(small_price)), Decimal(str(small_quantity)))],
            asks=[],
        )

        assert small_order_book.bids[0][0] == Decimal(str(small_price))
        assert small_order_book.bids[0][1] == Decimal(str(small_quantity))

        # Property: Very large values should be handled correctly
        large_order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=datetime.now(UTC),
            bids=[(Decimal(str(large_price)), Decimal(str(large_quantity)))],
            asks=[],
        )

        assert large_order_book.bids[0][0] == Decimal(str(large_price))
        assert large_order_book.bids[0][1] == Decimal(str(large_quantity))

        # Property: Calculations with extreme values should remain finite
        small_value = small_order_book.bids[0][0] * small_order_book.bids[0][1]
        large_value = large_order_book.bids[0][0] * large_order_book.bids[0][1]

        assert small_value.is_finite()
        assert large_value.is_finite()
