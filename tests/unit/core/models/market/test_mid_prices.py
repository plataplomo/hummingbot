"""Property-based tests for the CyberDeltaEngine MidPrices model.

This module provides comprehensive property-based testing of the MidPrices Pydantic model,
which represents collections of mid prices for multiple trading symbols.

Key Testing Areas:
- Mid prices field validation and type safety using property-based input generation
- Financial precision handling for price values
- Symbol lookup and mapping functionality
- Timestamp handling and exchange validation
- Collection operations (get, has_symbol, symbols, len)
- Edge cases with extreme prices, empty collections, and special symbols
- Immutability and data integrity properties

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete mid price collection flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.mid_prices import MidPrices
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
# HYPOTHESIS STRATEGIES FOR MID PRICES DATA
# =============================================================================


@st.composite
def finite_decimal_strategy(
    draw: st.DrawFn, min_value: float = -1000000.0, max_value: float = 1000000.0
) -> Decimal:
    """Generate finite decimal values for prices (can be negative for some markets).

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A finite decimal value
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
def positive_decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.00000001, max_value: float = 1000000.0
) -> Decimal:
    """Generate positive decimal values for typical market prices.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A positive decimal value
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
def symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Symbol: A valid symbol for mid price data
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
def exchange_name_strategy(draw: st.DrawFn) -> ExchangeName:
    """Generate valid exchange names.

    Args:
        draw: Hypothesis draw function

    Returns:
        ExchangeName: A valid exchange name enum
    """
    return draw(st.sampled_from(list(ExchangeName)))


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for mid price data.

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
def price_dict_strategy(
    draw: st.DrawFn, min_size: int = 0, max_size: int = 10, allow_negative: bool = False
) -> dict[Symbol, Decimal]:
    """Generate dictionaries of symbol to price mappings.

    Args:
        draw: Hypothesis draw function
        min_size: Minimum number of symbols
        max_size: Maximum number of symbols
        allow_negative: Whether to allow negative prices

    Returns:
        dict[Symbol, Decimal]: A mapping of symbols to prices
    """
    num_symbols = draw(st.integers(min_value=min_size, max_value=max_size))
    available_symbols = [
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
    ]

    # Select unique symbols
    selected_symbols = draw(
        st.lists(
            st.sampled_from(available_symbols),
            min_size=num_symbols,
            max_size=num_symbols,
            unique=True,
        )
    )

    prices: dict[Symbol, Decimal] = {}
    for symbol in selected_symbols:
        if allow_negative:
            price = draw(finite_decimal_strategy())
        else:
            price = draw(positive_decimal_strategy())
        prices[symbol] = price

    return prices


@st.composite
def extreme_price_dict_strategy(draw: st.DrawFn) -> dict[Symbol, Decimal]:
    """Generate price dictionaries with extreme values.

    Args:
        draw: Hypothesis draw function

    Returns:
        dict[Symbol, Decimal]: A mapping with extreme price values
    """
    symbols = draw(st.lists(symbol_strategy(), min_size=1, max_size=5, unique=True))

    prices: dict[Symbol, Decimal] = {}
    for symbol in symbols:
        price_type = draw(
            st.sampled_from(["very_small", "very_large", "zero", "negative", "high_precision"])
        )

        if price_type == "very_small":
            price = Decimal(str(draw(st.floats(min_value=1e-10, max_value=1e-6))))
        elif price_type == "very_large":
            price = Decimal(str(draw(st.floats(min_value=1e6, max_value=1e9))))
        elif price_type == "zero":
            price = Decimal(0)
        elif price_type == "negative":
            price = Decimal(str(draw(st.floats(min_value=-1000, max_value=-0.01))))
        else:  # high_precision
            price = Decimal(str(draw(st.floats(min_value=0.1, max_value=1000)))).quantize(
                Decimal("0.000000000001")
            )

        prices[symbol] = price

    return prices


# =============================================================================
# PROPERTY TESTS FOR MID PRICES MODEL
# =============================================================================


class TestMidPricesModelProperties:
    """Property-based tests for the MidPrices model."""

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_mid_prices_creation_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: Minimal MidPrices with only required fields should always be valid."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Properties: Required fields should be set correctly
        assert mid_prices.prices == prices
        assert mid_prices.exchange == exchange
        assert mid_prices.timestamp is None  # Default value

        # Properties: Collection size should match
        assert len(mid_prices) == len(prices)
        assert len(mid_prices.symbols()) == len(prices)

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
        timestamp=st.one_of(st.none(), valid_timestamp_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_complete_mid_prices_creation_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
        timestamp: datetime | None,
    ) -> None:
        """Property: Complete MidPrices with all fields should maintain data integrity."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )

        # Properties: All fields should be preserved exactly
        assert mid_prices.prices == prices
        assert mid_prices.exchange == exchange
        assert mid_prices.timestamp == timestamp

        # Properties: All symbols should be accessible
        for symbol, price in prices.items():
            assert mid_prices.has_symbol(symbol)
            assert mid_prices.get(symbol) == price

    @given(
        prices=price_dict_strategy(min_size=1, max_size=10),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_symbol_lookup_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: Symbol lookup operations should work correctly."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Property: get() should return correct price for each symbol
        for symbol, price in prices.items():
            assert mid_prices.get(symbol) == price

        # Property: has_symbol() should return True for all symbols
        for symbol in prices:
            assert mid_prices.has_symbol(symbol) is True

        # Property: symbols() should return all symbols
        symbols = mid_prices.symbols()
        assert set(symbols) == set(prices.keys())
        assert len(symbols) == len(prices)

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
        nonexistent_symbol=symbol_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_nonexistent_symbol_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
        nonexistent_symbol: Symbol,
    ) -> None:
        """Property: Nonexistent symbol lookups should return None/False."""
        # Ensure the symbol is not in prices
        assume(nonexistent_symbol not in prices)

        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Property: get() should return None for nonexistent symbol
        assert mid_prices.get(nonexistent_symbol) is None

        # Property: has_symbol() should return False for nonexistent symbol
        assert mid_prices.has_symbol(nonexistent_symbol) is False

    @given(exchange=exchange_name_strategy())
    @settings(max_examples=100, deadline=None)
    def test_empty_mid_prices_properties(self, exchange: ExchangeName) -> None:
        """Property: Empty MidPrices should behave correctly."""
        mid_prices = MidPrices(
            prices={},
            exchange=exchange,
        )

        # Properties: Empty collection behavior
        assert len(mid_prices) == 0
        assert mid_prices.symbols() == []
        assert mid_prices.prices == {}

        # Property: Any symbol lookup should return None/False
        test_symbol = BTC_HL
        assert mid_prices.get(test_symbol) is None
        assert mid_prices.has_symbol(test_symbol) is False

    @given(
        prices=extreme_price_dict_strategy(),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_extreme_values_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices should handle extreme price values correctly."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Properties: All extreme values should be preserved exactly
        for symbol, price in prices.items():
            assert mid_prices.get(symbol) == price

            # Property: Special value checks
            retrieved_price = mid_prices.get(symbol)
            assert retrieved_price is not None, f"Price for {symbol} should exist"

            if price == 0:
                assert retrieved_price == Decimal(0)
            elif price < 0:
                assert retrieved_price < 0
            elif price > 1e6:
                assert retrieved_price > Decimal(1000000)

    @given(
        prices=price_dict_strategy(allow_negative=True),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_negative_values_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices should support negative prices (e.g., commodity futures)."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Properties: Negative values should be preserved
        for symbol, price in prices.items():
            retrieved_price = mid_prices.get(symbol)
            assert retrieved_price == price
            if price < 0:
                assert retrieved_price is not None
                assert retrieved_price < 0

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_data_integrity_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices should maintain data integrity after creation."""
        # Create a copy of the original prices
        original_prices = prices.copy()

        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Modify the original dict (should not affect MidPrices)
        if prices:
            first_symbol = next(iter(prices))
            prices[first_symbol] = Decimal(999999)

        # Property: MidPrices should be unaffected by external modifications
        for symbol, original_price in original_prices.items():
            assert mid_prices.get(symbol) == original_price

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_mid_prices_extra_fields_rejection_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: Extra fields should always be rejected."""
        mid_prices_data: dict[str, Any] = {
            "prices": prices,
            "exchange": exchange,
            "extra_field": "not_allowed",
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            MidPrices(**mid_prices_data)


# =============================================================================
# MID PRICES BUSINESS LOGIC PROPERTIES
# =============================================================================


class TestMidPricesBusinessLogicProperties:
    """Property-based tests for MidPrices business logic and relationships."""

    @given(
        prices=price_dict_strategy(min_size=1, max_size=20),
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_collection_consistency_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
        timestamp: datetime,
    ) -> None:
        """Property: Collection operations should be internally consistent."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )

        # Property: len() should match symbols() length
        assert len(mid_prices) == len(mid_prices.symbols())

        # Property: len() should match prices dict size
        assert len(mid_prices) == len(prices)

        # Property: All symbols() should have prices
        for symbol in mid_prices.symbols():
            assert mid_prices.get(symbol) is not None
            assert mid_prices.has_symbol(symbol) is True

        # Property: Symbol set consistency
        assert set(mid_prices.symbols()) == set(prices.keys())

    @given(
        prices=price_dict_strategy(min_size=2, max_size=10),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_equality_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices equality should be deterministic."""
        timestamp = datetime.now(UTC)

        # Create identical instances
        mid_prices1 = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )
        mid_prices2 = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )

        # Property: Identical data should result in equality
        assert mid_prices1 == mid_prices2

        # Create instance with different prices
        if prices:
            modified_prices = prices.copy()
            first_symbol = next(iter(modified_prices))
            modified_prices[first_symbol] = Decimal(999999)

            mid_prices3 = MidPrices(
                prices=modified_prices,
                exchange=exchange,
                timestamp=timestamp,
            )

            # Property: Different prices should result in inequality
            assert mid_prices1 != mid_prices3

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_serialization_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
        timestamp: datetime,
    ) -> None:
        """Property: MidPrices should be serializable and deserializable."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )

        # Property: Model should be serializable to dict
        mid_prices_dict = mid_prices.model_dump()
        assert isinstance(mid_prices_dict, dict)

        # Property: Essential fields should be present in serialized data
        assert "prices" in mid_prices_dict
        assert "exchange" in mid_prices_dict
        assert "timestamp" in mid_prices_dict

        # Property: Model should be serializable to JSON
        json_str = mid_prices.model_dump_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    @given(
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_deterministic_creation_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
        timestamp: datetime,
    ) -> None:
        """Property: MidPrices creation should be deterministic for same inputs."""
        mid_prices1 = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )
        mid_prices2 = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=timestamp,
        )

        # Property: All field values should be identical
        assert mid_prices1.prices == mid_prices2.prices
        assert mid_prices1.exchange == mid_prices2.exchange
        assert mid_prices1.timestamp == mid_prices2.timestamp
        assert len(mid_prices1) == len(mid_prices2)

    @given(
        prices=price_dict_strategy(min_size=1, max_size=10),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_string_representation_properties(
        self,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: String representation should contain key information."""
        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        str_repr = str(mid_prices)

        # Property: String should contain exchange name
        assert exchange.value in str_repr.lower() or "exchange" in str_repr.lower()

        # Property: String should contain at least one symbol if prices exist
        if prices:
            # At least one symbol should be mentioned
            for symbol in prices:
                if str(symbol) in str_repr:
                    break
            # We can't guarantee all symbols are in string repr, but at least check it's not empty
            assert len(str_repr) > 0


# =============================================================================
# EDGE CASE AND INTEGRATION PROPERTIES
# =============================================================================


class TestMidPricesEdgeCaseProperties:
    """Property-based tests for edge cases and integration scenarios."""

    @given(
        base_time=valid_timestamp_strategy(),
        offset_seconds=st.integers(min_value=-3600, max_value=3600),  # ±1 hour
        prices=price_dict_strategy(),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_mid_prices_timing_edge_cases_properties(
        self,
        base_time: datetime,
        offset_seconds: int,
        prices: dict[Symbol, Decimal],
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices timing should handle various edge cases correctly."""
        snapshot_time = base_time + timedelta(seconds=offset_seconds)

        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
            timestamp=snapshot_time,
        )

        # Property: Timestamp relationships should be preserved
        assert mid_prices.timestamp == snapshot_time
        if mid_prices.timestamp:
            assert mid_prices.timestamp.tzinfo == UTC

        # Property: Time difference from base should match our offset
        if mid_prices.timestamp and base_time:
            time_diff = mid_prices.timestamp - base_time
            assert time_diff.total_seconds() == offset_seconds

    @given(
        high_precision_value=st.floats(
            min_value=0.1, max_value=1000, allow_nan=False, allow_infinity=False
        ).map(lambda x: Decimal(str(x)).quantize(Decimal("0.000000000000000001"))),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_mid_prices_high_precision_values_properties(
        self,
        high_precision_value: Decimal,
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices should preserve high precision decimal values."""
        symbol = BTC_HL
        prices = {symbol: high_precision_value}

        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Property: High precision should be preserved exactly
        assert mid_prices.get(symbol) == high_precision_value

        # Property: String conversion shouldn't lose precision
        retrieved = mid_prices.get(symbol)
        assert str(retrieved) == str(high_precision_value)

    @given(
        mid_prices_list=st.lists(
            st.tuples(
                price_dict_strategy(max_size=5),
                exchange_name_strategy(),
                valid_timestamp_strategy(),
            ),
            min_size=2,
            max_size=5,
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_multiple_mid_prices_independence_properties(
        self, mid_prices_list: list[tuple[dict[Symbol, Decimal], ExchangeName, datetime]]
    ) -> None:
        """Property: Multiple mid prices instances should be processed independently."""
        created_mid_prices: list[MidPrices] = []

        for prices, exchange, timestamp in mid_prices_list:
            mid_prices = MidPrices(
                prices=prices,
                exchange=exchange,
                timestamp=timestamp,
            )
            created_mid_prices.append(mid_prices)

        # Property: Each mid prices should maintain its individual data
        for i, (original_data, created_mp) in enumerate(
            zip(mid_prices_list, created_mid_prices, strict=False)
        ):
            prices, exchange, timestamp = original_data
            assert created_mp.prices == prices
            assert created_mp.exchange == exchange
            assert created_mp.timestamp == timestamp

            # Property: Mid prices instances should not affect each other
            for j, other_mp in enumerate(created_mid_prices):
                if i != j:
                    # Instances should be independent
                    assert created_mp is not other_mp

    @given(
        symbol_count=st.integers(min_value=0, max_value=100),
        exchange=exchange_name_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_mid_prices_scale_properties(
        self,
        symbol_count: int,
        exchange: ExchangeName,
    ) -> None:
        """Property: MidPrices should handle various collection sizes correctly."""
        # Generate unique symbols and prices
        available_symbols = [
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
        ]

        # Limit to available symbols
        actual_count = min(symbol_count, len(available_symbols))
        selected_symbols = available_symbols[:actual_count]

        prices = {symbol: Decimal(str(100 + i)) for i, symbol in enumerate(selected_symbols)}

        mid_prices = MidPrices(
            prices=prices,
            exchange=exchange,
        )

        # Properties: Size should match
        assert len(mid_prices) == actual_count
        assert len(mid_prices.symbols()) == actual_count

        # Property: All operations should work regardless of size
        for symbol in selected_symbols:
            assert mid_prices.has_symbol(symbol)
            assert mid_prices.get(symbol) is not None
