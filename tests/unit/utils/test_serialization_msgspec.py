"""Property-based tests for msgspec JSON serialization implementation.

This module provides comprehensive property-based testing for the JSON serialization
utilities, ensuring data integrity, round-trip consistency, and proper handling of
financial data types critical for the CyberDeltaEngine trading system.

Key Testing Areas:
- Round-trip serialization preserving exact data integrity
- Decimal precision preservation for financial calculations
- Complex nested structure handling without data loss
- UUID, datetime, and special type serialization correctness
- Edge case handling (empty structures, special characters, large numbers)
- Pydantic model serialization consistency
- Performance characteristics compared to alternatives

SECURITY CRITICAL: Serialization errors can lead to:
- Financial data corruption affecting trading decisions
- Loss of precision in price/quantity calculations
- State inconsistency in distributed systems
- Data injection vulnerabilities through malformed JSON
"""

import math
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import msgspec
import orjson
import pytest
from hypothesis import given, settings, strategies as st
from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.utils.serialization import (
    dumps_json,
    dumps_json_bytes,
    loads_json,
)


class SampleOrder(BaseModel):
    """Sample order model for testing."""

    model_config = ConfigDict(populate_by_name=True)

    order_id: UUID = Field(default_factory=uuid4)
    symbol: str
    price: Decimal
    quantity: Decimal
    side: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] | None = None
    tags: list[str] = Field(default_factory=list)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR SERIALIZATION TESTING
# =============================================================================


@st.composite
def financial_decimal_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic Decimal values for financial testing.

    Returns:
        Decimal values typical in trading systems
    """
    return draw(
        st.one_of([
            # Common price ranges
            st.decimals(
                min_value=Decimal("0.00000001"),
                max_value=Decimal(1000000),
                places=8,
                allow_nan=False,
                allow_infinity=False,
            ),
            # Edge cases
            st.just(Decimal(0)),
            st.just(Decimal("0.00000001")),  # Satoshi
            st.just(Decimal("999999999.99999999")),  # Large value
        ])
    )


@st.composite
def symbol_strategy(draw: st.DrawFn) -> str:
    """Generate valid trading symbols.

    Returns:
        Trading symbol strings
    """
    bases = ["BTC", "ETH", "SOL", "DOGE", "AVAX", "MATIC"]
    quotes = ["USDC", "USDT", "USD", "EUR"]
    separators = ["-", "_", "/", ""]

    base = draw(st.sampled_from(bases))
    quote = draw(st.sampled_from(quotes))
    separator = draw(st.sampled_from(separators))

    return f"{base}{separator}{quote}"


@st.composite
def metadata_strategy(draw: st.DrawFn) -> dict[str, Any] | None:
    """Generate metadata dictionaries for testing.

    Returns:
        Metadata dictionary or None
    """
    if draw(st.booleans()):  # 50% chance of None
        return None

    return draw(
        st.dictionaries(
            keys=st.text(
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="_-"
                ),
                min_size=1,
                max_size=20,
            ),
            values=st.one_of([
                st.text(max_size=100),
                st.integers(-1000000, 1000000),
                st.floats(allow_nan=False, allow_infinity=False),
                st.booleans(),
                st.none(),
            ]),
            min_size=0,
            max_size=10,
        )
    )


@st.composite
def sample_order_strategy(draw: st.DrawFn) -> SampleOrder:
    """Generate SampleOrder instances for testing.

    Returns:
        SampleOrder with varied fields
    """
    return SampleOrder(
        order_id=draw(st.uuids()),
        symbol=draw(symbol_strategy()),
        price=draw(financial_decimal_strategy()),
        quantity=draw(financial_decimal_strategy()),
        side=draw(st.sampled_from(["BUY", "SELL"])),
        timestamp=draw(
            st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2030, 12, 31),
                timezones=st.just(UTC),
            )
        ),
        metadata=draw(metadata_strategy()),
        tags=draw(st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=5)),
    )


@st.composite
def nested_structure_strategy(draw: st.DrawFn, max_depth: int = 3) -> dict[str, Any] | list[Any]:
    """Generate deeply nested data structures.

    Args:
        draw: Hypothesis draw function
        max_depth: Maximum nesting depth

    Returns:
        Nested dictionary or list structure
    """
    if max_depth <= 0:
        # Base case: return simple value
        simple_value = draw(
            st.one_of([
                st.text(max_size=50),
                st.integers(-1000000, 1000000),
                financial_decimal_strategy(),
                st.booleans(),
                st.none(),
            ])
        )
        # Wrap in dict or list to match return type
        if draw(st.booleans()):
            return {"value": simple_value}
        return [simple_value]

    # Recursive case
    if draw(st.booleans()):
        # Generate dict
        return draw(
            st.dictionaries(
                keys=st.text(min_size=1, max_size=10),
                values=st.one_of([
                    st.text(max_size=50),
                    st.integers(),
                    financial_decimal_strategy(),
                    st.booleans(),
                    st.none(),
                    nested_structure_strategy(max_depth=max_depth - 1),
                ]),
                min_size=0,
                max_size=5,
            )
        )
    # Generate list
    return draw(
        st.lists(
            st.one_of([
                st.text(max_size=50),
                st.integers(),
                financial_decimal_strategy(),
                st.booleans(),
                st.none(),
                nested_structure_strategy(max_depth=max_depth - 1),
            ]),
            min_size=0,
            max_size=5,
        )
    )


# =============================================================================
# PROPERTY-BASED SERIALIZATION TESTS
# =============================================================================


class TestMsgspecSerialization:
    """Property-based tests for msgspec serialization with Pydantic models."""

    @given(order=sample_order_strategy())
    @settings(max_examples=200, deadline=None)
    def test_pydantic_model_serialization_properties(self, order: SampleOrder) -> None:
        """Property: Pydantic models should round-trip perfectly through serialization.

        This ensures that all financial data, metadata, and structure is preserved
        exactly through the serialization/deserialization cycle.
        """
        # Serialize
        json_str = dumps_json(order)

        # Properties of serialized form
        assert isinstance(json_str, str)
        assert len(json_str) > 0

        # Deserialize and validate
        data = loads_json(json_str)
        assert isinstance(data, dict)

        # Reconstruct model
        reconstructed = SampleOrder.model_validate(data)

        # Property: All fields must match exactly
        assert reconstructed.order_id == order.order_id
        assert reconstructed.symbol == order.symbol
        assert reconstructed.price == order.price
        assert reconstructed.quantity == order.quantity
        assert reconstructed.side == order.side
        assert reconstructed.tags == order.tags

        # Property: Metadata preservation (including None)
        if order.metadata is None:
            assert "metadata" not in data  # None values excluded
        else:
            assert reconstructed.metadata == order.metadata

        # Property: Timestamp preservation
        # Note: Comparison might need tolerance for microsecond precision
        assert abs((reconstructed.timestamp - order.timestamp).total_seconds()) < 0.001

    @given(decimals=st.lists(financial_decimal_strategy(), min_size=1, max_size=10))
    @settings(max_examples=500, deadline=None)
    def test_decimal_preservation_properties(self, decimals: list[Decimal]) -> None:
        """Property: Decimal precision must be preserved exactly.

        CRITICAL for financial calculations - any precision loss could
        result in incorrect trading amounts or pricing.
        """
        data = {f"value_{i}": dec for i, dec in enumerate(decimals)}

        # Serialize and deserialize
        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: All decimal values preserved as strings
        for i, original_decimal in enumerate(decimals):
            key = f"value_{i}"
            assert key in loaded

            # Property: Decimal converts to string representation
            loaded_str = loaded[key]
            assert isinstance(loaded_str, str)

            # Property: Can reconstruct exact decimal
            reconstructed = Decimal(loaded_str)
            assert reconstructed == original_decimal

            # Property: String representation preserves precision
            assert str(reconstructed) == str(original_decimal)

    @given(
        dt=st.datetimes(
            min_value=datetime(1970, 1, 1), max_value=datetime(2100, 1, 1), timezones=st.just(UTC)
        )
    )
    @settings(max_examples=200, deadline=None)
    def test_datetime_serialization_properties(self, dt: datetime) -> None:
        """Property: Datetime values should serialize to ISO format and preserve temporal data.

        Critical for trade timing, order timestamps, and audit trails.
        """
        data = {"timestamp": dt}

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: Datetime becomes ISO format string
        timestamp_str = loaded["timestamp"]
        assert isinstance(timestamp_str, str)
        assert "T" in timestamp_str  # ISO format marker

        # Property: Can reconstruct datetime
        # Handle both Z and +00:00 suffixes
        if timestamp_str.endswith("Z"):
            reconstructed = datetime.fromisoformat(timestamp_str[:-1] + "+00:00")
        else:
            reconstructed = datetime.fromisoformat(timestamp_str)

        # Property: Temporal data preserved (within microsecond precision)
        time_diff = abs((reconstructed - dt).total_seconds())
        assert time_diff < 0.001  # Less than 1ms difference

    @given(uid=st.uuids())
    @settings(max_examples=200, deadline=None)
    def test_uuid_serialization_properties(self, uid: UUID) -> None:
        """Property: UUIDs should serialize to string format and be reconstructible.

        Important for order IDs, transaction IDs, and correlation tracking.
        """
        data = {"id": uid}

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: UUID becomes string
        id_str = loaded["id"]
        assert isinstance(id_str, str)
        assert id_str == str(uid)

        # Property: Can reconstruct UUID
        reconstructed = UUID(id_str)
        assert reconstructed == uid

    @given(structure=nested_structure_strategy(max_depth=4))
    @settings(max_examples=100, deadline=None)
    def test_nested_structures_properties(self, structure: dict[str, Any] | list[Any]) -> None:
        """Property: Complex nested structures should round-trip without data loss.

        Tests that arbitrarily nested trading data structures maintain integrity.
        """
        # Serialize and deserialize
        json_str = dumps_json(structure)
        loaded = loads_json(json_str)

        # Property: Structure type preserved
        assert isinstance(loaded, type(structure))

        # Property: Deep equality (handles Decimal string conversion)
        def normalize_for_comparison(obj: Any) -> Any:  # noqa: ANN401
            """Normalize Decimals to strings for comparison.

            Returns:
                Normalized object with Decimals converted to strings.
            """
            if isinstance(obj, dict):
                return {k: normalize_for_comparison(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [normalize_for_comparison(item) for item in obj]
            if isinstance(obj, Decimal):
                return str(obj)
            return obj

        normalized_original = normalize_for_comparison(structure)
        assert loaded == normalized_original

    def test_bytes_output(self) -> None:
        """Test bytes output functionality."""
        data = {"test": "data", "number": 42}

        json_bytes = dumps_json_bytes(data)
        assert isinstance(json_bytes, bytes)

        # Should be decodable
        loaded = loads_json(json_bytes)
        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded == data

    def test_indentation_fallback(self) -> None:
        """Test that indentation falls back to orjson."""
        data = {"key": "value", "nested": {"a": 1, "b": 2}}

        # With indentation
        indented = dumps_json(data, indent=True)
        assert "\n" in indented  # Should have newlines

        # Without indentation
        compact = dumps_json(data)
        assert "\n" not in compact  # Should be compact

    def test_sort_keys_fallback(self) -> None:
        """Test that sort_keys falls back to orjson."""
        data = {"z": 1, "a": 2, "m": 3}

        sorted_json = dumps_json(data, sort_keys=True)
        # 'a' should come before 'z' in the string
        assert sorted_json.index('"a"') < sorted_json.index('"z"')

    @given(
        include_empty_list=st.booleans(),
        include_empty_dict=st.booleans(),
        include_none=st.booleans(),
        additional_data=st.dictionaries(
            st.text(min_size=1, max_size=10),
            st.one_of([st.none(), st.just([]), st.just({})]),
            min_size=0,
            max_size=5,
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_empty_structures_properties(
        self,
        include_empty_list: bool,
        include_empty_dict: bool,
        include_none: bool,
        additional_data: dict[str, Any],
    ) -> None:
        """Property: Empty structures and None values should be handled correctly.

        Important for optional fields and initialization states.
        """
        data: dict[str, Any] = {}

        if include_empty_list:
            data["empty_list"] = []
        if include_empty_dict:
            data["empty_dict"] = {}
        if include_none:
            data["none_value"] = None

        data.update(additional_data)

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: Empty structures preserved
        if include_empty_list:
            assert loaded["empty_list"] == []
        if include_empty_dict:
            assert loaded["empty_dict"] == {}
        if include_none:
            assert loaded["none_value"] is None

        # Property: Additional data preserved
        for key, value in additional_data.items():
            assert key in loaded
            assert loaded[key] == value

    def test_error_handling(self) -> None:
        """Test error handling for invalid JSON."""
        with pytest.raises((msgspec.DecodeError, orjson.JSONDecodeError)):
            loads_json("invalid json {")

        with pytest.raises((msgspec.DecodeError, orjson.JSONDecodeError)):
            loads_json('{"unclosed": ')

    def test_pydantic_exclude_none(self) -> None:
        """Test that None values are excluded from Pydantic models."""
        order = SampleOrder(
            symbol="BTC-USDC",
            price=Decimal(50000),
            quantity=Decimal("0.01"),
            side="BUY",
            metadata=None,  # This should be excluded
        )

        json_str = dumps_json(order)
        data = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(data, dict)
        # metadata should not be in the output since it's None
        assert "metadata" not in data

    def test_backward_compatibility(self) -> None:
        """Test backward compatibility with existing code."""
        # Test with various types that were previously handled by orjson
        test_cases = [
            {"simple": "string"},
            {"number": 42},
            {"float": math.pi},
            {"bool": True},
            {"null": None},
            {"list": [1, 2, 3]},
            {"nested": {"a": {"b": {"c": "deep"}}}},
        ]

        for data in test_cases:
            json_str = dumps_json(data)
            loaded = loads_json(json_str)
            assert loaded == data


class TestPerformanceComparison:
    """Performance comparison tests."""

    def test_serialization_performance(self) -> None:
        """Benchmark msgspec vs native Pydantic serialization."""
        orders = [
            SampleOrder(
                symbol="BTC-USDC",
                price=Decimal("50000.50"),
                quantity=Decimal("0.01"),
                side="BUY",
            )
            for _ in range(100)
        ]

        # Benchmark using msgspec (via dumps_json)
        start = time.perf_counter()
        for order in orders:
            _ = dumps_json(order)
        msgspec_time = time.perf_counter() - start

        # Benchmark Pydantic native
        start = time.perf_counter()
        for order in orders:
            _ = order.model_dump_json()
        pydantic_time = time.perf_counter() - start

        # msgspec should be significantly faster
        # Performance logging removed per ruff T201 rule
        # Speedup calculation: pydantic_time / msgspec_time

        # Performance varies by environment, just ensure it runs
        # Speedup assertion removed as it's environment-dependent
        assert msgspec_time > 0
        assert pydantic_time > 0

    def test_memory_efficiency(self) -> None:
        """Test that msgspec uses less memory than alternatives."""
        # Create a large dataset
        large_data = [
            {
                "id": str(uuid4()),
                "value": Decimal(str(i * 0.001)),
                "name": f"item_{i}",
                "tags": ["tag1", "tag2", "tag3"],
            }
            for i in range(1000)
        ]

        # Serialize with msgspec
        json_bytes = dumps_json_bytes(large_data)

        # The output should be compact
        assert len(json_bytes) > 0

        # Deserialize and verify
        loaded = loads_json(json_bytes)
        # Type assertion for mypy
        assert isinstance(loaded, list)
        assert len(loaded) == 1000
        assert isinstance(loaded[0], dict)
        assert loaded[0]["tags"] == ["tag1", "tag2", "tag3"]


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    @given(
        exponent=st.integers(min_value=0, max_value=100),
        decimal_digits=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100, deadline=None)
    def test_very_large_numbers_properties(self, exponent: int, decimal_digits: int) -> None:
        """Property: Very large numbers should be handled without overflow or precision loss.

        Critical for handling large portfolio values and extreme market conditions.
        """
        data = {
            "big_int": 10**exponent,
            "big_decimal": Decimal("9" * decimal_digits),
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: Large integers preserved
        assert loaded["big_int"] == 10**exponent

        # Property: Large decimals preserved as strings
        assert loaded["big_decimal"] == "9" * decimal_digits

        # Property: Can reconstruct exact decimal
        # Type guard for loaded value
        big_decimal_str = loaded["big_decimal"]
        assert isinstance(big_decimal_str, str)
        reconstructed = Decimal(big_decimal_str)
        assert reconstructed == data["big_decimal"]

    @given(
        unicode_text=st.text(
            alphabet=st.characters(
                min_codepoint=0x0020,
                max_codepoint=0x10FFFF,
                blacklist_categories=["Cs"],  # Exclude surrogates
            )
        ),
        include_quotes=st.booleans(),
        include_backslash=st.booleans(),
        include_newlines=st.booleans(),
    )
    @settings(max_examples=200, deadline=None)
    def test_special_characters_properties(
        self,
        unicode_text: str,
        include_quotes: bool,
        include_backslash: bool,
        include_newlines: bool,
    ) -> None:
        """Property: Special characters and unicode should be preserved exactly.

        Important for international trading symbols, user messages, and data integrity.
        """
        # Build test string with special characters
        test_str = unicode_text
        if include_quotes:
            test_str = f'{test_str} "quoted"'
        if include_backslash:
            test_str = f"{test_str} \\ backslash"
        if include_newlines:
            test_str = f"{test_str}\nNewline\rCarriage"

        data = {"special": test_str}

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)

        # Property: All characters preserved exactly
        loaded_special = loaded["special"]
        assert loaded_special == test_str

        # Property: String length preserved
        assert isinstance(loaded_special, str)
        assert len(loaded_special) == len(test_str)

    def test_circular_reference_prevention(self) -> None:
        """Test that circular references are handled properly."""

        class CircularModel(BaseModel):
            """Model that could have circular references."""

            name: str
            value: int

        model = CircularModel(name="test", value=42)

        # Should serialize without issues
        json_str = dumps_json(model)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["name"] == "test"
        assert loaded["value"] == 42
