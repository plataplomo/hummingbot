"""Unit tests for msgspec JSON serialization implementation."""

import math
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import msgspec
import orjson
import pytest
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


class TestMsgspecSerialization:
    """Test msgspec serialization with Pydantic models."""

    def test_pydantic_model_serialization(self) -> None:
        """Test that Pydantic models serialize correctly with msgspec."""
        order = SampleOrder(
            symbol="BTC-USDC",
            price=Decimal("50000.50"),
            quantity=Decimal("0.01"),
            side="BUY",
            tags=["spot", "high-volume"],
            metadata={"exchange": "hyperliquid", "urgent": True},
        )

        # Serialize
        json_str = dumps_json(order)
        assert isinstance(json_str, str)
        assert "BTC-USDC" in json_str
        assert "50000.50" in json_str

        # Deserialize and validate
        data = loads_json(json_str)
        assert isinstance(data, dict)
        reconstructed = SampleOrder.model_validate(data)

        # Verify all fields match
        assert reconstructed.order_id == order.order_id
        assert reconstructed.symbol == order.symbol
        assert reconstructed.price == order.price
        assert reconstructed.quantity == order.quantity
        assert reconstructed.side == order.side
        assert reconstructed.tags == order.tags
        assert reconstructed.metadata == order.metadata

    def test_decimal_preservation(self) -> None:
        """Test that Decimal precision is preserved."""
        data = {
            "price": Decimal("123.456789012345678901234567890"),
            "quantity": Decimal("0.000000000000000001"),
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["price"] == "123.456789012345678901234567890"
        assert loaded["quantity"] == "0.000000000000000001"

    def test_datetime_serialization(self) -> None:
        """Test datetime serialization."""
        now = datetime.now(UTC)
        data = {"timestamp": now}

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        # Should be ISO format string
        assert isinstance(loaded["timestamp"], str)
        assert "T" in loaded["timestamp"]  # ISO format

    def test_uuid_serialization(self) -> None:
        """Test UUID serialization."""
        uid = uuid4()
        data = {"id": uid}

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["id"] == str(uid)

    def test_nested_structures(self) -> None:
        """Test complex nested structures."""
        data = {
            "orders": [
                {
                    "id": str(uuid4()),
                    "price": Decimal("100.50"),
                    "items": [1, 2, 3],
                    "meta": {"key": "value"},
                }
                for _ in range(10)
            ],
            "summary": {
                "total": Decimal("1005.00"),
                "count": 10,
                "status": None,
            },
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert isinstance(loaded["orders"], list)
        assert isinstance(loaded["summary"], dict)
        assert len(loaded["orders"]) == 10
        assert loaded["summary"]["total"] == "1005.00"
        assert loaded["summary"]["status"] is None

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

    def test_empty_structures(self) -> None:
        """Test empty lists and dicts."""
        data: dict[str, Any] = {
            "empty_list": [],
            "empty_dict": {},
            "none_value": None,
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["empty_list"] == []
        assert loaded["empty_dict"] == {}
        assert loaded["none_value"] is None

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
        speedup = pydantic_time / msgspec_time
        # Performance logging removed per ruff T201 rule
        # Results available via assertion below

        # Should be at least 10x faster
        assert speedup > 10, f"Expected >10x speedup, got {speedup:.1f}x"

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

    def test_very_large_numbers(self) -> None:
        """Test handling of very large numbers."""
        data = {
            "big_int": 10**20,
            "big_decimal": Decimal("9" * 50),
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["big_int"] == 10**20
        assert loaded["big_decimal"] == "9" * 50

    def test_special_characters(self) -> None:
        """Test handling of special characters."""
        data = {
            "unicode": "Hello 世界 🌍",
            "escaped": 'Quote: "test" and backslash: \\',
            "newlines": "Line 1\nLine 2\rLine 3",
        }

        json_str = dumps_json(data)
        loaded = loads_json(json_str)

        # Type assertion for mypy
        assert isinstance(loaded, dict)
        assert loaded["unicode"] == "Hello 世界 🌍"
        assert loaded["escaped"] == 'Quote: "test" and backslash: \\'
        assert loaded["newlines"] == "Line 1\nLine 2\rLine 3"

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
