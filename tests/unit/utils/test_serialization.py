"""Unit tests for the serialization utilities.

Tests JSON serialization functionality including custom encoders for Decimal and other types.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import json
import math
from datetime import UTC, datetime, timezone
from decimal import Decimal
from typing import Any

import numpy as np
import pytest
from pydantic import BaseModel

from cyberdelta.utils.serialization import (
    CyberDeltaJSONEncoder,
    JSONSerializationError,
    dump_json,
    load_json,
)


# Test models for serialization
class SimpleModel(BaseModel):
    """Simple Pydantic model for testing."""

    name: str
    value: int


class ComplexModel(BaseModel):
    """Complex Pydantic model with various types."""

    id: int
    amount: Decimal
    timestamp: datetime
    nested: SimpleModel
    optional: str | None = None


class TestJSONSerializationError:
    """Test suite for JSONSerializationError exception."""

    # ==================== SUCCESS CASES ====================

    def test_error_initialization_success(self) -> None:
        """Test successful error initialization with type information."""
        # Arrange
        test_type = dict

        # Act
        error = JSONSerializationError(test_type)

        # Assert
        assert error.obj_type == test_type
        assert str(error) == "Object of type dict is not JSON serializable"

    def test_error_with_custom_type(self) -> None:
        """Test error with custom class type."""

        # Arrange
        class CustomClass:
            pass

        # Act
        error = JSONSerializationError(CustomClass)

        # Assert
        assert error.obj_type == CustomClass
        assert "CustomClass" in str(error)


class TestCyberDeltaJSONEncoder:
    """Test suite for CyberDeltaJSONEncoder."""

    # ==================== SUCCESS CASES ====================

    def test_encode_decimal_success(self) -> None:
        """Test successful encoding of Decimal values."""
        # Arrange
        data = {"price": Decimal("123.456789"), "quantity": Decimal(1000)}

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        assert result == '{"price": "123.456789", "quantity": "1000"}'
        # Verify it can be decoded back
        decoded = json.loads(result)
        assert decoded["price"] == "123.456789"
        assert decoded["quantity"] == "1000"

    def test_encode_datetime_success(self) -> None:
        """Test successful encoding of datetime values."""
        # Arrange
        dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        data = {"timestamp": dt}

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        assert result == '{"timestamp": "2024-01-15T10:30:45+00:00"}'

    def test_encode_numpy_integer_success(self) -> None:
        """Test successful encoding of numpy integer types."""
        # Arrange
        data = {
            "int8": np.int8(42),
            "int16": np.int16(1000),
            "int32": np.int32(100000),
            "int64": np.int64(10000000000),
        }

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["int8"] == 42
        assert parsed["int16"] == 1000
        assert parsed["int32"] == 100000
        assert parsed["int64"] == 10000000000

    def test_encode_numpy_float_success(self) -> None:
        """Test successful encoding of numpy float types."""
        # Arrange
        data = {
            "float16": np.float16(math.pi),
            "float32": np.float32(math.e),
            "float64": np.float64(1.41421356),
        }

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert isinstance(parsed["float16"], float)
        assert isinstance(parsed["float32"], float)
        assert isinstance(parsed["float64"], float)
        assert abs(parsed["float32"] - math.e) < 0.00001

    def test_encode_pydantic_model_success(self) -> None:
        """Test successful encoding of Pydantic models."""
        # Arrange
        model = SimpleModel(name="test", value=42)
        data = {"model": model}

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["model"]["name"] == "test"
        assert parsed["model"]["value"] == 42

    def test_encode_nested_structures_success(self) -> None:
        """Test successful encoding of nested data structures."""
        # Arrange
        dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        nested_model = SimpleModel(name="nested", value=100)
        complex_model = ComplexModel(
            id=1,
            amount=Decimal("999.99"),
            timestamp=dt,
            nested=nested_model,
            optional="extra",
        )
        data = {
            "models": [complex_model],
            "decimals": [Decimal("1.1"), Decimal("2.2")],
            "mixed": {
                "numpy": np.int32(42),
                "decimal": Decimal("3.14159"),
                "datetime": dt,
            },
        }

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["models"][0]["amount"] == "999.99"
        assert parsed["decimals"] == ["1.1", "2.2"]
        assert parsed["mixed"]["numpy"] == 42
        assert parsed["mixed"]["decimal"] == "3.14159"

    # ==================== EDGE CASES ====================

    def test_encode_edge_zero_decimal(self) -> None:
        """Test encoding of zero Decimal value."""
        # Arrange
        data = {"zero": Decimal(0), "negative_zero": Decimal("-0")}

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["zero"] == "0"
        assert parsed["negative_zero"] == "-0"

    def test_encode_edge_extreme_decimals(self) -> None:
        """Test encoding of extreme Decimal values."""
        # Arrange
        data = {
            "very_small": Decimal("0.00000000000000000001"),
            "very_large": Decimal(99999999999999999999),  # String required for precision
            "scientific": Decimal("1.23E+10"),
        }

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["very_small"] == "1E-20"
        assert parsed["very_large"] == "99999999999999999999"
        assert parsed["scientific"] == "1.23E+10"

    def test_encode_edge_datetime_variants(self) -> None:
        """Test encoding of various datetime formats."""
        # Arrange
        data = {
            "utc": datetime(2024, 1, 1, tzinfo=UTC),
            "naive": datetime(2024, 1, 1, tzinfo=UTC),  # Using UTC timezone
            "other_tz": datetime(2024, 1, 1, tzinfo=timezone.min),
        }

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert "2024-01-01" in parsed["utc"]
        assert "+00:00" in parsed["utc"]
        assert "2024-01-01" in parsed["naive"]
        assert "-23:59" in parsed["other_tz"]  # timezone.min offset

    def test_encode_edge_empty_structures(self) -> None:
        """Test encoding of empty data structures."""
        # Arrange
        data: dict[str, Any] = {"empty_dict": {}, "empty_list": [], "none": None}

        # Act
        result = json.dumps(data, cls=CyberDeltaJSONEncoder)

        # Assert
        parsed = json.loads(result)
        assert parsed["empty_dict"] == {}
        assert parsed["empty_list"] == []
        assert parsed["none"] is None

    # ==================== FAILURE CASES ====================

    def test_encode_failure_unsupported_type(self) -> None:
        """Test encoding failure with unsupported type."""

        # Arrange
        class UnsupportedClass:
            pass

        data = {"unsupported": UnsupportedClass()}

        # Act & Assert
        with pytest.raises(JSONSerializationError) as exc_info:
            json.dumps(data, cls=CyberDeltaJSONEncoder)
        assert "UnsupportedClass" in str(exc_info.value)

    def test_encode_failure_complex_number(self) -> None:
        """Test encoding failure with complex numbers."""
        # Arrange
        data = {"complex": complex(1, 2)}

        # Act & Assert
        with pytest.raises(JSONSerializationError) as exc_info:
            json.dumps(data, cls=CyberDeltaJSONEncoder)
        assert "complex" in str(exc_info.value)

    def test_encode_failure_set_type(self) -> None:
        """Test encoding failure with set type."""
        # Arrange
        data = {"set": {1, 2, 3}}

        # Act & Assert
        with pytest.raises(JSONSerializationError) as exc_info:
            json.dumps(data, cls=CyberDeltaJSONEncoder)
        assert "set" in str(exc_info.value)


class TestDumpJson:
    """Test suite for dump_json helper function."""

    # ==================== SUCCESS CASES ====================

    def test_dump_json_success_basic(self) -> None:
        """Test successful JSON dumping with basic types."""
        # Arrange
        data = {"name": "test", "value": 42, "active": True}

        # Act
        result = dump_json(data)

        # Assert
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == data

    def test_dump_json_success_with_decimal(self) -> None:
        """Test successful JSON dumping with Decimal values."""
        # Arrange
        data = {"amount": Decimal("123.45"), "rate": Decimal("0.05")}

        # Act
        result = dump_json(data)

        # Assert
        parsed = json.loads(result)
        assert parsed["amount"] == "123.45"
        assert parsed["rate"] == "0.05"

    def test_dump_json_success_with_formatting(self) -> None:
        """Test successful JSON dumping with formatting options."""
        # Arrange
        data = {"key": "value", "number": 42}

        # Act
        result = dump_json(data, indent=2, sort_keys=True)

        # Assert
        assert "{\n  " in result  # Check for indentation
        assert result.index('"key"') < result.index('"number"')  # Check sorting

    # ==================== EDGE CASES ====================

    def test_dump_json_edge_with_skipkeys(self) -> None:
        """Test JSON dumping with skipkeys option."""
        # Arrange
        data = {
            "valid": "value",
            123: "numeric_key",  # This will be skipped
            "nested": {"valid2": "value2"},
        }

        # Act
        result = dump_json(data, skipkeys=True)

        # Assert
        parsed = json.loads(result)
        assert "valid" in parsed
        assert 123 not in parsed
        assert parsed["nested"]["valid2"] == "value2"

    def test_dump_json_edge_with_ensure_ascii(self) -> None:
        """Test JSON dumping with unicode characters."""
        # Arrange
        data = {"unicode": "测试", "emoji": "🚀"}

        # Act
        result_ascii = dump_json(data, ensure_ascii=True)
        result_unicode = dump_json(data, ensure_ascii=False)

        # Assert
        assert "\\u" in result_ascii  # Unicode escapes
        assert "测试" in result_unicode  # Direct unicode
        assert "🚀" in result_unicode

    def test_dump_json_edge_with_nan(self) -> None:
        """Test JSON dumping with NaN values."""
        # Arrange
        data = {"value": float("nan"), "inf": float("inf")}

        # Act
        result = dump_json(data, allow_nan=True)

        # Assert
        assert "NaN" in result
        assert "Infinity" in result

    # ==================== FAILURE CASES ====================

    def test_dump_json_failure_circular_reference(self) -> None:
        """Test JSON dumping failure with circular references."""
        # Arrange
        data: dict[str, Any] = {"key": "value"}
        data["circular"] = data  # Create circular reference

        # Act & Assert
        with pytest.raises(ValueError, match="Circular reference"):
            dump_json(data, check_circular=True)

    def test_dump_json_failure_nan_not_allowed(self) -> None:
        """Test JSON dumping failure when NaN not allowed."""
        # Arrange
        data = {"value": float("nan")}

        # Act & Assert
        with pytest.raises(ValueError):
            dump_json(data, allow_nan=False)


class TestLoadJson:
    """Test suite for load_json helper function."""

    # ==================== SUCCESS CASES ====================

    def test_load_json_success_basic(self) -> None:
        """Test successful JSON loading with basic types."""
        # Arrange
        json_str = '{"name": "test", "value": 42, "active": true}'

        # Act
        result = load_json(json_str)

        # Assert
        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["value"] == 42
        assert result["active"] is True

    def test_load_json_success_arrays(self) -> None:
        """Test successful JSON loading with arrays."""
        # Arrange
        json_str = '{"numbers": [1, 2, 3], "strings": ["a", "b", "c"]}'

        # Act
        result = load_json(json_str)

        # Assert
        assert isinstance(result, dict)
        assert result["numbers"] == [1, 2, 3]
        assert result["strings"] == ["a", "b", "c"]

    # ==================== EDGE CASES ====================

    def test_load_json_edge_empty_structures(self) -> None:
        """Test loading empty JSON structures."""
        # Arrange & Act & Assert
        assert load_json("{}") == {}
        assert load_json("[]") == []
        empty_str_result = load_json('""')
        assert isinstance(empty_str_result, str)
        assert not empty_str_result  # Explicitly checking for empty string, not falsy
        assert load_json("null") is None

    def test_load_json_edge_whitespace(self) -> None:
        """Test loading JSON with extra whitespace."""
        # Arrange
        json_str = '  \n\t{ "key" : "value" }  \n  '

        # Act
        result = load_json(json_str)

        # Assert
        assert result == {"key": "value"}

    # ==================== FAILURE CASES ====================

    def test_load_json_failure_invalid_syntax(self) -> None:
        """Test loading failure with invalid JSON syntax."""
        # Arrange
        json_str = '{"key": "value",}'  # Trailing comma

        # Act & Assert
        with pytest.raises(json.JSONDecodeError):
            load_json(json_str)

    def test_load_json_failure_unclosed_structure(self) -> None:
        """Test loading failure with unclosed structures."""
        # Arrange
        json_str = '{"key": "value"'  # Missing closing brace

        # Act & Assert
        with pytest.raises(json.JSONDecodeError):
            load_json(json_str)


# ==================== INTEGRATION TESTS ====================


class TestSerializationIntegration:
    """Integration tests for serialization functionality."""

    def test_roundtrip_complex_data(self) -> None:
        """Test serialization roundtrip with complex data."""
        # Arrange
        dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        original_data = {
            "decimal": Decimal("123.456789"),
            "datetime": dt,
            "numpy_int": np.int64(42),
            "numpy_float": np.float64(math.pi),
            "model": SimpleModel(name="test", value=100),
            "nested": {
                "list": [Decimal("1.1"), Decimal("2.2")],
                "dict": {"key": "value"},
            },
        }

        # Act
        json_str = dump_json(original_data, indent=2)
        loaded_data = load_json(json_str)
        assert isinstance(loaded_data, dict)

        # Assert - access dict after type narrowing
        loaded_dict = loaded_data
        assert loaded_dict["decimal"] == "123.456789"
        assert loaded_dict["datetime"] == dt.isoformat()
        assert loaded_dict["numpy_int"] == 42
        numpy_float_val = loaded_dict["numpy_float"]
        assert isinstance(numpy_float_val, float)
        assert abs(numpy_float_val - math.pi) < 0.00001
        assert loaded_dict["model"]["name"] == "test"
        assert loaded_dict["nested"]["list"] == ["1.1", "2.2"]

    def test_error_handling_with_mixed_types(self) -> None:
        """Test error handling with mixed supported and unsupported types."""

        # Arrange
        class BadClass:
            pass

        data = {
            "good": Decimal(123),
            "bad": BadClass(),
        }

        # Act & Assert
        with pytest.raises(JSONSerializationError) as exc_info:
            dump_json(data)
        assert exc_info.value.obj_type == BadClass


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("input_value", "expected", "description"),
    [
        # Success cases
        (Decimal("123.45"), '"123.45"', "typical decimal"),
        (Decimal(0), '"0"', "zero decimal"),
        (Decimal("-999.99"), '"-999.99"', "negative decimal"),
        # Edge cases
        (Decimal("1E+10"), '"1E+10"', "scientific notation"),
        (Decimal("0.0000001"), '"1E-7"', "very small decimal"),
        # Special numpy types
        (np.int8(127), "127", "numpy int8 max"),
        (np.float32(1.5), "1.5", "numpy float32"),
    ],
)
def test_encoder_parametrized(
    input_value: Decimal | np.int8 | np.float32, expected: str, description: str
) -> None:
    """Test encoder with various input types: {description}."""
    # Act
    result = json.dumps(input_value, cls=CyberDeltaJSONEncoder)

    # Assert
    assert result == expected
