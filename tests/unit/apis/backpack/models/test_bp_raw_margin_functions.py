"""Unit tests for Backpack Raw Margin Function models."""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)

# --- Test BackpackRawImfFunction ---


def test_bp_raw_imf_function_valid() -> None:
    """Test successful validation of BackpackRawImfFunction."""
    data = {"base": "0.01", "factor": "0.005"}
    model = BackpackRawImfFunction.model_validate(data)
    assert model.base == "0.01"
    assert model.factor == "0.005"


def test_bp_raw_imf_function_valid_aliases() -> None:
    """Test successful validation with potential aliases (same keys here)."""
    data = {"base": "1.2", "factor": "0.9"}
    model = BackpackRawImfFunction(**data)
    assert model.base == "1.2"
    assert model.factor == "0.9"


def test_bp_raw_imf_function_missing_field() -> None:
    """Test validation failure when a required field is missing."""
    with pytest.raises(ValidationError, match="Field required"):
        BackpackRawImfFunction.model_validate({"base": "0.1"})
    with pytest.raises(ValidationError, match="Field required"):
        BackpackRawImfFunction.model_validate({"factor": "0.2"})


def test_bp_raw_imf_function_invalid_type() -> None:
    """Test validation failure when fields have incorrect types."""
    with pytest.raises(ValidationError, match="base: Validation failed - base: Expected string"):
        BackpackRawImfFunction.model_validate({"base": 1.0, "factor": "0.1"})
    with pytest.raises(
        ValidationError, match="factor: Validation failed - factor: Expected string",
    ):
        BackpackRawImfFunction.model_validate({"base": "0.1", "factor": None})


def test_bp_raw_imf_function_invalid_decimal_format() -> None:
    """Test validation failure for invalid decimal string formats."""
    with pytest.raises(ValidationError, match="Cannot convert 'abc' to Decimal"):
        BackpackRawImfFunction.model_validate({"base": "abc", "factor": "0.1"})
    with pytest.raises(ValidationError, match="Value must be a finite decimal"):
        BackpackRawImfFunction.model_validate({"base": "0.1", "factor": "Infinity"})
    with pytest.raises(ValidationError, match="Value must be a finite decimal"):
        BackpackRawImfFunction.model_validate({"base": "NaN", "factor": "0.1"})


def test_bp_raw_imf_function_empty_string() -> None:
    """Test validation failure for empty string fields."""
    with pytest.raises(
        ValidationError, match="base: Validation failed - base: String cannot be empty",
    ):
        BackpackRawImfFunction.model_validate({"base": "", "factor": "0.1"})


def test_bp_raw_imf_function_extra_fields() -> None:
    """Test that extra fields are ignored due to extra='ignore'."""
    data = {"base": "0.1", "factor": "0.2", "extra": "ignored", "type": "sqrt"}
    model = BackpackRawImfFunction.model_validate(data)
    assert model.base == "0.1"
    assert model.factor == "0.2"
    assert not hasattr(model, "extra")
    assert not hasattr(model, "type")


# --- Test BackpackRawMmfFunction ---


def test_bp_raw_mmf_function_valid() -> None:
    """Test successful validation of BackpackRawMmfFunction."""
    data = {"base": "0.02", "factor": "0.008"}
    model = BackpackRawMmfFunction.model_validate(data)
    assert model.base == "0.02"
    assert model.factor == "0.008"


def test_bp_raw_mmf_function_missing_field() -> None:
    """Test validation failure when a required field is missing."""
    with pytest.raises(ValidationError, match="Field required"):
        BackpackRawMmfFunction.model_validate({"base": "0.1"})
    with pytest.raises(ValidationError, match="Field required"):
        BackpackRawMmfFunction.model_validate({"factor": "0.2"})


def test_bp_raw_mmf_function_invalid_type() -> None:
    """Test validation failure when fields have incorrect types."""
    with pytest.raises(ValidationError, match="base: Validation failed - base: Expected string"):
        BackpackRawMmfFunction.model_validate({"base": True, "factor": "0.1"})


def test_bp_raw_mmf_function_invalid_decimal_format() -> None:
    """Test validation failure for invalid decimal string formats."""
    with pytest.raises(ValidationError, match="Value must be a finite decimal"):
        BackpackRawMmfFunction.model_validate({"base": "-Infinity", "factor": "0.1"})


def test_bp_raw_mmf_function_empty_string() -> None:
    """Test validation failure for empty string fields."""
    with pytest.raises(
        ValidationError, match="factor: Validation failed - factor: String cannot be empty",
    ):
        BackpackRawMmfFunction.model_validate({"base": "0.1", "factor": ""})


def test_bp_raw_mmf_function_extra_fields() -> None:
    """Test that extra fields are ignored."""
    data = {"base": "0.1", "factor": "0.2", "another_extra": 123, "type": "sqrt"}
    model = BackpackRawMmfFunction.model_validate(data)
    assert model.base == "0.1"
    assert model.factor == "0.2"
    assert not hasattr(model, "another_extra")
    assert not hasattr(model, "type")
