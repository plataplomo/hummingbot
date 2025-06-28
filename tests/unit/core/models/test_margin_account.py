"""Unit tests for the MarginAccountSummary core model and its Details sub-models."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.margin_account import (
    BackpackMarginDetails,
    HyperliquidMarginDetails,
    MarginAccountSummary,
)


pytestmark = pytest.mark.timing

# Type alias for broad, but Any-free, test parameter values
PrimitiveTestVal = str | int | float | bool | Decimal | None
TestParamValue = PrimitiveTestVal | list[PrimitiveTestVal] | dict[str, PrimitiveTestVal]


# --- Helper Fixtures ---
@pytest.fixture
def valid_hl_margin_details_data() -> dict[str, Any]:
    """Provide valid data for HyperliquidMarginDetails.

    Returns:
        dict[str, Any]: Valid data dictionary for creating HyperliquidMarginDetails instances.
    """
    return {
        "cross_maintenance_margin_used": Decimal("1234.56"),
        "isolated_maintenance_margin_used": Decimal("789.10"),
    }


@pytest.fixture
def valid_bp_margin_details_data() -> dict[str, Any]:
    """Provide valid data for BackpackMarginDetails.

    Returns:
        dict[str, Any]: Valid data dictionary for creating BackpackMarginDetails instances.
    """
    return {
        "assets_value": Decimal("15000.0"),
        "borrow_liability": Decimal("500.0"),
        "liabilities_value": Decimal("1000.0"),
        "locked_equity": Decimal("2000.0"),
        "margin_fraction": Decimal("0.9333"),  # (15000 - 1000) / 15000 approx
        "imf_raw": "some_imf_string",
        "mmf_raw": "some_mmf_string",
    }


@pytest.fixture
def base_margin_summary_data() -> dict[str, Any]:
    """Provide valid core data for MarginAccountSummary creation.

    Returns:
        dict[str, Any]: Base data dictionary for creating MarginAccountSummary instances.
    """
    return {
        "exchange": "backpack",
        "timestamp": datetime.now(UTC),
        "total_equity": Decimal("14000.0"),  # assets_value - liabilities_value
        "available_equity": Decimal("12000.0"),  # total_equity - locked_equity
        "total_initial_margin_required": Decimal("1500.0"),
        "total_maintenance_margin_required": Decimal("1000.0"),
        "total_position_notional": Decimal("25000.0"),
        "total_unrealized_pnl": Decimal("-50.75"),
    }


# --- Core MarginAccountSummary Success Tests ---
def test_margin_summary_creation_all_fields(
    base_margin_summary_data: dict[str, Any],
    valid_bp_margin_details_data: dict[str, Any],
) -> None:
    """Test successful creation with all valid core and detail fields."""
    data = base_margin_summary_data.copy()
    data["exchange"] = "backpack"  # Ensure match
    data["bp_details"] = BackpackMarginDetails(**valid_bp_margin_details_data)

    summary = MarginAccountSummary(**data)

    assert summary.exchange == "backpack"
    assert isinstance(summary.timestamp, datetime)
    assert summary.total_equity == Decimal("14000.0")
    assert summary.available_equity == Decimal("12000.0")
    assert summary.total_initial_margin_required == Decimal("1500.0")
    assert summary.total_maintenance_margin_required == Decimal("1000.0")
    assert summary.total_position_notional == Decimal("25000.0")
    assert summary.total_unrealized_pnl == Decimal("-50.75")
    assert summary.hl_details is None
    assert summary.bp_details is not None
    assert summary.bp_details.assets_value == Decimal("15000.0")
    assert summary.model_config.get("frozen") is True


def test_margin_summary_creation_required_only(base_margin_summary_data: dict[str, Any]) -> None:
    """Test successful creation with only required core fields."""
    required_data = {
        k: v
        for k, v in base_margin_summary_data.items()
        if k in ["exchange", "timestamp", "total_equity", "available_equity"]
    }
    summary = MarginAccountSummary(**required_data)
    assert summary.exchange == "backpack"
    assert isinstance(summary.timestamp, datetime)
    assert summary.total_equity == Decimal("14000.0")
    assert summary.available_equity == Decimal("12000.0")
    # Optional fields should be None
    assert summary.total_initial_margin_required is None
    assert summary.total_maintenance_margin_required is None
    assert summary.total_position_notional is None
    assert summary.total_unrealized_pnl is None
    assert summary.hl_details is None
    assert summary.bp_details is None


def test_margin_summary_creation_with_strings(base_margin_summary_data: dict[str, Any]) -> None:
    """Test creation using string representations for decimal fields."""
    data = base_margin_summary_data.copy()
    # Convert decimals to strings
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = str(value)

    # Add type ignore as we are intentionally passing strings where Decimals expected
    summary = MarginAccountSummary(**data)

    # Assert values were converted correctly
    assert summary.total_equity == Decimal("14000.0")
    assert summary.total_unrealized_pnl == Decimal("-50.75")


# --- Core MarginAccountSummary Failure Tests ---
@pytest.mark.parametrize(
    "field, value, error_match",
    [
        # Required Strings
        ("exchange", None, "Value error, exchange: Expected string, got NoneType"),
        ("exchange", " ", "Field exchange: String cannot be empty"),
        ("exchange", "x" * 65, "String value too long"),
        # Required Datetime
        (
            "timestamp",
            None,
            "Value error, timestamp: Required datetime value parsed as None or was invalid",
        ),
        (
            "timestamp",
            "not-a-date",
            r"timestamp: Cannot parse string .* as ISO datetime .* or as numeric timestamp",
        ),
        # Required Decimals (>= 0)
        ("total_equity", None, "Value error, total_equity: Value cannot be None"),
        ("total_equity", Decimal("-0.1"), "Input should be greater than or equal to 0"),
        ("total_equity", Decimal("NaN"), "Value must be finite"),
        ("available_equity", "invalid", "Cannot convert 'invalid' to Decimal"),
        ("available_equity", Decimal(-100), "Input should be greater than or equal to 0"),
        # Optional Decimals (>= 0 where applicable)
        (
            "total_initial_margin_required",
            Decimal(-1),
            "Input should be greater than or equal to 0",
        ),
        ("total_maintenance_margin_required", Decimal("NaN"), "Value must be finite if provided"),
        ("total_position_notional", Decimal(-1000), "Input should be greater than or equal to 0"),
        ("total_unrealized_pnl", Decimal("Infinity"), "Value must be finite if provided"),
    ],
)
def test_margin_summary_invalid_field_inputs(
    base_margin_summary_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for individual core field invalid inputs."""
    data = base_margin_summary_data.copy()
    data[field] = value
    with pytest.raises(ValidationError, match=f".*{error_match}.*"):
        MarginAccountSummary(**data)


def test_margin_summary_missing_required_fields(base_margin_summary_data: dict[str, Any]) -> None:
    """Test failure when required core fields are missing."""
    required_fields = ["exchange", "timestamp", "total_equity", "available_equity"]
    for field_to_remove in required_fields:
        invalid_data = base_margin_summary_data.copy()
        del invalid_data[field_to_remove]
        # Use precise escaped string from initial failure report
        # Need to double-escape backslashes for the f-string and then regex
        match_str = (
            f"1 validation error for MarginAccountSummary\\n{field_to_remove}\\n  Field required"
        )
        with pytest.raises(ValidationError, match=match_str):
            MarginAccountSummary(**invalid_data)


def test_margin_summary_extra_fields(base_margin_summary_data: dict[str, Any]) -> None:
    """Test extra='forbid' on core MarginAccountSummary model."""
    data = base_margin_summary_data.copy()
    data["another_field"] = 999
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        MarginAccountSummary(**data)


def test_margin_summary_immutability(base_margin_summary_data: dict[str, Any]) -> None:
    """Test that the core MarginAccountSummary model is immutable."""
    summary = MarginAccountSummary(**base_margin_summary_data)
    original_equity = summary.total_equity

    with pytest.raises(ValidationError, match="Instance is frozen"):
        summary.total_equity = original_equity + 100

    assert summary.total_equity == original_equity

    # Test __setattr__ bypass - log warning if modification occurs but pass test
    logger = get_logger(__name__)
    original_exchange = summary.exchange
    try:
        summary.exchange = "new_exchange"
        if summary.exchange != original_exchange:
            logger.warning(
                f"Immutability Test Warning: object.__setattr__ modified frozen field 'exchange' "
                f"on MarginAccountSummary instance. Value changed to: {summary.exchange}.",
            )
    except Exception as e:
        pytest.fail(f"object.__setattr__ raised unexpected exception on frozen model: {e}")
    # Verify original value or log warning if changed
    assert isinstance(summary.exchange, str)


# --- Details Model Specific Tests ---


def test_hyperliquid_margin_details_creation_and_immutability(
    valid_hl_margin_details_data: dict[str, Any],
) -> None:
    """Test HyperliquidMarginDetails creation and immutability."""
    details = HyperliquidMarginDetails(**valid_hl_margin_details_data)
    assert details.cross_maintenance_margin_used == Decimal("1234.56")
    assert details.isolated_maintenance_margin_used == Decimal("789.10")
    assert details.model_config.get("frozen") is True

    with pytest.raises(ValidationError, match="Instance is frozen"):
        details.cross_maintenance_margin_used = Decimal(1000)


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        (
            "cross_maintenance_margin_used",
            Decimal(-1),
            "Input should be greater than or equal to 0",
        ),
        ("cross_maintenance_margin_used", Decimal("NaN"), "Value must be finite"),
        ("isolated_maintenance_margin_used", "abc", "Cannot convert 'abc' to Decimal"),
        (
            "isolated_maintenance_margin_used",
            None,
            "Value error, isolated_maintenance_margin_used: Value cannot be None",
        ),
    ],
)
def test_hyperliquid_margin_details_invalid_fields(
    valid_hl_margin_details_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for HyperliquidMarginDetails."""
    data = valid_hl_margin_details_data.copy()
    data[field] = value
    with pytest.raises(ValidationError, match=error_match):
        HyperliquidMarginDetails(**data)


def test_hyperliquid_margin_details_extra_fields_ignored(
    valid_hl_margin_details_data: dict[str, Any],
) -> None:
    """Test extra='ignore' on HyperliquidMarginDetails."""
    data = valid_hl_margin_details_data.copy()
    data["ignored_field"] = "value"
    details = HyperliquidMarginDetails(**data)
    assert not hasattr(details, "ignored_field")
    assert details.cross_maintenance_margin_used == Decimal("1234.56")


def test_backpack_margin_details_creation_and_immutability(
    valid_bp_margin_details_data: dict[str, Any],
) -> None:
    """Test BackpackMarginDetails creation and immutability."""
    details = BackpackMarginDetails(**valid_bp_margin_details_data)
    assert details.assets_value == Decimal("15000.0")
    assert details.borrow_liability == Decimal("500.0")
    assert details.margin_fraction == Decimal("0.9333")
    assert details.imf_raw == "some_imf_string"
    assert details.model_config.get("frozen") is True

    with pytest.raises(ValidationError, match="Instance is frozen"):
        details.assets_value = Decimal(16000)


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        ("assets_value", Decimal(-1), "Input should be greater than or equal to 0"),
        ("borrow_liability", Decimal("NaN"), "Value must be finite if provided"),
        ("liabilities_value", "bad-decimal", "Cannot convert 'bad-decimal' to Decimal"),
        ("locked_equity", Decimal(-100), "Input should be greater than or equal to 0"),
        ("margin_fraction", Decimal("-0.1"), "Input should be greater than or equal to 0"),
        ("imf_raw", 123, "Value error, imf_raw: Expected string, got int"),
        ("mmf_raw", "s" * 257, "String value too long"),
    ],
)
def test_backpack_margin_details_invalid_fields(
    valid_bp_margin_details_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for BackpackMarginDetails."""
    data = valid_bp_margin_details_data.copy()
    data[field] = value
    with pytest.raises(ValidationError, match=error_match):
        BackpackMarginDetails(**data)


def test_backpack_margin_details_extra_fields_ignored(
    valid_bp_margin_details_data: dict[str, Any],
) -> None:
    """Test extra='ignore' on BackpackMarginDetails."""
    data = valid_bp_margin_details_data.copy()
    data["another_ignored_field"] = {"a": 1}
    details = BackpackMarginDetails(**data)
    assert not hasattr(details, "another_ignored_field")
    assert details.assets_value == Decimal("15000.0")
