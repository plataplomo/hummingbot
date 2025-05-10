"""
Unit tests for Hyperliquid Processed Exchange Response Models.
"""

from typing import Any, cast  # Added for casting

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_processed_exchange_responses import (
    HyperliquidErrorStatus,
    HyperliquidSuccessfulOrderStatus,
)


class TestHyperliquidSuccessfulOrderStatus:
    """Tests for HyperliquidSuccessfulOrderStatus model."""

    def test_valid_resting_order(self) -> None:
        """Test valid instantiation for a resting order."""
        data: dict[str, Any] = {"status_type": "resting", "oid": 12345}
        model = HyperliquidSuccessfulOrderStatus(**data)
        assert model.status_type == "resting"
        assert model.oid == 12345
        assert model.total_sz is None
        assert model.avg_px is None

    def test_valid_filled_order(self) -> None:
        """Test valid instantiation for a filled order."""
        data: dict[str, Any] = {
            "status_type": "filled",
            "oid": 67890,
            "total_sz": "1.00",
            "avg_px": "3000.50",
        }
        model = HyperliquidSuccessfulOrderStatus(**data)
        assert model.status_type == "filled"
        assert model.oid == 67890
        assert model.total_sz == "1.00"
        assert model.avg_px == "3000.50"

    def test_valid_canceled_order_object(self) -> None:
        """Test valid instantiation for a canceled order (object type)."""
        data: dict[str, Any] = {"status_type": "canceled", "oid": 54321}
        model = HyperliquidSuccessfulOrderStatus(**data)
        assert model.status_type == "canceled"
        assert model.oid == 54321
        assert model.total_sz is None
        assert model.avg_px is None

    def test_valid_canceled_order_string(self) -> None:
        """Test valid instantiation for a canceled order (string type)."""
        data: dict[str, Any] = {"status_type": "canceled_str"}
        model = HyperliquidSuccessfulOrderStatus(**data)
        assert model.status_type == "canceled_str"
        assert model.oid is None
        assert model.total_sz is None
        assert model.avg_px is None

    @pytest.mark.parametrize(
        "invalid_oid_data",
        [
            ({"status_type": "resting", "oid": -1}),  # Negative oid
            ({"status_type": "resting", "oid": "not_an_int"}),  # Invalid type for oid
        ],
    )
    def test_invalid_oid(self, invalid_oid_data: dict[str, Any]) -> None:  # Typed here
        """Test invalid oid values."""
        with pytest.raises(ValidationError):
            HyperliquidSuccessfulOrderStatus(**invalid_oid_data)

    @pytest.mark.parametrize(
        "invalid_sz_data",
        [
            (
                {
                    "status_type": "filled",
                    "oid": 1,
                    "total_sz": "-1.0",
                    "avg_px": "100",
                }
            ),  # Negative total_sz
            (
                {
                    "status_type": "filled",
                    "oid": 1,
                    "total_sz": "not_a_decimal",
                    "avg_px": "100",
                }
            ),  # Invalid type for total_sz
        ],
    )
    def test_invalid_total_sz(self, invalid_sz_data: dict[str, Any]) -> None:  # Typed here
        """Test invalid total_sz values."""
        with pytest.raises(ValidationError):
            HyperliquidSuccessfulOrderStatus(**invalid_sz_data)

    @pytest.mark.parametrize(
        "invalid_px_data",
        [
            (
                {
                    "status_type": "filled",
                    "oid": 1,
                    "total_sz": "1.0",
                    "avg_px": "-100.0",
                }
            ),  # Negative avg_px
            (
                {
                    "status_type": "filled",
                    "oid": 1,
                    "total_sz": "1.0",
                    "avg_px": "0.0",
                }
            ),  # Zero avg_px (must be positive)
            (
                {
                    "status_type": "filled",
                    "oid": 1,
                    "total_sz": "1.0",
                    "avg_px": "not_a_decimal",
                }
            ),  # Invalid type for avg_px
        ],
    )
    def test_invalid_avg_px(self, invalid_px_data: dict[str, Any]) -> None:  # Typed here
        """Test invalid avg_px values."""
        with pytest.raises(ValidationError):
            HyperliquidSuccessfulOrderStatus(**invalid_px_data)

    def test_invalid_status_type(self) -> None:
        """Test invalid status_type value."""
        data: dict[str, Any] = {"status_type": "unknown_status", "oid": 123}
        with pytest.raises(ValidationError):
            HyperliquidSuccessfulOrderStatus(**data)

    def test_extra_fields_not_allowed(self) -> None:
        """Test that extra fields are not allowed."""
        data: dict[str, Any] = {"status_type": "resting", "oid": 123, "extra_field": "value"}
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidSuccessfulOrderStatus(**data)
        assert "extra_field" in str(exc_info.value)
        assert "Extra inputs are not permitted" in str(exc_info.value)

    def test_frozen_model(self) -> None:
        """Test that the model is frozen (immutable)."""
        data: dict[str, Any] = {"status_type": "resting", "oid": 12345}
        model = HyperliquidSuccessfulOrderStatus(**data)
        with pytest.raises(
            ValidationError
        ) as exc_info:  # Pydantic v2 raises ValidationError for frozen
            model.oid = 54321
        assert "Instance is frozen" in str(exc_info.value)


class TestHyperliquidErrorStatus:
    """Tests for HyperliquidErrorStatus model."""

    def test_valid_error_status(self) -> None:
        """Test valid instantiation."""
        data: dict[str, Any] = {"message": "This is a valid error message."}
        model = HyperliquidErrorStatus(**data)
        assert model.message == "This is a valid error message."

    @pytest.mark.parametrize(
        "invalid_message_data",
        [
            ({"message": ""}),  # Empty message
            ({"message": "a" * 2000}),  # Message too long (RawApiErrorStringHL default is 1024)
        ],
    )
    def test_invalid_message(self, invalid_message_data: dict[str, Any]) -> None:  # Typed here
        """Test invalid message values."""
        with pytest.raises(ValidationError):
            HyperliquidErrorStatus(**invalid_message_data)

    def test_missing_message(self) -> None:
        """Test missing message field."""
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidErrorStatus(**cast(dict[str, Any], {}))  # Simplified cast
        assert "message" in str(exc_info.value)
        assert "Field required" in str(exc_info.value)

    def test_extra_fields_not_allowed(self) -> None:
        """Test that extra fields are not allowed."""
        data: dict[str, Any] = {"message": "Error", "extra_field": "value"}
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidErrorStatus(**data)
        assert "extra_field" in str(exc_info.value)
        assert "Extra inputs are not permitted" in str(exc_info.value)

    def test_frozen_model(self) -> None:
        """Test that the model is frozen (immutable)."""
        data: dict[str, Any] = {"message": "Initial error"}
        model = HyperliquidErrorStatus(**data)
        with pytest.raises(
            ValidationError
        ) as exc_info:  # Pydantic v2 raises ValidationError for frozen
            model.message = "New error"
        assert "Instance is frozen" in str(exc_info.value)
