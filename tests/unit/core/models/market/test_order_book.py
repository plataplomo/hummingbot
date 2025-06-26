"""Unit tests for the core OrderBook model.

Tests validation, parsing, immutability, and level structure validation.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.order_book import OrderBook


pytestmark = pytest.mark.timing


class TestOrderBook:
    """Unit tests for the cyberdelta.core.models.market.order_book.OrderBook model."""

    def test_minimal_creation(self) -> None:
        """Test creating an OrderBook with minimal valid data (empty bids/asks)."""
        now = datetime.now(UTC)
        ob = OrderBook(symbol="BTC-PERP", timestamp=now, bids=[], asks=[])
        assert ob.symbol == "BTC-PERP"
        assert ob.timestamp == now
        assert ob.bids == []
        assert ob.asks == []

    def test_creation_with_levels(self) -> None:
        """Test creating an OrderBook with valid bid/ask levels."""
        now = datetime.now(UTC)
        bids = [("50000.0", "1.5"), (Decimal("49999.5"), 2.0)]  # Mix types
        asks = [(Decimal("50000.5"), 1), ("50001.0", "0.5")]  # Mix types
        expected_bids = [(Decimal("50000.0"), Decimal("1.5")), (Decimal("49999.5"), Decimal("2.0"))]
        expected_asks = [(Decimal("50000.5"), Decimal("1.0")), (Decimal("50001.0"), Decimal("0.5"))]

        # Use Any to test validator handling of mixed types
        kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "bids": bids,
            "asks": asks,
        }
        ob = OrderBook(**kwargs)
        assert ob.bids == expected_bids
        assert ob.asks == expected_asks

    def test_required_fields(self) -> None:
        """Test that required fields (symbol, timestamp, bids, asks) raise errors if missing."""
        now = datetime.now(UTC)
        # Pydantic raises ValidationError if fields are missing entirely
        with pytest.raises(ValidationError, match="Field required"):
            # Ignore call-arg error: Intentionally missing 'symbol'
            # to test Pydantic's required field validation.
            OrderBook(timestamp=now, bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            # Ignore call-arg error: Intentionally missing 'timestamp'
            # to test Pydantic's required field validation.
            OrderBook(symbol="BTC", bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            # Ignore call-arg error: Intentionally missing 'bids'
            # to test Pydantic's required field validation.
            OrderBook(symbol="BTC", timestamp=now, asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            # Ignore call-arg error: Intentionally missing 'asks'
            # to test Pydantic's required field validation.
            OrderBook(symbol="BTC", timestamp=now, bids=[])  # type: ignore[call-arg]

    def test_symbol_validation(self) -> None:
        """Test validation rules for the symbol field."""
        now = datetime.now(UTC)
        with pytest.raises(ValueError, match="Field symbol: String cannot be empty"):
            OrderBook(symbol="", timestamp=now, bids=[], asks=[])
        with pytest.raises(ValueError, match="Field symbol: String cannot be empty"):
            OrderBook(symbol="   ", timestamp=now, bids=[], asks=[])
        with pytest.raises(ValueError, match="String value too long"):
            OrderBook(symbol="A" * 65, timestamp=now, bids=[], asks=[])
        # Valid symbol should pass
        OrderBook(symbol="VALID-SYM_123", timestamp=now, bids=[], asks=[])

    def test_timestamp_validation_required(self) -> None:
        """Test that providing timestamp=None raises a ValueError from the validator."""
        # Using Any to bypass static checks for testing runtime validation of None input.
        invalid_data: dict[str, Any] = {"symbol": "BTC", "timestamp": None, "bids": [], "asks": []}
        with pytest.raises(ValueError, match="timestamp must not be None"):
            # No type ignore needed here as Mypy doesn't flag an error for passing None
            # when the validator explicitly accepts Optional types in its signature.
            OrderBook(**invalid_data)

    def test_timestamp_validation_parsing(self) -> None:
        """Test timestamp parsing from various supported formats (int, str, datetime)."""
        ms_timestamp = 1678881600000  # 2023-03-15 12:00:00 UTC
        iso_timestamp = "2023-03-15T12:00:00Z"
        naive_dt = datetime(2023, 3, 15, 12, 0, 0)
        expected_dt = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)

        # Test int timestamp parsing using Any
        kwargs_int: dict[str, Any] = {
            "symbol": "T",
            "timestamp": ms_timestamp,
            "bids": [],
            "asks": [],
        }
        ob_int = OrderBook(**kwargs_int)
        assert ob_int.timestamp == expected_dt

        # Test ISO string timestamp parsing using Any
        kwargs_iso: dict[str, Any] = {
            "symbol": "T",
            "timestamp": iso_timestamp,
            "bids": [],
            "asks": [],
        }
        ob_iso = OrderBook(**kwargs_iso)
        assert ob_iso.timestamp == expected_dt

        # From naive datetime
        ob_naive = OrderBook(symbol="T", timestamp=naive_dt, bids=[], asks=[])
        assert ob_naive.timestamp == expected_dt  # Should be made UTC aware

        # From aware datetime
        ob_aware = OrderBook(symbol="T", timestamp=expected_dt, bids=[], asks=[])
        assert ob_aware.timestamp == expected_dt

    # Remove test_level_structure_validation
    # Remove test_level_content_validation

    # Add new combined test
    def test_level_validation_and_parsing(self) -> None:
        """Test the combined `validate_and_parse_levels` validator for bids/asks.

        Covers validation of:
        - Top-level list structure
        - Individual level item structure (list/tuple, length 2)
        - Price type, parsability, and finiteness
        - Quantity type, parsability, finiteness, and non-negativity
        - Handling of various valid input formats (raw strings, Decimals, mixed)
        """
        now = datetime.now(UTC)
        valid_level_raw = ("10.0", "1.5")  # Use strings to test parsing
        valid_level_parsed = (Decimal("10.0"), Decimal("1.5"))
        zero_qty_level = (Decimal(10), Decimal(0))  # Zero quantity is valid

        # --- Test Top-Level Structure ---
        with pytest.raises(TypeError, match="bids must be a list"):
            # Test invalid bids type using Any
            kwargs_bids: dict[str, Any] = {
                "symbol": "T",
                "timestamp": now,
                "bids": "not_a_list",
                "asks": [],
            }
            OrderBook(**kwargs_bids)
        with pytest.raises(TypeError, match="asks must be a list"):
            # Test invalid asks type using Any
            kwargs_asks: dict[str, Any] = {"symbol": "T", "timestamp": now, "bids": [], "asks": {}}
            OrderBook(**kwargs_asks)

        # --- Test Level Item Structure ---
        with pytest.raises(TypeError, match="must be a list or tuple"):
            invalid_bids_item_type: Any = [valid_level_raw, 123]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_item_type, asks=[])
        with pytest.raises(ValueError, match="must have length 2"):
            invalid_bids_len1: Any = [valid_level_raw, ("9",)]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_len1, asks=[])
        with pytest.raises(ValueError, match="must have length 2"):
            invalid_bids_len3: Any = [valid_level_raw, ("9", "1", "2")]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_len3, asks=[])

        # --- Test Level Content - Price ---
        with pytest.raises(TypeError, match="Invalid price type"):
            invalid_price_type: Any = [(None, "1")]
            OrderBook(symbol="T", timestamp=now, bids=invalid_price_type, asks=[])
        with pytest.raises(TypeError, match="Invalid price type"):
            invalid_price_type_obj: Any = [({"a": 1}, "1")]
            OrderBook(symbol="T", timestamp=now, bids=invalid_price_type_obj, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid price value.*Cannot convert",
        ):
            invalid_price_parse: Any = [("not_a_number", "1")]
            OrderBook(symbol="T", timestamp=now, bids=invalid_price_parse, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid price value.*Expected finite Decimal, got Infinity",
        ):
            infinite_price: Any = [(Decimal("Infinity"), "1")]
            OrderBook(symbol="T", timestamp=now, bids=infinite_price, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid price value.*Expected finite Decimal, got NaN",
        ):
            nan_price: Any = [(Decimal("NaN"), "1")]
            OrderBook(symbol="T", timestamp=now, bids=nan_price, asks=[])

        # --- Test Level Content - Quantity ---
        with pytest.raises(TypeError, match="Invalid quantity type"):
            invalid_qty_type: Any = [("10", None)]
            OrderBook(symbol="T", timestamp=now, bids=invalid_qty_type, asks=[])
        with pytest.raises(TypeError, match="Invalid quantity type"):
            invalid_qty_type_obj: Any = [("10", ["1"])]
            OrderBook(symbol="T", timestamp=now, bids=invalid_qty_type_obj, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid quantity value.*Cannot convert",
        ):
            invalid_qty_parse: Any = [("10", "not_a_number")]
            OrderBook(symbol="T", timestamp=now, bids=invalid_qty_parse, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid quantity value.*Expected finite Decimal, got Infinity",
        ):
            infinite_qty: Any = [("10", Decimal("Infinity"))]
            OrderBook(symbol="T", timestamp=now, bids=infinite_qty, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid quantity value.*Expected finite Decimal, got NaN",
        ):
            nan_qty: Any = [("10", Decimal("NaN"))]
            OrderBook(symbol="T", timestamp=now, bids=nan_qty, asks=[])
        with pytest.raises(
            ValidationError,
            match=r"Value error, Invalid quantity value.*Must be non-negative",
        ):
            negative_qty: Any = [("10", "-1")]
            OrderBook(symbol="T", timestamp=now, bids=negative_qty, asks=[])

        # --- Test Valid Cases ---
        # Empty lists
        ob_empty = OrderBook(symbol="T", timestamp=now, bids=[], asks=[])
        assert ob_empty.bids == []
        assert ob_empty.asks == []

        # Valid list with raw data needing parsing
        # Ignore Mypy's list-item error: Intentionally providing list[tuple[str, str]]
        # to test the validator's parsing from string to Decimal.
        ob_raw = OrderBook(symbol="T", timestamp=now, bids=[valid_level_raw], asks=[])  # type: ignore[list-item]
        assert ob_raw.bids == [valid_level_parsed]

        # Valid list with pre-parsed Decimals and zero quantity
        ob_parsed = OrderBook(symbol="T", timestamp=now, bids=[zero_qty_level], asks=[])
        assert ob_parsed.bids == [zero_qty_level]

        # Valid list with mixed types
        mixed_bids_raw: Any = [("10.1", 1), (Decimal("9.9"), "0.5")]
        mixed_bids_expected = [(Decimal("10.1"), Decimal(1)), (Decimal("9.9"), Decimal("0.5"))]
        # Ignore arg-type because testing validator's mixed raw input handling
        # Mypy doesn't flag an error here (likely due to Any type hint on raw list)
        # No ignore needed: Mypy accepts Any here, Pyright infers correctly due to validator.
        ob_mixed = OrderBook(symbol="T", timestamp=now, bids=mixed_bids_raw, asks=[])
        assert ob_mixed.bids == mixed_bids_expected

    def test_extra_fields_forbidden(self) -> None:
        """Test that initializing with unexpected fields raises ValidationError (extra='forbid')."""
        now = datetime.now(UTC)
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            # Using Any to bypass type checking for extra_field
            extra_field_kwargs: dict[str, Any] = {
                "symbol": "BTC",
                "timestamp": now,
                "bids": [],
                "asks": [],
                "extra_field": "some_value",
            }
            OrderBook(**extra_field_kwargs)

    def test_immutability(self) -> None:
        """Test that the OrderBook model instance is immutable (frozen=True)."""
        now = datetime.now(UTC)
        ob = OrderBook(symbol="BTC-PERP", timestamp=now, bids=[], asks=[])

        # Direct attribute assignment raises ValidationError due to frozen=True
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.symbol = "ETH-PERP"
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.timestamp = now + timedelta(seconds=1)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.bids = [(Decimal(1), Decimal(1))]

        # Setting NEW attributes on a frozen model also raises ValidationError
        with pytest.raises(ValidationError, match="Instance is frozen"):
            # Ignore Mypy's attr-defined error: Intentionally trying to assign a non-existent
            # attribute to test the frozen=True behavior.
            ob.new_field = "test"  # type: ignore[attr-defined]
