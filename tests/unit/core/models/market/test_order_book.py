from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.order_book import OrderBook


class TestOrderBook:
    def test_minimal_creation(self) -> None:
        """Test creating an OrderBook with minimal valid data."""
        now = datetime.now(UTC)
        ob = OrderBook(symbol="BTC-PERP", timestamp=now, bids=[], asks=[])
        assert ob.symbol == "BTC-PERP"
        assert ob.timestamp == now
        assert ob.bids == []
        assert ob.asks == []

    def test_creation_with_levels(self) -> None:
        """Test creating an OrderBook with valid bid/ask levels."""
        now = datetime.now(UTC)
        bids = [(Decimal("50000.0"), Decimal("1.5")), (Decimal("49999.5"), Decimal("2.0"))]
        asks = [(Decimal("50000.5"), Decimal("1.0")), (Decimal("50001.0"), Decimal("0.5"))]
        ob = OrderBook(symbol="BTC-PERP", timestamp=now, bids=bids, asks=asks)
        assert ob.bids == bids
        assert ob.asks == asks

    def test_required_fields(self) -> None:
        """Test that required fields (symbol, timestamp, bids, asks) raise errors if missing."""
        now = datetime.now(UTC)
        with pytest.raises(ValidationError, match="symbol"):
            OrderBook(timestamp=now, bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="timestamp"):
            OrderBook(symbol="BTC", bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="bids"):
            OrderBook(symbol="BTC", timestamp=now, asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="asks"):
            OrderBook(symbol="BTC", timestamp=now, bids=[])  # type: ignore[call-arg]

    def test_symbol_validation(self) -> None:
        """Test validation rules for the symbol field."""
        now = datetime.now(UTC)
        with pytest.raises(ValueError, match="String cannot be empty or whitespace"):
            OrderBook(symbol="", timestamp=now, bids=[], asks=[])
        with pytest.raises(ValueError, match="String cannot be empty or whitespace"):
            OrderBook(symbol="   ", timestamp=now, bids=[], asks=[])
        with pytest.raises(ValueError, match="String value too long"):
            OrderBook(symbol="A" * 65, timestamp=now, bids=[], asks=[])
        # Valid symbol should pass
        OrderBook(symbol="VALID-SYM_123", timestamp=now, bids=[], asks=[])

    def test_timestamp_validation_required(self) -> None:
        """Test that timestamp cannot be None."""
        # Using Any/cast to bypass static checks for testing runtime validation
        invalid_data: dict[str, Any] = {"symbol": "BTC", "timestamp": None, "bids": [], "asks": []}
        with pytest.raises(ValueError, match="timestamp must not be None"):
            OrderBook(**invalid_data)

    def test_timestamp_validation_parsing(self) -> None:
        """Test timestamp parsing from various formats."""
        ms_timestamp = 1678881600000  # 2023-03-15 12:00:00 UTC
        iso_timestamp = "2023-03-15T12:00:00Z"
        naive_dt = datetime(2023, 3, 15, 12, 0, 0)
        expected_dt = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)

        # From ms int
        ob_int = OrderBook(symbol="T", timestamp=ms_timestamp, bids=[], asks=[])  # type: ignore[arg-type]
        assert ob_int.timestamp == expected_dt

        # From ISO string
        ob_iso = OrderBook(symbol="T", timestamp=iso_timestamp, bids=[], asks=[])
        assert ob_iso.timestamp == expected_dt

        # From naive datetime
        ob_naive = OrderBook(symbol="T", timestamp=naive_dt, bids=[], asks=[])
        assert ob_naive.timestamp == expected_dt  # Should be made UTC aware

        # From aware datetime
        ob_aware = OrderBook(symbol="T", timestamp=expected_dt, bids=[], asks=[])
        assert ob_aware.timestamp == expected_dt

    def test_level_structure_validation(self) -> None:
        """Test structure validation (mode='before') of bids/asks lists."""
        now = datetime.now(UTC)
        valid_level = (Decimal("10"), Decimal("1"))

        # Bids/Asks not a list
        with pytest.raises(TypeError, match="must be a list"):
            OrderBook(symbol="T", timestamp=now, bids="not_a_list", asks=[valid_level])
        with pytest.raises(TypeError, match="must be a list"):
            OrderBook(symbol="T", timestamp=now, bids=[valid_level], asks={})

        # List contains non-list/tuple item
        with pytest.raises(TypeError, match="must be a list or tuple"):
            invalid_bids_item_type: Any = [valid_level, 123]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_item_type, asks=[])

        # List contains item of wrong length
        with pytest.raises(ValueError, match="must have length 2"):
            invalid_bids_len1: Any = [valid_level, (Decimal("9"),)]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_len1, asks=[])
        with pytest.raises(ValueError, match="must have length 2"):
            invalid_bids_len3: Any = [valid_level, (Decimal("9"), Decimal("1"), Decimal("2"))]
            OrderBook(symbol="T", timestamp=now, bids=invalid_bids_len3, asks=[])

        # Valid structure should pass (content errors tested separately)
        OrderBook(symbol="T", timestamp=now, bids=[(Decimal("10"), Decimal("1"))], asks=[])

    def test_level_content_validation(self) -> None:
        """Test content validation (type coercion & mode='after') within bid/ask levels."""
        now = datetime.now(UTC)
        zero_qty_level = (Decimal("10"), Decimal("0"))  # Zero quantity is valid

        # Invalid price type (Pydantic coercion fails between validators)
        invalid_price_type_bids: Any = [("invalid_price", Decimal("1"))]
        with pytest.raises(ValidationError, match="Input should be a valid decimal"):
            OrderBook(symbol="T", timestamp=now, bids=invalid_price_type_bids, asks=[])

        # Invalid quantity type (Pydantic coercion fails between validators)
        invalid_qty_type_bids: Any = [(Decimal("10"), "invalid_qty")]
        with pytest.raises(ValidationError, match="Input should be a valid decimal"):
            OrderBook(symbol="T", timestamp=now, bids=invalid_qty_type_bids, asks=[])

        # --- Content checks handled by `validate_level_content` (mode='after') ---

        # Non-finite price (Infinity) -> ValidationError from Pydantic coercion
        infinite_price_bids: Any = [(Decimal("Infinity"), Decimal("1"))]
        with pytest.raises(ValidationError, match="Input should be a finite number"):
            OrderBook(symbol="T", timestamp=now, bids=infinite_price_bids, asks=[])

        # Non-finite price (NaN) -> ValidationError from Pydantic coercion
        nan_price_bids: Any = [(Decimal("NaN"), Decimal("1"))]
        with pytest.raises(ValidationError, match="Input should be a finite number"):
            OrderBook(symbol="T", timestamp=now, bids=nan_price_bids, asks=[])

        # Non-finite quantity (Infinity) -> ValidationError from Pydantic coercion
        infinite_qty_bids: Any = [(Decimal("10"), Decimal("Infinity"))]
        with pytest.raises(ValidationError, match="Input should be a finite number"):
            OrderBook(symbol="T", timestamp=now, bids=infinite_qty_bids, asks=[])

        # Non-finite quantity (NaN) -> ValidationError from Pydantic coercion
        nan_qty_bids: Any = [(Decimal("10"), Decimal("NaN"))]
        with pytest.raises(ValidationError, match="Input should be a finite number"):
            OrderBook(symbol="T", timestamp=now, bids=nan_qty_bids, asks=[])

        # Negative quantity -> ValueError from mode='after' validator
        negative_qty_bids: Any = [(Decimal("10"), Decimal("-1"))]
        with pytest.raises(ValueError, match="Must be non-negative"):
            OrderBook(symbol="T", timestamp=now, bids=negative_qty_bids, asks=[])

        # Valid level with zero quantity should pass all validation
        OrderBook(symbol="T", timestamp=now, bids=[zero_qty_level], asks=[])

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
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
        """Test that the OrderBook model is immutable."""
        now = datetime.now(UTC)
        ob = OrderBook(symbol="BTC-PERP", timestamp=now, bids=[], asks=[])

        with pytest.raises(ValidationError):
            cast(Any, ob).symbol = "ETH-PERP"
        with pytest.raises(ValidationError):
            cast(Any, ob).timestamp = now + timedelta(seconds=1)
        with pytest.raises(ValidationError):
            cast(Any, ob).bids = [(Decimal("1"), Decimal("1"))]
        with pytest.raises(ValidationError):
            cast(Any, ob).new_field = "test"
