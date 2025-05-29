from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.ticker import Ticker

# Constants for testing
NOW: datetime = datetime.now(UTC)
VALID_SYMBOL: str = "BTC-PERP"
DEC_ZERO: Decimal = Decimal("0")
DEC_ONE: Decimal = Decimal("1")
DEC_NEG_ONE: Decimal = Decimal("-1")
DEC_NAN: Decimal = Decimal("NaN")
DEC_INF: Decimal = Decimal("Infinity")
DEC_NEG_INF: Decimal = Decimal("-Infinity")


class TestTicker:
    """Unit tests for the cyberdelta.core.models.market.ticker.Ticker model."""

    def test_minimal_creation_required_fields(self) -> None:
        """Test creating a Ticker with only required fields (symbol, timestamp)."""
        ticker = Ticker(symbol=VALID_SYMBOL, timestamp=NOW)
        assert ticker.symbol == VALID_SYMBOL
        assert ticker.timestamp == NOW
        assert ticker.price is None
        assert ticker.bid is None
        assert ticker.ask is None
        assert ticker.volume is None

    def test_full_creation_with_valid_data(self) -> None:
        """Test creating a Ticker with all fields populated with valid data types."""
        ticker = Ticker(
            symbol=VALID_SYMBOL,
            timestamp=NOW,
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
            volume=Decimal("1234.56"),
        )
        assert ticker.symbol == VALID_SYMBOL
        assert ticker.timestamp == NOW
        assert ticker.price == Decimal("50000.5")
        assert ticker.bid == Decimal("50000.0")
        assert ticker.ask == Decimal("50001.0")
        assert ticker.volume == Decimal("1234.56")

    def test_creation_with_parsable_data(self) -> None:
        """Test creating a Ticker with data needing parsing (str, int, float)."""
        ms_timestamp = int(NOW.timestamp() * 1000)
        # Calculate expected time after ms conversion loss
        expected_dt_from_ms = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)

        ticker = Ticker(
            symbol=VALID_SYMBOL,
            timestamp=ms_timestamp,  # Test int parsing
            price="50000.5",  # Test str parsing
            bid=50000.0,  # Test float parsing
            ask=50001,  # Test int parsing
            volume="1234.56",  # Test str parsing
        )
        # Compare timestamp to the value expected after ms conversion precision loss
        assert ticker.timestamp == expected_dt_from_ms
        assert ticker.price == Decimal("50000.5")
        assert ticker.bid == Decimal("50000.0")
        assert ticker.ask == Decimal("50001")
        assert ticker.volume == Decimal("1234.56")

    # --- Validation Tests --- #

    def test_required_fields_validation(self) -> None:
        """Test that required fields (symbol, timestamp) raise errors if missing."""
        with pytest.raises(ValidationError, match="Field required"):
            Ticker(timestamp=NOW)  # type: ignore[call-arg] # Missing symbol
        with pytest.raises(ValidationError, match="Field required"):
            Ticker(symbol=VALID_SYMBOL)  # type: ignore[call-arg] # Missing timestamp

    def test_symbol_validation(self) -> None:
        """Test validation rules for the symbol field (required, non-empty, length)."""
        with pytest.raises(ValueError, match="Field symbol: String cannot be empty"):
            Ticker(symbol="", timestamp=NOW)
        with pytest.raises(ValueError, match="Field symbol: String cannot be empty"):
            Ticker(symbol="   ", timestamp=NOW)
        with pytest.raises(ValueError, match="String value too long"):
            Ticker(symbol="A" * 65, timestamp=NOW)
        # Valid symbol should pass
        Ticker(symbol="VALID-SYM_123", timestamp=NOW)

    def test_timestamp_validation(self) -> None:
        """Test timestamp validation (required, parsing, None handling)."""
        # Test None raises error
        with pytest.raises(ValueError, match="timestamp must not be None"):
            Ticker(symbol=VALID_SYMBOL, timestamp=None)

        # Test invalid format raises error (Pydantic wraps underlying errors)
        with pytest.raises(
            ValidationError,
            match=r"timestamp.*Cannot parse string .* as ISO datetime .* or as numeric timestamp",
        ):
            Ticker(symbol=VALID_SYMBOL, timestamp="invalid-date-string")

        # Test valid parsing (already covered in test_creation_with_parsable_data)
        ms_timestamp = int(NOW.timestamp() * 1000)
        iso_timestamp = NOW.isoformat().replace("+00:00", "Z")
        expected_dt_from_ms = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)

        # Ignore needed for passing int timestamp to validator
        assert Ticker(symbol=VALID_SYMBOL, timestamp=ms_timestamp).timestamp == expected_dt_from_ms
        # Ignore needed for passing str timestamp to validator
        assert Ticker(symbol=VALID_SYMBOL, timestamp=iso_timestamp).timestamp == NOW
        assert Ticker(symbol=VALID_SYMBOL, timestamp=NOW).timestamp == NOW

    @pytest.mark.parametrize("field_name", ["price", "bid", "ask", "volume"])
    def test_decimal_fields_parsing_and_validation(self, field_name: str) -> None:
        """Test parsing, finiteness, and non-negativity for optional decimal fields."""
        valid_kwargs_base: dict[str, Any] = {"symbol": VALID_SYMBOL, "timestamp": NOW}

        # Helper function to create Ticker instance and get attribute
        def get_ticker_field_value(value: str | int | float | Decimal | None) -> Decimal | None:
            kwargs = valid_kwargs_base.copy()
            kwargs[field_name] = value
            # Ignore arg-type specifically for the field being parameterized,
            # as we are intentionally passing raw types (str, int, float, None).
            ticker_instance = Ticker(**kwargs)
            value_out = getattr(ticker_instance, field_name)
            # Assert type for static analysis before returning
            assert isinstance(value_out, Decimal | None), (
                f"Expected Decimal or None, got {type(value_out)}"
            )
            return value_out

        # Test valid parsing
        assert get_ticker_field_value("123.45") == Decimal("123.45")
        assert get_ticker_field_value(123) == DEC_ONE * 123
        assert get_ticker_field_value(123.0) == DEC_ONE * 123
        assert get_ticker_field_value(DEC_ONE) == DEC_ONE
        assert get_ticker_field_value(DEC_ZERO) == DEC_ZERO
        assert get_ticker_field_value(None) is None

        # Test invalid parsing (Pydantic wraps underlying errors)
        # Match needs to accommodate the field name which might be included
        with pytest.raises(ValidationError, match=rf"Value error, {field_name}: Cannot convert"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value("not-a-number")

        # Test non-finite values (handled by custom validator)
        with pytest.raises(ValidationError, match="finite Decimal"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_NAN)
        with pytest.raises(ValidationError, match="finite Decimal"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_INF)
        with pytest.raises(ValidationError, match="finite Decimal"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_NEG_INF)

        # Test non-negativity (ge=0 handled by Field)
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            # Ignore arg-type needed here as the exception happens during Ticker init
            # when passing a string that *would* parse to a negative Decimal.
            get_ticker_field_value("-0.001")

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields raise ValidationError (extra='forbid')."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            Ticker(
                symbol=VALID_SYMBOL,
                timestamp=NOW,
                extra_field="invalid",  # type: ignore[call-arg]
            )

    def test_immutability(self) -> None:
        """Test that the Ticker model is immutable (frozen=True)."""
        ticker = Ticker(symbol=VALID_SYMBOL, timestamp=NOW, price=DEC_ONE)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.symbol = "NEW-SYM"
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.timestamp = NOW + timedelta(seconds=1)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.price = DEC_ZERO
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.bid = DEC_ONE
        # Setting non-existent field
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.new_field = "test"  # type: ignore[attr-defined]
