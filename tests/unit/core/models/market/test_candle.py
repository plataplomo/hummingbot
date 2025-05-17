from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.candle import Candle

# Constants for testing
NOW: datetime = datetime.now(UTC)
VALID_SYMBOL: str = "BTC-PERP"
VALID_INTERVAL: str = "1m"
DEC_ZERO: Decimal = Decimal("0")
DEC_ONE: Decimal = Decimal("1")
DEC_TEN: Decimal = Decimal("10")
DEC_NEG_ONE: Decimal = Decimal("-1")
DEC_NAN: Decimal = Decimal("NaN")
DEC_INF: Decimal = Decimal("Infinity")


# Helper function to create valid candle data easily
def create_valid_candle_data(**overrides: object) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "symbol": VALID_SYMBOL,
        "interval": VALID_INTERVAL,
        "open_time": NOW,
        "open": Decimal("100.0"),
        "high": Decimal("105.0"),
        "low": Decimal("95.0"),
        "close": Decimal("102.0"),
        "volume": Decimal("1000.0"),
    }
    defaults.update(overrides)
    return defaults


class TestCandle:
    """Unit tests for the cyberdelta.core.models.market.candle.Candle model."""

    def test_valid_creation(self) -> None:
        """Test creating a Candle with valid data."""
        data = create_valid_candle_data()
        candle = Candle(**data)
        assert candle.symbol == VALID_SYMBOL
        assert candle.interval == VALID_INTERVAL
        assert candle.open_time == NOW
        assert candle.open == Decimal("100.0")
        assert candle.high == Decimal("105.0")
        assert candle.low == Decimal("95.0")
        assert candle.close == Decimal("102.0")
        assert candle.volume == Decimal("1000.0")

    def test_creation_with_parsable_data(self) -> None:
        """Test creating a Candle with data needing parsing (str, int, float)."""
        ms_timestamp = int(NOW.timestamp() * 1000)
        expected_dt_from_ms = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)
        data = {
            "symbol": VALID_SYMBOL,
            "interval": VALID_INTERVAL,
            "open_time": ms_timestamp,  # Test int parsing
            "open": "100.0",  # Test str parsing
            "high": 105.0,  # Test float parsing
            "low": 95,  # Test int parsing
            "close": "102.0",  # Test str parsing
            "volume": 1000,  # Test int parsing
        }
        # Mypy accepts the dict with raw types, validation happens in Candle init
        candle = Candle(**data)  # type: ignore[arg-type] # Ignore needed for Pyright/overall call type mismatch
        assert candle.open_time == expected_dt_from_ms
        assert candle.open == Decimal("100.0")
        assert candle.high == Decimal("105.0")
        assert candle.low == Decimal("95")
        assert candle.close == Decimal("102.0")
        assert candle.volume == Decimal("1000")

    # --- Field Validation Tests --- #

    @pytest.mark.parametrize(
        "field", ["symbol", "interval", "open_time", "open", "high", "low", "close", "volume"]
    )
    def test_required_fields(self, field: str) -> None:
        """Test that required fields raise errors if missing."""
        data = create_valid_candle_data()
        del data[field]
        with pytest.raises(ValidationError, match="Field required"):
            Candle(**data)

    def test_symbol_validation(self) -> None:
        """Test validation rules for the symbol field."""
        with pytest.raises(ValueError, match="String cannot be empty"):
            Candle(**create_valid_candle_data(symbol=""))
        with pytest.raises(ValueError, match="String cannot be empty"):
            Candle(**create_valid_candle_data(symbol="   "))
        with pytest.raises(ValueError, match="String value too long"):
            Candle(**create_valid_candle_data(symbol="A" * 65))

    def test_interval_validation(self) -> None:
        """Test validation rules for the interval field."""
        with pytest.raises(ValueError, match="String cannot be empty"):
            Candle(**create_valid_candle_data(interval=""))
        with pytest.raises(ValueError, match="String cannot be empty"):
            Candle(**create_valid_candle_data(interval="    "))
        with pytest.raises(ValueError, match="String value too long"):
            Candle(**create_valid_candle_data(interval="A" * 17))

    def test_open_time_validation(self) -> None:
        """Test open_time validation (required, parsing, None handling)."""
        with pytest.raises(ValueError, match="open_time must not be None"):
            Candle(**create_valid_candle_data(open_time=None))
        with pytest.raises(
            ValidationError,
            match=r"open_time.*Cannot parse string .* as ISO datetime .* or as numeric timestamp",
        ):
            Candle(**create_valid_candle_data(open_time="invalid-date"))

    @pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
    def test_decimal_parsing_finiteness_required(self, field: str) -> None:
        """Test parsing, finiteness for required Decimal fields (OHLCV)."""
        # Test None fails (validator raises ValueError before Pydantic)
        with pytest.raises(ValueError, match=f"Field '{field}' cannot be None"):
            Candle(**create_valid_candle_data(**{field: None}))

        # Test invalid parsing
        with pytest.raises(ValidationError, match=rf"Value error, {field}: Cannot convert"):
            Candle(**create_valid_candle_data(**{field: "not-a-number"}))

        # Test non-finite values
        with pytest.raises(ValidationError, match=rf"Field '{field}' must be a finite Decimal"):
            Candle(**create_valid_candle_data(**{field: DEC_NAN}))
        with pytest.raises(ValidationError, match=rf"Field '{field}' must be a finite Decimal"):
            Candle(**create_valid_candle_data(**{field: DEC_INF}))

    @pytest.mark.parametrize("field", ["open", "high", "low", "close"])
    def test_positive_price_validation(self, field: str) -> None:
        """Test that OHLC prices must be strictly positive (> 0)."""
        with pytest.raises(ValidationError, match="Input should be greater than 0"):
            Candle(**create_valid_candle_data(**{field: DEC_ZERO}))
        with pytest.raises(ValidationError, match="Input should be greater than 0"):
            Candle(**create_valid_candle_data(**{field: DEC_NEG_ONE}))

    def test_non_negative_volume_validation(self) -> None:
        """Test that volume must be non-negative (>= 0)."""
        # Zero volume is allowed
        Candle(**create_valid_candle_data(volume=DEC_ZERO))
        Candle(**create_valid_candle_data(volume="0.0"))

        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            Candle(**create_valid_candle_data(volume=DEC_NEG_ONE))
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            # Mypy accepts str here, validation in Candle init
            Candle(**create_valid_candle_data(volume="-0.1"))

    # --- Model Level Validation --- #

    def test_ohlc_consistency_validation(self) -> None:
        """Test the model-level OHLC consistency validation."""
        # high < low
        with pytest.raises(ValueError, match="high .* must be >= low"):
            Candle(**create_valid_candle_data(high=Decimal("90"), low=Decimal("95")))
        # high < open
        with pytest.raises(ValueError, match="high .* must be >= open"):
            Candle(**create_valid_candle_data(high=Decimal("99"), open=Decimal("100")))
        # high < close
        with pytest.raises(ValueError, match="high .* must be >= close"):
            Candle(**create_valid_candle_data(high=Decimal("101"), close=Decimal("102")))
        # low > open
        with pytest.raises(ValueError, match="low .* must be <= open"):
            Candle(**create_valid_candle_data(low=Decimal("101"), open=Decimal("100")))
        # low > close - This case also violates low > open, which is checked first.
        with pytest.raises(ValueError, match="low .* must be <= open"):
            Candle(**create_valid_candle_data(low=Decimal("103"), close=Decimal("102")))

        # Valid case (already tested in test_valid_creation, but good to be explicit)
        Candle(**create_valid_candle_data())

    # --- Other Model Config Tests --- #

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields raise ValidationError (extra='forbid')."""
        data = create_valid_candle_data()
        data["extra_field"] = "invalid"
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            Candle(**data)

    def test_immutability(self) -> None:
        """Test that the Candle model is immutable (frozen=True)."""
        candle = Candle(**create_valid_candle_data())

        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.symbol = "ETH-PERP"
        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.open_time = NOW
        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.high = DEC_TEN
        # Setting non-existent field
        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.new_field = "test"  # type: ignore[attr-defined]
