from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.funding_rate import (
    BackpackFundingDetails,
    FundingRate,
    HyperliquidFundingDetails,
)


class TestFundingRate:
    def test_core_required_fields(self) -> None:
        """Test that required fields are actually required."""
        # Symbol and timestamp are required
        with pytest.raises(ValidationError, match="1 validation error"):
            # We intentionally omit required 'symbol' to test validation
            kwargs1: dict[str, Any] = {"timestamp": datetime.now(UTC)}
            FundingRate(**kwargs1)

        with pytest.raises(ValidationError, match="1 validation error"):
            # We intentionally omit required 'timestamp' to test validation
            kwargs2: dict[str, Any] = {"symbol": "BTC-PERP"}
            FundingRate(**kwargs2)

        # Both required fields present should succeed
        fr = FundingRate(symbol="BTC-PERP", timestamp=datetime.now(UTC))
        assert fr.symbol == "BTC-PERP"
        assert isinstance(fr.timestamp, datetime)

    def test_core_minimal_creation(self) -> None:
        """Test creating a minimal FundingRate with only required fields."""
        now = datetime.now(UTC)
        fr = FundingRate(symbol="BTC-PERP", timestamp=now)

        assert fr.symbol == "BTC-PERP"
        assert fr.timestamp == now
        assert fr.funding_rate is None
        assert fr.predicted_rate is None
        assert fr.mark_price is None
        assert fr.index_price is None
        assert fr.next_funding_time is None
        assert fr.hl_details is None
        assert fr.bp_details is None

    def test_core_complete_creation(self) -> None:
        """Test creating a FundingRate with all core fields."""
        now = datetime.now(UTC)
        # Use timedelta for safe time addition
        next_time = now + timedelta(hours=1)
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            predicted_rate=Decimal("0.0002"),
            mark_price=Decimal("50000.0"),
            index_price=Decimal("49950.0"),
            next_funding_time=next_time,
        )

        assert funding_rate.symbol == "BTC-PERP"
        assert funding_rate.timestamp == now
        assert funding_rate.funding_rate == Decimal("0.0001")
        assert funding_rate.predicted_rate == Decimal("0.0002")
        assert funding_rate.mark_price == Decimal("50000.0")
        assert funding_rate.index_price == Decimal("49950.0")
        assert funding_rate.next_funding_time == next_time
        assert funding_rate.hl_details is None
        assert funding_rate.bp_details is None

    def test_core_with_extension_slots_populated(self) -> None:
        """Test creating a FundingRate with extension slots populated."""
        now = datetime.now(UTC)
        hl_details = HyperliquidFundingDetails(
            hl_funding_hourly=Decimal("0.0003"),
            hl_prev_day_px=Decimal("48000.0"),
            hl_day_ntl_vlm=Decimal("1000000.0"),
            hl_impact_px=Decimal("50100.0"),
        )

        bp_details = BackpackFundingDetails()

        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal("50000.0"),
            hl_details=hl_details,
            bp_details=bp_details,
        )

        assert fr.symbol == "BTC-PERP"
        assert fr.timestamp == now
        assert fr.funding_rate == Decimal("0.0001")
        assert fr.mark_price == Decimal("50000.0")
        assert fr.hl_details == hl_details
        assert fr.bp_details == bp_details
        assert isinstance(fr.hl_details, HyperliquidFundingDetails)
        assert isinstance(fr.bp_details, BackpackFundingDetails)
        assert fr.hl_details.hl_funding_hourly == Decimal("0.0003")

    def test_core_with_extension_slots_none(self) -> None:
        """Test creating a FundingRate with extension slots explicitly None."""
        now = datetime.now(UTC)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal("50000.0"),
            hl_details=None,
            bp_details=None,
        )

        assert fr.symbol == "BTC-PERP"
        assert fr.timestamp == now
        assert fr.hl_details is None
        assert fr.bp_details is None

    def test_validation_symbol(self) -> None:
        """Test symbol validation in FundingRate."""
        now = datetime.now(UTC)

        # Empty symbol
        with pytest.raises(ValueError, match="String cannot be empty or whitespace"):
            FundingRate(symbol="", timestamp=now)

        # Whitespace symbol
        with pytest.raises(ValueError, match="String cannot be empty or whitespace"):
            FundingRate(symbol="   ", timestamp=now)

        # Symbol too long (max 64 chars)
        with pytest.raises(ValueError, match="String value too long"):
            FundingRate(symbol="X" * 65, timestamp=now)

        # Valid symbol
        fr = FundingRate(symbol="BTC-PERP", timestamp=now)
        assert fr.symbol == "BTC-PERP"

    def test_validation_timestamp_required(self) -> None:
        """Test that timestamp is required and must not be None."""
        # None timestamp - we're explicitly testing validator behavior with None
        with pytest.raises(ValueError, match="timestamp must not be None"):
            # Pass None to a non-optional field to test validator behavior
            kwargs3: dict[str, Any] = {"symbol": "BTC-PERP", "timestamp": None}
            FundingRate(**kwargs3)

    def test_validation_timestamp_parsing(self) -> None:
        """Test timestamp parsing in FundingRate."""
        # Test with integer timestamps (unix timestamp in ms)
        ms_timestamp = 1647395427000
        # Pass integers where datetime is expected (parsing is handled in the model)
        ms_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": ms_timestamp,
            "next_funding_time": ms_timestamp,
        }
        fr = FundingRate(**ms_kwargs)
        assert isinstance(fr.timestamp, datetime)
        assert isinstance(fr.next_funding_time, datetime)
        assert fr.timestamp.tzinfo is not None
        assert fr.next_funding_time is not None and fr.next_funding_time.tzinfo is not None

        # Test with string ISO timestamps
        iso_timestamp = "2023-03-16T12:00:00Z"
        # Pass strings where datetime is expected (parsing is handled in the model)
        iso_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": iso_timestamp,
            "next_funding_time": iso_timestamp,
        }
        fr = FundingRate(**iso_kwargs)
        assert fr.timestamp is not None
        assert fr.next_funding_time is not None
        assert fr.timestamp.year == 2023
        assert fr.timestamp.month == 3
        assert fr.timestamp.day == 16
        assert fr.timestamp.hour == 12

        # Test with datetime objects (non-UTC should be converted to UTC)
        naive_dt = datetime(2023, 3, 16, 12, 0, 0)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=naive_dt,
        )
        assert fr.timestamp.tzinfo is not None

        # Already UTC-aware datetime should remain as is
        utc_dt = datetime(2023, 3, 16, 12, 0, 0, tzinfo=UTC)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=utc_dt,
        )
        assert fr.timestamp.tzinfo is UTC
        assert fr.timestamp == utc_dt

    def test_validation_price_parsing(self) -> None:
        """Test price/rate parsing in FundingRate."""
        now = datetime.now(UTC)

        # Test with string decimals
        # Pass strings where Decimal is expected (parsing is handled in the model)
        decimal_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "funding_rate": "0.0001",
            "predicted_rate": "0.0002",
            "mark_price": "50000.0",
            "index_price": "49950.0",
        }
        fr = FundingRate(**decimal_kwargs)
        assert isinstance(fr.funding_rate, Decimal)
        assert isinstance(fr.predicted_rate, Decimal)
        assert isinstance(fr.mark_price, Decimal)
        assert isinstance(fr.index_price, Decimal)
        assert fr.funding_rate == Decimal("0.0001")
        assert fr.mark_price == Decimal("50000.0")

        # Test with floats (should be converted to Decimal)
        # Pass floats where Decimal is expected (parsing is handled in the model)
        float_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "funding_rate": 0.0001,
            "mark_price": 50000.0,
        }
        fr = FundingRate(**float_kwargs)
        assert isinstance(fr.funding_rate, Decimal)
        assert isinstance(fr.mark_price, Decimal)

        # Test with comma-separated numbers
        # Pass comma-formatted string where Decimal is expected
        comma_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "mark_price": "50,000.0",
        }
        fr = FundingRate(**comma_kwargs)
        assert fr.mark_price == Decimal("50000.0")

    def test_validation_price_constraints(self) -> None:
        """Test price field constraints (prices must be positive)."""
        now = datetime.now(UTC)

        # Zero mark price (should fail)
        with pytest.raises(ValidationError, match="mark_price"):
            FundingRate(
                symbol="BTC-PERP",
                timestamp=now,
                mark_price=Decimal("0.0"),
            )

        # Negative mark price (should fail)
        with pytest.raises(ValidationError, match="mark_price"):
            FundingRate(
                symbol="BTC-PERP",
                timestamp=now,
                mark_price=Decimal("-1.0"),
            )

        # Zero index price (should fail)
        with pytest.raises(ValidationError, match="index_price"):
            FundingRate(
                symbol="BTC-PERP",
                timestamp=now,
                index_price=Decimal("0.0"),
            )

        # Negative index price (should fail)
        with pytest.raises(ValidationError, match="index_price"):
            FundingRate(
                symbol="BTC-PERP",
                timestamp=now,
                index_price=Decimal("-1.0"),
            )

        # Positive prices (should pass)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            mark_price=Decimal("0.0001"),
            index_price=Decimal("0.0001"),
        )
        assert fr.mark_price == Decimal("0.0001")
        assert fr.index_price == Decimal("0.0001")

    def test_validation_finite_decimals(self) -> None:
        """Test validation of finite decimal values."""
        now = datetime.now(UTC)

        # Infinity not allowed for prices
        # Pass non-finite value to test validator
        infinity_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "mark_price": Decimal("Infinity"),
        }
        with pytest.raises(ValueError, match="must be a finite decimal"):
            FundingRate(**infinity_kwargs)

        # NaN not allowed for rates
        # Pass non-finite value to test validator
        nan_kwargs: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "timestamp": now,
            "funding_rate": Decimal("NaN"),
        }
        with pytest.raises(ValueError, match="must be a finite decimal"):
            FundingRate(**nan_kwargs)

    def test_validation_rates_zero_allowed(self) -> None:
        """Test that funding_rate and predicted_rate can be zero (unlike prices)."""
        now = datetime.now(UTC)

        # Zero funding rate (should pass)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("0.0"),
        )
        assert fr.funding_rate == Decimal("0.0")

        # Zero predicted rate (should pass)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            predicted_rate=Decimal("0.0"),
        )
        assert fr.predicted_rate == Decimal("0.0")

        # Negative rates are also allowed
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("-0.0001"),
            predicted_rate=Decimal("-0.0002"),
        )
        assert fr.funding_rate == Decimal("-0.0001")
        assert fr.predicted_rate == Decimal("-0.0002")

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden in FundingRate."""
        now = datetime.now(UTC)

        # Attempt to pass an extra field that doesn't exist in the model
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            # Using Any to bypass type checking for extra_field
            extra_field_kwargs: dict[str, Any] = {
                "symbol": "BTC-PERP",
                "timestamp": now,
                "extra_field": "value",
            }
            FundingRate(**extra_field_kwargs)

    def test_immutability(self) -> None:
        """Test that FundingRate is immutable."""
        now = datetime.now(UTC)
        fr = FundingRate(
            symbol="BTC-PERP",
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal("50000.0"),
        )

        # Attempt to modify attributes - should raise ValidationError due to frozen=True
        with pytest.raises(ValidationError):
            # Using cast to bypass type checking for setattr operations on frozen objects
            cast(Any, fr).symbol = "ETH-PERP"

        with pytest.raises(ValidationError):
            cast(Any, fr).timestamp = now + timedelta(hours=1)

        with pytest.raises(ValidationError):
            cast(Any, fr).funding_rate = Decimal("0.0002")

        with pytest.raises(ValidationError):
            cast(Any, fr).mark_price = Decimal("51000.0")


class TestHyperliquidFundingDetails:
    def test_creation(self) -> None:
        """Test creating HyperliquidFundingDetails."""
        hl = HyperliquidFundingDetails(
            hl_funding_hourly=Decimal("0.0003"),
            hl_prev_day_px=Decimal("48000.0"),
            hl_day_ntl_vlm=Decimal("1000000.0"),
            hl_impact_px=Decimal("50100.0"),
        )

        assert hl.hl_funding_hourly == Decimal("0.0003")
        assert hl.hl_prev_day_px == Decimal("48000.0")
        assert hl.hl_day_ntl_vlm == Decimal("1000000.0")
        assert hl.hl_impact_px == Decimal("50100.0")

    def test_string_parsing(self) -> None:
        """Test string parsing for decimal fields."""
        # Pass strings where Decimal is expected (parsing is handled in the model)
        # Using Any to bypass type checking for string values
        hl_string_kwargs: dict[str, Any] = {
            "hl_funding_hourly": "0.0003",
            "hl_prev_day_px": "48000.0",
            "hl_day_ntl_vlm": "1000000.0",
            "hl_impact_px": "50100.0",
        }
        hl = HyperliquidFundingDetails(**hl_string_kwargs)

        assert isinstance(hl.hl_funding_hourly, Decimal)
        assert isinstance(hl.hl_prev_day_px, Decimal)
        assert isinstance(hl.hl_day_ntl_vlm, Decimal)
        assert isinstance(hl.hl_impact_px, Decimal)

    def test_validation_finite_decimals(self) -> None:
        """Test that Decimal fields must be finite."""
        # Infinity not allowed
        # Pass non-finite value to test validator
        with pytest.raises(ValueError, match="must be a finite decimal"):
            # Using Any to bypass type checking for non-finite Decimal
            hl_infinity_kwargs: dict[str, Any] = {"hl_funding_hourly": Decimal("Infinity")}
            HyperliquidFundingDetails(**hl_infinity_kwargs)

        # NaN not allowed
        # Pass non-finite value to test validator
        with pytest.raises(ValueError, match="must be a finite decimal"):
            # Using Any to bypass type checking for non-finite Decimal
            hl_nan_kwargs: dict[str, Any] = {"hl_prev_day_px": Decimal("NaN")}
            HyperliquidFundingDetails(**hl_nan_kwargs)

        # Finite decimals allowed (including zero and negative)
        hl = HyperliquidFundingDetails(
            hl_funding_hourly=Decimal("0.0"),
            hl_prev_day_px=Decimal("-1.0"),
        )
        assert hl.hl_funding_hourly == Decimal("0.0")
        assert hl.hl_prev_day_px == Decimal("-1.0")

    def test_extra_fields_ignored(self) -> None:
        """Test that extra fields are ignored in HyperliquidFundingDetails."""
        # Attempt to pass an extra field that doesn't exist in the model
        # Using Any to bypass type checking for extra_field
        hl_extra_kwargs: dict[str, Any] = {
            "hl_funding_hourly": Decimal("0.0003"),
            "extra_field": "value",
        }
        hl = HyperliquidFundingDetails(**hl_extra_kwargs)

        assert hl.hl_funding_hourly == Decimal("0.0003")
        # extra_field should be ignored
        assert not hasattr(hl, "extra_field")

    def test_immutability(self) -> None:
        """Test that HyperliquidFundingDetails is immutable."""
        hl = HyperliquidFundingDetails(hl_funding_hourly=Decimal("0.0003"))

        # Attempt to modify attributes - should raise ValidationError due to frozen=True
        with pytest.raises(ValidationError):
            # Using cast to bypass type checking for setattr operations on frozen objects
            cast(Any, hl).hl_funding_hourly = Decimal("0.0004")


class TestBackpackFundingDetails:
    def test_creation(self) -> None:
        """Test creating empty BackpackFundingDetails."""
        # Create an instance to verify it can be instantiated without errors
        bp = BackpackFundingDetails()
        assert isinstance(bp, BackpackFundingDetails)

    def test_extra_fields_ignored(self) -> None:
        """Test that extra fields are ignored in BackpackFundingDetails."""
        # Attempt to pass an extra field that doesn't exist in the model
        # Using Any to bypass type checking for extra_field
        bp_extra_kwargs: dict[str, Any] = {"extra_field": "value"}
        bp = BackpackFundingDetails(**bp_extra_kwargs)

        # extra_field should be ignored
        assert not hasattr(bp, "extra_field")

    def test_immutability(self) -> None:
        """Test that BackpackFundingDetails is immutable."""
        bp = BackpackFundingDetails()

        # We don't have any fields to test modification on, but we can test
        # that we can't add new attributes directly
        with pytest.raises(ValidationError):
            # Using cast to bypass type checking for setattr operations on frozen objects
            cast(Any, bp).new_field = "value"
