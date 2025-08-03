"""Unit tests for the core Ticker model.

Tests validation, parsing, immutability, and decimal field handling.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.ticker import (
    BackpackTickerDetails,
    HyperliquidTickerDetails,
    Ticker,
)
from cyberdelta.exceptions.parsing import DateTimeParsingError
from tests.common_symbols import BTC_BP, BTC_HL, ETH_HL


pytestmark = pytest.mark.timing

# Constants for testing
NOW: datetime = datetime.now(UTC)
DEC_ZERO: Decimal = Decimal(0)
DEC_ONE: Decimal = Decimal(1)
DEC_NEG_ONE: Decimal = Decimal(-1)
DEC_NAN: Decimal = Decimal("NaN")
DEC_INF: Decimal = Decimal("Infinity")
DEC_NEG_INF: Decimal = Decimal("-Infinity")


class TestTicker:
    """Unit tests for the cyberdelta.core.models.market.ticker.Ticker model."""

    def test_minimal_creation_required_fields(self) -> None:
        """Test creating a Ticker with only required fields (symbol, exchange, timestamp)."""
        btc_symbol = BTC_HL
        ticker = Ticker(symbol=btc_symbol, exchange="test_exchange", timestamp=NOW)
        assert ticker.symbol == btc_symbol
        assert ticker.timestamp == NOW
        assert ticker.price is None
        assert ticker.bid is None
        assert ticker.ask is None
        assert ticker.volume is None

    def test_full_creation_with_valid_data(self) -> None:
        """Test creating a Ticker with all fields populated with valid data types."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
            volume=Decimal("1234.56"),
        )
        assert ticker.symbol == btc_symbol
        assert ticker.timestamp == NOW
        assert ticker.price == Decimal("50000.5")
        assert ticker.bid == Decimal("50000.0")
        assert ticker.ask == Decimal("50001.0")
        assert ticker.volume == Decimal("1234.56")

    def test_creation_with_parsable_data(self) -> None:
        """Test creating a Ticker with data needing parsing (str, int, float)."""
        btc_symbol = BTC_HL
        ms_timestamp = int(NOW.timestamp() * 1000)
        # Calculate expected time after ms conversion loss
        expected_dt_from_ms = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)

        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=expected_dt_from_ms,  # Use the already calculated datetime
            price=Decimal("50000.5"),  # Use Decimal
            bid=Decimal("50000.0"),  # Use Decimal
            ask=Decimal(50001),  # Use Decimal
            volume=Decimal("1234.56"),  # Use Decimal
        )
        # Compare timestamp to the value expected after ms conversion precision loss
        assert ticker.timestamp == expected_dt_from_ms
        assert ticker.price == Decimal("50000.5")
        assert ticker.bid == Decimal("50000.0")
        assert ticker.ask == Decimal(50001)
        assert ticker.volume == Decimal("1234.56")

    # --- Validation Tests --- #

    def test_required_fields_validation(self) -> None:
        """Test that required fields (symbol, timestamp) raise errors if missing."""
        btc_symbol = BTC_HL
        with pytest.raises(ValidationError, match="Field required"):
            Ticker(exchange="test_exchange", timestamp=NOW)  # type: ignore[call-arg] # Missing symbol
        with pytest.raises(ValidationError, match="Field required"):
            Ticker(symbol=btc_symbol, exchange="test_exchange")  # type: ignore[call-arg] # Missing timestamp
        with pytest.raises(ValidationError, match="Field required"):
            Ticker(symbol=btc_symbol, timestamp=NOW)  # type: ignore[call-arg] # Missing exchange

    def test_symbol_validation(self) -> None:
        """Test validation rules for the symbol field (Symbol objects and string validation)."""
        from cyberdelta.core.symbols.api import symbol

        # Test that empty strings are rejected at Symbol creation level
        with pytest.raises(ValidationError, match="String should have at least 1 character"):
            symbol("", "hyperliquid")

        # Test that very long strings are rejected at Symbol creation level
        with pytest.raises(ValidationError, match="String should have at most 30 characters"):
            symbol("A" * 31, "hyperliquid")

        # Test that string symbols are rejected for Ticker (only Symbol objects accepted)
        with pytest.raises(ValidationError):
            Ticker(symbol="BTC-PERP", exchange="test_exchange", timestamp=NOW)  # type: ignore[arg-type]

        # Valid symbol should pass
        valid_symbol = symbol("VALID-SYM_123", "hyperliquid")
        ticker = Ticker(
            symbol=valid_symbol,
            exchange="test_exchange",
            timestamp=NOW,
        )
        assert ticker.symbol == valid_symbol

        # Whitespace-only symbols are allowed (edge case)
        whitespace_symbol = symbol("   ", "hyperliquid")
        whitespace_ticker = Ticker(
            symbol=whitespace_symbol,
            exchange="test_exchange",
            timestamp=NOW,
        )
        assert whitespace_ticker.symbol == whitespace_symbol

    def test_timestamp_validation(self) -> None:
        """Test timestamp validation (required, parsing, None handling)."""
        btc_symbol = BTC_HL
        # Test None raises error
        with pytest.raises(ValidationError, match=r"timestamp.*Ticker timestamp is required"):
            # Use Any to test validator behavior
            kwargs: dict[str, Any] = {
                "symbol": btc_symbol,
                "exchange": "test_exchange",
                "timestamp": None,
            }
            Ticker(**kwargs)

        # Test invalid format raises error (Pydantic wraps underlying errors)
        with pytest.raises(
            DateTimeParsingError, match=r"Cannot parse as ISO datetime.*or as numeric timestamp"
        ):
            # Use Any to test validator behavior
            kwargs_invalid: dict[str, Any] = {
                "symbol": btc_symbol,
                "exchange": "test_exchange",
                "timestamp": "invalid-date-string",
            }
            Ticker(**kwargs_invalid)

        # Test valid parsing (already covered in test_creation_with_parsable_data)
        ms_timestamp = int(NOW.timestamp() * 1000)
        iso_timestamp = NOW.isoformat().replace("+00:00", "Z")
        expected_dt_from_ms = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)

        # Test int timestamp parsing
        kwargs_int: dict[str, Any] = {
            "symbol": btc_symbol,
            "exchange": "test_exchange",
            "timestamp": ms_timestamp,
        }
        assert Ticker(**kwargs_int).timestamp == expected_dt_from_ms
        # Test string timestamp parsing
        kwargs_str: dict[str, Any] = {
            "symbol": btc_symbol,
            "exchange": "test_exchange",
            "timestamp": iso_timestamp,
        }
        assert Ticker(**kwargs_str).timestamp == NOW
        assert Ticker(symbol=btc_symbol, exchange="test_exchange", timestamp=NOW).timestamp == NOW

    @pytest.mark.parametrize("field_name", ["price", "bid", "ask", "volume"])
    def test_decimal_fields_parsing_and_validation(self, field_name: str) -> None:
        """Test parsing, finiteness, and non-negativity for optional decimal fields."""
        btc_symbol = BTC_HL
        valid_kwargs_base: dict[str, Any] = {
            "symbol": btc_symbol,
            "exchange": "test_exchange",
            "timestamp": NOW,
        }

        # Helper function to create Ticker instance and get attribute
        def get_ticker_field_value(value: str | float | Decimal | None) -> Decimal | None:
            """Get ticker field value for testing.

            Returns:
                Decimal | None: The processed field value from the ticker instance.
            """
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
        with pytest.raises(
            ValidationError,
            match=rf"Field '{field_name}' decimal validation failed.*Cannot convert to Decimal",
        ):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value("not-a-number")

        # Test non-finite values (handled by custom validator)
        with pytest.raises(ValidationError, match="must be finite for ticker price data"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_NAN)
        with pytest.raises(ValidationError, match="must be finite for ticker price data"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_INF)
        with pytest.raises(ValidationError, match="must be finite for ticker price data"):
            # No ignore needed here, exception is expected before getattr
            get_ticker_field_value(DEC_NEG_INF)

        # Test non-negativity (ge=0 handled by Field)
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            # Ignore arg-type needed here as the exception happens during Ticker init
            # when passing a string that *would* parse to a negative Decimal.
            get_ticker_field_value("-0.001")

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields raise ValidationError (extra='forbid')."""
        btc_symbol = BTC_HL
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            Ticker(
                symbol=btc_symbol,
                exchange="test_exchange",
                timestamp=NOW,
                extra_field="invalid",  # type: ignore[call-arg]
            )

    def test_immutability(self) -> None:
        """Test that the Ticker model is immutable (frozen=True)."""
        btc_symbol = BTC_HL
        ticker = Ticker(symbol=btc_symbol, exchange="test_exchange", timestamp=NOW, price=DEC_ONE)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.symbol = ETH_HL
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.timestamp = NOW + timedelta(seconds=1)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.price = DEC_ZERO
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.bid = DEC_ONE
        # Setting non-existent field
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.new_field = "test"  # type: ignore[attr-defined]

    # --- Mid-Price Calculation Tests --- #

    def test_mid_price_valid_calculation(self) -> None:
        """Test mid_price calculation with valid bid and ask prices."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=Decimal("100.0"),
            ask=Decimal("102.0"),
        )
        assert ticker.mid_price == Decimal("101.0")

    def test_mid_price_precise_calculation(self) -> None:
        """Test mid_price calculation preserves decimal precision."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=Decimal("100.123"),
            ask=Decimal("100.567"),
        )
        expected = (Decimal("100.123") + Decimal("100.567")) / Decimal(2)
        assert ticker.mid_price == expected
        assert ticker.mid_price == Decimal("100.345")

    def test_mid_price_none_when_bid_missing(self) -> None:
        """Test mid_price returns None when bid is None."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=None,
            ask=Decimal("102.0"),
        )
        assert ticker.mid_price is None

    def test_mid_price_none_when_ask_missing(self) -> None:
        """Test mid_price returns None when ask is None."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=Decimal("100.0"),
            ask=None,
        )
        assert ticker.mid_price is None

    def test_mid_price_none_when_both_missing(self) -> None:
        """Test mid_price returns None when both bid and ask are None."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=None,
            ask=None,
        )
        assert ticker.mid_price is None

    @pytest.mark.parametrize(
        ("field_name", "value"),
        [
            ("bid", DEC_NAN),
            ("ask", DEC_NAN),
            ("bid", DEC_INF),
            ("ask", DEC_INF),
            ("bid", DEC_NEG_INF),
            ("ask", DEC_NEG_INF),
        ],
    )
    def test_mid_price_validation_prevents_non_finite_inputs(
        self, field_name: str, value: Decimal
    ) -> None:
        """Test that non-finite bid/ask values are rejected during ticker creation."""
        btc_symbol = BTC_HL
        kwargs: dict[str, Any] = {
            "symbol": btc_symbol,
            "exchange": "test_exchange",
            "timestamp": NOW,
            "bid": Decimal("100.0"),
            "ask": Decimal("102.0"),
        }
        kwargs[field_name] = value

        with pytest.raises(ValidationError, match="must be finite for ticker price data"):
            Ticker(**kwargs)

    def test_mid_price_with_zero_values(self) -> None:
        """Test mid_price calculation with zero bid/ask values."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            timestamp=NOW,
            bid=DEC_ZERO,
            ask=DEC_ZERO,
        )
        assert ticker.mid_price == DEC_ZERO

    # --- Exchange-Specific Details Tests --- #

    def test_hyperliquid_details_creation(self) -> None:
        """Test creating ticker with Hyperliquid-specific details."""
        btc_symbol = BTC_HL
        hl_details = HyperliquidTickerDetails(mid_price_source="allMids")
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            timestamp=NOW,
            price=Decimal(30000),
            hl_details=hl_details,
        )
        assert ticker.hl_details is not None
        assert ticker.hl_details.mid_price_source == "allMids"
        assert ticker.bp_details is None

    def test_backpack_details_creation(self) -> None:
        """Test creating ticker with Backpack-specific details."""
        btc_backpack_symbol = BTC_BP
        bp_details = BackpackTickerDetails(
            first_price=Decimal(29000),
            high=Decimal(31000),
            low=Decimal(28500),
            price_change=Decimal(1000),
            price_change_percent=Decimal("3.45"),
            quote_volume=Decimal(1000000),
            trades=1500,
        )
        ticker = Ticker(
            symbol=btc_backpack_symbol,
            exchange="backpack",
            timestamp=NOW,
            price=Decimal(30000),
            bp_details=bp_details,
        )
        assert ticker.bp_details is not None
        assert ticker.bp_details.first_price == Decimal(29000)
        assert ticker.bp_details.high == Decimal(31000)
        assert ticker.bp_details.low == Decimal(28500)
        assert ticker.bp_details.price_change == Decimal(1000)
        assert ticker.bp_details.price_change_percent == Decimal("3.45")
        assert ticker.bp_details.quote_volume == Decimal(1000000)
        assert ticker.bp_details.trades == 1500
        assert ticker.hl_details is None

    def test_both_exchange_details_none_by_default(self) -> None:
        """Test that exchange-specific details are None by default."""
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="generic_exchange",
            timestamp=NOW,
        )
        assert ticker.hl_details is None
        assert ticker.bp_details is None


class TestHyperliquidTickerDetails:
    """Unit tests for HyperliquidTickerDetails model."""

    def test_minimal_creation(self) -> None:
        """Test creating HyperliquidTickerDetails with minimal data."""
        details = HyperliquidTickerDetails()
        assert details.mid_price_source is None

    def test_creation_with_data(self) -> None:
        """Test creating HyperliquidTickerDetails with data."""
        details = HyperliquidTickerDetails(mid_price_source="orderbook")
        assert details.mid_price_source == "orderbook"

    def test_immutability(self) -> None:
        """Test that HyperliquidTickerDetails is immutable."""
        details = HyperliquidTickerDetails(mid_price_source="allMids")
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.mid_price_source = "orderbook"

    def test_extra_fields_ignored(self) -> None:
        """Test that extra fields are ignored in HyperliquidTickerDetails."""
        # Should not raise error due to extra="ignore"
        details = HyperliquidTickerDetails(
            mid_price_source="allMids",
            unknown_field="should_be_ignored",  # type: ignore[call-arg]
        )
        assert details.mid_price_source == "allMids"
        assert not hasattr(details, "unknown_field")


class TestBackpackTickerDetails:
    """Unit tests for BackpackTickerDetails model."""

    def test_minimal_creation(self) -> None:
        """Test creating BackpackTickerDetails with minimal data."""
        details = BackpackTickerDetails()
        assert details.first_price is None
        assert details.high is None
        assert details.low is None
        assert details.price_change is None
        assert details.price_change_percent is None
        assert details.quote_volume is None
        assert details.trades is None

    def test_creation_with_all_fields(self) -> None:
        """Test creating BackpackTickerDetails with all fields."""
        details = BackpackTickerDetails(
            first_price=Decimal(29000),
            high=Decimal(31000),
            low=Decimal(28500),
            price_change=Decimal(-500),  # Can be negative
            price_change_percent=Decimal("-1.67"),  # Can be negative
            quote_volume=Decimal(500000),
            trades=750,
        )
        assert details.first_price == Decimal(29000)
        assert details.high == Decimal(31000)
        assert details.low == Decimal(28500)
        assert details.price_change == Decimal(-500)
        assert details.price_change_percent == Decimal("-1.67")
        assert details.quote_volume == Decimal(500000)
        assert details.trades == 750

    @pytest.mark.parametrize("field_name", ["first_price", "high", "low", "quote_volume"])
    def test_non_negative_price_fields_validation(self, field_name: str) -> None:
        """Test that price and volume fields reject negative values."""
        kwargs: dict[str, Any] = {}
        kwargs[field_name] = Decimal(-1)

        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            BackpackTickerDetails(**kwargs)

    @pytest.mark.parametrize("field_name", ["price_change", "price_change_percent"])
    def test_price_change_fields_allow_negative(self, field_name: str) -> None:
        """Test that price change fields allow negative values."""
        kwargs: dict[str, Any] = {}
        kwargs[field_name] = Decimal("-10.5")

        # Should not raise error
        details = BackpackTickerDetails(**kwargs)
        assert getattr(details, field_name) == Decimal("-10.5")

    def test_trades_field_validation(self) -> None:
        """Test trades field validation (integer, non-negative)."""
        # Valid positive integer
        details = BackpackTickerDetails(trades=100)
        assert details.trades == 100

        # Valid zero
        details = BackpackTickerDetails(trades=0)
        assert details.trades == 0

        # Invalid negative
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            BackpackTickerDetails(trades=-1)

    def test_immutability(self) -> None:
        """Test that BackpackTickerDetails is immutable."""
        details = BackpackTickerDetails(first_price=Decimal(30000))
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.first_price = Decimal(31000)

    def test_extra_fields_ignored(self) -> None:
        """Test that extra fields are ignored in BackpackTickerDetails."""
        # Should not raise error due to extra="ignore"
        details = BackpackTickerDetails(
            first_price=Decimal(30000),
            unknown_field="should_be_ignored",  # type: ignore[call-arg]
        )
        assert details.first_price == Decimal(30000)
        assert not hasattr(details, "unknown_field")
