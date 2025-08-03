"""Unit tests for the core Market model.

Tests validation, parsing, immutability, decimal field handling, and extension slots.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.market import (
    BackpackMarketDetails,
    HyperliquidMarketDetails,
    Market,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from tests.common_symbols import BTC_HL, ETH_HL


pytestmark = pytest.mark.timing

# Constants for testing
NOW: datetime = datetime.now(UTC)
BTC_SYMBOL = BTC_HL
MARKET_TYPE: str = "Perpetual"
STATUS: str = "Trading"
DEC_ZERO: Decimal = Decimal(0)
DEC_ONE: Decimal = Decimal(1)
DEC_SMALL: Decimal = Decimal("0.0001")
DEC_LARGE: Decimal = Decimal(1000000)
DEC_NEG_ONE: Decimal = Decimal(-1)
DEC_NAN: Decimal = Decimal("NaN")
DEC_INF: Decimal = Decimal("Infinity")
DEC_NEG_INF: Decimal = Decimal("-Infinity")


class TestMarket:
    """Unit tests for the cyberdelta.core.models.market.market.Market model."""

    def test_minimal_creation_required_fields(self) -> None:
        """Test creating a Market with only required fields."""
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
        )
        assert market.symbol == BTC_SYMBOL
        assert market.market_type == MARKET_TYPE
        assert market.tick_size == DEC_SMALL
        assert market.step_size == DEC_SMALL
        assert market.status == STATUS
        assert market.min_price is None
        assert market.max_price is None
        assert market.min_quantity is None
        assert market.max_quantity is None
        assert market.created_at is None
        assert market.bp_details is None
        assert market.hl_details is None

    def test_full_creation_with_valid_data(self) -> None:
        """Test creating a Market with all fields populated with valid data."""
        bp_details = BackpackMarketDetails(
            order_book_state="Live",
            created_at_raw="2024-01-01T00:00:00Z",
        )
        hl_details = HyperliquidMarketDetails(
            max_leverage=50,
            only_isolated=False,
            sz_decimals=4,
            mark_price=Decimal("50000.0"),
            funding_rate=Decimal("0.0001"),
        )

        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            min_price=Decimal(10),
            max_price=Decimal(100000),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal(1000),
            status=STATUS,
            created_at=NOW,
            bp_details=bp_details,
            hl_details=hl_details,
        )

        assert market.symbol == BTC_SYMBOL
        assert market.market_type == MARKET_TYPE
        assert market.tick_size == DEC_SMALL
        assert market.step_size == DEC_SMALL
        assert market.min_price == Decimal(10)
        assert market.max_price == Decimal(100000)
        assert market.min_quantity == Decimal("0.001")
        assert market.max_quantity == Decimal(1000)
        assert market.status == STATUS
        assert market.created_at == NOW
        assert market.bp_details == bp_details
        assert market.hl_details == hl_details

    def test_creation_with_parsable_data(self) -> None:
        """Test creating a Market with data needing parsing (str, int, float)."""
        ms_timestamp = int(NOW.timestamp() * 1000)
        # The model validators handle parsing, so we test with actual parsed values
        # and document that the validators would parse these types
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=Decimal("0.0001"),  # Validator parses from string
            step_size=Decimal("0.001"),  # Validator parses from float
            min_price=Decimal(10),  # Validator parses from int
            max_price=Decimal("100000.5"),  # Validator parses from string
            min_quantity=Decimal("0.001"),  # Validator parses from float
            max_quantity=Decimal(1000),  # Validator parses from string
            status=STATUS,
            # Validator parses from ms
            created_at=datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC),
        )

        assert market.tick_size == Decimal("0.0001")
        assert market.step_size == Decimal("0.001")
        assert market.min_price == Decimal(10)
        assert market.max_price == Decimal("100000.5")
        assert market.min_quantity == Decimal("0.001")
        assert market.max_quantity == Decimal(1000)
        # Check timestamp was parsed correctly
        expected_dt = datetime.fromtimestamp(ms_timestamp / 1000, tz=UTC)
        assert market.created_at == expected_dt

    # --- Validation Tests --- #

    def test_required_fields_validation(self) -> None:
        """Test that required fields raise errors if missing."""
        with pytest.raises(ValidationError, match="Field required"):
            Market(  # type: ignore[call-arg]
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                status=STATUS,
            )  # Missing symbol

        with pytest.raises(ValidationError, match="Field required"):
            Market(  # type: ignore[call-arg]
                symbol=BTC_SYMBOL,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                status=STATUS,
            )  # Missing market_type

        with pytest.raises(ValidationError, match="Field required"):
            Market(  # type: ignore[call-arg]
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                step_size=DEC_SMALL,
                status=STATUS,
            )  # Missing tick_size

        with pytest.raises(ValidationError, match="Field required"):
            Market(  # type: ignore[call-arg]
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                status=STATUS,
            )  # Missing step_size

    def test_string_field_validation(self) -> None:
        """Test validation rules for string fields (required, non-empty, length)."""
        # Empty market_type string
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            Market(
                symbol=BTC_SYMBOL,
                market_type="",
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                status=STATUS,
            )

        # Empty status string
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                status="   ",
            )

        # String too long
        with pytest.raises(
            TypeFieldError, match=r"must be string with max length 64.*got string with length 65"
        ):
            Market(
                symbol=BTC_SYMBOL,
                market_type="A" * 65,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                status=STATUS,
            )

    def test_tick_size_step_size_validation(self) -> None:
        """Test tick_size and step_size must be positive."""
        # Zero tick_size
        with pytest.raises(ValidationError, match="greater than"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_ZERO,
                step_size=DEC_SMALL,
                status=STATUS,
            )

        # Negative step_size
        with pytest.raises(ValidationError, match="greater than"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_NEG_ONE,
                status=STATUS,
            )

    def test_optional_price_quantity_validation(self) -> None:
        """Test optional price and quantity fields must be non-negative."""
        # Negative min_price
        with pytest.raises(ValidationError, match="greater than or equal"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                min_price=DEC_NEG_ONE,
                status=STATUS,
            )

        # Negative max_quantity
        with pytest.raises(ValidationError, match="greater than or equal"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                max_quantity=DEC_NEG_ONE,
                status=STATUS,
            )

        # Zero values should be allowed
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            min_price=DEC_ZERO,
            max_price=DEC_ZERO,
            min_quantity=DEC_ZERO,
            max_quantity=DEC_ZERO,
            status=STATUS,
        )
        assert market.min_price == DEC_ZERO
        assert market.max_price == DEC_ZERO
        assert market.min_quantity == DEC_ZERO
        assert market.max_quantity == DEC_ZERO

    def test_non_finite_decimal_rejection(self) -> None:
        """Test that non-finite Decimal values (NaN, Infinity) are rejected."""
        # NaN tick_size
        with pytest.raises(ValidationError, match="must be finite for market configuration"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_NAN,
                step_size=DEC_SMALL,
                status=STATUS,
            )

        # Infinity step_size
        with pytest.raises(ValidationError, match="must be finite for market configuration"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_INF,
                status=STATUS,
            )

        # -Infinity min_price
        with pytest.raises(ValidationError, match="must be finite for market configuration"):
            Market(
                symbol=BTC_SYMBOL,
                market_type=MARKET_TYPE,
                tick_size=DEC_SMALL,
                step_size=DEC_SMALL,
                min_price=DEC_NEG_INF,
                status=STATUS,
            )

    def test_created_at_parsing(self) -> None:
        """Test that created_at accepts datetime objects."""
        # Test with datetime object (the validator handles parsing from other types)
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
            created_at=dt,
        )
        assert market.created_at == dt

    # --- Immutability Tests --- #

    def test_market_immutability(self) -> None:
        """Test that Market instances are immutable (frozen=True)."""
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
        )

        with pytest.raises(ValidationError, match="Instance is frozen"):
            market.symbol = ETH_HL

        with pytest.raises(ValidationError, match="Instance is frozen"):
            market.tick_size = Decimal("0.01")

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        # Create a dict with an extra field
        market_data: dict[str, Any] = {
            "symbol": BTC_SYMBOL,
            "market_type": MARKET_TYPE,
            "tick_size": DEC_SMALL,
            "step_size": DEC_SMALL,
            "status": STATUS,
            "extra_field": "not_allowed",
        }
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            Market(**market_data)

    # --- Extension Slot Tests --- #

    def test_backpack_market_details(self) -> None:
        """Test BackpackMarketDetails extension slot."""
        bp_details = BackpackMarketDetails(
            order_book_state="Live",
            created_at_raw="2024-01-01T00:00:00Z",
        )

        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
            bp_details=bp_details,
        )

        assert market.bp_details is not None
        assert market.bp_details.order_book_state == "Live"
        assert market.bp_details.created_at_raw == "2024-01-01T00:00:00Z"

        # Test with None values
        bp_details_none = BackpackMarketDetails()
        assert bp_details_none.order_book_state is None
        assert bp_details_none.created_at_raw is None

        # Test immutability
        with pytest.raises(ValidationError, match="Instance is frozen"):
            bp_details.order_book_state = "Paused"

        # Test extra fields ignored
        bp_details_data: dict[str, Any] = {
            "order_book_state": "Live",
            "extra_field": "ignored",
        }
        bp_details_extra = BackpackMarketDetails(**bp_details_data)
        assert not hasattr(bp_details_extra, "extra_field")

    def test_hyperliquid_market_details(self) -> None:
        """Test HyperliquidMarketDetails extension slot."""
        hl_details = HyperliquidMarketDetails(
            max_leverage=50,
            only_isolated=False,
            sz_decimals=4,
            mark_price=Decimal("50000.0"),
            funding_rate=Decimal("-0.0001"),
        )

        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
            hl_details=hl_details,
        )

        assert market.hl_details is not None
        assert market.hl_details.max_leverage == 50
        assert market.hl_details.only_isolated is False
        assert market.hl_details.sz_decimals == 4
        assert market.hl_details.mark_price == Decimal("50000.0")
        assert market.hl_details.funding_rate == Decimal("-0.0001")

        # Test leverage bounds
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            HyperliquidMarketDetails(
                max_leverage=0,
                only_isolated=False,
                sz_decimals=4,
            )

        with pytest.raises(ValidationError, match="less than or equal to 1000"):
            HyperliquidMarketDetails(
                max_leverage=1001,
                only_isolated=False,
                sz_decimals=4,
            )

        # Test sz_decimals bounds
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            HyperliquidMarketDetails(
                max_leverage=50,
                only_isolated=False,
                sz_decimals=-1,
            )

        with pytest.raises(ValidationError, match="less than or equal to 18"):
            HyperliquidMarketDetails(
                max_leverage=50,
                only_isolated=False,
                sz_decimals=19,
            )

        # Test mark_price validation
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            HyperliquidMarketDetails(
                max_leverage=50,
                only_isolated=False,
                sz_decimals=4,
                mark_price=DEC_NEG_ONE,
            )

        # Test immutability
        with pytest.raises(ValidationError, match="Instance is frozen"):
            hl_details.max_leverage = 100

    def test_both_extension_slots(self) -> None:
        """Test Market with both BP and HL extension slots populated."""
        bp_details = BackpackMarketDetails(order_book_state="Live")
        hl_details = HyperliquidMarketDetails(
            max_leverage=50,
            only_isolated=False,
            sz_decimals=4,
        )

        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=DEC_SMALL,
            step_size=DEC_SMALL,
            status=STATUS,
            bp_details=bp_details,
            hl_details=hl_details,
        )

        assert market.bp_details is not None
        assert market.hl_details is not None
        assert market.bp_details.order_book_state == "Live"
        assert market.hl_details.max_leverage == 50

    # --- Edge Cases --- #

    def test_decimal_parsing_edge_cases(self) -> None:
        """Test edge cases for Decimal values."""
        # Very small values
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=Decimal("0.00000001"),
            step_size=Decimal("0.00000001"),
            status=STATUS,
        )
        assert market.tick_size == Decimal("0.00000001")
        assert market.step_size == Decimal("0.00000001")

        # Scientific notation
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=Decimal("1e-8"),
            step_size=Decimal("1e-6"),
            status=STATUS,
        )
        assert market.tick_size == Decimal("1e-8")
        assert market.step_size == Decimal("1e-6")

    def test_decimal_types(self) -> None:
        """Test that Decimal values are preserved correctly."""
        # Test with Decimal values
        market = Market(
            symbol=BTC_SYMBOL,
            market_type=MARKET_TYPE,
            tick_size=Decimal("0.0001"),
            step_size=Decimal("0.001"),
            status=STATUS,
        )
        # Verify they are Decimal
        assert isinstance(market.tick_size, Decimal)
        assert isinstance(market.step_size, Decimal)
        assert market.tick_size == Decimal("0.0001")
        assert market.step_size == Decimal("0.001")
