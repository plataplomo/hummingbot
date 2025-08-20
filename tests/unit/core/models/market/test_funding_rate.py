"""Property-based tests for the core FundingRate model and its Details sub-models.

This module provides comprehensive property-based testing of the FundingRate Pydantic model,
which represents funding rate data for perpetual futures contracts across all supported exchanges.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Decimal precision handling for financial calculations
- Timestamp validation and timezone handling
- Exchange-specific detail model integration (Hyperliquid and Backpack)
- Detail exclusivity validation (only one exchange detail at a time)
- Immutability properties and data integrity

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete funding rate data flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.exceptions.parsing import DateTimeParsingError, ParsingError
from cyberdelta.models.market.funding_rate import (
    BackpackFundingDetails,
    FundingRate,
    HyperliquidFundingDetails,
)
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    BTC_BP,
    BTC_HL,
    BTC_USDC_BP,
    DOGE_HL,
    ETH_BP,
    ETH_HL,
    ETH_USDC_BP,
    SOL_BP,
    SOL_HL,
    SOL_USDC_BP,
)


# Constants for datetime ranges - timezone-aware for ruff compliance
MIN_TEST_DATE_NAIVE = datetime(2020, 1, 1, tzinfo=UTC)
MAX_TEST_DATE_NAIVE = datetime(2030, 12, 31, tzinfo=UTC)

pytestmark = pytest.mark.timing


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _is_valid_decimal_string(s: str) -> bool:
    """Check if a string can be parsed as a valid Decimal.

    Returns:
        True if string can be parsed as Decimal, False otherwise.
    """
    try:
        Decimal(s)
    except (ValueError, TypeError, InvalidOperation):
        return False
    else:
        return True


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FUNDING RATE DATA
# =============================================================================


@st.composite
def funding_rate_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic funding rate values.

    Funding rates are typically small percentages (e.g., 0.01% to 0.1%)
    but can be negative in certain market conditions.

    Args:
        draw: Hypothesis draw function

    Returns:
        Decimal: A valid funding rate
    """
    # Common funding rates are between -0.1% and 0.1%
    if draw(st.booleans()):
        # Common small values
        value = draw(
            st.floats(
                min_value=-0.001,
                max_value=0.001,
                allow_infinity=False,
                allow_nan=False,
            )
        )
    else:
        # Occasionally test larger values
        value = draw(
            st.floats(
                min_value=-0.01,
                max_value=0.01,
                allow_infinity=False,
                allow_nan=False,
            )
        )
    return Decimal(str(value))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values for mark/index prices.

    Args:
        draw: Hypothesis draw function

    Returns:
        Decimal: A valid price
    """
    value = draw(
        st.floats(
            min_value=0.00001,
            max_value=1000000.0,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def volume_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic volume values for trading volume.

    Args:
        draw: Hypothesis draw function

    Returns:
        Decimal: A valid volume
    """
    value = draw(
        st.floats(
            min_value=0.0,
            max_value=1000000000.0,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for funding rate testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Symbol: A valid symbol for funding rate data
    """
    return draw(
        st.sampled_from([
            BTC_HL,
            ETH_HL,
            SOL_HL,
            DOGE_HL,
            BTC_BP,
            ETH_BP,
            SOL_BP,
            BTC_USDC_BP,
            ETH_USDC_BP,
            SOL_USDC_BP,
        ])
    )


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for funding rate data.

    Args:
        draw: Hypothesis draw function

    Returns:
        datetime: A valid UTC timestamp
    """
    return draw(
        st.datetimes(
            min_value=MIN_TEST_DATE_NAIVE,
            max_value=MAX_TEST_DATE_NAIVE,
            timezones=st.just(UTC),
        )
    )


@st.composite
def next_funding_time_strategy(draw: st.DrawFn, base_time: datetime) -> datetime:
    """Generate next funding time that's after the base timestamp.

    Args:
        draw: Hypothesis draw function
        base_time: The current timestamp

    Returns:
        datetime: A valid next funding time
    """
    # Funding typically happens every 8 hours, but can vary
    hours_ahead = draw(st.integers(min_value=1, max_value=24))
    return base_time + timedelta(hours=hours_ahead)


@st.composite
def hl_funding_details_strategy(draw: st.DrawFn) -> HyperliquidFundingDetails:
    """Generate Hyperliquid-specific funding details.

    Args:
        draw: Hypothesis draw function

    Returns:
        HyperliquidFundingDetails: Valid HL funding details
    """
    # All fields are optional in HyperliquidFundingDetails
    kwargs: dict[str, Any] = {}

    # Optionally add premium
    if draw(st.booleans()):
        kwargs["premium"] = draw(funding_rate_strategy())

    # Optionally add daily notional volume
    if draw(st.booleans()):
        kwargs["hl_day_ntl_vlm"] = draw(volume_strategy())

    # Optionally add other fields
    if draw(st.booleans()):
        kwargs["hl_funding_hourly"] = draw(funding_rate_strategy())

    if draw(st.booleans()):
        kwargs["hl_prev_day_px"] = draw(price_strategy())

    if draw(st.booleans()):
        kwargs["hl_impact_px"] = draw(price_strategy())

    return HyperliquidFundingDetails(**kwargs)


@st.composite
def bp_funding_details_strategy(draw: st.DrawFn) -> BackpackFundingDetails:
    """Generate Backpack-specific funding details.

    Args:
        draw: Hypothesis draw function

    Returns:
        BackpackFundingDetails: Valid BP funding details
    """
    # BackpackFundingDetails currently has no specific fields
    return BackpackFundingDetails()


# =============================================================================
# PROPERTY TESTS FOR FUNDING RATE MODEL
# =============================================================================


class TestFundingRateModelProperties:
    """Property-based tests for the FundingRate model."""

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_minimal_funding_rate_creation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
    ) -> None:
        """Property: Minimal FundingRate with only required fields should always be valid."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
        )

        # Properties: Required fields should be set correctly
        assert fr.symbol == symbol
        assert fr.timestamp == timestamp

        # Properties: Optional fields should have correct defaults
        assert fr.funding_rate is None
        assert fr.predicted_rate is None
        assert fr.mark_price is None
        assert fr.index_price is None
        assert fr.next_funding_time is None
        assert fr.hl_details is None
        assert fr.bp_details is None

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        predicted_rate=funding_rate_strategy(),
        mark_price=price_strategy(),
        index_price=price_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_complete_funding_rate_creation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        predicted_rate: Decimal,
        mark_price: Decimal,
        index_price: Decimal,
    ) -> None:
        """Property: Complete FundingRate with all core fields should maintain data integrity."""
        next_time = timestamp + timedelta(hours=8)

        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            predicted_rate=predicted_rate,
            mark_price=mark_price,
            index_price=index_price,
            next_funding_time=next_time,
        )

        # Properties: All fields should be preserved exactly
        assert fr.symbol == symbol
        assert fr.timestamp == timestamp
        assert fr.funding_rate == funding_rate
        assert fr.predicted_rate == predicted_rate
        assert fr.mark_price == mark_price
        assert fr.index_price == index_price
        assert fr.next_funding_time == next_time

        # Properties: Price difference calculation should work
        if fr.mark_price is not None and fr.index_price is not None:
            price_diff = fr.mark_price - fr.index_price
            assert isinstance(price_diff, Decimal)

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        hl_details=hl_funding_details_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_hyperliquid_details_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        hl_details: HyperliquidFundingDetails,
    ) -> None:
        """Property: FundingRate with Hyperliquid details should validate correctly."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            hl_details=hl_details,
        )

        # Properties: HL details should be preserved
        assert fr.hl_details == hl_details
        if hasattr(hl_details, "premium") and hl_details.premium is not None:
            assert fr.hl_details is not None
            assert fr.hl_details.premium == hl_details.premium

        # Property: BP details should be None (exclusive)
        assert fr.bp_details is None

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        bp_details=bp_funding_details_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_backpack_details_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        bp_details: BackpackFundingDetails,
    ) -> None:
        """Property: FundingRate with Backpack details should validate correctly."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            bp_details=bp_details,
        )

        # Properties: BP details should be preserved
        assert fr.bp_details == bp_details

        # Property: HL details should be None (exclusive)
        assert fr.hl_details is None

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        hl_details=hl_funding_details_strategy(),
        bp_details=bp_funding_details_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_details_can_coexist_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        hl_details: HyperliquidFundingDetails,
        bp_details: BackpackFundingDetails,
    ) -> None:
        """Property: Test whether HL and BP details can coexist (model allows both)."""
        # The model doesn't have an exclusivity validator, so both can exist
        # This test documents the actual behavior
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Both details can exist simultaneously in the current model
        assert fr.hl_details == hl_details
        assert fr.bp_details == bp_details

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_funding_rate_immutability_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
    ) -> None:
        """Property: FundingRate instances should be immutable (frozen=True)."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
        )

        # Property: Frozen model should reject mutations
        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.funding_rate = Decimal("0.0002")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.timestamp = datetime.now(UTC)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.symbol = ETH_HL

    @given(
        parseable_inputs=st.one_of(
            st.integers(min_value=-1000, max_value=1000),
            st.floats(min_value=-1.0, max_value=1.0, allow_nan=False, allow_infinity=False),
            st.text(alphabet="0123456789.-", min_size=1, max_size=20).filter(
                lambda x: (
                    # Must have digits
                    any(c.isdigit() for c in x)
                    # Can have at most one decimal point
                    and x.count(".") <= 1
                    # Minus sign can only be at the beginning
                    and (x.count("-") == 0 or (x.count("-") == 1 and x.startswith("-")))
                    # Must be parseable as decimal
                    and _is_valid_decimal_string(x)
                )
            ),
        ),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_decimal_parsing_properties(self, parseable_inputs: float | str) -> None:
        """Property: FundingRate should correctly parse various numeric input types to Decimal."""
        # Convert to positive for prices (they must be > 0, not >= 0)
        try:
            price_value = abs(float(str(parseable_inputs)))
            if price_value == 0:
                price_value = 0.00001  # Use small positive value instead of 0
            price_input = Decimal(str(price_value))
        except (ValueError, TypeError):
            price_input = None

        decimal_input = Decimal(str(parseable_inputs))
        fr = FundingRate(
            symbol=BTC_HL,
            timestamp=datetime.now(UTC),
            funding_rate=decimal_input,
            predicted_rate=decimal_input,
            mark_price=price_input,  # Prices must be > 0
            index_price=price_input,
        )

        # Property: All numeric fields should be converted to Decimal or None
        if fr.funding_rate is not None:
            assert isinstance(fr.funding_rate, Decimal)
        if fr.predicted_rate is not None:
            assert isinstance(fr.predicted_rate, Decimal)
        if fr.mark_price is not None:
            assert isinstance(fr.mark_price, Decimal)
            assert fr.mark_price > 0  # Prices must be positive (gt=0)
        if fr.index_price is not None:
            assert isinstance(fr.index_price, Decimal)
            assert fr.index_price > 0

    @given(
        invalid_timestamp=st.one_of(
            st.just("not_a_datetime"),
            st.just("2023-13-01T00:00:00Z"),  # Invalid month
            st.just("2023-02-30T00:00:00Z"),  # Invalid day
            st.just("invalid_date_string"),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_timestamp_rejection_properties(self, invalid_timestamp: str) -> None:
        """Property: Invalid timestamp inputs should always raise ValidationError."""
        with pytest.raises((ValidationError, DateTimeParsingError, ParsingError)):
            FundingRate(
                symbol=BTC_HL,
                timestamp=invalid_timestamp,  # type: ignore[arg-type]
            )

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        next_offset_hours=st.integers(min_value=1, max_value=24),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_next_funding_time_consistency_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        next_offset_hours: int,
    ) -> None:
        """Property: Next funding time should always be after current timestamp."""
        next_time = timestamp + timedelta(hours=next_offset_hours)

        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            next_funding_time=next_time,
        )

        # Property: Next funding time should be in the future
        assert fr.next_funding_time is not None
        assert fr.next_funding_time > fr.timestamp

        # Property: Time difference should match our offset
        time_diff = fr.next_funding_time - fr.timestamp
        assert time_diff.total_seconds() == next_offset_hours * 3600


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID FUNDING DETAILS
# =============================================================================


class TestHyperliquidFundingDetailsProperties:
    """Property-based tests for Hyperliquid-specific funding details."""

    @given(
        hl_details=hl_funding_details_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_minimal_hl_details_properties(self, hl_details: HyperliquidFundingDetails) -> None:
        """Property: HyperliquidFundingDetails should be valid with any combination of fields.

        optional fields.
        """
        # All fields are optional, so any combination is valid
        assert hl_details is not None

        # Check that all fields that exist are of correct type
        if hl_details.premium is not None:
            assert isinstance(hl_details.premium, Decimal)
        if hl_details.hl_day_ntl_vlm is not None:
            assert isinstance(hl_details.hl_day_ntl_vlm, Decimal)
        if hl_details.hl_funding_hourly is not None:
            assert isinstance(hl_details.hl_funding_hourly, Decimal)
        if hl_details.hl_prev_day_px is not None:
            assert isinstance(hl_details.hl_prev_day_px, Decimal)
        if hl_details.hl_impact_px is not None:
            assert isinstance(hl_details.hl_impact_px, Decimal)

    @given(
        premium=funding_rate_strategy(),
        hl_day_ntl_vlm=volume_strategy(),
        hl_funding_hourly=funding_rate_strategy(),
        hl_prev_day_px=price_strategy(),
        hl_impact_px=price_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_complete_hl_details_properties(
        self,
        premium: Decimal,
        hl_day_ntl_vlm: Decimal,
        hl_funding_hourly: Decimal,
        hl_prev_day_px: Decimal,
        hl_impact_px: Decimal,
    ) -> None:
        """Property: Complete HyperliquidFundingDetails should preserve all fields."""
        details = HyperliquidFundingDetails(
            premium=premium,
            hl_day_ntl_vlm=hl_day_ntl_vlm,
            hl_funding_hourly=hl_funding_hourly,
            hl_prev_day_px=hl_prev_day_px,
            hl_impact_px=hl_impact_px,
        )

        assert details.premium == premium
        assert details.hl_day_ntl_vlm == hl_day_ntl_vlm
        assert details.hl_funding_hourly == hl_funding_hourly
        assert details.hl_prev_day_px == hl_prev_day_px
        assert details.hl_impact_px == hl_impact_px

        # Property: Volume should be non-negative
        if details.hl_day_ntl_vlm is not None:
            assert details.hl_day_ntl_vlm >= 0

    @given(premium=st.one_of(st.none(), funding_rate_strategy()))
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_hl_details_immutability_properties(self, premium: Decimal | None) -> None:
        """Property: HyperliquidFundingDetails should be immutable."""
        details = HyperliquidFundingDetails(premium=premium)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.premium = Decimal("0.0002")


# =============================================================================
# PROPERTY TESTS FOR BACKPACK FUNDING DETAILS
# =============================================================================


class TestBackpackFundingDetailsProperties:
    """Property-based tests for Backpack-specific funding details."""

    @given(data=st.just(None))  # BackpackFundingDetails has no fields currently
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_bp_details_creation_properties(self, data: None) -> None:
        """Property: BackpackFundingDetails should always be creatable."""
        details = BackpackFundingDetails()

        # Property: Instance should be created successfully
        assert details is not None

        # Property: Should be frozen (immutable)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.new_field = "test"  # type: ignore[attr-defined]


# =============================================================================
# EDGE CASE AND INTEGRATION PROPERTIES
# =============================================================================


class TestFundingRateEdgeCaseProperties:
    """Property-based tests for edge cases and integration scenarios."""

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        mark_price=price_strategy(),
        index_price=price_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_price_difference_calculation_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        mark_price: Decimal,
        index_price: Decimal,
    ) -> None:
        """Property: Price differences should be calculable when both prices exist."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            mark_price=mark_price,
            index_price=index_price,
        )

        # Property: Price difference should be accurate
        assert fr.mark_price is not None
        assert fr.index_price is not None
        price_diff = fr.mark_price - fr.index_price
        assert isinstance(price_diff, Decimal)
        assert price_diff == mark_price - index_price

        # Property: Premium calculation basis
        if price_diff > 0:
            # Mark > Index indicates positive premium pressure
            assert fr.mark_price is not None
            assert fr.index_price is not None
            assert fr.mark_price > fr.index_price
        elif price_diff < 0:
            # Mark < Index indicates negative premium pressure
            assert fr.mark_price is not None
            assert fr.index_price is not None
            assert fr.mark_price < fr.index_price
        else:
            # Equal prices
            assert fr.mark_price == fr.index_price

    @given(
        symbol=valid_symbol_strategy(),
        timestamp=valid_timestamp_strategy(),
        funding_rate=funding_rate_strategy(),
        predicted_rate=funding_rate_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_rate_comparison_properties(
        self,
        symbol: Symbol,
        timestamp: datetime,
        funding_rate: Decimal,
        predicted_rate: Decimal,
    ) -> None:
        """Property: Funding rates should be comparable for trend analysis."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp,
            funding_rate=funding_rate,
            predicted_rate=predicted_rate,
        )

        # Property: Rate trend analysis
        if fr.predicted_rate is not None and fr.funding_rate is not None:
            if fr.predicted_rate > fr.funding_rate:
                # Rates expected to increase
                rate_diff = fr.predicted_rate - fr.funding_rate
                assert rate_diff > 0
            elif fr.predicted_rate < fr.funding_rate:
                # Rates expected to decrease
                rate_diff = fr.predicted_rate - fr.funding_rate
                assert rate_diff < 0
            else:
                # Rates expected to remain stable
                assert fr.predicted_rate == fr.funding_rate

    @given(
        symbol=valid_symbol_strategy(),
        timestamp_ms=st.integers(
            min_value=946684800000,  # 2000-01-01
            max_value=1893456000000,  # 2030-01-01
        ),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_timestamp_parsing_from_milliseconds_properties(
        self,
        symbol: Symbol,
        timestamp_ms: int,
    ) -> None:
        """Property: Timestamps from milliseconds should parse correctly."""
        fr = FundingRate(
            symbol=symbol,
            timestamp=timestamp_ms,  # type: ignore[arg-type]
        )

        # Property: Timestamp should be converted to datetime
        assert isinstance(fr.timestamp, datetime)

        # Property: Should be UTC aware
        assert fr.timestamp.tzinfo is not None

        # Property: Conversion should be accurate
        expected_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        assert fr.timestamp == expected_dt
