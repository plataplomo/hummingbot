"""Property-based tests for the core Candle model using Hypothesis.

This module provides comprehensive property-based testing of the Candle Pydantic model,
which represents OHLCV (Open, High, Low, Close, Volume) candlestick data for technical
analysis across all supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- OHLC price relationship invariants (high >= max(open, close), low <= min(open, close))
- Volume constraints and non-negativity
- Interval validation and format consistency
- Timestamp validation and timezone handling
- Decimal precision handling for financial calculations
- Exchange-specific detail model integration
- Immutability properties and data integrity

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete candle data flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.exceptions.parsing import DateTimeParsingError, EmptyStringError, ParsingError
from cyberdelta.models.market.candle import Candle
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


# =============================================================================
# HYPOTHESIS STRATEGIES FOR CANDLE DATA
# =============================================================================


@st.composite
def price_strategy(
    draw: st.DrawFn,
    min_value: float = 0.00001,
    max_value: float = 1000000.0,
) -> Decimal:
    """Generate realistic price values for OHLC data.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum price value
        max_value: Maximum price value

    Returns:
        Decimal: A valid price for candle data
    """
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def volume_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic volume values (non-negative).

    Args:
        draw: Hypothesis draw function

    Returns:
        Decimal: A valid volume value
    """
    # Include zero volume for low-activity periods
    if draw(st.booleans()):
        return Decimal(0)

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
def interval_strategy(draw: st.DrawFn) -> str:
    """Generate valid interval strings for candle data.

    Args:
        draw: Hypothesis draw function

    Returns:
        str: A valid interval string (e.g., '1m', '5m', '1h', '1d')
    """
    return draw(
        st.sampled_from([
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ])
    )


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for candle testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Symbol: A valid symbol for candle data
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
    """Generate valid UTC timestamps for candle data.

    Args:
        draw: Hypothesis draw function

    Returns:
        datetime: A valid UTC timestamp
    """
    return draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
        )
    )


@st.composite
def consistent_ohlc_prices_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Generate consistent OHLC prices that satisfy market invariants.

    OHLC Invariants:
    - High >= max(Open, Close)
    - Low <= min(Open, Close)
    - High >= Low
    - All prices must be positive

    Args:
        draw: Hypothesis draw function

    Returns:
        tuple: (open, high, low, close) prices that satisfy invariants
    """
    # Generate base prices
    prices = [draw(price_strategy()) for _ in range(4)]

    # Determine actual high and low
    high = max(prices)
    low = min(prices)

    # Open and close can be any values between low and high
    open_price = draw(price_strategy(min_value=float(low), max_value=float(high)))
    close_price = draw(price_strategy(min_value=float(low), max_value=float(high)))

    return open_price, high, low, close_price


# =============================================================================
# PROPERTY TESTS FOR CANDLE MODEL
# =============================================================================


class TestCandleModelProperties:
    """Property-based tests for the Candle model."""

    @given(
        symbol=valid_symbol_strategy(),
        interval=interval_strategy(),
        open_time=valid_timestamp_strategy(),
        ohlc_prices=consistent_ohlc_prices_strategy(),
        volume=volume_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_candle_creation_properties(
        self,
        symbol: Symbol,
        interval: str,
        open_time: datetime,
        ohlc_prices: tuple[Decimal, Decimal, Decimal, Decimal],
        volume: Decimal,
    ) -> None:
        """Property: Minimal candle with consistent OHLC data should always be valid."""
        open_price, high, low, close = ohlc_prices

        candle = Candle(
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )

        # Properties: Required fields should be set correctly
        assert candle.symbol == symbol
        assert candle.interval == interval
        assert candle.open_time == open_time
        assert candle.open == open_price
        assert candle.high == high
        assert candle.low == low
        assert candle.close == close
        assert candle.volume == volume

        # Properties: Optional fields should have correct defaults
        # Note: Candle model doesn't have close_time or trades_count fields

        # Properties: OHLC invariants
        assert candle.high >= max(candle.open, candle.close)
        assert candle.low <= min(candle.open, candle.close)
        assert candle.high >= candle.low
        assert candle.volume >= Decimal(0)

    @given(
        symbol=valid_symbol_strategy(),
        interval=interval_strategy(),
        open_time=valid_timestamp_strategy(),
        ohlc_prices=consistent_ohlc_prices_strategy(),
        volume=volume_strategy(),
    )
    @settings(max_examples=300, deadline=None)
    def test_full_candle_creation_properties(
        self,
        symbol: Symbol,
        interval: str,
        open_time: datetime,
        ohlc_prices: tuple[Decimal, Decimal, Decimal, Decimal],
        volume: Decimal,
    ) -> None:
        """Property: Full candle with all fields should maintain data integrity."""
        open_price, high, low, close = ohlc_prices

        candle = Candle(
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )

        # Properties: All fields should be preserved exactly
        assert candle.symbol == symbol
        assert candle.interval == interval
        assert candle.open_time == open_time
        assert candle.open == open_price
        assert candle.high == high
        assert candle.low == low
        assert candle.close == close
        assert candle.volume == volume

    @given(
        field_name=st.sampled_from(["open", "high", "low", "close", "volume"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_price_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Price and volume fields should reject invalid values."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "interval": "1h",
            "open_time": datetime.now(UTC),
            "open": Decimal("100.0"),
            "high": Decimal("105.0"),
            "low": Decimal("95.0"),
            "close": Decimal("102.0"),
            "volume": Decimal("1000.0"),
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            Candle(**kwargs)

    @given(
        symbol=valid_symbol_strategy(),
        interval=interval_strategy(),
        open_time=valid_timestamp_strategy(),
        ohlc_prices=consistent_ohlc_prices_strategy(),
        volume=volume_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_candle_immutability_properties(
        self,
        symbol: Symbol,
        interval: str,
        open_time: datetime,
        ohlc_prices: tuple[Decimal, Decimal, Decimal, Decimal],
        volume: Decimal,
    ) -> None:
        """Property: Candle instances should be immutable (frozen=True)."""
        open_price, high, low, close = ohlc_prices

        candle = Candle(
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )

        # Property: Frozen model should reject mutations
        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.open = Decimal("200.0")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.volume = Decimal("2000.0")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            candle.interval = "5m"

    @given(
        open_price=price_strategy(),
        high=price_strategy(),
        low=price_strategy(),
        close=price_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_ohlc_consistency_validation_properties(
        self, open_price: Decimal, high: Decimal, low: Decimal, close: Decimal
    ) -> None:
        """Property: OHLC relationships should be validated correctly."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "interval": "1h",
            "open_time": datetime.now(UTC),
            "volume": Decimal("1000.0"),
        }

        # Check if this combination violates OHLC invariants
        violates_invariants = (
            high < max(open_price, close) or low > min(open_price, close) or high < low
        )

        kwargs = base_kwargs.copy()
        kwargs.update({
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
        })

        if violates_invariants:
            # Should be rejected by model validation
            with pytest.raises((ValidationError, ValueError)):
                Candle(**kwargs)
        else:
            # Should be valid
            candle = Candle(**kwargs)
            assert candle.high >= max(candle.open, candle.close)
            assert candle.low <= min(candle.open, candle.close)
            assert candle.high >= candle.low

    @given(
        parseable_inputs=st.one_of(
            st.integers(
                min_value=1, max_value=1000000
            ),  # Changed from 0 to 1 since prices must be > 0
            st.floats(
                min_value=0.0001, max_value=1000000.0, allow_nan=False, allow_infinity=False
            ),  # Changed min_value
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: x.replace(".", "").isdigit()
                and len(x.replace(".", "")) > 0
                and x.count(".") <= 1
                and float(x) > 0  # Ensure parsed value is positive
            ),
        ),
    )
    @settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_decimal_parsing_properties(self, parseable_inputs: float | str) -> None:
        """Property: Candle should correctly parse various numeric input types to Decimal."""
        # Skip edge cases that might cause precision issues
        if isinstance(parseable_inputs, float):
            assume(abs(parseable_inputs) < 1e15)  # Avoid precision loss
            assume(parseable_inputs > 0)  # Ensure positive (not just non-negative)

        # For volume, we can use zero
        raw_volume = (
            0 if isinstance(parseable_inputs, int) and parseable_inputs == 1 else parseable_inputs
        )
        volume_input = Decimal(str(raw_volume))

        decimal_input = Decimal(str(parseable_inputs))
        candle = Candle(
            symbol=BTC_HL,
            interval="1h",
            open_time=datetime.now(UTC),
            open=decimal_input,
            high=decimal_input,
            low=decimal_input,
            close=decimal_input,
            volume=volume_input,  # Volume can be 0
        )

        # Property: All price fields should be converted to Decimal
        assert isinstance(candle.open, Decimal)
        assert isinstance(candle.high, Decimal)
        assert isinstance(candle.low, Decimal)
        assert isinstance(candle.close, Decimal)
        assert isinstance(candle.volume, Decimal)
        assert candle.open >= 0  # Should maintain non-negativity
        assert candle.volume >= 0  # Should maintain non-negativity

    @given(
        interval=st.text(
            min_size=0,
            max_size=10,
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
            ),
        ).filter(
            lambda x: x
            not in [
                "1m",
                "3m",
                "5m",
                "15m",
                "30m",
                "1h",
                "2h",
                "4h",
                "6h",
                "8h",
                "12h",
                "1d",
                "3d",
                "1w",
                "1M",
            ]
        ),
    )
    @settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_interval_validation_properties(self, interval: str) -> None:
        """Property: Invalid interval formats should be validated or accepted as strings."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "interval": interval,
            "open_time": datetime.now(UTC),
            "open": Decimal("100.0"),
            "high": Decimal("105.0"),
            "low": Decimal("95.0"),
            "close": Decimal("102.0"),
            "volume": Decimal("1000.0"),
        }

        # Empty string should be rejected
        if not interval or not interval.strip():
            with pytest.raises((ValidationError, EmptyStringError)):
                Candle(**base_kwargs)
        else:
            # Non-standard intervals are accepted as strings after being stripped
            candle = Candle(**base_kwargs)
            # The model config has str_strip_whitespace=True
            assert candle.interval == interval.strip()

    @given(
        invalid_timestamp=st.one_of(
            st.just("not_a_datetime"),
            st.just("2023-13-01T00:00:00Z"),  # Invalid month
            st.just("2023-02-30T00:00:00Z"),  # Invalid day
            st.just("invalid_date_string"),
            st.just("2023/01/01"),  # Wrong format
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_timestamp_rejection_properties(self, invalid_timestamp: str) -> None:
        """Property: Invalid timestamp inputs should always raise ValidationError."""
        with pytest.raises((ValidationError, DateTimeParsingError, ParsingError)):
            Candle(
                symbol=BTC_HL,
                interval="1h",
                open_time=cast(datetime, invalid_timestamp),
                open=Decimal("100.0"),
                high=Decimal("105.0"),
                low=Decimal("95.0"),
                close=Decimal("102.0"),
                volume=Decimal("1000.0"),
            )
