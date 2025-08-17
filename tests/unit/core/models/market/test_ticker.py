"""Property-based tests for the core Ticker model using Hypothesis.

This module provides comprehensive property-based testing of the Ticker Pydantic model,
which serves as the unified internal representation for market ticker data across all
supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Decimal precision handling for financial calculations (comprehensive value ranges)
- Cross-field validation logic with generated combinations
- Extension slot functionality with exchange-specific data generation
- Mid-price calculation properties across all possible bid/ask combinations
- Edge case handling through exhaustive generation
- Immutability properties under all mutation attempts

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete ticker data flows with real constraints
- Validates financial calculation invariants

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.ticker import (
    BackpackTickerDetails,
    HyperliquidTickerDetails,
    Ticker,
)
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    AVAX_HL,
    BTC_BP,
    BTC_HL,
    BTC_USDC_BP,
    DOGE_BP,
    DOGE_HL,
    ETH_BP,
    ETH_HL,
    ETH_USDC_BP,
    SOL_BP,
    SOL_HL,
    SOL_USDC_BP,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR TICKER DATA
# =============================================================================


@st.composite
def financial_decimal_strategy(
    draw: st.DrawFn,
    min_value: float = 0.000001,
    max_value: float = 1000000.0,
    allow_zero: bool = True,
) -> Decimal:
    """Generate realistic Decimal values for financial calculations.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value (exclusive)
        max_value: Maximum value (inclusive)
        allow_zero: Whether to allow zero values

    Returns:
        Decimal: A valid decimal for financial calculations
    """
    if allow_zero and draw(st.booleans()):
        return Decimal(0)

    # Generate financial precision values
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
            exclude_min=True,
        )
    )
    return Decimal(str(value))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values for ticker data.

    Returns:
        Decimal price value for ticker testing.
    """
    return draw(financial_decimal_strategy(min_value=0.01, max_value=100000.0, allow_zero=False))


@st.composite
def volume_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic volume values for ticker data.

    Returns:
        Decimal volume value for ticker testing.
    """
    return draw(
        financial_decimal_strategy(min_value=0.000001, max_value=10000000.0, allow_zero=True)
    )


@st.composite
def bid_ask_spread_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal]:
    """Generate realistic bid/ask pairs with proper spread.

    Returns:
        tuple[Decimal, Decimal]: (bid_price, ask_price) where ask >= bid
    """
    bid = draw(price_strategy())
    # Generate ask price that's equal or higher than bid
    spread_pct = draw(st.floats(min_value=0.0, max_value=0.1))  # 0-10% spread
    ask = bid * (Decimal(1) + Decimal(str(spread_pct)))
    return bid, ask


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for ticker testing.

    Returns:
        Valid Symbol object for testing.
    """
    return draw(
        st.sampled_from([
            BTC_HL,
            ETH_HL,
            SOL_HL,
            DOGE_HL,
            AVAX_HL,
            BTC_BP,
            ETH_BP,
            SOL_BP,
            DOGE_BP,
            BTC_USDC_BP,
            ETH_USDC_BP,
            SOL_USDC_BP,
        ])
    )


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for ticker data.

    Returns:
        UTC datetime object for ticker testing.
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
        )
    )
    # Convert to UTC timezone-aware datetime
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def hyperliquid_details_strategy(draw: st.DrawFn) -> HyperliquidTickerDetails:
    """Generate valid HyperliquidTickerDetails for testing.

    Returns:
        Valid HyperliquidTickerDetails object for testing.
    """
    mid_price_source = draw(
        st.one_of(st.none(), st.sampled_from(["allMids", "orderbook", "trades"]))
    )
    return HyperliquidTickerDetails(mid_price_source=mid_price_source)


@st.composite
def backpack_details_strategy(draw: st.DrawFn) -> BackpackTickerDetails:
    """Generate valid BackpackTickerDetails for testing.

    Returns:
        Valid BackpackTickerDetails object for testing.
    """
    # All fields are optional and can be None
    first_price = draw(st.one_of(st.none(), price_strategy()))
    high = draw(st.one_of(st.none(), price_strategy()))
    low = draw(st.one_of(st.none(), price_strategy()))

    # Ensure high >= low if both are present
    if high is not None and low is not None and high < low:
        high, low = low, high

    # Price change can be negative
    price_change = draw(
        st.one_of(st.none(), financial_decimal_strategy(min_value=-10000.0, max_value=10000.0))
    )

    price_change_percent = draw(
        st.one_of(st.none(), financial_decimal_strategy(min_value=-100.0, max_value=1000.0))
    )

    quote_volume = draw(st.one_of(st.none(), volume_strategy()))
    trades = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=1000000)))

    return BackpackTickerDetails(
        first_price=first_price,
        high=high,
        low=low,
        price_change=price_change,
        price_change_percent=price_change_percent,
        quote_volume=quote_volume,
        trades=trades,
    )


# =============================================================================
# PROPERTY TESTS FOR TICKER MODEL
# =============================================================================


class TestTickerModelProperties:
    """Property-based tests for the Ticker model."""

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_minimal_ticker_creation_properties(
        self, ticker_symbol: Symbol, exchange: ExchangeName, timestamp: datetime
    ) -> None:
        """Property: Minimal ticker with only required fields should always be valid."""
        ticker = Ticker(symbol=ticker_symbol, exchange=exchange, timestamp=timestamp)

        # Properties: Required fields should be set correctly
        assert ticker.symbol == ticker_symbol
        assert ticker.exchange == exchange
        assert ticker.timestamp == timestamp

        # Properties: Optional fields should have correct defaults
        assert ticker.price is None
        assert ticker.bid is None
        assert ticker.ask is None
        assert ticker.volume is None
        assert ticker.hl_details is None
        assert ticker.bp_details is None

        # Properties: Mid-price should be None when bid/ask are None
        assert ticker.mid_price is None

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        price=price_strategy(),
        bid_ask=bid_ask_spread_strategy(),
        volume=volume_strategy(),
    )
    @settings(max_examples=300, deadline=None)
    def test_full_ticker_creation_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        price: Decimal,
        bid_ask: tuple[Decimal, Decimal],
        volume: Decimal,
    ) -> None:
        """Property: Full ticker with all core fields should maintain data integrity."""
        bid, ask = bid_ask

        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            price=price,
            bid=bid,
            ask=ask,
            volume=volume,
        )

        # Properties: All fields should be preserved exactly
        assert ticker.symbol == ticker_symbol
        assert ticker.exchange == exchange
        assert ticker.timestamp == timestamp
        assert ticker.price == price
        assert ticker.bid == bid
        assert ticker.ask == ask
        assert ticker.volume == volume

        # Properties: Mid-price calculation should be exact
        expected_mid = (bid + ask) / Decimal(2)
        assert ticker.mid_price == expected_mid

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        bid_ask=bid_ask_spread_strategy(),
    )
    @settings(max_examples=500, deadline=None)
    def test_mid_price_calculation_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        bid_ask: tuple[Decimal, Decimal],
    ) -> None:
        """Property: Mid-price calculation should follow mathematical properties."""
        bid, ask = bid_ask

        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            bid=bid,
            ask=ask,
        )

        mid_price = ticker.mid_price
        assert mid_price is not None

        # Properties: Mathematical invariants for mid-price
        assert bid <= mid_price <= ask  # Mid-price should be between bid and ask
        assert mid_price == (bid + ask) / Decimal(2)  # Exact arithmetic

        # Properties: Precision preservation
        # If bid and ask have same precision, mid-price should have at most one more decimal place
        bid_exp = bid.as_tuple().exponent
        ask_exp = ask.as_tuple().exponent
        mid_exp = mid_price.as_tuple().exponent

        # Handle special values (infinity, NaN) - skip precision check for these
        if isinstance(bid_exp, str) or isinstance(ask_exp, str) or isinstance(mid_exp, str):
            return

        bid_places = max(0, -bid_exp)
        ask_places = max(0, -ask_exp)
        mid_places = max(0, -mid_exp)
        max_input_places = max(bid_places, ask_places)
        assert mid_places <= max_input_places + 1

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        bid=st.one_of(st.none(), price_strategy()),
        ask=st.one_of(st.none(), price_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_mid_price_none_handling_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        bid: Decimal | None,
        ask: Decimal | None,
    ) -> None:
        """Property: Mid-price should be None when either bid or ask is None."""
        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            bid=bid,
            ask=ask,
        )

        # Property: Mid-price calculation logic
        if bid is None or ask is None:
            assert ticker.mid_price is None
        else:
            assert ticker.mid_price is not None
            assert ticker.mid_price == (bid + ask) / Decimal(2)

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        decimal_field=st.sampled_from(["price", "bid", "ask", "volume"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_decimal_field_validation_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        decimal_field: str,
    ) -> None:
        """Property: Decimal fields should reject non-finite and negative values."""
        base_kwargs: dict[str, Any] = {
            "symbol": ticker_symbol,
            "exchange": exchange,
            "timestamp": timestamp,
        }

        # Property: Non-finite values should be rejected
        for invalid_value in [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")]:
            kwargs: dict[str, Any] = base_kwargs.copy()
            kwargs[decimal_field] = invalid_value

            with pytest.raises(
                ValidationError, match=r"Non-finite values.*not allowed in financial calculations"
            ):
                Ticker(**kwargs)

        # Property: Negative values should be rejected
        kwargs_negative: dict[str, Any] = base_kwargs.copy()
        kwargs_negative[decimal_field] = Decimal("-0.001")

        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            Ticker(**kwargs_negative)

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        price=price_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_ticker_immutability_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        price: Decimal,
    ) -> None:
        """Property: Ticker instances should be completely immutable."""
        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            price=price,
        )

        # Property: All field modifications should raise ValidationError
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.symbol = BTC_USDC_BP  # Different symbol

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.exchange = ExchangeName.BACKPACK

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.timestamp = timestamp + timedelta(seconds=1)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ticker.price = price + Decimal(1)

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        hl_details=hyperliquid_details_strategy(),
        bp_details=backpack_details_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_exchange_details_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        hl_details: HyperliquidTickerDetails,
        bp_details: BackpackTickerDetails,
    ) -> None:
        """Property: Exchange-specific details should be preserved correctly."""
        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: Details should be preserved
        assert ticker.hl_details == hl_details
        assert ticker.bp_details == bp_details

        # Properties: Details should be immutable
        if ticker.hl_details is not None:
            with pytest.raises(ValidationError, match="Instance is frozen"):
                ticker.hl_details.mid_price_source = "modified"

        if ticker.bp_details is not None and ticker.bp_details.trades is not None:
            with pytest.raises(ValidationError, match="Instance is frozen"):
                ticker.bp_details.trades = 999999

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        parseable_inputs=st.one_of(
            st.integers(min_value=0, max_value=1000000),
            st.floats(min_value=0.0, max_value=1000000.0, allow_nan=False, allow_infinity=False),
            st.text().filter(lambda x: x.replace(".", "").replace("-", "").isdigit()),
        ),
    )
    @settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_decimal_parsing_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        parseable_inputs: float | str,
    ) -> None:
        """Property: Ticker should correctly parse various numeric input types to Decimal."""
        # Skip edge cases that might cause precision issues
        if isinstance(parseable_inputs, float):
            assume(abs(parseable_inputs) < 1e15)  # Avoid precision loss
            assume(parseable_inputs >= 0)  # Ensure non-negative

        # This test explicitly verifies that Ticker can parse various input types (int, float, str)
        # to Decimal for financial calculations. The float input is intentional to test the model's
        # input validation and conversion capabilities. The Ticker model is designed to handle
        # these conversions internally and will convert the float to Decimal safely.
        # The assumption above ensures we only test with reasonable float values.
        price_input = cast(Decimal | None, parseable_inputs)
        assert isinstance(parseable_inputs, (int, float, str))  # Runtime verification of input type

        ticker = Ticker(
            symbol=ticker_symbol,
            exchange=exchange,
            timestamp=timestamp,
            price=price_input,
        )

        # Property: Price should be converted to Decimal
        assert isinstance(ticker.price, Decimal)
        assert ticker.price >= 0  # Should maintain non-negativity

        # Property: Should preserve reasonable precision
        if isinstance(parseable_inputs, (int, str)):
            # Exact conversion expected for integers and valid decimal strings
            expected = Decimal(str(parseable_inputs))
            assert ticker.price == expected

    @given(
        ticker_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        invalid_input=st.one_of(
            st.just(""),
            st.just("   "),
            st.just("not_a_number"),
            st.just("12..34"),
            st.text().filter(
                lambda x: x.strip() and not x.replace(".", "").replace("-", "").isdigit()
            ),
        ),
    )
    @settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_invalid_decimal_input_rejection_properties(
        self,
        ticker_symbol: Symbol,
        exchange: ExchangeName,
        invalid_input: str,
    ) -> None:
        """Property: Invalid decimal inputs should always raise ValidationError."""
        with pytest.raises(ValidationError):
            Ticker(
                symbol=ticker_symbol,
                exchange=exchange,
                timestamp=datetime.now(UTC),
                price=cast(Decimal, invalid_input),
            )


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC DETAIL MODELS
# =============================================================================


class TestHyperliquidTickerDetailsProperties:
    """Property-based tests for HyperliquidTickerDetails model."""

    @given(
        mid_price_source=st.one_of(
            st.none(),
            st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_hyperliquid_details_creation_properties(self, mid_price_source: str | None) -> None:
        """Property: HyperliquidTickerDetails should accept valid inputs correctly."""
        details = HyperliquidTickerDetails(mid_price_source=mid_price_source)

        # Property: Field should be preserved
        assert details.mid_price_source == mid_price_source

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.mid_price_source = "modified"


class TestBackpackTickerDetailsProperties:
    """Property-based tests for BackpackTickerDetails model."""

    @given(
        first_price=st.one_of(st.none(), price_strategy()),
        high=st.one_of(st.none(), price_strategy()),
        low=st.one_of(st.none(), price_strategy()),
        price_change=st.one_of(
            st.none(), financial_decimal_strategy(min_value=-10000.0, max_value=10000.0)
        ),
        price_change_percent=st.one_of(
            st.none(), financial_decimal_strategy(min_value=-100.0, max_value=1000.0)
        ),
        quote_volume=st.one_of(st.none(), volume_strategy()),
        trades=st.one_of(st.none(), st.integers(min_value=0, max_value=1000000)),
    )
    @settings(max_examples=200, deadline=None)
    def test_backpack_details_creation_properties(
        self,
        first_price: Decimal | None,
        high: Decimal | None,
        low: Decimal | None,
        price_change: Decimal | None,
        price_change_percent: Decimal | None,
        quote_volume: Decimal | None,
        trades: int | None,
    ) -> None:
        """Property: BackpackTickerDetails should handle all field combinations correctly."""
        # Ensure high >= low if both are present
        if high is not None and low is not None and high < low:
            high, low = low, high

        details = BackpackTickerDetails(
            first_price=first_price,
            high=high,
            low=low,
            price_change=price_change,
            price_change_percent=price_change_percent,
            quote_volume=quote_volume,
            trades=trades,
        )

        # Properties: All fields should be preserved
        assert details.first_price == first_price
        assert details.high == high
        assert details.low == low
        assert details.price_change == price_change
        assert details.price_change_percent == price_change_percent
        assert details.quote_volume == quote_volume
        assert details.trades == trades

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.first_price = Decimal(999)

    @given(
        field_name=st.sampled_from(["first_price", "high", "low", "quote_volume"]),
        negative_value=financial_decimal_strategy(
            min_value=-1000.0, max_value=-0.01, allow_zero=False
        ),
    )
    @settings(max_examples=50, deadline=None)
    def test_backpack_details_non_negative_validation_properties(
        self, field_name: str, negative_value: Decimal
    ) -> None:
        """Property: Non-negative fields should reject negative values."""
        kwargs: dict[str, Any] = {field_name: negative_value}

        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            BackpackTickerDetails(**kwargs)

    @given(
        field_name=st.sampled_from(["price_change", "price_change_percent"]),
        negative_value=financial_decimal_strategy(
            min_value=-1000.0, max_value=-0.01, allow_zero=False
        ),
    )
    @settings(max_examples=50, deadline=None)
    def test_backpack_details_negative_change_allowed_properties(
        self, field_name: str, negative_value: Decimal
    ) -> None:
        """Property: Price change fields should allow negative values."""
        kwargs: dict[str, Any] = {field_name: negative_value}

        # Should not raise an error
        details = BackpackTickerDetails(**kwargs)
        assert getattr(details, field_name) == negative_value

    @given(negative_trades=st.integers(min_value=-1000, max_value=-1))
    @settings(max_examples=50, deadline=None)
    def test_backpack_details_trades_validation_properties(self, negative_trades: int) -> None:
        """Property: Trades field should reject negative values."""
        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            BackpackTickerDetails(trades=negative_trades)
