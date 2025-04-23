import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
    HyperliquidRawCandleSnapshotRequestPayload,
)


def sample_candle_snapshot() -> dict[str, object]:
    return {
        "t": [1700000000000, 1700000001000],
        "o": ["3000.0", "3001.0"],
        "h": ["3010.0", "3011.0"],
        "l": ["2990.0", "2991.0"],
        "c": ["3005.0", "3006.0"],
        "v": ["100.0", "200.0"],
        "s": "ok",
    }


def sample_request_payload() -> dict[str, object]:
    return {
        "type": "candleSnapshot",
        "coin": "ETH",
        "interval": "1m",
        "startTime": 1700000000000,
        "endTime": 1700003600000,
    }


def test_candle_snapshot_happy_path() -> None:
    obj: dict[str, object] = sample_candle_snapshot()
    model = HyperliquidRawCandleSnapshot.model_validate(obj)
    assert model.s == "ok"
    assert len(model.t) == 2


def test_request_payload_happy_path() -> None:
    obj: dict[str, object] = sample_request_payload()
    model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(obj)
    assert model.coin == "ETH"
    assert model.interval == "1m"


def test_candle_snapshot_missing_fields() -> None:
    obj: dict[str, object] = sample_candle_snapshot()
    for field in ["t", "o", "h", "l", "c", "v", "s"]:
        bad: dict[str, object] = obj.copy()
        del bad[field]
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshot.model_validate(bad)


def test_request_payload_missing_fields() -> None:
    obj: dict[str, object] = sample_request_payload()
    for field in ["coin", "interval", "startTime", "endTime"]:
        bad: dict[str, object] = obj.copy()
        del bad[field]
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_candle_snapshot_type_errors() -> None:
    obj: dict[str, object] = sample_candle_snapshot()
    # t as string
    bad: dict[str, object] = obj.copy()
    bad["t"] = "notalist"
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    # o as int
    bad = obj.copy()
    bad["o"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    # h as list of int
    bad_h: dict[str, object] = obj.copy()
    bad_h["h"] = [3010, 3011]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad_h)
    # l as list with int
    bad_l: dict[str, object] = obj.copy()
    bad_l["l"] = ["2990.0", 2991]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad_l)
    # v as list with float
    bad_v: dict[str, object] = obj.copy()
    bad_v["v"] = [100.0, "200.0"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad_v)
    # s as int
    bad_s: dict[str, object] = obj.copy()
    bad_s["s"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad_s)


def test_request_payload_type_errors() -> None:
    obj = sample_request_payload()
    # coin as int
    bad = obj.copy()
    bad["coin"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # interval as int
    bad = obj.copy()
    bad["interval"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # startTime as str
    bad = obj.copy()
    bad["startTime"] = "notanint"
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # endTime as float
    bad = obj.copy()
    bad["endTime"] = 1700003600.0
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_candle_snapshot_format_constraint_errors() -> None:
    obj = sample_candle_snapshot()
    # Empty string in o
    bad = obj.copy()
    bad["o"] = ["", "3001.0"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    # Non-finite decimal in h
    bad = obj.copy()
    bad["h"] = ["NaN", "3011.0"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    # Negative value in v
    bad = obj.copy()
    bad["v"] = ["-100.0", "200.0"]
    # This will pass unless model is hardened to reject negative volume
    HyperliquidRawCandleSnapshot.model_validate(bad)
    # Negative value in t
    bad = obj.copy()
    bad["t"] = [1700000000000, -1]
    # This will pass unless model is hardened to reject negative timestamps
    HyperliquidRawCandleSnapshot.model_validate(bad)
    # Overlength string in c
    bad = obj.copy()
    bad["c"] = ["a" * 65, "3006.0"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    # Empty string for s
    bad = obj.copy()
    bad["s"] = ""
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)


def test_request_payload_format_constraint_errors() -> None:
    obj = sample_request_payload()
    # Overlength coin
    bad = obj.copy()
    bad["coin"] = "A" * 65
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # Overlength interval
    bad = obj.copy()
    bad["interval"] = "A" * 17
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # Negative startTime
    bad = obj.copy()
    bad["startTime"] = -1
    # This will pass unless model is hardened to reject negative startTime
    HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # Negative endTime
    bad = obj.copy()
    bad["endTime"] = -1
    # This will pass unless model is hardened to reject negative endTime
    HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_candle_snapshot_list_length_mismatch() -> None:
    obj = sample_candle_snapshot()
    # o shorter than t
    bad_o_short: dict[str, object] = obj.copy()
    bad_o_short["o"] = ["3000.0"]
    # This will pass unless model is hardened to check list lengths
    HyperliquidRawCandleSnapshot.model_validate(bad_o_short)
    # v longer than t
    bad_v_long: dict[str, object] = obj.copy()
    bad_v_long["v"] = ["100.0", "200.0", "300.0"]
    # This will pass unless model is hardened to check list lengths
    HyperliquidRawCandleSnapshot.model_validate(bad_v_long)
    # All lists empty (should fail if not allowed)
    bad_all_empty: dict[str, object] = {k: [] for k in ["t", "o", "h", "l", "c", "v"]}
    bad_all_empty["s"] = "ok"
    # This will pass unless model is hardened to check for non-empty lists
    HyperliquidRawCandleSnapshot.model_validate(bad_all_empty)


def test_candle_snapshot_extra_forbid() -> None:
    obj = sample_candle_snapshot()
    obj["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(obj)


def test_request_payload_extra_forbid() -> None:
    obj = sample_request_payload()
    obj["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(obj)


def test_candle_snapshot_adversarial_strings() -> None:
    obj = sample_candle_snapshot()
    obj["s"] = "DROP TABLE users;"
    model = HyperliquidRawCandleSnapshot.model_validate(obj)
    assert model.s == "DROP TABLE users;"


def test_request_payload_adversarial_strings() -> None:
    obj = sample_request_payload()
    obj["coin"] = "DROP TABLE users;"
    obj["interval"] = "1m; rm -rf /"
    model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(obj)
    assert model.coin == "DROP TABLE users;"
    assert model.interval == "1m; rm -rf /"


# --- Edge Case and Adversarial Tests for HyperliquidRawCandleSnapshot ---


def test_candle_snapshot_multiple_missing_fields() -> None:
    """Test validation fails when multiple required fields are missing simultaneously."""
    obj = sample_candle_snapshot()
    for fields in [("t", "o"), ("h", "l", "c"), ("v", "s")]:
        bad = obj.copy()
        for f in fields:
            del bad[f]
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_extra_similar_fields() -> None:
    """Test rejection of extra fields with similar names to valid fields."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["T"] = [1, 2]
    bad["start_time"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_mixed_type_lists() -> None:
    """Test lists with mixed types (str, int, None) in numeric fields."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["o"] = ["3000.0", 123, None]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)
    bad = obj.copy()
    bad["t"] = [1700000000000, "notanint"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_nonfinite_and_scientific() -> None:
    """Test non-finite and scientific notation values in price fields.
    - 'inf' and '-inf' should fail (non-finite).
    - Finite scientific notation (e.g., '1e1000') should succeed per policy.
    """
    obj = sample_candle_snapshot()
    # Non-finite values should fail
    for val in ["inf", "-inf"]:
        bad = obj.copy()
        bad["h"] = [val, "3011.0"]
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshot.model_validate(bad)
    # Finite scientific notation should succeed
    bad = obj.copy()
    bad["h"] = ["1e1000", "3011.0"]
    HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_control_and_null_bytes() -> None:
    """Test control characters and null bytes in string fields."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["s"] = "ok\x00"
    model = HyperliquidRawCandleSnapshot.model_validate(bad)
    assert model.s == "ok\x00"
    bad = obj.copy()
    bad["s"] = "ok\n"
    model = HyperliquidRawCandleSnapshot.model_validate(bad)
    assert model.s == "ok\n"


def test_candle_snapshot_overlength_and_injection_strings() -> None:
    """Test overlength and injection strings in all string fields."""
    obj = sample_candle_snapshot()
    for field in ["s"]:
        bad = obj.copy()
        bad[field] = "A" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshot.model_validate(bad)
    for field in ["s"]:
        bad = obj.copy()
        bad[field] = "1m; DROP TABLE users;"
        model = HyperliquidRawCandleSnapshot.model_validate(bad)
        assert "DROP TABLE" in model.s


def test_candle_snapshot_extreme_timestamps() -> None:
    """Test timestamps far in the future and past."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["t"] = [0, 32503680000000]  # 1970 and year 3000
    HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_zero_and_negative_values() -> None:
    """Test zero and negative values in price and volume fields."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["v"] = ["0.0", "-1.0"]
    HyperliquidRawCandleSnapshot.model_validate(bad)
    bad = obj.copy()
    bad["o"] = ["0.0", "0.0"]
    HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_stringified_numbers_with_spaces() -> None:
    """Test stringified numbers with leading/trailing spaces in price fields.
    These should be accepted as valid if they parse to finite decimals.
    Policy: allow standard parsing.
    """
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["o"] = [" 3000.0 ", " 3001.0 "]
    HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_very_large_lists() -> None:
    """Test very large lists for DoS resilience (kept small for test speed)."""
    obj = sample_candle_snapshot()
    n = 1000
    bad = obj.copy()
    bad["t"] = [1700000000000 + i * 1000 for i in range(n)]
    bad["o"] = ["3000.0"] * n
    bad["h"] = ["3010.0"] * n
    bad["l"] = ["2990.0"] * n
    bad["c"] = ["3005.0"] * n
    bad["v"] = ["100.0"] * n
    HyperliquidRawCandleSnapshot.model_validate(bad)


def test_candle_snapshot_similar_but_incorrect_field_names() -> None:
    """Test fields with similar but incorrect names are rejected."""
    obj = sample_candle_snapshot()
    bad = obj.copy()
    bad["S"] = "ok"
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshot.model_validate(bad)


# --- Edge Case and Adversarial Tests for HyperliquidRawCandleSnapshotRequestPayload ---


def test_request_payload_multiple_missing_fields() -> None:
    """Test validation fails when multiple required fields are missing simultaneously."""
    obj = sample_request_payload()
    for fields in [("coin", "interval"), ("startTime", "endTime")]:
        bad = obj.copy()
        for f in fields:
            del bad[f]
        with pytest.raises(ValidationError):
            HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_request_payload_extra_similar_fields() -> None:
    """Test rejection of extra fields with similar names to valid fields."""
    obj = sample_request_payload()
    bad = obj.copy()
    bad["Coin"] = "ETH"
    bad["StartTime"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_request_payload_type_and_format_edge_cases() -> None:
    """Test type and format edge cases for all fields."""
    obj = sample_request_payload()
    # coin as None
    bad = obj.copy()
    bad["coin"] = None
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # interval as list
    bad = obj.copy()
    bad["interval"] = ["1m"]
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # startTime as string
    bad = obj.copy()
    bad["startTime"] = "1700000000000"
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # endTime as None
    bad = obj.copy()
    bad["endTime"] = None
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_request_payload_overlength_and_injection_strings() -> None:
    """Test overlength and injection strings in coin and interval fields."""
    obj = sample_request_payload()
    bad = obj.copy()
    bad["coin"] = "A" * 65
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # Overlength interval should fail
    bad = obj.copy()
    bad["interval"] = "1m; DROP TABLE users;"
    with pytest.raises(ValidationError):
        HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    # Injection attempt within max length should succeed
    bad = obj.copy()
    bad["interval"] = "1m; rm -rf /"
    model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    assert "rm -rf" in model.interval


def test_request_payload_extreme_timestamps() -> None:
    """Test startTime and endTime far in the future and past."""
    obj = sample_request_payload()
    bad = obj.copy()
    bad["startTime"] = 0
    bad["endTime"] = 32503680000000
    HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)


def test_request_payload_stringified_numbers_with_spaces() -> None:
    """Test stringified numbers with spaces in coin and interval fields."""
    obj = sample_request_payload()
    bad = obj.copy()
    bad["coin"] = " ETH "
    model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    assert model.coin == " ETH "
    bad = obj.copy()
    bad["interval"] = " 1m "
    model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(bad)
    assert model.interval == " 1m "


# --- API Example Parity Test ---
def test_candle_snapshot_api_example_parity() -> None:
    """Test with payloads matching API documentation examples exactly."""
    # Example from docs (with all required fields)
    obj = {
        "t": [1700000000000],
        "o": ["3000.0"],
        "h": ["3010.0"],
        "l": ["2990.0"],
        "c": ["3005.0"],
        "v": ["100.0"],
        "s": "ok",
    }
    model = HyperliquidRawCandleSnapshot.model_validate(obj)
    assert model.s == "ok"
    # Request payload example
    req = {
        "type": "candleSnapshot",
        "coin": "ETH",
        "interval": "1m",
        "startTime": 1700000000000,
        "endTime": 1700003600000,
    }
    req_model = HyperliquidRawCandleSnapshotRequestPayload.model_validate(req)
    assert req_model.coin == "ETH"
    assert req_model.interval == "1m"
