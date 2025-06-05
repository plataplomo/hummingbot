"""Unit tests for Hyperliquid Raw API Error Models."""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError


# --- Helpers ---
def valid_error() -> dict[str, object]:
    """Return valid error for testing."""
    return {"error": "Something went wrong"}


# --- Tests ---
def test_happy_path() -> None:
    """Test happy path."""
    obj = HyperliquidRawApiError.model_validate(valid_error())
    assert obj.error == "Something went wrong"


def test_missing_error_field() -> None:
    """Test missing error field."""
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({})


def test_error_type_errors() -> None:
    """Test error type errors."""
    for bad in [123, 1.5, True, None, ["err"], {"msg": "err"}]:
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_error_empty_and_whitespace() -> None:
    """Test error empty and whitespace."""
    for bad in ["", "   "]:
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_error_too_long() -> None:
    """Test error too long."""
    msg = "a" * 1025
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({"error": msg})


def test_error_invalid_utf8() -> None:
    """Test error invalid utf8."""
    # Simulate a string with invalid UTF-8 by using surrogates (which are not valid in UTF-8)
    bad = "bad\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({"error": bad})


def test_extra_field() -> None:
    """Test extra field."""
    d = valid_error().copy()
    d["foo"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate(d)


def test_adversarial_strings_should_pass() -> None:
    """Test adversarial strings should pass."""
    # These are structurally valid and must be accepted
    for s in [
        "' OR 1=1 --",
        "<script>alert(1)</script>",
        "💣",
        "error: DROP TABLE users;",
        "SELECT * FROM errors WHERE msg='fail'",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_looks_like_json() -> None:
    """Test error string looks like json."""
    # Should accept strings that look like JSON, arrays, or objects
    for s in [
        '{"error": "fail"}',
        '["fail", "error"]',
        "{error: true}",
        '{\n  "error": "fail"\n}',
        "[1, 2, 3]",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_unicode_edge_cases() -> None:
    """Test error string unicode edge cases."""
    # Accept strings with combining characters, right-to-left, zero-width joiners
    for s in [
        "e\u0301rror",  # e + combining acute accent
        "\u202eerror",  # Right-to-left override
        "e\u200derror",  # Zero-width joiner
        "\u202aerror\u202c",  # Left-to-right embedding
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_extremely_short() -> None:
    """Test error string extremely short."""
    # Accept single character and whitespace+char
    for s in ["e", " e", "e ", "\te", "e\n"]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_with_control_characters() -> None:
    """Test error string with control characters."""
    # Accept strings with newlines, tabs, control chars (as long as valid UTF-8)
    for s in [
        "error\nmessage",
        "error\tmessage",
        "error\x07bell",
        "error\r\nnextline",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_looks_like_number_or_bool() -> None:
    """Test error string looks like number or bool."""
    # Accept strings that look like numbers or booleans
    for s in ["0", "1", "1234567890", "true", "false", "null", "None", "NaN"]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_long_repeated_pattern() -> None:
    """Test error string long repeated pattern."""
    # Accept long but valid repeated patterns (under 1024 chars)
    s = "error" * 200  # 1000 chars
    obj = HyperliquidRawApiError.model_validate({"error": s})
    assert obj.error == s


def test_error_string_with_rare_unicode() -> None:
    """Test error string with rare unicode."""
    # Accept rare but valid unicode (e.g., snowman, musical symbol)
    for s in [
        "error \u2603",  # snowman
        "error \U0001d11e",  # musical symbol G clef
        "error \u200b",  # zero-width space
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_with_escape_sequences() -> None:
    """Test error string with escape sequences."""
    # Accept strings with escape sequences
    for s in [
        "error\\nnewline",
        "error\\t tab",
        "error\\u2603 snowman",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_with_bidirectional_text() -> None:
    """Test error string with bidirectional text."""
    # Accept strings with bidirectional text controls
    for s in [
        "error \u202etxet detcerid-ot-thgir",  # RLO
        "\u202berror\u202c",  # RLE ... PDF
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_mixed_language_scripts() -> None:
    """Test error string mixed language scripts."""
    # Accept strings with mixed scripts (Latin + Cyrillic, Arabic + English)
    for s in [
        "Ошибка: error",  # Russian + English
        "خطأ: error",  # Arabic + English
        "error 错误",  # English + Chinese
        "エラー: error",  # Japanese + English
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_with_emoji_sequences() -> None:
    """Test error string with emoji sequences."""
    # Accept emoji with skin tone modifiers, ZWJ sequences
    for s in [
        "error 👨🏽‍💻",  # man technologist: medium skin tone
        "error 👩‍🔬",  # woman scientist
        "error 🏳️‍🌈",  # rainbow flag
        "error 👨‍👩‍👧‍👦",  # family emoji
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_excessive_whitespace() -> None:
    """Test error string excessive whitespace."""
    # Accept strings with excessive whitespace
    for s in [
        "   error   ",
        "error    message",
        "error\t\tmessage",
        "error\n\nmessage",
        "error\r\n\r\nmessage",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_only_symbols_or_punctuation() -> None:
    """Test error string only symbols or punctuation."""
    # Accept strings with only symbols or punctuation
    for s in [
        "!!!",
        "???",
        "@#$%^&*()",
        "-=~`|{}[]",
        "•••••",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_mimics_other_api_errors() -> None:
    """Test error string mimics other api errors."""
    # Accept strings that mimic common error messages from other APIs
    for s in [
        "INVALID_SIGNATURE",
        "Order not found",
        "INSUFFICIENT_FUNDS",
        "Too Many Requests",
        "SERVER_ERROR",
        "Unauthorized",
        "ResourceNotFound",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_various_line_breaks() -> None:
    """Test error string various line breaks."""
    # Accept strings with different line break formats
    for s in [
        "error\nmessage",
        "error\rmessage",
        "error\r\nmessage",
        "error\n\rmessage",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_surrogate_pairs_and_high_codepoints() -> None:
    """Test error string surrogate pairs and high codepoints."""
    # Accept valid UTF-8 high code points (e.g., musical symbols, rare emoji)
    for s in [
        "error \U0001f4a9",  # pile of poo
        "error \U0001f600",  # grinning face
        "error \U0001f9d1\U0001f3fb",  # person: light skin tone
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_with_invisible_characters() -> None:
    """Test error string with invisible characters."""
    # Accept strings with invisible but valid Unicode (LRM, RLM)
    for s in [
        "error\u200e",  # left-to-right mark
        "error\u200f",  # right-to-left mark
        "error\u2060",  # word joiner
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_error_string_palindrome_and_mirrored() -> None:
    """Test error string palindrome and mirrored."""
    # Accept palindromes and mirrored text
    for s in [
        "racecar",
        "madam",
        "error level reviver level error",
        "\u202eabc\u202c",  # mirrored by RLO/PDF
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s
