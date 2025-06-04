import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids


def test_all_mids_happy_path() -> None:
    """Test all mids happy path."""
    obj: dict[str, str] = {"ETH": "3000.0", "BTC": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_empty_dict() -> None:
    """Test all mids empty dict."""
    obj: dict[str, str] = {}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == {}


def test_all_mids_non_dict() -> None:
    """Test all mids non dict."""
    obj: list[str] = ["ETH", "BTC"]
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_non_str_keys() -> None:
    """Test all mids non str keys."""
    obj: dict[object, str] = {123: "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_non_str_values() -> None:
    """Test all mids non str values."""
    obj: dict[str, object] = {"ETH": 3000.0}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_extra_field_like_nested() -> None:
    """Test all mids extra field like nested."""
    # Not possible, as extra fields are not a concept in a dict root model, but test for nested dict
    obj: dict[str, object] = {"ETH": {"mid": "3000.0"}}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_symbol_too_long() -> None:
    """Test all mids symbol too long."""
    obj: dict[str, str] = {"E" * 65: "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_too_long() -> None:
    """Test all mids price too long."""
    obj: dict[str, str] = {"ETH": "1" * 65}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_not_decimal() -> None:
    """Test all mids price not decimal."""
    obj: dict[str, str] = {"ETH": "not_a_number"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_nan() -> None:
    """Test all mids price nan."""
    obj: dict[str, str] = {"ETH": "NaN"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_inf() -> None:
    """Test all mids price inf."""
    obj: dict[str, str] = {"ETH": "inf"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_adversarial_strings() -> None:
    """Test all mids adversarial strings."""
    # Should pass as long as they are valid decimals and within length
    obj: dict[str, str] = {"DROP TABLE users;": "123.456"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_unicode_symbol() -> None:
    """Test all mids unicode symbol."""
    # Unicode symbol (e.g., Greek letter)
    obj: dict[str, str] = {"ΞTH": "3000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_symbol_with_whitespace() -> None:
    """Test all mids symbol with whitespace."""
    # Symbol with whitespace
    obj: dict[str, str] = {"BTC USD": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_symbol_with_control_char() -> None:
    """Test all mids symbol with control char."""
    # Symbol with control character (should be accepted if string and length ok)
    obj: dict[str, str] = {"ETH\n": "3000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_price_excessive_precision() -> None:
    """Test all mids price excessive precision."""
    # Price with excessive precision
    obj: dict[str, str] = {"ETH": "0.123456789012345678901234567890"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_price_zero_and_negative() -> None:
    """Test all mids price zero and negative."""
    # Price as zero and negative (should both be accepted)
    obj: dict[str, str] = {"ETH": "0"}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise
    obj2: dict[str, str] = {"ETH": "-1.0"}
    HyperliquidRawAllMids.model_validate(obj2)  # Should not raise


def test_all_mids_price_leading_trailing_whitespace() -> None:
    """Test all mids price leading trailing whitespace."""
    # Price with leading/trailing whitespace (should be accepted)
    obj: dict[str, str] = {"ETH": " 3000.0 "}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise


def test_all_mids_empty_string_symbol_or_price() -> None:
    """Test all mids empty string symbol or price."""
    # Empty string as symbol or price (should be rejected)
    obj: dict[str, str] = {"": "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)
    obj2: dict[str, str] = {"ETH": ""}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj2)


def test_all_mids_many_assets() -> None:
    """Test all mids many assets."""
    # Many assets in dict
    obj: dict[str, str] = {f"ASSET{i}": str(i) for i in range(50)}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_padded_zero_price() -> None:
    """Test all mids padded zero price."""
    # Price as string with padded zeros
    obj: dict[str, str] = {"ETH": "0003000.00"}
    assert HyperliquidRawAllMids.model_validate(obj).root["ETH"].lstrip("0") == "3000.00"


def test_all_mids_testnet_asset_symbol() -> None:
    """Test all mids testnet asset symbol."""
    # Testnet asset symbol (e.g., "tETH")
    obj: dict[str, str] = {"tETH": "123.45"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_very_large_price() -> None:
    """Test all mids very large price."""
    # Very large price (should be accepted)
    obj: dict[str, str] = {"BTC": "1e1000"}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise


def test_all_mids_symbol_with_special_chars() -> None:
    """Test all mids symbol with special chars."""
    # Symbol with special characters
    obj: dict[str, str] = {"BTC-USD!@#": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj
