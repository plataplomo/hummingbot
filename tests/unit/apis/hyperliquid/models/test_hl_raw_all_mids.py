import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids


def test_all_mids_happy_path() -> None:
    obj: dict[str, str] = {"ETH": "3000.0", "BTC": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_empty_dict() -> None:
    obj: dict[str, str] = {}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == {}


def test_all_mids_non_dict() -> None:
    obj: list[str] = ["ETH", "BTC"]
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_non_str_keys() -> None:
    obj: dict[object, str] = {123: "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_non_str_values() -> None:
    obj: dict[str, object] = {"ETH": 3000.0}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_extra_field_like_nested() -> None:
    # Not possible, as extra fields are not a concept in a dict root model, but test for nested dict
    obj: dict[str, object] = {"ETH": {"mid": "3000.0"}}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_symbol_too_long() -> None:
    obj: dict[str, str] = {"E" * 65: "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_too_long() -> None:
    obj: dict[str, str] = {"ETH": "1" * 65}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_not_decimal() -> None:
    obj: dict[str, str] = {"ETH": "not_a_number"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_nan() -> None:
    obj: dict[str, str] = {"ETH": "NaN"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_price_inf() -> None:
    obj: dict[str, str] = {"ETH": "inf"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)


def test_all_mids_adversarial_strings() -> None:
    # Should pass as long as they are valid decimals and within length
    obj: dict[str, str] = {"DROP TABLE users;": "123.456"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_unicode_symbol() -> None:
    # Unicode symbol (e.g., Greek letter)
    obj: dict[str, str] = {"ΞTH": "3000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_symbol_with_whitespace() -> None:
    # Symbol with whitespace
    obj: dict[str, str] = {"BTC USD": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_symbol_with_control_char() -> None:
    # Symbol with control character (should be accepted if string and length ok)
    obj: dict[str, str] = {"ETH\n": "3000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_price_excessive_precision() -> None:
    # Price with excessive precision
    obj: dict[str, str] = {"ETH": "0.123456789012345678901234567890"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_price_zero_and_negative() -> None:
    # Price as zero and negative (should both be accepted)
    obj: dict[str, str] = {"ETH": "0"}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise
    obj2: dict[str, str] = {"ETH": "-1.0"}
    HyperliquidRawAllMids.model_validate(obj2)  # Should not raise


def test_all_mids_price_leading_trailing_whitespace() -> None:
    # Price with leading/trailing whitespace (should be accepted)
    obj: dict[str, str] = {"ETH": " 3000.0 "}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise


def test_all_mids_empty_string_symbol_or_price() -> None:
    # Empty string as symbol or price (should be rejected)
    obj: dict[str, str] = {"": "3000.0"}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj)
    obj2: dict[str, str] = {"ETH": ""}
    with pytest.raises(ValidationError):
        HyperliquidRawAllMids.model_validate(obj2)


def test_all_mids_many_assets() -> None:
    # Many assets in dict
    obj: dict[str, str] = {f"ASSET{i}": str(i) for i in range(50)}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_padded_zero_price() -> None:
    # Price as string with padded zeros
    obj: dict[str, str] = {"ETH": "0003000.00"}
    assert HyperliquidRawAllMids.model_validate(obj).root["ETH"].lstrip("0") == "3000.00"


def test_all_mids_testnet_asset_symbol() -> None:
    # Testnet asset symbol (e.g., "tETH")
    obj: dict[str, str] = {"tETH": "123.45"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj


def test_all_mids_very_large_price() -> None:
    # Very large price (should be accepted)
    obj: dict[str, str] = {"BTC": "1e1000"}
    HyperliquidRawAllMids.model_validate(obj)  # Should not raise


def test_all_mids_symbol_with_special_chars() -> None:
    # Symbol with special characters
    obj: dict[str, str] = {"BTC-USD!@#": "40000.0"}
    model = HyperliquidRawAllMids.model_validate(obj)
    assert model.root == obj
