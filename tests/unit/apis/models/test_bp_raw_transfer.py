import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_transfer import (
    BackpackRawDeposit,
    BackpackRawLiquidation,
    BackpackRawWithdrawal,
)


def valid_withdrawal() -> dict[str, Any]:
    return {
        "id": "wd_123",
        "asset": "USDC",
        "amount": "100.0",
        "status": "pending",
    }


def test_BackpackRawWithdrawal_happy_path() -> None:
    obj = BackpackRawWithdrawal.model_validate(valid_withdrawal())
    assert obj.id == "wd_123"
    assert obj.asset == "USDC"
    assert obj.amount == "100.0"
    assert obj.status == "pending"


def test_BackpackRawWithdrawal_missing_required_fields() -> None:
    for field in ["id", "asset", "amount", "status"]:
        p: dict[str, Any] = valid_withdrawal().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_withdrawal().copy()
    p["amount"] = 100.0
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)
    p = valid_withdrawal().copy()
    p["status"] = ["pending"]
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_withdrawal().copy()
    p["amount"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)
    p = valid_withdrawal().copy()
    p["status"] = "notastatus"
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_extra_field() -> None:
    p: dict[str, Any] = valid_withdrawal().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_withdrawal().copy()
    p["asset"] = None
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)
    # Unicode/control chars
    p = valid_withdrawal().copy()
    p["asset"] = "USDC\x00"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "USDC" in obj.asset
    # SQL injection
    p = valid_withdrawal().copy()
    p["id"] = "wd_123; DROP TABLE withdrawals;"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "wd_123" in obj.id
    # XSS
    p = valid_withdrawal().copy()
    p["asset"] = "<script>alert(1)</script>"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "script" in obj.asset
    # Emoji
    p = valid_withdrawal().copy()
    p["asset"] = "USDC😀"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert obj.asset.startswith("USDC")
    # Whitespace
    p = valid_withdrawal().copy()
    p["asset"] = "   USDC   "
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "USDC" in obj.asset
    # Excessive length
    p = valid_withdrawal().copy()
    p["asset"] = "A" * 10000
    obj = BackpackRawWithdrawal.model_validate(p)
    assert obj.asset.startswith("A")
    # Negative/zero/NaN/inf amounts
    for val in ["-100.0", "0", "NaN", "inf", "-inf"]:
        p = valid_withdrawal().copy()
        p["amount"] = val
        if val == "0":
            # Should pass (zero is a valid decimal string)
            obj = BackpackRawWithdrawal.model_validate(p)
            assert obj.amount == "0"
        else:
            with pytest.raises(ValidationError):
                BackpackRawWithdrawal.model_validate(p)
    # Scientific notation
    p = valid_withdrawal().copy()
    p["amount"] = "1e6"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert obj.amount == "1e6"
    # Truncated JSON
    bad_json = '{"id": "wd_123", "asset": "USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def valid_deposit() -> dict[str, Any]:
    return {
        "id": "dp_456",
        "asset": "BTC",
        "amount": "0.5",
        "status": "completed",
    }


def test_BackpackRawDeposit_happy_path() -> None:
    obj = BackpackRawDeposit.model_validate(valid_deposit())
    assert obj.id == "dp_456"
    assert obj.asset == "BTC"
    assert obj.amount == "0.5"
    assert obj.status == "completed"


def test_BackpackRawDeposit_missing_required_fields() -> None:
    for field in ["id", "asset", "amount", "status"]:
        p: dict[str, Any] = valid_deposit().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_deposit().copy()
    p["amount"] = 0.5
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)
    p = valid_deposit().copy()
    p["status"] = ["completed"]
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_deposit().copy()
    p["amount"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)
    p = valid_deposit().copy()
    p["status"] = "notastatus"
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_extra_field() -> None:
    p: dict[str, Any] = valid_deposit().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_deposit().copy()
    p["asset"] = None
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)
    # Unicode/control chars
    p = valid_deposit().copy()
    p["asset"] = "BTC\x00"
    obj = BackpackRawDeposit.model_validate(p)
    assert "BTC" in obj.asset
    # SQL injection
    p = valid_deposit().copy()
    p["id"] = "dp_456; DROP TABLE deposits;"
    obj = BackpackRawDeposit.model_validate(p)
    assert "dp_456" in obj.id
    # XSS
    p = valid_deposit().copy()
    p["asset"] = "<img src=x onerror=alert(1)>"
    obj = BackpackRawDeposit.model_validate(p)
    assert "img" in obj.asset
    # Emoji
    p = valid_deposit().copy()
    p["asset"] = "BTC😀"
    obj = BackpackRawDeposit.model_validate(p)
    assert obj.asset.startswith("BTC")
    # Whitespace
    p = valid_deposit().copy()
    p["asset"] = "   BTC   "
    obj = BackpackRawDeposit.model_validate(p)
    assert "BTC" in obj.asset
    # Excessive length
    p = valid_deposit().copy()
    p["asset"] = "B" * 10000
    obj = BackpackRawDeposit.model_validate(p)
    assert obj.asset.startswith("B")
    # Negative/zero/NaN/inf amounts
    for val in ["-0.5", "0", "NaN", "inf", "-inf"]:
        p = valid_deposit().copy()
        p["amount"] = val
        if val == "0":
            obj = BackpackRawDeposit.model_validate(p)
            assert obj.amount == "0"
        else:
            with pytest.raises(ValidationError):
                BackpackRawDeposit.model_validate(p)
    # Scientific notation
    p = valid_deposit().copy()
    p["amount"] = "2e-3"
    obj = BackpackRawDeposit.model_validate(p)
    assert obj.amount == "2e-3"
    # Truncated JSON
    bad_json = '{"id": "dp_456", "asset": "BTC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def valid_liquidation() -> dict[str, Any]:
    return {
        "symbol": "BTC_USDC",
        "price": "45000.0",
        "quantity": "0.01",
        "side": "sell",
    }


def test_BackpackRawLiquidation_happy_path() -> None:
    obj = BackpackRawLiquidation.model_validate(valid_liquidation())
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "45000.0"
    assert obj.quantity == "0.01"
    assert obj.side == "sell"


def test_BackpackRawLiquidation_missing_required_fields() -> None:
    for field in ["symbol", "price", "quantity", "side"]:
        p: dict[str, Any] = valid_liquidation().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_liquidation().copy()
    p["price"] = 45000.0
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["side"] = ["sell"]
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_liquidation().copy()
    p["price"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["side"] = "notaside"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_extra_field() -> None:
    p: dict[str, Any] = valid_liquidation().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_liquidation().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
    # Unicode/control chars
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawLiquidation.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Emoji
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC😀"
    obj = BackpackRawLiquidation.model_validate(p)
    assert obj.symbol.startswith("BTC_USDC")
    # Whitespace
    p = valid_liquidation().copy()
    p["symbol"] = "   BTC_USDC   "
    obj = BackpackRawLiquidation.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Garbled numerics
    p = valid_liquidation().copy()
    p["price"] = "NaN"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
    # Excessive length
    p = valid_liquidation().copy()
    p["symbol"] = "A" * 10000
    obj = BackpackRawLiquidation.model_validate(p)
    assert obj.symbol.startswith("A")
    # Negative/zero/NaN/inf quantities
    for val in ["-0.01", "0", "NaN", "inf", "-inf"]:
        p = valid_liquidation().copy()
        p["quantity"] = val
        if val == "0":
            obj = BackpackRawLiquidation.model_validate(p)
            assert obj.quantity == "0"
        else:
            with pytest.raises(ValidationError):
                BackpackRawLiquidation.model_validate(p)
    # Scientific notation
    p = valid_liquidation().copy()
    p["quantity"] = "2e-3"
    obj = BackpackRawLiquidation.model_validate(p)
    assert obj.quantity == "2e-3"
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "price": "45000.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawWithdrawal_real_json_edge_case() -> None:
    """Validate BackpackRawWithdrawal using a real JSON payload with edge values."""
    payload = {
        "id": "wd_999999999999999999",
        "asset": "USDC_😀",
        "amount": "0.00000001",
        "status": "completed",
    }
    obj = BackpackRawWithdrawal.model_validate(payload)
    assert obj.asset == "USDC_😀"
    assert obj.amount == "0.00000001"
    assert obj.status == "completed"


def test_BackpackRawWithdrawal_corruption_null_id() -> None:
    """Should fail: null value for required 'id'."""
    p = valid_withdrawal().copy()
    p["id"] = None
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_binary_asset() -> None:
    """Should fail: binary data for 'asset'."""
    p = valid_withdrawal().copy()
    p["asset"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_nested_amount() -> None:
    """Should fail: nested object for 'amount'."""
    p = valid_withdrawal().copy()
    p["amount"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_list_status() -> None:
    """Should fail: list for 'status'."""
    p = valid_withdrawal().copy()
    p["status"] = ["pending"]
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_garbled_unicode_asset() -> None:
    """Should fail: garbled unicode in 'asset'."""
    p = valid_withdrawal().copy()
    p["asset"] = "USDC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawDeposit_real_json_edge_case() -> None:
    """Validate BackpackRawDeposit using a real JSON payload with edge values."""
    payload = {
        "id": "dp_999999999999999999",
        "asset": "BTC_😀",
        "amount": "99999999.99999999",
        "status": "pending",
    }
    obj = BackpackRawDeposit.model_validate(payload)
    assert obj.asset == "BTC_😀"
    assert obj.amount == "99999999.99999999"
    assert obj.status == "pending"


def test_BackpackRawDeposit_corruption_null_id() -> None:
    """Should fail: null value for required 'id'."""
    p = valid_deposit().copy()
    p["id"] = None
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_binary_asset() -> None:
    """Should fail: binary data for 'asset'."""
    p = valid_deposit().copy()
    p["asset"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_nested_amount() -> None:
    """Should fail: nested object for 'amount'."""
    p = valid_deposit().copy()
    p["amount"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_list_status() -> None:
    """Should fail: list for 'status'."""
    p = valid_deposit().copy()
    p["status"] = ["completed"]
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_garbled_unicode_asset() -> None:
    """Should fail: garbled unicode in 'asset'."""
    p = valid_deposit().copy()
    p["asset"] = "BTC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawLiquidation_real_json_edge_case() -> None:
    """Validate BackpackRawLiquidation using a real JSON payload with edge values."""
    payload = {
        "symbol": "BTC_USDC_😀",
        "price": "0.00000001",
        "quantity": "99999999.99999999",
        "side": "buy",
    }
    obj = BackpackRawLiquidation.model_validate(payload)
    assert obj.symbol == "BTC_USDC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "99999999.99999999"
    assert obj.side == "buy"


def test_BackpackRawLiquidation_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_liquidation().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_binary_price() -> None:
    """Should fail: binary data for 'price'."""
    p = valid_liquidation().copy()
    p["price"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_nested_quantity() -> None:
    """Should fail: nested object for 'quantity'."""
    p = valid_liquidation().copy()
    p["quantity"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_list_side() -> None:
    """Should fail: list for 'side'."""
    p = valid_liquidation().copy()
    p["side"] = ["buy"]
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
