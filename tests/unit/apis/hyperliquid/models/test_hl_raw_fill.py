# tests/unit/apis/hyperliquid/models/test_hl_raw_fill.py imports

from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill


# Placeholder valid data for the fixture
@pytest.fixture
def valid_fill_data() -> dict[str, Any]:
    """Provides a dictionary with minimal valid data for HyperliquidRawFill."""
    return {
        "tid": 12345,
        "oid": 67890,
        "coin": "BTC-PERP",
        "px": "30000.0",
        "sz": "0.1",
        "startPosition": "0.0",
        "fee": "0.01",
        "liquidationMarkPx": None,
        "time": 1678886400123,  # ms timestamp
        "side": "B",
        "dir": "Open Long",
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef123456cdef",
        "isMaker": True,
        "cloid": "my_client_order_id_123",
    }


def test_valid_hyperliquid_fill(valid_fill_data: dict[str, Any]) -> None:
    """Test that HyperliquidRawFill can parse valid data successfully."""
    fill = HyperliquidRawFill.model_validate(valid_fill_data)
    assert fill.tid == 12345
    assert fill.oid == 67890
    assert fill.coin == "BTC-PERP"
    assert fill.px == "30000.0"
    assert fill.sz == "0.1"
    assert fill.start_position == "0.0"
    assert fill.fee == "0.01"
    assert fill.liquidation_mark_px is None
    assert fill.time == 1678886400123
    assert fill.side == "B"
    assert fill.dir == "Open Long"
    assert fill.hash == "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef123456cdef"
    assert fill.is_maker is True
    assert fill.cloid == "my_client_order_id_123"
    assert fill.model_config.get("extra") == "forbid"
    assert fill.model_config.get("frozen") is True
    assert fill.model_config.get("populate_by_name") is True


# @pytest.mark.parametrize(
#     "field, invalid_value, match_pattern",
#     [
#         # TODO: Add comprehensive test cases for HyperliquidRawFill validation
#         ("qty", None, "Field required"),  # Placeholder: test missing required field
#     ],
# )
# def test_invalid_hyperliquid_fill(
#     field: str,
#     invalid_value: Any,   # Intentional Any for testing invalid inputs
#     match_pattern: str,
#     valid_fill_data: dict[str, Any],
# ) -> None:
#     """Test that HyperliquidRawFill raises ValidationError for invalid inputs.""""
#     # ... test body ...
