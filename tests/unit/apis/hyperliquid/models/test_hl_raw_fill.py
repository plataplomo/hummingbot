# tests/unit/apis/hyperliquid/models/test_hl_raw_fill.py imports

from typing import Any

import pytest


# Placeholder valid data for the fixture
@pytest.fixture
def valid_fill_data() -> dict[str, Any]:
    """Provides a dictionary with minimal valid data for HyperliquidRawFill."""
    return {
        "coin": "BTC",
        "dir": "B",  # Buy
        "px": "30000.0",
        "sz": "0.1",
        "time": 1678886400123,  # ms timestamp
        "hash": "0xabcdef123",
        "startPosition": "0.0",
        "liquidationMarkPx": None,  # Optional
        "side": "B",  # Added based on Raw model
        "tid": 12345,  # Added based on Raw model
        "oid": 67890,  # Added based on Raw model
    }


@pytest.mark.parametrize(
    "field, invalid_value, match_pattern",
    [
        # TODO: Add comprehensive test cases for HyperliquidRawFill validation
        ("qty", None, "Field required"),  # Placeholder: test missing required field
    ],
)
def test_invalid_hyperliquid_fill(
    field: str,
    invalid_value: Any,  # noqa: ANN401 # Intentional Any for testing invalid inputs
    match_pattern: str,
    valid_fill_data: dict[str, Any],
) -> None:
    """Test that HyperliquidRawFill raises ValidationError for invalid inputs."""
    # ... test body ...
