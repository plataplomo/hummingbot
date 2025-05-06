# tests/unit/apis/hyperliquid/models/test_hl_raw_fill.py imports

from typing import Any

import pytest

# ... rest of file ...


@pytest.mark.parametrize(
    "field, invalid_value, match_pattern",
    [
        # ... test cases ...
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
