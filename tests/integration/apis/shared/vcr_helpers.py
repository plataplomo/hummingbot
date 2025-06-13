"""Shared VCR configuration helpers for pytest integration tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def vcr_cassette_dir(request: Any, custom_vcr_cassette_dir: Any = None) -> str:
    """Dynamic VCR cassette directory based on test location."""
    if custom_vcr_cassette_dir:
        return custom_vcr_cassette_dir
    
    test_file = Path(request.module.__file__)
    return str(test_file.parent.relative_to(Path("tests/integration/apis")))


@pytest.fixture 
def spot_test_symbols() -> list[str]:
    """Common spot trading symbols for testing."""
    return ["SOL_USDC", "BTC_USDC", "ETH_USDC"]


@pytest.fixture  
def perp_test_symbols() -> list[str]:
    """Common perp symbols for testing."""
    return ["SOL-PERP", "BTC-PERP", "ETH-PERP"]


@pytest.fixture
def precision_test_amounts() -> list[Any]:
    """Test amounts for precision validation."""
    from decimal import Decimal
    return [
        Decimal("0.00000001"),  # Dust
        Decimal("0.1"),         # Small
        Decimal("100"),         # Normal
        Decimal("999999.99")    # Large
    ]