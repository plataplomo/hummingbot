"""Shared VCR configuration helpers for pytest integration tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from tests.common_symbols import (
    BTC_USDC_BP,
    BTC_USDC_PERP_BP,
    ETH_USDC_BP,
    ETH_USDC_PERP_BP,
    SOL_USDC_BP,
    SOL_USDC_PERP_BP,
)


if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


@pytest.fixture
def vcr_cassette_dir(request: FixtureRequest, custom_vcr_cassette_dir: str | None = None) -> str:
    """Dynamic VCR cassette directory based on test location.

    Returns:
        Directory path for VCR cassettes

    Raises:
        ValueError: If unable to determine test module or file path.
    """
    if custom_vcr_cassette_dir:
        return custom_vcr_cassette_dir

    # Access module from request object
    module: Any = getattr(request, "module", None)
    if module is None:
        raise ValueError("Unable to determine test module")

    module_file: str | None = getattr(module, "__file__", None)
    if module_file is None:
        raise ValueError("Unable to determine test module file path")

    test_file = Path(module_file)
    return str(test_file.parent.relative_to(Path("tests/integration/apis")))


@pytest.fixture
def spot_test_symbols() -> list[str]:
    """Common spot trading symbols for testing.

    Returns:
        List of spot trading symbols
    """
    return [SOL_USDC_BP.value, BTC_USDC_BP.value, ETH_USDC_BP.value]


@pytest.fixture
def perp_test_symbols() -> list[str]:
    """Common perp symbols for testing.

    Returns:
        List of perpetual futures symbols
    """
    return [SOL_USDC_PERP_BP.value, BTC_USDC_PERP_BP.value, ETH_USDC_PERP_BP.value]


@pytest.fixture
def precision_test_amounts() -> list[Decimal]:
    """Test amounts for precision validation.

    Returns:
        List of Decimal amounts for testing precision
    """
    return [
        Decimal("0.00000001"),  # Dust
        Decimal("0.1"),  # Small
        Decimal(100),  # Normal
        Decimal("999999.99"),  # Large
    ]
