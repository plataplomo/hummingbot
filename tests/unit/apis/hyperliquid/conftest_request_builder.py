"""Shared fixtures for HyperliquidRequestBuilder unit tests."""

import pytest
from cyberdelta.core.symbols import symbols


@pytest.fixture
def valid_wallet_address() -> str:
    """Return a valid Ethereum wallet address for testing."""
    return "0xAbCDeF0123456789AbCDeF0123456789AbCDeF01"


@pytest.fixture
def symbol() -> str:
    """Return a standard symbol for testing."""
    return symbols.ETH.hyperliquid().value


@pytest.fixture
def asset_index() -> int:
    """Return a standard asset index for testing."""
    return 0
