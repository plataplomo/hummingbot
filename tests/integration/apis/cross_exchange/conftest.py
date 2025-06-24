"""Cross-exchange test fixtures."""

from __future__ import annotations

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI


# Fixtures bp_api_for_test_env and hl_api_for_test_env are inherited
# from parent apis/conftest.py


@pytest.fixture(params=["backpack", "hyperliquid"])
def exchange_client(
    request: pytest.FixtureRequest,
    bp_api_for_test_env: BackpackAPI,
    hl_api_for_test_env: HyperliquidAPI,
) -> BackpackAPI | HyperliquidAPI:
    """Parametrized exchange client fixture."""
    if request.param == "backpack":
        return bp_api_for_test_env
    else:
        return hl_api_for_test_env


@pytest.fixture
def all_exchange_clients(
    bp_api_for_test_env: BackpackAPI, hl_api_for_test_env: HyperliquidAPI
) -> dict[str, BackpackAPI | HyperliquidAPI]:
    """All exchange clients for cross-exchange testing."""
    return {
        "backpack": bp_api_for_test_env,
        "hyperliquid": hl_api_for_test_env,
    }
