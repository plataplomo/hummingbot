"""Cross-exchange test fixtures."""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI


@pytest.fixture(params=["backpack", "hyperliquid"])
def exchange_client(request: Any, bp_api_for_test_env: BackpackAPI, hl_api_for_test_env: HyperliquidAPI) -> Any:
    """Parametrized exchange client fixture."""
    if request.param == "backpack":
        return bp_api_for_test_env
    else:
        return hl_api_for_test_env


@pytest.fixture
def all_exchange_clients(bp_api_for_test_env: BackpackAPI, hl_api_for_test_env: HyperliquidAPI) -> dict[str, Any]:
    """All exchange clients for cross-exchange testing."""
    return {
        "backpack": bp_api_for_test_env,
        "hyperliquid": hl_api_for_test_env,
    }