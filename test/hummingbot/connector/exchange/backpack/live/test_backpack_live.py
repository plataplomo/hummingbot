import os

import pytest

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


def _get_live_credentials() -> tuple[str, str]:
    if os.getenv("BACKPACK_INTEGRATION_TESTS") != "1":
        pytest.skip("Backpack integration tests disabled (BACKPACK_INTEGRATION_TESTS != 1).")

    api_key = os.getenv("BACKPACK_API_KEY")
    api_secret = os.getenv("BACKPACK_API_SECRET")
    if not api_key or not api_secret:
        pytest.skip("Missing BACKPACK_API_KEY or BACKPACK_API_SECRET for live tests.")

    return api_key, api_secret


def _extract_collateral(response) -> list:
    if isinstance(response, dict) and isinstance(response.get("collateral"), list):
        return response["collateral"]
    if isinstance(response, list):
        return response
    return []


@pytest.mark.asyncio
async def test_spot_authentication_can_fetch_collateral():
    api_key, api_secret = _get_live_credentials()
    exchange = BackpackExchange(
        backpack_api_key=api_key,
        backpack_api_secret=api_secret,
        trading_pairs=[],
        trading_required=False,
        domain=CONSTANTS.DEFAULT_DOMAIN,
    )

    response = await exchange._api_get(
        path_url=CONSTANTS.COLLATERAL_URL,
        is_auth_required=True,
        limit_id=CONSTANTS.COLLATERAL_URL,
    )

    collateral = _extract_collateral(response)
    assert collateral
