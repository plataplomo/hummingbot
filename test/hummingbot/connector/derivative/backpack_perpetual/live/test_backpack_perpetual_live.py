import os

import pytest

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative


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
async def test_perpetual_authentication_can_fetch_positions():
    api_key, api_secret = _get_live_credentials()
    exchange = BackpackPerpetualDerivative(
        backpack_perpetual_api_key=api_key,
        backpack_perpetual_api_secret=api_secret,
        trading_pairs=[],
        trading_required=False,
        domain=CONSTANTS.DEFAULT_DOMAIN,
    )

    collateral_response = await exchange._api_get(
        path_url=CONSTANTS.COLLATERAL_URL,
        is_auth_required=True,
        limit_id=CONSTANTS.COLLATERAL_URL,
    )

    collateral = _extract_collateral(collateral_response)
    assert collateral

    response = await exchange._api_get(
        path_url=CONSTANTS.POSITIONS_URL,
        is_auth_required=True,
        limit_id=CONSTANTS.POSITIONS_URL,
    )

    positions = web_utils.normalize_response_to_list(response)
    assert isinstance(positions, list)
