import types
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import aiohttp
import pytest
from web3.auto import w3  # Import w3

from cyberdelta.utils.config import Config


# Mock aiohttp ClientSession and Response for API testing
class MockResponse:
    def __init__(
        self,
        data: dict[str, Any] | list[Any] | str,  # More specific than Any
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> None:
        self._data = data
        self.status = status
        self.headers = headers or {}
        self.content_type = content_type
        self._raise_for_status_called = False

    async def json(self) -> dict[str, Any] | list[Any] | str:  # Match data type hint
        return self._data

    async def text(self) -> str:
        return str(self._data)

    async def __aenter__(self) -> "MockResponse":
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        pass

    def raise_for_status(self) -> None:  # Add return type hint
        self._raise_for_status_called = True
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=self.status
            )


class MockClientSession:
    def __init__(self, responses: dict[tuple[str, str], MockResponse] | None = None) -> None:
        self.responses = responses or {}
        self.requests: list[dict[str, Any]] = []
        self.closed = False

    async def __aenter__(self) -> "MockClientSession":
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def _request(self, method: str, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})

        # Find match in responses
        for pattern, response in (self.responses or {}).items():
            if (
                (method, url) == pattern
                or (method, pattern[1]) == pattern
                and url.startswith(pattern[1])
            ):
                return response

        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        return await self._request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        return await self._request("DELETE", url, **kwargs)


@pytest.fixture
def mock_client_session() -> Callable[..., MockClientSession]:
    """Fixture to provide a mock aiohttp ClientSession."""

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        return MockClientSession(responses)

    return create_session


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
    """Fixture to provide Hyperliquid API configuration."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"POST:/user": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def backpack_config() -> dict[str, Any]:
    """Fixture to provide Backpack API configuration."""
    return {
        "rest_endpoint": "https://api.backpack.exchange",
        "ws_endpoint": "wss://ws.backpack.exchange",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"GET:/api/v1/depth": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def hyperliquid_secrets() -> dict[str, str]:
    """Fixture to provide Hyperliquid API secrets with a VALID derived address."""
    # Use a fixed dummy private key for reproducibility in tests
    dummy_private_key = "0x1111111111111111111111111111111111111111111111111111111111111111"
    try:
        account = w3.eth.account.from_key(dummy_private_key)
        derived_address = account.address
    except Exception as e:
        # Fallback if w3 or account generation fails unexpectedly
        print(f"Error generating Hyperliquid mock account: {e}")
        derived_address = "0xMockAddressCreationFailed"  # Provide a fallback

    return {
        "private_key": dummy_private_key,
        "wallet_address": derived_address,
    }


@pytest.fixture
def backpack_secrets() -> dict[str, str]:
    """Fixture to provide Backpack API secrets."""
    return {
        "BACKPACK_API_KEY": "backpack-api-key-123456",
        "BACKPACK_API_SECRET": "backpack-api-secret-123456",
    }


@pytest.fixture
def mock_config() -> Callable[..., Config]:
    """Fixture to create a Config object with the provided data dictionary."""

    def _create_config(config_data: dict[str, Any] | None = None) -> Config:
        if config_data is None:
            config_data = {
                "general": {"log_level": "INFO", "safe_mode": True},
                "exchanges": {
                    "hyperliquid": {
                        "enabled": True,
                        "api_base_url": "https://api.hyperliquid.xyz",
                        "ws_url": "wss://api.hyperliquid.xyz/ws",
                        "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                    },
                    "backpack": {
                        "enabled": True,
                        "api_base_url": "https://api.backpack.exchange",
                        "ws_url": "wss://ws.backpack.exchange",
                        "symbols": {"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
                    },
                },
                "risk": {"global": {"max_position_usd": 1000.0}},
                "execution": {
                    "max_slippage": 0.002,
                    "max_retries": 3,
                    "retry_delay_base": 1.0,
                    "settlement_delay": 2.0,
                    "compensation": {
                        "use_limit_orders": True,
                        "limit_price_offset_pct": 0.05,
                    },
                },
            }
        return Config(config_data)

    return _create_config
