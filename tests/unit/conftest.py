import pytest
import asyncio
from unittest.mock import MagicMock, patch
import aiohttp
from typing import Dict, Any

# Mock aiohttp ClientSession and Response for API testing
class MockResponse:
    def __init__(self, data, status=200, headers=None, content_type="application/json"):
        self._data = data
        self.status = status
        self.headers = headers or {}
        self.content_type = content_type
        self._raise_for_status_called = False

    async def json(self):
        return self._data

    async def text(self):
        return str(self._data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def raise_for_status(self):
        self._raise_for_status_called = True
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=self.status
            )

class MockClientSession:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.requests = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
        self.closed = True

    async def _request(self, method, url, **kwargs):
        self.requests.append({
            'method': method,
            'url': url,
            'kwargs': kwargs
        })
        
        # Find match in responses
        for pattern, response in self.responses.items():
            if (method, url) == pattern or (method, pattern[1]) == pattern and url.startswith(pattern[1]):
                return response
        
        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self._request("POST", url, **kwargs)

    async def put(self, url, **kwargs):
        return await self._request("PUT", url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self._request("DELETE", url, **kwargs)

@pytest.fixture
def mock_client_session():
    """Fixture to provide a mock aiohttp ClientSession."""
    def create_session(responses=None):
        return MockClientSession(responses)
    return create_session

@pytest.fixture
def hyperliquid_config():
    """Fixture to provide Hyperliquid API configuration."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {
                "POST:/user": {
                    "rate": 5.0,
                    "bucket": 20
                }
            }
        }
    }

@pytest.fixture
def backpack_config():
    """Fixture to provide Backpack API configuration."""
    return {
        "rest_endpoint": "https://api.backpack.exchange",
        "ws_endpoint": "wss://ws.backpack.exchange",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {
                "GET:/api/v1/depth": {
                    "rate": 5.0,
                    "bucket": 20
                }
            }
        }
    }

@pytest.fixture
def hyperliquid_secrets():
    """Fixture to provide Hyperliquid API secrets."""
    return {
        "HYPERLIQUID_WALLET_PRIVATE_KEY": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "HYPERLIQUID_WALLET_ADDRESS": "0xabcdef1234567890abcdef1234567890abcdef12"
    }

@pytest.fixture
def backpack_secrets():
    """Fixture to provide Backpack API secrets."""
    return {
        "BACKPACK_API_KEY": "backpack-api-key-123456",
        "BACKPACK_API_SECRET": "backpack-api-secret-123456"
    } 