import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import json
import time

from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.apis.base import APIError, APIErrorCode
from cyberdelta.core.models import OrderSide, OrderType, Position, Balance, FundingRate
from tests.unit.conftest import MockResponse

class TestHyperliquidAPI:
    """Test suite for HyperliquidAPI client."""

    @pytest.fixture
    def api_client(self, hyperliquid_config, hyperliquid_secrets):
        """Create a HyperliquidAPI client for testing."""
        return HyperliquidAPI(hyperliquid_config, hyperliquid_secrets)

    @pytest.fixture
    def mock_session(self, monkeypatch, mock_client_session):
        """Setup mock aiohttp session with predefined responses."""
        # Define mock responses for different API endpoints
        responses = {
            # Mock response for get_balances
            ("POST", "https://api.hyperliquid.xyz/user"): MockResponse({
                "data": {
                    "walletBalanceUsd": "1000.50"
                }
            }),
            
            # Mock response for get_positions
            ("POST", "https://api.hyperliquid.xyz/user"): MockResponse({
                "data": [
                    {
                        "coin": "BTC",
                        "szi": "0.5",  # Positive value for long
                        "entryPx": "40000.0",
                        "markPx": "42000.0",
                        "leverage": "10",
                        "unrealizedPnl": "1000.0"
                    },
                    {
                        "coin": "ETH",
                        "szi": "-2.0",  # Negative value for short
                        "entryPx": "2500.0",
                        "markPx": "2400.0",
                        "leverage": "5",
                        "unrealizedPnl": "200.0"
                    }
                ]
            }),

            # Mock response for get_funding_rate
            ("GET", "https://api.hyperliquid.xyz/info"): MockResponse({
                "fundingInfo": [
                    {
                        "asset": "BTC",
                        "funding": "0.0001",
                        "projectedFunding": "0.00012",
                        "nextFundingTime": str(int(time.time() * 1000) + 3600000)
                    }
                ]
            }),

            # Mock response for get_ticker
            ("GET", "https://api.hyperliquid.xyz/info"): MockResponse({
                "assetInfo": [
                    {
                        "name": "BTC",
                        "midPrice": "42500.0",
                        "bid": "42450.0",
                        "ask": "42550.0",
                        "last24hVolume": "5000.0"
                    }
                ]
            }),

            # Mock response for place_order
            ("POST", "https://api.hyperliquid.xyz/exchange"): MockResponse({
                "status": "ok",
                "data": {
                    "order": {
                        "id": "123456789",
                        "clientId": "test-order-123",
                        "time": str(int(time.time() * 1000)),
                        "coin": "BTC",
                        "side": "B",  # Buy
                        "limitPx": "42000.0",
                        "sz": "0.1",
                        "status": "OPEN"
                    }
                }
            }),
        }

        # Create session with predefined responses
        session = mock_client_session(responses)
        
        # Patch client session creation
        async def mock_session_factory(*args, **kwargs):
            return session
        
        # Apply the patch
        with patch('aiohttp.ClientSession', mock_session_factory):
            yield session

    @pytest.mark.asyncio
    async def test_get_balances(self, api_client, mock_session):
        """Test get_balances returns proper Balance objects."""
        # Mock the _request method to return the balance response
        api_client._request = AsyncMock(return_value={
            "data": {
                "walletBalanceUsd": "1000.50"
            }
        })
        
        # Get balances
        balances = await api_client.get_balances()
        
        # Verify expected data
        assert "USDC" in balances
        assert isinstance(balances["USDC"], Balance)
        assert balances["USDC"].asset == "USDC"
        assert balances["USDC"].free == 1000.50
        assert balances["USDC"].total == 1000.50
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "POST", "/user", data={"type": "clearinghouseState"}, signed=True
        )

    @pytest.mark.asyncio
    async def test_get_positions(self, api_client, mock_session):
        """Test get_positions returns proper Position objects."""
        # Mock the _request method to return the positions response
        api_client._request = AsyncMock(return_value={
            "data": [
                {
                    "coin": "BTC",
                    "szi": "0.5",  # Positive value for long
                    "entryPx": "40000.0",
                    "markPx": "42000.0",
                    "leverage": "10",
                    "unrealizedPnl": "1000.0"
                },
                {
                    "coin": "ETH",
                    "szi": "-2.0",  # Negative value for short
                    "entryPx": "2500.0",
                    "markPx": "2400.0",
                    "leverage": "5",
                    "unrealizedPnl": "200.0"
                }
            ]
        })
        
        # Get positions
        positions = await api_client.get_positions()
        
        # Verify expected data for BTC long position
        assert "BTC" in positions
        assert isinstance(positions["BTC"], Position)
        assert positions["BTC"].symbol == "BTC"
        assert positions["BTC"].size == 0.5
        assert positions["BTC"].entry_price == 40000.0
        assert positions["BTC"].mark_price == 42000.0
        assert positions["BTC"].leverage == 10.0
        assert positions["BTC"].side == OrderSide.BUY
        assert positions["BTC"].unrealized_pnl == 1000.0
        
        # Verify expected data for ETH short position
        assert "ETH" in positions
        assert isinstance(positions["ETH"], Position)
        assert positions["ETH"].symbol == "ETH"
        assert positions["ETH"].size == 2.0  # Absolute value
        assert positions["ETH"].entry_price == 2500.0
        assert positions["ETH"].mark_price == 2400.0
        assert positions["ETH"].leverage == 5.0
        assert positions["ETH"].side == OrderSide.SELL
        assert positions["ETH"].unrealized_pnl == 200.0
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "POST", "/user", data={"type": "positions"}, signed=True
        )

    @pytest.mark.asyncio
    async def test_get_funding_rate(self, api_client, mock_session):
        """Test get_funding_rate returns proper FundingRate object."""
        # Mock the _request method to return the funding rate response
        api_client._request = AsyncMock(return_value={
            "assetInfo": [
                {
                    "name": "BTC",
                    "fundingInfo": {
                        "instFunding": "0.0001",  # Current funding rate
                        "prevFunding": "0.00008",  # Previous funding rate
                        "nextFundingTime": str(int(time.time() * 1000) + 3600000)  # 1 hour later
                    }
                },
                {
                    "name": "ETH",
                    "fundingInfo": {
                        "instFunding": "0.0002",
                        "prevFunding": "0.00015",
                        "nextFundingTime": str(int(time.time() * 1000) + 3600000)
                    }
                }
            ]
        })
        
        # Get funding rate for BTC
        funding_rate = await api_client.get_funding_rate("BTC")
        
        # Verify expected data
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == "BTC"
        assert funding_rate.funding_rate == 0.0001
        assert funding_rate.predicted_rate == 0.0001  # Uses current rate as predicted
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with("GET", "/info")

    @pytest.mark.asyncio
    async def test_place_order(self, api_client, mock_session):
        """Test place_order functionality."""
        # Mock the _request method to return the order response
        api_client._request = AsyncMock(return_value={
            "status": "ok",
            "data": {
                "order": {
                    "id": "123456789",
                    "clientId": "test-order-123",
                    "time": str(int(time.time() * 1000)),
                    "coin": "BTC",
                    "side": "B",  # Buy
                    "limitPx": "42000.0",
                    "sz": "0.1",
                    "status": "OPEN"
                }
            }
        })
        
        # Place order
        order = await api_client.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=0.1,
            price=42000.0,
            client_order_id="test-order-123"
        )
        
        # Verify expected data
        assert order.id == "123456789"
        assert order.symbol == "BTC"
        assert order.side == OrderSide.BUY
        assert order.type == OrderType.LIMIT
        assert order.price == 42000.0
        assert order.quantity == 0.1
        assert order.client_order_id == "test-order-123"
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once()
        call_args = api_client._request.call_args[0]
        assert call_args[0] == "POST"
        assert call_args[1] == "/exchange"
        assert "data" in api_client._request.call_args[1]
        assert api_client._request.call_args[1]["signed"] is True

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client, mock_session):
        """Test cancel_order functionality."""
        # Mock the _request method to return success
        api_client._request = AsyncMock(return_value={
            "status": "ok",
            "data": {
                "statuses": ["CANCELED"]
            }
        })
        
        # Cancel order
        result = await api_client.cancel_order(order_id="123456789", symbol="BTC")
        
        # Verify result
        assert result is True
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once()
        call_args = api_client._request.call_args[0]
        assert call_args[0] == "POST"
        assert call_args[1] == "/exchange"
        assert api_client._request.call_args[1]["signed"] is True

    @pytest.mark.asyncio
    async def test_authentication(self, api_client):
        """Test authentication header generation."""
        # We can't fully test the signature without the actual private key
        # but we can verify the structure of the authentication data using test mode
        auth_data = await api_client._authenticate("POST", "/exchange/test", data={"type": "order", "test": True})
        
        # Verify expected structure
        assert "headers" in auth_data
        assert "X-HL-Signature" in auth_data["headers"]
        assert "X-HL-Timestamp" in auth_data["headers"]
        assert "X-HL-Nonce" in auth_data["headers"]
        
        # Verify data was passed through
        assert auth_data["data"]["type"] == "order"
        assert auth_data["data"]["test"] == True 