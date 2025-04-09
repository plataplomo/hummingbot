import pytest
import asyncio
import time
import hmac
import hashlib
from unittest.mock import patch, MagicMock, AsyncMock
import json

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.base import APIError, APIErrorCode
from cyberdelta.core.models import OrderSide, OrderType, Position, Balance, FundingRate, OrderBook, Trade
from tests.unit.conftest import MockResponse

class TestBackpackAPI:
    """Test suite for BackpackAPI client."""

    @pytest.fixture
    def api_client(self, backpack_config, backpack_secrets):
        """Create a BackpackAPI client for testing."""
        return BackpackAPI(backpack_config, backpack_secrets)

    @pytest.fixture
    def mock_session(self, monkeypatch, mock_client_session):
        """Setup mock aiohttp session with predefined responses."""
        # Define mock responses for different API endpoints
        responses = {
            # Mock response for get_ticker
            ("GET", "https://api.backpack.exchange/api/v1/ticker/BTCUSDC"): MockResponse({
                "symbol": "BTCUSDC",
                "bidPrice": "42450.50",
                "askPrice": "42550.75",
                "lastPrice": "42500.25",
                "volume": "1200.5",
                "time": str(int(time.time() * 1000))
            }),
            
            # Mock response for get_order_book
            ("GET", "https://api.backpack.exchange/api/v1/depth"): MockResponse({
                "lastUpdateId": 123456789,
                "bids": [
                    ["42450.50", "0.5"],
                    ["42440.25", "1.2"],
                    ["42430.00", "2.5"]
                ],
                "asks": [
                    ["42550.75", "0.3"],
                    ["42560.50", "0.8"],
                    ["42570.25", "1.5"]
                ],
                "time": str(int(time.time() * 1000))
            }),
            
            # Mock response for get_recent_trades
            ("GET", "https://api.backpack.exchange/api/v1/trades"): MockResponse([
                {
                    "id": "12345",
                    "price": "42500.25",
                    "qty": "0.05",
                    "quoteQty": "2125.01",
                    "time": str(int(time.time() * 1000)),
                    "isBuyerMaker": True
                },
                {
                    "id": "12346",
                    "price": "42505.50",
                    "qty": "0.03",
                    "quoteQty": "1275.16",
                    "time": str(int(time.time() * 1000) - 5000),
                    "isBuyerMaker": False
                }
            ]),
            
            # Mock response for get_funding_rate
            ("GET", "https://api.backpack.exchange/api/v1/fundingInfo"): MockResponse([
                {
                    "symbol": "BTCUSDC",
                    "fundingRate": "0.0001",
                    "fundingTime": str(int(time.time() * 1000) + 3600000)
                },
                {
                    "symbol": "ETHUSDC",
                    "fundingRate": "0.0002",
                    "fundingTime": str(int(time.time() * 1000) + 3600000)
                }
            ]),
            
            # Mock response for get_balances
            ("GET", "https://api.backpack.exchange/api/v1/capital"): MockResponse({
                "balances": [
                    {
                        "asset": "BTC",
                        "free": "0.5",
                        "locked": "0.1",
                        "total": "0.6"
                    },
                    {
                        "asset": "USDC",
                        "free": "10000.50",
                        "locked": "500.25",
                        "total": "10500.75"
                    }
                ]
            }),
            
            # Mock response for get_positions
            ("GET", "https://api.backpack.exchange/api/v1/positions"): MockResponse([
                {
                    "symbol": "BTCUSDC",
                    "positionAmt": "0.5",
                    "entryPrice": "40000.0",
                    "markPrice": "42000.0",
                    "unRealizedProfit": "1000.0",
                    "liquidationPrice": "35000.0",
                    "leverage": "10",
                    "marginType": "cross"
                },
                {
                    "symbol": "ETHUSDC",
                    "positionAmt": "-2.0",
                    "entryPrice": "2500.0",
                    "markPrice": "2450.0",
                    "unRealizedProfit": "100.0",
                    "liquidationPrice": "3000.0",
                    "leverage": "5",
                    "marginType": "cross"
                }
            ]),
            
            # Mock response for place_order
            ("POST", "https://api.backpack.exchange/api/v1/order"): MockResponse({
                "orderId": "123456789",
                "symbol": "BTCUSDC",
                "clientOrderId": "test-order-123",
                "transactTime": str(int(time.time() * 1000)),
                "price": "42000.0",
                "origQty": "0.1",
                "executedQty": "0.0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
            }),
            
            # Mock response for cancel_order
            ("DELETE", "https://api.backpack.exchange/api/v1/order"): MockResponse({
                "orderId": "123456789",
                "symbol": "BTCUSDC",
                "status": "CANCELED",
                "clientOrderId": "test-order-123",
                "price": "42000.0",
                "origQty": "0.1",
                "executedQty": "0.0",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
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
    async def test_get_ticker(self, api_client, mock_session):
        """Test get_ticker returns proper Ticker object."""
        # Mock the _request method to return the ticker response
        api_client._request = AsyncMock(return_value={
            "symbol": "BTCUSDC",
            "bidPrice": "42450.50",
            "askPrice": "42550.75",
            "lastPrice": "42500.25",
            "volume": "1200.5",
            "time": str(int(time.time() * 1000))
        })
        
        # Get ticker
        ticker = await api_client.get_ticker("BTCUSDC")
        
        # Verify expected data
        assert ticker.symbol == "BTCUSDC"
        assert ticker.bid == 42450.50
        assert ticker.ask == 42550.75
        assert ticker.price == 42500.25  # Using lastPrice
        assert ticker.volume == 1200.5
        assert ticker.timestamp > 0
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", f"/api/v1/ticker/BTCUSDC"
        )

    @pytest.mark.asyncio
    async def test_get_order_book(self, api_client, mock_session):
        """Test get_order_book returns proper OrderBook object."""
        # Mock the _request method to return the order book response
        mock_time = int(time.time() * 1000)
        api_client._request = AsyncMock(return_value={
            "lastUpdateId": 123456789,
            "bids": [
                ["42450.50", "0.5"],
                ["42440.25", "1.2"],
                ["42430.00", "2.5"]
            ],
            "asks": [
                ["42550.75", "0.3"],
                ["42560.50", "0.8"],
                ["42570.25", "1.5"]
            ],
            "time": str(mock_time)
        })
        
        # Get order book with depth=5
        order_book = await api_client.get_order_book("BTCUSDC", depth=5)
        
        # Verify expected data
        assert isinstance(order_book, OrderBook)
        assert order_book.symbol == "BTCUSDC"
        assert len(order_book.bids) == 3
        assert len(order_book.asks) == 3
        
        # Verify first bid
        assert order_book.bids[0][0] == 42450.50  # price
        assert order_book.bids[0][1] == 0.5  # quantity
        
        # Verify first ask
        assert order_book.asks[0][0] == 42550.75  # price
        assert order_book.asks[0][1] == 0.3  # quantity
        
        # Verify timestamp
        assert order_book.timestamp == mock_time
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", "/api/v1/depth", params={"symbol": "BTCUSDC", "limit": 5}
        )

    @pytest.mark.asyncio
    async def test_get_recent_trades(self, api_client, mock_session):
        """Test get_recent_trades returns proper Trade objects."""
        # Mock the _request method to return the trades response
        mock_time1 = int(time.time() * 1000)
        mock_time2 = mock_time1 - 5000
        api_client._request = AsyncMock(return_value=[
            {
                "id": "12345",
                "price": "42500.25",
                "qty": "0.05",
                "quoteQty": "2125.01",
                "time": str(mock_time1),
                "isBuyerMaker": True
            },
            {
                "id": "12346",
                "price": "42505.50",
                "qty": "0.03",
                "quoteQty": "1275.16",
                "time": str(mock_time2),
                "isBuyerMaker": False
            }
        ])
        
        # Get recent trades with limit=10
        trades = await api_client.get_recent_trades("BTCUSDC", limit=10)
        
        # Verify expected data
        assert len(trades) == 2
        
        # Verify first trade
        assert trades[0].symbol == "BTCUSDC"
        assert trades[0].id == "12345"
        assert trades[0].price == 42500.25
        assert trades[0].quantity == 0.05
        assert trades[0].time == mock_time1
        assert trades[0].side == OrderSide.BUY
        
        # Verify second trade
        assert trades[1].symbol == "BTCUSDC"
        assert trades[1].id == "12346"
        assert trades[1].price == 42505.50
        assert trades[1].quantity == 0.03
        assert trades[1].time == mock_time2
        assert trades[1].side == OrderSide.SELL
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", "/api/v1/trades", params={"symbol": "BTCUSDC", "limit": 10}
        )

    @pytest.mark.asyncio
    async def test_get_funding_rate(self, api_client, mock_session):
        """Test get_funding_rate returns proper FundingRate object."""
        # Mock the _request method to return the funding rate response
        mock_time = int(time.time() * 1000) + 3600000
        api_client._request = AsyncMock(return_value=[
            {
                "symbol": "BTCUSDC",
                "fundingRate": "0.0001",
                "fundingTime": str(mock_time)
            },
            {
                "symbol": "ETHUSDC",
                "fundingRate": "0.0002",
                "fundingTime": str(mock_time)
            }
        ])
        
        # Get funding rate for BTC
        funding_rate = await api_client.get_funding_rate("BTCUSDC")
        
        # Verify expected data
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == "BTCUSDC"
        assert funding_rate.funding_rate == 0.0001
        assert funding_rate.next_funding_time == mock_time
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", "/api/v1/fundingInfo", params={"symbol": "BTCUSDC"}
        )

    @pytest.mark.asyncio
    async def test_get_balances(self, api_client, mock_session):
        """Test get_balances returns proper Balance objects."""
        # Mock the _request method to return the balance response
        api_client._request = AsyncMock(return_value={
            "balances": [
                {
                    "asset": "BTC",
                    "free": "0.5",
                    "locked": "0.1",
                    "total": "0.6"
                },
                {
                    "asset": "USDC",
                    "free": "10000.50",
                    "locked": "500.25",
                    "total": "10500.75"
                }
            ]
        })
        
        # Get balances
        balances = await api_client.get_balances()
        
        # Verify expected data for BTC
        assert "BTC" in balances
        assert isinstance(balances["BTC"], Balance)
        assert balances["BTC"].asset == "BTC"
        assert balances["BTC"].free == 0.5
        assert balances["BTC"].locked == 0.1
        assert balances["BTC"].total == 0.6
        
        # Verify expected data for USDC
        assert "USDC" in balances
        assert isinstance(balances["USDC"], Balance)
        assert balances["USDC"].asset == "USDC"
        assert balances["USDC"].free == 10000.50
        assert balances["USDC"].locked == 500.25
        assert balances["USDC"].total == 10500.75
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", "/api/v1/capital", signed=True
        )

    @pytest.mark.asyncio
    async def test_get_positions(self, api_client, mock_session):
        """Test get_positions returns proper Position objects."""
        # Mock the _request method to return the positions response
        api_client._request = AsyncMock(return_value=[
            {
                "symbol": "BTCUSDC",
                "positionAmt": "0.5",
                "entryPrice": "40000.0",
                "markPrice": "42000.0",
                "unRealizedProfit": "1000.0",
                "liquidationPrice": "35000.0",
                "leverage": "10",
                "marginType": "cross"
            },
            {
                "symbol": "ETHUSDC",
                "positionAmt": "-2.0",
                "entryPrice": "2500.0",
                "markPrice": "2450.0",
                "unRealizedProfit": "100.0",
                "liquidationPrice": "3000.0",
                "leverage": "5",
                "marginType": "cross"
            }
        ])
        
        # Get positions
        positions = await api_client.get_positions()
        
        # Verify expected data for BTC long position
        assert "BTCUSDC" in positions
        assert isinstance(positions["BTCUSDC"], Position)
        assert positions["BTCUSDC"].symbol == "BTCUSDC"
        assert positions["BTCUSDC"].size == 0.5
        assert positions["BTCUSDC"].entry_price == 40000.0
        assert positions["BTCUSDC"].mark_price == 42000.0
        assert positions["BTCUSDC"].liquidation_price == 35000.0
        assert positions["BTCUSDC"].leverage == 10.0
        assert positions["BTCUSDC"].side == OrderSide.BUY
        assert positions["BTCUSDC"].unrealized_pnl == 1000.0
        
        # Verify expected data for ETH short position
        assert "ETHUSDC" in positions
        assert isinstance(positions["ETHUSDC"], Position)
        assert positions["ETHUSDC"].symbol == "ETHUSDC"
        assert positions["ETHUSDC"].size == 2.0  # Absolute value
        assert positions["ETHUSDC"].entry_price == 2500.0
        assert positions["ETHUSDC"].mark_price == 2450.0
        assert positions["ETHUSDC"].liquidation_price == 3000.0
        assert positions["ETHUSDC"].leverage == 5.0
        assert positions["ETHUSDC"].side == OrderSide.SELL
        assert positions["ETHUSDC"].unrealized_pnl == 100.0
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "GET", "/api/v1/positions", signed=True
        )

    @pytest.mark.asyncio
    async def test_place_order(self, api_client, mock_session):
        """Test place_order functionality."""
        # Mock the _request method to return the order response
        api_client._request = AsyncMock(return_value={
            "orderId": "123456789",
            "symbol": "BTCUSDC",
            "clientOrderId": "test-order-123",
            "transactTime": str(int(time.time() * 1000)),
            "price": "42000.0",
            "origQty": "0.1",
            "executedQty": "0.0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        })
        
        # Place order
        order = await api_client.place_order(
            symbol="BTCUSDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=0.1,
            price=42000.0,
            client_order_id="test-order-123"
        )
        
        # Verify expected data
        assert order.id == "123456789"
        assert order.symbol == "BTCUSDC"
        assert order.side == OrderSide.BUY
        assert order.type == OrderType.LIMIT
        assert order.price == 42000.0
        assert order.quantity == 0.1
        assert order.filled_quantity == 0.0
        assert order.status == "NEW"
        assert order.client_order_id == "test-order-123"
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once()
        call_args = api_client._request.call_args[0]
        assert call_args[0] == "POST"
        assert call_args[1] == "/api/v1/order"
        assert api_client._request.call_args[1]["signed"] is True

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client, mock_session):
        """Test cancel_order functionality."""
        # Mock the _request method to return success
        api_client._request = AsyncMock(return_value={
            "orderId": "123456789",
            "symbol": "BTCUSDC",
            "status": "CANCELED",
            "clientOrderId": "test-order-123",
            "price": "42000.0",
            "origQty": "0.1",
            "executedQty": "0.0",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        })
        
        # Cancel order
        result = await api_client.cancel_order(order_id="123456789", symbol="BTCUSDC")
        
        # Verify result
        assert result is True
        
        # Verify _request was called with correct parameters
        api_client._request.assert_called_once_with(
            "DELETE", "/api/v1/order", params={"symbol": "BTCUSDC", "orderId": "123456789"}, signed=True
        )

    @pytest.mark.asyncio
    async def test_sign_request(self, api_client):
        """Test request signing functionality."""
        # Test GET request signing
        timestamp = int(time.time() * 1000)
        params = {"symbol": "BTCUSDC", "limit": 10}
        
        # Override time.time to return consistent timestamp for testing
        with patch('time.time', return_value=timestamp/1000):
            auth_data = api_client._sign_request("GET", "/api/v1/trades", params=params)
        
        # Verify headers
        assert "headers" in auth_data
        assert "X-API-Key" in auth_data["headers"]
        assert "X-Timestamp" in auth_data["headers"] 
        assert "X-Signature" in auth_data["headers"]
        assert auth_data["headers"]["X-API-Key"] == "backpack-api-key-123456"
        assert auth_data["headers"]["X-Timestamp"] == str(timestamp)
        
        # Verify signature generation logic
        expected_signature_payload = str(timestamp)
        if params:
            query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            expected_signature_payload += query_string
            
        expected_signature = hmac.new(
            "backpack-api-secret-123456".encode('utf-8'),
            expected_signature_payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        assert auth_data["headers"]["X-Signature"] == expected_signature
        
        # Test POST request signing
        data = {"symbol": "BTCUSDC", "side": "BUY", "type": "LIMIT", "quantity": 0.1, "price": 42000.0}
        
        # Override time.time to return consistent timestamp for testing
        with patch('time.time', return_value=timestamp/1000):
            auth_data = api_client._sign_request("POST", "/api/v1/order", data=data)
        
        # Verify headers again for POST
        assert "headers" in auth_data
        assert "X-API-Key" in auth_data["headers"]
        assert "X-Timestamp" in auth_data["headers"]
        assert "X-Signature" in auth_data["headers"] 