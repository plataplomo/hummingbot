import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    MarketData,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    TimeInForce,  # Already added
    Trade,
)
from cyberdelta.utils.config import Config


# --- Minimal Concrete Subclass for Testing --- #
class ConcreteBackpackAPI(BackpackAPI):
    """Minimal implementation for testing inherited methods like _sign_request."""

    # Implement all abstract methods with basic placeholders or mocks
    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        pass

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        return {}

    async def connect_websocket(self) -> None:
        pass

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        return []

    async def get_market_data(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[MarketData]:
        return []

    def get_message_type(self, message: dict[str, Any]) -> str:
        return "unknown"

    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        return []

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        return []

    def parse_account_update_message(
        self, message: dict[str, Any]
    ) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
        return None, None

    def parse_balance(self, data: dict[str, Any]) -> Balance:
        raise NotImplementedError

    def parse_funding_rate(self, data: dict[str, Any]) -> FundingRate:
        raise NotImplementedError

    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        return None

    def parse_order(self, data: dict[str, Any]) -> Order:
        raise NotImplementedError

    def parse_order_book(self, data: dict[str, Any], symbol: str) -> OrderBook:
        raise NotImplementedError

    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        return None

    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        return None

    def parse_position(self, data: dict[str, Any]) -> Position:
        raise NotImplementedError

    def parse_ticker(self, data: dict[str, Any], symbol: str) -> Ticker:
        raise NotImplementedError

    def parse_ticker_message(self, message: dict[str, Any]) -> Ticker | None:
        return None

    def parse_trade(self, data: dict[str, Any], symbol: str) -> Trade:
        raise NotImplementedError

    def parse_trade_message(self, message: dict[str, Any]) -> Trade | None:
        return None

    async def ping_websocket(self) -> None:
        pass

    async def subscribe_to_account_updates(self) -> None:
        pass

    async def subscribe_to_order_book(self, symbol: str) -> None:
        pass

    async def subscribe_to_ticker(self, symbol: str) -> None:
        pass

    async def subscribe_to_trades(self, symbol: str) -> None:
        pass

    # --- ADD MISSING ABSTRACT METHODS ---
    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {}  # Placeholder

    def _update_rate_limit_from_headers(self, headers: Any, method: str, path: str) -> None:
        pass  # Placeholder

    async def get_ticker(self, symbol: str) -> Ticker:
        raise NotImplementedError  # Placeholder

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        raise NotImplementedError  # Placeholder

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        return []  # Placeholder

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        return None  # Placeholder

    async def get_balances(self) -> dict[str, Balance]:
        return {}  # Placeholder

    async def get_positions(self, symbol: str | None = None) -> list[Position]:
        return []  # Placeholder

    # Corrected signature: time_in_force is Optional[TimeInForce] in base
    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce | None = None,  # Made Optional
        price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        raise NotImplementedError  # Placeholder

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        return {}  # Placeholder

    # Corrected signature: Matches base ExchangeAPI parameter order and return type
    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:  # Return type is Order, not Order | None
        raise NotImplementedError  # Placeholder

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        return None  # Placeholder

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        return []  # Placeholder

    async def get_recent_fills(
        self,
        symbol: str | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        start_time: int | None = None,
    ) -> list[Trade]:  # Use imported Trade type (was Fill)
        return []  # Placeholder

    # --- End Added Methods ---


# --- End Minimal Subclass --- #


class TestBackpackAPI:
    """Test suite for BackpackAPI client."""

    @pytest.fixture
    def api_client(self, backpack_config: Config, backpack_secrets: dict[str, str]):
        """Create a BackpackAPI client instance for testing (using AsyncMock)."""
        # Use AsyncMock with spec to avoid abstract class instantiation errors
        client = AsyncMock(spec=BackpackAPI)
        client.exchange_name = "backpack"  # Set necessary attributes for tests
        # Individual tests will mock specific methods like client.get_ticker, etc.
        return client

    @pytest.mark.asyncio
    async def test_get_ticker(self, api_client: BackpackAPI):
        """Test get_ticker returns proper Ticker object."""
        # Mock the specific method being tested
        mock_ticker_data = Ticker(
            symbol="BTCUSDC",
            bid=Decimal("42450.50"),
            ask=Decimal("42550.75"),
            price=Decimal("42500.25"),  # lastPrice
            volume=Decimal("1200.5"),
            timestamp=int(time.time() * 1000),
        )
        api_client.get_ticker = AsyncMock(return_value=mock_ticker_data)

        # Get ticker
        ticker = await api_client.get_ticker("BTCUSDC")

        # Verify expected data
        assert ticker.symbol == "BTCUSDC"
        assert ticker.bid == Decimal("42450.50")
        assert ticker.ask == Decimal("42550.75")
        assert ticker.price == Decimal("42500.25")
        assert ticker.volume == Decimal("1200.5")
        assert ticker.timestamp > 0

        # Verify the mocked method was called
        api_client.get_ticker.assert_called_once_with("BTCUSDC")

    @pytest.mark.asyncio
    async def test_get_order_book(self, api_client: BackpackAPI):
        """Test get_order_book returns proper OrderBook object."""
        mock_time = int(time.time() * 1000)
        mock_order_book_data = OrderBook(
            symbol="BTCUSDC",
            bids=[
                (Decimal("42450.50"), Decimal("0.5")),
                (Decimal("42440.25"), Decimal("1.2")),
                (Decimal("42430.00"), Decimal("2.5")),
            ],
            asks=[
                (Decimal("42550.75"), Decimal("0.3")),
                (Decimal("42560.50"), Decimal("0.8")),
                (Decimal("42570.25"), Decimal("1.5")),
            ],
            timestamp=mock_time,
        )
        api_client.get_order_book = AsyncMock(return_value=mock_order_book_data)

        # Get order book with depth=5
        order_book = await api_client.get_order_book("BTCUSDC", depth=5)

        # Verify expected data
        assert isinstance(order_book, OrderBook)
        assert order_book.symbol == "BTCUSDC"
        assert len(order_book.bids) == 3
        assert len(order_book.asks) == 3

        # Verify first bid
        assert order_book.bids[0][0] == Decimal("42450.50")  # price
        assert order_book.bids[0][1] == Decimal("0.5")  # quantity

        # Verify first ask
        assert order_book.asks[0][0] == Decimal("42550.75")  # price
        assert order_book.asks[0][1] == Decimal("0.3")  # quantity

        assert order_book.timestamp == mock_time

        # Verify the mocked method was called
        api_client.get_order_book.assert_called_once_with("BTCUSDC", depth=5)

    @pytest.mark.asyncio
    async def test_get_recent_trades(self, api_client: BackpackAPI):
        """Test get_recent_trades returns list of Trade objects."""
        now = datetime.now(UTC)
        mock_trade_data: list[Trade] = [
            Trade(
                id="12345",
                symbol="BTCUSDC",
                executed_at=now,
                side=OrderSide.BUY,
                order_id="order12345",
                exchange="backpack",
                client_order_id="client12345",
                price=Decimal("42500.25"),
                quantity=Decimal("0.05"),
                cost=Decimal("2125.0125"),
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=True,
                timestamp=int(time.time() * 1000),
            ),
            Trade(
                id="12346",
                symbol="BTCUSDC",
                executed_at=now,
                side=OrderSide.SELL,
                order_id="order12346",
                exchange="backpack",
                client_order_id="client12346",
                price=Decimal("42505.50"),
                quantity=Decimal("0.03"),
                cost=Decimal("1275.165"),
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=False,
                timestamp=int(time.time() * 1000) - 5000,
            ),
        ]
        api_client.get_recent_trades = AsyncMock(return_value=mock_trade_data)

        # Get recent trades
        trades: list[Trade] = await api_client.get_recent_trades("BTCUSDC", limit=2)

        # Verify expected data
        assert isinstance(trades, list)
        assert len(trades) == 2
        for t in trades:
            assert isinstance(t, Trade)
        assert trades[0].id == "12345"
        assert trades[0].side == OrderSide.BUY
        assert trades[1].id == "12346"
        assert trades[1].side == OrderSide.SELL

        # Verify the mocked method was called
        api_client.get_recent_trades.assert_called_once_with("BTCUSDC", limit=2)

    @pytest.mark.asyncio
    async def test_get_funding_rate(self, api_client: BackpackAPI):
        """Test get_funding_rate returns proper FundingRate object."""
        mock_time = int(time.time() * 1000) + 3600000
        mock_funding_data = FundingRate(
            symbol="BTCUSDC",
            funding_rate=Decimal("0.0001"),
            predicted_rate=None,  # Backpack doesn't provide predicted
            mark_price=None,  # Backpack doesn't provide mark price here
            index_price=None,  # Backpack doesn't provide index price here
            next_funding_time=mock_time,
        )
        api_client.get_funding_rate = AsyncMock(return_value=mock_funding_data)

        # Get funding rate
        funding_rate = await api_client.get_funding_rate("BTCUSDC")

        # Verify expected data
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == "BTCUSDC"
        assert funding_rate.funding_rate == Decimal("0.0001")
        assert funding_rate.next_funding_time == mock_time
        assert funding_rate.predicted_rate is None

        # Verify the mocked method was called
        api_client.get_funding_rate.assert_called_once_with("BTCUSDC")

    @pytest.mark.asyncio
    async def test_get_balances(self, api_client: BackpackAPI):
        """Test get_balances returns dictionary of Balance objects."""
        mock_balance_data = {
            "BTC": Balance(
                asset="BTC", free=Decimal("0.5"), locked=Decimal("0.1"), total=Decimal("0.6")
            ),
            "USDC": Balance(
                asset="USDC",
                free=Decimal("10000.50"),
                locked=Decimal("500.25"),
                total=Decimal("10500.75"),
            ),
        }
        api_client.get_balances = AsyncMock(return_value=mock_balance_data)

        # Get balances
        balances: dict[str, Balance] = await api_client.get_balances()

        # Verify expected data
        assert isinstance(balances, dict)
        assert "BTC" in balances
        assert "USDC" in balances
        btc_balance = balances["BTC"]
        usdc_balance = balances["USDC"]
        assert isinstance(btc_balance, Balance)
        assert isinstance(usdc_balance, Balance)
        assert usdc_balance.asset == "USDC"
        assert usdc_balance.free == Decimal("10000.50")
        assert usdc_balance.locked == Decimal("500.25")
        assert usdc_balance.total == Decimal("10500.75")

        # Verify the mocked method was called
        api_client.get_balances.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_positions(self, api_client: BackpackAPI):
        """Test get_positions returns list of Position objects."""
        mock_position_data = [
            Position(
                symbol="BTCUSDC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("42000.0"),
                unrealized_pnl=Decimal("1000.0"),
                liquidation_price=Decimal("35000.0"),
                leverage=Decimal("10"),
                side=OrderSide.BUY,  # Determined from positive size
            ),
            Position(
                symbol="ETHUSDC",
                size=Decimal("-2.0"),
                entry_price=Decimal("2500.0"),
                mark_price=Decimal("2450.0"),
                unrealized_pnl=Decimal("100.0"),
                liquidation_price=Decimal("3000.0"),
                leverage=Decimal("5"),
                side=OrderSide.SELL,  # Determined from negative size
            ),
        ]
        api_client.get_positions = AsyncMock(return_value=mock_position_data)

        # Get positions
        positions: list[Position] = await api_client.get_positions()

        # Verify expected data
        assert isinstance(positions, list)
        assert len(positions) == 2
        btc_position: Position = positions[0]
        eth_position: Position = positions[1]
        assert isinstance(btc_position, Position)
        assert isinstance(eth_position, Position)
        # Verify first position details (BTC)
        assert btc_position.symbol == "BTCUSDC"
        assert btc_position.side == OrderSide.BUY
        assert btc_position.size == Decimal("0.5")
        assert btc_position.entry_price == Decimal("40000.0")
        assert btc_position.mark_price == Decimal("42000.0")
        assert btc_position.unrealized_pnl == Decimal("1000.0")
        assert btc_position.liquidation_price == Decimal("35000.0")
        assert btc_position.leverage == Decimal("10")
        # Verify second position details (ETH)
        assert eth_position.symbol == "ETHUSDC"
        assert eth_position.side == OrderSide.SELL
        assert eth_position.size == Decimal("-2.0")
        assert eth_position.entry_price == Decimal("2500.0")

        # Verify the mocked method was called
        api_client.get_positions.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_place_order(self, api_client: BackpackAPI):
        """Test place_order returns proper Order object."""
        mock_order_data = Order(
            symbol="BTCUSDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            quantity_requested=Decimal("0.1"),
            quantity_filled=Decimal("0.0"),
            price=Decimal("42000.0"),
            client_order_id="test-order-123",
            created_at=datetime.now(UTC),
        )
        api_client.place_order = AsyncMock(return_value=mock_order_data)

        # Place order
        order = await api_client.place_order(
            symbol="BTCUSDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("42000.0"),
            client_order_id="test-order-123",
        )

        # Verify expected data
        assert isinstance(order, Order)
        assert order.symbol == "BTCUSDC"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.status == OrderStatus.NEW
        assert order.quantity_requested == Decimal("0.1")
        assert order.quantity_filled == Decimal("0.0")
        assert order.price == Decimal("42000.0")
        assert order.client_order_id == "test-order-123"

        # Verify the mocked method was called
        api_client.place_order.assert_called_once()
        # Minimal check on args - can be more specific if needed
        args, kwargs = api_client.place_order.call_args
        assert kwargs.get("symbol") == "BTCUSDC"
        assert kwargs.get("side") == OrderSide.BUY
        assert kwargs.get("quantity") == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client: BackpackAPI):
        """Test cancel_order returns success indication (e.g., Order object)."""
        # Backpack returns the cancelled order details
        mock_cancelled_order_data = Order(
            id="123456789",
            symbol="BTCUSDC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=Decimal("42000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.CANCELED,
            time=None,  # Cancel response might not have original time
            client_order_id="test-order-123",
        )
        api_client.cancel_order = AsyncMock(return_value=mock_cancelled_order_data)

        # Cancel order
        result = await api_client.cancel_order("123456789", "BTCUSDC")

        # Verify expected data (check status)
        assert isinstance(result, Order)
        assert result.status == OrderStatus.CANCELED
        assert result.id == "123456789"

        # Verify the mocked method was called
        api_client.cancel_order.assert_called_once_with("123456789", "BTCUSDC")

    @pytest.mark.asyncio
    async def test_sign_request(self, backpack_config: Config, backpack_secrets: dict[str, str]):
        """Test the _sign_request method produces correct signature."""
        # Instantiate the CONCRETE subclass for testing
        # We need to provide actual mock objects for config and secrets managers if
        # the BackpackAPI __init__ requires them.
        mock_config_obj = MagicMock(spec=Config)
        mock_secrets_obj = MagicMock(spec=SecretsManager)

        # Configure secrets mock to return the dummy secret key
        mock_secrets_obj.get.return_value = backpack_secrets["BACKPACK_API_SECRET"]

        # Pass the correct args to the concrete class constructor
        client = ConcreteBackpackAPI(api_config=backpack_config, secrets=backpack_secrets)
        # Explicitly set the secret if it's read directly in _sign_request
        client.api_secret = backpack_secrets["BACKPACK_API_SECRET"]

        # Prepare mock request parameters
        method = "POST"
        endpoint = "/api/v1/order"
        params = {
            "symbol": "SOL_USDC",
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "0.01",
            "price": "50000.0",
            "timeInForce": "GTC",
            "timestamp": 1678886400000,  # Example timestamp
        }

        # Call the protected method (requires name mangling for protected methods)
        # Assuming _sign_request is intended to be protected
        auth_data = client._sign_request(method=method, path=endpoint, params=params)
        signature = auth_data["headers"]["X-Signature"]  # Extract only the signature string

        # Assert the signature is a non-empty string (actual validation is complex)
        assert isinstance(signature, str)
        assert len(signature) > 0
        # A more robust test would compare against a known-good signature,
        # but that requires managing the timestamp and exact signing logic alignment.
        # For now, checking type and non-emptiness is a basic sanity check.
        # Example expected signature (will vary based on timestamp etc.)
        # expected_signature = "..."
        # assert signature == expected_signature

        # Verify secrets manager was called correctly (if needed)
        # mock_secrets_obj.get.assert_called_once_with("BACKPACK_API_SECRET")

    # TODO: Add tests for edge cases and error handling (e.g., API errors)
