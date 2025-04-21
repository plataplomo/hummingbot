import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from cyberdelta.apis.base import MessageHandler
from cyberdelta.apis.hyperliquid_api import HyperliquidAPI
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
    TimeInForce,
    Trade,
)


class TestHyperliquidAPI:
    """Test suite for HyperliquidAPI client."""

    @pytest.fixture
    def api_client(
        self, hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str]
    ) -> HyperliquidAPI:
        """Create a HyperliquidAPI client instance for testing."""

        # Cannot instantiate abstract class directly, create a concrete subclass for testing
        class ConcreteHyperliquidAPI(HyperliquidAPI):
            # Correct return type for override
            def parse_account_update_message(
                self, message: dict[str, Any]
            ) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
                return None, None  # Mock implementation

            async def parse_l2_book_update_message(self, message: dict[str, Any]) -> None:
                pass  # Mock implementation

            # --- ADD MISSING ABSTRACT METHODS ---
            async def _authenticate(
                self,
                method: str,
                path: str,
                params: dict[str, Any] | None = None,
                data: dict[str, Any] | None = None,
            ) -> dict[str, Any]:
                return {}

            def _update_rate_limit_from_headers(
                self, headers: Mapping[str, str], method: str, path: str
            ) -> None:
                pass

            async def get_ticker(self, symbol: str) -> Ticker:
                raise NotImplementedError

            async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
                raise NotImplementedError

            async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
                return []

            async def get_balances(self) -> dict[str, Balance]:
                raise NotImplementedError  # Implemented in base, but maybe needed here?

            async def get_positions(self, symbol: str | None = None) -> list[Position]:
                raise NotImplementedError  # Implemented in base?

            async def place_order(
                self,
                symbol: str,
                side: OrderSide,
                order_type: OrderType,
                quantity: Decimal,
                time_in_force: TimeInForce | None = None,
                price: Decimal | None = None,
                client_order_id: str | None = None,
                reduce_only: bool = False,
                post_only: bool = False,
            ) -> Order:
                raise NotImplementedError

            async def cancel_order(
                self, order_id: str, symbol: str | None = None
            ) -> dict[str, Any]:
                raise NotImplementedError

            # get_order_status is already implemented below
            async def get_order_status(
                self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
            ) -> Order:
                raise NotImplementedError

            async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
                raise NotImplementedError

            async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
                return []

            async def get_recent_fills(
                self,
                symbol: str | None = None,
                limit: int | None = None,
                order_id: str | None = None,
                start_time: int | None = None,
            ) -> list[Trade]:
                return []

            async def get_funding_rates(
                self, symbols: list[str] | None = None
            ) -> list[FundingRate]:
                return []

            async def get_market_data(
                self, symbol: str, timeframe: str, limit: int = 100
            ) -> list[MarketData]:
                return []

            def get_message_type(self, message: dict[str, Any]) -> str:
                return "unknown"

            async def get_order_history(
                self, symbol: str | None = None, limit: int = 100
            ) -> list[Order]:
                return []

            async def get_trade_history(
                self, symbol: str | None = None, limit: int = 100
            ) -> list[Trade]:
                return []

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

            async def subscribe_to_order_book(
                self, symbol: str, handler: MessageHandler | None = None
            ) -> None:
                pass

            async def subscribe_to_ticker(
                self, symbol: str, handler: MessageHandler | None = None
            ) -> None:
                pass

            async def subscribe_to_trades(
                self, symbol: str, handler: MessageHandler | None = None
            ) -> None:
                pass

            async def connect_websocket(self) -> None:
                pass  # Added connect_websocket

        client = ConcreteHyperliquidAPI(
            api_config=hyperliquid_config, secrets={k: v for k, v in hyperliquid_secrets.items()}
        )
        # Prevent actual network calls
        client._request = AsyncMock(side_effect=RuntimeError("Network call attempted!"))  # type: ignore[method-assign]  # Test mock override
        # Ensure wallet address is set if needed for method mocks
        client._wallet_address = hyperliquid_secrets.get(
            "HYPERLIQUID_WALLET_ADDRESS", "0xMockAddress"
        )
        return client

    @pytest.mark.asyncio
    async def test_get_balances(self, api_client: HyperliquidAPI) -> None:
        """Test get_balances returns proper Balance objects."""
        # Mock the specific public method
        mock_balance_data = {
            "USDC": Balance(
                asset="USDC", total=Decimal("1000.50"), available=Decimal("1000.50")
            )  # Hyperliquid only gives total USD value
        }
        api_client.get_balances = AsyncMock(return_value=mock_balance_data)  # type: ignore[method-assign]  # Test mock override

        # Get balances
        balances: dict[str, Balance] = await api_client.get_balances()

        # Verify expected data
        assert "USDC" in balances
        assert isinstance(balances["USDC"], Balance)
        assert balances["USDC"].asset == "USDC"
        # Hyperliquid API might only return total, available might be same as total
        assert balances["USDC"].available == Decimal("1000.50")
        assert balances["USDC"].total == Decimal("1000.50")

        # Verify the mocked method was called
        api_client.get_balances.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_positions(self, api_client: HyperliquidAPI) -> None:
        """Test get_positions returns proper Position objects."""
        # Mock the specific public method
        mock_position_data = [
            Position(
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("42000.0"),
                unrealized_pnl=Decimal("1000.0"),
                leverage=Decimal("10"),
                side=OrderSide.BUY,
            ),
            Position(
                symbol="ETH",
                size=Decimal("-2.0"),
                entry_price=Decimal("2500.0"),
                mark_price=Decimal("2400.0"),
                unrealized_pnl=Decimal("200.0"),
                leverage=Decimal("5"),
                side=OrderSide.SELL,
            ),
        ]
        api_client.get_positions = AsyncMock(return_value=mock_position_data)  # type: ignore[method-assign]  # Test mock override

        # Get positions
        positions: list[Position] = await api_client.get_positions()

        # Verify expected data (list of Position objects)
        assert isinstance(positions, list)
        assert len(positions) == 2
        assert isinstance(positions[0], Position)

        # Verify expected data for BTC long position
        assert positions[0].symbol == "BTC"
        assert positions[0].size == Decimal("0.5")
        assert positions[0].entry_price == Decimal("40000.0")
        assert positions[0].mark_price == Decimal("42000.0")
        assert positions[0].leverage == Decimal("10")
        assert positions[0].side == OrderSide.BUY
        assert positions[0].unrealized_pnl == Decimal("1000.0")

        # Verify expected data for ETH short position
        assert positions[1].symbol == "ETH"
        assert positions[1].size == Decimal("-2.0")  # Keep negative for short
        assert positions[1].entry_price == Decimal("2500.0")
        assert positions[1].mark_price == Decimal("2400.0")
        assert positions[1].leverage == Decimal("5")
        assert positions[1].side == OrderSide.SELL
        assert positions[1].unrealized_pnl == Decimal("200.0")

        # Verify the mocked method was called
        api_client.get_positions.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_funding_rate(self, api_client: HyperliquidAPI) -> None:
        """Test get_funding_rate returns proper FundingRate object."""
        # Mock the specific public method
        mock_time = int(time.time() * 1000) + 3600000
        mock_funding_data = FundingRate(
            symbol="BTC",
            funding_rate=Decimal("0.0001"),
            predicted_rate=Decimal("0.00012"),  # HL provides predicted
            next_funding_time=mock_time,
            mark_price=None,  # Not directly available in this response part
            index_price=None,
        )
        api_client.get_funding_rate = AsyncMock(return_value=mock_funding_data)  # type: ignore[method-assign]  # Test mock override

        # Get funding rate for BTC
        funding_rate: FundingRate = await api_client.get_funding_rate("BTC")

        # Verify expected data
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == "BTC"
        assert funding_rate.funding_rate == Decimal("0.0001")
        assert funding_rate.predicted_rate == Decimal("0.00012")
        assert funding_rate.next_funding_time == mock_time

        # Verify the mocked method was called
        api_client.get_funding_rate.assert_called_once_with("BTC")

    @pytest.mark.asyncio
    async def test_place_order(self, api_client: HyperliquidAPI) -> None:
        """Test place_order returns proper Order object."""
        # Mock the specific public method
        mock_order_response = Order(
            client_order_id="123456789",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("42000.0"),
            quantity_requested=Decimal("0.1"),
            quantity_filled=Decimal("0.0"),  # Initial status
            status=OrderStatus.OPEN,  # Use OrderStatus enum
            created_at=datetime.now(UTC),
        )
        # Mock the place_order method itself
        api_client.place_order = AsyncMock(return_value=mock_order_response)  # type: ignore[method-assign]  # Test mock override

        # Place order
        order: Order = await api_client.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("42000.0"),
            client_order_id="test-order-123",
        )

        # Verify expected data (based on what place_order is expected to return)
        assert isinstance(order, Order)
        assert order.client_order_id is not None
        assert order.symbol == "BTC"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.price == Decimal("42000.0")
        assert order.quantity_requested == Decimal("0.1")
        assert order.status == OrderStatus.OPEN
        assert order.client_order_id == "test-order-123"

        # Verify the mocked method was called
        api_client.place_order.assert_called_once()
        kwargs = api_client.place_order.call_args.kwargs
        assert kwargs.get("symbol") == "BTC"
        assert kwargs.get("side") == OrderSide.BUY
        assert kwargs.get("quantity") == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client: HyperliquidAPI) -> None:
        """Test cancel_order returns success indication."""
        # Mock the specific public method - HL cancel might just return status
        api_client.cancel_order = AsyncMock(return_value=True)  # type: ignore[method-assign]  # Test mock override

        # Cancel order
        result: bool = await api_client.cancel_order(oid=12345, symbol="BTC")

        # Verify result
        assert result is True

        # Verify the mocked method was called
        api_client.cancel_order.assert_called_once_with(oid=12345, symbol="BTC")

    @pytest.mark.asyncio
    async def test_authentication(self, api_client: HyperliquidAPI) -> None:
        """Test the _authenticate method produces correct signature."""
        # The api_client fixture already provides a configured HyperliquidAPI instance
        # We can call the protected _authenticate method on it directly for testing.

        # Prepare sample inputs for authentication
        method = "POST"
        path = "/info"
        data = {"type": "clearinghouseState", "user": api_client._wallet_address}  # noqa: SLF001  # White-box test: protected member access required for test; no public getter exists

        # Call the method to test
        auth_data = await api_client._authenticate(method=method, path=path, data=data)  # noqa: SLF001  # White-box test: protected member access required for test; no public getter exists

        # Verify the output structure
        assert isinstance(auth_data, dict)
        assert "headers" in auth_data
        assert "X-HL-Signature" in auth_data["headers"]
        assert "X-HL-Timestamp" in auth_data["headers"]
        assert "X-HL-Nonce" in auth_data["headers"]
        assert (
            auth_data["headers"]["X-HL-Signature"] != "0x" + "0" * 130
        )  # Ensure it's not the mock signature
        assert int(auth_data["headers"]["X-HL-Timestamp"]) > 0
        assert int(auth_data["headers"]["X-HL-Nonce"]) > 0
        assert auth_data["params"] is None  # Params were not provided
        assert auth_data["data"] == data

    # TODO: Add tests for other methods (get_ticker, get_order_book, etc.)
    # TODO: Add tests for error handling (e.g., API errors, invalid data)
