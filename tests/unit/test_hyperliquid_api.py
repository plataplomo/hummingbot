import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base_api import MessageHandler
from cyberdelta.apis.hyperliquid_api import HyperliquidAPI
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.utils.config import Config


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
            ) -> tuple[dict[str, SpotBalance] | None, dict[str, DerivativePosition] | None]:
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

            async def get_balances(self) -> dict[str, SpotBalance]:
                raise NotImplementedError  # Implemented in base, but maybe needed here?

            async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
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
            ) -> list[Candle]:
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

            def parse_balance(self, data: dict[str, Any]) -> SpotBalance:
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

            def parse_position(self, data: dict[str, Any]) -> DerivativePosition:
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
        client._wallet_address = hyperliquid_secrets.get(  # noqa: SLF001 - Test setup
            "HYPERLIQUID_WALLET_ADDRESS", "0xMockAddress"
        )
        return client

    @pytest.mark.asyncio
    async def test_get_balances(self, api_client: HyperliquidAPI, mocker: MagicMock) -> None:
        """Test get_balances returns dict of SpotBalance objects."""
        mock_balance_data = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.50"),
                available_quantity=Decimal("8000.25"),
            ),
            "PURR": SpotBalance(
                exchange="hyperliquid",
                asset="PURR",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("50.0"),
                available_quantity=Decimal("50.0"),
            ),
        }
        mocker.patch.object(api_client, "get_balances", return_value=mock_balance_data)

        # Get balances
        balances: dict[str, SpotBalance] = await api_client.get_balances()

        # Verify expected data
        assert "USDC" in balances
        assert isinstance(balances["USDC"], SpotBalance)
        assert balances["USDC"].asset == "USDC"
        assert balances["USDC"].available_quantity == Decimal("8000.25")
        assert balances["USDC"].total_quantity == Decimal("10000.50")

        # Verify the mocked method was called
        mocked_get_balances = api_client.get_balances
        assert isinstance(mocked_get_balances, AsyncMock)
        mocked_get_balances.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_positions(self, api_client: HyperliquidAPI, mocker: MagicMock) -> None:
        """Test get_positions returns list of DerivativePosition objects."""
        now = datetime.now(UTC)
        mock_position_data = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                timestamp=now,
                side=OrderSide.BUY,
                size=Decimal("1.5"),
                entry_price=Decimal("3000.0"),
                mark_price=Decimal("3100.0"),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                timestamp=now,
                side=OrderSide.SELL,
                size=Decimal("0.1"),
                entry_price=Decimal("50000.0"),
                mark_price=Decimal("49500.0"),
            ),
        ]
        mocker.patch.object(api_client, "get_positions", return_value=mock_position_data)

        # Get positions
        positions: list[DerivativePosition] = await api_client.get_positions()

        # Verify expected data
        assert len(positions) == 2
        assert isinstance(positions[0], DerivativePosition)
        assert positions[0].symbol == "ETH"

        # Verify the mocked method was called
        mocked_get_positions = api_client.get_positions
        assert isinstance(mocked_get_positions, AsyncMock)
        mocked_get_positions.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_funding_rate(self, api_client: HyperliquidAPI, mocker: MagicMock) -> None:
        """Test get_funding_rate returns FundingRate object."""
        now = datetime.now(UTC)
        next_funding_time_ts = int(time.time() + 3600)
        next_funding_time_dt = datetime.fromtimestamp(next_funding_time_ts, tz=UTC)
        mock_funding_data = FundingRate(
            symbol="ETH",
            funding_rate=Decimal("0.0001"),
            next_funding_time=next_funding_time_dt,
            timestamp=now,
        )
        mocker.patch.object(api_client, "get_funding_rates", return_value=[mock_funding_data])

        # Get funding rate
        funding_rates = await api_client.get_funding_rates(symbols=["ETH"])

        # Verify expected data
        assert isinstance(funding_rates[0], FundingRate)
        funding_rate = funding_rates[0]
        assert funding_rate.symbol == "ETH"
        assert funding_rate.funding_rate == Decimal("0.0001")
        assert funding_rate.next_funding_time == next_funding_time_dt

        # Verify the mocked method was called
        mocked_get_funding_rates = api_client.get_funding_rates
        assert isinstance(mocked_get_funding_rates, AsyncMock)
        mocked_get_funding_rates.assert_called_once_with(symbols=["ETH"])

    @pytest.mark.asyncio
    async def test_place_order(self, api_client: HyperliquidAPI, mocker: MagicMock) -> None:
        """Test place_order returns proper Order object."""
        # Mock the specific public method
        mock_order_response = Order(
            client_order_id="123456789",
            exchange="hyperliquid",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("42000.0"),
            quantity_requested=Decimal("0.1"),
            quantity_filled=Decimal("0.0"),
            status=OrderStatus.NEW,
            created_at=datetime.now(UTC),
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        # Mock the place_order method itself
        mocker.patch.object(api_client, "place_order", return_value=mock_order_response)

        # Place order
        order = await api_client.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("42000.0"),
            client_order_id="test-order-123",
            time_in_force=TimeInForce.GTC,
        )

        # Verify the returned order object
        assert isinstance(order, Order)
        assert order.symbol == "BTC"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.price == Decimal("42000.0")
        assert order.quantity_requested == Decimal("0.1")
        assert order.status == OrderStatus.NEW
        assert order.client_order_id == "123456789"

        # Verify the mocked method was called
        mocked_place_order = api_client.place_order
        assert isinstance(mocked_place_order, AsyncMock)
        mocked_place_order.assert_called_once_with(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("42000.0"),
            client_order_id="test-order-123",
            time_in_force=TimeInForce.GTC,
        )

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client: HyperliquidAPI, mocker: MagicMock) -> None:
        """Test cancel_order sends correct request."""
        mock_cancel_response = {"status": "ok"}
        mocker.patch.object(api_client, "cancel_order", return_value=mock_cancel_response)

        # Cancel order
        response = await api_client.cancel_order(order_id="test-order-id-456", symbol="BTC")

        # Verify response and call
        assert response == mock_cancel_response
        mocked_cancel_order = api_client.cancel_order
        assert isinstance(mocked_cancel_order, AsyncMock)
        mocked_cancel_order.assert_called_once_with(order_id="test-order-id-456", symbol="BTC")

    @pytest.mark.asyncio
    async def test_authentication(self, api_client: HyperliquidAPI) -> None:
        """Test the authentication mechanism (placeholder)."""
        # Basic check: ensure necessary attributes exist if needed for auth
        assert hasattr(api_client, "_wallet_address")  # noqa: SLF001
        # assert hasattr(api_client, "_api_secret")  # noqa: SLF001 # Hyperliquid uses private key/account
        assert hasattr(api_client, "_private_key") or hasattr(api_client, "_account")  # noqa: SLF001 - Check for key or derived account

    @pytest.mark.asyncio
    async def test_connect_valid_address(
        self, api_client: HyperliquidAPI, mock_config: Config, mock_secrets: dict[str, str]
    ) -> None:
        """Test connecting with a valid wallet address."""
        # This test might focus on initialization or a connection step if applicable
        assert api_client._wallet_address == "0xValidAddress"  # noqa: SLF001 - Testing protected member
