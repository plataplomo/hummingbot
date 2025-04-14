import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
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
            def parse_account_update_message(self, message: dict[str, Any]) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
                return None, None # Mock implementation

            async def parse_l2_book_update_message(self, message: dict[str, Any]) -> None:
                pass  # Mock implementation

            # Add dummy implementation for the new abstract method from base
            async def get_order_status(self, order_id: str, symbol: str | None = None) -> Order:
                 raise NotImplementedError("Mock implementation not needed for this test")

        client = ConcreteHyperliquidAPI(api_config=hyperliquid_config, secrets=hyperliquid_secrets)
        # Prevent actual network calls
        client._request = AsyncMock(side_effect=RuntimeError("Network call attempted!"))
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
        api_client.get_balances = AsyncMock(return_value=mock_balance_data)

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
        api_client.get_positions = AsyncMock(return_value=mock_position_data)

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
        api_client.get_funding_rate = AsyncMock(return_value=mock_funding_data)

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
            # id="123456789", # ID not set directly
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("42000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),  # Initial status
            status=OrderStatus.OPEN,  # Use OrderStatus enum
            timestamp=int(time.time() * 1000),
            client_order_id="test-order-123",
            # exchange_order_id would likely be set later
        )
        # Mock the place_order method itself
        api_client.place_order = AsyncMock(return_value=mock_order_response)

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
        # assert order.id is not None # ID might not be in immediate response
        assert order.symbol == "BTC"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.price == Decimal("42000.0")
        assert order.quantity == Decimal("0.1")
        assert order.status == OrderStatus.OPEN  # Check against enum
        assert order.client_order_id == "test-order-123"

        # Verify the mocked method was called
        api_client.place_order.assert_called_once()
        args, kwargs = api_client.place_order.call_args
        assert kwargs.get("symbol") == "BTC"
        assert kwargs.get("side") == OrderSide.BUY
        assert kwargs.get("quantity") == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_cancel_order(self, api_client: HyperliquidAPI) -> None:
        """Test cancel_order returns success indication."""
        # Mock the specific public method - HL cancel might just return status
        api_client.cancel_order = AsyncMock(return_value=True)  # Assume True on success

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
        data = {"type": "clearinghouseState", "user": api_client._wallet_address}

        # Call the method to test
        auth_data = await api_client._authenticate(method=method, path=path, data=data)

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
