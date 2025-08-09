"""Integration tests for msgspec with existing CyberDelta models."""

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawOrderExecuteRequest,
)
from cyberdelta.utils.serialization import dumps_json, loads_json


class TestExistingModelsCompatibility:
    """Test msgspec compatibility with existing CyberDelta models."""

    def test_backpack_order_request_model(self) -> None:
        """Test serialization of BackpackRawOrderExecuteRequest."""
        order_request = BackpackRawOrderExecuteRequest(
            symbol="BTC_USDC",
            side="Bid",  # Corrected from "Buy" to "Bid" per model spec
            orderType="Limit",
            quantity="0.001",
            price="50000.00",
            timeInForce="GTC",
        )

        # Serialize with msgspec
        json_str = dumps_json(order_request)
        assert isinstance(json_str, str)
        assert "BTC_USDC" in json_str
        assert "50000.00" in json_str

        # Deserialize
        data = loads_json(json_str)
        assert isinstance(data, dict)

        # Reconstruct model
        reconstructed = BackpackRawOrderExecuteRequest.model_validate(data)
        assert reconstructed.symbol == order_request.symbol
        assert reconstructed.price == order_request.price
        assert reconstructed.quantity == order_request.quantity

    def test_complex_nested_model(self) -> None:
        """Test with complex nested structures."""

        class Position(BaseModel):
            """Position model."""

            symbol: str
            quantity: Decimal
            entry_price: Decimal
            current_price: Decimal
            unrealized_pnl: Decimal

        class Portfolio(BaseModel):
            """Portfolio model."""

            account_id: str
            positions: list[Position]
            total_equity: Decimal
            timestamp: datetime

        portfolio = Portfolio(
            account_id="test_account",
            positions=[
                Position(
                    symbol="BTC-USDC",
                    quantity=Decimal("0.5"),
                    entry_price=Decimal(45000),
                    current_price=Decimal(50000),
                    unrealized_pnl=Decimal(2500),
                ),
                Position(
                    symbol="ETH-USDC",
                    quantity=Decimal("2.0"),
                    entry_price=Decimal(3000),
                    current_price=Decimal(3200),
                    unrealized_pnl=Decimal(400),
                ),
            ],
            total_equity=Decimal(100000),
            timestamp=datetime.now(UTC),
        )

        # Serialize
        json_str = dumps_json(portfolio)
        assert isinstance(json_str, str)

        # Deserialize
        data = loads_json(json_str)
        reconstructed = Portfolio.model_validate(data)

        # Verify
        assert reconstructed.account_id == portfolio.account_id
        assert len(reconstructed.positions) == 2
        assert reconstructed.positions[0].symbol == "BTC-USDC"
        assert reconstructed.positions[0].unrealized_pnl == Decimal(2500)
        assert reconstructed.total_equity == Decimal(100000)

    def test_model_with_optional_fields(self) -> None:
        """Test models with optional fields."""

        class OrderUpdate(BaseModel):
            """Order update model."""

            order_id: str
            status: str
            filled_quantity: Decimal | None = None
            fill_price: Decimal | None = None
            error_message: str | None = None

        # With all fields
        update_full = OrderUpdate(
            order_id="order_123",
            status="FILLED",
            filled_quantity=Decimal("0.01"),
            fill_price=Decimal(50000),
            error_message=None,
        )

        json_str = dumps_json(update_full)
        data = loads_json(json_str)
        # Type assertion for mypy
        assert isinstance(data, dict)
        reconstructed = OrderUpdate.model_validate(data)

        assert reconstructed.order_id == "order_123"
        assert reconstructed.filled_quantity == Decimal("0.01")
        # None fields should be excluded
        assert "error_message" not in data

        # With minimal fields
        update_minimal = OrderUpdate(
            order_id="order_456",
            status="PENDING",
        )

        json_str = dumps_json(update_minimal)
        data = loads_json(json_str)
        reconstructed = OrderUpdate.model_validate(data)

        assert reconstructed.order_id == "order_456"
        assert reconstructed.filled_quantity is None
        assert reconstructed.fill_price is None
