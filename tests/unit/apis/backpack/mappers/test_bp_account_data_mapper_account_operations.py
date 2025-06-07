"""CyberDeltaEngine: Backpack Account Data Mapper Account Operations Tests.

-----------------------------------------------------------------------

Comprehensive test suite for BackpackAccountDataMapper account operations methods.
Tests all public transformation methods with various scenarios including:
- Transfer transformations with different statuses
- Withdrawal transformations with various response formats
- WebSocket event transformations for fills and position updates
- Error handling and edge cases
- Boundary value testing
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, cast
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.bp_response_handler import RawJsonResponse
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionUpdate
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import DerivativePosition, Trade
from cyberdelta.core.models.enums import InternalTransferStatus, InternalWithdrawalStatus, OrderSide
from cyberdelta.core.models.operations import (
    Transfer,
    Withdrawal,
)
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def mapper() -> BackpackAccountDataMapper:
    """Fixture providing a BackpackAccountDataMapper instance."""
    return BackpackAccountDataMapper()


def create_raw_transfer_response(
    transfer_id: str = "transfer123",
    status: str = "success",
    message: str | None = None,
    timestamp: str | None = "1678886400000",
) -> RawJsonResponse:
    """Create raw transfer response dictionaries for testing account operations."""
    response: dict[str, str | int | float | bool | None] = {
        "id": transfer_id,
        "status": status,
    }
    if message is not None:
        response["message"] = message
    if timestamp is not None:
        response["timestamp"] = timestamp
    # Cast to RawJsonResponse for compatibility
    return cast("RawJsonResponse", response)


def _create_base_withdrawal_data(**kwargs: str | int | float | bool | None) -> dict[str, Any]:
    """Create base withdrawal data dictionary."""
    defaults: dict[str, Any] = {
        "id": 123,
        "status": "confirmed",
        "blockchain": "Ethereum",
        "quantity": "1000.0",
        "fee": "5.0",
        "symbol": "USDC",
        "toAddress": "0xabc123",
        "createdAt": "2024-01-15T10:30:00Z",
        "isInternal": False,
        "transactionHash": "0xhash123",
    }
    
    # Map snake_case kwargs to the correct field names
    field_mapping = {
        "withdrawal_id": "id",
        "to_address": "toAddress", 
        "created_at": "createdAt",
        "is_internal": "isInternal",
        "transaction_hash": "transactionHash",
        "client_id": "clientId",
    }
    
    # Apply mapped kwargs, removing original snake_case keys
    filtered_kwargs: dict[str, str | int | float | bool | None] = {}
    for key, value in kwargs.items():
        if key in field_mapping:
            defaults[field_mapping[key]] = value
        else:
            filtered_kwargs[key] = value
    
    # Update with remaining kwargs
    defaults.update(filtered_kwargs)
    return defaults


def _add_optional_withdrawal_fields(
    data: dict[str, Any], **optional_fields: str | int | float | bool | None
) -> dict[str, Any]:
    """Add optional fields to withdrawal data if they are not None."""
    for key, value in optional_fields.items():
        if value is not None:
            data[key] = value
    return data


def create_raw_withdrawal_response(
    **kwargs: str | int | float | bool | None,
) -> BackpackRawWithdrawalResponse:
    """Create BackpackRawWithdrawalResponse instances for testing withdrawal operations."""
    # Create base data with defaults
    raw_data = _create_base_withdrawal_data(**kwargs)
    
    # Add optional fields
    optional_fields = {
        "clientId": kwargs.get("client_id"),
        "identifier": kwargs.get("identifier"),
        "fiatFee": kwargs.get("fiat_fee"),
        "fiatState": kwargs.get("fiat_state"),
        "fiatSymbol": kwargs.get("fiat_symbol"),
        "providerId": kwargs.get("provider_id"),
        "subaccountId": kwargs.get("subaccount_id"),
        "bankName": kwargs.get("bank_name"),
        "bankIdentifier": kwargs.get("bank_identifier"),
        "accountIdentifier": kwargs.get("account_identifier"),
    }
    raw_data = _add_optional_withdrawal_fields(raw_data, **optional_fields)

    return BackpackRawWithdrawalResponse.model_validate(raw_data)


def create_raw_fill(
    trade_id: int = 12345,
    symbol: str = "SOL-USDC",
    side: Literal["Bid", "Ask"] = "Bid",
    quantity: str = "10.0",
    price: str = "100.0",
    fee: str = "0.05",
    fee_symbol: str = "USDC",
    is_maker: bool = True,
    timestamp: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
    client_id: str | None = "client123",
) -> BackpackRawFill:
    """Create BackpackRawFill instances for testing fill data mapping."""
    return BackpackRawFill(
        tradeId=trade_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        fee=fee,
        feeSymbol=fee_symbol,
        isMaker=is_maker,
        timestamp=timestamp,
        orderId=order_id,
        clientId=client_id,
    )


def create_raw_position_update(
    event_type: Literal["positionUpdate"] = "positionUpdate",
    event_time: int = 1678886400000,
    symbol: str = "SOL-USDC",
    b: str | None = "100.25",  # breakEventPrice
    B: str | None = "100.00",  # entryPrice
    liq_price: str | None = "90.00",  # liquidationPrice
    f: str | None = "0.05",  # initialMarginFraction
    M: str | None = "100.50",  # markPrice
    m: str | None = "0.02",  # maintenanceMarginFraction
    q: str | None = "10.0",  # netQuantity
    Q: str | None = "10.0",  # netExposureQuantity
    n: str | None = "1000.0",  # netExposureNotional
) -> BackpackRawPositionUpdate:
    """Create BackpackRawPositionUpdate instances for testing."""
    return BackpackRawPositionUpdate(
        e=event_type,
        E=event_time,
        s=symbol,
        b=b,
        B=B,
        l=liq_price,
        f=f,
        M=M,
        m=m,
        q=q,
        Q=Q,
        n=n,
    )


class TestTransferTransformation:
    """Test cases for transfer transformation functionality."""

    def test_transform_raw_transfer_to_internal_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful transformation of transfer data to internal Transfer."""
        raw_response = create_raw_transfer_response(
            transfer_id="transfer123",
            status="success",
            message="Transfer completed",
            timestamp="1678886400000",
        )

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("1000.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id="client123",
        )

        assert isinstance(result, Transfer)
        assert result.id == "transfer123"
        assert result.exchange == "backpack"
        assert result.asset == "USDC"
        assert result.quantity == Decimal("1000.0")
        assert result.status == InternalTransferStatus.COMPLETED
        assert result.response_message == "Transfer completed"
        assert result.bp_details is not None
        assert result.bp_details.client_id == "client123"
        assert result.bp_details.from_account_type == "spot"
        assert result.bp_details.to_account_type == "margin"
        assert isinstance(result.timestamp, datetime)

    def test_transform_raw_transfer_different_statuses(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test transfer transformation with different status values."""
        status_mappings = [
            ("success", InternalTransferStatus.COMPLETED),
            ("completed", InternalTransferStatus.COMPLETED),
            ("confirmed", InternalTransferStatus.UNKNOWN),
            ("pending", InternalTransferStatus.PENDING),
            ("processing", InternalTransferStatus.PENDING),
            ("failed", InternalTransferStatus.FAILED),
            ("rejected", InternalTransferStatus.FAILED),
            ("error", InternalTransferStatus.UNKNOWN),
            ("unknown_status", InternalTransferStatus.UNKNOWN),
        ]

        for raw_status, expected_internal_status in status_mappings:
            raw_response = create_raw_transfer_response(status=raw_status)

            result = mapper.transform_raw_transfer_to_internal(
                raw_response=raw_response,
                exchange_name="backpack",
                asset="USDC",
                quantity=Decimal("100.0"),
                from_account_type_raw="spot",
                to_account_type_raw="margin",
                client_transfer_id=None,
            )

            assert result.status == expected_internal_status

    def test_transform_raw_transfer_missing_id_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing transfer ID raises TransformationError."""
        raw_response: RawJsonResponse = cast(
            "RawJsonResponse",
            {
                "status": "success",
                # Missing 'id' field
            },
        )

        with pytest.raises(TransformationError, match="Missing 'id' in raw transfer response"):
            mapper.transform_raw_transfer_to_internal(
                raw_response=raw_response,
                exchange_name="backpack",
                asset="USDC",
                quantity=Decimal("100.0"),
                from_account_type_raw="spot",
                to_account_type_raw="margin",
                client_transfer_id=None,
            )

    def test_transform_raw_transfer_invalid_response_type_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that non-dict response type raises TransformationError."""
        with pytest.raises(TransformationError, match="Raw transfer response is not a dict"):
            mapper.transform_raw_transfer_to_internal(
                raw_response=cast("RawJsonResponse", "invalid_response"),
                exchange_name="backpack",
                asset="USDC",
                quantity=Decimal("100.0"),
                from_account_type_raw="spot",
                to_account_type_raw="margin",
                client_transfer_id=None,
            )

    def test_transform_raw_transfer_none_status_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that None status is handled gracefully."""
        # Create a proper mutable dict that can be modified
        mutable_response = {
            "id": "transfer123",
            "status": None,  # Set to None directly
        }

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=cast("RawJsonResponse", mutable_response),
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("100.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=None,
        )

        assert result.status == InternalTransferStatus.UNKNOWN

    def test_transform_raw_transfer_invalid_timestamp_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that invalid timestamp is handled gracefully."""
        raw_response = create_raw_transfer_response(timestamp="invalid_timestamp")

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("100.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=None,
        )

        # Should default to current time if timestamp is invalid
        assert isinstance(result.timestamp, datetime)

    def test_transform_raw_transfer_large_values(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transfer transformation with large decimal values."""
        raw_response = create_raw_transfer_response()

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="BTC",
            quantity=Decimal("999999.123456789012345"),  # Large, high-precision amount
            from_account_type_raw="margin",
            to_account_type_raw="spot",
            client_transfer_id="large_transfer_123",
        )

        assert result.quantity == Decimal("999999.123456789012345")
        assert result.asset == "BTC"


class TestWithdrawalTransformation:
    """Test cases for withdrawal transformation functionality."""

    def test_transform_raw_withdrawal_response_to_internal_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful transformation of withdrawal response to internal Withdrawal."""
        raw_response = create_raw_withdrawal_response(
            withdrawal_id=123,
            status="confirmed",
            blockchain="Ethereum",
            quantity="1000.0",
            fee="5.0",
            symbol="USDC",
            to_address="0xabc123",
            created_at="2024-01-15T10:30:00Z",
            is_internal=False,
            transaction_hash="0xhash123",
            client_id="client123",
        )

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="USDC",
            quantity=Decimal("1000.0"),
            address="0xabc123",
            network="ethereum",
            client_withdrawal_id="client123",
            tag=None,
        )

        assert isinstance(result, Withdrawal)
        assert result.id == "123"
        assert result.asset == "USDC"
        assert result.quantity == Decimal("1000.0")
        assert result.address == "0xabc123"
        assert result.status == InternalWithdrawalStatus.COMPLETED
        assert result.fee == Decimal("5.0")
        assert result.tx_hash == "0xhash123"
        assert result.bp_details is not None
        assert result.bp_details.blockchain == "ethereum"
        assert not result.bp_details.is_internal
        assert result.bp_details.client_id == "client123"

    def test_transform_raw_withdrawal_different_statuses(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test withdrawal status mapping for different statuses."""
        # Test valid statuses that the raw model accepts
        valid_status_mappings = [
            ("confirmed", InternalWithdrawalStatus.COMPLETED),
            ("pending", InternalWithdrawalStatus.PENDING),
        ]

        for raw_status, expected_internal_status in valid_status_mappings:
            raw_response = create_raw_withdrawal_response(status=raw_status)

            result = mapper.transform_raw_withdrawal_response_to_internal(
                raw_response=raw_response,
                asset="USDC",
                quantity=Decimal("100.0"),
                address="0xtest",
                network="ethereum",
                client_withdrawal_id=None,
                tag=None,
            )

            assert result.status == expected_internal_status

        # Test mapper's handling of other statuses by creating a valid raw model
        # and then modifying its status attribute to test the mapper logic
        raw_response = create_raw_withdrawal_response(status="confirmed")

        # Test other status mappings by directly modifying the raw model
        other_status_mappings = [
            ("failure", InternalWithdrawalStatus.FAILED),
            ("rejected", InternalWithdrawalStatus.FAILED),
            ("cancelled", InternalWithdrawalStatus.CANCELED),
            ("unknown_status", InternalWithdrawalStatus.UNKNOWN),
        ]

        for raw_status, expected_internal_status in other_status_mappings:
            # Modify the status directly on the raw model to bypass validation
            raw_response_copy = raw_response.model_copy()
            raw_response_copy.__dict__["status"] = raw_status

            result = mapper.transform_raw_withdrawal_response_to_internal(
                raw_response=raw_response_copy,
                asset="USDC",
                quantity=Decimal("100.0"),
                address="0xtest",
                network="ethereum",
                client_withdrawal_id=None,
                tag=None,
            )

            assert result.status == expected_internal_status

    def test_transform_raw_withdrawal_with_tag(self, mapper: BackpackAccountDataMapper) -> None:
        """Test withdrawal transformation with destination tag."""
        raw_response = create_raw_withdrawal_response(
            symbol="XRP",
            to_address="rAddress123",
        )

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="XRP",
            quantity=Decimal("1000.0"),
            address="rAddress123",
            network="xrp",
            client_withdrawal_id=None,
            tag="12345",
        )

        assert result.asset == "XRP"
        assert result.address == "rAddress123"
        assert result.bp_details is not None

    def test_transform_raw_withdrawal_internal_transfer(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test withdrawal transformation for internal transfers."""
        raw_response = create_raw_withdrawal_response(
            is_internal=True,
            blockchain="Ethereum",
            transaction_hash=None,
        )

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="USDC",
            quantity=Decimal("500.0"),
            address="internal_address",
            network=None,
            client_withdrawal_id="internal_123",
            tag=None,
        )

        # DEFENSIVE CHECK: bp_details could be None after transformation.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result.bp_details is not None, "Expected bp_details but got None"
        assert result.bp_details.is_internal
        assert result.bp_details.blockchain == "Ethereum"
        assert result.tx_hash is None

    def test_transform_raw_withdrawal_invalid_timestamp_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that invalid withdrawal timestamp is handled gracefully."""
        # Create a valid raw response first
        raw_response = create_raw_withdrawal_response(created_at="2024-01-15T10:30:00Z")

        # Mock parse_datetime_utc to simulate invalid timestamp handling
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_datetime_utc",
        ) as mock_parse_datetime:
            # Return None to simulate invalid timestamp parsing
            mock_parse_datetime.return_value = None

            result = mapper.transform_raw_withdrawal_response_to_internal(
                raw_response=raw_response,
                asset="USDC",
                quantity=Decimal("100.0"),
                address="0xtest",
                network="ethereum",
                client_withdrawal_id=None,
                tag=None,
            )

            # Should default to current time if timestamp is invalid
            assert isinstance(result.timestamp, datetime)

    def test_transform_raw_withdrawal_none_fee_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that None fee value is handled gracefully."""
        # Create a valid raw response first
        raw_response = create_raw_withdrawal_response(fee="5.0")

        # Mock parse_decimal_value to return None for fee
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion based on field name."""
                _ = allow_none  # Acknowledge parameter
                if field_name == "fee":
                    return None
                # For other parsing calls, return a valid decimal
                try:
                    return Decimal(str(value)) if value else None
                except Exception:
                    return None

            mock_parse.side_effect = side_effect

            result = mapper.transform_raw_withdrawal_response_to_internal(
                raw_response=raw_response,
                asset="USDC",
                quantity=Decimal("100.0"),
                address="0xtest",
                network="ethereum",
                client_withdrawal_id=None,
                tag=None,
            )

            assert result.fee is None

    def test_transform_raw_withdrawal_large_amounts(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test withdrawal transformation with large amounts."""
        raw_response = create_raw_withdrawal_response(
            withdrawal_id=999999999,
            quantity="999999.123456789012345",
            fee="99.987654321098765",
        )

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="BTC",
            quantity=Decimal("999999.123456789012345"),
            address="bc1qlargewithdrawal123",
            network="bitcoin",
            client_withdrawal_id="large_withdrawal_123",
            tag=None,
        )

        assert result.id == "999999999"
        assert result.quantity == Decimal("999999.123456789012345")
        assert result.fee == Decimal("99.987654321098765")


class TestWebSocketFillTransformation:
    """Test cases for WebSocket fill event transformation functionality."""

    def test_transform_ws_fill_event_to_internal_trade_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful WebSocket fill event transformation to Trade."""
        raw_fill = create_raw_fill(
            trade_id=12345,
            symbol="SOL-USDC",
            side="Bid",
            quantity="10.0",
            price="100.0",
            fee="0.05",
            fee_symbol="USDC",
            is_maker=True,
            timestamp="2024-01-15T10:30:00Z",
            order_id="order123",
            client_id="client123",
        )

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is not None
        assert isinstance(result, Trade)
        assert result.id == "12345"
        assert result.symbol == "SOL-USDC"
        assert result.side == OrderSide.BUY  # Bid -> BUY
        assert result.quantity == Decimal("10.0")
        assert result.price == Decimal("100.0")
        assert result.fee == Decimal("0.05")
        assert result.fee_asset == "USDC"
        assert result.is_maker
        assert result.order_id == "order123"
        assert result.client_order_id == "client123"
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None

    def test_transform_ws_fill_event_ask_side(self, mapper: BackpackAccountDataMapper) -> None:
        """Test WebSocket fill event transformation with Ask side."""
        raw_fill = create_raw_fill(side="Ask", is_maker=False)

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is not None
        assert result.side == OrderSide.SELL  # Ask -> SELL
        assert not result.is_maker

    def test_transform_ws_fill_event_zero_price_returns_none(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that WebSocket fill with zero price returns None."""
        raw_fill = create_raw_fill(price="0.0")

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is None

    def test_transform_ws_fill_event_zero_quantity_returns_none(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that WebSocket fill with zero quantity returns None."""
        raw_fill = create_raw_fill(quantity="0.0")

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is None

    def test_transform_ws_fill_event_none_client_id_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that None client_id is handled gracefully."""
        raw_fill = create_raw_fill(client_id=None)

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is not None
        assert result.client_order_id is None

    def test_transform_ws_fill_event_transformation_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that WebSocket fill transformation errors are properly wrapped."""
        raw_fill = create_raw_fill()

        # Mock parse_decimal_value to raise an error
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawFill to Trade",
            ):
                mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

    def test_transform_ws_fill_event_high_precision_values(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test WebSocket fill transformation with high precision values."""
        raw_fill = create_raw_fill(
            quantity="10.123456789012345",
            price="100.987654321098765",
            fee="0.012345678901234",
        )

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is not None
        assert result.quantity == Decimal("10.123456789012345")
        assert result.price == Decimal("100.987654321098765")
        assert result.fee == Decimal("0.012345678901234")


class TestWebSocketPositionUpdateTransformation:
    """Test cases for WebSocket position update transformation functionality."""

    def test_transform_ws_position_update_to_internal_position_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful WebSocket position update transformation to DerivativePosition."""
        raw_position_update = create_raw_position_update(
            symbol="SOL-USDC",
            b="100.25",  # breakEventPrice
            B="100.00",  # entryPrice
            liq_price="90.00",  # liquidationPrice
            f="0.05",  # initialMarginFraction
            M="100.50",  # markPrice
            m="0.02",  # maintenanceMarginFraction
            q="10.0",  # netQuantity
            Q="10.0",  # netExposureQuantity
            n="1000.0",  # netExposureNotional
        )

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert isinstance(result, DerivativePosition)
        assert result.symbol == "SOL-USDC"
        assert result.side == OrderSide.BUY  # Positive quantity -> BUY
        assert result.size == Decimal("10.0")
        assert result.entry_price == Decimal("100.00")
        assert result.mark_price == Decimal("100.50")
        assert result.liquidation_price == Decimal("90.00")
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None
        assert result.bp_details.imf_base == Decimal("0.05")
        assert result.bp_details.mmf_base == Decimal("0.02")
        assert isinstance(result.timestamp, datetime)

    def test_transform_ws_position_update_short_position(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test WebSocket position update transformation for short position."""
        raw_position_update = create_raw_position_update(
            q="-5.0",  # Negative quantity (short)
        )

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert result.side == OrderSide.SELL  # Negative quantity -> SELL
        assert result.size == Decimal("-5.0")

    def test_transform_ws_position_update_none_optional_fields_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test WebSocket position update with None optional fields."""
        raw_position_update = create_raw_position_update(
            b=None,  # No break even price
            liq_price=None,  # No liquidation price
            f=None,  # No initial margin fraction
            m=None,  # No maintenance margin fraction
        )

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert isinstance(result, DerivativePosition)
        assert result.liquidation_price is None
        assert result.bp_details is not None
        # Should handle None values gracefully
        assert result.bp_details.imf_base is None
        assert result.bp_details.mmf_base is None

    def test_transform_ws_position_update_zero_quantity_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test WebSocket position update with zero quantity."""
        raw_position_update = create_raw_position_update(q="0.0")

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert result.size == Decimal("0.0")
        assert result.side == OrderSide.SELL  # Zero defaults to SELL

    def test_transform_ws_position_update_missing_net_quantity_handled_gracefully(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing net quantity defaults to zero gracefully."""
        raw_position_update = create_raw_position_update(q=None)  # No net quantity

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert isinstance(result, DerivativePosition)
        assert result.size == Decimal("0.0")
        assert result.side == OrderSide.SELL  # Zero defaults to SELL
        assert result.entry_price is None  # Entry price should be None for zero size

    def test_transform_ws_position_update_transformation_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that WebSocket position update transformation errors are properly wrapped."""
        raw_position_update = create_raw_position_update()

        # Mock parse_decimal_value to raise an error
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform WebSocket position update to internal",
            ):
                mapper.transform_ws_position_update_to_internal_position(raw_position_update)

    def test_transform_ws_position_update_high_precision_values(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test WebSocket position update with high precision values."""
        raw_position_update = create_raw_position_update(
            q="10.123456789012345",  # High precision quantity
            B="100.987654321098765",  # High precision entry price
            M="101.111111111111111",  # High precision mark price
            liq_price="89.999999999999999",  # High precision liquidation price
        )

        result = mapper.transform_ws_position_update_to_internal_position(raw_position_update)

        assert result.size == Decimal("10.123456789012345")
        assert result.entry_price == Decimal("100.987654321098765")
        assert result.mark_price == Decimal("101.111111111111111")
        assert result.liquidation_price == Decimal("89.999999999999999")


class TestErrorHandling:
    """Test cases for error handling and edge cases."""

    def test_edge_case_unicode_asset_names(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transformation with Unicode asset names."""
        raw_response = create_raw_transfer_response()

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC🚀",  # Unicode emoji in asset name
            quantity=Decimal("100.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=None,
        )

        assert result.asset == "USDC🚀"

    def test_edge_case_very_long_client_ids(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transformation with very long client IDs."""
        long_client_id = "client_" + "a" * 100  # 107 characters
        raw_response = create_raw_transfer_response()

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("100.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=long_client_id,
        )

        # DEFENSIVE CHECK: bp_details could be None after transformation.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result.bp_details is not None, "Expected bp_details but got None"
        assert result.bp_details.client_id == long_client_id

    def test_edge_case_very_large_withdrawal_ids(self, mapper: BackpackAccountDataMapper) -> None:
        """Test withdrawal transformation with very large withdrawal IDs."""
        large_id = 999999999999999999  # Very large withdrawal ID
        raw_response = create_raw_withdrawal_response(withdrawal_id=large_id)

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="USDC",
            quantity=Decimal("100.0"),
            address="0xtest",
            network="ethereum",
            client_withdrawal_id=None,
            tag=None,
        )

        assert result.id == str(large_id)
