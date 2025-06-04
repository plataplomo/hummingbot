"""Unit tests for Backpack Raw API Request Payload Models.

Tests the Pydantic models defined in cyberdelta.apis.backpack.models.bp_raw_api_request_payloads.py.
These models represent request payloads sent to Backpack Exchange API endpoints.

Tests focus on:
1. Valid instantiation with required and optional fields
2. Field-level validation (type, format, Literal constraints)
3. Alias functionality (Field(alias=...))
4. Model configuration (extra="forbid", frozen=True)
"""

from typing import Any, Literal

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountConvertDustRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawInternalTransferRequest,
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
    BackpackRawQuoteAcceptRequest,
    BackpackRawQuoteSubmitRequest,
    BackpackRawRequestForQuoteCancelRequest,
    BackpackRawRequestForQuoteRefreshRequest,
    BackpackRawRequestForQuoteRequest,
    BackpackRawUpdateAccountSettingsRequest,
)


class TestBackpackRawOrderExecuteRequest:
    """Tests for BackpackRawOrderExecuteRequest model."""

    def test_valid_minimal_limit_order(self) -> None:
        """Test valid instantiation with minimal required fields for LIMIT order."""
        request = BackpackRawOrderExecuteRequest(
            orderType="Limit",
            side="Bid",
            symbol="SOL_USDC",
            price="100.50",
            quantity="10.0",
        )
        assert request.orderType == "Limit"
        assert request.side == "Bid"
        assert request.symbol == "SOL_USDC"
        assert request.price == "100.50"
        assert request.quantity == "10.0"
        # All optional fields should be None by default
        assert request.clientId is None
        assert request.postOnly is None
        assert request.reduceOnly is None

    def test_valid_minimal_market_order(self) -> None:
        """Test valid instantiation with minimal required fields for MARKET order."""
        request = BackpackRawOrderExecuteRequest(
            orderType="Market",
            side="Ask",
            symbol="BTC_USDC",
            quantity="0.5",
        )
        assert request.orderType == "Market"
        assert request.side == "Ask"
        assert request.symbol == "BTC_USDC"
        assert request.quantity == "0.5"
        assert request.price is None  # Not required for market orders

    def test_valid_full_order_with_all_fields(self) -> None:
        """Test valid instantiation with all fields populated."""
        request = BackpackRawOrderExecuteRequest(
            orderType="Limit",
            side="Bid",
            symbol="ETH_USDC",
            clientId=12345,
            postOnly=True,
            price="1800.00",
            quantity="1.0",
            quoteQuantity="1800.00",
            reduceOnly=False,
            selfTradePrevention="RejectTaker",
            timeInForce="GTC",
            autoLend=True,
            autoLendRedeem=False,
            autoBorrow=True,
            autoBorrowRepay=False,
            stopLossTriggerPrice="1700.00",
            stopLossTriggerBy="LastPrice",
            stopLossLimitPrice="1650.00",
            takeProfitTriggerPrice="2000.00",
            takeProfitTriggerBy="MarkPrice",
            takeProfitLimitPrice="2050.00",
        )
        assert request.orderType == "Limit"
        assert request.clientId == 12345
        assert request.postOnly is True
        assert request.selfTradePrevention == "RejectTaker"
        assert request.timeInForce == "GTC"
        assert request.stopLossTriggerBy == "LastPrice"
        assert request.takeProfitTriggerBy == "MarkPrice"

    def test_aliases_work_correctly(self) -> None:
        """Test that Field aliases work correctly."""
        # Create using valid typed data
        request = BackpackRawOrderExecuteRequest(
            orderType="Limit",
            side="Bid",
            symbol="SOL_USDC",
            clientId=123,
            price="100.00",
            quantity="10.0",
        )

        # Access using Python attribute names
        assert request.orderType == "Limit"
        assert request.side == "Bid"
        assert request.symbol == "SOL_USDC"
        assert request.clientId == 123

    def test_invalid_order_type_literal(self) -> None:
        """Test validation error for invalid orderType."""
        invalid_data: dict[str, Any] = {
            "orderType": "Invalid",  # Not in Literal["Market", "Limit"]
            "side": "Bid",
            "symbol": "SOL_USDC",
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_side_literal(self) -> None:
        """Test validation error for invalid side."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "side": "Invalid",  # Not in Literal["Bid", "Ask"]
            "symbol": "SOL_USDC",
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_time_in_force_literal(self) -> None:
        """Test validation error for invalid timeInForce."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "SOL_USDC",
            "timeInForce": "Invalid",  # Not in Literal["GTC", "IOC", "FOK"]
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_self_trade_prevention_literal(self) -> None:
        """Test validation error for invalid selfTradePrevention."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "SOL_USDC",
            "selfTradePrevention": "Invalid",  # Not in allowed literals
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_trigger_by_literal(self) -> None:
        """Test validation error for invalid trigger by fields."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "SOL_USDC",
            "stopLossTriggerBy": "Invalid",  # Not in allowed literals
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_client_id_type(self) -> None:
        """Test validation error for invalid clientId type."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "symbol": "BTC_USDC",
            "side": "Bid",
            "quantity": "1.0",
            "price": "50000.0",
            "clientId": "invalid_string_id",  # Should be integer
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_price_format(self) -> None:
        """Test validation error for invalid price format."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "symbol": "BTC_USDC",
            "side": "Bid",
            "quantity": "1.0",
            "price": 50000,  # Should be string
            "clientId": 12345,
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_invalid_boolean_type(self) -> None:
        """Test validation error for invalid boolean fields."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "symbol": "BTC_USDC",
            "side": "Bid",
            "quantity": "1.0",
            "price": "50000.0",
            "clientId": 12345,
            "reduceOnly": "true",  # Should be boolean
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        invalid_data: dict[str, Any] = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "SOL_USDC",
            "extraField": "not_allowed",  # Should be forbidden
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest(**invalid_data)

    def test_model_is_frozen(self) -> None:
        """Test that model instances are immutable (frozen=True)."""
        request = BackpackRawOrderExecuteRequest(
            orderType="Limit",
            side="Bid",
            symbol="SOL_USDC",
        )
        with pytest.raises(ValidationError):
            request.orderType = "Market"  # Should fail due to frozen=True


class TestBackpackRawOrderCancelRequest:
    """Tests for BackpackRawOrderCancelRequest model."""

    def test_valid_with_order_id(self) -> None:
        """Test valid instantiation with orderId."""
        request = BackpackRawOrderCancelRequest(
            symbol="SOL_USDC",
            orderId="order_123",
        )
        assert request.symbol == "SOL_USDC"
        assert request.orderId == "order_123"
        assert request.clientId is None

    def test_valid_with_client_id(self) -> None:
        """Test valid instantiation with clientId."""
        request = BackpackRawOrderCancelRequest(
            symbol="SOL_USDC",
            clientId=123,
        )
        assert request.symbol == "SOL_USDC"
        assert request.clientId == 123
        assert request.orderId is None

    def test_valid_with_neither_id(self) -> None:
        """Test valid instantiation with neither ID (model allows this)."""
        request = BackpackRawOrderCancelRequest(
            symbol="SOL_USDC",
        )
        assert request.symbol == "SOL_USDC"
        assert request.orderId is None
        assert request.clientId is None

    def test_invalid_client_id_type(self) -> None:
        """Test validation error for invalid clientId type."""
        invalid_data: dict[str, Any] = {
            "symbol": "SOL_USDC",
            "clientId": "not_an_int",
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderCancelRequest(**invalid_data)

    def test_empty_symbol_validation(self) -> None:
        """Test validation error for empty symbol."""
        with pytest.raises(ValidationError):
            BackpackRawOrderCancelRequest(
                symbol="",  # Empty string should fail
            )

    def test_model_is_frozen(self) -> None:
        """Test that model instances are immutable."""
        request = BackpackRawOrderCancelRequest(symbol="SOL_USDC")
        with pytest.raises(ValidationError):
            request.symbol = "BTC_USDC"


class TestBackpackRawOrderCancelAllRequest:
    """Tests for BackpackRawOrderCancelAllRequest model."""

    def test_valid_minimal(self) -> None:
        """Test valid instantiation with only required fields."""
        request = BackpackRawOrderCancelAllRequest(symbol="SOL_USDC")
        assert request.symbol == "SOL_USDC"
        assert request.orderType is None

    def test_valid_with_order_type_filter(self) -> None:
        """Test valid instantiation with orderType filter."""
        request = BackpackRawOrderCancelAllRequest(
            symbol="SOL_USDC",
            orderType="RestingLimitOrder",
        )
        assert request.symbol == "SOL_USDC"
        assert request.orderType == "RestingLimitOrder"

    def test_invalid_order_type_literal(self) -> None:
        """Test validation error for invalid orderType."""
        invalid_data: dict[str, Any] = {
            "symbol": "SOL_USDC",
            "orderType": "Invalid",  # Not in allowed literals
        }
        with pytest.raises(ValidationError):
            BackpackRawOrderCancelAllRequest(**invalid_data)

    def test_conditional_order_type_valid(self) -> None:
        """Test valid ConditionalOrder orderType."""
        request = BackpackRawOrderCancelAllRequest(
            symbol="SOL_USDC",
            orderType="ConditionalOrder",
        )
        assert request.orderType == "ConditionalOrder"


class TestBackpackRawAccountWithdrawalRequest:
    """Tests for BackpackRawAccountWithdrawalRequest model."""

    def test_valid_minimal_withdrawal(self) -> None:
        """Test valid instantiation with minimal required fields."""
        request = BackpackRawAccountWithdrawalRequest(
            address="0x1234567890abcdef",
            blockchain="Ethereum",
            quantity="100.50",
            symbol="USDC",
        )
        assert request.address == "0x1234567890abcdef"
        assert request.blockchain == "Ethereum"
        assert request.quantity == "100.50"
        assert request.symbol == "USDC"
        assert request.clientId is None
        assert request.twoFactorToken is None

    def test_valid_with_optional_fields(self) -> None:
        """Test valid instantiation with optional fields."""
        request = BackpackRawAccountWithdrawalRequest(
            address="0x1234567890abcdef",
            blockchain="Polygon",
            quantity="50.0",
            symbol="BTC",
            clientId="withdrawal_123",
            twoFactorToken="2fa_token",
            addressTag="memo_tag",
            autoBorrow=True,
            autoLendRedeem=False,
        )
        assert request.clientId == "withdrawal_123"
        assert request.twoFactorToken == "2fa_token"
        assert request.addressTag == "memo_tag"
        assert request.autoBorrow is True
        assert request.autoLendRedeem is False

    def test_invalid_blockchain_literal(self) -> None:
        """Test validation error for invalid blockchain."""
        invalid_data: dict[str, Any] = {
            "address": "0x1234567890abcdef",
            "blockchain": "InvalidChain",  # Not in allowed literals
            "quantity": "100.0",
            "symbol": "USDC",
        }
        with pytest.raises(ValidationError):
            BackpackRawAccountWithdrawalRequest(**invalid_data)

    def test_invalid_symbol_literal(self) -> None:
        """Test validation error for invalid symbol."""
        invalid_data: dict[str, Any] = {
            "address": "0x1234567890abcdef",
            "blockchain": "Ethereum",
            "quantity": "100.0",
            "symbol": "INVALID_TOKEN",  # Not in allowed symbols
        }
        with pytest.raises(ValidationError):
            BackpackRawAccountWithdrawalRequest(**invalid_data)

    @pytest.mark.parametrize(
        "blockchain",
        ["Arbitrum", "Base", "Bitcoin", "Solana", "XRP"],
    )
    def test_valid_various_blockchains(
        self, blockchain: Literal["Arbitrum", "Base", "Bitcoin", "Solana", "XRP"],
    ) -> None:
        """Test valid instantiation with various blockchain options."""
        request = BackpackRawAccountWithdrawalRequest(
            address="test_address",
            blockchain=blockchain,
            quantity="10.0",
            symbol="BTC",
        )
        assert request.blockchain == blockchain

    @pytest.mark.parametrize(
        "symbol",
        ["BTC", "ETH", "SOL", "USDT", "DOGE", "ADA"],
    )
    def test_valid_various_symbols(
        self, symbol: Literal["BTC", "ETH", "SOL", "USDT", "DOGE", "ADA"],
    ) -> None:
        """Test valid instantiation with various symbol options."""
        request = BackpackRawAccountWithdrawalRequest(
            address="test_address",
            blockchain="Ethereum",
            quantity="10.0",
            symbol=symbol,
        )
        assert request.symbol == symbol


class TestBackpackRawUpdateAccountSettingsRequest:
    """Tests for BackpackRawUpdateAccountSettingsRequest model."""

    def test_valid_empty_settings(self) -> None:
        """Test valid instantiation with no fields (all optional)."""
        request = BackpackRawUpdateAccountSettingsRequest()
        assert request.autoBorrowSettlements is None
        assert request.autoLend is None
        assert request.autoRealizePnl is None
        assert request.autoRepayBorrows is None

    def test_valid_partial_settings(self) -> None:
        """Test valid instantiation with some fields."""
        request = BackpackRawUpdateAccountSettingsRequest(
            autoLend=True,
            autoRealizePnl=False,
        )
        assert request.autoLend is True
        assert request.autoRealizePnl is False
        assert request.autoBorrowSettlements is None
        assert request.autoRepayBorrows is None

    def test_valid_all_settings(self) -> None:
        """Test valid instantiation with all fields."""
        request = BackpackRawUpdateAccountSettingsRequest(
            autoBorrowSettlements=True,
            autoLend=False,
            autoRealizePnl=True,
            autoRepayBorrows=False,
        )
        assert request.autoBorrowSettlements is True
        assert request.autoLend is False
        assert request.autoRealizePnl is True
        assert request.autoRepayBorrows is False

    def test_invalid_boolean_type(self) -> None:
        """Test validation error for invalid boolean type."""
        invalid_data: dict[str, Any] = {
            "autoLend": "true",  # Must be bool, not string
        }
        with pytest.raises(ValidationError):
            BackpackRawUpdateAccountSettingsRequest(**invalid_data)


class TestBackpackRawAccountConvertDustRequest:
    """Tests for BackpackRawAccountConvertDustRequest model."""

    def test_valid_dust_conversion(self) -> None:
        """Test valid instantiation."""
        request = BackpackRawAccountConvertDustRequest(symbol="BTC")
        assert request.symbol == "BTC"

    def test_invalid_symbol_literal(self) -> None:
        """Test validation error for invalid symbol."""
        invalid_data: dict[str, Any] = {"symbol": "INVALID_SYMBOL"}
        with pytest.raises(ValidationError):
            BackpackRawAccountConvertDustRequest(**invalid_data)

    @pytest.mark.parametrize(
        "symbol",
        ["BTC", "ETH", "SOL", "USDT", "DOGE", "ADA"],
    )
    def test_valid_various_symbols(
        self, symbol: Literal["BTC", "ETH", "SOL", "USDT", "DOGE", "ADA"],
    ) -> None:
        """Test valid instantiation with various symbols."""
        request = BackpackRawAccountConvertDustRequest(symbol=symbol)
        assert request.symbol == symbol


class TestBackpackRawBorrowLendExecuteRequest:
    """Tests for BackpackRawBorrowLendExecuteRequest model."""

    def test_valid_borrow_request(self) -> None:
        """Test valid borrow request."""
        request = BackpackRawBorrowLendExecuteRequest(
            quantity="100.0",
            side="Borrow",
            symbol="USDC",
        )
        assert request.quantity == "100.0"
        assert request.side == "Borrow"
        assert request.symbol == "USDC"

    @pytest.mark.parametrize(
        "side",
        ["Borrow", "Lend", "Repay", "Redeem"],
    )
    def test_valid_all_sides(self, side: Literal["Borrow", "Lend", "Repay", "Redeem"]) -> None:
        """Test valid instantiation with all side options."""
        request = BackpackRawBorrowLendExecuteRequest(
            quantity="50.0",
            side=side,
            symbol="BTC",
        )
        assert request.side == side

    def test_invalid_side_literal(self) -> None:
        """Test validation error for invalid side."""
        invalid_data: dict[str, Any] = {
            "quantity": "100.0",
            "side": "Invalid",  # Not in allowed literals
            "symbol": "USDC",
        }
        with pytest.raises(ValidationError):
            BackpackRawBorrowLendExecuteRequest(**invalid_data)

    def test_invalid_symbol_literal(self) -> None:
        """Test validation error for invalid symbol."""
        invalid_data: dict[str, Any] = {
            "quantity": "100.0",
            "side": "Borrow",
            "symbol": "INVALID_SYMBOL",
        }
        with pytest.raises(ValidationError):
            BackpackRawBorrowLendExecuteRequest(**invalid_data)


class TestBackpackRawRequestForQuoteRequest:
    """Tests for BackpackRawRequestForQuoteRequest model."""

    def test_valid_minimal_rfq(self) -> None:
        """Test valid instantiation with minimal required fields."""
        request = BackpackRawRequestForQuoteRequest(symbol="SOL_USDC")
        assert request.symbol == "SOL_USDC"
        assert request.quantity is None
        assert request.quoteQuantity is None

    def test_valid_with_quantity(self) -> None:
        """Test valid instantiation with quantity."""
        request = BackpackRawRequestForQuoteRequest(
            symbol="BTC_USDC",
            quantity="1.0",
            autoAcceptThreshold="50000.0",
            submissionTimeMs=1640995200000,
            expiryTimeMs=1640995260000,
            clientId="rfq_123",
        )
        assert request.symbol == "BTC_USDC"
        assert request.quantity == "1.0"
        assert request.autoAcceptThreshold == "50000.0"
        assert request.submissionTimeMs == 1640995200000
        assert request.expiryTimeMs == 1640995260000
        assert request.clientId == "rfq_123"

    def test_valid_with_quote_quantity(self) -> None:
        """Test valid instantiation with quoteQuantity."""
        request = BackpackRawRequestForQuoteRequest(
            symbol="ETH_USDC",
            quoteQuantity="1000.0",
        )
        assert request.symbol == "ETH_USDC"
        assert request.quoteQuantity == "1000.0"
        assert request.quantity is None

    def test_invalid_quantity_format(self) -> None:
        """Test validation error for invalid quantity format."""
        with pytest.raises(ValidationError):
            BackpackRawRequestForQuoteRequest(
                symbol="SOL_USDC",
                quantity="not_a_number",
            )

    def test_invalid_timestamp_type(self) -> None:
        """Test validation error for invalid timestamp type."""
        invalid_data: dict[str, Any] = {
            "symbol": "SOL_USDC",
            "submissionTimeMs": "not_an_int",
        }
        with pytest.raises(ValidationError):
            BackpackRawRequestForQuoteRequest(**invalid_data)


class TestBackpackRawQuoteSubmitRequest:
    """Tests for BackpackRawQuoteSubmitRequest model."""

    def test_valid_quote_submission(self) -> None:
        """Test valid quote submission."""
        request = BackpackRawQuoteSubmitRequest(
            rfqId="rfq_123",
            side="Bid",
            price="100.50",
        )
        assert request.rfqId == "rfq_123"
        assert request.side == "Bid"
        assert request.price == "100.50"
        assert request.clientQuoteId is None

    def test_valid_with_client_quote_id(self) -> None:
        """Test valid quote submission with client quote ID."""
        request = BackpackRawQuoteSubmitRequest(
            rfqId="rfq_456",
            side="Ask",
            price="101.00",
            clientQuoteId="quote_789",
        )
        assert request.clientQuoteId == "quote_789"

    def test_invalid_side_literal(self) -> None:
        """Test validation error for invalid side."""
        invalid_data: dict[str, Any] = {
            "rfqId": "rfq_123",
            "side": "Invalid",  # Not in Literal["Bid", "Ask"]
            "price": "100.0",
        }
        with pytest.raises(ValidationError):
            BackpackRawQuoteSubmitRequest(**invalid_data)

    @pytest.mark.parametrize("side", ["Bid", "Ask"])
    def test_both_sides_valid(self, side: Literal["Bid", "Ask"]) -> None:
        """Test both valid side options."""
        request = BackpackRawQuoteSubmitRequest(
            rfqId="rfq_123",
            side=side,
            price="100.0",
        )
        assert request.side == side


class TestBackpackRawQuoteAcceptRequest:
    """Tests for BackpackRawQuoteAcceptRequest model."""

    def test_valid_quote_acceptance(self) -> None:
        """Test valid quote acceptance."""
        request = BackpackRawQuoteAcceptRequest(
            rfqId="rfq_123",
            quoteId="quote_456",
        )
        assert request.rfqId == "rfq_123"
        assert request.quoteId == "quote_456"

    def test_empty_strings_fail_validation(self) -> None:
        """Test that empty strings fail validation."""
        with pytest.raises(ValidationError):
            BackpackRawQuoteAcceptRequest(
                rfqId="",  # Empty string should fail
                quoteId="quote_456",
            )


class TestBackpackRawRequestForQuoteCancelRequest:
    """Tests for BackpackRawRequestForQuoteCancelRequest model."""

    def test_valid_rfq_cancellation(self) -> None:
        """Test valid RFQ cancellation."""
        request = BackpackRawRequestForQuoteCancelRequest(rfqId="rfq_123")
        assert request.rfqId == "rfq_123"

    def test_empty_rfq_id_fails(self) -> None:
        """Test that empty rfqId fails validation."""
        with pytest.raises(ValidationError):
            BackpackRawRequestForQuoteCancelRequest(rfqId="")


class TestBackpackRawRequestForQuoteRefreshRequest:
    """Tests for BackpackRawRequestForQuoteRefreshRequest model."""

    def test_valid_minimal_refresh(self) -> None:
        """Test valid RFQ refresh with minimal fields."""
        request = BackpackRawRequestForQuoteRefreshRequest(rfqId="rfq_123")
        assert request.rfqId == "rfq_123"
        assert request.submissionTimeMs is None
        assert request.expiryTimeMs is None

    def test_valid_with_timestamps(self) -> None:
        """Test valid RFQ refresh with timestamps."""
        request = BackpackRawRequestForQuoteRefreshRequest(
            rfqId="rfq_123",
            submissionTimeMs=1640995200000,
            expiryTimeMs=1640995260000,
        )
        assert request.rfqId == "rfq_123"
        assert request.submissionTimeMs == 1640995200000
        assert request.expiryTimeMs == 1640995260000

    def test_invalid_timestamp_type(self) -> None:
        """Test validation error for invalid timestamp type."""
        invalid_data: dict[str, Any] = {
            "rfqId": "rfq_123",
            "submissionTimeMs": "not_an_int",
        }
        with pytest.raises(ValidationError):
            BackpackRawRequestForQuoteRefreshRequest(**invalid_data)


class TestBackpackRawInternalTransferRequest:
    """Tests for BackpackRawInternalTransferRequest model."""

    def test_valid_minimal_transfer(self) -> None:
        """Test valid internal transfer with minimal fields."""
        request = BackpackRawInternalTransferRequest(
            symbol="USDC",
            quantity="100.0",
            fromAccount="SPOT",
            toAccount="MARGIN",
        )
        assert request.symbol == "USDC"
        assert request.quantity == "100.0"
        assert request.fromAccount == "SPOT"
        assert request.toAccount == "MARGIN"
        assert request.clientId is None

    def test_valid_with_client_id(self) -> None:
        """Test valid internal transfer with client ID."""
        request = BackpackRawInternalTransferRequest(
            symbol="BTC",
            quantity="0.5",
            fromAccount="MARGIN",
            toAccount="FUTURES",
            clientId="transfer_123",
        )
        assert request.clientId == "transfer_123"

    @pytest.mark.parametrize(
        "from_account,to_account",
        [
            ("SPOT", "MARGIN"),
            ("SPOT", "FUTURES"),
            ("MARGIN", "SPOT"),
            ("MARGIN", "FUTURES"),
            ("FUTURES", "SPOT"),
            ("FUTURES", "MARGIN"),
        ],
    )
    def test_valid_all_account_combinations(
        self,
        from_account: Literal["SPOT", "MARGIN", "FUTURES"],
        to_account: Literal["SPOT", "MARGIN", "FUTURES"],
    ) -> None:
        """Test valid account type combinations."""
        request = BackpackRawInternalTransferRequest(
            symbol="USDC",
            quantity="10.0",
            fromAccount=from_account,
            toAccount=to_account,
        )
        assert request.fromAccount == from_account
        assert request.toAccount == to_account

    def test_invalid_from_account_literal(self) -> None:
        """Test validation error for invalid fromAccount."""
        invalid_data: dict[str, Any] = {
            "symbol": "USDC",
            "quantity": "100.0",
            "fromAccount": "INVALID",  # Not in allowed literals
            "toAccount": "SPOT",
        }
        with pytest.raises(ValidationError):
            BackpackRawInternalTransferRequest(**invalid_data)

    def test_invalid_to_account_literal(self) -> None:
        """Test validation error for invalid toAccount."""
        invalid_data: dict[str, Any] = {
            "symbol": "USDC",
            "quantity": "100.0",
            "fromAccount": "SPOT",
            "toAccount": "INVALID",  # Not in allowed literals
        }
        with pytest.raises(ValidationError):
            BackpackRawInternalTransferRequest(**invalid_data)

    def test_same_accounts_allowed_by_model(self) -> None:
        """Test that model allows same from/to accounts (business logic handles this)."""
        # The raw model should allow this - business validation happens in the builder
        request = BackpackRawInternalTransferRequest(
            symbol="USDC",
            quantity="100.0",
            fromAccount="SPOT",
            toAccount="SPOT",  # Same account - allowed by model, rejected by builder
        )
        assert request.fromAccount == request.toAccount == "SPOT"
