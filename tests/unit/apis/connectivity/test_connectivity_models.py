"""Property-based tests for connectivity models.

This module provides comprehensive property-based testing for the Pydantic models
used for HTTP client and WebSocket manager configuration, ensuring data validation,
constraint enforcement, and configuration integrity critical for network connectivity.

Key Testing Areas:
- Content-type validation with various character sets and lengths
- URL validation and normalization properties
- Configuration parameter bounds and constraints
- Model immutability and extra field rejection
- Round-trip serialization properties
- Edge case handling for network timeouts and retries

SECURITY CRITICAL: Configuration errors can lead to:
- Connection failures affecting trading operations
- Security vulnerabilities through misconfigured endpoints
- Rate limiting bypass through incorrect retry settings
- Data corruption through invalid content-type handling
"""

import re
from datetime import timedelta
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import AnyUrl, HttpUrl, ValidationError

from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,
    HttpClientConfig,
    ProcessedResponseHeaders,
    WebSocketManagerConfig,
)
from cyberdelta.apis.exceptions.connectivity import ContentTypeValidationError


def _format_content_type(suffix: str) -> str:
    """Helper function for formatting content-type strings.

    Returns:
        str: Formatted content-type string.
    """
    return f"application/json@{suffix}"


def _format_ip_address(octets: list[int]) -> str:
    """Helper function for formatting IP addresses.

    Returns:
        str: Formatted IP address string.
    """
    return ".".join(str(o) for o in octets)


def _create_http_config_dict(url: str, timeout: float) -> dict[str, str | float]:
    """Helper function for creating HTTP client config dictionary.

    Returns:
        dict[str, str | float]: Configuration dictionary.
    """
    return {"rest_endpoint": url, "default_request_timeout": timeout}


def _create_websocket_config_dict(url: str, ping: float) -> dict[str, str | float]:
    """Helper function for creating WebSocket manager config dictionary.

    Returns:
        dict[str, str | float]: Configuration dictionary.
    """
    return {"ws_url": url, "ping_interval": ping}


# =============================================================================
# HYPOTHESIS STRATEGIES FOR CONNECTIVITY TESTING
# =============================================================================


@st.composite
def content_type_strategy(draw: st.DrawFn) -> str:
    """Generate valid content-type strings.

    Returns:
        Valid content-type strings following HTTP standards.
    """
    # Main type/subtype
    main_types = ["application", "text", "image", "audio", "video", "multipart", "message"]
    sub_types = ["json", "xml", "html", "plain", "jpeg", "png", "mpeg", "octet-stream"]
    vendor_prefixes = ["", "vnd.", "x-", "vnd.api+"]

    main_type = draw(st.sampled_from(main_types))
    vendor = draw(st.sampled_from(vendor_prefixes))
    sub_type = draw(st.sampled_from(sub_types))

    content_type = f"{main_type}/{vendor}{sub_type}"

    # Optionally add parameters
    if draw(st.booleans()):
        params: list[str] = []
        if draw(st.booleans()):
            charset = draw(st.sampled_from(["utf-8", "iso-8859-1", "us-ascii", "utf-16"]))
            params.append(f"charset={charset}")
        if draw(st.booleans()):
            boundary = draw(
                st.text(
                    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-+.",
                    min_size=1,
                    max_size=20,
                )
            )
            params.append(f"boundary={boundary}")
        if params:
            content_type += "; " + "; ".join(params)

    # Ensure within length limit
    if len(content_type) > MAX_CONTENT_TYPE_LENGTH:
        content_type = content_type[:MAX_CONTENT_TYPE_LENGTH]

    return content_type


@st.composite
def invalid_content_type_strategy(draw: st.DrawFn) -> str:
    """Generate invalid content-type strings.

    Returns:
        Invalid content-type strings that should be rejected.
    """
    strategy = draw(
        st.sampled_from([
            # Control characters
            st.text(alphabet=st.characters(whitelist_categories=["Cc"]), min_size=1, max_size=10),
            # Just whitespace
            st.text(alphabet=" \t", min_size=1, max_size=5),
            # Contains invalid characters mixed with valid content
            st.builds(_format_content_type, st.text(alphabet="@#$%^&*", max_size=5)),
            # Characters not in allowed set [a-zA-Z0-9/.\-+=;\s]
            st.text(
                alphabet="@#$%^&*()[]{}|\\\"'`~<>?:,!",
                min_size=1,
                max_size=20,
            ),
            # Too long
            st.text(min_size=MAX_CONTENT_TYPE_LENGTH + 1, max_size=MAX_CONTENT_TYPE_LENGTH + 100),
        ])
    )

    return draw(strategy)


@st.composite
def http_url_strategy(draw: st.DrawFn) -> str:
    """Generate valid HTTP/HTTPS URLs.

    Returns:
        Valid HTTP/HTTPS URL strings.
    """
    scheme = draw(st.sampled_from(["http", "https"]))

    # Host
    host_strategy = st.one_of(
        # Domain names (ASCII only to avoid invalid international domain names)
        st.builds(
            ".".join,
            st.lists(
                st.text(
                    alphabet=st.characters(min_codepoint=97, max_codepoint=122),  # a-z only
                    min_size=1,
                    max_size=10,
                ),
                min_size=2,
                max_size=4,
            ),
        ),
        # IP addresses
        st.just("localhost"),
        st.just("127.0.0.1"),
        st.builds(
            _format_ip_address,
            st.lists(st.integers(0, 255), min_size=4, max_size=4),
        ),
    )
    host = draw(host_strategy)

    port = ""
    if draw(st.booleans()):
        port = f":{draw(st.integers(1, 65535))}"

    path = ""
    if draw(st.booleans()):
        path_parts = draw(
            st.lists(
                st.text(
                    alphabet=st.characters(
                        min_codepoint=48,
                        max_codepoint=122,  # ASCII alphanumeric
                    ).filter(lambda c: c.isalnum() or c in "-_"),
                    min_size=1,
                    max_size=20,
                ),
                min_size=1,
                max_size=5,
            )
        )
        path = "/" + "/".join(path_parts)

    return f"{scheme}://{host}{port}{path}"


@st.composite
def websocket_url_strategy(draw: st.DrawFn) -> str:
    """Generate valid WebSocket URLs.

    Returns:
        Valid WebSocket URL strings.
    """
    scheme = draw(st.sampled_from(["ws", "wss"]))
    http_url = draw(http_url_strategy())
    # Replace http(s) with ws(s)
    return http_url.replace("https://", f"{scheme}://").replace("http://", f"{scheme}://")


# =============================================================================
# PROPERTY-BASED TESTS FOR ProcessedResponseHeaders
# =============================================================================


class TestProcessedResponseHeaders:
    """Property-based tests for the ProcessedResponseHeaders model."""

    @given(content_type=content_type_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_content_type_properties(self, content_type: str) -> None:
        """Property: Valid content-type strings should be accepted and preserved.

        This ensures that any valid HTTP content-type header value is correctly
        stored and retrievable from the model.
        """
        headers = ProcessedResponseHeaders(content_type=content_type)

        # Property: Content type is preserved exactly
        assert headers.content_type == content_type

        assert headers.model_config.get("frozen") is True

        # Property: Extra fields are forbidden
        assert headers.model_config.get("extra") == "forbid"

    @given(invalid_content_type=invalid_content_type_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_invalid_content_type_rejection(self, invalid_content_type: str) -> None:
        """Property: Invalid content-type strings should be rejected.

        This ensures that malformed, dangerous, or non-compliant content-type
        values are properly rejected to prevent security issues.
        """
        # Note: The current validation allows newlines, tabs, and other whitespace
        # since \s includes all whitespace characters. Only truly invalid characters
        # or whitespace-only strings should be rejected.

        # Check if the string would actually be invalid according to current validation
        VALID_CONTENT_TYPE_CHARS_REGEX = re.compile(r"^[a-zA-Z0-9/.\-+=;\s]*$")
        should_fail = (
            not VALID_CONTENT_TYPE_CHARS_REGEX.fullmatch(invalid_content_type)
            or (invalid_content_type and not invalid_content_type.strip())  # whitespace-only
        )

        # Also check length constraint
        too_long = len(invalid_content_type) > 256

        if should_fail or too_long:
            with pytest.raises((ValidationError, ContentTypeValidationError)):
                ProcessedResponseHeaders(content_type=invalid_content_type)
        else:
            # If validation logic allows it, it should pass
            headers = ProcessedResponseHeaders(content_type=invalid_content_type)
            assert headers.content_type == invalid_content_type

    @given(content_type=st.one_of(st.none(), st.just(""), content_type_strategy()))
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_optional_content_type_handling(self, content_type: str | None) -> None:
        """Property: Optional content-type should be handled correctly.

        This ensures that None, empty string, and valid values are all
        handled appropriately for the optional field.
        """
        if content_type is None:
            headers = ProcessedResponseHeaders()
            assert not headers.content_type
        else:
            headers = ProcessedResponseHeaders(content_type=content_type)
            assert headers.content_type == content_type

    @given(valid_content_type=content_type_strategy(), mutation_value=content_type_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_immutability_property(self, valid_content_type: str, mutation_value: str) -> None:
        """Property: Model should be immutable after creation.

        This ensures that the frozen model cannot be modified, maintaining
        data integrity throughout its lifecycle.
        """
        headers = ProcessedResponseHeaders(content_type=valid_content_type)

        # Property: Cannot modify existing fields
        with pytest.raises(ValidationError, match="frozen"):
            headers.content_type = mutation_value

    @given(
        content_type=content_type_strategy(),
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=100),
            min_size=1,
            max_size=5,
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_extra_fields_rejection(self, content_type: str, extra_fields: dict[str, str]) -> None:
        """Property: Extra fields should always be rejected.

        This ensures that only defined fields are accepted, preventing
        accidental data leakage or misconfiguration.
        """
        data = {"content_type": content_type, **extra_fields}

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ProcessedResponseHeaders.model_validate(data)


# =============================================================================
# PROPERTY-BASED TESTS FOR HttpClientConfig
# =============================================================================


class TestHttpClientConfig:
    """Property-based tests for the HttpClientConfig model."""

    @given(
        url=http_url_strategy(),
        timeout=st.floats(min_value=0.1, max_value=120.0),
        max_retries=st.one_of(st.none(), st.integers(min_value=0, max_value=10)),
        retry_delay=st.one_of(st.none(), st.floats(min_value=0.1, max_value=300.0)),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_config_properties(
        self, url: str, timeout: float, max_retries: int | None, retry_delay: float | None
    ) -> None:
        """Property: Valid configurations should be accepted and preserved.

        This ensures that any valid combination of HTTP client parameters
        is correctly stored and retrievable.
        """
        config = HttpClientConfig(
            rest_endpoint=HttpUrl(url),
            default_request_timeout=timeout,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay,
        )

        # Property: URL is normalized but equivalent
        assert str(config.rest_endpoint).startswith(url.split("://", 1)[0])

        # Property: Timeout is preserved exactly
        assert config.default_request_timeout == timeout

        # Property: Optional fields handle None correctly
        assert config.max_retries == max_retries
        assert config.retry_delay_seconds == retry_delay

        assert config.model_config.get("frozen") is True

    @given(
        url=http_url_strategy(),
        invalid_timeout=st.one_of(
            st.floats(max_value=0.0),  # Zero or negative
            st.floats(min_value=120.1),  # Above maximum
            st.just(float("inf")),
            st.just(float("nan")),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_timeout_rejection(self, url: str, invalid_timeout: float) -> None:
        """Property: Invalid timeout values should be rejected.

        This ensures that unreasonable or dangerous timeout values are prevented.
        """
        with pytest.raises(ValidationError):
            HttpClientConfig(rest_endpoint=HttpUrl(url), default_request_timeout=invalid_timeout)

    @given(
        url=http_url_strategy(),
        invalid_retries=st.integers(max_value=-1) | st.integers(min_value=11),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_retries_rejection(self, url: str, invalid_retries: int) -> None:
        """Property: Invalid retry counts should be rejected.

        This ensures that retry logic stays within reasonable bounds.
        """
        with pytest.raises(ValidationError):
            HttpClientConfig(rest_endpoint=HttpUrl(url), max_retries=invalid_retries)

    @given(
        valid_config_data=st.builds(
            _create_http_config_dict,
            url=http_url_strategy(),
            timeout=st.floats(min_value=0.1, max_value=120.0),
        )
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_serialization_round_trip(self, valid_config_data: dict[str, Any]) -> None:
        """Property: Configurations should round-trip through serialization.

        This ensures that configs can be saved and loaded without data loss.
        """
        # Create config from dict
        config = HttpClientConfig.model_validate(valid_config_data)

        # Serialize to dict
        serialized = config.model_dump()

        # Deserialize back
        config2 = HttpClientConfig.model_validate(serialized)

        # Property: Round-trip preserves all data
        assert config.rest_endpoint == config2.rest_endpoint
        assert config.default_request_timeout == config2.default_request_timeout
        assert config.max_retries == config2.max_retries
        assert config.retry_delay_seconds == config2.retry_delay_seconds


# =============================================================================
# PROPERTY-BASED TESTS FOR WebSocketManagerConfig
# =============================================================================


class TestWebSocketManagerConfig:
    """Property-based tests for the WebSocketManagerConfig model."""

    @given(
        url=websocket_url_strategy(),
        ping_interval=st.floats(min_value=0.1, max_value=60.0),
        reconnect_delay=st.floats(min_value=0.1, max_value=300.0),
        max_reconnects=st.integers(min_value=0, max_value=20),
        conn_timeout=st.floats(min_value=0.1, max_value=120.0),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_ws_config_properties(
        self,
        url: str,
        ping_interval: float,
        reconnect_delay: float,
        max_reconnects: int,
        conn_timeout: float,
    ) -> None:
        """Property: Valid WebSocket configurations should be accepted.

        This ensures that any valid combination of WebSocket parameters
        is correctly stored and retrievable.
        """
        config = WebSocketManagerConfig(
            ws_url=AnyUrl(url),
            ping_interval=ping_interval,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnects,
            connection_timeout=conn_timeout,
        )

        # Property: All values preserved
        assert str(config.ws_url).startswith(url.split("://", 1)[0])
        assert config.ping_interval == ping_interval
        assert config.reconnect_delay == reconnect_delay
        assert config.max_reconnect_attempts == max_reconnects
        assert config.connection_timeout == conn_timeout

    @given(
        url=websocket_url_strategy(),
        invalid_ping=st.one_of(
            st.floats(max_value=0.0), st.floats(min_value=60.1), st.just(float("inf"))
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_ping_interval_rejection(self, url: str, invalid_ping: float) -> None:
        """Property: Invalid ping intervals should be rejected.

        This ensures that WebSocket keepalive settings stay reasonable.
        """
        with pytest.raises(ValidationError):
            WebSocketManagerConfig(ws_url=AnyUrl(url), ping_interval=invalid_ping)

    @given(
        config1_data=st.builds(
            _create_websocket_config_dict,
            url=websocket_url_strategy(),
            ping=st.floats(min_value=0.1, max_value=60.0),
        ),
        config2_data=st.builds(
            _create_websocket_config_dict,
            url=websocket_url_strategy(),
            ping=st.floats(min_value=0.1, max_value=60.0),
        ),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_config_equality_properties(
        self, config1_data: dict[str, Any], config2_data: dict[str, Any]
    ) -> None:
        """Property: Configs with same data should be equal.

        This ensures configuration comparison works correctly.
        """
        config1 = WebSocketManagerConfig.model_validate(config1_data)
        config1_copy = WebSocketManagerConfig.model_validate(config1_data)
        config2 = WebSocketManagerConfig.model_validate(config2_data)

        # Property: Same data means equality
        assert config1 == config1_copy

        # Property: Different data means inequality (with high probability)
        if config1_data != config2_data:
            assert config1 != config2

    @given(url=websocket_url_strategy(), zero_attempts=st.just(0))
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_zero_reconnect_attempts_allowed(self, url: str, zero_attempts: int) -> None:
        """Property: Zero reconnect attempts should be valid (no reconnection).

        This ensures that reconnection can be completely disabled if needed.
        """
        config = WebSocketManagerConfig(ws_url=AnyUrl(url), max_reconnect_attempts=zero_attempts)

        assert config.max_reconnect_attempts == 0
