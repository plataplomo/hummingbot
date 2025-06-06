"""VCR configuration for pytest-recording cassette-based integration testing.

This module provides VCR.py configuration for recording and playing back HTTP interactions,
with robust filtering for sensitive data including authentication tokens, signatures,
timestamps, and personal information.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    # VCR request/response objects aren't well-typed, so we use protocols
    from typing import Protocol

    class VCRRequest(Protocol):
        """Protocol for VCR request objects."""

        body: Any

    class VCRResponse(Protocol):
        """Protocol for VCR response objects."""

        body: Any
else:
    # At runtime, these are just Any to avoid import issues
    VCRRequest = Any
    VCRResponse = Any


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """VCR.py configuration for pytest-recording cassette-based integration testing.

    Provides comprehensive configuration for recording and playing back HTTP interactions,
    with robust filtering for sensitive data including authentication tokens, signatures,
    timestamps, and personal information.
    """

    def filter_request_body(request: VCRRequest) -> VCRRequest:
        """Filter and sanitize request body content for VCR cassette recording."""
        if hasattr(request, "body") and getattr(request, "body", None):
            # Filter known sensitive patterns in request bodies
            request_body: Any = request.body
            body_str = (
                request_body.decode("utf-8")
                if isinstance(request_body, bytes)
                else str(request_body)
            )

            # Replace common sensitive patterns
            import re

            # Filter private keys (hex strings that look like private keys)
            body_str = re.sub(
                r'"private_key":\s*"0x[a-fA-F0-9]{64}"',
                '"private_key": "FILTERED_PRIVATE_KEY"',
                body_str,
            )
            # Filter API keys
            body_str = re.sub(r'"api_key":\s*"[^"]*"', '"api_key": "FILTERED_API_KEY"', body_str)
            # Filter signatures
            body_str = re.sub(
                r'"signature":\s*"[^"]*"',
                '"signature": "FILTERED_SIGNATURE"',
                body_str,
            )
            # Filter timestamps to make tests more deterministic
            body_str = re.sub(r'"timestamp":\s*\d+', '"timestamp": 1234567890', body_str)

            request.body = body_str.encode("utf-8") if isinstance(request_body, bytes) else body_str
        return request

    def filter_response_body(response: VCRResponse) -> VCRResponse:
        """Filter and sanitize response body content for VCR cassette recording."""
        if hasattr(response, "body") and getattr(response, "body", None):
            # For now, we don't filter response bodies as they typically don't contain
            # user credentials, but this hook is available for future use
            pass
        return response

    return {
        "filter_headers": [
            # ===== GLOBAL HEADERS =====
            # Standard authentication headers
            ("Authorization", "FILTERED_AUTHORIZATION_HEADER"),
            ("Bearer", "FILTERED_BEARER_TOKEN"),
            ("Cookie", "FILTERED_COOKIE"),
            ("Set-Cookie", "FILTERED_SET_COOKIE"),
            # API key headers (various formats)
            ("X-API-Key", "FILTERED_API_KEY"),
            ("X-Api-Key", "FILTERED_API_KEY"),  # Case variation
            ("API-Key", "FILTERED_API_KEY"),
            ("Api-Key", "FILTERED_API_KEY"),
            ("X-Auth-Token", "FILTERED_AUTH_TOKEN"),
            ("X-Access-Token", "FILTERED_ACCESS_TOKEN"),
            # Signature headers (for HMAC-based auth)
            ("X-Signature", "FILTERED_SIGNATURE"),
            ("X-Sig", "FILTERED_SIGNATURE"),
            ("Signature", "FILTERED_SIGNATURE"),
            # Timestamp headers (for replay protection)
            ("X-Timestamp", "FILTERED_TIMESTAMP"),
            ("X-Time", "FILTERED_TIMESTAMP"),
            ("Timestamp", "FILTERED_TIMESTAMP"),
            # Window headers (for time-based auth)
            ("X-Window", "FILTERED_WINDOW"),
            ("X-Time-Window", "FILTERED_WINDOW"),
            # User agent (normalize for consistency)
            ("User-Agent", "CyberDeltaEngine-Test-Suite/1.0"),
            # ===== EXCHANGE-SPECIFIC HEADERS =====
            # Backpack Exchange headers
            ("X-BP-API-Key", "FILTERED_BACKPACK_API_KEY"),
            ("X-BP-Signature", "FILTERED_BACKPACK_SIGNATURE"),
            ("X-BP-Timestamp", "FILTERED_BACKPACK_TIMESTAMP"),
            # Hyperliquid Exchange headers
            ("X-HL-Agent", "FILTERED_HYPERLIQUID_AGENT"),
            ("X-HL-Signature", "FILTERED_HYPERLIQUID_SIGNATURE"),
            # Common exchange headers that might contain sensitive data
            ("X-Nonce", "FILTERED_NONCE"),
            ("X-Request-Id", "FILTERED_REQUEST_ID"),
            ("X-Client-Id", "FILTERED_CLIENT_ID"),
            # Session and tracking headers
            ("X-Session-Id", "FILTERED_SESSION_ID"),
            ("X-Trace-Id", "FILTERED_TRACE_ID"),
            ("X-Correlation-Id", "FILTERED_CORRELATION_ID"),
        ],
        "filter_query_parameters": [
            # ===== AUTHENTICATION PARAMETERS =====
            ("api_key", "FILTERED_QUERY_API_KEY"),
            ("apikey", "FILTERED_QUERY_API_KEY"),
            ("key", "FILTERED_QUERY_KEY"),
            ("token", "FILTERED_QUERY_TOKEN"),
            ("auth", "FILTERED_QUERY_AUTH"),
            ("authorization", "FILTERED_QUERY_AUTHORIZATION"),
            # ===== SIGNATURE PARAMETERS =====
            ("signature", "FILTERED_QUERY_SIGNATURE"),
            ("sig", "FILTERED_QUERY_SIGNATURE"),
            ("sign", "FILTERED_QUERY_SIGNATURE"),
            ("hmac", "FILTERED_QUERY_HMAC"),
            # ===== TIMESTAMP PARAMETERS =====
            ("timestamp", "FILTERED_QUERY_TIMESTAMP"),
            ("ts", "FILTERED_QUERY_TIMESTAMP"),
            ("time", "FILTERED_QUERY_TIMESTAMP"),
            ("nonce", "FILTERED_QUERY_NONCE"),
            # ===== SESSION PARAMETERS =====
            ("session", "FILTERED_QUERY_SESSION"),
            ("session_id", "FILTERED_QUERY_SESSION_ID"),
            ("request_id", "FILTERED_QUERY_REQUEST_ID"),
            # ===== USER IDENTIFICATION =====
            ("user_id", "FILTERED_QUERY_USER_ID"),
            ("client_id", "FILTERED_QUERY_CLIENT_ID"),
            ("wallet", "FILTERED_QUERY_WALLET"),
            ("address", "FILTERED_QUERY_ADDRESS"),
        ],
        "filter_post_data_parameters": [
            # ===== POST BODY PARAMETERS =====
            # Same patterns as query parameters but for POST body
            ("api_key", "FILTERED_POST_API_KEY"),
            ("signature", "FILTERED_POST_SIGNATURE"),
            ("timestamp", "FILTERED_POST_TIMESTAMP"),
            ("private_key", "FILTERED_POST_PRIVATE_KEY"),
            ("secret", "FILTERED_POST_SECRET"),
            ("password", "FILTERED_POST_PASSWORD"),
            ("passphrase", "FILTERED_POST_PASSPHRASE"),
            ("mnemonic", "FILTERED_POST_MNEMONIC"),
            ("seed", "FILTERED_POST_SEED"),
        ],
        # ===== CUSTOM FILTERS =====
        "before_record_request": filter_request_body,
        "before_record_response": filter_response_body,
        # ===== MATCHING CONFIGURATION =====
        # Match on method, URI components, but NOT on filtered query params
        "match_on": ["method", "scheme", "host", "port", "path"],
        # ===== CASSETTE CONFIGURATION =====
        "cassette_library_dir": "tests/cassettes",
        # Record mode can be controlled via environment variable
        # - 'once': Record if cassette doesn't exist, otherwise replay (default)
        # - 'new_episodes': Record new interactions, replay existing ones
        # - 'all': Always record (overwrite cassettes)
        # - 'none': Never record, only replay (fail if cassette missing)
        "record_mode": os.environ.get("VCR_RECORD_MODE", "once"),
        # ===== RESPONSE PROCESSING =====
        "decode_compressed_response": True,  # Handle gzipped responses
        # ===== SECURITY OPTIONS =====
        # Ignore certain hosts that shouldn't be recorded (if any)
        "ignore_hosts": [],
        # Ignore localhost/development endpoints that might contain secrets
        "ignore_localhost": True,
    }


@pytest.fixture
def custom_vcr_config(vcr_config: dict[str, Any], request: pytest.FixtureRequest) -> dict[str, Any]:
    """VCR configuration with custom cassette path based on exchange and endpoint type.

    This fixture allows tests to organize cassettes in subdirectories by exchange and endpoint type.
    Usage in tests:
        @pytest.mark.parametrize("custom_vcr_cassette_dir",
                                 ["api/hyperliquid/public"], indirect=True)
        @pytest.mark.vcr
        async def test_something(custom_vcr_config):
            ...
    """
    # Make a copy of the base config
    config = vcr_config.copy()

    # Check if a custom cassette directory was specified via parametrize
    if hasattr(request, "param") and isinstance(request.param, str):
        # Construct the full cassette path
        base_dir = Path("tests/cassettes")
        custom_dir = base_dir / request.param
        config["cassette_library_dir"] = str(custom_dir)

        # Ensure the directory exists
        custom_dir.mkdir(parents=True, exist_ok=True)

    return config


@pytest.fixture
def vcr_cassette_dir(request: pytest.FixtureRequest) -> str:
    """Fixture to specify custom VCR cassette directory for a test.

    Use with pytest.mark.parametrize to organize cassettes by exchange/endpoint:
        @pytest.mark.parametrize("vcr_cassette_dir", ["api/backpack/public"], indirect=True)
        @pytest.mark.vcr
        async def test_backpack_public_endpoint(vcr_cassette_dir):
            ...
    """
    if hasattr(request, "param"):
        # Create the full path
        base_dir = Path("tests/cassettes")
        custom_dir = base_dir / request.param
        # Ensure directory exists
        custom_dir.mkdir(parents=True, exist_ok=True)
        return str(custom_dir)
    return "tests/cassettes"  # Default
