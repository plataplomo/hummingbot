"""Integration test fixtures and helpers for CyberDeltaEngine.

Provides shared fixtures for integration testing including mock exchange APIs,
core component instances, and test data helpers. These fixtures support
end-to-end testing of the trading engine components working together.
"""

import os
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from typing import Protocol

    class PytestMarker(Protocol):
        """Protocol for pytest marker objects."""

        args: tuple[Any, ...]

    class PytestCallSpec(Protocol):
        """Protocol for pytest callspec objects."""

        params: dict[str, Any]

    class PytestNode(Protocol):
        """Protocol for pytest node objects."""

        name: str
        fspath: str | Any  # pytest uses py.path.local which isn't well typed
        callspec: PytestCallSpec | None

        def get_closest_marker(self, name: str) -> PytestMarker | None:
            """Get the closest marker with the given name."""
            ...

    class PytestRequest(Protocol):
        """Protocol for pytest request fixture."""

        node: PytestNode
        param: Any  # For parametrized fixtures

    class VCRRequest(Protocol):
        """Protocol for VCR request objects."""

        body: Any

    class VCRResponse(Protocol):
        """Protocol for VCR response objects."""

        body: Any

else:
    PytestNode = Any
    PytestMarker = Any
    PytestCallSpec = Any
    PytestRequest = Any
    VCRRequest = Any
    VCRResponse = Any

import pytest

from cyberdelta.config import AppSettings

# PortfolioTrackerConfig removed - using AppSettings portfolio_tracker section instead
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import get_symbol_service
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.models import Ticker


logger = get_logger(__name__)


# --- Integration Test Specific Helpers & Fixtures ---
# Moved from test_core_workflow.py
def create_mock_ticker(
    symbol: str,
    bid: str | float | Decimal,  # Allow various inputs
    ask: str | float | Decimal,
    price: str | float | Decimal,
    timestamp: datetime,  # Expect datetime object
    exchange: str = "test_exchange",  # Default for integration tests
) -> Ticker:  # Return Ticker object
    """Create a Ticker object with Decimal conversion.

    Returns:
        Ticker: A Ticker object with converted Decimal values.
    """
    return Ticker(
        symbol=symbol,
        exchange=exchange,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=timestamp,  # Pass datetime directly
    )


# Add other integration-specific fixtures here if needed
# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str | None]]:
    """Provide dummy secrets needed by integration mock APIs.

    Returns:
        dict[str, dict[str, str | None]]: Mock secrets configuration.
    """
    return {
        "mock_hl": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


# --- Core Component Fixtures ---
# Define symbol_mapper fixture
@pytest.fixture
def symbol_mapper(mock_config: AppSettings) -> SymbolService:
    """Provide a SymbolService instance initialized with mock config.

    Returns:
        SymbolService: Symbol service instance for testing.
    """
    # Get the global symbol service
    # In real usage, the service is initialized with the global registry
    return get_symbol_service()


# --- Safety System Specific Fixtures ---
# --- VCR Configuration Override for Integration Tests ---
@pytest.fixture
def vcr_config() -> dict[str, Any]:
    """VCR configuration for integration tests without calling base fixture directly.

    Provides complete VCR configuration with environment variable support for
    organized cassette directory structure. This avoids calling fixtures directly
    which is deprecated in pytest.

    Returns:
        dict[str, Any]: Complete VCR configuration for integration tests.
    """

    def filter_request_body(request: VCRRequest) -> VCRRequest:
        """Filter and sanitize request body content for VCR cassette recording.

        Returns:
            VCRRequest: Filtered request with sanitized body content.
        """
        if hasattr(request, "body") and getattr(request, "body", None):
            # Filter known sensitive patterns in request bodies
            request_body: Any = request.body
            body_str = (
                request_body.decode("utf-8")
                if isinstance(request_body, bytes)
                else str(request_body)
            )

            # Replace common sensitive patterns

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
        """Filter and sanitize response body content for VCR cassette recording.

        Returns:
            VCRResponse: Filtered response with sanitized body content.
        """
        if hasattr(response, "body") and getattr(response, "body", None):
            # For now, we don't filter response bodies as they typically don't contain
            # user credentials, but this hook is available for future use
            pass
        return response

    # Base VCR configuration (copied from tests.fixtures.vcr_config to avoid fixture calling)
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
        # NOTE: cassette_library_dir is handled by the vcr_cassette_dir fixture
        # Record mode can be controlled via environment variable
        "record_mode": os.environ.get("VCR_RECORD_MODE", "once"),
        # ===== RESPONSE PROCESSING =====
        "decode_compressed_response": True,  # Handle gzipped responses
        # ===== SECURITY OPTIONS =====
        "ignore_hosts": [],
        "ignore_localhost": True,
    }

    # NOTE: cassette_library_dir is now managed by the vcr_cassette_dir fixture
    # which handles organized directory structure based on test parametrization


@pytest.fixture
def custom_vcr_cassette_dir(request: PytestRequest) -> str:
    """Fixture to specify custom VCR cassette directory for integration tests.

    Use with pytest.mark.parametrize to organize cassettes by exchange/endpoint:
        @pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
        @pytest.mark.vcr
        async def test_backpack_public_endpoint(custom_vcr_cassette_dir):
            ...

    Returns:
        str: Custom cassette directory path for VCR recordings.
    """
    if hasattr(request, "param"):
        # Create the full path
        base_dir = Path("tests/cassettes")
        custom_dir = base_dir / request.param
        # Ensure directory exists
        custom_dir.mkdir(parents=True, exist_ok=True)
        return str(custom_dir)
    return "tests/cassettes"  # Default


@pytest.fixture
def custom_vcr_config(vcr_config: dict[str, Any], custom_vcr_cassette_dir: str) -> dict[str, Any]:
    """VCR configuration with custom cassette path for integration tests.

    This fixture uses the custom_vcr_cassette_dir to set the cassette directory.

    Returns:
        dict[str, Any]: VCR configuration with custom cassette directory.
    """
    # Make a copy of the base config
    config = vcr_config.copy()

    # Override cassette directory with the custom one
    config["cassette_library_dir"] = custom_vcr_cassette_dir

    return config


def _get_custom_cassette_dir(request: PytestRequest) -> str | None:
    """Extract custom VCR cassette directory from test parametrization.

    Returns:
        str | None: Custom cassette directory if parametrized, None otherwise.
    """
    if not (hasattr(request, "node") and hasattr(request.node, "callspec")):
        return None

    callspec = getattr(request.node, "callspec", None)
    if not (callspec and hasattr(callspec, "params")):
        return None

    if "custom_vcr_cassette_dir" not in callspec.params:
        return None

    base_dir = Path("tests/cassettes")
    param_value = callspec.params["custom_vcr_cassette_dir"]
    custom_dir = base_dir / str(param_value)
    custom_dir.mkdir(parents=True, exist_ok=True)
    return str(custom_dir)


def _get_module_based_cassette_dir(request: PytestRequest) -> str:
    """Get module-based cassette directory from test file path.

    Returns:
        str: Module-based cassette directory path.
    """
    # Ensure node and fspath exist
    if not (hasattr(request, "node") and hasattr(request.node, "fspath")):
        return "tests/cassettes"

    node_fspath = getattr(request.node, "fspath", None)
    if node_fspath is None:
        return "tests/cassettes"

    # Convert to string path regardless of pytest's internal type
    node_path = str(node_fspath)
    module_path = Path(node_path)

    try:
        module_relative_path = module_path.relative_to(Path("tests"))
        cassettes_dir = (
            Path("tests/cassettes") / module_relative_path.parent / module_relative_path.stem
        )
        cassettes_dir.mkdir(parents=True, exist_ok=True)
        return str(cassettes_dir)
    except ValueError:
        # If path is not relative to tests/, fall back to default
        return "tests/cassettes"


@pytest.fixture
def vcr_cassette_dir(request: PytestRequest) -> str:
    """Override pytest-recording's default cassette directory logic.

    This fixture is automatically used by pytest-recording to determine where
    to save cassette files. We override it to use organized subdirectories
    based on the test parametrization.

    Returns:
        str: VCR cassette directory path for test recordings.
    """
    # Check for custom parametrized cassette directory
    custom_dir = _get_custom_cassette_dir(request)
    if custom_dir:
        return custom_dir

    # Fall back to module-based directory structure
    return _get_module_based_cassette_dir(request)
