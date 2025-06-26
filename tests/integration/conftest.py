"""Integration test fixtures and helpers for CyberDeltaEngine.

Provides shared fixtures for integration testing including mock exchange APIs,
core component instances, and test data helpers. These fixtures support
end-to-end testing of the trading engine components working together.
"""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import create_autospec, patch


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
import pytest_asyncio
from pydantic import AnyUrl, HttpUrl

from cyberdelta.config import AppSettings
from cyberdelta.config.models.config_models import PortfolioTrackerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import SpotBalance, Ticker
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import (
    FundingRateValidatorProtocol,
    PortfolioTrackerProtocol,
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem
from tests.integration.mocks.mock_exchange import MockExchangeAPI


logger = get_logger(__name__)

# --- Integration Test Specific Helpers & Fixtures ---


# Moved from test_core_workflow.py
def create_mock_ticker(
    symbol: str,
    bid: str | float | Decimal,  # Allow various inputs
    ask: str | float | Decimal,
    price: str | float | Decimal,
    timestamp: datetime,  # Expect datetime object
) -> Ticker:  # Return Ticker object
    """Create a Ticker object with Decimal conversion."""
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=timestamp,  # Pass datetime directly
    )


@pytest.fixture(scope="function")
def basic_opportunity() -> ArbitrageOpportunity:
    """Provide a basic ArbitrageOpportunity instance for integration tests."""
    # Note: basis_volatility is set after creation currently, which is fine.
    # Ensure all required fields are present.
    opp = ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="backpack",  # Use real exchange name
        short_exchange="hyperliquid",  # Use real exchange name
        long_price=Decimal(30001),  # Already correct
        short_price=Decimal(30010),  # Already correct
        long_funding_rate=Decimal("0.0001"),  # Already correct
        short_funding_rate=Decimal("-0.00005"),  # Already correct
        net_funding_differential=Decimal("0.00015"),  # Already correct
        timestamp=datetime.now(UTC),  # Already correct
        # Add missing optional args if needed, or ensure they are None
        basis_volatility=0.01,  # Increased for conservative, safe sizing
        utility_score=None,  # Add optional float
        expected_profit=Decimal("0.01"),  # Set via constructor, not as attribute
    )
    return opp


@pytest.fixture
def mock_pt_config() -> PortfolioTrackerConfig:
    """Create a PortfolioTrackerConfig for testing."""
    return PortfolioTrackerConfig(
        data_freshness_seconds=60,
        initial_balances={},
        initial_positions=[],
    )


@pytest_asyncio.fixture(scope="function")
async def real_portfolio_tracker(
    mock_config: AppSettings,
    mock_pt_config: PortfolioTrackerConfig,
) -> AsyncGenerator[PortfolioTracker]:
    """Provide a real PortfolioTracker instance initialized with mock config."""
    tracker = PortfolioTracker(mock_config, mock_pt_config)
    # DO NOT call await tracker.initialize() here.
    # Initialization should happen in the test or a more specific fixture
    # after API clients are registered.
    yield tracker
    # No specific teardown needed for PortfolioTracker itself unless it holds resources
    # that need explicit async closing beyond what its components (like api_clients) handle.


# Add other integration-specific fixtures here if needed


# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str | None]]:
    """Provide dummy secrets needed by integration mock APIs."""
    return {
        "mock_hl": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


@pytest_asyncio.fixture(scope="function")
async def mock_hl_api(
    mock_config: AppSettings,
    mock_secrets: dict[str, dict[str, str | None]],
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock HyperLiquid API with patched clients."""
    exchange_name = "mock_hl"
    # Create a proper ExchangeSpecificConfig object for the mock
    from cyberdelta.config.models.config_models import (
        AddressActionSafetyNetConfig,
        ExchangeSpecificConfig,
    )
    from cyberdelta.enums.exchange_names import ExchangeName

    exchange_config = ExchangeSpecificConfig(
        exchange_name=ExchangeName.HYPERLIQUID,
        enabled=True,
        api_base_url_mainnet=HttpUrl("http://fixedmock.exchange"),
        ws_url_mainnet=AnyUrl("ws://fixedmock.exchange"),
        is_mainnet_environment=True,
        rate_limit_per_minute=120,
        symbols={"BTC": "BTC", "ETH": "ETH"},
        # Hyperliquid-specific required fields
        ip_weight_limit_per_minute=1200,
        info_request_type_ip_weights={"meta": 1, "allMids": 2},
        default_info_weight=1,
        exchange_action_base_ip_weight=1,
        address_action_safety_net=AddressActionSafetyNetConfig(rate_per_minute=600),
    )

    exchange_secrets = mock_secrets[exchange_name]

    with (
        patch("cyberdelta.apis.connectivity.http_client.HttpClient.__init__", return_value=None),
        patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.__init__",
            return_value=None,
        ),
    ):
        api = MockExchangeAPI(
            exchange_name=exchange_name,
            config=exchange_config,
            secrets=exchange_secrets,
            config_obj=mock_config,
        )
        try:
            yield api
        finally:
            await api.close()


@pytest_asyncio.fixture(scope="function")
async def mock_bp_api(
    mock_config: AppSettings,
    mock_secrets: dict[str, dict[str, str | None]],
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock Backpack API with patched clients."""
    exchange_name = "mock_bp"
    # Create a proper ExchangeSpecificConfig object for the mock
    from cyberdelta.config.models.config_models import ExchangeSpecificConfig
    from cyberdelta.enums.exchange_names import ExchangeName

    exchange_config = ExchangeSpecificConfig(
        exchange_name=ExchangeName.BACKPACK,
        enabled=True,
        api_base_url_mainnet=HttpUrl("http://fixedmock.exchange"),
        ws_url_mainnet=AnyUrl("ws://fixedmock.exchange"),
        is_mainnet_environment=True,
        rate_limit_per_minute=120,
        symbols={"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
    )

    exchange_secrets = mock_secrets[exchange_name]

    with (
        patch(
            "cyberdelta.apis.connectivity.http_client.HttpClient.__init__",
            return_value=None,
        ),
        patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.__init__",
            return_value=None,
        ),
    ):
        api = MockExchangeAPI(
            exchange_name=exchange_name,
            config=exchange_config,
            secrets=exchange_secrets,
            config_obj=mock_config,
        )
        try:
            yield api
        finally:
            await api.close()


# --- Core Component Fixtures ---


@pytest.fixture
def data_handler(
    mock_config: AppSettings,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    symbol_mapper: SymbolMapper,
    real_portfolio_tracker: PortfolioTracker,
) -> DataHandler:
    """Create Data Handler instance with mock APIs registered."""
    from typing import cast

    from cyberdelta.apis.base.exchange_api import ExchangeAPI

    api_clients: dict[str, ExchangeAPI] = cast(
        "dict[str, ExchangeAPI]",
        {
            "hyperliquid": mock_hl_api,
            "backpack": mock_bp_api,
        },
    )
    dh = DataHandler(
        app_settings=mock_config,
        api_clients=api_clients,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper,
    )
    return dh


# Define symbol_mapper fixture
@pytest.fixture
def symbol_mapper(mock_config: AppSettings) -> SymbolMapper:
    """Provide a SymbolMapper instance initialized with mock config."""
    # Convert AppSettings exchanges config to dict format that SymbolMapper expects
    # SymbolMapper expects {exchange_name: {"symbols": {...}}} format, not {"exchanges": {...}}
    config_data_for_mapper: dict[str, Any] = {
        exchange_name: {"symbols": exchange_config.symbols}
        for exchange_name, exchange_config in mock_config.exchanges.items()
    }
    return SymbolMapper(config_data_for_mapper)


@pytest.fixture(scope="function")
def signal_generator(
    mock_config: AppSettings,
    data_handler: DataHandler,
    symbol_mapper: SymbolMapper,
) -> SignalGenerator:
    """Fixture for a SignalGenerator instance with mock data handler."""
    return SignalGenerator(mock_config, data_handler, symbol_mapper)


@pytest.fixture
def risk_manager(
    mock_config: AppSettings,
) -> object:  # Keep as object to avoid circular dependency if RiskManager imports protocols
    """Create Risk Manager instance using protocol-compliant mocks.

    Uses mocks for portfolio tracker and funding rate validator.
    """
    from cyberdelta.core.risk_manager import RiskManager  # Local import

    mock_portfolio_tracker = create_autospec(PortfolioTrackerProtocol, instance=True)
    mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
    mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal(0)
    mock_portfolio_tracker.get_current_drawdown.return_value = Decimal(0)
    mock_spot_balance = SpotBalance(
        exchange="mock_generic",
        asset="USDC",
        total_quantity=Decimal("1000.0"),
        available_quantity=Decimal("1000.0"),
        timestamp=datetime.now(UTC),
    )
    mock_portfolio_tracker.get_exchange_balance.return_value = mock_spot_balance
    mock_funding_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return RiskManager(
        app_settings=mock_config,
        portfolio_tracker=mock_portfolio_tracker,
        funding_rate_validator=mock_funding_validator,
    )


@pytest.fixture
def execution_handler(
    mock_config: AppSettings,
    real_portfolio_tracker: PortfolioTracker,  # Will use the async real_portfolio_tracker
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    circuit_breaker_system: CircuitBreakerSystem,
) -> ExecutionHandler:
    """Create Execution Handler instance with real tracker, mock APIs, and CB system."""
    from cyberdelta.core.execution_handler import ExecutionHandler  # Local import

    # Convert AppSettings exchanges config to dict format that SymbolMapper expects
    # SymbolMapper expects {exchange_name: {"symbols": {...}}} format, not {"exchanges": {...}}
    config_data_for_mapper_eh: dict[str, Any] = {
        exchange_name: {"symbols": exchange_config.symbols}
        for exchange_name, exchange_config in mock_config.exchanges.items()
    }
    symbol_mapper_instance = SymbolMapper(config_data_for_mapper_eh)
    eh = ExecutionHandler(
        app_settings=mock_config,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper_instance,
        circuit_breaker_system=circuit_breaker_system,
    )
    eh.register_api_client("hyperliquid", mock_hl_api)
    eh.register_api_client("backpack", mock_bp_api)
    return eh


# --- Safety System Specific Fixtures ---


@pytest.fixture
def funding_rate_validator() -> FundingRateValidatorProtocol:
    """Provide a protocol-compliant mock for the FundingRateValidator."""
    from typing import cast
    from unittest.mock import create_autospec  # Local import

    mock_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return cast("FundingRateValidatorProtocol", mock_validator)


@pytest_asyncio.fixture(scope="function")  # Changed to async fixture
async def position_reconciler(
    mock_config: AppSettings,
    mock_pt_config: PortfolioTrackerConfig,
    # real_portfolio_tracker: PortfolioTracker, # No longer directly used, will create its own
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
) -> AsyncGenerator[PositionReconciliationSystem]:  # Changed return type
    """Provide a PositionReconciliationSystem instance with mock APIs."""
    # Create a fresh PortfolioTracker for this fixture
    portfolio_tracker = PortfolioTracker(mock_config, mock_pt_config)
    await portfolio_tracker.initialize()  # Initialize it
    portfolio_tracker.register_api_client("hyperliquid", mock_hl_api)
    portfolio_tracker.register_api_client("backpack", mock_bp_api)

    reconciler = PositionReconciliationSystem(
        app_settings=mock_config,
        portfolio_tracker=portfolio_tracker,
    )
    try:
        yield reconciler
    finally:
        # Clean up if needed
        pass  # PortfolioTracker doesn't have a shutdown method


@pytest.fixture
def circuit_breaker_system(mock_config: AppSettings) -> CircuitBreakerSystem:
    """Provide a CircuitBreakerSystem instance initialized with mock config."""
    # The mock_config already has safety_systems configured, use it directly
    return CircuitBreakerSystem(mock_config)


# Find opportunity creation/mocking
@pytest.fixture
def mock_opportunity() -> ArbitrageOpportunity:
    """Return mock opportunity for testing."""
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="mock_hl",
        short_exchange="mock_bp",
        long_price=Decimal(30000),  # Already correct
        short_price=Decimal(30050),  # Already correct
        long_funding_rate=Decimal("0.0001"),  # Already correct
        short_funding_rate=Decimal("-0.0001"),  # Already correct
        net_funding_differential=Decimal("0.0002"),  # Already correct
        timestamp=datetime.now(UTC),  # Already correct
        expected_profit=Decimal("5.0"),  # Already correct
        # Add missing optional args
        basis_volatility=0.002,  # Example float value
        utility_score=0.6,  # Example float value
    )


# --- VCR Configuration Override for Integration Tests ---


@pytest.fixture
def vcr_config() -> dict[str, Any]:
    """VCR configuration for integration tests without calling base fixture directly.

    Provides complete VCR configuration with environment variable support for
    organized cassette directory structure. This avoids calling fixtures directly
    which is deprecated in pytest.
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

    # Base VCR configuration (copied from tests.fixtures.vcr_config to avoid fixture calling)
    config = {
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

    return config


@pytest.fixture
def custom_vcr_cassette_dir(request: PytestRequest) -> str:
    """Fixture to specify custom VCR cassette directory for integration tests.

    Use with pytest.mark.parametrize to organize cassettes by exchange/endpoint:
        @pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
        @pytest.mark.vcr
        async def test_backpack_public_endpoint(custom_vcr_cassette_dir):
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


@pytest.fixture
def custom_vcr_config(vcr_config: dict[str, Any], custom_vcr_cassette_dir: str) -> dict[str, Any]:
    """VCR configuration with custom cassette path for integration tests.

    This fixture uses the custom_vcr_cassette_dir to set the cassette directory.
    """
    # Make a copy of the base config
    config = vcr_config.copy()

    # Override cassette directory with the custom one
    config["cassette_library_dir"] = custom_vcr_cassette_dir

    return config


def _get_custom_cassette_dir(request: PytestRequest) -> str | None:
    """Extract custom VCR cassette directory from test parametrization."""
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
    """Get module-based cassette directory from test file path."""
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
    """
    # Check for custom parametrized cassette directory
    custom_dir = _get_custom_cassette_dir(request)
    if custom_dir:
        return custom_dir

    # Fall back to module-based directory structure
    return _get_module_based_cassette_dir(request)
