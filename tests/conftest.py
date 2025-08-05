"""Test configuration and fixtures for CyberDeltaEngine test suite.

This module provides pytest fixtures and configuration for testing the CyberDelta trading engine.
The fixtures are organized into separate modules for better maintainability:

- fixtures.http_mocks: HTTP mocking utilities for API testing
- fixtures.config_fixtures: Configuration and settings fixtures
- fixtures.exchange_mocks: Exchange API mocks and trading data fixtures
- fixtures.vcr_config: VCR configuration for cassette-based testing
- fixtures.time_fixtures: Time control and mocking fixtures
"""

from __future__ import annotations

# Import all fixtures from the organized modules
# This makes all fixtures available to tests as if they were defined in this file
from tests.fixtures.config_fixtures import (
    active_bp_config,
    active_bp_secrets,
    active_hl_config,
    active_hl_secrets,
    backpack_config,
    backpack_secrets,
    # circuit_breaker_system,  # Removed - validation module deleted
    hl_test_environment,
    hl_test_environment_from_config,
    hyperliquid_config,
    hyperliquid_secrets,
    mock_config,
    mock_get_config,
    mock_secrets_manager_with_missing,
    test_app_settings,
    test_config_file_path,
    test_secrets_config,
    test_secrets_file_path,
)
from tests.fixtures.exchange_mocks import (
    # mock_arbitrage_opportunity,  # Removed - validation module deleted
    mock_data_handler,
    mock_exchange_api,
    mock_portfolio_state_manager,
)
from tests.fixtures.http_mocks import (
    MockClientSession,
    MockResponse,
    create_mock_response,
    mock_client_session,
    mock_request,
)
from tests.fixtures.symbol_fixtures import (
    any_exchange,
    backpack_symbol,
    btc_arbitrage_pair,
    btc_perp_any_exchange,
    btc_perp_bp,
    btc_perp_hl,
    btc_spot_bp,
    btc_spot_hl,
    common_test_symbols,
    eth_arbitrage_pair,
    eth_perp_any_exchange,
    eth_perp_bp,
    eth_perp_hl,
    exchange_symbol,
    hyperliquid_symbol,
    invalid_symbol_empty,
    invalid_symbol_long,
    invalid_symbol_special_chars,
)
from tests.fixtures.time_fixtures import (
    FreezerProtocol,
    frozen_time,
    market_time_simulation,
    mock_time_factory,
    mock_time_patch,
    rate_limit_timer,
)
from tests.fixtures.vcr_config import (
    custom_vcr_config,
    vcr_cassette_dir,
    vcr_config,
)


# Re-export all imported fixtures so they can be discovered by pytest
__all__ = [
    # Time fixtures
    "FreezerProtocol",
    # HTTP mocks
    "MockClientSession",
    "MockResponse",
    # Configuration fixtures
    "active_bp_config",
    "active_bp_secrets",
    "active_hl_config",
    "active_hl_secrets",
    "backpack_config",
    "backpack_secrets",
    "create_mock_response",
    # VCR configuration
    "custom_vcr_config",
    "frozen_time",
    "hl_test_environment",
    "hl_test_environment_from_config",
    "hyperliquid_config",
    "hyperliquid_secrets",
    "market_time_simulation",
    # Exchange mocks
    "mock_client_session",
    "mock_config",
    "mock_data_handler",
    "mock_exchange_api",
    "mock_get_config",
    "mock_portfolio_state_manager",  # Note: provides PortfolioStateManager mock, not legacy PortfolioStateManager
    "mock_request",
    "mock_secrets_manager_with_missing",
    "mock_time_factory",
    "mock_time_patch",
    "rate_limit_timer",
    "test_app_settings",
    "test_config_file_path",
    "test_secrets_config",
    "test_secrets_file_path",
    "vcr_cassette_dir",
    "vcr_config",
    # Symbol fixtures
    "btc_perp_hl",
    "btc_perp_bp",
    "eth_perp_hl",
    "eth_perp_bp",
    "btc_spot_hl",
    "btc_spot_bp",
    "exchange_symbol",
    "any_exchange",
    "btc_perp_any_exchange",
    "eth_perp_any_exchange",
    "invalid_symbol_long",
    "invalid_symbol_empty",
    "invalid_symbol_special_chars",
    "btc_arbitrage_pair",
    "eth_arbitrage_pair",
    "common_test_symbols",
    "hyperliquid_symbol",
    "backpack_symbol",
]
