"""Test configuration and fixtures for CyberDeltaEngine test suite.

This module provides pytest fixtures and configuration for testing the CyberDelta trading engine.
The fixtures are organized into separate modules for better maintainability:

- fixtures.http_mocks: HTTP mocking utilities for API testing
- fixtures.config_fixtures: Configuration and settings fixtures
- fixtures.exchange_mocks: Exchange API mocks and trading data fixtures
- fixtures.vcr_config: VCR configuration for cassette-based testing
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
    circuit_breaker_system,
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
    mock_arbitrage_opportunity,
    mock_data_handler,
    mock_exchange_api,
    mock_portfolio_tracker,
)
from tests.fixtures.http_mocks import (
    MockClientSession,
    MockResponse,
    create_mock_response,
    mock_client_session,
    mock_request,
)
from tests.fixtures.vcr_config import (
    custom_vcr_config,
    vcr_cassette_dir,
    vcr_config,
)

# Re-export all imported fixtures so they can be discovered by pytest
__all__ = [
    # HTTP mocks
    "MockClientSession",
    "MockResponse",
    "create_mock_response",
    "mock_client_session",
    "mock_request",
    # Configuration fixtures
    "active_bp_config",
    "active_bp_secrets",
    "active_hl_config",
    "active_hl_secrets",
    "backpack_config",
    "backpack_secrets",
    "circuit_breaker_system",
    "hl_test_environment",
    "hl_test_environment_from_config",
    "hyperliquid_config",
    "hyperliquid_secrets",
    "mock_config",
    "mock_get_config",
    "mock_secrets_manager_with_missing",
    "test_app_settings",
    "test_config_file_path",
    "test_secrets_config",
    "test_secrets_file_path",
    # Exchange mocks
    "mock_arbitrage_opportunity",
    "mock_data_handler",
    "mock_exchange_api",
    "mock_portfolio_tracker",
    # VCR configuration
    "custom_vcr_config",
    "vcr_cassette_dir",
    "vcr_config",
]
