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
    hl_test_environment,
    hl_test_environment_from_config,
    test_app_settings,
    test_config_file_path,
    test_secrets_config,
    test_secrets_file_path,
)
from tests.fixtures.time_fixtures import (
    frozen_time,
    market_time_simulation,
    mock_time_factory,
    mock_time_patch,
    rate_limit_timer,
)


# Re-export all imported fixtures so they can be discovered by pytest
__all__: list[str] = [
    "active_bp_config",
    "active_bp_secrets",
    "active_hl_config",
    "active_hl_secrets",
    "frozen_time",
    "hl_test_environment",
    "hl_test_environment_from_config",
    "market_time_simulation",
    "mock_time_factory",
    "mock_time_patch",
    "rate_limit_timer",
    "test_app_settings",
    "test_config_file_path",
    "test_secrets_config",
    "test_secrets_file_path",
]
