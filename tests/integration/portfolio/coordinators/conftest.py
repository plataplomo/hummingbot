"""Test fixtures for portfolio coordinator integration tests.

Provides real service factories and API instances for integration testing.
No mocking of critical financial operations per TESTING_SECURITY_RULES.md.
"""
import pytest
from typing import AsyncGenerator

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.services.portfolio_service_factory import PortfolioServiceFactory
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import HyperliquidSecrets, BackpackSecrets
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.environments import EnvironmentType


@pytest.fixture
async def portfolio_service_factory() -> AsyncGenerator[PortfolioServiceFactory, None]:
    """Provide real portfolio service factory for integration tests."""
    app_settings = AppSettings()
    factory = PortfolioServiceFactory(app_settings)
    
    # Initialize all services
    await factory.initialize_all()
    
    yield factory
    
    # Cleanup
    await factory.shutdown_all()


@pytest.fixture
async def risk_service_factory() -> AsyncGenerator[RiskServiceFactory, None]:
    """Provide real risk service factory for integration tests."""
    app_settings = AppSettings()
    factory = RiskServiceFactory(app_settings)
    
    yield factory


@pytest.fixture
def hyperliquid_testnet_config() -> ExchangeSpecificConfig:
    """Provide Hyperliquid testnet configuration."""
    return ExchangeSpecificConfig(
        exchange_name=ExchangeName.HYPERLIQUID,
        environment_type=EnvironmentType.TESTNET,
        api_base_url_mainnet="https://api.hyperliquid.xyz",
        api_base_url_testnet="https://api.hyperliquid-testnet.xyz",
        ws_url_mainnet="wss://api.hyperliquid.xyz/ws",
        ws_url_testnet="wss://api.hyperliquid-testnet.xyz/ws",
        chain_id=421614,  # Arbitrum Sepolia testnet
        rate_limit_per_minute=1200,
        request_timeout_seconds=30,
        max_retries=3,
        retry_delay_seconds=1,
        ws_ping_interval_seconds=30,
        ws_reconnect_delay_seconds=5,
        ws_max_reconnect_attempts=5,
        ws_connection_timeout_seconds=30,
    )


@pytest.fixture
def backpack_testnet_config() -> ExchangeSpecificConfig:
    """Provide Backpack testnet configuration."""
    return ExchangeSpecificConfig(
        exchange_name=ExchangeName.BACKPACK,
        environment_type=EnvironmentType.TESTNET,
        api_base_url_mainnet="https://api.backpack.exchange",
        api_base_url_testnet="https://api.backpack.exchange",  # Same as mainnet
        ws_url_mainnet="wss://api.backpack.exchange/ws",
        ws_url_testnet="wss://api.backpack.exchange/ws",
        rate_limit_per_minute=600,
        request_timeout_seconds=30,
        max_retries=3,
        retry_delay_seconds=1,
        ws_ping_interval_seconds=30,
        ws_reconnect_delay_seconds=5,
        ws_max_reconnect_attempts=5,
        ws_connection_timeout_seconds=30,
    )


@pytest.fixture
def hyperliquid_test_secrets() -> HyperliquidSecrets:
    """Provide test Hyperliquid secrets.
    
    Note: In real integration tests, these would come from secure environment
    variables or secret management system. Never hardcode real secrets.
    """
    import os
    
    # Get from environment or use test values
    private_key = os.getenv("HYPERLIQUID_TEST_PRIVATE_KEY", "")
    if not private_key:
        pytest.skip("HYPERLIQUID_TEST_PRIVATE_KEY not set for integration testing")
    
    return HyperliquidSecrets(private_key=private_key)


@pytest.fixture
def backpack_test_secrets() -> BackpackSecrets:
    """Provide test Backpack secrets.
    
    Note: In real integration tests, these would come from secure environment
    variables or secret management system. Never hardcode real secrets.
    """
    import os
    
    # Get from environment or use test values
    api_key = os.getenv("BACKPACK_TEST_API_KEY", "")
    api_secret = os.getenv("BACKPACK_TEST_API_SECRET", "")
    
    if not api_key or not api_secret:
        pytest.skip("BACKPACK_TEST_API_KEY/SECRET not set for integration testing")
    
    return BackpackSecrets(
        api_key=api_key,
        api_secret=api_secret,
    )


@pytest.fixture
async def hyperliquid_api(
    hyperliquid_testnet_config: ExchangeSpecificConfig,
    hyperliquid_test_secrets: HyperliquidSecrets,
) -> AsyncGenerator[HyperliquidAPI, None]:
    """Provide real Hyperliquid API instance for integration tests."""
    api = HyperliquidAPI(
        exchange_config=hyperliquid_testnet_config,
        exchange_secrets=hyperliquid_test_secrets,
    )
    
    yield api
    
    # Cleanup
    await api.close()


@pytest.fixture
async def backpack_api(
    backpack_testnet_config: ExchangeSpecificConfig,
    backpack_test_secrets: BackpackSecrets,
) -> AsyncGenerator[BackpackAPI, None]:
    """Provide real Backpack API instance for integration tests."""
    api = BackpackAPI(
        exchange_config=backpack_testnet_config,
        exchange_secrets=backpack_test_secrets,
    )
    
    yield api
    
    # Cleanup
    await api.close()