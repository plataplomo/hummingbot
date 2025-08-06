"""Builder fixtures for Symbol testing.

These fixtures provide builder instances and helper functions
for constructing complex test scenarios.
"""

from collections.abc import Callable
from typing import Any

import pytest

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols.models import Symbol
from tests.builders.symbol_builders import (
    ArbitrageSymbolBuilder,
    MarketDataSymbolBuilder,
    TradingSymbolBuilder,
)
from tests.factories.symbol_test_factory import (
    ComponentsTestFactory,
    MetadataTestFactory,
    SymbolTestFactory,
)
from tests.mocks.symbol_mocks import (
    MockExchangeHandler,
    MockSymbolRegistry,
    MockSymbolService,
)


@pytest.fixture
def symbol_builder() -> ArbitrageSymbolBuilder:
    """Flexible symbol builder for arbitrage tests.

    Returns:
        ArbitrageSymbolBuilder: Builder instance
    """
    return ArbitrageSymbolBuilder()


@pytest.fixture
def market_data_builder() -> MarketDataSymbolBuilder:
    """Market data scenario builder.

    Returns:
        MarketDataSymbolBuilder: Builder instance
    """
    return MarketDataSymbolBuilder()


@pytest.fixture
def trading_builder() -> TradingSymbolBuilder:
    """Trading scenario builder.

    Returns:
        TradingSymbolBuilder: Builder instance
    """
    return TradingSymbolBuilder()


@pytest.fixture
def metadata_builder() -> MetadataTestFactory:
    """Metadata builder for custom scenarios.

    Returns:
        MetadataTestFactory: Factory instance
    """
    return MetadataTestFactory()


@pytest.fixture
def components_builder() -> ComponentsTestFactory:
    """Components builder for testing.

    Returns:
        ComponentsTestFactory: Factory instance
    """
    return ComponentsTestFactory()


@pytest.fixture
def mock_symbol_service() -> MockSymbolService:
    """Configurable mock symbol service.

    Returns:
        MockSymbolService: Mock builder instance
    """
    return MockSymbolService()


@pytest.fixture
def mock_exchange_handler_factory() -> Callable[[str], MockExchangeHandler]:
    """Factory for creating mock exchange handlers.

    Returns:
        Callable: Function that creates mock handlers
    """

    def create_handler(exchange_name: str) -> MockExchangeHandler:
        exchange = ExchangeName[exchange_name.upper()]
        return MockExchangeHandler(exchange)

    return create_handler


@pytest.fixture
def mock_symbol_registry() -> MockSymbolRegistry:
    """Configurable mock symbol registry.

    Returns:
        MockSymbolRegistry: Mock builder instance
    """
    return MockSymbolRegistry()


@pytest.fixture
def symbol_test_factory() -> SymbolTestFactory:
    """Enhanced symbol test factory.

    Returns:
        SymbolTestFactory: Factory instance
    """
    return SymbolTestFactory()


@pytest.fixture
def create_test_portfolio() -> Callable[
    [list[str], bool], dict[str, dict[str, tuple[Symbol, Symbol]]]
]:
    """Factory function for creating test portfolios.

    Returns:
        Callable: Function that creates test portfolio
    """

    def _create(
        assets: list[str], include_spot: bool = False
    ) -> dict[str, dict[str, tuple[Symbol, Symbol]]]:
        return SymbolTestFactory.create_arbitrage_set(
            assets=assets,
            include_spot=include_spot,
        )

    return _create


@pytest.fixture
def create_invalid_symbols() -> Callable[[], dict[str, Any]]:
    """Factory function for creating invalid test cases.

    Returns:
        Callable: Function that returns invalid test cases
    """

    def _create() -> dict[str, Any]:
        return SymbolTestFactory.create_invalid_test_cases()

    return _create


@pytest.fixture
def standard_metadata_sets() -> dict[str, Any]:
    """Standard metadata sets for testing.

    Returns:
        dict: Asset to exchange to metadata mapping
    """
    return MetadataTestFactory.create_test_metadata_sets()


@pytest.fixture
def standard_components() -> dict[str, Any]:
    """Standard component sets for testing.

    Returns:
        dict: Component name to SymbolComponents mapping
    """
    return ComponentsTestFactory.create_test_component_sets()
