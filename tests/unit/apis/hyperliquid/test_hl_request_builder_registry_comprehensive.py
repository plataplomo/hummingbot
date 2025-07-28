"""Comprehensive unit tests for HyperliquidRequestBuilderRegistry.

Tests registry functionality for managing request builders,
focusing only on public API behavior with comprehensive success, edge, and failure cases.
"""

from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.request_builders.hl_request_builder_registry import (
    HyperliquidRequestBuilderRegistry,
    IRequestBuilder,
)


class TestHyperliquidRequestBuilderRegistry:
    """Test HyperliquidRequestBuilderRegistry public API functionality."""

    @pytest.fixture
    def registry(self) -> HyperliquidRequestBuilderRegistry:
        """Create a fresh registry instance for testing.
        
        Returns:
            HyperliquidRequestBuilderRegistry: A fresh registry instance for testing.
        """
        return HyperliquidRequestBuilderRegistry()

    @pytest.fixture
    def mock_builder(self) -> MagicMock:
        """Create a mock request builder that implements IRequestBuilder protocol.
        
        Returns:
            MagicMock: A mock request builder instance for testing.
        """
        mock = MagicMock(spec=IRequestBuilder)
        mock.build_request.return_value = {"mock": "request"}
        return mock

    def test_registry_initialization(self, registry: HyperliquidRequestBuilderRegistry) -> None:
        """Test that registry initializes correctly."""
        assert registry is not None
        assert isinstance(registry, HyperliquidRequestBuilderRegistry)
        # Test initialization state through public behavior
        assert len(registry) == 0

    def test_initialize_default_builders_success(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test successful initialization of default builders."""
        with (
            patch.object(registry, "_register_trading_builders") as mock_trading,
            patch.object(registry, "_register_account_builders") as mock_account,
            patch.object(registry, "_register_market_data_builders") as mock_market,
        ):
            registry.initialize_default_builders()

            # Verify all registration methods were called
            mock_trading.assert_called_once()
            mock_account.assert_called_once()
            mock_market.assert_called_once()

    def test_initialize_default_builders_already_initialized(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test that re-initialization is prevented and logged."""
        # First initialization
        with patch.object(registry, "_register_trading_builders"):
            registry.initialize_default_builders()

        # Second attempt should be prevented
        with patch.object(registry, "_register_trading_builders") as mock_trading:
            registry.initialize_default_builders()

            # Should not be called again
            mock_trading.assert_not_called()

    def test_register_trading_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test registering a trading request builder."""
        operation = "place_order"

        registry.register_trading_builder(operation, mock_builder)

        # Verify registration
        assert registry.is_registered(f"trading.{operation}")
        retrieved_builder = registry.get(f"trading.{operation}")
        assert retrieved_builder is mock_builder

    def test_register_account_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test registering an account request builder."""
        operation = "get_balance"

        registry.register_account_builder(operation, mock_builder)

        # Verify registration
        assert registry.is_registered(f"account.{operation}")
        retrieved_builder = registry.get(f"account.{operation}")
        assert retrieved_builder is mock_builder

    def test_register_market_data_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test registering a market data request builder."""
        operation = "get_ticker"

        registry.register_market_data_builder(operation, mock_builder)

        # Verify registration
        assert registry.is_registered(f"market_data.{operation}")
        retrieved_builder = registry.get(f"market_data.{operation}")
        assert retrieved_builder is mock_builder

    def test_get_trading_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test retrieving a trading request builder."""
        operation = "cancel_order"
        registry.register_trading_builder(operation, mock_builder)

        retrieved_builder = registry.get_trading_builder(operation)

        assert retrieved_builder is mock_builder

    def test_get_account_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test retrieving an account request builder."""
        operation = "get_positions"
        registry.register_account_builder(operation, mock_builder)

        retrieved_builder = registry.get_account_builder(operation)

        assert retrieved_builder is mock_builder

    def test_get_market_data_builder(
        self, registry: HyperliquidRequestBuilderRegistry, mock_builder: MagicMock
    ) -> None:
        """Test retrieving a market data request builder."""
        operation = "get_orderbook"
        registry.register_market_data_builder(operation, mock_builder)

        retrieved_builder = registry.get_market_data_builder(operation)

        assert retrieved_builder is mock_builder

    @pytest.mark.parametrize(
        ("domain", "operations"),
        [
            ("trading", ["place_order", "cancel_order", "get_order"]),
            ("account", ["get_balance", "get_positions", "get_trades"]),
            ("market_data", ["get_ticker", "get_orderbook", "get_candles"]),
        ],
    )
    def test_get_builders_by_domain(
        self, registry: HyperliquidRequestBuilderRegistry, domain: str, operations: list[str]
    ) -> None:
        """Test retrieving builders by domain."""
        builders = {}

        # Register builders for the domain
        for operation in operations:
            mock_builder = MagicMock(spec=IRequestBuilder)
            getattr(registry, f"register_{domain}_builder")(operation, mock_builder)
            builders[operation] = mock_builder

        # Retrieve builders by domain
        domain_builders = registry.get_builders_by_domain(domain)

        # Verify all builders are returned
        assert len(domain_builders) == len(operations)
        for operation in operations:
            assert operation in domain_builders
            assert domain_builders[operation] is builders[operation]

    def test_get_builders_by_domain_empty(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test retrieving builders by domain when none are registered."""
        domain_builders = registry.get_builders_by_domain("trading")

        assert domain_builders == {}

    def test_get_builders_by_domain_mixed_domains(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test that get_builders_by_domain only returns builders for the specified domain."""
        # Register builders in different domains
        trading_builder = MagicMock(spec=IRequestBuilder)
        account_builder = MagicMock(spec=IRequestBuilder)

        registry.register_trading_builder("place_order", trading_builder)
        registry.register_account_builder("get_balance", account_builder)

        # Get only trading builders
        trading_builders = registry.get_builders_by_domain("trading")

        assert len(trading_builders) == 1
        assert "place_order" in trading_builders
        assert trading_builders["place_order"] is trading_builder
        assert "get_balance" not in trading_builders

    def test_validate_registry_all_present(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test registry validation when all expected builders are present."""
        # Register all expected builders
        expected_builders = [
            ("trading", ["place_order", "cancel_order", "get_order", "batch_orders"]),
            ("account", ["get_balance", "get_positions", "get_account_summary"]),
            ("market_data", ["get_ticker", "get_orderbook", "get_trades"]),
        ]

        for domain, operations in expected_builders:
            for operation in operations:
                mock_builder = MagicMock(spec=IRequestBuilder)
                getattr(registry, f"register_{domain}_builder")(operation, mock_builder)

        issues = registry.validate_registry()

        assert issues == []  # No issues should be found

    def test_validate_registry_missing_builders(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test registry validation when some builders are missing."""
        # Register only some builders
        trading_builder = MagicMock(spec=IRequestBuilder)
        registry.register_trading_builder("place_order", trading_builder)

        issues = registry.validate_registry()

        # Should report missing builders
        assert len(issues) > 0
        assert any("Missing builder: trading.cancel_order" in issue for issue in issues)
        assert any("Missing builder: account.get_balance" in issue for issue in issues)
        assert any("Missing builder: market_data.get_ticker" in issue for issue in issues)

    def test_validate_registry_comprehensive_check(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test comprehensive registry validation."""
        issues = registry.validate_registry()

        # With empty registry, all expected builders should be missing
        expected_missing = [
            "trading.place_order",
            "trading.cancel_order",
            "trading.get_order",
            "trading.batch_orders",
            "account.get_balance",
            "account.get_positions",
            "account.get_account_summary",
            "market_data.get_ticker",
            "market_data.get_orderbook",
            "market_data.get_trades",
        ]

        assert len(issues) == len(expected_missing)
        for expected in expected_missing:
            assert any(expected in issue for issue in issues)

    @pytest.mark.parametrize("domain", ["trading", "account", "market_data"])
    def test_domain_specific_registration_and_retrieval(
        self, registry: HyperliquidRequestBuilderRegistry, domain: str
    ) -> None:
        """Test domain-specific registration and retrieval methods."""
        operation = "test_operation"
        mock_builder = MagicMock(spec=IRequestBuilder)

        # Register using domain-specific method
        register_method = getattr(registry, f"register_{domain}_builder")
        register_method(operation, mock_builder)

        # Retrieve using domain-specific method
        get_method = getattr(registry, f"get_{domain}_builder")
        retrieved_builder = get_method(operation)

        assert retrieved_builder is mock_builder

    def test_builder_protocol_compliance(self, registry: HyperliquidRequestBuilderRegistry) -> None:
        """Test that registered builders must implement IRequestBuilder protocol."""
        # Create a proper mock that implements the protocol
        mock_builder = MagicMock(spec=IRequestBuilder)
        mock_builder.build_request.return_value = {"test": "request"}

        registry.register_trading_builder("test_op", mock_builder)
        retrieved_builder = registry.get_trading_builder("test_op")

        # Should be able to call protocol methods
        result = retrieved_builder.build_request(test="param")
        assert result == {"test": "request"}
        mock_builder.build_request.assert_called_once_with(test="param")

    def test_registry_inheritance_from_base(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test that registry properly inherits from BaseComponentRegistry."""
        # Test inherited methods work
        mock_builder = MagicMock(spec=IRequestBuilder)

        # Use inherited methods
        registry.register("custom.builder", mock_builder)
        assert registry.is_registered("custom.builder")
        assert registry.get("custom.builder") is mock_builder

        # Test len method
        assert len(registry) == 1

    def test_multiple_operation_registration(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test registering multiple operations across domains."""
        operations = [
            ("trading", "place_order"),
            ("trading", "cancel_order"),
            ("account", "get_balance"),
            ("market_data", "get_ticker"),
        ]

        builders = {}
        for domain, operation in operations:
            mock_builder = MagicMock(spec=IRequestBuilder)
            builders[f"{domain}.{operation}"] = mock_builder
            getattr(registry, f"register_{domain}_builder")(operation, mock_builder)

        # Verify all operations are registered correctly
        for domain, operation in operations:
            builder_name = f"{domain}.{operation}"
            assert registry.is_registered(builder_name)
            assert registry.get(builder_name) is builders[builder_name]

    def test_edge_case_operation_names(self, registry: HyperliquidRequestBuilderRegistry) -> None:
        """Test registration with edge case operation names."""
        edge_case_names = [
            "operation_with_underscores",
            "operation-with-dashes",
            "operation.with.dots",
            "123numeric_start",
            "UPPERCASE_OPERATION",
            "mixedCaseOperation",
        ]

        for operation_name in edge_case_names:
            mock_builder = MagicMock(spec=IRequestBuilder)
            registry.register_trading_builder(operation_name, mock_builder)

            # Should be retrievable
            retrieved = registry.get_trading_builder(operation_name)
            assert retrieved is mock_builder

    def test_initialization_state_persistence(
        self, registry: HyperliquidRequestBuilderRegistry
    ) -> None:
        """Test that initialization state persists correctly."""
        # Test through public behavior rather than private members
        assert len(registry) == 0

        with patch.object(registry, "_register_trading_builders"):
            registry.initialize_default_builders()

            # Call again to test persistence - should not re-register
            with patch.object(registry, "_register_trading_builders") as mock_trading:
                registry.initialize_default_builders()
                mock_trading.assert_not_called()
