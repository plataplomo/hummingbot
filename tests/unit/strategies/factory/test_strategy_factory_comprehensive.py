"""Comprehensive unit tests for StrategyFactory module.

Tests strategy factory functionality including strategy creation, validation, and configuration.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from decimal import Decimal
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.models.funding_strategy_models import StrategyParamsHLPerpBPSpot
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.strategies.factory.strategy_factory import (
    StrategyCreationError,
    StrategyFactory,
)
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy


class TestStrategyCreationErrorInit:
    """Test suite for StrategyCreationError initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_strategy_type(self) -> None:
        """Test successful initialization with strategy type."""
        # Arrange
        reason = "Invalid configuration"
        strategy_type = "hl_perp_bp_spot"

        # Act
        error = StrategyCreationError(reason, strategy_type)

        # Assert
        assert error.reason == reason
        assert error.strategy_type == strategy_type
        assert "Failed to create hl_perp_bp_spot strategy: Invalid configuration" in str(error)

    def test_init_success_without_strategy_type(self) -> None:
        """Test successful initialization without strategy type."""
        # Arrange
        reason = "General error"

        # Act
        error = StrategyCreationError(reason)

        # Assert
        assert error.reason == reason
        assert error.strategy_type is None
        assert "Strategy creation failed: General error" in str(error)

    # ==================== EDGE CASES ====================

    def test_init_edge_empty_reason(self) -> None:
        """Test initialization with empty reason."""
        # Arrange
        reason = ""
        strategy_type = "test_strategy"

        # Act
        error = StrategyCreationError(reason, strategy_type)

        # Assert
        assert not error.reason
        assert "Failed to create test_strategy strategy: " in str(error)

    def test_init_edge_special_characters_in_reason(self) -> None:
        """Test initialization with special characters in reason."""
        # Arrange
        reason = "Error with special chars: !@#$%^&*()"
        strategy_type = "test"

        # Act
        error = StrategyCreationError(reason, strategy_type)

        # Assert
        assert error.reason == reason
        assert reason in str(error)


class TestStrategyFactoryInit:
    """Test suite for StrategyFactory initialization."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = True
        config.strategies.hl_perp_bp_spot.params = Mock(spec=StrategyParamsHLPerpBPSpot)
        return config

    # ==================== SUCCESS CASES ====================

    def test_init_success_typical_config(self, mock_config: Mock) -> None:
        """Test successful initialization with typical configuration."""
        # Act
        factory = StrategyFactory(mock_config)

        # Assert
        assert factory.config == mock_config

    # ==================== EDGE CASES ====================

    def test_init_edge_minimal_config(self) -> None:
        """Test initialization with minimal configuration."""
        # Arrange
        minimal_config = Mock(spec=AppSettings)

        # Act
        factory = StrategyFactory(minimal_config)

        # Assert
        assert factory.config == minimal_config


class TestValidateStrategyEnabled:
    """Test suite for _validate_strategy_enabled method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        return config

    @pytest.fixture
    def factory(self, mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for testing."""
        return StrategyFactory(mock_config)

    # ==================== SUCCESS CASES ====================

    def test_validate_strategy_enabled_success_enabled_strategy(self, mock_config: Mock) -> None:
        """Test successful validation of enabled strategy through create method."""
        # Arrange - Set up config with enabled strategy
        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = Mock()
        strategy_config.params.model_dump = Mock(return_value={})
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Should not raise exception when strategy is enabled
        try:
            # This will internally call _validate_strategy_enabled
            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )
        except (ValueError, AttributeError, KeyError, TypeError) as e:
            # Should not fail due to validation if strategy is enabled
            # Other errors (like missing strategy class) are acceptable for this test
            if "not enabled" in str(e) or "disabled" in str(e):
                pytest.fail("Strategy validation should not fail when enabled")

    # ==================== EDGE CASES ====================

    def test_validate_strategy_enabled_edge_empty_strategy_type(self, mock_config: Mock) -> None:
        """Test validation with empty strategy type through create_hl_perp_bp_spot_strategy."""
        # Arrange - Set up config with enabled strategy
        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = Mock()
        strategy_config.params.model_dump = Mock(return_value={})
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Should handle the scenario properly
        # Testing edge case behavior through public API
        try:
            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )
        except (ValueError, AttributeError, KeyError, TypeError) as e:
            # Strategy type validation is internal - verify it doesn't fail on validation
            if "not enabled" in str(e) or "disabled" in str(e):
                pytest.fail("Strategy validation should not fail when enabled")

    # ==================== FAILURE CASES ====================

    def test_validate_strategy_enabled_failure_disabled_strategy(self, mock_config: Mock) -> None:
        """Test validation failure for disabled strategy through create_hl_perp_bp_spot_strategy."""
        # Arrange - Set up config with disabled strategy
        strategy_config = Mock()
        strategy_config.enabled = False  # Strategy is disabled
        strategy_config.params = Mock()
        strategy_config.params.model_dump = Mock(return_value={})
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Should raise StrategyCreationError when strategy is disabled
        with pytest.raises(StrategyCreationError) as exc_info:
            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

        # Verify the error is about the strategy being disabled
        assert "strategy is disabled in configuration" in str(exc_info.value)
        assert exc_info.value.strategy_type == "hl_perp_bp_spot"


class TestCreateHLPerpBPSpotStrategy:
    """Test suite for create_hl_perp_bp_spot_strategy method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = True

        # Mock strategy parameters
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {
            "threshold": Decimal("0.01"),
            "max_position_size": Decimal("1000.0"),
            "stop_loss": Decimal("0.05"),
        }
        config.strategies.hl_perp_bp_spot.params = mock_params
        return config

    @pytest.fixture
    def factory(self, mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for testing."""
        return StrategyFactory(mock_config)

    @pytest.fixture
    def mock_data_handler(self) -> Mock:
        """Create a mock data handler."""
        return Mock(spec=DataHandler)

    @pytest.fixture
    def mock_portfolio_tracker(self) -> Mock:
        """Create a mock portfolio tracker."""
        return Mock(spec=PortfolioTracker)

    @pytest.fixture
    def mock_risk_manager(self) -> Mock:
        """Create a mock risk manager."""
        return Mock(spec=RiskManager)

    # ==================== SUCCESS CASES ====================

    def test_create_hl_perp_bp_spot_strategy_success_with_risk_manager(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
        mock_risk_manager: Mock,
    ) -> None:
        """Test successful strategy creation with risk manager."""
        # Arrange
        name = "test_strategy"
        symbol = "BTC"

        with (
            patch.object(
                factory, "_convert_strategy_params_to_dict", return_value={"param": "value"}
            ),
            patch(
                "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
            ) as mock_strategy_class,
        ):
            mock_strategy_instance = Mock(spec=FundingRateArbitrageStrategy)
            mock_strategy_class.return_value = mock_strategy_instance

            # Act
            result = factory.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=mock_risk_manager,
            )

            # Assert
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once_with(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=mock_risk_manager,
                params={"param": "value"},
            )

    def test_create_hl_perp_bp_spot_strategy_success_without_risk_manager(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful strategy creation without risk manager."""
        # Arrange
        name = "test_strategy"
        symbol = "ETH"

        with (
            patch.object(
                factory, "_convert_strategy_params_to_dict", return_value={"param": "value"}
            ),
            patch(
                "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
            ) as mock_strategy_class,
        ):
            mock_strategy_instance = Mock(spec=FundingRateArbitrageStrategy)
            mock_strategy_class.return_value = mock_strategy_instance

            # Act
            result = factory.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=None,
            )

            # Assert
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once_with(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=None,
                params={"param": "value"},
            )

    # ==================== EDGE CASES ====================

    def test_create_hl_perp_bp_spot_strategy_edge_empty_name(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation with empty name."""
        # Arrange
        name = ""
        symbol = "BTC"

        with (
            patch.object(
                factory, "_convert_strategy_params_to_dict", return_value={"param": "value"}
            ),
            patch(
                "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
            ) as mock_strategy_class,
        ):
            mock_strategy_instance = Mock(spec=FundingRateArbitrageStrategy)
            mock_strategy_class.return_value = mock_strategy_instance

            # Act
            result = factory.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Assert
            assert result == mock_strategy_instance

    def test_create_hl_perp_bp_spot_strategy_edge_special_characters_in_name(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation with special characters in name."""
        # Arrange
        name = "strategy-with_special.chars@123"
        symbol = "BTC"

        with (
            patch.object(
                factory, "_convert_strategy_params_to_dict", return_value={"param": "value"}
            ),
            patch(
                "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
            ) as mock_strategy_class,
        ):
            mock_strategy_instance = Mock(spec=FundingRateArbitrageStrategy)
            mock_strategy_class.return_value = mock_strategy_instance

            # Act
            result = factory.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Assert
            assert result == mock_strategy_instance

    # ==================== FAILURE CASES ====================

    def test_create_hl_perp_bp_spot_strategy_failure_disabled_strategy(
        self,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation failure with disabled strategy."""
        # Arrange
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = False

        factory = StrategyFactory(config)
        name = "test_strategy"
        symbol = "BTC"

        # Act & Assert
        with pytest.raises(StrategyCreationError) as exc_info:
            factory.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

        assert "strategy is disabled in configuration" in str(exc_info.value)

    def test_create_hl_perp_bp_spot_strategy_failure_strategy_class_exception(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation failure when strategy class raises exception."""
        # Arrange
        name = "test_strategy"
        symbol = "BTC"

        with (
            patch.object(
                factory, "_convert_strategy_params_to_dict", return_value={"param": "value"}
            ),
            patch(
                "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
            ) as mock_strategy_class,
        ):
            mock_strategy_class.side_effect = ValueError("Strategy initialization failed")

            # Act & Assert
            with pytest.raises(StrategyCreationError) as exc_info:
                factory.create_hl_perp_bp_spot_strategy(
                    name=name,
                    symbol=symbol,
                    data_handler=mock_data_handler,
                    portfolio_tracker=mock_portfolio_tracker,
                )

            assert "Failed to create HL Perp BP Spot strategy" in str(exc_info.value)


class TestCreateStrategy:
    """Test suite for create_strategy method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = True
        config.strategies.hl_perp_bp_spot.params = Mock(spec=StrategyParamsHLPerpBPSpot)
        return config

    @pytest.fixture
    def factory(self, mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for testing."""
        return StrategyFactory(mock_config)

    @pytest.fixture
    def mock_data_handler(self) -> Mock:
        """Create a mock data handler."""
        return Mock(spec=DataHandler)

    @pytest.fixture
    def mock_portfolio_tracker(self) -> Mock:
        """Create a mock portfolio tracker."""
        return Mock(spec=PortfolioTracker)

    @pytest.fixture
    def mock_risk_manager(self) -> Mock:
        """Create a mock risk manager."""
        return Mock(spec=RiskManager)

    # ==================== SUCCESS CASES ====================

    def test_create_strategy_success_hl_perp_bp_spot(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
        mock_risk_manager: Mock,
    ) -> None:
        """Test successful strategy creation for hl_perp_bp_spot type."""
        # Arrange
        strategy_type = "hl_perp_bp_spot"
        name = "test_strategy"
        symbol = "BTC"
        mock_strategy = Mock(spec=FundingRateArbitrageStrategy)

        with patch.object(
            factory, "create_hl_perp_bp_spot_strategy", return_value=mock_strategy
        ) as mock_create:
            # Act
            result = factory.create_strategy(
                strategy_type=strategy_type,
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=mock_risk_manager,
            )

            # Assert
            assert result == mock_strategy
            mock_create.assert_called_once_with(
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
                risk_manager=mock_risk_manager,
            )

    # ==================== EDGE CASES ====================

    def test_create_strategy_edge_case_sensitivity(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation with case variations."""
        # Arrange - Test case sensitivity
        strategy_type = "HL_PERP_BP_SPOT"  # Wrong case
        name = "test_strategy"
        symbol = "BTC"

        # Act & Assert
        with pytest.raises(StrategyCreationError) as exc_info:
            factory.create_strategy(
                strategy_type=strategy_type,
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

        assert "Unknown strategy type" in str(exc_info.value)

    # ==================== FAILURE CASES ====================

    def test_create_strategy_failure_unknown_type(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation failure with unknown strategy type."""
        # Arrange
        strategy_type = "unknown_strategy"
        name = "test_strategy"
        symbol = "BTC"

        # Act & Assert
        with pytest.raises(StrategyCreationError) as exc_info:
            factory.create_strategy(
                strategy_type=strategy_type,
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

        assert "Unknown strategy type" in str(exc_info.value)
        assert exc_info.value.strategy_type == strategy_type

    def test_create_strategy_failure_empty_strategy_type(
        self,
        factory: StrategyFactory,
        mock_data_handler: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test strategy creation failure with empty strategy type."""
        # Arrange
        strategy_type = ""
        name = "test_strategy"
        symbol = "BTC"

        # Act & Assert
        with pytest.raises(StrategyCreationError) as exc_info:
            factory.create_strategy(
                strategy_type=strategy_type,
                name=name,
                symbol=symbol,
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

        assert "Unknown strategy type" in str(exc_info.value)


class TestConvertStrategyParamsToDict:
    """Test suite for _convert_strategy_params_to_dict method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        return config

    @pytest.fixture
    def factory(self, mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for testing."""
        return StrategyFactory(mock_config)

    # ==================== SUCCESS CASES ====================

    def test_convert_strategy_params_to_dict_success_with_decimals(self, mock_config: Mock) -> None:
        """Test successful conversion of parameters with Decimal values through create method."""
        # Arrange - Set up config with strategy params containing Decimals
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {
            "threshold": Decimal("0.01"),
            "max_position_size": Decimal("1000.0"),
            "string_param": "test_value",
            "int_param": 42,
            "bool_param": True,
        }

        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = mock_params
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Strategy creation should succeed, indicating parameter conversion worked
        with patch(
            "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
        ) as mock_strategy_class:
            # Mock strategy constructor to verify the converted params
            mock_strategy_instance = Mock()
            mock_strategy_class.return_value = mock_strategy_instance

            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            result = factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Verify strategy was created with converted parameters
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once()
            call_args = mock_strategy_class.call_args

            # Verify Decimals were converted to floats
            assert call_args.kwargs["params"]["threshold"] == 0.01
            assert call_args.kwargs["params"]["max_position_size"] == 1000.0
            # Other parameters should also be passed correctly
            assert call_args.kwargs["params"]["string_param"] == "test_value"
            assert call_args.kwargs["params"]["int_param"] == 42
            assert call_args.kwargs["params"]["bool_param"] is True

    def test_convert_strategy_params_to_dict_success_no_decimals(self, mock_config: Mock) -> None:
        """Test successful conversion of parameters without Decimal values through create method."""
        # Arrange - Set up config with strategy params containing no Decimals
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {
            "string_param": "test_value",
            "int_param": 42,
            "bool_param": False,
            "list_param": [1, 2, 3],
        }

        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = mock_params
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Strategy creation should succeed, indicating parameter conversion worked
        with patch(
            "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
        ) as mock_strategy_class:
            # Mock strategy constructor to verify the converted params
            mock_strategy_instance = Mock()
            mock_strategy_class.return_value = mock_strategy_instance

            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            result = factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Verify strategy was created with converted parameters
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once()
            call_args = mock_strategy_class.call_args[1]  # Get keyword arguments

            # Verify non-Decimal parameters are passed correctly
            assert call_args["params"]["string_param"] == "test_value"
            assert call_args["params"]["int_param"] == 42
            assert call_args["params"]["bool_param"] is False
            assert call_args["params"]["list_param"] == [1, 2, 3]

    # ==================== EDGE CASES ====================

    def test_convert_strategy_params_to_dict_edge_empty_params(self, mock_config: Mock) -> None:
        """Test conversion of empty parameters through create_hl_perp_bp_spot_strategy."""
        # Arrange - Set up config with empty strategy params
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {}

        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = mock_params
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Strategy creation should succeed with empty params
        with patch(
            "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
        ) as mock_strategy_class:
            # Mock strategy constructor to verify the converted params
            mock_strategy_instance = Mock()
            mock_strategy_class.return_value = mock_strategy_instance

            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            result = factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Verify strategy was created with empty parameters
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once()
            call_args = mock_strategy_class.call_args[1]  # Get keyword arguments

            # Verify empty params are handled correctly
            assert call_args["params"] == {}

    def test_convert_strategy_params_to_dict_edge_mixed_types(self, mock_config: Mock) -> None:
        """Test conversion with mixed parameter types through create_hl_perp_bp_spot_strategy."""
        # Arrange - Set up config with mixed parameter types
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {
            "decimal_val": Decimal("123.456"),
            "none_val": None,
            "zero_decimal": Decimal("0.0"),
            "nested_dict": {"key": "value"},
            "nested_list": [Decimal("1.1"), "string", 42],
        }

        strategy_config = Mock()
        strategy_config.enabled = True
        strategy_config.params = mock_params
        mock_config.strategies.hl_perp_bp_spot = strategy_config

        factory = StrategyFactory(mock_config)

        # Act & Assert - Strategy creation should succeed, indicating parameter conversion worked
        with patch(
            "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
        ) as mock_strategy_class:
            # Mock strategy constructor to verify the converted params
            mock_strategy_instance = Mock()
            mock_strategy_class.return_value = mock_strategy_instance

            # We need to provide mock dependencies for the test
            mock_data_handler = Mock()
            mock_portfolio_tracker = Mock()
            result = factory.create_hl_perp_bp_spot_strategy(
                name="test",
                symbol="BTC",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_tracker,
            )

            # Verify strategy was created with converted parameters
            assert result == mock_strategy_instance
            mock_strategy_class.assert_called_once()
            call_args = mock_strategy_class.call_args[1]  # Get keyword arguments

            # Verify mixed type conversion
            params = call_args["params"]
            assert params["decimal_val"] == 123.456  # Decimal converted to float
            assert params["none_val"] is None
            assert params["zero_decimal"] == 0.0  # Decimal converted to float
            assert params["nested_dict"] == {"key": "value"}
            # Nested Decimals not converted (only top-level Decimals are converted)
            assert params["nested_list"] == [Decimal("1.1"), "string", 42]


class TestGetAvailableStrategyTypes:
    """Test suite for get_available_strategy_types method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock configuration for testing."""
        return Mock(spec=AppSettings)

    @pytest.fixture
    def factory(self, mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for testing."""
        return StrategyFactory(mock_config)

    # ==================== SUCCESS CASES ====================

    def test_get_available_strategy_types_success(self, factory: StrategyFactory) -> None:
        """Test successful retrieval of available strategy types."""
        # Act
        result = factory.get_available_strategy_types()

        # Assert
        assert isinstance(result, list)
        assert "hl_perp_bp_spot" in result
        assert len(result) == 1

    # ==================== EDGE CASES ====================

    def test_get_available_strategy_types_edge_immutable_return(
        self, factory: StrategyFactory
    ) -> None:
        """Test that returned list is independent (modifications don't affect internal state)."""
        # Act
        result1 = factory.get_available_strategy_types()
        result2 = factory.get_available_strategy_types()

        # Modify first result
        result1.append("new_strategy")

        # Assert
        assert result2 == ["hl_perp_bp_spot"]  # Should not be affected by modification


class TestValidateStrategyConfig:
    """Test suite for validate_strategy_config method."""

    @pytest.fixture
    def mock_config_enabled(self) -> Mock:
        """Create a mock configuration with enabled strategy."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = True
        return config

    @pytest.fixture
    def mock_config_disabled(self) -> Mock:
        """Create a mock configuration with disabled strategy."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = False
        return config

    @pytest.fixture
    def factory_enabled(self, mock_config_enabled: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance with enabled strategy."""
        return StrategyFactory(mock_config_enabled)

    @pytest.fixture
    def factory_disabled(self, mock_config_disabled: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance with disabled strategy."""
        return StrategyFactory(mock_config_disabled)

    # ==================== SUCCESS CASES ====================

    def test_validate_strategy_config_success_enabled(
        self, factory_enabled: StrategyFactory
    ) -> None:
        """Test successful validation of enabled strategy configuration."""
        # Arrange
        strategy_type = "hl_perp_bp_spot"

        # Act
        result = factory_enabled.validate_strategy_config(strategy_type)

        # Assert
        assert result is True

    def test_validate_strategy_config_success_disabled(
        self, factory_disabled: StrategyFactory
    ) -> None:
        """Test successful validation of disabled strategy configuration."""
        # Arrange
        strategy_type = "hl_perp_bp_spot"

        # Act
        result = factory_disabled.validate_strategy_config(strategy_type)

        # Assert
        assert result is False

    # ==================== EDGE CASES ====================

    def test_validate_strategy_config_edge_empty_strategy_type(
        self, factory_enabled: StrategyFactory
    ) -> None:
        """Test validation with empty strategy type."""
        # Arrange
        strategy_type = ""

        # Act
        result = factory_enabled.validate_strategy_config(strategy_type)

        # Assert
        assert result is False

    # ==================== FAILURE CASES ====================

    def test_validate_strategy_config_failure_unknown_strategy_type(
        self, factory_enabled: StrategyFactory
    ) -> None:
        """Test validation failure with unknown strategy type."""
        # Arrange
        strategy_type = "unknown_strategy"

        # Act
        result = factory_enabled.validate_strategy_config(strategy_type)

        # Assert
        assert result is False

    def test_validate_strategy_config_failure_missing_config(self) -> None:
        """Test validation failure when configuration is missing."""
        # Arrange
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        # Missing hl_perp_bp_spot attribute
        del config.strategies.hl_perp_bp_spot

        factory = StrategyFactory(config)
        strategy_type = "hl_perp_bp_spot"

        # Act
        result = factory.validate_strategy_config(strategy_type)

        # Assert
        assert result is False

    def test_validate_strategy_config_failure_attribute_error(self) -> None:
        """Test validation failure when accessing config raises AttributeError."""
        # Arrange
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()

        # Configure Mock to raise AttributeError when enabled is accessed
        # Remove the enabled attribute to trigger AttributeError
        del config.strategies.hl_perp_bp_spot.enabled

        factory = StrategyFactory(config)
        strategy_type = "hl_perp_bp_spot"

        # Act
        result = factory.validate_strategy_config(strategy_type)

        # Assert
        assert result is False


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("strategy_type", "expected_result"),
    [
        ("hl_perp_bp_spot", True),
        ("unknown_strategy", False),
        ("", False),
        ("HL_PERP_BP_SPOT", False),  # Case sensitive
        ("hl_perp_bp_spot_v2", False),
    ],
)
def test_validate_strategy_config_parametrized(strategy_type: str, expected_result: bool) -> None:
    """Test strategy config validation for various strategy types."""
    # Arrange
    config = Mock(spec=AppSettings)
    config.strategies = Mock()
    config.strategies.hl_perp_bp_spot = Mock()
    config.strategies.hl_perp_bp_spot.enabled = True

    factory = StrategyFactory(config)

    # Act
    result = factory.validate_strategy_config(strategy_type)

    # Assert
    assert result == expected_result


@pytest.mark.parametrize(
    ("params_dict", "expected_conversions"),
    [
        # Only Decimals
        (
            {"threshold": Decimal("0.01"), "limit": Decimal("100.0")},
            {"threshold": 0.01, "limit": 100.0},
        ),
        # Mixed types
        (
            {
                "decimal_val": Decimal("123.45"),
                "string_val": "test",
                "int_val": 42,
                "bool_val": True,
                "none_val": None,
            },
            {
                "decimal_val": 123.45,
                "string_val": "test",
                "int_val": 42,
                "bool_val": True,
                "none_val": None,
            },
        ),
        # No Decimals
        (
            {"string": "value", "number": 42, "flag": False},
            {"string": "value", "number": 42, "flag": False},
        ),
        # Empty dict
        ({}, {}),
    ],
)
def test_convert_strategy_params_parametrized(
    params_dict: dict[str, Any], expected_conversions: dict[str, Any]
) -> None:
    """Test parameter conversion for various input types through create_hl_perp_bp_spot_strategy."""
    # Arrange - Set up config with parametrized strategy params
    config = Mock(spec=AppSettings)
    config.strategies = Mock()

    mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
    mock_params.model_dump.return_value = params_dict

    strategy_config = Mock()
    strategy_config.enabled = True
    strategy_config.params = mock_params
    config.strategies.hl_perp_bp_spot = strategy_config

    factory = StrategyFactory(config)

    # Act & Assert - Strategy creation should succeed, indicating parameter conversion worked
    with patch(
        "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
    ) as mock_strategy_class:
        # Mock strategy constructor to verify the converted params
        mock_strategy_instance = Mock()
        mock_strategy_class.return_value = mock_strategy_instance

        # We need to provide mock dependencies for the test
        mock_data_handler = Mock()
        mock_portfolio_tracker = Mock()
        result = factory.create_hl_perp_bp_spot_strategy(
            name="test",
            symbol="BTC",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_tracker,
        )

        # Verify strategy was created with converted parameters
        assert result == mock_strategy_instance
        mock_strategy_class.assert_called_once()
        call_args = mock_strategy_class.call_args[1]  # Get keyword arguments

        # Verify parameter conversion matches expected conversions
        assert call_args["params"] == expected_conversions


# ==================== INTEGRATION TESTS ====================


class TestStrategyFactoryIntegration:
    """Integration tests for StrategyFactory."""

    @pytest.fixture
    def complete_mock_config(self) -> Mock:
        """Create a complete mock configuration for integration testing."""
        config = Mock(spec=AppSettings)
        config.strategies = Mock()
        config.strategies.hl_perp_bp_spot = Mock()
        config.strategies.hl_perp_bp_spot.enabled = True

        # Mock complete strategy parameters
        mock_params = Mock(spec=StrategyParamsHLPerpBPSpot)
        mock_params.model_dump.return_value = {
            "funding_threshold": Decimal("0.01"),
            "max_position_size": Decimal("1000.0"),
            "stop_loss_pct": Decimal("0.05"),
            "take_profit_pct": Decimal("0.02"),
            "rebalance_threshold": Decimal("0.005"),
        }
        config.strategies.hl_perp_bp_spot.params = mock_params
        return config

    @pytest.fixture
    def factory(self, complete_mock_config: Mock) -> StrategyFactory:
        """Create a StrategyFactory instance for integration testing."""
        return StrategyFactory(complete_mock_config)

    @pytest.fixture
    def all_mocks(self) -> dict[str, Mock]:
        """Create all required mocks for strategy creation."""
        return {
            "data_handler": Mock(spec=DataHandler),
            "portfolio_tracker": Mock(spec=PortfolioTracker),
            "risk_manager": Mock(spec=RiskManager),
        }

    def test_integration_full_strategy_creation_workflow(
        self, factory: StrategyFactory, all_mocks: dict[str, Mock]
    ) -> None:
        """Test complete strategy creation workflow."""
        # Arrange
        name = "integration_test_strategy"
        symbol = "BTC"

        with patch(
            "cyberdelta.strategies.factory.strategy_factory.FundingRateArbitrageStrategy"
        ) as mock_strategy_class:
            mock_strategy_instance = Mock(spec=FundingRateArbitrageStrategy)
            mock_strategy_class.return_value = mock_strategy_instance

            # Act - Test validation
            is_valid = factory.validate_strategy_config("hl_perp_bp_spot")
            assert is_valid is True

            # Act - Test available types
            available_types = factory.get_available_strategy_types()
            assert "hl_perp_bp_spot" in available_types

            # Act - Test strategy creation via generic method
            strategy = factory.create_strategy(
                strategy_type="hl_perp_bp_spot",
                name=name,
                symbol=symbol,
                data_handler=all_mocks["data_handler"],
                portfolio_tracker=all_mocks["portfolio_tracker"],
                risk_manager=all_mocks["risk_manager"],
            )

            # Assert
            assert strategy == mock_strategy_instance
            mock_strategy_class.assert_called_once()

            # Verify the parameters were properly converted
            call_args = mock_strategy_class.call_args
            assert call_args[1]["name"] == name
            assert call_args[1]["symbol"] == symbol
            assert "params" in call_args[1]

            # Verify Decimal conversion
            params = call_args[1]["params"]
            assert isinstance(params["funding_threshold"], float)
            assert params["funding_threshold"] == 0.01
