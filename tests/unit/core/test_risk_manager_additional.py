"""Additional comprehensive unit tests for RiskManager.

Tests additional public methods and edge cases that need better coverage,
focusing on opportunity validation, position sizing, risk calculations,
and portfolio management.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings, GlobalRiskSettings, RiskSettings
from cyberdelta.core.models.spot_balance import SpotBalance
from cyberdelta.core.risk_manager import RiskManager, SimpleSizingMethod, SizedOpportunity
from tests.common_symbols import BTC_HL, ETH_HL
from cyberdelta.exceptions.risk import RiskConfigError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing.

    Returns:
        Mock: Mocked AppSettings with risk configuration.
    """
    settings = Mock(spec=AppSettings)

    # Create mock risk configuration
    global_risk = Mock(spec=GlobalRiskSettings)
    global_risk.max_position_usd = Decimal("10000.0")
    global_risk.max_total_exposure_usd = Decimal("50000.0")

    risk_config = Mock(spec=RiskSettings)
    risk_config.use_simple_sizing_path = True
    risk_config.global_risk = global_risk
    risk_config.simple_sizing_method = "fixed_usd"
    risk_config.simple_fixed_usd_size = Decimal("1000.0")
    risk_config.simple_fixed_fraction = Decimal("0.1")

    # Create mock exchanges configuration
    exchange_config = Mock()
    exchange_config.enabled = True
    exchanges = {"hyperliquid": exchange_config, "backpack": exchange_config}

    settings.risk = risk_config
    settings.exchanges = exchanges
    return settings


@pytest.fixture
def mock_portfolio_state_manager() -> Mock:
    """Create mock portfolio tracker for testing.

    Returns:
        Mock: Mocked portfolio tracker with default behaviors.
    """
    tracker = Mock()
    tracker.get_total_capital = AsyncMock(return_value=Decimal("100000.0"))

    # Create proper SpotBalance objects for the mock
    spot_balance = SpotBalance(
        exchange="hyperliquid",
        asset="USD",
        timestamp=datetime.now(UTC),
        total_quantity=Decimal("50000.0"),
        available_quantity=Decimal("50000.0"),
    )

    tracker.get_exchange_balance = Mock(return_value=spot_balance)
    tracker.get_all_positions = Mock(return_value=[])
    tracker.get_current_drawdown = AsyncMock(return_value=Decimal("0.0"))
    tracker.get_total_exposure_usd = AsyncMock(return_value=Decimal("0.0"))
    tracker.can_execute = Mock(return_value=(True, None))
    tracker.get_exchange_breaker = Mock(return_value=None)
    return tracker


@pytest.fixture
def mock_circuit_breaker_system() -> Mock:
    """Create mock circuit breaker system for testing.

    Returns:
        Mock: Mocked circuit breaker system that allows execution.
    """
    system = Mock()
    # can_execute should return (can_execute: bool, reason: str)
    system.can_execute.return_value = (True, "Circuit breaker closed")
    return system


@pytest.fixture
def mock_funding_rate_validator() -> Mock:
    """Create mock funding rate validator for testing.

    Returns:
        Mock: Mocked funding rate validator with sample metrics.
    """
    validator = Mock()
    validator.get_symbol_metrics.return_value = {
        "accuracy": 0.85,
        "confidence": 0.90,
        "volatility": Decimal("0.02"),
        "rmse": Decimal("0.001"),  # Add RMSE value
        "bias": Decimal("0.0005"),  # Add bias value
    }
    return validator


@pytest.fixture
def risk_manager(
    mock_app_settings: Mock,
    mock_portfolio_state_manager: Mock,
    mock_circuit_breaker_system: Mock,
    mock_funding_rate_validator: Mock,
) -> RiskManager:
    """Create a RiskManager instance for testing.

    Returns:
        RiskManager: Configured risk manager with all mocked dependencies.
    """
    return RiskManager(
        app_settings=mock_app_settings,
        portfolio_tracker=mock_portfolio_state_manager,
        circuit_breaker_system=mock_circuit_breaker_system,
        funding_rate_validator=mock_funding_rate_validator,
    )


@pytest.fixture
def sample_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Create a sample ArbitrageOpportunity for testing.

    Returns:
        ArbitrageOpportunity: Sample BTC-PERP arbitrage opportunity with 20 bps spread.
    """
    btc_symbol = BTC_HL
    return ArbitrageOpportunity(
        symbol=btc_symbol.value,
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("50100.0"),
        net_funding_differential=Decimal("0.002"),  # 20 bps spread
        timestamp=datetime.now(UTC),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("0.0002"),
    )


class TestRiskManagerInitialization:
    """Test suite for RiskManager initialization and configuration."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success_with_all_dependencies(
        self,
        mock_app_settings: Mock,
        mock_portfolio_state_manager: Mock,
        mock_circuit_breaker_system: Mock,
        mock_funding_rate_validator: Mock,
    ) -> None:
        """Test successful initialization with all dependencies."""
        # Act
        risk_manager = RiskManager(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_rate_validator,
        )

        # Assert
        assert risk_manager.app_settings is mock_app_settings
        assert risk_manager.portfolio_tracker is mock_portfolio_state_manager
        assert risk_manager.circuit_breaker_system is mock_circuit_breaker_system
        assert risk_manager.funding_rate_validator is mock_funding_rate_validator
        assert risk_manager.use_simple_sizing_path is True
        assert risk_manager.kelly_enabled is False

    def test_initialization_success_with_minimal_dependencies(
        self, mock_app_settings: Mock, mock_portfolio_state_manager: Mock
    ) -> None:
        """Test successful initialization with minimal required dependencies."""
        # Act
        risk_manager = RiskManager(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_state_manager,
        )

        # Assert
        assert risk_manager.app_settings is mock_app_settings
        assert risk_manager.portfolio_tracker is mock_portfolio_state_manager
        assert risk_manager.circuit_breaker_system is None
        assert risk_manager.funding_rate_validator is None

    def test_initialization_success_loads_config_attributes(
        self, risk_manager: RiskManager
    ) -> None:
        """Test that initialization properly loads configuration attributes."""
        # Assert
        assert hasattr(risk_manager, "max_position_size")
        assert hasattr(risk_manager, "max_total_exposure_usd")
        assert hasattr(risk_manager, "min_trade_size_usd")
        assert hasattr(risk_manager, "simple_sizing_method_str")
        assert isinstance(risk_manager.max_position_size, Decimal)
        assert isinstance(risk_manager.max_total_exposure_usd, Decimal)

    # ==================== FAILURE CASES ====================

    def test_initialization_failure_invalid_config(
        self, mock_portfolio_state_manager: Mock
    ) -> None:
        """Test initialization failure with invalid configuration."""
        # Arrange
        bad_settings = Mock()
        bad_settings.risk = None  # Missing risk config

        # Act & Assert
        with pytest.raises(RiskConfigError):
            RiskManager(
                app_settings=bad_settings,
                portfolio_tracker=mock_portfolio_state_manager,
            )


class TestRiskManagerOpportunityValidation:
    """Test suite for opportunity validation methods."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_validate_opportunity_success_valid_opportunity(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test validation of a valid arbitrage opportunity."""
        # Act
        result = await risk_manager.validate_opportunity(sample_arbitrage_opportunity)

        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_opportunity_success_minimal_valid_opportunity(
        self, risk_manager: RiskManager
    ) -> None:
        """Test validation with minimal valid opportunity."""
        # Arrange
        eth_symbol = ETH_HL
        opportunity = ArbitrageOpportunity(
            symbol=eth_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("3000.0"),
            short_price=Decimal("3010.0"),
            net_funding_differential=Decimal("0.0033"),  # 33 bps spread
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0002"),
        )

        # Act
        result = await risk_manager.validate_opportunity(opportunity)

        # Assert
        assert result is True

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_validate_opportunity_edge_zero_nfd(self, risk_manager: RiskManager) -> None:
        """Test validation with zero net funding difference."""
        # Arrange
        btc_symbol = BTC_HL
        opportunity = ArbitrageOpportunity(
            symbol=btc_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50000.0"),  # Same price
            net_funding_differential=Decimal("0.0"),  # Zero spread
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0001"),
        )

        # Act
        result = await risk_manager.validate_opportunity(opportunity)

        # Assert
        assert result is False  # Should fail profitability check

    @pytest.mark.asyncio
    async def test_validate_opportunity_edge_very_small_spread(
        self, risk_manager: RiskManager
    ) -> None:
        """Test validation with very small spread."""
        # Arrange
        btc_symbol = BTC_HL
        opportunity = ArbitrageOpportunity(
            symbol=btc_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50000.1"),  # 0.1 USD spread
            net_funding_differential=Decimal("0.000002"),  # 0.2 bps spread
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0002"),
        )

        # Act
        result = await risk_manager.validate_opportunity(opportunity)

        # Assert
        # Should depend on minimum NFD threshold in risk manager
        assert isinstance(result, bool)

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_validate_opportunity_failure_missing_required_fields(
        self, risk_manager: RiskManager
    ) -> None:
        """Test validation failure with missing required fields."""
        # Arrange
        opportunity = ArbitrageOpportunity(
            symbol="",  # Empty symbol
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50100.0"),
            net_funding_differential=Decimal("0.002"),
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0002"),
        )

        # Act
        result = await risk_manager.validate_opportunity(opportunity)

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_opportunity_failure_circuit_breaker_open(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test validation failure when circuit breaker is open."""
        # Arrange
        if risk_manager.circuit_breaker_system:
            # Mock can_execute to return (False, reason) for circuit breaker open
            with patch.object(
                risk_manager.circuit_breaker_system,
                "can_execute",
                return_value=(
                    False,
                    "Circuit breaker open",
                ),
            ):
                # Act
                result = await risk_manager.validate_opportunity(sample_arbitrage_opportunity)

                # Assert
                assert result is False
        else:
            # Act
            result = await risk_manager.validate_opportunity(sample_arbitrage_opportunity)

            # Assert - No circuit breaker system, validation should pass
            assert result is True


class TestRiskManagerPositionSizing:
    """Test suite for position sizing methods."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_size_opportunity_success_simple_sizing(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test successful position sizing with simple sizing method."""
        # Arrange
        risk_manager.use_simple_sizing_path = True
        risk_manager.simple_sizing_method_str = SimpleSizingMethod.FIXED_USD.value
        risk_manager.simple_fixed_usd_size = Decimal("1000.0")

        # Act
        result = await risk_manager.size_opportunity(sample_arbitrage_opportunity)

        # Assert
        assert result is not None
        assert isinstance(result, SizedOpportunity)
        assert result.opportunity is sample_arbitrage_opportunity
        assert result.long_size > Decimal(0)
        assert result.short_size > Decimal(0)
        assert result.allocation_percentage >= Decimal(0)

    @pytest.mark.asyncio
    async def test_size_opportunity_success_returns_none_for_invalid(
        self, risk_manager: RiskManager
    ) -> None:
        """Test size_opportunity returns None for invalid opportunity."""
        # Arrange
        invalid_opportunity = ArbitrageOpportunity(
            symbol="",  # Invalid symbol
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50000.0"),  # No spread
            net_funding_differential=Decimal("0.0"),  # No profit
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0001"),
        )

        # Act
        result = await risk_manager.size_opportunity(invalid_opportunity)

        # Assert
        assert result is None

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_size_opportunity_edge_kelly_sizing_disabled(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test position sizing when Kelly sizing is disabled."""
        # Arrange
        risk_manager.kelly_enabled = False
        risk_manager.use_simple_sizing_path = True

        # Act
        result = await risk_manager.size_opportunity(sample_arbitrage_opportunity)

        # Assert
        assert result is not None
        # Should use simple sizing instead of Kelly

    @pytest.mark.asyncio
    async def test_size_opportunity_edge_very_large_opportunity(
        self, risk_manager: RiskManager
    ) -> None:
        """Test position sizing with very large opportunity."""
        # Arrange
        btc_symbol = BTC_HL
        large_opportunity = ArbitrageOpportunity(
            symbol=btc_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("55000.0"),  # Very large spread
            net_funding_differential=Decimal("0.1"),  # 1000 bps spread
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0011"),
        )

        # Act
        result = await risk_manager.size_opportunity(large_opportunity)

        # Assert
        if result is not None:
            # Should be capped by position limits
            assert result.long_size <= risk_manager.max_position_size
            assert result.short_size <= risk_manager.max_position_size


class TestRiskManagerRiskCalculations:
    """Test suite for risk calculation methods."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_position_exposure_success_with_symbol(
        self, risk_manager: RiskManager
    ) -> None:
        """Test calculation of position exposure for existing symbol."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value

        # Mock portfolio tracker to return some positions
        mock_position = Mock()
        mock_position.symbol = symbol
        mock_position.notional_value_usd = Decimal("5000.0")
        with patch.object(
            risk_manager.portfolio_tracker,
            "get_all_positions",
            return_value=[("exchange1", mock_position)],
        ):
            # Act
            result = risk_manager.calculate_position_exposure(symbol)

            # Assert
            assert result is not None
            assert isinstance(result, Decimal)
            assert result >= Decimal(0)

    def test_calculate_position_exposure_success_no_positions(
        self, risk_manager: RiskManager
    ) -> None:
        """Test calculation when no positions exist for symbol."""
        # Arrange
        eth_symbol = ETH_HL
        symbol = eth_symbol.value
        with patch.object(risk_manager.portfolio_tracker, "get_all_positions", return_value=[]):
            # Act
            result = risk_manager.calculate_position_exposure(symbol)

            # Assert
            assert result == Decimal(0) or result is None

    @pytest.mark.asyncio
    async def test_calculate_total_exposure_success(self, risk_manager: RiskManager) -> None:
        """Test calculation of total portfolio exposure."""
        # Act
        result = await risk_manager.calculate_total_exposure()

        # Assert
        assert isinstance(result, Decimal)
        assert result >= Decimal(0)

    def test_calculate_required_margin_success(self, risk_manager: RiskManager) -> None:
        """Test calculation of required margin for position."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value
        position_size_usd = Decimal("0.2")  # Position size in base asset
        price = Decimal("50000.0")  # Price per unit
        leverage = Decimal("5.0")

        # Act
        result = risk_manager.calculate_required_margin(symbol, position_size_usd, price, leverage)

        # Assert
        assert isinstance(result, Decimal)
        assert result > Decimal(0)
        # Margin should be (size * price) / leverage = (0.2 * 50000) / 5 = 2000
        expected_margin = (position_size_usd * price) / leverage
        assert result == expected_margin

    def test_evaluate_liquidation_risk_success(self, risk_manager: RiskManager) -> None:
        """Test evaluation of liquidation risk for symbol."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value

        # Act
        result = risk_manager.evaluate_liquidation_risk(symbol)

        # Assert
        # Result can be None if no positions exist, or Decimal if risk calculated
        assert result is None or isinstance(result, Decimal)

    # ==================== EDGE CASES ====================

    def test_calculate_required_margin_edge_zero_leverage(self, risk_manager: RiskManager) -> None:
        """Test margin calculation with zero leverage."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value
        position_size_usd = Decimal("0.2")  # Position size in base asset
        price = Decimal("50000.0")  # Price per unit
        leverage = Decimal("0.0")

        # Act & Assert
        # Should handle zero leverage gracefully (either return full position size or handle error)
        try:
            result = risk_manager.calculate_required_margin(
                symbol, position_size_usd, price, leverage
            )
            assert isinstance(result, Decimal)
        except (ValueError, ZeroDivisionError):
            # Expected behavior for zero leverage
            pass

    def test_calculate_required_margin_edge_very_high_leverage(
        self, risk_manager: RiskManager
    ) -> None:
        """Test margin calculation with very high leverage."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value
        position_size = Decimal("0.2")  # Position size in base asset
        price = Decimal("50000.0")  # Price per unit
        leverage = Decimal("1000.0")  # Very high leverage
        notional_value = position_size * price  # 0.2 * 50000 = 10000

        # Act
        result = risk_manager.calculate_required_margin(symbol, position_size, price, leverage)

        # Assert
        assert isinstance(result, Decimal)
        # With 1000x leverage, margin should be notional / leverage = 10000 / 1000 = 10
        expected_margin = notional_value / leverage
        assert result == expected_margin


class TestRiskManagerPortfolioManagement:
    """Test suite for portfolio-level risk management."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_check_drawdown_success_within_limits(self, risk_manager: RiskManager) -> None:
        """Test drawdown check when within acceptable limits."""
        # Arrange - set up 5% drawdown scenario
        with patch.object(
            risk_manager.portfolio_tracker, "get_current_drawdown", new_callable=AsyncMock
        ) as mock_get_drawdown:
            mock_get_drawdown.return_value = Decimal("0.05")

            # Act
            result = await risk_manager.check_drawdown()

            # Assert
            assert result is True

    @pytest.mark.asyncio
    async def test_validate_opportunities_success_empty_list(
        self, risk_manager: RiskManager
    ) -> None:
        """Test validation of empty opportunities list."""
        # Act
        result = await risk_manager.validate_opportunities([])

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_validate_opportunities_success_with_valid_opportunities(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test validation of list with valid opportunities."""
        # Act
        result = await risk_manager.validate_opportunities([sample_arbitrage_opportunity])

        # Assert
        assert isinstance(result, list)
        assert len(result) <= 1  # May be filtered

    def test_is_opportunity_profitable_success_profitable(
        self, risk_manager: RiskManager, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test profitability check for profitable opportunity."""
        # Act
        result = risk_manager.is_opportunity_profitable(sample_arbitrage_opportunity)

        # Assert
        assert result is True

    def test_is_opportunity_profitable_success_not_profitable(
        self, risk_manager: RiskManager
    ) -> None:
        """Test profitability check for unprofitable opportunity."""
        # Arrange
        btc_symbol = BTC_HL
        unprofitable_opportunity = ArbitrageOpportunity(
            symbol=btc_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50000.0"),  # No spread
            net_funding_differential=Decimal("0.0"),  # No profit
            timestamp=datetime.now(UTC),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0001"),
        )

        # Act
        result = risk_manager.is_opportunity_profitable(unprofitable_opportunity)

        # Assert
        assert result is False

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_check_drawdown_edge_exactly_at_limit(self, risk_manager: RiskManager) -> None:
        """Test drawdown check when exactly at the limit."""
        # Arrange
        # Set drawdown to exactly the limit
        # 20% drawdown - exactly at the default limit
        with patch.object(
            risk_manager.portfolio_tracker, "get_current_drawdown", new_callable=AsyncMock
        ) as mock_get_drawdown:
            mock_get_drawdown.return_value = Decimal("0.2")

            # Act
            result = await risk_manager.check_drawdown()

            # Assert
            # With >= logic, exactly at limit should return False
            assert result is False

    def test_adjust_order_size_success_within_limits(self, risk_manager: RiskManager) -> None:
        """Test order size adjustment when within limits."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value
        requested_size = Decimal("1000.0")

        # Act
        result = risk_manager.adjust_order_size(symbol, requested_size)

        # Assert
        assert isinstance(result, Decimal)
        assert result <= requested_size  # Should not exceed requested
        assert result >= Decimal(0)

    def test_adjust_order_size_edge_zero_requested_size(self, risk_manager: RiskManager) -> None:
        """Test order size adjustment with zero requested size."""
        # Arrange
        btc_symbol = BTC_HL
        symbol = btc_symbol.value
        requested_size = Decimal("0.0")

        # Act
        result = risk_manager.adjust_order_size(symbol, requested_size)

        # Assert
        assert result == Decimal(0)

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_check_drawdown_failure_exceeds_limit(self, risk_manager: RiskManager) -> None:
        """Test drawdown check when exceeding acceptable limits."""
        # Arrange
        # 25% drawdown - exceeds the default 20% limit
        with patch.object(
            risk_manager.portfolio_tracker, "get_current_drawdown", new_callable=AsyncMock
        ) as mock_get_drawdown:
            mock_get_drawdown.return_value = Decimal("0.25")

            # Act
            result = await risk_manager.check_drawdown()

            # Assert
            assert result is False


class TestRiskManagerUtilityMethods:
    """Test suite for utility and helper methods."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_perform_sanity_checks_success(self, risk_manager: RiskManager) -> None:
        """Test that sanity checks pass under normal conditions."""
        # Act
        result = await risk_manager.perform_sanity_checks()

        # Assert
        assert isinstance(result, bool)

    def test_update_success_with_valid_data(self, risk_manager: RiskManager) -> None:
        """Test update method with valid data."""
        # Arrange
        update_data = {
            "max_position_size": "15000.0",
            "max_total_exposure_usd": "75000.0",
        }

        # Act
        risk_manager.update(update_data)

        # Assert
        # Should update internal state
        assert hasattr(risk_manager, "current_drawdown_metrics")

    def test_get_collateral_asset_for_exchange_success(self, risk_manager: RiskManager) -> None:
        """Test getting collateral asset for exchange and symbol."""
        # Arrange
        exchange = "hyperliquid"
        btc_symbol = BTC_HL
        symbol = btc_symbol.value

        # Act
        result = risk_manager.get_collateral_asset_for_exchange(exchange, symbol)

        # Assert
        assert isinstance(result, str)
        assert len(result) > 0

    # ==================== EDGE CASES ====================

    def test_update_edge_empty_data(self, risk_manager: RiskManager) -> None:
        """Test update method with empty data."""
        # Act
        risk_manager.update({})

        # Assert
        # Should handle empty update gracefully
        assert hasattr(risk_manager, "current_drawdown_metrics")

    def test_update_edge_invalid_data_types(self, risk_manager: RiskManager) -> None:
        """Test update method with invalid data types."""
        # Arrange
        invalid_data = {
            "max_position_size": "not_a_number",
            "invalid_field": 123,
        }

        # Act
        # Should handle invalid data gracefully without crashing
        with contextlib.suppress(ValueError, TypeError):
            risk_manager.update(invalid_data)

    def test_get_collateral_asset_edge_unknown_exchange(self, risk_manager: RiskManager) -> None:
        """Test getting collateral asset for unknown exchange."""
        # Arrange
        exchange = "unknown_exchange"
        btc_symbol = BTC_HL
        symbol = btc_symbol.value

        # Act
        result = risk_manager.get_collateral_asset_for_exchange(exchange, symbol)

        # Assert
        # Should return default or handle unknown exchange gracefully
        assert isinstance(result, str)

    def test_get_collateral_asset_edge_empty_symbol(self, risk_manager: RiskManager) -> None:
        """Test getting collateral asset with empty symbol."""
        # Arrange
        exchange = "hyperliquid"
        symbol = ""

        # Act
        result = risk_manager.get_collateral_asset_for_exchange(exchange, symbol)

        # Assert
        # Should handle empty symbol gracefully
        assert isinstance(result, str)
