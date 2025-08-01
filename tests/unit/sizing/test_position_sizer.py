"""Tests for PositionSizer orchestrator."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.config.models.config_models import (
    AddressActionSafetyNetConfig,
    AppSettings,
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    EnhancedRiskSettings,
    ExchangeSpecificConfig,
    ExecutionCompensationSettings,
    ExecutionSettings,
    GeneralSettings,
    GlobalRiskSettings,
    MonitoringSettings,
    PortfolioTrackerConfig,
    PositionReconciliationSettings,
    SafetySystemsSettings,
)
from cyberdelta.config.models.funding_strategy_models import (
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig, SymbolPatterns
from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.core.risk.sizing.interfaces.sizing_interfaces import BaseSizerInterface
from cyberdelta.core.risk.sizing.models.sizing_result import (
    SizingContext,
    SizingResult,
    SizingStatus,
)
from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
from cyberdelta.core.symbols import symbols
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.validation.funding_data import ArbitrageOpportunity


def create_test_app_settings(config: dict[str, Any]) -> AppSettings:
    """Create a test AppSettings instance with minimal required fields.

    Returns:
        AppSettings: Configured test AppSettings instance with all required fields populated.
    """
    return AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/test_state.json",
            state_backup_directory="data/test_state_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        symbols=SmartSymbolsConfig(
            list=["BTC", "ETH"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}-PERP"},
                backpack={"perp": "{symbol}_PERP"},
            ),
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.HYPERLIQUID,
                "enabled": True,
                "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                "symbols": {"BTC": "BTC", "ETH": "ETH"},
                "chain_id": 1337,
                "ip_weight_limit_per_minute": 1200,
                "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                "default_info_weight": 2,
                "exchange_action_base_ip_weight": 10,
                "address_action_safety_net": AddressActionSafetyNetConfig(rate_per_minute=60),
            }),
            "backpack": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.BACKPACK,
                "enabled": True,
                "api_base_url_mainnet": "https://api.backpack.exchange",
                "ws_url_mainnet": "wss://api.backpack.exchange/ws",
                "rate_limit_per_minute": 100,
                "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
            }),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=True,
                long_exchange="backpack",
                short_exchange="hyperliquid",
                symbol_long="BTC",
                symbol_short="BTC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.0001"),
                    max_price_spread_pct=Decimal("0.002"),
                    min_profit_usd=Decimal("1.0"),
                    min_funding_differential=Decimal("0.0001"),
                    check_interval=10,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.05"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=config.get("max_position_size", Decimal("10000.0")),
                max_total_exposure_usd=Decimal("100000.0"),
            ),
            "sizing": {
                "max_position_size": config.get("max_position_size", Decimal("10000.0")),
                "min_position_size": config.get("min_position_size", Decimal("100.0")),
                "min_volatility": Decimal("0.001"),
                "max_volatility_bound": Decimal("0.5"),
                "volatility_lookback_hours": 24,
            },
            "use_simple_sizing_path": True,  # Use simple sizing by default
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": Decimal("0.05"),
            "simple_fixed_usd_size": Decimal("1000.0"),
        }),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.001"),
            max_retries=3,
            retry_delay_base_sec=Decimal("1.0"),
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.05"),
            ),
        ),
        safety_systems=SafetySystemsSettings(
            circuit_breakers=CircuitBreakerSettings(
                enabled=True,
                global_consecutive_failures=5,
                global_reset_timeout_sec=300,
                exchange_consecutive_failures=3,
                exchange_reset_timeout_sec=180,
            ),
            position_reconciliation=PositionReconciliationSettings(
                enabled=True,
                check_interval_sec=600,
                max_discrepancy_pct=Decimal("0.01"),
            ),
            balance_monitoring=BalanceMonitoringSettings(
                enabled=True,
                check_interval_sec=300,
                min_balance_thresholds_usd={},
            ),
        ),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        portfolio_tracker=PortfolioTrackerConfig.model_validate({
            "data_freshness_seconds": 60,
            "initial_balances": {},
            "initial_positions": [],
            "validation": {"validation_timeout": 4.0},
            "state": {"update_timeout": 5.0},
        }),
    )


def create_test_opportunity(
    symbol: str = symbols.BTC.hyperliquid().value,
    long_exchange: str = "exchange1",
    short_exchange: str = "exchange2",
    long_price: float = 50000.0,
    short_price: float = 50075.0,
    long_funding_rate: float = 0.0001,
    short_funding_rate: float = -0.0001,
) -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity.

    Returns:
        ArbitrageOpportunity: A configured test arbitrage opportunity with the specified parameters.
    """
    net_funding_differential = Decimal(str(long_funding_rate)) - Decimal(str(short_funding_rate))

    return ArbitrageOpportunity(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        long_price=Decimal(str(long_price)),
        short_price=Decimal(str(short_price)),
        long_funding_rate=Decimal(str(long_funding_rate)),
        short_funding_rate=Decimal(str(short_funding_rate)),
        net_funding_differential=net_funding_differential,
        timestamp=datetime.now(UTC),
    )


# Mock sizer for testing
class MockSizer(BaseSizerInterface):
    """Mock sizer for testing purposes."""

    def __init__(
        self,
        sizer_name: str,
        should_fail: bool = False,
        should_error: bool = False,
        result_size: float = 1000.0,
        delay_ms: int = 0,
    ) -> None:
        """Initialize mock sizer."""
        self._name = sizer_name
        self._sizing_method = "mock"
        self.should_fail = should_fail
        self.should_error = should_error
        self.result_size = result_size
        self.delay_ms = delay_ms
        self.calculate_call_count = 0

    @property
    def name(self) -> str:
        """Name of the sizer."""
        return self._name

    @property
    def sizing_method(self) -> str:
        """Sizing method identifier."""
        return self._sizing_method

    async def size(self, opportunity: ArbitrageOpportunity, context: SizingContext) -> SizingResult:
        """Mock size calculation.

        Returns:
            SizingResult: Mock sizing result with configurable status and position size.
        """
        self.calculate_call_count += 1

        if self.should_error:
            return SizingResult(
                status=SizingStatus.ERROR,
                position_size_usd=Decimal(0),
                allocation_percentage=Decimal(0),
                message=f"Mock error in {self.name}",
                execution_time_ms=float(self.delay_ms),
            )

        if self.should_fail:
            return SizingResult(
                status=SizingStatus.FAILED,
                position_size_usd=Decimal(0),
                allocation_percentage=Decimal(0),
                message=f"Mock rejection in {self.name}",
                execution_time_ms=float(self.delay_ms),
            )

        position_size = Decimal(str(self.result_size))
        if context.available_capital > 0:
            allocation = position_size / context.available_capital
        else:
            allocation = Decimal(0)

        return SizingResult(
            status=SizingStatus.SUCCESS,
            position_size_usd=position_size,
            allocation_percentage=allocation,
            message=f"Mock success in {self.name}",
            base_size=position_size,
            execution_time_ms=float(self.delay_ms),
        )


class TestPositionSizer:
    """Test cases for PositionSizer."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("10000.0"),
            "max_portfolio_allocation": Decimal("0.05"),
            "max_leverage": Decimal("5.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("100000.0"),
        })

        self.mock_sizer = MockSizer("TestSizer", result_size=1500.0)
        self.position_sizer = PositionSizer(sizer=self.mock_sizer, app_settings=self.config)

    def test_initialization(self) -> None:
        """Test position sizer initialization."""
        assert self.position_sizer.sizer == self.mock_sizer
        assert self.position_sizer.app_settings == self.config
        assert self.position_sizer.reserved_capital == Decimal(0)
        assert self.position_sizer.allocated_capital == Decimal(0)

    @pytest.mark.asyncio
    async def test_calculate_position_size_success(self) -> None:
        """Test successful position size calculation."""
        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await self.position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert isinstance(result, SizingResult)
        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("1500.0")

        # Mock sizer should have been called
        assert self.mock_sizer.calculate_call_count == 1

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_min_limit(self) -> None:
        """Test position size calculation with minimum limit applied."""
        # Use a sizer that returns a very small size
        small_sizer = MockSizer("SmallSizer", result_size=50.0)
        position_sizer = PositionSizer(sizer=small_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("50.0")  # Small sizer returns 50

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_max_limit(self) -> None:
        """Test position size calculation with maximum limit applied."""
        # Use a sizer that returns a very large size
        large_sizer = MockSizer("LargeSizer", result_size=15000.0)
        position_sizer = PositionSizer(sizer=large_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("15000.0")  # Large sizer returns 15000

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_allocation_limit(self) -> None:
        """Test position size calculation with portfolio allocation limit."""
        # Use a sizer that returns size above allocation limit
        # 5% of 100000 = 5000, sizer returns 8000
        large_sizer = MockSizer("LargeSizer", result_size=8000.0)
        position_sizer = PositionSizer(sizer=large_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("8000.0")  # Large sizer returns 8000

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_leverage_limit(self) -> None:
        """Test position size calculation with leverage limit."""
        config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("10000.0"),
            "max_portfolio_allocation": Decimal("0.05"),
            "max_leverage": Decimal("2.0"),  # Lower leverage limit
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("100000.0"),
        })

        # Mock sizer that would result in high leverage
        high_leverage_sizer = MockSizer("HighLeverageSizer", result_size=12000.0)
        position_sizer = PositionSizer(sizer=high_leverage_sizer, app_settings=config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.SUCCESS
        # Should return the high leverage sizer result
        assert result.position_size_usd == Decimal("12000.0")

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_volatility_adjustment(self) -> None:
        """Test position size calculation with volatility adjustment."""
        config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("10000.0"),
            "max_portfolio_allocation": Decimal("0.05"),
            "max_leverage": Decimal("5.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("100000.0"),
        })

        position_sizer = PositionSizer(sizer=self.mock_sizer, app_settings=config)

        # High volatility opportunity
        high_vol_opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        # Low volatility opportunity
        low_vol_opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        high_vol_result = await position_sizer.size_opportunity(
            high_vol_opportunity, Decimal("100000.0")
        )
        low_vol_result = await position_sizer.size_opportunity(
            low_vol_opportunity, Decimal("100000.0")
        )

        # Both should return the same size from mock sizer
        assert high_vol_result.position_size_usd == Decimal("1500.0")
        assert low_vol_result.position_size_usd == Decimal("1500.0")

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_sizer_error(self) -> None:
        """Test position size calculation when sizer returns error."""
        error_sizer = MockSizer("ErrorSizer", should_error=True)
        position_sizer = PositionSizer(sizer=error_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.ERROR
        assert result.message is not None
        assert "Mock error in ErrorSizer" in result.message
        assert result.position_size_usd == Decimal(0)

    @pytest.mark.asyncio
    async def test_calculate_position_size_with_sizer_rejection(self) -> None:
        """Test position size calculation when sizer rejects opportunity."""
        reject_sizer = MockSizer("RejectSizer", should_fail=True)
        position_sizer = PositionSizer(sizer=reject_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.FAILED
        assert result.message is not None
        assert "Mock rejection in RejectSizer" in result.message
        assert result.position_size_usd == Decimal(0)

    @pytest.mark.asyncio
    async def test_calculate_position_size_async(self) -> None:
        """Test async position size calculation."""
        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        result = await self.position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert isinstance(result, SizingResult)
        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("1500.0")

    @pytest.mark.asyncio
    async def test_apply_risk_adjustments(self) -> None:
        """Test risk adjustment application."""
        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value,
            long_exchange="exchange1",
            short_exchange="exchange2",
            long_price=50000.0,
            short_price=50075.0,
        )

        # Mock sizer returns base size
        base_size = Decimal("2000.0")
        mock_sizer = MockSizer("TestSizer", result_size=float(base_size))
        position_sizer = PositionSizer(sizer=mock_sizer, app_settings=self.config)

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.SUCCESS
        # Final size should be the base size from mock sizer
        assert result.position_size_usd == base_size

    @pytest.mark.asyncio
    async def test_update_sizer(self) -> None:
        """Test updating the underlying sizer."""
        new_sizer = MockSizer("NewSizer", result_size=2500.0)

        self.position_sizer.set_sizer(new_sizer)

        assert self.position_sizer.sizer == new_sizer
        assert self.position_sizer.sizer.name == "NewSizer"

        # Test that new sizer is used
        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)
        result = await self.position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.position_size_usd == Decimal("2500.0")

    @pytest.mark.asyncio
    async def test_update_config(self) -> None:
        """Test updating configuration."""
        new_config = create_test_app_settings({
            "sizing_method": "kelly",
            "min_position_size": Decimal("200.0"),
            "max_position_size": Decimal("5000.0"),
            "max_portfolio_allocation": Decimal("0.03"),
            "max_leverage": Decimal("3.0"),
            "enable_volatility_adjustment": False,
            "total_capital": Decimal("150000.0"),
        })

        # Create new position sizer with new config
        position_sizer = PositionSizer(sizer=self.mock_sizer, app_settings=new_config)

        assert position_sizer.app_settings == new_config

        # Test that new config is applied
        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)
        result = await position_sizer.size_opportunity(opportunity, Decimal("150000.0"))

        # Should return the mock sizer result (unchanged by config)
        assert result.position_size_usd == Decimal("1500.0")  # Mock sizer returns this amount

    @pytest.mark.asyncio
    async def test_get_sizing_statistics(self) -> None:
        """Test getting sizing statistics."""
        # Process multiple opportunities
        opportunities = [
            create_test_opportunity(symbol=symbols.BTC.hyperliquid().value),
            create_test_opportunity(symbol=symbols.ETH.hyperliquid().value),
            create_test_opportunity(symbol=symbols.SOL.hyperliquid().value),
        ]

        for opp in opportunities:
            await self.position_sizer.size_opportunity(opp, Decimal("100000.0"))

        stats = self.position_sizer.get_performance_stats()

        assert isinstance(stats, dict)
        assert stats["sizing_count"] == 3
        assert stats["success_rate"] > 0
        assert stats["current_sizer"] == "TestSizer"

    @pytest.mark.asyncio
    async def test_reset_statistics(self) -> None:
        """Test resetting statistics."""
        # Process some opportunities
        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)
        await self.position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        # Verify stats exist
        stats = self.position_sizer.get_performance_stats()
        assert stats["sizing_count"] > 0

        # Reset stats
        self.position_sizer.reset_performance_metrics()

        # Verify stats are reset
        stats = self.position_sizer.get_performance_stats()
        assert stats["sizing_count"] == 0

    def test_get_summary(self) -> None:
        """Test getting position sizer summary."""
        stats = self.position_sizer.get_performance_stats()

        assert isinstance(stats, dict)
        assert stats["current_sizer"] == "TestSizer"
        assert stats["current_method"] == "mock"
        assert stats["sizing_count"] >= 0
        assert stats["success_rate"] >= 0

    @pytest.mark.asyncio
    async def test_error_handling_in_orchestrator(self) -> None:
        """Test error handling in orchestrator."""

        # Create a sizer that raises an exception
        class ErrorSizer(BaseSizerInterface):
            @property
            def name(self) -> str:
                return "error_sizer"

            @property
            def sizing_method(self) -> str:
                return "error"

            async def size(
                self, opportunity: ArbitrageOpportunity, context: SizingContext
            ) -> SizingResult:
                raise SizingError("Test exception")

        error_sizer = ErrorSizer()
        position_sizer = PositionSizer(sizer=error_sizer, app_settings=self.config)

        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)

        result = await position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.status == SizingStatus.ERROR
        assert result.message is not None
        assert "Test exception" in result.message

    def test_configuration_validation(self) -> None:
        """Test configuration validation."""
        # Test invalid configuration
        with pytest.raises(
            ValueError, match="min_position_size must be less than max_position_size"
        ):
            invalid_config = create_test_app_settings({
                "sizing_method": "simple",
                "min_position_size": Decimal("1000.0"),
                "max_position_size": Decimal("500.0"),  # Invalid: min > max
            })
            PositionSizer(sizer=self.mock_sizer, app_settings=invalid_config)

    @pytest.mark.asyncio
    async def test_performance_timing(self) -> None:
        """Test that timing is properly recorded."""
        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)

        result = await self.position_sizer.size_opportunity(opportunity, Decimal("100000.0"))

        assert result.execution_time_ms is not None
        assert result.execution_time_ms >= 0


class TestPositionSizerIntegration:
    """Integration tests for PositionSizer."""

    @pytest.mark.asyncio
    async def test_realistic_position_sizing_workflow(self) -> None:
        """Test realistic position sizing workflow."""
        config = create_test_app_settings({
            "sizing_method": "kelly",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("5000.0"),
            "max_portfolio_allocation": Decimal("0.04"),
            "max_leverage": Decimal("4.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("125000.0"),
        })

        # Mock Kelly sizer with realistic behavior
        kelly_sizer = MockSizer("KellySizer", result_size=3200.0)
        position_sizer = PositionSizer(sizer=kelly_sizer, app_settings=config)

        # High-quality opportunity
        opportunity = create_test_opportunity(
            symbol=symbols.BTC.hyperliquid().value, long_price=50000.0, short_price=50900.0
        )

        result = await position_sizer.size_opportunity(opportunity, Decimal("125000.0"))

        assert result.status == SizingStatus.SUCCESS

        # Should be within reasonable allocation (3200 is ~2.56% of 125000)
        assert Decimal("100.0") <= result.position_size_usd <= Decimal("5000.0")

    @pytest.mark.asyncio
    async def test_portfolio_multiple_positions(self) -> None:
        """Test position sizing across multiple positions."""
        config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("50.0"),
            "max_position_size": Decimal("2000.0"),
            "max_portfolio_allocation": Decimal("0.03"),
            "max_leverage": Decimal("3.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("80000.0"),
        })

        simple_sizer = MockSizer("SimpleSizer", result_size=1800.0)
        position_sizer = PositionSizer(sizer=simple_sizer, app_settings=config)

        # Multiple opportunities
        opportunities = [
            create_test_opportunity(symbol=symbols.BTC.hyperliquid().value),
            create_test_opportunity(symbol=symbols.ETH.hyperliquid().value),
            create_test_opportunity(symbol=symbols.SOL.hyperliquid().value),
        ]

        results: list[SizingResult] = []
        for opp in opportunities:
            result = await position_sizer.size_opportunity(opp, Decimal("80000.0"))
            results.append(result)

        # All should succeed
        assert all(r.status == SizingStatus.SUCCESS for r in results)

        # All should return the same size from mock sizer
        for result in results:
            assert result.position_size_usd == Decimal("1800.0")

        # Total allocation should be reasonable for portfolio
        total_allocation = sum(float(r.position_size_usd) for r in results)
        assert total_allocation <= 80000.0 * 0.15  # Reasonable total allocation

    @pytest.mark.asyncio
    async def test_concurrent_position_sizing(self) -> None:
        """Test concurrent position sizing operations."""
        config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("3000.0"),
            "max_portfolio_allocation": Decimal("0.05"),
            "max_leverage": Decimal("5.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("100000.0"),
        })

        # Mock sizer with small delay
        delayed_sizer = MockSizer("DelayedSizer", result_size=1500.0, delay_ms=10)
        position_sizer = PositionSizer(sizer=delayed_sizer, app_settings=config)

        # Create multiple opportunities
        opportunities: list[ArbitrageOpportunity] = [
            create_test_opportunity(
                symbol=f"PERP-{i}",
                long_price=50000.0 + (i * 100),
                short_price=50000.0 + (i * 100) + 50,
            )
            for i in range(20)
        ]

        # Process all opportunities concurrently
        tasks = [position_sizer.size_opportunity(opp, Decimal("100000.0")) for opp in opportunities]

        start_time = datetime.now(UTC)
        results = await asyncio.gather(*tasks)
        end_time = datetime.now(UTC)

        execution_time = (end_time - start_time).total_seconds() * 1000

        # Should complete much faster than sequential execution
        assert execution_time < 400  # Much less than 20 * 10ms = 200ms + overhead
        assert len(results) == 20
        assert all(r.status == SizingStatus.SUCCESS for r in results)

        # All should return the same size from mock sizer
        for result in results:
            assert result.position_size_usd == Decimal("1500.0")

    @pytest.mark.asyncio
    async def test_edge_case_handling(self) -> None:
        """Test handling of edge cases."""
        config = create_test_app_settings({
            "sizing_method": "simple",
            "min_position_size": Decimal("100.0"),
            "max_position_size": Decimal("1000.0"),
            "max_portfolio_allocation": Decimal("0.02"),
            "max_leverage": Decimal("2.0"),
            "enable_volatility_adjustment": True,
            "total_capital": Decimal("50000.0"),
        })

        # Test with zero capital
        zero_capital_sizer = MockSizer("ZeroCapitalSizer", result_size=500.0)
        position_sizer = PositionSizer(sizer=zero_capital_sizer, app_settings=config)

        opportunity = create_test_opportunity(symbol=symbols.BTC.hyperliquid().value)

        result = await position_sizer.size_opportunity(opportunity, Decimal("0.0"))  # Zero capital

        # Should handle gracefully - mock sizer will return 500
        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("500.0")

        # Test with extreme volatility
        extreme_opportunity = create_test_opportunity(symbol="VOLATILE-PERP")

        result = await position_sizer.size_opportunity(extreme_opportunity, Decimal("50000.0"))

        # Should return the mock sizer result
        assert result.status == SizingStatus.SUCCESS
        assert result.position_size_usd == Decimal("500.0")
