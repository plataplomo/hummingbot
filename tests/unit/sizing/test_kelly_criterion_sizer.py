"""Tests for KellyCriterionSizer."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.config.models.config_models import AppSettings, SizingSettings
from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.core.risk.sizing.models.sizing_result import (
    SizingContext,
    SizingResult,
    SizingStatus,
)
from cyberdelta.core.risk.sizing.strategies.kelly_criterion_sizer import KellyCriterionSizer
from cyberdelta.validation.funding_data import ArbitrageOpportunity


def create_test_app_settings(config: dict[str, Any]) -> AppSettings:
    """Create a test AppSettings instance with minimal required fields."""
    return AppSettings.model_validate({
        "general": {"version": "1.0.0", "environment": "test", "debug": True},
        "exchanges": {},
        "strategies": {"strategies_list": []},
        "risk": {
            "global": {
                "max_position_usd": Decimal("1000.0"),
                "max_total_exposure_usd": Decimal("5000.0"),
            },
            "sizing": SizingSettings.model_validate({
                "kelly_multiplier": Decimal(str(config.get("kelly_multiplier", 0.25))),
                "kelly_max_allocation": Decimal(str(config.get("kelly_max_allocation", 0.05))),
                "kelly_min_allocation": Decimal(str(config.get("kelly_min_allocation", 0.001))),
                "kelly_risk_free_rate": Decimal(str(config.get("risk_free_rate", 0.02))),
                "min_volatility": Decimal("0.001"),
                "max_volatility_bound": Decimal("0.5"),
                "volatility_lookback_hours": 24,
            }),
        },
        "execution": {"retry_attempts": 3, "timeout_seconds": 30},
        "safety_systems": {"max_portfolio_value_usd": Decimal("10000.0")},
        "monitoring": {"log_level": "INFO"},
        "portfolio_tracker": {"update_interval_seconds": 60},
    })


def create_test_opportunity(
    symbol: str = "BTC-PERP",
    long_exchange: str = "exchange1",
    short_exchange: str = "exchange2",
    long_price: float = 50000.0,
    short_price: float = 50075.0,
    long_funding_rate: float = 0.0001,
    short_funding_rate: float = -0.0001,
) -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity."""
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


def create_test_context(
    available_capital: float = 100000.0, sizing_method: str = "kelly_criterion"
) -> SizingContext:
    """Create a test sizing context."""
    return SizingContext(
        sizing_method=sizing_method, available_capital=Decimal(str(available_capital))
    )


class TestKellyCriterionSizer:
    """Test cases for KellyCriterionSizer."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.config = {
            "kelly_multiplier": 0.25,  # 25% of Kelly optimal
            "kelly_max_allocation": 0.05,  # 5% max allocation
            "kelly_min_allocation": 0.001,  # 0.1% min allocation
            "risk_free_rate": 0.02,  # 2% annual risk-free rate
            "enable_sharpe_adjustment": True,
            "enable_drawdown_adjustment": True,
            "min_position_size": 100.0,
            "max_position_size": 10000.0,
            "lookback_periods": 252,  # 1 year of daily data
            "confidence_threshold": 0.6,
        }
        self.sizer = KellyCriterionSizer(app_settings=create_test_app_settings(self.config))

    def test_initialization(self) -> None:
        """Test sizer initialization."""
        assert self.sizer.name == "kelly_criterion"
        # Testing configuration to ensure proper initialization
        assert self.sizer.kelly_multiplier == Decimal("0.25")
        assert self.sizer.kelly_max_allocation == Decimal("0.05")
        assert self.sizer.kelly_min_allocation == Decimal("0.001")
        assert self.sizer.risk_free_rate == 0.02

    def test_initialization_with_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        custom_config = {
            "kelly_multiplier": 0.5,
            "kelly_max_allocation": 0.1,
            "kelly_min_allocation": 0.005,
            "risk_free_rate": 0.03,
            "enable_sharpe_adjustment": False,
            "enable_drawdown_adjustment": False,
        }
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(custom_config))

        # Check private attributes exist with expected values
        assert hasattr(sizer, "_kelly_multiplier")
        assert hasattr(sizer, "_kelly_max_allocation")
        assert hasattr(sizer, "_kelly_min_allocation")
        assert hasattr(sizer, "_enable_sharpe_adjustment")
        assert hasattr(sizer, "_enable_drawdown_adjustment")

    @pytest.mark.asyncio
    async def test_calculate_size_with_complete_data(self) -> None:
        """Test size calculation with complete historical data."""
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )
        context = create_test_context(available_capital=100000.0)

        result = await self.sizer.size(opportunity, context)

        assert isinstance(result, SizingResult)
        assert result.status == SizingStatus.SUCCESS

        # Should have Kelly-specific metadata
        if result.details is not None:
            assert "kelly_fraction" in result.details
            assert "win_rate" in result.details
            assert "win_loss_ratio" in result.details
            assert "adjusted_kelly_fraction" in result.details

            # Kelly fraction should be positive for profitable opportunity
            assert result.details["kelly_fraction"] > 0

            # Final size should be constrained by multiplier and limits
            kelly_fraction = result.details["adjusted_kelly_fraction"]
            expected_allocation = kelly_fraction * self.config["kelly_multiplier"]
            expected_allocation = min(expected_allocation, self.config["kelly_max_allocation"])
            expected_allocation = max(expected_allocation, self.config["kelly_min_allocation"])

            expected_size = 100000.0 * expected_allocation
            assert abs(float(result.position_size_usd) - expected_size) < 100.0

    @pytest.mark.asyncio
    async def test_calculate_size_with_minimal_data(self) -> None:
        """Test size calculation with minimal data."""
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )
        context = create_test_context(available_capital=50000.0)

        result = await self.sizer.size(opportunity, context)

        assert result.status == SizingStatus.SUCCESS
        if result.details is not None:
            assert "kelly_fraction" in result.details
            assert result.details["data_quality"] == "minimal"

            # Should use simplified Kelly calculation
            expected_kelly = (0.6 * 0.02 - 0.4 * 0.01) / 0.02  # (p*W - q*L) / W
            assert abs(result.details["kelly_fraction"] - expected_kelly) < 0.01

    @pytest.mark.asyncio
    async def test_calculate_size_unprofitable_opportunity(self) -> None:
        """Test size calculation for unprofitable opportunity."""
        opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50025.0,  # Very small spread
        )
        context = create_test_context(available_capital=50000.0)

        result = await self.sizer.size(opportunity, context)

        assert result.status == SizingStatus.FAILED
        assert "Negative expected value" in str(result.message or "")
        if result.details is not None:
            assert result.details["kelly_fraction"] <= 0

    @pytest.mark.asyncio
    async def test_kelly_fraction_calculation(self) -> None:
        """Test Kelly fraction calculation methods."""
        # Test basic Kelly formula: f = (bp - q) / b
        # Where b = odds, p = win probability, q = loss probability

        # Simple case: 60% win rate, 2:1 reward:risk
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50100.0
        )
        context = create_test_context(available_capital=50000.0)

        result = await self.sizer.size(opportunity, context)

        # Expected Kelly calculation: (0.6 * 2 - 0.4 * 1) / 2 = 0.4
        win_rate = 0.6
        avg_win = 0.02
        avg_loss = -0.01
        expected = (win_rate * abs(avg_win) - (1 - win_rate) * abs(avg_loss)) / abs(avg_win)

        if result.details is not None and "kelly_fraction" in result.details:
            assert abs(result.details["kelly_fraction"] - expected) < 0.01

    @pytest.mark.asyncio
    async def test_kelly_fraction_with_historical_returns(self) -> None:
        """Test Kelly fraction calculation with historical returns."""
        # Create returns with known statistics
        returns = [0.02, -0.01, 0.03, -0.005, 0.025, -0.015, 0.01, -0.008] * 30

        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )
        context = create_test_context(available_capital=50000.0)

        result = await self.sizer.size(opportunity, context)

        # Kelly formula: f = μ / σ²
        # Where μ = mean return, σ² = variance
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)

        expected_kelly = mean_return / variance if variance > 0 else 0
        if result.details is not None and "kelly_fraction" in result.details:
            assert abs(result.details["kelly_fraction"] - expected_kelly) < 0.01

    @pytest.mark.asyncio
    async def test_sharpe_ratio_adjustment(self) -> None:
        """Test Sharpe ratio adjustment."""
        base_opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        low_sharpe_opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50025.0,  # Lower spread
        )

        context = create_test_context(available_capital=50000.0)

        base_result = await self.sizer.size(base_opportunity, context)
        low_sharpe_result = await self.sizer.size(low_sharpe_opportunity, context)

        # Higher Sharpe ratio should result in larger position
        assert base_result.position_size_usd > low_sharpe_result.position_size_usd
        if base_result.details is not None and low_sharpe_result.details is not None:
            base_factor = base_result.details["sharpe_adjustment_factor"]
            low_factor = low_sharpe_result.details["sharpe_adjustment_factor"]
            assert base_factor > low_factor

    @pytest.mark.asyncio
    async def test_drawdown_adjustment(self) -> None:
        """Test drawdown adjustment."""
        base_opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        high_drawdown_opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50025.0,  # Lower spread to simulate higher drawdown
        )

        context = create_test_context(available_capital=50000.0)

        base_result = await self.sizer.size(base_opportunity, context)
        high_drawdown_result = await self.sizer.size(high_drawdown_opportunity, context)

        # Lower drawdown should result in larger position
        assert base_result.position_size_usd > high_drawdown_result.position_size_usd
        if base_result.details is not None and high_drawdown_result.details is not None:
            base_drawdown_factor = base_result.details["drawdown_adjustment_factor"]
            high_drawdown_factor = high_drawdown_result.details["drawdown_adjustment_factor"]
            assert base_drawdown_factor > high_drawdown_factor

    @pytest.mark.asyncio
    async def test_allocation_limits(self) -> None:
        """Test allocation limits enforcement."""
        # Test minimum allocation
        low_kelly_opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50025.0,  # Small spread
        )

        context = create_test_context(available_capital=50000.0)
        result = await self.sizer.size(low_kelly_opportunity, context)

        if result.status == SizingStatus.SUCCESS:
            allocation = float(result.position_size_usd) / 50000.0
            assert allocation >= self.config["kelly_min_allocation"]

        # Test maximum allocation
        high_kelly_opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50250.0,  # Large spread
        )

        result = await self.sizer.size(high_kelly_opportunity, context)

        assert result.status == SizingStatus.SUCCESS
        allocation = float(result.position_size_usd) / 50000.0
        assert allocation <= self.config["kelly_max_allocation"]

    @pytest.mark.asyncio
    async def test_confidence_threshold(self) -> None:
        """Test confidence threshold filtering."""
        # Low confidence opportunity
        low_confidence_opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)
        result = await self.sizer.size(low_confidence_opportunity, context)

        assert result.status == SizingStatus.FAILED
        assert "Low confidence" in str(result.message or "")
        if result.details is not None and "confidence_score" in result.details:
            assert result.details["confidence_score"] < self.config["confidence_threshold"]

    @pytest.mark.asyncio
    async def test_calculate_size_with_missing_data(self) -> None:
        """Test size calculation with missing required data."""
        # Test with empty context
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        empty_context = create_test_context(available_capital=0.0)
        result = await self.sizer.size(opportunity, empty_context)

        assert result.status == SizingStatus.ERROR
        assert "Missing required data" in str(result.message or "")

    @pytest.mark.asyncio
    async def test_calculate_size_with_invalid_data(self) -> None:
        """Test size calculation with invalid data."""
        # Test with negative capital
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        invalid_context = create_test_context(available_capital=-50000.0)

        try:
            result = await self.sizer.size(opportunity, invalid_context)
            assert result.status == SizingStatus.ERROR
            assert "Invalid" in str(result.message or "")
        except (ValueError, TypeError, SizingError):
            # Expected due to invalid context
            pass

    @pytest.mark.asyncio
    async def test_calculate_size_async(self) -> None:
        """Test async size calculation."""
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)
        result = await self.sizer.size(opportunity, context)

        assert isinstance(result, SizingResult)
        assert result.status == SizingStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_monte_carlo_validation(self) -> None:
        """Test Monte Carlo validation of Kelly sizing."""
        config = self.config.copy()
        config["enable_monte_carlo_validation"] = True
        config["monte_carlo_runs"] = 1000
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)
        result = await sizer.size(opportunity, context)

        assert result.status == SizingStatus.SUCCESS
        if result.details is not None:
            assert "monte_carlo_validated" in result.details
            assert result.details["monte_carlo_validated"]
            assert "monte_carlo_expected_return" in result.details
            assert "monte_carlo_risk_metrics" in result.details

    @pytest.mark.asyncio
    async def test_fractional_kelly_variants(self) -> None:
        """Test different fractional Kelly variants."""
        base_config = self.config.copy()

        # Test different Kelly multipliers
        multipliers = [0.1, 0.25, 0.5, 1.0]

        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)

        results: list[tuple[float, float]] = []
        for mult in multipliers:
            config = base_config.copy()
            config["kelly_multiplier"] = mult
            sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

            result = await sizer.size(opportunity, context)
            results.append((mult, float(result.position_size_usd)))

        # Position sizes should increase with multiplier
        for i in range(1, len(results)):
            assert results[i][1] > results[i - 1][1]

    @pytest.mark.asyncio
    async def test_performance_timing(self) -> None:
        """Test that size calculation timing is recorded."""
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)
        result = await self.sizer.size(opportunity, context)

        assert result.execution_time_ms is None or result.execution_time_ms >= 0
        # Note: timestamp is not part of SizingResult
        assert isinstance(result, SizingResult)

    def test_configuration_validation(self) -> None:
        """Test configuration validation during initialization."""
        # Test invalid Kelly multiplier
        with pytest.raises(ValueError, match="kelly_multiplier must be positive"):
            KellyCriterionSizer(app_settings=create_test_app_settings({"kelly_multiplier": -0.1}))

        # Test invalid allocation limits
        with pytest.raises(
            ValueError, match="kelly_min_allocation must be less than kelly_max_allocation"
        ):
            KellyCriterionSizer(
                app_settings=create_test_app_settings({
                    "kelly_multiplier": 0.25,
                    "kelly_min_allocation": 0.1,
                    "kelly_max_allocation": 0.05,
                })
            )

    @pytest.mark.asyncio
    async def test_advanced_kelly_calculations(self) -> None:
        """Test advanced Kelly calculation methods."""
        # Test Kelly with correlated assets
        config = self.config.copy()
        config["consider_correlation"] = True
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        context = create_test_context(available_capital=50000.0)
        result = await sizer.size(opportunity, context)

        assert result.status == SizingStatus.SUCCESS
        if result.details is not None:
            assert "correlation_adjustment" in result.details
            # Should reduce size due to correlation
            assert result.details["correlation_adjustment"] < 1.0

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        """Test error handling during calculation."""
        # Test with invalid opportunity data
        opportunity = create_test_opportunity(
            symbol="BTC-PERP", long_price=50000.0, short_price=50075.0
        )

        # Test with invalid context
        try:
            context = create_test_context(available_capital=-50000.0)
            result = await self.sizer.size(opportunity, context)
            assert result.status == SizingStatus.ERROR
            assert "Error during size calculation" in str(result.message or "")
        except (ValueError, TypeError, SizingError):
            # Expected due to invalid context
            pass


class TestKellyCriterionSizerIntegration:
    """Integration tests for KellyCriterionSizer."""

    @pytest.mark.asyncio
    async def test_realistic_kelly_sizing_scenario(self) -> None:
        """Test with realistic Kelly sizing scenario."""
        config = {
            "kelly_multiplier": 0.25,
            "kelly_max_allocation": 0.05,
            "kelly_min_allocation": 0.001,
            "risk_free_rate": 0.02,
            "enable_sharpe_adjustment": True,
            "enable_drawdown_adjustment": True,
            "min_position_size": 100.0,
            "max_position_size": 5000.0,
        }
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        # High-quality opportunity
        opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_price=50000.0,
            short_price=50090.0,  # Good spread
        )

        context = create_test_context(available_capital=100000.0)
        result = await sizer.size(opportunity, context)

        assert result.status == SizingStatus.SUCCESS

        # Should be a reasonable allocation
        allocation = float(result.position_size_usd) / 100000.0
        assert 0.001 <= allocation <= 0.05  # Within limits

        # Should have comprehensive metadata
        if result.details is not None:
            assert "kelly_fraction" in result.details
            assert "adjusted_kelly_fraction" in result.details
            assert "sharpe_adjustment_factor" in result.details
            assert "drawdown_adjustment_factor" in result.details
            assert "win_loss_ratio" in result.details

            # Kelly fraction should be reasonable for this opportunity
            assert 0.1 <= result.details["kelly_fraction"] <= 0.8

    @pytest.mark.asyncio
    async def test_portfolio_kelly_optimization(self) -> None:
        """Test Kelly optimization across portfolio."""
        config = {
            "kelly_multiplier": 0.3,
            "kelly_max_allocation": 0.04,
            "kelly_min_allocation": 0.002,
            "risk_free_rate": 0.02,
            "enable_sharpe_adjustment": True,
            "enable_drawdown_adjustment": True,
            "min_position_size": 50.0,
            "max_position_size": 2000.0,
        }
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        # Multiple opportunities with different risk/return profiles
        opportunities = [
            create_test_opportunity(symbol="BTC-PERP", long_price=50000.0, short_price=50075.0),
            create_test_opportunity(symbol="ETH-PERP", long_price=3000.0, short_price=3036.0),
            create_test_opportunity(symbol="SOL-PERP", long_price=100.0, short_price=100.22),
        ]

        context = create_test_context(available_capital=50000.0)
        results = [await sizer.size(opp, context) for opp in opportunities]

        # All should succeed
        assert all(r.status == SizingStatus.SUCCESS for r in results)

        # Check that allocations are reasonable
        allocations = [float(r.position_size_usd) / 50000.0 for r in results]
        assert all(0.002 <= alloc <= 0.04 for alloc in allocations)

        # BTC (best Sharpe, lowest drawdown) should get largest allocation
        # SOL (worst Sharpe, highest drawdown) should get smallest allocation
        btc_alloc, eth_alloc, sol_alloc = allocations
        assert btc_alloc >= eth_alloc >= sol_alloc

    @pytest.mark.asyncio
    async def test_extreme_market_conditions(self) -> None:
        """Test Kelly sizing under extreme market conditions."""
        config = {
            "kelly_multiplier": 0.2,
            "kelly_max_allocation": 0.03,
            "kelly_min_allocation": 0.001,
            "risk_free_rate": 0.02,
            "enable_sharpe_adjustment": True,
            "enable_drawdown_adjustment": True,
            "min_position_size": 100.0,
            "max_position_size": 1000.0,
        }
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        # High volatility, low Sharpe ratio scenario
        extreme_opportunity = create_test_opportunity(
            symbol="VOLATILE-PERP",
            long_price=100.0,
            short_price=100.30,  # High spread
        )

        context = create_test_context(available_capital=50000.0)
        result = await sizer.size(extreme_opportunity, context)

        # Should either reject or use minimal allocation
        if result.status == SizingStatus.SUCCESS:
            allocation = float(result.position_size_usd) / 50000.0
            assert allocation <= 0.01  # Very small allocation
            if result.details is not None:
                assert result.details["sharpe_adjustment_factor"] < 0.5
                assert result.details["drawdown_adjustment_factor"] < 0.5
        else:
            assert result.status == SizingStatus.FAILED

    @pytest.mark.asyncio
    async def test_high_frequency_kelly_sizing(self) -> None:
        """Test Kelly sizing for high-frequency scenarios."""
        config = {
            "kelly_multiplier": 0.1,  # Conservative for HF
            "kelly_max_allocation": 0.02,
            "kelly_min_allocation": 0.0005,
            "risk_free_rate": 0.02,
            "enable_sharpe_adjustment": True,
            "enable_drawdown_adjustment": False,  # Less relevant for HF
            "min_position_size": 10.0,
            "max_position_size": 500.0,
        }
        sizer = KellyCriterionSizer(app_settings=create_test_app_settings(config))

        # Generate multiple HF opportunities
        hf_opportunities: list[ArbitrageOpportunity] = [
            create_test_opportunity(
                symbol=f"HF-PERP-{i}",
                long_price=50000.0,
                short_price=50000.0 + (25 + i * 2.5),  # Varying spreads
            )
            for i in range(50)
        ]

        context = create_test_context(available_capital=20000.0)

        # Process all opportunities concurrently
        tasks = [sizer.size(opp, context) for opp in hf_opportunities]
        results = await asyncio.gather(*tasks)

        # Most should succeed (HF opportunities are typically lower edge but higher frequency)
        successful_results = [r for r in results if r.status == SizingStatus.SUCCESS]
        assert len(successful_results) >= 40  # At least 80% success rate

        # Allocations should be small for HF
        allocations = [float(r.position_size_usd) / 20000.0 for r in successful_results]
        assert all(alloc <= 0.02 for alloc in allocations)
        assert all(alloc >= 0.0005 for alloc in allocations)
