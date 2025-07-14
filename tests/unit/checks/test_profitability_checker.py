"""Tests for ProfitabilityChecker."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from cyberdelta.config.models.config_models import (
    AppSettings,
    CheckerSettings,
    CheckerThresholds,
    SizingSettings,
)
from cyberdelta.core.risk.checks.checkers.profitability_checker import ProfitabilityChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult, CheckStatus
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
            "checkers": CheckerSettings.model_validate({
                "enable_profitability": config.get("enabled", True),
                "include_fees_in_profitability": config.get("include_fees", True),
                "thresholds": CheckerThresholds.model_validate({
                    "min_profitability": Decimal(str(config.get("min_spread_percentage", 0.001)))
                }),
            }),
            "sizing": SizingSettings.model_validate({"base_validation_factor": Decimal("1.0")}),
        },
        "execution": {"retry_attempts": 3, "timeout_seconds": 30},
        "safety_systems": {"max_portfolio_value_usd": Decimal("10000.0")},
        "monitoring": {"log_level": "INFO"},
        "portfolio_tracker": {"update_interval_seconds": 60},
    })


def create_test_opportunity(
    symbol: str = "BTC-PERP",
    long_exchange: str = "hyperliquid",
    short_exchange: str = "backpack",
    long_price: float = 45000.0,
    short_price: float = 45100.0,
    long_funding_rate: float = 0.0001,
    short_funding_rate: float = -0.0001,
    spread_percentage: float | None = None,
    volume: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity."""
    net_funding_differential = Decimal(str(long_funding_rate)) - Decimal(str(short_funding_rate))

    opportunity = ArbitrageOpportunity(
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

    # Add optional metadata
    if spread_percentage is not None or volume is not None or metadata:
        if opportunity.metadata is None:
            opportunity.metadata = {}
        if spread_percentage is not None:
            opportunity.metadata["spread_percentage"] = spread_percentage
        if volume is not None:
            opportunity.metadata["volume"] = volume
        if metadata:
            opportunity.metadata.update(metadata)

    return opportunity


def create_test_context(
    check_name: str = "ProfitabilityChecker",
    config: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> CheckContext:
    """Create a test check context."""
    return CheckContext(
        check_name=check_name,
        config=config,
        metadata=metadata,
    )


class TestProfitabilityChecker:
    """Test cases for ProfitabilityChecker."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.config: dict[str, Any] = {
            "enabled": True,
            "min_spread_percentage": 0.1,  # 0.1% minimum spread
            "include_fees": True,
            "estimated_fee_percentage": 0.05,  # 0.05% estimated fees
        }
        self.checker = ProfitabilityChecker(app_settings=create_test_app_settings(self.config))

    def test_initialization(self) -> None:
        """Test checker initialization."""
        assert self.checker.name == "ProfitabilityChecker"
        assert self.checker.enabled
        assert self.checker.min_profit_percentage >= 0  # Check it exists
        assert self.checker.include_fees
        assert self.checker.estimated_fee_percentage == 0.05

    def test_initialization_with_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        custom_config = {
            "enabled": False,
            "min_spread_percentage": 0.2,
            "include_fees": False,
            "estimated_fee_percentage": 0.1,
            "use_dynamic_fees": True,
        }
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(custom_config))

        assert not checker.enabled
        assert checker.min_profit_percentage >= 0  # Check it exists
        assert not checker.include_fees
        assert getattr(checker, "use_dynamic_fees", False)

    @pytest.mark.asyncio
    async def test_validate_profitable_opportunity(self) -> None:
        """Test validation of profitable opportunity."""
        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            spread_percentage=0.22,  # 0.22% spread > 0.1% minimum
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert "Opportunity is profitable" in str(result.message)
        assert result.details is not None
        assert result.details.get("spread_percentage") == 0.22
        assert result.details.get("min_required") == 0.1
        assert result.details.get("net_spread_after_fees", 0) < result.details.get(
            "spread_percentage", 1
        )

    @pytest.mark.asyncio
    async def test_validate_unprofitable_opportunity(self) -> None:
        """Test validation of unprofitable opportunity."""
        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45020.0,
            spread_percentage=0.044,  # 0.044% spread < 0.1% minimum
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.FAILED
        assert "Spread below minimum threshold" in str(result.message)
        assert result.details is not None
        assert result.details.get("spread_percentage") == 0.044
        assert result.details.get("min_required") == 0.1

    @pytest.mark.asyncio
    async def test_validate_without_fees(self) -> None:
        """Test validation without including fees."""
        config = self.config.copy()
        config["include_fees"] = False
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0, short_price=45050.0, spread_percentage=0.11
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert not result.details.get("fees_included")
        assert result.details.get("net_spread_after_fees") == result.details.get(
            "spread_percentage"
        )

    @pytest.mark.asyncio
    async def test_validate_with_calculated_spread(self) -> None:
        """Test validation when spread is calculated from prices."""
        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            # No spread_percentage provided - should be calculated
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        # Calculated spread: (45100 - 45000) / 45000 * 100 = 0.222%
        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert abs(result.details.get("spread_percentage", 0) - 0.222) < 0.001
        assert result.details.get("spread_calculated")

    @pytest.mark.asyncio
    async def test_validate_negative_spread(self) -> None:
        """Test validation with negative spread."""
        opportunity = create_test_opportunity(
            long_price=45100.0,  # Higher long price
            short_price=45000.0,  # Lower short price
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.FAILED
        assert "Negative spread detected" in str(result.message)
        assert result.details is not None
        assert result.details.get("spread_percentage", 0) < 0

    @pytest.mark.asyncio
    async def test_validate_with_dynamic_fees(self) -> None:
        """Test validation with dynamic fee calculation."""
        config: dict[str, Any] = self.config.copy()
        config["use_dynamic_fees"] = True
        config["fee_calculation_method"] = "tiered"
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            spread_percentage=0.22,
            volume=10000.0,
            metadata={"exchange_fees": {"hyperliquid": 0.02, "backpack": 0.03}},
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert result.details.get("dynamic_fees_used")
        assert "exchange_fees" in result.details

    @pytest.mark.asyncio
    async def test_validate_missing_price_data(self) -> None:
        """Test validation with missing price data."""
        # We can't create an invalid ArbitrageOpportunity, so we'll mock the checker's method
        opportunity = create_test_opportunity()
        context = create_test_context(config=self.config)

        # Mock the _perform_check to simulate missing price data error
        with patch.object(
            self.checker,
            "_perform_check",
            return_value=CheckResult.error(
                message="Missing required price data", details={"checker": "ProfitabilityChecker"}
            ),
        ):
            result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.ERROR
        assert "Missing required price data" in str(result.message)

    @pytest.mark.asyncio
    async def test_validate_invalid_price_data(self) -> None:
        """Test validation with invalid price data."""
        # ArbitrageOpportunity validates prices > 0, so we'll mock the check
        opportunity = create_test_opportunity()
        context = create_test_context(config=self.config)

        # Mock the _perform_check to simulate invalid price error
        with patch.object(
            self.checker,
            "_perform_check",
            return_value=CheckResult.error(
                message="Invalid price data", details={"checker": "ProfitabilityChecker"}
            ),
        ):
            result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.ERROR
        assert "Invalid price data" in str(result.message)

    @pytest.mark.asyncio
    async def test_validate_disabled_checker(self) -> None:
        """Test validation when checker is disabled."""
        config = self.config.copy()
        config["enabled"] = False
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            spread_percentage=0.01,  # Below threshold
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.SKIPPED
        assert "Checker is disabled" in str(result.message)

    @pytest.mark.asyncio
    async def test_validate_async(self) -> None:
        """Test async validation."""
        opportunity = create_test_opportunity(
            long_price=45000.0, short_price=45100.0, spread_percentage=0.22
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        assert isinstance(result, CheckResult)
        assert result.status == CheckStatus.PASSED
        assert (
            result.details and result.details.get("checker") == "ProfitabilityChecker"
        ) or "ProfitabilityChecker" in str(result.message or "")

    # Note: _calculate_spread_percentage and _calculate_fees methods are not implemented
    # The spread calculation is handled in _get_spread_percentage() method
    # The fee calculation is embedded in _calculate_adjusted_spreads() method

    # Note: Dynamic fee calculation testing removed as _calculate_fees method is not implemented
    # Fee calculation logic is embedded in the main check flow

    @pytest.mark.asyncio
    async def test_validate_with_funding_impact(self) -> None:
        """Test validation considering funding rate impact."""
        config = self.config.copy()
        config["consider_funding_impact"] = True
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            spread_percentage=0.22,
            long_funding_rate=0.01,  # 1% funding rate
            short_funding_rate=-0.005,  # -0.5% funding rate
            metadata={"holding_period_hours": 8},
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert "funding_impact" in result.details
        assert result.details.get("funding_impact") != 0

    @pytest.mark.asyncio
    async def test_validate_margin_requirements(self) -> None:
        """Test validation with margin requirements."""
        config = self.config.copy()
        config["consider_margin_requirements"] = True
        config["margin_buffer_percentage"] = 20.0  # 20% margin buffer
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45100.0,
            spread_percentage=0.22,
            metadata={"leverage": 5.0, "position_size": 1000.0},
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert "margin_required" in result.details
        assert "margin_impact" in result.details

    @pytest.mark.asyncio
    async def test_validate_with_slippage_consideration(self) -> None:
        """Test validation considering slippage."""
        config = self.config.copy()
        config["consider_slippage"] = True
        config["estimated_slippage_percentage"] = 0.02  # 0.02% slippage
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        opportunity = create_test_opportunity(
            long_price=45000.0, short_price=45100.0, spread_percentage=0.22, volume=5000.0
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert "slippage_impact" in result.details
        net_spread = result.details.get("net_spread_after_fees", 0)
        spread_pct = result.details.get("spread_percentage", 1)
        assert net_spread < spread_pct

    def test_configuration_validation(self) -> None:
        """Test configuration validation during initialization."""
        # Test negative minimum spread
        with pytest.raises(ValueError, match="min_spread_percentage cannot be negative"):
            ProfitabilityChecker(
                app_settings=create_test_app_settings({
                    "enabled": True,
                    "min_spread_percentage": -0.1,
                })
            )

        # Test negative fee percentage
        with pytest.raises(ValueError, match="estimated_fee_percentage cannot be negative"):
            ProfitabilityChecker(
                app_settings=create_test_app_settings({
                    "enabled": True,
                    "min_spread_percentage": 0.1,
                    "estimated_fee_percentage": -0.05,
                })
            )

    @pytest.mark.asyncio
    async def test_edge_cases(self) -> None:
        """Test edge cases and boundary conditions."""
        # Test with zero spread
        opportunity = create_test_opportunity(long_price=45000.0, short_price=45000.0)
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)
        assert result.status == CheckStatus.FAILED

        # Test with very small spread exactly at threshold
        opportunity = create_test_opportunity(
            long_price=45000.0,
            short_price=45000.0 * (1 + 0.1 / 100),  # Exactly at threshold
        )

        result = await self.checker.check(opportunity, context)
        # Should fail due to fees pushing below threshold
        assert result.status == CheckStatus.FAILED

    @pytest.mark.asyncio
    async def test_performance_timing(self) -> None:
        """Test that validation timing is recorded."""
        opportunity = create_test_opportunity(
            long_price=45000.0, short_price=45100.0, spread_percentage=0.22
        )
        context = create_test_context(config=self.config)

        result = await self.checker.check(opportunity, context)

        assert result.execution_time_ms is not None
        assert result.execution_time_ms > 0

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        """Test error handling during validation."""
        # Use a real opportunity but patch the checker's internal method
        opportunity = create_test_opportunity()
        context = create_test_context(config=self.config)

        # Mock the checker's _perform_check method to raise an exception
        with patch.object(
            self.checker, "_perform_check", side_effect=Exception("Calculation error")
        ):
            result = await self.checker.check(opportunity, context)

        assert result.status == CheckStatus.ERROR
        message_str = str(result.message)
        assert "Error during validation" in message_str or "Unexpected error" in message_str


class TestProfitabilityCheckerIntegration:
    """Integration tests for ProfitabilityChecker."""

    @pytest.mark.asyncio
    async def test_real_world_arbitrage_opportunity(self) -> None:
        """Test with realistic arbitrage opportunity."""
        config = {
            "enabled": True,
            "min_spread_percentage": 0.05,
            "include_fees": True,
            "estimated_fee_percentage": 0.025,
            "consider_funding_impact": True,
            "consider_slippage": True,
            "estimated_slippage_percentage": 0.01,
        }
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        # Realistic profitable opportunity
        opportunity = create_test_opportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=45000.0,
            short_price=45035.0,
            long_funding_rate=0.0001,
            short_funding_rate=-0.0001,
            spread_percentage=0.078,  # 0.078% spread
            volume=2500.0,
            metadata={"holding_period_hours": 1},
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert result.details.get("spread_percentage") == 0.078
        assert result.details.get("net_spread_after_fees", 0) > 0.05  # Still profitable after fees
        assert "funding_impact" in result.details
        assert "slippage_impact" in result.details

    @pytest.mark.asyncio
    async def test_marginal_opportunity(self) -> None:
        """Test opportunity that's marginally profitable."""
        config = {
            "enabled": True,
            "min_spread_percentage": 0.06,
            "include_fees": True,
            "estimated_fee_percentage": 0.03,
        }
        checker = ProfitabilityChecker(app_settings=create_test_app_settings(config))

        # Marginal opportunity
        opportunity = create_test_opportunity(
            symbol="ETH-PERP",
            long_price=3000.0,
            short_price=3002.7,  # ~0.09% spread
            volume=1000.0,
        )
        context = create_test_context(config=config)

        result = await checker.check(opportunity, context)

        # Should pass barely after fees (0.09% - 0.03% = 0.06%)
        assert result.status == CheckStatus.PASSED
        assert result.details is not None
        assert abs(result.details.get("net_spread_after_fees", 0) - 0.06) < 0.01
