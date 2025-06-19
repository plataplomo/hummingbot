"""Unit tests for MarketOrderConfig."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig


class TestMarketOrderConfig:
    """Test cases for MarketOrderConfig validation and behavior."""

    def test_default_config_creation(self) -> None:
        """Test creating config with default values."""
        config = MarketOrderConfig()

        assert config.default_slippage_pct == Decimal("0.001")
        assert config.max_slippage_pct == Decimal("0.05")
        assert config.max_price_deviation_pct == Decimal("0.10")
        assert config.min_liquidity_ratio == Decimal("2.0")
        assert config.enabled is True
        assert config.use_all_mids_for_reference is False
        assert config.order_timeout_seconds == 10

    def test_custom_config_creation(self) -> None:
        """Test creating config with custom values."""
        config = MarketOrderConfig(
            default_slippage_pct=Decimal("0.002"),
            max_slippage_pct=Decimal("0.08"),
            max_price_deviation_pct=Decimal("0.15"),
            min_liquidity_ratio=Decimal("3.0"),
            enabled=False,
            order_timeout_seconds=30,
        )

        assert config.default_slippage_pct == Decimal("0.002")
        assert config.max_slippage_pct == Decimal("0.08")
        assert config.max_price_deviation_pct == Decimal("0.15")
        assert config.min_liquidity_ratio == Decimal("3.0")
        assert config.enabled is False
        assert config.order_timeout_seconds == 30

    def test_slippage_by_symbol_default(self) -> None:
        """Test default slippage by symbol configuration."""
        config = MarketOrderConfig()

        assert config.slippage_by_symbol["BTC"] == Decimal("0.005")
        assert config.slippage_by_symbol["ETH"] == Decimal("0.005")
        assert config.slippage_by_symbol["SOL"] == Decimal("0.01")
        assert config.slippage_by_symbol["default"] == Decimal("0.02")

    def test_get_slippage_for_symbol(self) -> None:
        """Test getting slippage for specific symbols."""
        config = MarketOrderConfig()

        assert config.get_slippage_for_symbol("BTC") == Decimal("0.005")
        assert config.get_slippage_for_symbol("ETH") == Decimal("0.005")
        assert config.get_slippage_for_symbol("UNKNOWN") == Decimal("0.02")  # Uses default

    def test_custom_slippage_by_symbol(self) -> None:
        """Test custom slippage by symbol configuration."""
        custom_slippage = {
            "AVAX": Decimal("0.015"),
            "MATIC": Decimal("0.012"),
            "default": Decimal("0.025"),
        }
        config = MarketOrderConfig(slippage_by_symbol=custom_slippage)

        assert config.get_slippage_for_symbol("AVAX") == Decimal("0.015")
        assert config.get_slippage_for_symbol("MATIC") == Decimal("0.012")
        assert config.get_slippage_for_symbol("XYZ") == Decimal("0.025")

    def test_validate_slippage(self) -> None:
        """Test slippage validation method."""
        config = MarketOrderConfig(max_slippage_pct=Decimal("0.05"))

        # Valid slippage under max
        assert config.validate_slippage(Decimal("0.03")) == Decimal("0.03")

        # Slippage exceeds max - should cap
        assert config.validate_slippage(Decimal("0.10")) == Decimal("0.05")

        # Negative slippage - should use default
        assert config.validate_slippage(Decimal("-0.01")) == Decimal("0.001")

        # Non-finite slippage - should use default
        assert config.validate_slippage(Decimal("Infinity")) == Decimal("0.001")

    def test_percentage_validation(self) -> None:
        """Test percentage field validation."""
        # Negative percentage should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(default_slippage_pct=Decimal("-0.01"))
        assert "Percentage must be positive" in str(exc_info.value)

        # Zero percentage should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(max_slippage_pct=Decimal("0"))
        assert "ensure this value is greater than 0" in str(exc_info.value)

        # Too high percentage should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(max_slippage_pct=Decimal("0.25"))
        assert "ensure this value is less than or equal to 0.2" in str(exc_info.value)

    def test_liquidity_ratio_validation(self) -> None:
        """Test liquidity ratio validation."""
        # Valid ratios
        config = MarketOrderConfig(min_liquidity_ratio=Decimal("1.5"))
        assert config.min_liquidity_ratio == Decimal("1.5")

        # Below minimum should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(min_liquidity_ratio=Decimal("0.5"))
        assert "ensure this value is greater than or equal to 1.0" in str(exc_info.value)

        # Above maximum should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(min_liquidity_ratio=Decimal("15.0"))
        assert "ensure this value is less than or equal to 10.0" in str(exc_info.value)

    def test_timeout_validation(self) -> None:
        """Test order timeout validation."""
        # Valid timeout
        config = MarketOrderConfig(order_timeout_seconds=30)
        assert config.order_timeout_seconds == 30

        # Zero timeout should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(order_timeout_seconds=0)
        assert "ensure this value is greater than 0" in str(exc_info.value)

        # Too high timeout should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(order_timeout_seconds=120)
        assert "ensure this value is less than or equal to 60" in str(exc_info.value)

    def test_slippage_map_validation(self) -> None:
        """Test slippage by symbol map validation."""
        # Missing default should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(slippage_by_symbol={"BTC": Decimal("0.01")})
        assert "slippage_by_symbol must contain a 'default' entry" in str(exc_info.value)

        # Invalid slippage value should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(slippage_by_symbol={"default": Decimal("-0.01")})
        assert "Invalid slippage for default" in str(exc_info.value)

        # Non-finite slippage should fail
        with pytest.raises(ValidationError) as exc_info:
            MarketOrderConfig(slippage_by_symbol={"default": Decimal("Infinity")})
        assert "Invalid slippage for default" in str(exc_info.value)

    def test_config_immutability(self) -> None:
        """Test that config is immutable after creation."""
        config = MarketOrderConfig()

        # Should not be able to modify fields
        with pytest.raises(ValidationError):
            config.default_slippage_pct = Decimal("0.002")

        with pytest.raises(ValidationError):
            config.enabled = False

    def test_config_serialization(self) -> None:
        """Test config serialization to dict."""
        config = MarketOrderConfig(
            default_slippage_pct=Decimal("0.002"),
            enabled=False,
        )

        data = config.model_dump()
        assert data["default_slippage_pct"] == Decimal("0.002")
        assert data["enabled"] is False
        assert data["max_slippage_pct"] == Decimal("0.05")

    def test_config_deserialization(self) -> None:
        """Test config deserialization from dict."""
        data = {
            "default_slippage_pct": "0.003",
            "max_slippage_pct": "0.07",
            "enabled": True,
            "slippage_by_symbol": {
                "BTC": "0.004",
                "default": "0.015",
            },
        }

        config = MarketOrderConfig.model_validate(data)
        assert config.default_slippage_pct == Decimal("0.003")
        assert config.max_slippage_pct == Decimal("0.07")
        assert config.slippage_by_symbol["BTC"] == Decimal("0.004")
        assert config.slippage_by_symbol["default"] == Decimal("0.015")
