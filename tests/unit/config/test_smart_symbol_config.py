# tests/unit/config/test_smart_symbol_config.py
"""Unit tests for Smart Symbol Configuration System.

Validates that the smart configuration generates identical UnifiedSymbolConfig
objects as the verbose configuration format.
"""

import time

import pytest

from cyberdelta.config.models.smart_symbol_generator import SmartSymbolGenerator
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig, SymbolPatterns
from cyberdelta.config.models.symbol_configs import (
    ExchangeSymbolConfig,
    InternalSymbolConfig,
    UnifiedSymbolConfig,
)


class TestSymbolPatterns:
    """Test SymbolPatterns model validation."""

    def test_valid_patterns(self) -> None:
        """Test valid pattern configuration."""
        patterns = SymbolPatterns(
            hyperliquid={"perp": "{symbol}", "spot": "{base}/{quote}"},
            backpack={"perp": "{symbol}_PERP", "spot": "{base}_{quote}"},
        )

        assert patterns.hyperliquid["perp"] == "{symbol}"
        assert patterns.backpack["perp"] == "{symbol}_PERP"

    def test_missing_perp_pattern_fails(self) -> None:
        """Test that missing perp pattern fails validation."""
        with pytest.raises(ValueError, match="must include 'perp' market type"):
            SymbolPatterns(
                hyperliquid={"spot": "{base}/{quote}"},  # Missing perp
                backpack={"perp": "{symbol}_PERP"},
            )

    def test_invalid_pattern_format_fails(self) -> None:
        """Test that patterns without placeholders fail validation."""
        with pytest.raises(ValueError, match=r"must contain.*placeholder"):
            SymbolPatterns(
                hyperliquid={"perp": "FIXED_VALUE"},  # No placeholder
                backpack={"perp": "{symbol}_PERP"},
            )


class TestSmartSymbolsConfig:
    """Test SmartSymbolsConfig model validation."""

    def test_valid_smart_config(self) -> None:
        """Test valid smart symbol configuration."""
        config = SmartSymbolsConfig(
            list=["BTC", "ETH", "SOL"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
            ),
            defaults={"market_type": "PERP"},
            overrides={},
        )

        assert len(config.list) == 3
        assert "BTC" in config.list
        assert config.defaults["market_type"] == "PERP"

    def test_symbol_validation(self) -> None:
        """Test that symbol list validation uses existing SymbolValidator."""
        # Valid symbols should pass
        config = SmartSymbolsConfig(
            list=["BTC", "ETH", "SOL123"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
            ),
        )
        assert config.list == ["BTC", "ETH", "SOL123"]

    def test_invalid_market_type_fails(self) -> None:
        """Test that invalid market type in defaults fails."""
        with pytest.raises(ValueError, match="Invalid market_type"):
            SmartSymbolsConfig(
                list=["BTC"],
                patterns=SymbolPatterns(
                    hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
                ),
                defaults={"market_type": "INVALID"},
            )

    def test_invalid_exchange_override_fails(self) -> None:
        """Test that invalid exchange in overrides fails."""
        with pytest.raises(ValueError, match="Invalid exchange"):
            SmartSymbolsConfig(
                list=["BTC"],
                patterns=SymbolPatterns(
                    hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
                ),
                overrides={"BTC": {"invalid_exchange": "BTC_CUSTOM"}},
            )


class TestSmartSymbolGenerator:
    """Test SmartSymbolGenerator functionality."""

    @pytest.fixture
    def smart_config(self) -> SmartSymbolsConfig:
        """Create test smart configuration."""
        return SmartSymbolsConfig(
            list=["BTC", "ETH"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}", "spot": "{base}/{quote}"},
                backpack={"perp": "{symbol}_PERP", "spot": "{base}_{quote}"},
            ),
            defaults={"market_type": "PERP"},
            overrides={},
        )

    def test_generate_unified_symbols(self, smart_config: SmartSymbolsConfig) -> None:
        """Test generating unified symbols from smart config."""
        generator = SmartSymbolGenerator(smart_config)
        symbols = generator.generate_unified_symbols()

        assert len(symbols) == 2

        # Check BTC symbol
        btc_symbol = next(s for s in symbols if s.internal.value == "BTC")
        assert btc_symbol.internal.value == "BTC"
        assert btc_symbol.internal.base_asset == "BTC"
        assert btc_symbol.internal.quote_asset is None
        assert btc_symbol.internal.market_type == "PERP"

        # Check exchange mappings
        assert "hyperliquid" in btc_symbol.exchange_mappings
        assert "backpack" in btc_symbol.exchange_mappings
        assert btc_symbol.exchange_mappings["hyperliquid"].value == "BTC"
        assert btc_symbol.exchange_mappings["backpack"].value == "BTC_PERP"

    def test_pattern_override(self) -> None:
        """Test that symbol overrides work correctly."""
        config = SmartSymbolsConfig(
            list=["BTC", "SPECIAL"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
            ),
            defaults={"market_type": "PERP"},
            overrides={"SPECIAL": {"hyperliquid": "CUSTOM_HL", "backpack": "CUSTOM_BP"}},
        )

        generator = SmartSymbolGenerator(config)
        symbols = generator.generate_unified_symbols()

        special_symbol = next(s for s in symbols if s.internal.value == "SPECIAL")
        assert special_symbol.exchange_mappings["hyperliquid"].value == "CUSTOM_HL"
        assert special_symbol.exchange_mappings["backpack"].value == "CUSTOM_BP"

    def test_spot_pattern_formatting(self) -> None:
        """Test spot market pattern formatting with base/quote assets."""
        config = SmartSymbolsConfig(
            list=["BTC"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}", "spot": "{base}/{quote}"},
                backpack={"perp": "{symbol}_PERP", "spot": "{base}_{quote}"},
            ),
            defaults={"market_type": "SPOT"},
            overrides={},
        )

        generator = SmartSymbolGenerator(config)
        symbols = generator.generate_unified_symbols()

        btc_symbol = symbols[0]
        # Note: Spot patterns use 'USDC' as default quote
        assert btc_symbol.exchange_mappings["hyperliquid"].value == "BTC/USDC"
        assert btc_symbol.exchange_mappings["backpack"].value == "BTC_USDC"


class TestSmartConfigEquivalence:
    """Test that smart config generates equivalent results to verbose config."""

    def test_smart_config_matches_verbose_output(self) -> None:
        """Test that smart config generates identical UnifiedSymbolConfig objects."""
        # Create smart configuration
        smart_config = SmartSymbolsConfig(
            list=["BTC", "ETH"],
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
            ),
            defaults={"market_type": "PERP"},
            overrides={},
        )

        # Generate from smart config
        generator = SmartSymbolGenerator(smart_config)
        smart_symbols = generator.generate_unified_symbols()

        # Create equivalent verbose config (what we used to have)
        verbose_symbols = [
            UnifiedSymbolConfig(
                internal=InternalSymbolConfig(
                    value="BTC", base_asset="BTC", quote_asset=None, market_type="PERP"
                ),
                exchange_mappings={
                    "hyperliquid": ExchangeSymbolConfig(
                        value="BTC", exchange_id="hyperliquid", asset_index=None, symbol_id=None
                    ),
                    "backpack": ExchangeSymbolConfig(
                        value="BTC_PERP", exchange_id="backpack", asset_index=None, symbol_id=None
                    ),
                },
            ),
            UnifiedSymbolConfig(
                internal=InternalSymbolConfig(
                    value="ETH", base_asset="ETH", quote_asset=None, market_type="PERP"
                ),
                exchange_mappings={
                    "hyperliquid": ExchangeSymbolConfig(
                        value="ETH", exchange_id="hyperliquid", asset_index=None, symbol_id=None
                    ),
                    "backpack": ExchangeSymbolConfig(
                        value="ETH_PERP", exchange_id="backpack", asset_index=None, symbol_id=None
                    ),
                },
            ),
        ]

        # Validate identical output
        assert len(smart_symbols) == len(verbose_symbols)

        for smart, verbose in zip(smart_symbols, verbose_symbols, strict=False):
            # Check internal symbol matches
            assert smart.internal.value == verbose.internal.value
            assert smart.internal.base_asset == verbose.internal.base_asset
            assert smart.internal.quote_asset == verbose.internal.quote_asset
            assert smart.internal.market_type == verbose.internal.market_type

            # Check exchange mappings match
            assert smart.exchange_mappings.keys() == verbose.exchange_mappings.keys()
            for exchange_id in smart.exchange_mappings:
                assert (
                    smart.exchange_mappings[exchange_id].value
                    == verbose.exchange_mappings[exchange_id].value
                )
                assert (
                    smart.exchange_mappings[exchange_id].exchange_id
                    == verbose.exchange_mappings[exchange_id].exchange_id
                )

    def test_performance_improvement(self) -> None:
        """Test that smart config processes much faster than verbose equivalent."""
        # Large symbol list to measure performance difference
        large_symbol_list = [f"SYM{i:03d}" for i in range(100)]

        # Create smart config
        smart_config = SmartSymbolsConfig(
            list=large_symbol_list,
            patterns=SymbolPatterns(
                hyperliquid={"perp": "{symbol}"}, backpack={"perp": "{symbol}_PERP"}
            ),
            defaults={"market_type": "PERP"},
            overrides={},
        )

        # Time smart generation
        start_time = time.perf_counter()
        generator = SmartSymbolGenerator(smart_config)
        smart_symbols = generator.generate_unified_symbols()
        smart_time = time.perf_counter() - start_time

        # Verify we got all symbols
        assert len(smart_symbols) == 100

        # Smart generation should be very fast (< 100ms for 100 symbols)
        assert smart_time < 0.1, f"Smart generation too slow: {smart_time:.3f}s"

        # Smart config generated symbols successfully within performance requirements
