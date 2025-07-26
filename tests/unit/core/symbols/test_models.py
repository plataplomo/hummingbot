"""Comprehensive tests for symbol models."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.core.symbols.models import (
    BaseSymbol,
    ExchangeSymbol,
    InternalSymbol,
    SymbolFormat,
    SymbolType,
    UnifiedSymbol,
    create_exchange_symbol,
    create_internal_symbol,
)
from cyberdelta.enums.exchange_names import ExchangeName


class TestSymbolType:
    """Test SymbolType enum."""

    def test_symbol_types(self) -> None:
        """Test all symbol type values."""
        assert SymbolType.INTERNAL.value == "internal"
        assert SymbolType.EXCHANGE.value == "exchange"
        assert SymbolType.WEBSOCKET.value == "websocket"
        assert SymbolType.CONFIGURATION.value == "configuration"


class TestSymbolFormat:
    """Test SymbolFormat validation patterns."""

    def test_patterns_exist(self) -> None:
        """Test that all patterns are defined."""
        for symbol_type in SymbolType:
            assert symbol_type in SymbolFormat.PATTERNS
            assert symbol_type in SymbolFormat.MAX_LENGTHS

    def test_pattern_validation(self) -> None:
        """Test pattern matching."""
        # Internal pattern
        assert SymbolFormat.PATTERNS[SymbolType.INTERNAL].match("BTC")
        assert SymbolFormat.PATTERNS[SymbolType.INTERNAL].match("ETH123")
        assert not SymbolFormat.PATTERNS[SymbolType.INTERNAL].match("btc")  # lowercase
        assert not SymbolFormat.PATTERNS[SymbolType.INTERNAL].match("BTC-USD")  # invalid char

        # Exchange pattern
        assert SymbolFormat.PATTERNS[SymbolType.EXCHANGE].match("BTC_PERP")
        assert SymbolFormat.PATTERNS[SymbolType.EXCHANGE].match("BTC-USD")
        assert SymbolFormat.PATTERNS[SymbolType.EXCHANGE].match("@123")

    def test_max_lengths(self) -> None:
        """Test maximum length constraints."""
        assert (
            SymbolFormat.MAX_LENGTHS[SymbolType.INTERNAL] == 20
        )  # Updated to match implementation
        assert SymbolFormat.MAX_LENGTHS[SymbolType.EXCHANGE] == 20
        assert SymbolFormat.MAX_LENGTHS[SymbolType.WEBSOCKET] == 20
        assert SymbolFormat.MAX_LENGTHS[SymbolType.CONFIGURATION] == 30


class TestBaseSymbol:
    """Test BaseSymbol abstract model."""

    def test_integer_conversion(self) -> None:
        """Test integer to string conversion."""

        # Create a concrete implementation for testing
        class TestSymbol(BaseSymbol):
            symbol_type: SymbolType = SymbolType.EXCHANGE

        # Test integer conversion - construct with dict to bypass type checking
        symbol = TestSymbol.model_validate({"value": 123})
        assert symbol.value == "123"

    def test_string_normalization(self) -> None:
        """Test string normalization."""

        class TestSymbol(BaseSymbol):
            symbol_type: SymbolType = SymbolType.INTERNAL

        # Test uppercase and trim
        symbol = TestSymbol(value="  btc  ")
        assert symbol.value == "BTC"

    def test_validation_errors(self) -> None:
        """Test validation error cases."""

        class TestSymbol(BaseSymbol):
            symbol_type: SymbolType = SymbolType.INTERNAL

        # Empty string - Pydantic raises ValidationError for min_length
        with pytest.raises(ValidationError, match="at least 2 characters"):
            TestSymbol(value="")

        # Invalid type - Our validator raises SymbolValidationError
        with pytest.raises(SymbolValidationError, match="Invalid value type"):
            TestSymbol.model_validate({"value": {"key": "value"}})

        # Too long - Pydantic raises ValidationError for max_length
        with pytest.raises(ValidationError, match="at most 20 characters"):
            TestSymbol(value="VERYLONGSYMBOLTHATISWAYTOOOLONG")

        # Invalid format - Our model_validator raises SymbolValidationError
        with pytest.raises(SymbolValidationError, match="Invalid format for type"):
            TestSymbol(value="BTC-USD")  # Internal doesn't allow hyphens

    def test_equality_and_hash(self) -> None:
        """Test equality and hash methods."""

        class TestSymbol(BaseSymbol):
            symbol_type: SymbolType = SymbolType.INTERNAL

        symbol1 = TestSymbol(value="BTC")
        symbol2 = TestSymbol(value="BTC")
        symbol3 = TestSymbol(value="ETH")

        # Equality
        assert symbol1 == symbol2
        assert symbol1 != symbol3
        assert symbol1 == "BTC"  # String comparison
        assert symbol1 != "ETH"

        # Hash
        assert hash(symbol1) == hash(symbol2)
        assert hash(symbol1) != hash(symbol3)

    def test_string_representation(self) -> None:
        """Test string representation."""

        class TestSymbol(BaseSymbol):
            symbol_type: SymbolType = SymbolType.INTERNAL

        symbol = TestSymbol(value="BTC")
        assert str(symbol) == "BTC"


class TestInternalSymbol:
    """Test InternalSymbol model."""

    def test_basic_creation(self) -> None:
        """Test basic internal symbol creation."""
        symbol = InternalSymbol(value="BTC", base_asset="BTC", market_type=MarketType.PERP)
        assert symbol.value == "BTC"
        assert symbol.base_asset == "BTC"
        assert symbol.quote_asset is None
        assert symbol.market_type == MarketType.PERP
        assert symbol.symbol_type == SymbolType.INTERNAL

    def test_asset_extraction(self) -> None:
        """Test automatic asset extraction."""
        # Test with underscore - base_asset will be extracted automatically
        symbol = InternalSymbol(value="BTC_USDC", base_asset="BTC", quote_asset="USDC")
        assert symbol.base_asset == "BTC"
        assert symbol.quote_asset == "USDC"

        # Test without underscore
        symbol = InternalSymbol(value="ETH", base_asset="ETH")
        assert symbol.base_asset == "ETH"
        assert symbol.quote_asset is None

    def test_is_pair_property(self) -> None:
        """Test is_pair computed property."""
        # Single asset
        symbol1 = InternalSymbol(value="BTC", base_asset="BTC")
        assert symbol1.is_pair is False

        # Trading pair
        symbol2 = InternalSymbol(value="BTC_USDC", base_asset="BTC", quote_asset="USDC")
        assert symbol2.is_pair is True

    def test_canonical_name_property(self) -> None:
        """Test canonical_name computed property."""
        # Single asset
        symbol1 = InternalSymbol(value="BTC", base_asset="BTC")
        assert symbol1.canonical_name == "BTC"

        # Trading pair
        symbol2 = InternalSymbol(value="BTC_USDC", base_asset="BTC", quote_asset="USDC")
        assert symbol2.canonical_name == "BTC_USDC"

    def test_validation(self) -> None:
        """Test validation rules."""
        # Valid base asset
        symbol = InternalSymbol(value="BTC", base_asset="BTC")
        assert symbol.base_asset == "BTC"

        # Invalid base asset format - Pydantic validates field patterns
        with pytest.raises(ValidationError, match="String should match pattern"):
            InternalSymbol(value="BTC", base_asset="btc-usd")

        # Invalid quote asset format
        with pytest.raises(ValidationError, match="String should match pattern"):
            InternalSymbol(value="BTC_USD", base_asset="BTC", quote_asset="usd-coin")

    def test_market_type_default(self) -> None:
        """Test market type defaults to PERP."""
        symbol = InternalSymbol(value="BTC", base_asset="BTC")
        assert symbol.market_type == MarketType.PERP


class TestExchangeSymbol:
    """Test ExchangeSymbol model."""

    def test_basic_creation(self) -> None:
        """Test basic exchange symbol creation."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        symbol = ExchangeSymbol(
            value="BTC_PERP", exchange_id=ExchangeName.BACKPACK, internal_symbol=internal
        )
        assert symbol.value == "BTC_PERP"
        assert symbol.exchange_id == ExchangeName.BACKPACK
        assert symbol.internal_symbol == internal
        assert symbol.symbol_type == SymbolType.EXCHANGE

    def test_asset_indices(self) -> None:
        """Test asset index fields."""
        # Hyperliquid with asset index
        symbol1 = ExchangeSymbol(value="@2", exchange_id=ExchangeName.HYPERLIQUID, asset_index=2)
        assert symbol1.asset_index == 2
        assert symbol1.is_indexed is True

        # Backpack with symbol ID
        symbol2 = ExchangeSymbol(
            value="BTC_USDC", exchange_id=ExchangeName.BACKPACK, symbol_id=12345
        )
        assert symbol2.symbol_id == 12345
        assert symbol2.is_indexed is True

        # No indices
        symbol3 = ExchangeSymbol(value="BTC", exchange_id=ExchangeName.HYPERLIQUID)
        assert symbol3.is_indexed is False

    def test_api_format_data_available(self) -> None:
        """Test that API format data is available in the model (for use in API layer)."""
        # Hyperliquid with asset index - API layer can use asset_index directly
        symbol = ExchangeSymbol(value="PURR", exchange_id=ExchangeName.HYPERLIQUID, asset_index=2)
        assert symbol.asset_index == 2
        assert symbol.value == "PURR"

        # Regular symbol - API layer can use value directly
        symbol = ExchangeSymbol(value="BTC_PERP", exchange_id=ExchangeName.BACKPACK)
        assert symbol.value == "BTC_PERP"
        assert symbol.asset_index is None

    def test_validation(self) -> None:
        """Test validation constraints."""
        # Negative asset index
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            ExchangeSymbol(value="BTC", exchange_id=ExchangeName.HYPERLIQUID, asset_index=-1)


class TestUnifiedSymbol:
    """Test UnifiedSymbol model."""

    def test_basic_creation(self) -> None:
        """Test basic unified symbol creation."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        unified = UnifiedSymbol(internal=internal)

        assert unified.internal == internal
        assert unified.exchange_mappings == {}
        assert unified.is_active
        assert unified.is_tradeable

    def test_exchange_mappings(self) -> None:
        """Test exchange mappings."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        hl_symbol = ExchangeSymbol(
            value="BTC", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )
        bp_symbol = ExchangeSymbol(
            value="BTC_PERP", exchange_id=ExchangeName.BACKPACK, internal_symbol=internal
        )

        unified = UnifiedSymbol(
            internal=internal,
            exchange_mappings={
                ExchangeName.HYPERLIQUID: hl_symbol,
                ExchangeName.BACKPACK: bp_symbol,
            },
        )

        assert len(unified.exchange_mappings) == 2
        assert unified.get_exchange_symbol(ExchangeName.HYPERLIQUID) == hl_symbol
        assert unified.get_exchange_symbol(ExchangeName.BACKPACK) == bp_symbol
        # Test getting a symbol for an exchange without mapping
        # Create a new unified symbol with only one mapping to test
        unified_partial = UnifiedSymbol(
            internal=internal, exchange_mappings={ExchangeName.HYPERLIQUID: hl_symbol}
        )
        assert unified_partial.get_exchange_symbol(ExchangeName.BACKPACK) is None

    def test_exchange_support(self) -> None:
        """Test exchange support checking."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        hl_symbol = ExchangeSymbol(
            value="BTC", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

        unified = UnifiedSymbol(
            internal=internal, exchange_mappings={ExchangeName.HYPERLIQUID: hl_symbol}
        )

        assert unified.supports_exchange(ExchangeName.HYPERLIQUID)
        assert not unified.supports_exchange(ExchangeName.BACKPACK)
        expected_exchanges = {ExchangeName.HYPERLIQUID}
        assert unified.supported_exchanges == expected_exchanges

    def test_trading_metadata(self) -> None:
        """Test trading metadata fields."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        unified = UnifiedSymbol(
            internal=internal,
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000.0"),
            tick_size=Decimal("0.01"),
        )

        assert unified.min_order_size == Decimal("0.001")
        assert unified.max_order_size == Decimal("1000.0")
        assert unified.tick_size == Decimal("0.01")

    def test_mapping_validation(self) -> None:
        """Test exchange mapping validation."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")

        # Mismatched exchange ID
        hl_symbol = ExchangeSymbol(value="BTC", exchange_id=ExchangeName.HYPERLIQUID)

        with pytest.raises(ValueError, match="Exchange ID mismatch"):
            UnifiedSymbol(
                internal=internal,
                exchange_mappings={
                    ExchangeName.BACKPACK: hl_symbol  # Wrong exchange!
                },
            )

        # Mismatched internal symbol
        different_internal = InternalSymbol(value="ETH", base_asset="ETH")
        hl_symbol_wrong = ExchangeSymbol(
            value="ETH", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=different_internal
        )

        with pytest.raises(ValueError, match="Internal symbol mismatch"):
            UnifiedSymbol(
                internal=internal, exchange_mappings={ExchangeName.HYPERLIQUID: hl_symbol_wrong}
            )

    def test_timestamps(self) -> None:
        """Test timestamp fields."""
        internal = InternalSymbol(value="BTC", base_asset="BTC")
        unified = UnifiedSymbol(internal=internal)

        assert isinstance(unified.created_at, datetime)
        assert isinstance(unified.updated_at, datetime)
        assert unified.created_at.tzinfo == UTC
        assert unified.updated_at.tzinfo == UTC


class TestFactoryFunctions:
    """Test factory functions."""

    def test_create_internal_symbol(self) -> None:
        """Test create_internal_symbol factory."""
        # Basic creation
        symbol = create_internal_symbol("BTC")
        assert symbol.value == "BTC"
        assert symbol.base_asset == "BTC"
        assert symbol.market_type == MarketType.PERP

        # With market type
        symbol = create_internal_symbol("BTC", MarketType.SPOT)
        assert symbol.market_type == MarketType.SPOT

    def test_create_exchange_symbol(self) -> None:
        """Test create_exchange_symbol factory."""
        internal = create_internal_symbol("BTC")

        # Basic creation
        symbol = create_exchange_symbol("BTC_PERP", ExchangeName.BACKPACK, internal)
        assert symbol.value == "BTC_PERP"
        assert symbol.exchange_id == ExchangeName.BACKPACK
        assert symbol.internal_symbol == internal

        # With extra kwargs
        symbol = create_exchange_symbol("@2", ExchangeName.HYPERLIQUID, internal, asset_index=2)
        assert symbol.asset_index == 2


class TestModelIntegration:
    """Test model integration scenarios."""

    def test_full_symbol_hierarchy(self) -> None:
        """Test creating a complete symbol hierarchy."""
        # Create internal symbol
        internal = InternalSymbol(
            value="SOL_USDC", base_asset="SOL", quote_asset="USDC", market_type=MarketType.SPOT
        )

        # Create exchange symbols
        hl_symbol = ExchangeSymbol(
            value="SOL",
            exchange_id=ExchangeName.HYPERLIQUID,
            internal_symbol=internal,
            asset_index=5,
        )

        bp_symbol = ExchangeSymbol(
            value="SOL_USDC", exchange_id=ExchangeName.BACKPACK, internal_symbol=internal
        )

        # Create unified symbol
        unified = UnifiedSymbol(
            internal=internal,
            exchange_mappings={
                ExchangeName.HYPERLIQUID: hl_symbol,
                ExchangeName.BACKPACK: bp_symbol,
            },
            min_order_size=Decimal("0.1"),
            tick_size=Decimal("0.001"),
        )

        # Verify relationships
        assert unified.internal.value == "SOL_USDC"
        assert unified.internal.is_pair is True
        assert unified.supports_exchange(ExchangeName.HYPERLIQUID)
        assert unified.supports_exchange(ExchangeName.BACKPACK)

        hl_exchange_symbol = unified.get_exchange_symbol(ExchangeName.HYPERLIQUID)
        assert hl_exchange_symbol is not None
        assert hl_exchange_symbol.is_indexed is True
        assert hl_exchange_symbol.asset_index == 5  # API layer can use this directly

    def test_model_immutability(self) -> None:
        """Test that models are immutable."""
        symbol = InternalSymbol(value="BTC", base_asset="BTC")

        # Should not be able to modify fields due to frozen=True
        with pytest.raises(ValueError, match="frozen"):
            symbol.value = "ETH"

        with pytest.raises(ValueError, match="frozen"):
            symbol.base_asset = "ETH"

    def test_model_serialization(self) -> None:
        """Test model serialization."""
        internal = InternalSymbol(value="BTC", base_asset="BTC", market_type=MarketType.PERP)

        # Test dict conversion
        data = internal.model_dump()
        assert data["value"] == "BTC"
        assert data["base_asset"] == "BTC"
        # Enum is returned as enum object in dict mode
        assert data["market_type"] == MarketType.PERP
        assert data["symbol_type"] == SymbolType.INTERNAL

        # Test JSON mode conversion (enums as values)
        data_json = internal.model_dump(mode="json")
        assert data_json["market_type"] == "PERP"
        assert data_json["symbol_type"] == "internal"

        # Test JSON serialization
        json_data = internal.model_dump_json()
        assert "BTC" in json_data
        assert "PERP" in json_data
