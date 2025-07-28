"""Comprehensive tests for symbol validators."""

from typing import Any

import pytest

from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.core.symbols.models import (
    ExchangeSymbol,
    SymbolType,
    create_internal_symbol,
)
from cyberdelta.core.symbols.validators import (
    CrossExchangeValidator,
    SymbolValidator,
)
from cyberdelta.enums.exchange_names import ExchangeName


class TestSymbolValidator:
    """Test SymbolValidator class."""

    def test_validate_symbol_basic(self) -> None:
        """Test basic symbol validation."""
        # Valid internal symbol
        result = SymbolValidator.validate_symbol("BTC", SymbolType.INTERNAL)
        assert result == "BTC"

        # Valid exchange symbol
        result = SymbolValidator.validate_symbol("BTC_PERP", SymbolType.EXCHANGE)
        assert result == "BTC_PERP"

        # Valid WebSocket symbol
        result = SymbolValidator.validate_symbol("BTC-USD", SymbolType.WEBSOCKET)
        assert result == "BTC-USD"

    def test_integer_conversion(self) -> None:
        """Test integer to string conversion."""
        # Integer input with Backpack WebSocket context
        result = SymbolValidator.validate_symbol(
            123, SymbolType.EXCHANGE, exchange_id=ExchangeName.BACKPACK, field_name="symbol"
        )
        assert result == "123"

        # Already string
        result = SymbolValidator.validate_symbol("456", SymbolType.EXCHANGE)
        assert result == "456"

        # Integer without proper context should fail
        with pytest.raises(SymbolValidationError, match="Integer symbols only supported"):
            SymbolValidator.validate_symbol(123, SymbolType.EXCHANGE)

    def test_normalization(self) -> None:
        """Test symbol normalization."""
        # Lowercase to uppercase
        result = SymbolValidator.validate_symbol("btc", SymbolType.INTERNAL)
        assert result == "BTC"

        # Strip whitespace
        result = SymbolValidator.validate_symbol("  ETH  ", SymbolType.INTERNAL)
        assert result == "ETH"

        # Both
        result = SymbolValidator.validate_symbol("  sol  ", SymbolType.INTERNAL)
        assert result == "SOL"

    def test_validation_errors(self) -> None:
        """Test validation error cases."""
        # Empty string
        with pytest.raises(SymbolValidationError, match="cannot be empty"):
            SymbolValidator.validate_symbol("", SymbolType.INTERNAL)

        # Wrong type - use Any to test runtime validation
        invalid_value: Any = {"symbol": "BTC"}
        with pytest.raises(SymbolValidationError, match="must be string or int"):
            SymbolValidator.validate_symbol(invalid_value, SymbolType.INTERNAL)

        # Too long (21 characters for internal symbols with max 20)
        with pytest.raises(SymbolValidationError, match="exceeds max length"):
            SymbolValidator.validate_symbol("VERYLONGSYMBOL1234567", SymbolType.INTERNAL)

        # Invalid pattern for internal
        with pytest.raises(SymbolValidationError, match="doesn't match pattern"):
            SymbolValidator.validate_symbol("BTC-USD", SymbolType.INTERNAL)

        # Invalid pattern for exchange
        with pytest.raises(SymbolValidationError, match="doesn't match pattern"):
            SymbolValidator.validate_symbol("BTC/USD", SymbolType.EXCHANGE)

    def test_exchange_specific_validation(self) -> None:
        """Test exchange-specific validation rules."""
        # Hyperliquid asset index format
        result = SymbolValidator.validate_symbol(
            "@2", SymbolType.EXCHANGE, ExchangeName.HYPERLIQUID
        )
        assert result == "@2"

        result = SymbolValidator.validate_symbol(
            "@123", SymbolType.EXCHANGE, ExchangeName.HYPERLIQUID
        )
        assert result == "@123"

        # Invalid asset index format
        with pytest.raises(SymbolValidationError, match="Invalid Hyperliquid asset index"):
            SymbolValidator.validate_symbol("@ABC", SymbolType.EXCHANGE, ExchangeName.HYPERLIQUID)

        # Backpack underscore requirement
        with pytest.raises(SymbolValidationError, match="require underscore separation"):
            SymbolValidator.validate_symbol("BTCUSDC", SymbolType.EXCHANGE, ExchangeName.BACKPACK)

        # Backpack perpetual exception
        result = SymbolValidator.validate_symbol(
            "BTCPERP", SymbolType.EXCHANGE, ExchangeName.BACKPACK
        )
        assert result == "BTCPERP"  # PERP suffix doesn't require underscore

    def test_validate_symbol_pair(self) -> None:
        """Test symbol pair validation."""
        # Valid pair
        result = SymbolValidator.validate_symbol_pair("BTC", "USDC")
        assert result["base"] == "BTC"
        assert result["quote"] == "USDC"
        assert result["pair"] == "BTC_USDC"

        # Normalization applied
        result = SymbolValidator.validate_symbol_pair("btc", "usdc")
        assert result["base"] == "BTC"
        assert result["quote"] == "USDC"

        # Same asset error
        with pytest.raises(SymbolValidationError, match="cannot be the same"):
            SymbolValidator.validate_symbol_pair("BTC", "BTC")

        # With exchange validation
        result = SymbolValidator.validate_symbol_pair("SOL", "USDC", ExchangeName.BACKPACK)
        assert result["pair"] == "SOL_USDC"

    def test_is_valid_symbol(self) -> None:
        """Test is_valid_symbol helper."""
        # Valid symbols
        assert SymbolValidator.is_valid_symbol("BTC", SymbolType.INTERNAL)
        assert SymbolValidator.is_valid_symbol("BTC_PERP", SymbolType.EXCHANGE)
        assert SymbolValidator.is_valid_symbol("@2", SymbolType.EXCHANGE, ExchangeName.HYPERLIQUID)

        # Invalid symbols
        assert not SymbolValidator.is_valid_symbol("", SymbolType.INTERNAL)
        assert not SymbolValidator.is_valid_symbol("btc-usd", SymbolType.INTERNAL)
        assert not SymbolValidator.is_valid_symbol("VERYLONGSYMBOL1234567", SymbolType.INTERNAL)
        assert not SymbolValidator.is_valid_symbol(
            "@ABC", SymbolType.EXCHANGE, ExchangeName.HYPERLIQUID
        )

    def test_suggest_corrections(self) -> None:
        """Test correction suggestions."""
        pytest.skip("suggest_corrections method was removed in symbol refactor")


@pytest.mark.skip(reason="SymbolRegistry was removed in symbol refactor")
class TestCrossExchangeValidator:
    """Test CrossExchangeValidator class."""

    @pytest.fixture
    def setup_registry(self) -> object:  # SymbolRegistry was removed
        """Set up registry with test symbols.
        
        Returns:
            object: Registry placeholder (SymbolRegistry was removed).
        """
        registry = None  # Placeholder since SymbolRegistry was removed

        # Register BTC (available on both exchanges)
        btc_internal = create_internal_symbol("BTC")
        _ = ExchangeSymbol(
            value="BTC", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=btc_internal
        )
        _ = ExchangeSymbol(
            value="BTC_PERP", exchange_id=ExchangeName.BACKPACK, internal_symbol=btc_internal
        )

        # Register ETH (only on Hyperliquid)
        eth_internal = create_internal_symbol("ETH")
        _ = ExchangeSymbol(
            value="ETH", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=eth_internal
        )

        return registry

    def test_validate_arbitrage_pair_success(self, setup_registry: object) -> None:
        """Test successful arbitrage pair validation."""
        result = CrossExchangeValidator.validate_arbitrage_pair(
            "BTC",  # Pass string instead of InternalSymbol object
            [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK],
        )

        assert result["valid"] is True
        assert len(result["exchanges"]) == 2
        assert result["exchanges"]["hyperliquid"]["available"] is True
        assert result["exchanges"]["hyperliquid"]["symbol"] == "BTC"
        assert result["exchanges"]["backpack"]["available"] is True
        assert result["exchanges"]["backpack"]["symbol"] == "BTC_PERP"
        assert len(result["issues"]) == 0

    def test_validate_arbitrage_pair_partial_failure(self, setup_registry: object) -> None:
        """Test partial failure in arbitrage pair validation."""
        result = CrossExchangeValidator.validate_arbitrage_pair(
            "ETH",  # Pass string instead of InternalSymbol object
            [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK],
        )

        assert result["valid"] is False
        assert result["exchanges"]["hyperliquid"]["available"] is True
        assert result["exchanges"]["hyperliquid"]["symbol"] == "ETH"
        assert result["exchanges"]["backpack"]["available"] is False
        assert "error" in result["exchanges"]["backpack"]
        assert len(result["issues"]) == 1
        assert "ETH" in result["issues"][0]
        assert "backpack" in result["issues"][0]

    def test_validate_arbitrage_pair_complete_failure(self, setup_registry: object) -> None:
        """Test complete failure in arbitrage pair validation."""
        result = CrossExchangeValidator.validate_arbitrage_pair(
            "UNKNOWN",  # Pass string instead of InternalSymbol object
            [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK],
        )

        assert result["valid"] is False
        assert result["exchanges"]["hyperliquid"]["available"] is False
        assert result["exchanges"]["backpack"]["available"] is False
        assert len(result["issues"]) == 2

    def test_validate_single_exchange(self, setup_registry: object) -> None:
        """Test validation with single exchange."""
        result = CrossExchangeValidator.validate_arbitrage_pair(
            "BTC",  # Pass string instead of InternalSymbol object
            [ExchangeName.HYPERLIQUID],
        )

        assert result["valid"] is True
        assert len(result["exchanges"]) == 1
        assert result["exchanges"]["hyperliquid"]["available"] is True


class TestValidatorIntegration:
    """Test validator integration scenarios."""

    def test_full_validation_pipeline(self) -> None:
        """Test complete validation pipeline."""
        # Raw input
        raw_symbol = "  btc_perp  "

        # Validate as exchange symbol
        validated = SymbolValidator.validate_symbol(
            raw_symbol, SymbolType.EXCHANGE, ExchangeName.BACKPACK
        )
        assert validated == "BTC_PERP"

        # Validate pair components
        pair_result = SymbolValidator.validate_symbol_pair("btc", "usdc")
        assert pair_result["pair"] == "BTC_USDC"

        # Check if valid
        assert SymbolValidator.is_valid_symbol(validated, SymbolType.EXCHANGE)

    def test_websocket_symbol_validation(self) -> None:
        """Test WebSocket-specific symbol validation."""
        # Integer symbol (Backpack WebSocket)
        result = SymbolValidator.validate_symbol(
            12345, SymbolType.WEBSOCKET, exchange_id=ExchangeName.BACKPACK, field_name="symbol"
        )
        assert result == "12345"

        # Topic format
        result = SymbolValidator.validate_symbol("BTC_USDC", SymbolType.WEBSOCKET)
        assert result == "BTC_USDC"

        # With special characters
        result = SymbolValidator.validate_symbol("BTC-USD", SymbolType.WEBSOCKET)
        assert result == "BTC-USD"

    def test_configuration_symbol_validation(self) -> None:
        """Test configuration file symbol validation."""
        # Longer format allowed
        result = SymbolValidator.validate_symbol("BTC_USDC_PERPETUAL", SymbolType.CONFIGURATION)
        assert result == "BTC_USDC_PERPETUAL"

        # With slash
        result = SymbolValidator.validate_symbol("BTC/USDC", SymbolType.CONFIGURATION)
        assert result == "BTC/USDC"
