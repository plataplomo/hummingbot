"""Unit tests for the unified symbol service."""

from decimal import Decimal
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.core.symbol_service import UnifiedSymbolService, get_symbol_service
from cyberdelta.core.symbols.exceptions import SymbolNotFoundError
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, MarketType, UnifiedSymbol
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


class TestUnifiedSymbolService:
    """Unit tests for UnifiedSymbolService."""

    @pytest.fixture
    def mock_service(self) -> Mock:
        """Create a mock symbol service.
        
        Returns:
            Mock: Mock instance of SymbolService for testing.
        """
        return Mock(spec=SymbolService)

    @pytest.fixture
    def service(self, mock_service: Mock) -> UnifiedSymbolService:
        """Create a UnifiedSymbolService with mocked service.
        
        Returns:
            UnifiedSymbolService: Service instance with mocked dependencies for testing.
        """
        with patch("cyberdelta.core.symbol_service.SymbolService", return_value=mock_service):
            return UnifiedSymbolService()

    @pytest.fixture
    def sample_symbols(self) -> dict[str, Any]:
        """Create sample symbols for testing.
        
        Returns:
            dict[str, Any]: Dictionary containing sample symbol objects for testing.
        """
        btc_internal = InternalSymbol(value="BTC", base_asset="BTC", market_type=MarketType.PERP)

        btc_hl_exchange = ExchangeSymbol(
            value="BTC", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=btc_internal
        )

        btc_bp_exchange = ExchangeSymbol(
            value="BTC_PERP", exchange_id=ExchangeName.BACKPACK, internal_symbol=btc_internal
        )

        btc_unified = UnifiedSymbol(
            internal=btc_internal,
            exchange_mappings={
                ExchangeName.HYPERLIQUID: btc_hl_exchange,
                ExchangeName.BACKPACK: btc_bp_exchange,
            },
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000.0"),
        )

        return {
            "btc_internal": btc_internal,
            "btc_hl_exchange": btc_hl_exchange,
            "btc_bp_exchange": btc_bp_exchange,
            "btc_unified": btc_unified,
        }

    def test_get_exchange_symbol_value(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting exchange symbol value."""
        # Setup mock
        mock_registry.get_exchange_symbol.return_value = sample_symbols["btc_bp_exchange"]

        # Test
        result = service.get_exchange_symbol_value("BTC", ExchangeName.BACKPACK)

        # Verify
        assert result == "BTC_PERP"
        mock_registry.get_exchange_symbol.assert_called_once_with("BTC", ExchangeName.BACKPACK)

    def test_get_internal_symbol_value(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting internal symbol value."""
        # Setup mock
        mock_registry.get_internal_symbol.return_value = sample_symbols["btc_internal"]

        # Test
        result = service.get_internal_symbol_value("BTC_PERP", ExchangeName.BACKPACK)

        # Verify
        assert result == "BTC"
        mock_registry.get_internal_symbol.assert_called_once_with("BTC_PERP", ExchangeName.BACKPACK)

    def test_get_unified_symbol(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting unified symbol."""
        # Setup mock
        mock_registry.get_unified_symbol.return_value = sample_symbols["btc_unified"]

        # Test
        result = service.get_unified_symbol("BTC")

        # Verify
        assert result == sample_symbols["btc_unified"]
        assert result.internal.value == "BTC"
        assert result.internal.base_asset == "BTC"
        mock_registry.get_unified_symbol.assert_called_once_with("BTC")

    def test_get_unified_symbol_not_found(
        self, service: UnifiedSymbolService, mock_registry: Mock
    ) -> None:
        """Test getting unified symbol that doesn't exist."""
        # Setup mock
        mock_registry.get_unified_symbol.side_effect = SymbolNotFoundError(
            symbol="NONEXISTENT", context="registry"
        )

        # Test and verify exception
        with pytest.raises(SymbolNotFoundError):
            service.get_unified_symbol("NONEXISTENT")

    def test_get_trading_specifications(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting trading specifications."""
        # Setup mock
        mock_registry.get_unified_symbol.return_value = sample_symbols["btc_unified"]

        # Test
        result = service.get_trading_specifications("BTC")

        # Verify
        assert result["tick_size"] == Decimal("0.01")
        assert result["min_order_size"] == Decimal("0.001")
        assert result["max_order_size"] == Decimal("1000.0")
        assert result["lot_size"] is None
        assert result["is_tradeable"] is True
        assert result["is_active"] is True
        assert set(result["supported_exchanges"]) == {
            ExchangeName.HYPERLIQUID,
            ExchangeName.BACKPACK,
        }

    def test_is_symbol_supported_true(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test symbol support check when symbol is supported."""
        # Setup mock
        mock_registry.get_exchange_symbol.return_value = sample_symbols["btc_hl_exchange"]

        # Test
        result = service.is_symbol_supported("BTC", ExchangeName.HYPERLIQUID)

        # Verify
        assert result is True
        mock_registry.get_exchange_symbol.assert_called_once_with("BTC", ExchangeName.HYPERLIQUID)

    def test_is_symbol_supported_false(
        self, service: UnifiedSymbolService, mock_registry: Mock
    ) -> None:
        """Test symbol support check when symbol is not supported."""
        # Setup mock
        mock_registry.get_exchange_symbol.side_effect = SymbolNotFoundError(
            symbol="INVALID", context="registry"
        )

        # Test
        result = service.is_symbol_supported("INVALID", ExchangeName.HYPERLIQUID)

        # Verify
        assert result is False
        mock_registry.get_exchange_symbol.assert_called_once_with(
            "INVALID", ExchangeName.HYPERLIQUID
        )

    def test_get_all_internal_symbols(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting all internal symbols."""
        # Create additional test symbol
        eth_internal = InternalSymbol(value="ETH", base_asset="ETH", market_type=MarketType.PERP)
        eth_unified = UnifiedSymbol(internal=eth_internal, exchange_mappings={})

        # Setup mock
        mock_registry.get_all_symbols.return_value = [sample_symbols["btc_unified"], eth_unified]

        # Test
        result = service.get_all_internal_symbols()

        # Verify
        assert result == ["BTC", "ETH"]
        mock_registry.get_all_symbols.assert_called_once()

    def test_get_symbols_for_exchange(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test getting symbols for specific exchange."""
        # Create additional test symbol that doesn't support Hyperliquid
        eth_internal = InternalSymbol(value="ETH", base_asset="ETH", market_type=MarketType.PERP)
        eth_bp_exchange = ExchangeSymbol(
            value="ETH_PERP", exchange_id=ExchangeName.BACKPACK, internal_symbol=eth_internal
        )
        eth_unified = UnifiedSymbol(
            internal=eth_internal, exchange_mappings={ExchangeName.BACKPACK: eth_bp_exchange}
        )

        # Setup mock for different calls
        def mock_get_all_symbols(exchange_id: ExchangeName | None = None) -> list[UnifiedSymbol]:
            if exchange_id == ExchangeName.HYPERLIQUID:
                return [sample_symbols["btc_unified"]]  # Only BTC supports HL
            if exchange_id == ExchangeName.BACKPACK:
                return [sample_symbols["btc_unified"], eth_unified]  # Both support BP
            return [sample_symbols["btc_unified"], eth_unified]  # All symbols

        mock_registry.get_all_symbols.side_effect = mock_get_all_symbols

        # Test for Hyperliquid (should only return BTC)
        result_hl = service.get_symbols_for_exchange(ExchangeName.HYPERLIQUID)
        assert result_hl == ["BTC"]

        # Test for Backpack (should return both)
        result_bp = service.get_symbols_for_exchange(ExchangeName.BACKPACK)
        assert set(result_bp) == {"BTC", "ETH"}

    def test_error_propagation(self, service: UnifiedSymbolService, mock_registry: Mock) -> None:
        """Test that registry errors are properly propagated."""
        # Setup mock to raise exception
        mock_registry.get_exchange_symbol.side_effect = SymbolNotFoundError(
            symbol="INVALID", context="test registry"
        )

        # Test that exception is propagated
        with pytest.raises(SymbolNotFoundError, match=r"INVALID.*not found.*test registry"):
            service.get_exchange_symbol_value("INVALID", ExchangeName.HYPERLIQUID)

    def test_different_market_types(
        self, service: UnifiedSymbolService, mock_registry: Mock
    ) -> None:
        """Test handling of different market types."""
        # Create SPOT symbol
        sol_internal = InternalSymbol(
            value="SOL_USDC", base_asset="SOL", quote_asset="USDC", market_type=MarketType.SPOT
        )
        sol_unified = UnifiedSymbol(internal=sol_internal, exchange_mappings={})

        # Setup mock
        mock_registry.get_unified_symbol.return_value = sol_unified

        # Test
        result = service.get_unified_symbol("SOL_USDC")

        # Verify
        assert result.internal.market_type == MarketType.SPOT
        assert result.internal.quote_asset == "USDC"

    def test_empty_registry(self, service: UnifiedSymbolService, mock_registry: Mock) -> None:
        """Test behavior with empty registry."""
        # Setup mock
        mock_registry.get_all_symbols.return_value = []

        # Test
        result = service.get_all_internal_symbols()

        # Verify
        assert result == []

    def test_multiple_exchanges_support(
        self, service: UnifiedSymbolService, mock_registry: Mock, sample_symbols: dict[str, Any]
    ) -> None:
        """Test symbol with multiple exchange support."""
        # Test that unified symbol reports correct supported exchanges
        mock_registry.get_unified_symbol.return_value = sample_symbols["btc_unified"]

        specs = service.get_trading_specifications("BTC")
        supported_exchanges = specs["supported_exchanges"]

        assert ExchangeName.HYPERLIQUID in supported_exchanges
        assert ExchangeName.BACKPACK in supported_exchanges
        assert len(supported_exchanges) == 2


class TestSymbolServiceSingleton:
    """Test the singleton pattern for symbol service."""

    def test_get_symbol_service_singleton(self) -> None:
        """Test that get_symbol_service returns the same instance."""
        service1 = get_symbol_service()
        service2 = get_symbol_service()

        # Should be the same instance
        assert service1 is service2

    @patch("cyberdelta.core.symbol_service._symbol_service", None)
    def test_service_initialization(self) -> None:
        """Test that service is properly initialized on first call."""
        with patch("cyberdelta.core.symbol_service.UnifiedSymbolService") as mock_service_class:
            mock_instance = Mock()
            mock_service_class.return_value = mock_instance

            service = get_symbol_service()

            # Should create new instance
            mock_service_class.assert_called_once()
            assert service is mock_instance


class TestSymbolServiceIntegration:
    """Integration tests for symbol service with real components."""

    def test_service_with_real_registry(self) -> None:
        """Test service with actual registry instance."""
        # This test uses real components but with minimal setup
        service = UnifiedSymbolService()

        # Should not raise exceptions during initialization
        assert service.service is not None  # UnifiedSymbolService wraps SymbolService

        # Should handle empty registry gracefully
        all_symbols = service.get_all_internal_symbols()
        assert isinstance(all_symbols, list)

    def test_error_handling_with_real_components(self) -> None:
        """Test error handling with real component integration."""
        service = UnifiedSymbolService()

        # Should raise appropriate exception for non-existent symbol
        with pytest.raises(SymbolNotFoundError):
            service.get_exchange_symbol_value("NONEXISTENT", ExchangeName.HYPERLIQUID)

        with pytest.raises(SymbolNotFoundError):
            service.get_unified_symbol("NONEXISTENT")
