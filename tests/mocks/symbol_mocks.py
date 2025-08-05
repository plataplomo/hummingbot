"""Enhanced Symbol mocks for comprehensive testing.

These mocks provide builder patterns for configuring complex
symbol service behaviors in tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Self
from unittest.mock import Mock

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols import Symbol, symbol
from cyberdelta.core.symbols.models import BackpackMetadata, HyperliquidMetadata, SymbolComponents
from cyberdelta.core.symbols.protocols import ExchangeHandler
from cyberdelta.core.symbols.registry import SymbolRegistry
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


class MockSymbolService:
    """Enhanced mock for SymbolService with builder pattern."""

    def __init__(self) -> None:
        """Initialize the mock service builder."""
        self._conversions: dict[tuple[str, ExchangeName, ExchangeName], Symbol] = {}
        self._equivalences: dict[str, list[Symbol]] = {}
        self._canonical_mappings: dict[tuple[str, ExchangeName], str] = {}
        self._components: dict[Symbol, SymbolComponents] = {}
        self._find_mappings: dict[tuple[str, ExchangeName], Symbol] = {}

    def with_conversion(
        self,
        from_symbol: Symbol,
        to_exchange: ExchangeName,
        result: Symbol,
    ) -> Self:
        """Add conversion rule.

        Args:
            from_symbol: Source symbol
            to_exchange: Target exchange
            result: Conversion result

        Returns:
            Self for chaining
        """
        key = (from_symbol.value, from_symbol.exchange, to_exchange)
        self._conversions[key] = result

        # Also ensure equivalence
        canonical = self._get_or_create_canonical(from_symbol)
        if canonical not in self._equivalences:
            self._equivalences[canonical] = []
        if from_symbol not in self._equivalences[canonical]:
            self._equivalences[canonical].append(from_symbol)
        if result not in self._equivalences[canonical]:
            self._equivalences[canonical].append(result)

        return self

    def with_equivalence(self, symbols: list[Symbol]) -> Self:
        """Add equivalence relationship.

        Args:
            symbols: List of equivalent symbols

        Returns:
            Self for chaining
        """
        if not symbols:
            return self

        # Use first symbol's canonical as key
        canonical = self._get_or_create_canonical(symbols[0])
        self._equivalences[canonical] = symbols.copy()

        # Set up conversions between all pairs
        for i, sym1 in enumerate(symbols):
            for sym2 in symbols[i + 1 :]:
                if sym1.exchange != sym2.exchange:
                    key1 = (sym1.value, sym1.exchange, sym2.exchange)
                    key2 = (sym2.value, sym2.exchange, sym1.exchange)
                    self._conversions[key1] = sym2
                    self._conversions[key2] = sym1

        return self

    def with_canonical(self, symbol: Symbol, canonical: str) -> Self:
        """Set canonical representation for a symbol.

        Args:
            symbol: Symbol
            canonical: Canonical representation

        Returns:
            Self for chaining
        """
        key = (symbol.value, symbol.exchange)
        self._canonical_mappings[key] = canonical
        return self

    def with_components(self, symbol: Symbol, components: SymbolComponents) -> Self:
        """Set components for a symbol.

        Args:
            symbol: Symbol
            components: Symbol components

        Returns:
            Self for chaining
        """
        self._components[symbol] = components
        return self

    def with_find_result(self, value: str, exchange: ExchangeName, result: Symbol) -> Self:
        """Set result for find_symbol.

        Args:
            value: Symbol value
            exchange: Exchange name
            result: Symbol to return

        Returns:
            Self for chaining
        """
        self._find_mappings[value, exchange] = result
        return self

    def build(self) -> SymbolService:
        """Build configured mock service.

        Returns:
            Mock SymbolService with configured behavior
        """
        mock = Mock(spec=SymbolService)
        
        self._configure_core_methods(mock)
        self._configure_equivalence_methods(mock)
        self._configure_component_methods(mock)
        self._configure_registry_methods(mock)
        
        return mock
    
    def _configure_core_methods(self, mock: Mock) -> None:
        """Configure core symbol methods."""
        def create_symbol(value: str, exchange: ExchangeName, **kwargs: dict[str, Any]) -> Symbol:
            return symbol(value, exchange, **kwargs)

        def convert_symbol(from_symbol: Symbol, target_exchange: ExchangeName) -> Symbol:
            key = (from_symbol.value, from_symbol.exchange, target_exchange)
            if key in self._conversions:
                return self._conversions[key]
            return create_symbol(from_symbol.value, target_exchange)

        mock.create_symbol.side_effect = create_symbol
        mock.convert_symbol.side_effect = convert_symbol
    
    def _configure_equivalence_methods(self, mock: Mock) -> None:
        """Configure symbol equivalence methods."""
        def get_canonical(symbol: Symbol) -> str:
            key = (symbol.value, symbol.exchange)
            if key in self._canonical_mappings:
                return self._canonical_mappings[key]
            return self._get_or_create_canonical(symbol)

        def get_equivalent_symbols(symbol: Symbol) -> list[Symbol]:
            canonical = get_canonical(symbol)
            return self._equivalences.get(canonical, [symbol])

        def are_equivalent(symbol1: Symbol, symbol2: Symbol) -> bool:
            return get_canonical(symbol1) == get_canonical(symbol2)

        mock.get_canonical.side_effect = get_canonical
        mock.get_equivalent_symbols.side_effect = get_equivalent_symbols
        mock.are_equivalent.side_effect = are_equivalent
    
    def _configure_component_methods(self, mock: Mock) -> None:
        """Configure symbol component parsing methods."""
        def parse_components(symbol: Symbol) -> SymbolComponents:
            if symbol in self._components:
                return self._components[symbol]
            return SymbolComponents(
                base_asset=symbol.value.split("-")[0].split("_")[0],
                quote_asset="USD" if "PERP" in symbol.value else "USDC",
                market_type=MarketType.PERP if "PERP" in symbol.value else MarketType.SPOT,
            )

        mock.parse_components.side_effect = parse_components
    
    def _configure_registry_methods(self, mock: Mock) -> None:
        """Configure symbol registry methods."""
        def find_symbol(value: str, exchange: ExchangeName) -> Symbol | None:
            key = (value, exchange)
            return self._find_mappings.get(key)

        def register_symbol(symbol: Symbol) -> None:
            self._find_mappings[symbol.value, symbol.exchange] = symbol

        mock.find_symbol.side_effect = find_symbol
        mock.register_symbol.side_effect = register_symbol

    def _get_or_create_canonical(self, symbol: Symbol) -> str:
        """Get or create canonical representation.
        
        Returns:
            str: Canonical representation of the symbol.
        """
        key = (symbol.value, symbol.exchange)
        if key in self._canonical_mappings:
            return self._canonical_mappings[key]

        # Create canonical from symbol value
        base = symbol.value.split("-")[0].split("_")[0]
        suffix = "_PERP" if "PERP" in symbol.value else "_SPOT"
        return f"{base}{suffix}"


class MockExchangeHandler:
    """Mock exchange handler with configurable behavior."""

    def __init__(self, exchange: ExchangeName) -> None:
        """Initialize mock handler.

        Args:
            exchange: Exchange this handler is for
        """
        self._exchange = exchange
        self._parse_rules: dict[str, SymbolComponents] = {}
        self._format_rules: dict[tuple[str, str | None, str], str] = {}
        self._canonical_rules: dict[str, tuple[str, SymbolComponents]] = {}

    def with_parse_rule(self, value: str, components: SymbolComponents) -> Self:
        """Add parsing rule.

        Args:
            value: Symbol value
            components: Parsed components

        Returns:
            Self for chaining
        """
        self._parse_rules[value] = components
        return self

    def with_format_rule(
        self,
        base_asset: str,
        quote_asset: str | None,
        market_type: str,
        result: str,
    ) -> Self:
        """Add formatting rule.

        Args:
            base_asset: Base asset
            quote_asset: Quote asset (optional)
            market_type: Market type
            result: Formatted symbol value

        Returns:
            Self for chaining
        """
        key = (base_asset, quote_asset, market_type)
        self._format_rules[key] = result
        return self

    def with_canonical_rule(
        self,
        value: str,
        canonical: str,
        components: SymbolComponents,
    ) -> Self:
        """Add canonical conversion rule.

        Args:
            value: Symbol value
            canonical: Canonical representation
            components: Symbol components

        Returns:
            Self for chaining
        """
        self._canonical_rules[value] = (canonical, components)
        return self

    def build(self) -> ExchangeHandler[Any]:
        """Build the mock handler.

        Returns:
            Mock ExchangeHandler
        """
        mock = Mock(spec=ExchangeHandler)
        mock.exchange = self._exchange

        self._configure_parsing_methods(mock)
        self._configure_formatting_methods(mock)
        self._configure_canonical_methods(mock)
        self._configure_metadata_methods(mock)

        return mock
    
    def _configure_parsing_methods(self, mock: Mock) -> None:
        """Configure component parsing methods."""
        def parse_components(value: str) -> SymbolComponents:
            if value in self._parse_rules:
                return self._parse_rules[value]
            return self._default_parse_components(value)

        mock.parse_components.side_effect = parse_components
    
    def _configure_formatting_methods(self, mock: Mock) -> None:
        """Configure symbol formatting methods."""
        def format_symbol(components: SymbolComponents) -> str:
            key = (
                components.base_asset,
                components.quote_asset,
                components.market_type.value,
            )
            if key in self._format_rules:
                return self._format_rules[key]
            return self._default_format_symbol(components)

        mock.format_symbol.side_effect = format_symbol
    
    def _configure_canonical_methods(self, mock: Mock) -> None:
        """Configure canonical representation methods."""
        def to_canonical(value: str) -> tuple[str, SymbolComponents]:
            if value in self._canonical_rules:
                return self._canonical_rules[value]
            components = mock.parse_components.side_effect(value)
            canonical = f"{components.base_asset}_{components.market_type.value.upper()}"
            return (canonical, components)

        def from_canonical(canonical: str, components: SymbolComponents) -> str:
            return mock.format_symbol.side_effect(components)

        mock.to_canonical.side_effect = to_canonical
        mock.from_canonical.side_effect = from_canonical
    
    def _configure_metadata_methods(self, mock: Mock) -> None:
        """Configure metadata creation methods."""
        def create_metadata(**kwargs: dict[str, Any]) -> HyperliquidMetadata | BackpackMetadata:
            if self._exchange == ExchangeName.HYPERLIQUID:
                return HyperliquidMetadata(asset_index=kwargs.get("asset_index"))
            return BackpackMetadata(symbol_id=kwargs.get("symbol_id"))

        def create_symbol(value: str, **kwargs: dict[str, Any]) -> Symbol:
            return symbol(value, self._exchange, **kwargs)

        mock.create_metadata.side_effect = create_metadata
        mock.create_symbol.side_effect = create_symbol
    
    def _default_parse_components(self, value: str) -> SymbolComponents:
        """Default component parsing logic.
        
        Returns:
            SymbolComponents: Parsed components from the symbol value.
        """
        base = value.split("-", 1)[0].split("_", 1)[0]
        if "PERP" in value:
            return SymbolComponents(
                base_asset=base,
                quote_asset="USD",
                market_type=MarketType.PERP,
            )
        parts = value.split("-", 1) if "-" in value else value.split("_", 1)
        return SymbolComponents(
            base_asset=parts[0],
            quote_asset=parts[1] if len(parts) > 1 else "USDC",
            market_type=MarketType.SPOT,
        )
    
    def _default_format_symbol(self, components: SymbolComponents) -> str:
        """Default symbol formatting logic.
        
        Returns:
            str: Formatted symbol value.
        """
        if self._exchange == ExchangeName.HYPERLIQUID:
            if components.market_type.value == "PERP":
                return f"{components.base_asset}-PERP"
            return f"{components.base_asset}-{components.quote_asset or 'USDC'}"
        if components.market_type.value == "PERP":
            return f"{components.base_asset}_PERP"
        return f"{components.base_asset}_{components.quote_asset or 'USDC'}"


class MockSymbolRegistry:
    """Mock symbol registry with builder pattern."""

    def __init__(self) -> None:
        """Initialize mock registry builder."""
        self._symbols: dict[tuple[str, ExchangeName], Symbol] = {}
        self._handlers: dict[ExchangeName, ExchangeHandler[Any]] = {}
        self._common_symbols: dict[str, dict[ExchangeName, Symbol]] = {}

    def with_symbol(self, symbol: Symbol) -> Self:
        """Register a symbol.

        Args:
            symbol: Symbol to register

        Returns:
            Self for chaining
        """
        key = (symbol.value, symbol.exchange)
        self._symbols[key] = symbol
        return self

    def with_handler(self, exchange: ExchangeName, handler: ExchangeHandler[Any]) -> Self:
        """Register a handler.

        Args:
            exchange: Exchange name
            handler: Exchange handler

        Returns:
            Self for chaining
        """
        self._handlers[exchange] = handler
        return self

    def with_common_symbol(self, asset: str, symbols: dict[ExchangeName, Symbol]) -> Self:
        """Register common symbol set.

        Args:
            asset: Asset name (e.g., "BTC")
            symbols: Exchange to symbol mapping

        Returns:
            Self for chaining
        """
        self._common_symbols[asset] = symbols
        return self

    def build(self) -> SymbolRegistry:
        """Build the mock registry.

        Returns:
            Mock SymbolRegistry
        """
        mock = Mock(spec=SymbolRegistry)

        # Configure create_symbol
        def create_symbol(value: str, exchange: ExchangeName, **kwargs: dict[str, Any]) -> Symbol:
            sym = symbol(value, exchange, **kwargs)
            self._symbols[value, exchange] = sym
            return sym

        mock.create_symbol.side_effect = create_symbol

        # Configure get_factory
        def get_factory(exchange: ExchangeName) -> Callable[[str], Symbol] | None:
            if exchange in self._handlers:
                handler = self._handlers[exchange]

                def factory(value: str, **kwargs: Any) -> Symbol:
                    return handler.create_symbol(value, **kwargs)

                return factory
            return None

        mock.get_factory.side_effect = get_factory

        # Configure get_handlers
        def get_handlers() -> dict[ExchangeName, ExchangeHandler[Any]]:
            return self._handlers.copy()

        mock.get_handlers.side_effect = get_handlers

        # Configure __getattr__ for exchange access
        def getattr_handler(name: str) -> Callable[[str], Symbol] | None:
            exchange = ExchangeName[name.upper()]
            return get_factory(exchange)

        mock.__getattr__ = getattr_handler

        return mock
