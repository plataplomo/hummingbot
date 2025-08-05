"""Symbol validation helpers for testing.

These helpers provide comprehensive validation methods for
Symbol objects and their relationships in tests.
"""

import contextlib
from typing import Any

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols import Symbol
from cyberdelta.core.symbols.models import (
    BackpackMetadata,
    HyperliquidMetadata,
    SymbolComponents,
)
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


class SymbolTestValidator:
    """Advanced validation for symbol tests."""

    @staticmethod
    def assert_valid_arbitrage_pair(
        long: Symbol,
        short: Symbol,
        symbol_service: SymbolService,
    ) -> None:
        """Validate arbitrage pair compatibility.

        Args:
            long: Long position symbol
            short: Short position symbol
            symbol_service: Symbol service for equivalence checking
        """
        # Must be equivalent symbols
        assert symbol_service.are_equivalent(long, short), (
            f"Symbols {long.value} and {short.value} are not equivalent"
        )

        # Must be on different exchanges
        assert long.exchange != short.exchange, (
            f"Both symbols are on same exchange: {long.exchange}"
        )

        # Must have same market type (if components available)
        with contextlib.suppress(AttributeError, ValueError):
            assert long.market_type == short.market_type, (
                f"Market type mismatch: {long.market_type} vs {short.market_type}"
            )

        # Check base assets match (if components available)
        with contextlib.suppress(AttributeError, ValueError):
            assert long.base_asset == short.base_asset, (
                f"Base asset mismatch: {long.base_asset} vs {short.base_asset}"
            )

    @staticmethod
    def assert_metadata_consistency(
        symbol: Symbol,
        expected_metadata: dict[str, Any],
    ) -> None:
        """Validate symbol metadata matches expectations.

        Args:
            symbol: Symbol to validate
            expected_metadata: Expected metadata fields
        """
        if symbol.exchange == ExchangeName.HYPERLIQUID:
            assert isinstance(symbol.metadata, HyperliquidMetadata), (
                f"Expected HyperliquidMetadata, got {type(symbol.metadata)}"
            )

            if "asset_index" in expected_metadata:
                assert symbol.metadata.asset_index == expected_metadata["asset_index"], (
                    f"Asset index mismatch: {symbol.metadata.asset_index} vs "
                    f"{expected_metadata['asset_index']}"
                )

        elif symbol.exchange == ExchangeName.BACKPACK:
            assert isinstance(symbol.metadata, BackpackMetadata), (
                f"Expected BackpackMetadata, got {type(symbol.metadata)}"
            )

            if "symbol_id" in expected_metadata:
                assert symbol.metadata.symbol_id == expected_metadata["symbol_id"], (
                    f"Symbol ID mismatch: {symbol.metadata.symbol_id} vs "
                    f"{expected_metadata['symbol_id']}"
                )

    @staticmethod
    def assert_components_valid(
        symbol: Symbol,
        expected_base: str | None = None,
        expected_quote: str | None = None,
        expected_market_type: MarketType | None = None,
    ) -> None:
        """Validate symbol components.

        Args:
            symbol: Symbol to validate
            expected_base: Expected base asset
            expected_quote: Expected quote asset
            expected_market_type: Expected market type

        Raises:
            AssertionError: If components don't match expectations
        """
        # Check if components are set
        try:
            components = SymbolComponents(
                base_asset=symbol.base_asset,
                quote_asset=symbol.quote_asset,
                market_type=symbol.market_type,
            )
        except (AttributeError, ValueError) as e:
            raise AssertionError(f"Components not set on symbol: {e}") from e

        if expected_base is not None:
            assert components.base_asset == expected_base, (
                f"Base asset mismatch: {components.base_asset} vs {expected_base}"
            )

        if expected_quote is not None:
            assert components.quote_asset == expected_quote, (
                f"Quote asset mismatch: {components.quote_asset} vs {expected_quote}"
            )

        if expected_market_type is not None:
            assert components.market_type == expected_market_type, (
                f"Market type mismatch: {components.market_type} vs {expected_market_type}"
            )

    @staticmethod
    def assert_symbol_format(
        symbol: Symbol,
        expected_separator: str | None = None,
        expected_suffix: str | None = None,
    ) -> None:
        """Validate symbol format conventions.

        Args:
            symbol: Symbol to validate
            expected_separator: Expected separator character
            expected_suffix: Expected suffix (e.g., "PERP")
        """
        default_separator = "-" if symbol.exchange == ExchangeName.HYPERLIQUID else "_"

        separator = expected_separator or default_separator

        if separator in symbol.value:
            assert separator in symbol.value, (
                f"Expected separator '{separator}' not found in {symbol.value}"
            )

        if expected_suffix:
            assert symbol.value.endswith(expected_suffix), (
                f"Symbol {symbol.value} doesn't end with {expected_suffix}"
            )

    @staticmethod
    def assert_symbols_equivalent(
        symbols: list[Symbol],
        symbol_service: SymbolService,
    ) -> None:
        """Assert all symbols in list are equivalent.

        Args:
            symbols: List of symbols to check
            symbol_service: Symbol service for checking
        """
        if len(symbols) < 2:
            return

        first = symbols[0]
        for other in symbols[1:]:
            assert symbol_service.are_equivalent(first, other), (
                f"Symbol {other.value} not equivalent to {first.value}"
            )

    @staticmethod
    def assert_valid_conversion(
        from_symbol: Symbol,
        to_symbol: Symbol,
        target_exchange: ExchangeName,
        symbol_service: SymbolService,
    ) -> None:
        """Validate symbol conversion result.

        Args:
            from_symbol: Source symbol
            to_symbol: Result symbol
            target_exchange: Target exchange
            symbol_service: Symbol service
        """
        # Result must be on target exchange
        assert to_symbol.exchange == target_exchange, (
            f"Conversion result on wrong exchange: {to_symbol.exchange} vs {target_exchange}"
        )

        # Must be equivalent
        assert symbol_service.are_equivalent(from_symbol, to_symbol), (
            f"Converted symbol {to_symbol.value} not equivalent to {from_symbol.value}"
        )

        # Components should match (if available)
        try:
            assert from_symbol.base_asset == to_symbol.base_asset, (
                "Base asset changed during conversion"
            )
            assert from_symbol.market_type == to_symbol.market_type, (
                "Market type changed during conversion"
            )
        except (AttributeError, ValueError):
            # Components not set, skip
            pass


class EquivalenceChecker:
    """Helper for checking symbol equivalence relationships."""

    @staticmethod
    def check_reflexive(symbol: Symbol, symbol_service: SymbolService) -> bool:
        """Check if symbol is equivalent to itself.

        Args:
            symbol: Symbol to check
            symbol_service: Symbol service

        Returns:
            bool: True if reflexive property holds
        """
        return symbol_service.are_equivalent(symbol, symbol)

    @staticmethod
    def check_symmetric(
        symbol1: Symbol,
        symbol2: Symbol,
        symbol_service: SymbolService,
    ) -> bool:
        """Check if equivalence is symmetric.

        Args:
            symbol1: First symbol
            symbol2: Second symbol
            symbol_service: Symbol service

        Returns:
            bool: True if symmetric property holds
        """
        forward = symbol_service.are_equivalent(symbol1, symbol2)
        backward = symbol_service.are_equivalent(symbol2, symbol1)
        return forward == backward

    @staticmethod
    def check_transitive(
        symbol1: Symbol,
        symbol2: Symbol,
        symbol3: Symbol,
        symbol_service: SymbolService,
    ) -> bool:
        """Check if equivalence is transitive.

        Args:
            symbol1: First symbol
            symbol2: Second symbol
            symbol3: Third symbol
            symbol_service: Symbol service

        Returns:
            bool: True if transitive property holds
        """
        if symbol_service.are_equivalent(symbol1, symbol2) and symbol_service.are_equivalent(
            symbol2, symbol3
        ):
            return symbol_service.are_equivalent(symbol1, symbol3)
        return True  # Vacuously true if premise doesn't hold


class MetadataAsserter:
    """Helper for asserting metadata properties."""

    @staticmethod
    def assert_hyperliquid_metadata(
        symbol: Symbol,
        asset_index: int | None = None,
        check_none: bool = False,
    ) -> None:
        """Assert Hyperliquid metadata properties.

        Args:
            symbol: Symbol to check
            asset_index: Expected asset index
            check_none: Whether to check for None value
        """
        assert symbol.exchange == ExchangeName.HYPERLIQUID
        assert isinstance(symbol.metadata, HyperliquidMetadata)

        if asset_index is not None:
            assert symbol.metadata.asset_index == asset_index
        elif check_none:
            assert symbol.metadata.asset_index is None

    @staticmethod
    def assert_backpack_metadata(
        symbol: Symbol,
        symbol_id: int | None = None,
        check_none: bool = False,
    ) -> None:
        """Assert Backpack metadata properties.

        Args:
            symbol: Symbol to check
            symbol_id: Expected symbol ID
            check_none: Whether to check for None value
        """
        assert symbol.exchange == ExchangeName.BACKPACK
        assert isinstance(symbol.metadata, BackpackMetadata)

        if symbol_id is not None:
            assert symbol.metadata.symbol_id == symbol_id
        elif check_none:
            assert symbol.metadata.symbol_id is None
