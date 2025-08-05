"""Symbol builders for creating complex test scenarios.

These builders provide fluent interfaces for constructing test data
that leverages the full Symbol system capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Self

from cyberdelta.core.symbols import Symbol, symbol
from cyberdelta.enums.exchange_names import ExchangeName


@dataclass
class ArbitragePair:
    """Represents a symbol pair for arbitrage testing."""

    long: Symbol
    short: Symbol
    spread_threshold: Decimal = Decimal("0.001")
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArbitrageTestData:
    """Complete arbitrage test scenario data."""

    pairs: dict[str, ArbitragePair]
    prices: dict[Symbol, Decimal]
    funding_rates: dict[Symbol, Decimal]
    metadata: dict[str, Any]


class ArbitrageSymbolBuilder:
    """Builder for arbitrage test scenarios with Symbol support."""

    def __init__(self) -> None:
        """Initialize the builder."""
        self._pairs: dict[str, ArbitragePair] = {}
        self._prices: dict[Symbol, Decimal] = {}
        self._funding_rates: dict[Symbol, Decimal] = {}
        self._metadata: dict[str, str | int | float | bool | dict[str, Any] | list[Any]] = {}

    def add_perpetual_pair(
        self,
        base_asset: str,
        hl_metadata: dict[str, str | int | float | bool] | None = None,
        bp_metadata: dict[str, str | int | float | bool] | None = None,
        spread_threshold: Decimal = Decimal("0.001"),
    ) -> Self:
        """Add perpetual pair with custom metadata.

        Args:
            base_asset: Base asset (e.g., "BTC")
            hl_metadata: Hyperliquid-specific metadata
            bp_metadata: Backpack-specific metadata
            spread_threshold: Threshold for arbitrage signals

        Returns:
            Self for chaining
        """
        # Create Hyperliquid symbol
        hl_value = f"{base_asset}-PERP"
        hl_kwargs = hl_metadata or {}
        hl_symbol = symbol(
            hl_value,
            ExchangeName.HYPERLIQUID,
            asset_index=int(hl_kwargs["asset_index"])
            if "asset_index" in hl_kwargs and isinstance(hl_kwargs["asset_index"], (int, str))
            else None,
        )

        # Create Backpack symbol
        bp_value = f"{base_asset}_PERP"
        bp_kwargs = bp_metadata or {}
        bp_symbol = symbol(
            bp_value,
            ExchangeName.BACKPACK,
            symbol_id=int(bp_kwargs["symbol_id"])
            if "symbol_id" in bp_kwargs and isinstance(bp_kwargs["symbol_id"], (int, str))
            else None,
        )

        # Create pair
        self._pairs[base_asset] = ArbitragePair(
            long=hl_symbol,
            short=bp_symbol,
            spread_threshold=spread_threshold,
        )

        return self

    def add_spot_pair(
        self,
        base_asset: str,
        quote_asset: str = "USDC",
        hl_metadata: dict[str, str | int | float | bool] | None = None,
        bp_metadata: dict[str, str | int | float | bool] | None = None,
    ) -> Self:
        """Add spot pair for both exchanges.

        Args:
            base_asset: Base asset
            quote_asset: Quote asset
            hl_metadata: Hyperliquid-specific metadata
            bp_metadata: Backpack-specific metadata

        Returns:
            Self for chaining
        """
        # Create Hyperliquid spot symbol
        hl_value = f"{base_asset}-{quote_asset}"
        hl_kwargs = hl_metadata or {}
        hl_symbol = symbol(
            hl_value,
            ExchangeName.HYPERLIQUID,
            asset_index=int(hl_kwargs["asset_index"])
            if "asset_index" in hl_kwargs and isinstance(hl_kwargs["asset_index"], (int, str))
            else None,
        )

        # Create Backpack spot symbol
        bp_value = f"{base_asset}_{quote_asset}"
        bp_kwargs = bp_metadata or {}
        bp_symbol = symbol(
            bp_value,
            ExchangeName.BACKPACK,
            symbol_id=int(bp_kwargs["symbol_id"])
            if "symbol_id" in bp_kwargs and isinstance(bp_kwargs["symbol_id"], (int, str))
            else None,
        )

        # Create pair with spot key
        self._pairs[f"{base_asset}_spot"] = ArbitragePair(
            long=hl_symbol,
            short=bp_symbol,
            spread_threshold=Decimal("0.0005"),  # Tighter spread for spot
        )

        return self

    def with_price_discrepancy(
        self,
        base_asset: str,
        hl_price: Decimal,
        bp_price: Decimal,
    ) -> Self:
        """Add price data for testing arbitrage signals.

        Args:
            base_asset: Base asset
            hl_price: Hyperliquid price
            bp_price: Backpack price

        Returns:
            Self for chaining

        Raises:
            ValueError: If base_asset pair hasn't been added yet
        """
        if base_asset not in self._pairs:
            raise ValueError(f"Must add {base_asset} pair before setting prices")

        pair = self._pairs[base_asset]
        self._prices[pair.long] = hl_price
        self._prices[pair.short] = bp_price

        return self

    def with_funding_rates(
        self,
        base_asset: str,
        hl_rate: Decimal,
        bp_rate: Decimal,
    ) -> Self:
        """Add funding rate data.

        Args:
            base_asset: Base asset
            hl_rate: Hyperliquid funding rate
            bp_rate: Backpack funding rate

        Returns:
            Self for chaining

        Raises:
            ValueError: If base_asset pair hasn't been added yet
        """
        if base_asset not in self._pairs:
            raise ValueError(f"Must add {base_asset} pair before setting funding rates")

        pair = self._pairs[base_asset]
        self._funding_rates[pair.long] = hl_rate
        self._funding_rates[pair.short] = bp_rate

        return self

    def with_metadata(
        self, key: str, value: str | float | bool | dict[str, Any] | list[Any]
    ) -> Self:
        """Add metadata to the test scenario.

        Args:
            key: Metadata key
            value: Metadata value

        Returns:
            Self for chaining
        """
        self._metadata[key] = value
        return self

    def build(self) -> ArbitrageTestData:
        """Build complete test scenario.

        Returns:
            ArbitrageTestData with all configured data
        """
        return ArbitrageTestData(
            pairs=self._pairs.copy(),
            prices=self._prices.copy(),
            funding_rates=self._funding_rates.copy(),
            metadata=self._metadata.copy(),
        )


@dataclass
class MarketDataTestScenario:
    """Market data test scenario."""

    symbols: list[Symbol]
    tickers: dict[Symbol, dict[str, Decimal | float]]
    order_books: dict[Symbol, dict[str, list[tuple[Decimal, Decimal]] | Decimal | None]]
    candles: dict[Symbol, list[dict[str, str | float | int]]]
    metadata: dict[str, str | int | float | bool | dict[str, Any] | list[Any]]


class MarketDataSymbolBuilder:
    """Builder for market data test scenarios."""

    def __init__(self) -> None:
        """Initialize the builder."""
        self._symbols: list[Symbol] = []
        self._tickers: dict[Symbol, dict[str, Decimal | float]] = {}
        self._order_books: dict[
            Symbol, dict[str, list[tuple[Decimal, Decimal]] | Decimal | None]
        ] = {}
        self._candles: dict[Symbol, list[dict[str, str | float | int]]] = {}
        self._metadata: dict[str, str | int | float | bool | dict[str, Any] | list[Any]] = {}

    def add_symbol(
        self,
        value: str,
        exchange: ExchangeName,
        **metadata_kwargs: str | float | bool,
    ) -> Self:
        """Add a symbol to the scenario.

        Args:
            value: Symbol value
            exchange: Exchange name
            **metadata_kwargs: Exchange-specific metadata

        Returns:
            Self for chaining
        """
        # Filter metadata_kwargs to only pass valid parameters
        valid_kwargs: dict[str, int] = {}
        if "asset_index" in metadata_kwargs and isinstance(metadata_kwargs["asset_index"], int):
            valid_kwargs["asset_index"] = metadata_kwargs["asset_index"]
        if "symbol_id" in metadata_kwargs and isinstance(metadata_kwargs["symbol_id"], int):
            valid_kwargs["symbol_id"] = metadata_kwargs["symbol_id"]
        sym = symbol(value, exchange, **valid_kwargs)
        self._symbols.append(sym)
        return self

    def with_ticker(
        self,
        symbol_value: str,
        exchange: ExchangeName,
        bid: Decimal,
        ask: Decimal,
        last: Decimal,
        volume: Decimal,
    ) -> Self:
        """Add ticker data for a symbol.

        Args:
            symbol_value: Symbol value
            exchange: Exchange name
            bid: Bid price
            ask: Ask price
            last: Last price
            volume: 24h volume

        Returns:
            Self for chaining

        Raises:
            ValueError: If symbol not found in scenario
        """
        # Find matching symbol
        sym = next(
            (s for s in self._symbols if s.value == symbol_value and s.exchange == exchange),
            None,
        )
        if not sym:
            raise ValueError(f"Symbol {symbol_value} on {exchange} not found in scenario")

        self._tickers[sym] = {
            "bid": bid,
            "ask": ask,
            "last": last,
            "volume": volume,
            "spread": ask - bid,
            "mid": (bid + ask) / 2,
        }

        return self

    def with_order_book(
        self,
        symbol_value: str,
        exchange: ExchangeName,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> Self:
        """Add order book data.

        Args:
            symbol_value: Symbol value
            exchange: Exchange name
            bids: List of (price, quantity) tuples
            asks: List of (price, quantity) tuples

        Returns:
            Self for chaining

        Raises:
            ValueError: If symbol not found in scenario
        """
        sym = next(
            (s for s in self._symbols if s.value == symbol_value and s.exchange == exchange),
            None,
        )
        if not sym:
            raise ValueError(f"Symbol {symbol_value} on {exchange} not found in scenario")

        self._order_books[sym] = {
            "bids": bids,
            "asks": asks,
            "best_bid": bids[0][0] if bids else None,
            "best_ask": asks[0][0] if asks else None,
            "spread": asks[0][0] - bids[0][0] if bids and asks else None,
        }

        return self

    def build(self) -> MarketDataTestScenario:
        """Build the market data scenario.

        Returns:
            Complete market data test scenario
        """
        return MarketDataTestScenario(
            symbols=self._symbols.copy(),
            tickers=self._tickers.copy(),
            order_books=self._order_books.copy(),
            candles=self._candles.copy(),
            metadata=self._metadata.copy(),
        )


@dataclass
class TradingTestScenario:
    """Trading test scenario with orders and positions."""

    symbols: dict[str, Symbol]
    positions: dict[Symbol, Decimal]
    orders: dict[Symbol, list[dict[str, str | int | float | bool]]]
    balances: dict[str, Decimal]
    metadata: dict[str, str | int | float | bool | dict[str, Any] | list[Any]]


class TradingSymbolBuilder:
    """Builder for trading test scenarios."""

    def __init__(self) -> None:
        """Initialize the builder."""
        self._symbols: dict[str, Symbol] = {}
        self._positions: dict[Symbol, Decimal] = {}
        self._orders: dict[Symbol, list[dict[str, str | int | float | bool]]] = {}
        self._balances: dict[str, Decimal] = {}
        self._metadata: dict[str, str | int | float | bool | dict[str, Any] | list[Any]] = {}

    def add_trading_symbol(
        self,
        key: str,
        value: str,
        exchange: ExchangeName,
        **metadata_kwargs: str | float | bool,
    ) -> Self:
        """Add a symbol for trading.

        Args:
            key: Identifier for the symbol
            value: Symbol value
            exchange: Exchange name
            **metadata_kwargs: Exchange-specific metadata

        Returns:
            Self for chaining
        """
        # Filter metadata_kwargs to only pass valid parameters
        valid_kwargs: dict[str, int] = {}
        if "asset_index" in metadata_kwargs and isinstance(metadata_kwargs["asset_index"], int):
            valid_kwargs["asset_index"] = metadata_kwargs["asset_index"]
        if "symbol_id" in metadata_kwargs and isinstance(metadata_kwargs["symbol_id"], int):
            valid_kwargs["symbol_id"] = metadata_kwargs["symbol_id"]
        self._symbols[key] = symbol(value, exchange, **valid_kwargs)
        return self

    def with_position(
        self,
        symbol_key: str,
        size: Decimal,
        entry_price: Decimal | None = None,
    ) -> Self:
        """Add position for a symbol.

        Args:
            symbol_key: Symbol identifier
            size: Position size (positive for long, negative for short)
            entry_price: Optional entry price

        Returns:
            Self for chaining

        Raises:
            ValueError: If symbol_key not found
        """
        if symbol_key not in self._symbols:
            raise ValueError(f"Symbol {symbol_key} not found")

        sym = self._symbols[symbol_key]
        self._positions[sym] = size

        if entry_price:
            self._metadata[f"{symbol_key}_entry_price"] = float(entry_price)

        return self

    def with_orders(
        self,
        symbol_key: str,
        orders: list[dict[str, str | int | float | bool]],
    ) -> Self:
        """Add orders for a symbol.

        Args:
            symbol_key: Symbol identifier
            orders: List of order data

        Returns:
            Self for chaining

        Raises:
            ValueError: If symbol_key not found
        """
        if symbol_key not in self._symbols:
            raise ValueError(f"Symbol {symbol_key} not found")

        sym = self._symbols[symbol_key]
        self._orders[sym] = orders

        return self

    def with_balance(self, asset: str, amount: Decimal) -> Self:
        """Add balance for an asset.

        Args:
            asset: Asset name
            amount: Balance amount

        Returns:
            Self for chaining
        """
        self._balances[asset] = amount
        return self

    def build(self) -> TradingTestScenario:
        """Build the trading scenario.

        Returns:
            Complete trading test scenario
        """
        return TradingTestScenario(
            symbols=self._symbols.copy(),
            positions=self._positions.copy(),
            orders=self._orders.copy(),
            balances=self._balances.copy(),
            metadata=self._metadata.copy(),
        )
