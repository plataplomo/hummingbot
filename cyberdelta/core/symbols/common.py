"""Common symbol constants for frequently used symbols."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from cyberdelta.enums.exchange_names import ExchangeName

from .api import symbol
from .models import Symbol


@dataclass
class AssetSymbols:
    """Container for an asset's symbols across exchanges."""

    asset: str
    _creators: dict[ExchangeName, Callable[[], Symbol]]

    def __getattr__(self, exchange_name: str) -> Callable[[], Symbol]:
        """Get symbol creator for an exchange.

        Usage: btc.hyperliquid() or btc.backpack()
        
        Returns:
            Callable[[], Symbol]: Function that creates a symbol for the exchange.
            
        Raises:
            AttributeError: If exchange is not found or invalid.
        """
        try:
            exchange = ExchangeName(exchange_name)
            return self._creators[exchange]
        except (ValueError, KeyError) as e:
            msg = f"No symbol defined for {exchange_name}"
            raise AttributeError(msg) from e

    def all(self) -> list[Symbol]:
        """Get symbols for all configured exchanges.
        
        Returns:
            list[Symbol]: List of symbols for all exchanges.
        """
        return [creator() for creator in self._creators.values()]

    def for_exchanges(self, exchanges: list[ExchangeName]) -> list[Symbol]:
        """Get symbols for specific exchanges.
        
        Returns:
            list[Symbol]: List of symbols for the specified exchanges.
        """
        return [self._creators[ex]() for ex in exchanges if ex in self._creators]


class CommonSymbols:
    """Common symbols for major assets."""

    # BTC symbols across our two exchanges
    BTC = AssetSymbols(
        asset="BTC",
        _creators={
            ExchangeName.HYPERLIQUID: lambda: symbol("BTC-PERP", ExchangeName.HYPERLIQUID),
            ExchangeName.BACKPACK: lambda: symbol(
                "BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345
            ),
        },
    )

    # ETH symbols across our two exchanges
    ETH = AssetSymbols(
        asset="ETH",
        _creators={
            ExchangeName.HYPERLIQUID: lambda: symbol("ETH-PERP", ExchangeName.HYPERLIQUID),
            ExchangeName.BACKPACK: lambda: symbol(
                "ETH_USD_PERP", ExchangeName.BACKPACK, symbol_id=67890
            ),
        },
    )

    # SOL symbols
    SOL = AssetSymbols(
        asset="SOL",
        _creators={
            ExchangeName.HYPERLIQUID: lambda: symbol("SOL-PERP", ExchangeName.HYPERLIQUID),
            ExchangeName.BACKPACK: lambda: symbol(
                "SOL_USD_PERP", ExchangeName.BACKPACK, symbol_id=98765
            ),
        },
    )

    @classmethod
    def add_asset(cls, asset: str, symbols_config: dict[ExchangeName, dict[str, Any]]) -> None:
        """Dynamically add a new asset."""
        creators: dict[ExchangeName, Callable[[], Symbol]] = {}
        for exchange, config in symbols_config.items():
            value = config.pop("value")
            asset_index = config.get("asset_index")
            symbol_id = config.get("symbol_id")

            # Create a closure for each exchange config
            def make_creator(
                v: str, e: ExchangeName, ai: int | None = None, si: int | None = None
            ) -> Callable[[], Symbol]:
                return lambda: symbol(v, e, asset_index=ai, symbol_id=si)

            creators[exchange] = make_creator(value, exchange, asset_index, symbol_id)

        setattr(cls, asset.upper(), AssetSymbols(asset=asset, _creators=creators))


# Shorter alias
symbols = CommonSymbols
