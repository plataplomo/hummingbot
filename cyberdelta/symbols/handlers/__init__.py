"""Exchange Handlers."""

from typing import Any

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols.protocols import ExchangeHandler

from .backpack import BackpackHandler
from .hyperliquid import HyperliquidHandler


# Default handler registry
DEFAULT_HANDLERS: dict[ExchangeName, ExchangeHandler[Any]] = {
    ExchangeName.HYPERLIQUID: HyperliquidHandler(),
    ExchangeName.BACKPACK: BackpackHandler(),
}

__all__ = [
    "DEFAULT_HANDLERS",
    "BackpackHandler",
    "HyperliquidHandler",
]
