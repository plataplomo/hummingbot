"""Exchange Handlers."""

from typing import Any

from cyberdelta.core.symbols.protocols import ExchangeHandler
from cyberdelta.enums.exchange_names import ExchangeName

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
