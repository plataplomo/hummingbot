"""Exchange name enumeration for CyberDeltaEngine."""

from enum import Enum


class ExchangeName(str, Enum):
    """Enumeration of supported exchanges.

    This enum defines the canonical names for all exchanges supported
    by the CyberDeltaEngine. These names are used throughout the system
    for configuration, routing, and identification.
    """

    HYPERLIQUID = "hyperliquid"
    BACKPACK = "backpack"


__all__ = ["ExchangeName"]
