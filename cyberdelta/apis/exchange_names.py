from enum import Enum


class ExchangeName(str, Enum):
    """
    Canonical exchange name identifiers for CyberDeltaEngine.
    Use these instead of hardcoded strings for all exchange selection, logging, and API dispatch.
    """

    BACKPACK = "backpack"
    HYPERLIQUID = "hyperliquid"
