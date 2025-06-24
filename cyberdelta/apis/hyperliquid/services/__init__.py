"""CyberDeltaEngine: Hyperliquid Exchange Services.

----------------------------------------------

This package contains service classes that encapsulate specific categories of
API interactions for the Hyperliquid exchange, such as account management,
market data, and trading operations.

Services typically use a request builder, an HTTP client (or a requester callable),
and a response handler to interact with the API and return validated raw Pydantic models.
The main API client (`HyperliquidAPI`) then consumes these raw models and uses mappers
to convert them into internal domain models.
"""

from .hl_account_service import HyperliquidAccountService
from .hl_market_data_service import HyperliquidMarketDataService


# Placeholder for HyperliquidTradingService, to be added in this step
# from .hl_trading_service import HyperliquidTradingService

__all__ = [
    "HyperliquidAccountService",
    "HyperliquidMarketDataService",
    # "HyperliquidTradingService", # Uncomment when implemented
]

# This file makes Python treat the directory as a package.

# Required for package recognition
