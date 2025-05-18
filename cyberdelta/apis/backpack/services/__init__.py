"""
CyberDeltaEngine: Backpack Exchange Services
-------------------------------------------

This package contains service classes that encapsulate specific categories of
API interactions for the Backpack exchange, such as account management, market data,
and trading operations.

Services typically use a request builder, an HTTP client (or a requester callable),
and a response handler to interact with the API and return validated raw Pydantic models.
The main API client (`BackpackAPI`) then consumes these raw models and uses mappers
to convert them into internal domain models.
"""

from .bp_account_service import BackpackAccountService
from .bp_market_data_service import BackpackMarketDataService
from .bp_trading_service import BackpackTradingService

# Placeholder for BackpackTradingService, to be added in this step
# from .bp_trading_service import BackpackTradingService

__all__ = [
    "BackpackAccountService",
    "BackpackMarketDataService",
    "BackpackTradingService",
    # "BackpackTradingService", # Uncomment when implemented
]
