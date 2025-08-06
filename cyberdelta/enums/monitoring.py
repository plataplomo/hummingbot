"""Monitoring-related enums."""

from enum import Enum


class ServiceType(Enum):
    """Types of services that can be monitored."""

    PORTFOLIO = "portfolio_service"
    MARKET_DATA = "market_data_service"
    RISK = "risk_service"
    EXECUTION = "execution_engine"
    TRADING = "trading_service"
    SIGNAL = "signal_service"
    STRATEGY = "strategy_service"
    EVENT_BUS = "event_bus"
