"""CyberDeltaEngine: APIs base package.

This package contains base classes and interfaces for exchange API implementations.
"""

from __future__ import annotations

from .authenticator_interface import AuthenticatedRequestComponents, IAuthenticator
from .exchange_api import ExchangeAPI
from .payload_serialization_strategy import (
    DefaultSerializationStrategy,
    PayloadSerializationStrategy,
)
from .rate_limit_models import RateLimitRequestContext
from .rate_limit_strategy_interface import RateLimitStrategy
from .simple_rate_limit_strategy import SimpleTokenBucketStrategy


# Note: IErrorMapper is now in cyberdelta.apis.common to avoid circular imports

__all__ = [
    # Authentication interfaces
    "AuthenticatedRequestComponents",
    "IAuthenticator",
    # Core base class
    "ExchangeAPI",
    # Serialization strategies
    "DefaultSerializationStrategy",
    "PayloadSerializationStrategy",
    # Rate limiting
    "RateLimitRequestContext",
    "RateLimitStrategy",
    "SimpleTokenBucketStrategy",
]
