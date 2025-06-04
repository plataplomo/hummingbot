"""CyberDeltaEngine: Backpack Exchange API Package
-----------------------------------------------

This package contains all Backpack exchange-specific API implementations,
including the main API client, authentication, error handling, and rate limiting.
"""

from __future__ import annotations

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy

__all__ = [
    "BackpackAPI",
    "BackpackEd25519Authenticator",
    "BackpackErrorMapper",
    "BackpackRateLimitStrategy",
]
