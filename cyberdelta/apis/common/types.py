"""CyberDeltaEngine: Common API types.

This module contains shared type aliases used across exchange API implementations.
"""

from __future__ import annotations

# Re-export MessageHandler from base_types for backward compatibility
from cyberdelta.apis.common.base_types import MessageHandler


__all__ = ["MessageHandler"]
