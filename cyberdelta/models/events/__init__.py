"""Domain event models for the trading system.

This module provides the domain event system for all events,
using a generic event pattern for flexibility and maintainability.
"""

from cyberdelta.models.events.domain_event import DomainEvent


__all__ = [
    "DomainEvent",  # Main event class
]
