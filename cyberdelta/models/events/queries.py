"""Query and response events for request/response patterns.

These events enable async queries using the EventBus request/response pattern,
replacing synchronous operations with truly async event-driven queries.
"""

from decimal import Decimal

import msgspec

from cyberdelta.enums.exchange_names import ExchangeName


# Position Query Events
class PositionQuery(msgspec.Struct, tag="position_query", frozen=True):
    """Query for position information using event bus.

    Enables truly async position queries instead of synchronous dict lookups.
    """

    request_id: str
    symbol: str
    exchange: ExchangeName
    query_type: str  # "exists", "details", "pnl"


class PositionQueryResponse(msgspec.Struct, tag="position_response", frozen=True):
    """Response to position query."""

    request_id: str
    exists: bool
    size: Decimal | None = None
    entry_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    error: str | None = None
