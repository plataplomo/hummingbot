"""Error context builders.

Builders for creating StreamErrorContext instances from different sources.
"""

from cyberdelta.apis.models.websocket.router import RouterErrorMetadata

from .processor_builder import ProcessorErrorContextBuilder, ProcessorErrorMetadata
from .router_builder import RouterErrorContextBuilder


__all__ = [
    "ProcessorErrorContextBuilder",
    "ProcessorErrorMetadata",
    "RouterErrorContextBuilder",
    "RouterErrorMetadata",
]
