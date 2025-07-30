"""Symbol Protocols for Clean Architecture."""

from typing import Protocol, TypeVar

from cyberdelta.enums.exchange_names import ExchangeName

from .models import Symbol, SymbolComponents, SymbolMetadata


TMetadata_co = TypeVar("TMetadata_co", bound=SymbolMetadata, covariant=True)


class ExchangeHandler(Protocol[TMetadata_co]):
    """Protocol for exchange-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        """The exchange this handler is for."""
        ...

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse symbol value into components using exchange rules."""
        ...

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into exchange-specific symbol value."""
        ...

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format and return components."""
        ...

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical format to exchange format."""
        ...

    def create_metadata(
        self, asset_index: int | None = None, symbol_id: int | None = None
    ) -> TMetadata_co:
        """Create exchange-specific metadata."""
        ...

    def create_symbol(
        self, value: str, asset_index: int | None = None, symbol_id: int | None = None
    ) -> Symbol:
        """Create symbol with proper metadata."""
        ...
