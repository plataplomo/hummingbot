"""Symbol System Usage Examples."""

from decimal import Decimal

from pydantic import BaseModel

from cyberdelta.core.symbols import (
    Symbol,
    bp_symbol,
    get_symbol_service,
    hl_symbol,
)
from cyberdelta.enums.exchange_names import ExchangeName


def basic_usage() -> None:
    """Basic symbol creation and usage."""
    # Create symbols using factory functions
    bp_symbol("BTC_PERP", symbol_id=1)
    _ = bp_symbol("ETH_USDC", symbol_id=20)  # Example of creating another symbol

    # Direct property access

    # Type-safe metadata access


def cross_exchange_operations() -> None:
    """Working with symbols across exchanges."""
    service = get_symbol_service()

    # Create equivalent symbols
    btc_bp = bp_symbol("BTC_PERP", symbol_id=1)
    btc_hl = hl_symbol("BTC-PERP", asset_index=0)

    # Register for equivalence tracking
    service.register_symbol(btc_bp)
    service.register_symbol(btc_hl)

    # Check if they're the same instrument
    if service.are_equivalent(btc_bp, btc_hl):
        pass

    # Convert between exchanges
    eth_hl = hl_symbol("ETH-PERP")
    service.convert_symbol(eth_hl, ExchangeName.BACKPACK)


def using_in_domain_models() -> None:
    """Using symbols in domain models."""

    class Ticker(BaseModel):
        """Domain model using symbols."""

        symbol: Symbol
        bid_price: Decimal
        ask_price: Decimal

        @property
        def spread(self) -> Decimal:
            return self.ask_price - self.bid_price

        @property
        def display_name(self) -> str:
            """Get display name using symbol properties."""
            if self.symbol.quote_asset:
                return f"{self.symbol.base_asset}/{self.symbol.quote_asset}"
            return self.symbol.base_asset

    # Usage
    Ticker(
        symbol=bp_symbol("BTC_PERP", symbol_id=1),
        bid_price=Decimal("50000.00"),
        ask_price=Decimal("50001.00"),
    )


def batch_operations() -> None:
    """Working with multiple symbols."""
    service = get_symbol_service()

    # Define symbols to create
    symbols_to_create = [
        ("BTC_PERP", 1),
        ("ETH_PERP", 2),
        ("SOL_PERP", 3),
    ]

    # Create and register
    for value, symbol_id in symbols_to_create:
        symbol = bp_symbol(value, symbol_id=symbol_id)
        service.register_symbol(symbol)

    # Find specific symbol
    btc = service.find_symbol("BTC_PERP", ExchangeName.BACKPACK)
    if btc:
        pass


if __name__ == "__main__":
    basic_usage()
    cross_exchange_operations()
    using_in_domain_models()
    batch_operations()
