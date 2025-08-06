"""Factory functions for creating symbol domain objects for testing.

This module provides convenient factory functions to create valid symbol domain objects
with realistic test data, supporting both simple and complex test scenarios.
"""

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols import Symbol, get_symbol_service


def create_symbol(
    value: str, exchange: ExchangeName, asset_index: int | None = None, symbol_id: int | None = None
) -> Symbol:
    """Create a symbol using the global symbol service.

    Returns:
        Symbol: Created symbol instance.
    """
    return get_symbol_service().create_symbol(
        value, exchange, asset_index=asset_index, symbol_id=symbol_id
    )


class SymbolFactory:
    """Factory for creating Symbol test objects."""

    @staticmethod
    def create_btc_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid BTC perpetual symbol.

        Returns:
            Symbol for BTC perpetual on Hyperliquid.
        """
        return create_symbol(
            value="BTC-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=0,  # BTC is typically index 0
        )

    @staticmethod
    def create_eth_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid ETH perpetual symbol.

        Returns:
            Symbol for ETH perpetual on Hyperliquid.
        """
        return create_symbol(
            value="ETH-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=1,  # ETH is typically index 1
        )

    @staticmethod
    def create_btc_perp_backpack() -> Symbol:
        """Create Backpack BTC perpetual symbol.

        Returns:
            Symbol for BTC perpetual on Backpack.
        """
        return create_symbol(
            value="BTC_PERP",
            exchange=ExchangeName.BACKPACK,
        )

    @staticmethod
    def create_eth_perp_backpack() -> Symbol:
        """Create Backpack ETH perpetual symbol.

        Returns:
            Symbol for ETH perpetual on Backpack.
        """
        return create_symbol(
            value="ETH_PERP",
            exchange=ExchangeName.BACKPACK,
        )

    @staticmethod
    def create_btc_usdc_spot_hyperliquid() -> Symbol:
        """Create Hyperliquid BTC/USDC spot symbol.

        Returns:
            Symbol for BTC/USDC spot on Hyperliquid.
        """
        return create_symbol(
            value="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
        )

    @staticmethod
    def create_btc_usdc_spot_backpack() -> Symbol:
        """Create Backpack BTC/USDC spot symbol.

        Returns:
            Symbol for BTC/USDC spot on Backpack.
        """
        return create_symbol(
            value="BTC_USDC",
            exchange=ExchangeName.BACKPACK,
        )

    @staticmethod
    def create_sol_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid SOL perpetual symbol.

        Returns:
            Symbol for SOL perpetual on Hyperliquid.
        """
        return create_symbol(
            value="SOL-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=4,  # SOL index may vary
        )

    @staticmethod
    def create_sol_perp_backpack() -> Symbol:
        """Create Backpack SOL perpetual symbol.

        Returns:
            Symbol for SOL perpetual on Backpack.
        """
        return create_symbol(
            value="SOL_PERP",
            exchange=ExchangeName.BACKPACK,
        )

    @staticmethod
    def create_spot_index_1_hyperliquid() -> Symbol:
        """Create Hyperliquid spot index @1 symbol.

        Returns:
            Symbol for spot index @1 on Hyperliquid.
        """
        return create_symbol(
            value="@1",
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=1,
        )

    @staticmethod
    def create_custom_hyperliquid(value: str, asset_index: int | None = None) -> Symbol:
        """Create custom Hyperliquid symbol.

        Args:
            value: Symbol value
            asset_index: Optional asset index

        Returns:
            Symbol for Hyperliquid
        """
        return create_symbol(
            value=value,
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=asset_index,
        )

    @staticmethod
    def create_custom_backpack(value: str) -> Symbol:
        """Create custom Backpack symbol.

        Args:
            value: Symbol value

        Returns:
            Symbol for Backpack
        """
        return create_symbol(
            value=value,
            exchange=ExchangeName.BACKPACK,
        )


# Backward compatibility aliases for easier migration
class InternalSymbolFactory:
    """Factory for creating symbols (backward compatibility)."""

    @staticmethod
    def create_btc_usd_perp() -> Symbol:
        """Create BTC perpetual symbol.

        Returns:
            Symbol: BTC perpetual symbol.
        """
        return SymbolFactory.create_btc_perp_hyperliquid()

    @staticmethod
    def create_eth_usd_perp() -> Symbol:
        """Create ETH perpetual symbol.

        Returns:
            Symbol: ETH perpetual symbol.
        """
        return SymbolFactory.create_eth_perp_hyperliquid()

    @staticmethod
    def create_btc_usdc_spot() -> Symbol:
        """Create BTC/USDC spot symbol.

        Returns:
            Symbol: BTC/USDC spot symbol.
        """
        return SymbolFactory.create_btc_usdc_spot_hyperliquid()

    @staticmethod
    def create_eth_usdc_spot() -> Symbol:
        """Create ETH/USDC spot symbol.

        Returns:
            Symbol: ETH/USDC spot symbol.
        """
        return create_symbol(
            value="ETH-USDC",
            exchange=ExchangeName.HYPERLIQUID,
        )

    @staticmethod
    def create_sol_usd_perp() -> Symbol:
        """Create SOL perpetual symbol.

        Returns:
            Symbol: SOL perpetual symbol.
        """
        return SymbolFactory.create_sol_perp_hyperliquid()

    @staticmethod
    def create_custom(
        base_asset: str, quote_asset: str | None = None, market_type: MarketType = MarketType.PERP
    ) -> Symbol:
        """Create custom symbol.

        Returns:
            Symbol: Custom symbol based on specified assets and market type.
        """
        if market_type == MarketType.PERP:
            value = f"{base_asset}-PERP"
        else:
            value = f"{base_asset}-{quote_asset or 'USDC'}"

        return create_symbol(
            value=value,
            exchange=ExchangeName.HYPERLIQUID,
        )


# Backward compatibility for ExchangeSymbolFactory
class ExchangeSymbolFactory:
    """Factory for creating exchange symbols (backward compatibility)."""

    @staticmethod
    def create_hyperliquid_btc_perp() -> Symbol:
        """Create Hyperliquid BTC perpetual symbol.

        Returns:
            Symbol: BTC perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_btc_perp_hyperliquid()

    @staticmethod
    def btc_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid BTC perpetual symbol.

        Returns:
            Symbol: BTC perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_btc_perp_hyperliquid()

    @staticmethod
    def create_hyperliquid_eth_perp() -> Symbol:
        """Create Hyperliquid ETH perpetual symbol.

        Returns:
            Symbol: ETH perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_eth_perp_hyperliquid()

    @staticmethod
    def eth_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid ETH perpetual symbol.

        Returns:
            Symbol: ETH perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_eth_perp_hyperliquid()

    @staticmethod
    def create_backpack_btc_perp() -> Symbol:
        """Create Backpack BTC perpetual symbol.

        Returns:
            Symbol: BTC perpetual symbol for Backpack.
        """
        return SymbolFactory.create_btc_perp_backpack()

    @staticmethod
    def btc_perp_backpack() -> Symbol:
        """Create Backpack BTC perpetual symbol.

        Returns:
            Symbol: BTC perpetual symbol for Backpack.
        """
        return SymbolFactory.create_btc_perp_backpack()

    @staticmethod
    def create_backpack_eth_perp() -> Symbol:
        """Create Backpack ETH perpetual symbol.

        Returns:
            Symbol: ETH perpetual symbol for Backpack.
        """
        return SymbolFactory.create_eth_perp_backpack()

    @staticmethod
    def eth_perp_backpack() -> Symbol:
        """Create Backpack ETH perpetual symbol.

        Returns:
            Symbol: ETH perpetual symbol for Backpack.
        """
        return SymbolFactory.create_eth_perp_backpack()

    @staticmethod
    def create_hyperliquid_btc_usdc_spot() -> Symbol:
        """Create Hyperliquid BTC/USDC spot symbol.

        Returns:
            Symbol: BTC/USDC spot symbol for Hyperliquid.
        """
        return SymbolFactory.create_btc_usdc_spot_hyperliquid()

    @staticmethod
    def btc_usdc_spot_hyperliquid() -> Symbol:
        """Create Hyperliquid BTC/USDC spot symbol.

        Returns:
            Symbol: BTC/USDC spot symbol for Hyperliquid.
        """
        return SymbolFactory.create_btc_usdc_spot_hyperliquid()

    @staticmethod
    def create_backpack_btc_usdc_spot() -> Symbol:
        """Create Backpack BTC/USDC spot symbol.

        Returns:
            Symbol: BTC/USDC spot symbol for Backpack.
        """
        return SymbolFactory.create_btc_usdc_spot_backpack()

    @staticmethod
    def btc_usdc_spot_backpack() -> Symbol:
        """Create Backpack BTC/USDC spot symbol.

        Returns:
            Symbol: BTC/USDC spot symbol for Backpack.
        """
        return SymbolFactory.create_btc_usdc_spot_backpack()

    @staticmethod
    def create_hyperliquid_sol_perp() -> Symbol:
        """Create Hyperliquid SOL perpetual symbol.

        Returns:
            Symbol: SOL perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_sol_perp_hyperliquid()

    @staticmethod
    def sol_perp_hyperliquid() -> Symbol:
        """Create Hyperliquid SOL perpetual symbol.

        Returns:
            Symbol: SOL perpetual symbol for Hyperliquid.
        """
        return SymbolFactory.create_sol_perp_hyperliquid()

    @staticmethod
    def create_backpack_sol_perp() -> Symbol:
        """Create Backpack SOL perpetual symbol.

        Returns:
            Symbol: SOL perpetual symbol for Backpack.
        """
        return SymbolFactory.create_sol_perp_backpack()

    @staticmethod
    def sol_perp_backpack() -> Symbol:
        """Create Backpack SOL perpetual symbol.

        Returns:
            Symbol: SOL perpetual symbol for Backpack.
        """
        return SymbolFactory.create_sol_perp_backpack()

    @staticmethod
    def create_hyperliquid_spot_index(index: int) -> Symbol:
        """Create Hyperliquid spot index symbol.

        Returns:
            Symbol: Spot index symbol for Hyperliquid.
        """
        return create_symbol(
            value=f"@{index}",
            exchange=ExchangeName.HYPERLIQUID,
            asset_index=index,
        )

    @staticmethod
    def spot_index_1_hyperliquid() -> Symbol:
        """Create Hyperliquid spot index @1 symbol.

        Returns:
            Symbol: Spot index @1 symbol for Hyperliquid.
        """
        return SymbolFactory.create_spot_index_1_hyperliquid()

    @staticmethod
    def create_hyperliquid_btc_spot() -> Symbol:
        """Create Hyperliquid BTC spot symbol (same as BTC/USDC spot).

        Returns:
            Symbol: BTC spot symbol for Hyperliquid.
        """
        return SymbolFactory.create_btc_usdc_spot_hyperliquid()

    @staticmethod
    def create_backpack_btc_spot() -> Symbol:
        """Create Backpack BTC spot symbol (same as BTC/USDC spot).

        Returns:
            Symbol: BTC spot symbol for Backpack.
        """
        return SymbolFactory.create_btc_usdc_spot_backpack()

    @staticmethod
    def create_hyperliquid_usdc() -> Symbol:
        """Create Hyperliquid USDC asset symbol.

        Returns:
            Symbol: USDC asset symbol for Hyperliquid.
        """
        return SymbolFactory.create_custom_hyperliquid("USDC")

    @staticmethod
    def create_backpack_usdc() -> Symbol:
        """Create Backpack USDC asset symbol.

        Returns:
            Symbol: USDC asset symbol for Backpack.
        """
        return SymbolFactory.create_custom_backpack("USDC")

    @staticmethod
    def create_hyperliquid_btc() -> Symbol:
        """Create Hyperliquid BTC asset symbol.

        Returns:
            Symbol: BTC asset symbol for Hyperliquid.
        """
        return SymbolFactory.create_custom_hyperliquid("BTC")

    @staticmethod
    def create_backpack_btc() -> Symbol:
        """Create Backpack BTC asset symbol.

        Returns:
            Symbol: BTC asset symbol for Backpack.
        """
        return SymbolFactory.create_custom_backpack("BTC")

    @staticmethod
    def create_custom(
        value: str,
        exchange_id: ExchangeName,
        internal_symbol: Symbol | None = None,  # Not used in new system
    ) -> Symbol:
        """Create custom exchange symbol.

        Returns:
            Symbol: Custom symbol for the specified exchange.
        """
        if exchange_id == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_custom_hyperliquid(value)
        return SymbolFactory.create_custom_backpack(value)


# Export the unified symbol factory for convenience
class UnifiedSymbolFactory:
    """Factory for creating unified symbols (backward compatibility)."""

    @staticmethod
    def create_btc_perp(exchanges: list[ExchangeName] | None = None) -> Symbol:
        """Create BTC perpetual symbol.

        For backward compatibility, returns a single symbol for the first exchange.

        Returns:
            Symbol: BTC perpetual symbol for the first specified exchange.
        """
        if not exchanges:
            exchanges = [ExchangeName.HYPERLIQUID]

        if exchanges[0] == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_btc_perp_hyperliquid()
        return SymbolFactory.create_btc_perp_backpack()

    @staticmethod
    def create_eth_perp(exchanges: list[ExchangeName] | None = None) -> Symbol:
        """Create ETH perpetual symbol.

        For backward compatibility, returns a single symbol for the first exchange.

        Returns:
            Symbol: ETH perpetual symbol for the first specified exchange.
        """
        if not exchanges:
            exchanges = [ExchangeName.HYPERLIQUID]

        if exchanges[0] == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_eth_perp_hyperliquid()
        return SymbolFactory.create_eth_perp_backpack()

    @staticmethod
    def create_btc_usdc_spot(exchanges: list[ExchangeName] | None = None) -> Symbol:
        """Create BTC/USDC spot symbol.

        For backward compatibility, returns a single symbol for the first exchange.

        Returns:
            Symbol: BTC/USDC spot symbol for the first specified exchange.
        """
        if not exchanges:
            exchanges = [ExchangeName.HYPERLIQUID]

        if exchanges[0] == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_btc_usdc_spot_hyperliquid()
        return SymbolFactory.create_btc_usdc_spot_backpack()

    @staticmethod
    def create_sol_perp(exchanges: list[ExchangeName] | None = None) -> Symbol:
        """Create SOL perpetual symbol.

        For backward compatibility, returns a single symbol for the first exchange.

        Returns:
            Symbol: SOL perpetual symbol for the first specified exchange.
        """
        if not exchanges:
            exchanges = [ExchangeName.HYPERLIQUID]

        if exchanges[0] == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_sol_perp_hyperliquid()
        return SymbolFactory.create_sol_perp_backpack()

    @staticmethod
    def create_custom(
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType = MarketType.PERP,
        exchanges: list[ExchangeName] | None = None,
    ) -> Symbol:
        """Create custom unified symbol.

        For backward compatibility, returns a single symbol for the first exchange.

        Returns:
            Symbol: Custom symbol based on specified assets and market type for the first exchange.
        """
        if not exchanges:
            exchanges = [ExchangeName.HYPERLIQUID]

        if exchanges[0] == ExchangeName.HYPERLIQUID:
            if market_type == MarketType.PERP:
                value = f"{base_asset}-PERP"
            else:
                value = f"{base_asset}-{quote_asset or 'USDC'}"
            return SymbolFactory.create_custom_hyperliquid(value)
        if market_type == MarketType.PERP:
            value = f"{base_asset}_PERP"
        else:
            value = f"{base_asset}_{quote_asset or 'USDC'}"
        return SymbolFactory.create_custom_backpack(value)
