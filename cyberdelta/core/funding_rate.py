import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

# Configure logging
logger = logging.getLogger(__name__)


# Define Exchange IDs for system identification
class ExchangeId(Enum):
    HYPERLIQUID = "hyperliquid"
    BACKPACK = "backpack"


@dataclass
class Asset:
    """
    Represents a tradable asset with its specifications.
    """

    symbol: str  # Trading symbol (e.g., "BTC-PERP")
    exchange: ExchangeId  # Exchange ID
    tick_size: float  # Minimum price movement
    step_size: float  # Minimum quantity movement
    min_qty: float  # Minimum order quantity
    max_leverage: float  # Maximum allowed leverage


@dataclass
class FundingRateInfo:
    """
    Represents funding rate information for a perpetual contract.
    """

    symbol: str
    current_rate: float  # Current funding rate (annualized %)
    historical_rates: list[float]  # Recent historical rates
    historical_volatility: float  # Volatility of historical rates
    next_funding_time: int  # Timestamp of next funding payment
    funding_interval: int  # Funding interval in hours


@dataclass
class Position:
    """
    Represents an open position.
    """

    symbol: str
    exchange: ExchangeId
    size: float
    entry_price: float
    leverage: float
    liquidation_price: float | None = None
    unrealized_pnl: float = 0.0


class PositionSide(Enum):
    """
    Position side (long or short).
    """

    LONG = "long"
    SHORT = "short"


@dataclass
class ArbitrageOpportunity:
    """
    Represents an identified funding rate arbitrage opportunity.
    """

    asset: Asset
    funding_rate: float
    expected_return: float
    optimal_size: float
    side: PositionSide
    confidence: float
    timestamp: datetime


class ExchangeAdapter:
    """
    Base class for exchange adapters, providing a common interface
    for interacting with different exchanges.
    """

    def __init__(self, exchange_id: ExchangeId, config: dict):
        """
        Initialize the exchange adapter.

        Args:
            exchange_id: The exchange identifier
            config: Configuration parameters
        """
        self.exchange_id = exchange_id
        self.config = config
        self.assets = {}  # Symbol -> Asset mapping

    async def initialize(self):
        """
        Initialize the adapter by fetching exchange information and setting up connections.
        """
        raise NotImplementedError("Subclasses must implement initialize()")

    async def get_funding_rates(self) -> dict[str, FundingRateInfo]:
        """
        Get current funding rates for all available assets.

        Returns:
            Dictionary mapping asset symbols to their funding rate information
        """
        raise NotImplementedError("Subclasses must implement get_funding_rates()")

    async def get_historical_funding_rates(
        self, symbol: str, lookback_hours: int = 24
    ) -> list[tuple[int, float]]:
        """
        Get historical funding rates for an asset.

        Args:
            symbol: Asset symbol
            lookback_hours: Number of hours to look back

        Returns:
            List of (timestamp, rate) tuples
        """
        raise NotImplementedError("Subclasses must implement get_historical_funding_rates()")

    async def get_market_price(self, symbol: str) -> float:
        """
        Get current market price for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Current market price
        """
        raise NotImplementedError("Subclasses must implement get_market_price()")

    async def get_position(self, symbol: str) -> Position | None:
        """
        Get current position for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Position object or None if no position exists
        """
        raise NotImplementedError("Subclasses must implement get_position()")

    async def open_position(self, symbol: str, side: PositionSide, size: float) -> bool:
        """
        Open a new position.

        Args:
            symbol: Asset symbol
            side: Position side (LONG or SHORT)
            size: Position size

        Returns:
            True if successful, False otherwise
        """
        raise NotImplementedError("Subclasses must implement open_position()")

    async def close_position(self, symbol: str) -> bool:
        """
        Close an existing position.

        Args:
            symbol: Asset symbol

        Returns:
            True if successful, False otherwise
        """
        raise NotImplementedError("Subclasses must implement close_position()")


class HyperliquidAdapter(ExchangeAdapter):
    """
    Adapter for Hyperliquid exchange.
    """

    def __init__(self, config: dict, api_client):
        super().__init__(ExchangeId.HYPERLIQUID, config)
        self.api_client = api_client

    async def initialize(self):
        """Initialize the adapter by fetching available assets."""
        logger.info("Initializing Hyperliquid adapter")

        try:
            # Fetch available assets
            meta_response = await self.api_client._request(
                "POST", "/info", data={"type": "metaAndAssetCtxs"}
            )

            if "data" in meta_response and "assetCtxs" in meta_response["data"]:
                asset_contexts = meta_response["data"]["assetCtxs"]

                for asset_ctx in asset_contexts:
                    symbol = asset_ctx.get("name")
                    if not symbol:
                        continue

                    # Extract specification from asset context
                    tick_size = float(asset_ctx.get("tickSize", "0.1"))
                    step_size = float(asset_ctx.get("stepSize", "0.001"))
                    min_qty = float(asset_ctx.get("minSize", "0.001"))
                    max_leverage = float(asset_ctx.get("maxLeverage", "10"))

                    self.assets[symbol] = Asset(
                        symbol=symbol,
                        exchange=self.exchange_id,
                        tick_size=tick_size,
                        step_size=step_size,
                        min_qty=min_qty,
                        max_leverage=max_leverage,
                    )

            logger.info(f"Initialized {len(self.assets)} assets for Hyperliquid")

        except Exception as e:
            logger.error(f"Error initializing Hyperliquid adapter: {e}")
            raise

    async def get_funding_rates(self) -> dict[str, FundingRateInfo]:
        """Get current funding rates for all assets."""
        funding_rates = {}

        try:
            # Fetch funding rates for all assets
            meta_response = await self.api_client._request(
                "POST", "/info", data={"type": "metaAndAssetCtxs"}
            )

            if "data" in meta_response and "assetCtxs" in meta_response["data"]:
                asset_contexts = meta_response["data"]["assetCtxs"]

                current_time = int(time.time() * 1000)

                for asset_ctx in asset_contexts:
                    symbol = asset_ctx.get("name")
                    if not symbol or symbol not in self.assets:
                        continue

                    # Extract funding rate (converting to annualized percentage)
                    funding_rate_raw = float(asset_ctx.get("funding", {}).get("rate", "0"))
                    # Hyperliquid funding rates are typically in decimal form and need to be converted to percentage
                    # For example, 0.0001 -> 0.01% per hour -> 0.01*24 = 0.24% per day -> 0.24*365 = 87.6% annualized
                    funding_rate = funding_rate_raw * 24 * 365 * 100

                    # Fetch historical rates for volatility calculation
                    historical_rates = await self.get_historical_funding_rates(symbol)
                    historical_rates_values = [rate for _, rate in historical_rates]

                    # Calculate volatility (standard deviation)
                    if historical_rates_values:
                        mean = sum(historical_rates_values) / len(historical_rates_values)
                        variance = sum((x - mean) ** 2 for x in historical_rates_values) / len(
                            historical_rates_values
                        )
                        volatility = variance**0.5
                    else:
                        volatility = 0.0

                    # Determine next funding time (Hyperliquid has hourly funding)
                    minutes_to_hour = 60 - (datetime.fromtimestamp(current_time / 1000).minute)
                    next_funding_time = current_time + (minutes_to_hour * 60 * 1000)

                    funding_rates[symbol] = FundingRateInfo(
                        symbol=symbol,
                        current_rate=funding_rate,
                        historical_rates=historical_rates_values,
                        historical_volatility=volatility,
                        next_funding_time=next_funding_time,
                        funding_interval=1,  # 1 hour funding interval
                    )

            return funding_rates

        except Exception as e:
            logger.error(f"Error getting funding rates from Hyperliquid: {e}")
            return {}

    async def get_historical_funding_rates(
        self, symbol: str, lookback_hours: int = 24
    ) -> list[tuple[int, float]]:
        """Get historical funding rates for an asset."""
        try:
            # Hyperliquid doesn't have a specific endpoint for historical funding rates
            # We'll use a simulated approach for the prototype
            # In production, you would fetch from a database or other source

            # Example implementation that simulates historical data
            current_time = int(time.time() * 1000)
            historical_rates = []

            # Get current rate as a reference
            current_rate_raw = 0.0
            try:
                funding_response = await self.api_client._request(
                    "POST", "/info", data={"type": "fundingRate", "coin": symbol}
                )
                if "data" in funding_response and "fundingRate" in funding_response["data"]:
                    current_rate_raw = float(funding_response["data"]["fundingRate"])
            except Exception as e:
                logger.warning(f"Error getting current funding rate for {symbol}: {e}")

            # Convert to annualized percentage
            current_rate = current_rate_raw * 24 * 365 * 100

            # Generate historical rates (in a real implementation, fetch from database)
            hour_ms = 60 * 60 * 1000
            for hour in range(lookback_hours, 0, -1):
                timestamp = current_time - (hour * hour_ms)

                # Add some randomness to simulate historical variation
                # In a real implementation, this would be actual historical data
                variation = random.uniform(-0.5, 0.5)  # Small random variation
                historical_rate = max(0, current_rate + variation)

                historical_rates.append((timestamp, historical_rate))

            return historical_rates

        except Exception as e:
            logger.error(f"Error getting historical funding rates for {symbol}: {e}")
            return []

    async def get_market_price(self, symbol: str) -> float:
        """Get current market price for an asset."""
        try:
            ticker_response = await self.api_client.get_ticker(symbol)
            return ticker_response.price
        except Exception as e:
            logger.error(f"Error getting market price for {symbol}: {e}")
            return 0.0

    async def get_position(self, symbol: str) -> Position | None:
        """Get current position for an asset."""
        try:
            positions = await self.api_client.get_positions()

            if symbol in positions:
                position = positions[symbol]

                return Position(
                    symbol=symbol,
                    exchange=self.exchange_id,
                    size=position.size,
                    entry_price=position.entry_price,
                    leverage=position.leverage,
                    liquidation_price=position.liquidation_price,
                    unrealized_pnl=position.unrealized_pnl,
                )

            return None

        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return None

    async def open_position(self, symbol: str, side: PositionSide, size: float) -> bool:
        """Open a new position."""
        try:
            # Convert side to OrderSide
            from ..apis.base import OrderSide

            order_side = OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL

            # Get current market price
            price = await self.get_market_price(symbol)
            if price <= 0:
                logger.error(f"Invalid market price {price} for {symbol}")
                return False

            # Place a market order
            from ..apis.base import OrderType

            response = await self.api_client.place_order(
                symbol=symbol,
                side=order_side,
                order_type=OrderType.MARKET,
                quantity=size,
            )

            logger.info(f"Opened position for {symbol} on Hyperliquid: {side.value}, size: {size}")
            return True

        except Exception as e:
            logger.error(f"Error opening position for {symbol} on Hyperliquid: {e}")
            return False

    async def close_position(self, symbol: str) -> bool:
        """Close an existing position."""
        try:
            # Get current position
            position = await self.get_position(symbol)

            if not position or position.size <= 0:
                logger.warning(f"No position found for {symbol}")
                return False

            # Determine close side (opposite of current position)
            from ..apis.base import OrderSide

            close_side = OrderSide.SELL if position.entry_price > 0 else OrderSide.BUY

            # Place market order to close
            from ..apis.base import OrderType

            response = await self.api_client.place_order(
                symbol=symbol,
                side=close_side,
                order_type=OrderType.MARKET,
                quantity=position.size,
            )

            logger.info(f"Closed position for {symbol} on Hyperliquid: size {position.size}")
            return True

        except Exception as e:
            logger.error(f"Error closing position for {symbol} on Hyperliquid: {e}")
            return False


class BackpackAdapter(ExchangeAdapter):
    """
    Adapter for Backpack exchange.
    """

    def __init__(self, config: dict, api_client):
        super().__init__(ExchangeId.BACKPACK, config)
        self.api_client = api_client

    async def initialize(self):
        """Initialize the adapter by fetching available assets."""
        logger.info("Initializing Backpack adapter")

        try:
            # Fetch exchange information
            exchange_info = await self.api_client._request("GET", "/api/v1/exchangeInfo")

            if "symbols" in exchange_info:
                for symbol_info in exchange_info["symbols"]:
                    symbol = symbol_info.get("symbol")

                    # Only include perpetual contracts
                    if not symbol or not symbol.endswith("-PERP"):
                        continue

                    # Extract specification
                    tick_size = float(symbol_info.get("tickSize", "0.1"))
                    min_qty = float(symbol_info.get("minQty", "0.001"))
                    max_leverage = float(symbol_info.get("maxLeverage", "20"))

                    self.assets[symbol] = Asset(
                        symbol=symbol,
                        exchange=self.exchange_id,
                        tick_size=tick_size,
                        step_size=min_qty,  # Using minQty as step size
                        min_qty=min_qty,
                        max_leverage=max_leverage,
                    )

            logger.info(f"Initialized {len(self.assets)} assets for Backpack")

        except Exception as e:
            logger.error(f"Error initializing Backpack adapter: {e}")
            raise

    async def get_funding_rates(self) -> dict[str, FundingRateInfo]:
        """Get current funding rates for all assets."""
        funding_rates = {}

        try:
            # Fetch funding information
            funding_info = await self.api_client._request("GET", "/api/v1/fundingInfo")

            if not funding_info:
                logger.warning("Empty funding info from Backpack")
                return funding_rates

            current_time = int(time.time() * 1000)

            for fund_item in funding_info:
                symbol = fund_item.get("symbol")
                if not symbol or symbol not in self.assets:
                    continue

                # Extract funding rate (convert to annualized percentage)
                funding_rate_raw = float(fund_item.get("fundingRate", "0"))
                funding_rate_volatility = float(fund_item.get("rateVolatility", "0"))

                # Convert to annualized percentage (assuming 8-hour funding)
                # For example, 0.001 -> 0.1% per 8 hours -> 0.1*3 = 0.3% per day -> 0.3*365 = 109.5% annualized
                funding_rate = funding_rate_raw * 3 * 365 * 100

                # Get historical rates
                historical_rates = await self.get_historical_funding_rates(symbol)
                historical_rates_values = [rate for _, rate in historical_rates]

                # Calculate volatility or use provided volatility
                if historical_rates_values:
                    mean = sum(historical_rates_values) / len(historical_rates_values)
                    variance = sum((x - mean) ** 2 for x in historical_rates_values) / len(
                        historical_rates_values
                    )
                    volatility = variance**0.5
                else:
                    volatility = funding_rate_volatility * 3 * 365 * 100  # Annualized

                # Determine next funding time (Backpack typically has 8-hour funding)
                # Next funding at 00:00, 08:00, 16:00 UTC
                current_hour = datetime.fromtimestamp(current_time / 1000).hour
                next_period = (current_hour // 8 + 1) % 3
                hours_until_next = (next_period * 8) - current_hour
                if hours_until_next <= 0:
                    hours_until_next += 24

                next_funding_time = current_time + (hours_until_next * 60 * 60 * 1000)

                funding_rates[symbol] = FundingRateInfo(
                    symbol=symbol,
                    current_rate=funding_rate,
                    historical_rates=historical_rates_values,
                    historical_volatility=volatility,
                    next_funding_time=next_funding_time,
                    funding_interval=8,  # 8-hour funding interval
                )

            return funding_rates

        except Exception as e:
            logger.error(f"Error getting funding rates from Backpack: {e}")
            return {}

    async def get_historical_funding_rates(
        self, symbol: str, lookback_hours: int = 24
    ) -> list[tuple[int, float]]:
        """Get historical funding rates for an asset."""
        try:
            # Backpack might not have a specific endpoint for historical funding rates
            # In a production system, you would fetch from a database where you've stored historical rates

            # Example implementation that simulates historical data
            current_time = int(time.time() * 1000)
            historical_rates = []

            # Try to get current rate as a reference
            current_rate = 0.0
            try:
                funding_info = await self.api_client._request("GET", "/api/v1/fundingInfo")

                for fund_item in funding_info:
                    if fund_item.get("symbol") == symbol:
                        funding_rate_raw = float(fund_item.get("fundingRate", "0"))
                        # Convert to annualized percentage
                        current_rate = funding_rate_raw * 3 * 365 * 100
                        break
            except Exception as e:
                logger.warning(f"Error getting current funding rate for {symbol}: {e}")

            # Generate historical rates (in a real implementation, fetch from database)
            # Backpack has 8-hour funding intervals
            interval_ms = 8 * 60 * 60 * 1000

            # Calculate how many intervals to generate
            intervals = (lookback_hours + 7) // 8  # Round up to cover lookback_hours

            for interval in range(intervals, 0, -1):
                timestamp = current_time - (interval * interval_ms)

                # Add some randomness to simulate historical variation
                variation = random.uniform(-1.0, 1.0)  # Random variation
                historical_rate = max(0, current_rate + variation)

                historical_rates.append((timestamp, historical_rate))

            return historical_rates

        except Exception as e:
            logger.error(f"Error getting historical funding rates for {symbol}: {e}")
            return []

    async def get_market_price(self, symbol: str) -> float:
        """Get current market price for an asset."""
        try:
            ticker_response = await self.api_client.get_ticker(symbol)
            return ticker_response.price
        except Exception as e:
            logger.error(f"Error getting market price for {symbol}: {e}")
            return 0.0

    async def get_position(self, symbol: str) -> Position | None:
        """Get current position for an asset."""
        try:
            positions = await self.api_client.get_positions()

            if symbol in positions:
                position = positions[symbol]

                return Position(
                    symbol=symbol,
                    exchange=self.exchange_id,
                    size=position.size,
                    entry_price=position.entry_price,
                    leverage=position.leverage,
                    liquidation_price=position.liquidation_price,
                    unrealized_pnl=position.unrealized_pnl,
                )

            return None

        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return None

    async def open_position(self, symbol: str, side: PositionSide, size: float) -> bool:
        """Open a new position."""
        try:
            # Convert side to OrderSide
            from ..apis.base import OrderSide

            order_side = OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL

            # Place a market order
            from ..apis.base import OrderType

            response = await self.api_client.place_order(
                symbol=symbol,
                side=order_side,
                order_type=OrderType.MARKET,
                quantity=size,
            )

            logger.info(f"Opened position for {symbol} on Backpack: {side.value}, size: {size}")
            return True

        except Exception as e:
            logger.error(f"Error opening position for {symbol} on Backpack: {e}")
            return False

    async def close_position(self, symbol: str) -> bool:
        """Close an existing position."""
        try:
            # Get current position
            position = await self.get_position(symbol)

            if not position or position.size <= 0:
                logger.warning(f"No position found for {symbol}")
                return False

            # Determine close side (opposite of current position)
            from ..apis.base import OrderSide

            close_side = OrderSide.SELL if position.entry_price > 0 else OrderSide.BUY

            # In a real implementation, this would place an actual order
            # For demonstration, we'll just simulate a successful close

            logger.info(f"Simulated closing position of {position.size} {symbol} on Backpack")
            return True

        except Exception as e:
            logger.error(f"Error closing position on Backpack: {e}")
            return False

    async def _request(
        self, method: str, endpoint: str, params: dict = None, signed: bool = False
    ) -> dict:
        """Make an API request to Backpack"""
        # Placeholder for actual HTTP request implementation
        # In production: implement proper HTTP requests with authentication

        # Generate simulated responses based on the endpoint
        if endpoint == "/api/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BTC-PERP",
                        "tickSize": "0.1",
                        "minQty": "0.001",
                        "maxLeverage": "20",
                    },
                    {
                        "symbol": "ETH-PERP",
                        "tickSize": "0.01",
                        "minQty": "0.01",
                        "maxLeverage": "20",
                    },
                    {
                        "symbol": "SOL-PERP",
                        "tickSize": "0.001",
                        "minQty": "0.1",
                        "maxLeverage": "20",
                    },
                ]
            }
        elif endpoint == "/api/v1/fundingInfo":
            return [
                {
                    "symbol": "BTC-PERP",
                    "fundingRate": "0.0010",
                    "rateVolatility": "0.0002",
                },
                {
                    "symbol": "ETH-PERP",
                    "fundingRate": "0.0012",
                    "rateVolatility": "0.0003",
                },
                {
                    "symbol": "SOL-PERP",
                    "fundingRate": "0.0020",
                    "rateVolatility": "0.0005",
                },
            ]
        elif "/api/v1/ticker/price" in endpoint:
            symbol = params.get("symbol") if params else "BTC-PERP"
            prices = {"BTC-PERP": "50000", "ETH-PERP": "3000", "SOL-PERP": "100"}
            return {"symbol": symbol, "price": prices.get(symbol, "0")}
        elif endpoint == "/api/v1/positions" and signed:
            return [
                {
                    "symbol": "BTC-PERP",
                    "positionAmt": "0.5",
                    "entryPrice": "49000",
                    "markPrice": "50000",
                    "unrealizedProfit": "500",
                    "liquidationPrice": "40000",
                }
            ]

        return {}  # Default empty response
