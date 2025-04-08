#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Funding Rate Arbitrage Implementation
CyberDeltaEngine Project

This module implements the core algorithms for funding rate arbitrage strategies
with a particular focus on Hyperliquid's unique hourly funding payment structure.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging
import time
from datetime import datetime, timedelta
import asyncio
import json
import hmac
import hashlib
import base64
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Data Structures
# -------------------------------------------------------------------------

class ExchangeId(Enum):
    """Supported exchanges"""
    HYPERLIQUID = "hyperliquid"
    BACKPACK = "backpack"
    PARADEX = "paradex"

class PositionSide(Enum):
    """Position side: long or short"""
    LONG = "long"
    SHORT = "short"

@dataclass
class AssetInfo:
    """Information about an asset"""
    symbol: str
    exchange: ExchangeId
    tick_size: float
    min_order_size: float
    max_leverage: float
    funding_interval: timedelta  # e.g., 1 hour for Hyperliquid, 8 hours for others

@dataclass
class FundingRateInfo:
    """Funding rate information for an asset"""
    symbol: str
    exchange: ExchangeId
    current_rate: float  # Annualized percentage
    predicted_next_rate: float
    timestamp: datetime
    next_payment_time: datetime
    historical_volatility: float

@dataclass
class ArbitrageOpportunity:
    """Identified arbitrage opportunity"""
    asset: AssetInfo
    funding_rate: float
    expected_return: float
    optimal_size: float
    side: PositionSide
    confidence: float
    timestamp: datetime
    
@dataclass
class Position:
    """Open position"""
    symbol: str
    exchange: ExchangeId
    side: PositionSide
    size: float
    entry_price: float
    entry_time: datetime
    expected_funding_rate: float
    collected_funding: float = 0.0
    current_pnl: float = 0.0
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

# -------------------------------------------------------------------------
# Exchange Adapter Base
# -------------------------------------------------------------------------

class ExchangeAdapter:
    """Base class for exchange adapters"""
    
    def __init__(self, api_key: str, api_secret: str, test_mode: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.test_mode = test_mode
        self.assets: Dict[str, AssetInfo] = {}
        
    async def initialize(self):
        """Initialize the exchange adapter"""
        raise NotImplementedError
        
    async def get_funding_rates(self) -> Dict[str, FundingRateInfo]:
        """Get current funding rates for all assets"""
        raise NotImplementedError
        
    async def get_market_price(self, symbol: str) -> float:
        """Get current market price for an asset"""
        raise NotImplementedError
        
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get current position for an asset"""
        raise NotImplementedError
        
    async def open_position(
        self, 
        symbol: str, 
        side: PositionSide, 
        size: float, 
        price: Optional[float] = None
    ) -> bool:
        """Open a new position"""
        raise NotImplementedError
        
    async def close_position(self, symbol: str) -> bool:
        """Close an existing position"""
        raise NotImplementedError
        
    async def get_historical_funding_rates(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> pd.DataFrame:
        """Get historical funding rates"""
        raise NotImplementedError

# -------------------------------------------------------------------------
# Hyperliquid Exchange Adapter
# -------------------------------------------------------------------------

class HyperliquidAdapter(ExchangeAdapter):
    """Adapter for Hyperliquid exchange"""
    
    def __init__(self, api_key: str, api_secret: str, test_mode: bool = False):
        super().__init__(api_key, api_secret, test_mode)
        self.base_url = "https://api.hyperliquid.xyz"
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        
    async def initialize(self):
        """Initialize Hyperliquid adapter"""
        logger.info("Initializing Hyperliquid adapter")
        
        # Get all tradable assets and their specifications
        assets_data = await self._request("GET", "/info")
        
        for asset in assets_data["assets"]:
            symbol = asset["symbol"]
            self.assets[symbol] = AssetInfo(
                symbol=symbol,
                exchange=ExchangeId.HYPERLIQUID,
                tick_size=float(asset["tickSize"]),
                min_order_size=float(asset["minOrderSize"]),
                max_leverage=float(asset["maxLeverage"]),
                funding_interval=timedelta(hours=1)  # Hyperliquid has hourly funding
            )
        
        logger.info(f"Initialized with {len(self.assets)} assets on Hyperliquid")
        
    async def get_funding_rates(self) -> Dict[str, FundingRateInfo]:
        """Get current funding rates for all assets on Hyperliquid"""
        funding_data = await self._request("GET", "/funding")
        result = {}
        
        for item in funding_data:
            symbol = item["symbol"]
            if symbol in self.assets:
                # Hyperliquid provides funding rate as a percentage (not annualized)
                # Convert to annualized percentage
                hourly_rate = float(item["fundingRate"])
                annualized_rate = hourly_rate * 24 * 365
                
                next_payment = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
                if next_payment < datetime.utcnow():
                    next_payment += timedelta(hours=1)
                
                result[symbol] = FundingRateInfo(
                    symbol=symbol,
                    exchange=ExchangeId.HYPERLIQUID,
                    current_rate=annualized_rate,
                    predicted_next_rate=annualized_rate,  # Simple prediction: same as current
                    timestamp=datetime.utcnow(),
                    next_payment_time=next_payment,
                    historical_volatility=float(item.get("rateVolatility", 0.01))
                )
        
        return result
    
    async def get_historical_funding_rates(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> pd.DataFrame:
        """Get historical funding rates from Hyperliquid's AWS S3 bucket"""
        # Construct the URL for Hyperliquid's historical data (example)
        base_url = "https://hyperliquid-historical-data.s3.amazonaws.com"
        
        # Create date range for hourly intervals
        dates = pd.date_range(start=start_time, end=end_time, freq='H')
        
        # Placeholder for results
        results = []
        
        # For demonstration purposes - in production would use boto3/requests to fetch from S3
        for date in dates:
            date_str = date.strftime('%Y-%m-%d')
            hour_str = date.strftime('%H')
            
            # Construct simulated data based on documented patterns
            # In production: fetch actual data from S3
            simulated_rate = 0.01 * np.sin(date.hour / 12 * np.pi) + 0.005 * np.random.randn()
            
            results.append({
                'timestamp': date,
                'symbol': symbol,
                'funding_rate': simulated_rate,
                'funding_rate_annualized': simulated_rate * 24 * 365
            })
        
        return pd.DataFrame(results)
    
    async def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make an API request to Hyperliquid"""
        # Placeholder for actual HTTP request implementation
        # In production: implement proper HTTP requests with authentication
        
        # Simulated response for demonstration
        if endpoint == "/info":
            return {
                "assets": [
                    {"symbol": "BTC-PERP", "tickSize": "0.1", "minOrderSize": "0.001", "maxLeverage": "20"},
                    {"symbol": "ETH-PERP", "tickSize": "0.01", "minOrderSize": "0.01", "maxLeverage": "20"},
                    {"symbol": "SOL-PERP", "tickSize": "0.001", "minOrderSize": "0.1", "maxLeverage": "20"}
                ]
            }
        elif endpoint == "/funding":
            return [
                {"symbol": "BTC-PERP", "fundingRate": "0.0012", "rateVolatility": "0.0003"},
                {"symbol": "ETH-PERP", "fundingRate": "0.0015", "rateVolatility": "0.0004"},
                {"symbol": "SOL-PERP", "fundingRate": "0.0025", "rateVolatility": "0.0008"}
            ]
        
        return {}  # Default empty response
        
# -------------------------------------------------------------------------
# Backpack Exchange Adapter
# -------------------------------------------------------------------------

class BackpackAdapter(ExchangeAdapter):
    """Adapter for Backpack exchange"""
    
    def __init__(self, api_key: str, api_secret: str, test_mode: bool = False):
        super().__init__(api_key, api_secret, test_mode)
        self.base_url = "https://api.backpack.exchange"
        self.ws_url = "wss://ws.backpack.exchange"
        
    async def initialize(self):
        """Initialize Backpack adapter"""
        logger.info("Initializing Backpack adapter")
        
        # Get all tradable assets and their specifications
        assets_data = await self._request("GET", "/api/v1/exchangeInfo")
        
        for symbol_info in assets_data.get("symbols", []):
            symbol = symbol_info["symbol"]
            if symbol.endswith("-PERP"):  # Only consider perpetuals
                self.assets[symbol] = AssetInfo(
                    symbol=symbol,
                    exchange=ExchangeId.BACKPACK,
                    tick_size=float(symbol_info.get("tickSize", "0.01")),
                    min_order_size=float(symbol_info.get("minQty", "0.001")),
                    max_leverage=float(symbol_info.get("maxLeverage", "10")),  # Default if not specified
                    funding_interval=timedelta(hours=8)  # Backpack typically has 8-hour funding intervals
                )
        
        logger.info(f"Initialized with {len(self.assets)} assets on Backpack")
        
    async def get_funding_rates(self) -> Dict[str, FundingRateInfo]:
        """Get current funding rates for all assets on Backpack"""
        funding_data = await self._request("GET", "/api/v1/fundingInfo")
        result = {}
        
        for item in funding_data:
            symbol = item.get("symbol")
            if symbol in self.assets:
                # Convert funding rate to annualized percentage
                funding_rate = float(item.get("fundingRate", "0"))
                # Backpack typically has 8-hour funding intervals, so multiply by 3 per day * 365 days
                annualized_rate = funding_rate * 3 * 365
                
                # Calculate next funding time
                current_time = datetime.utcnow()
                hours_until_funding = (8 - (current_time.hour % 8)) % 8
                next_payment = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=hours_until_funding)
                
                result[symbol] = FundingRateInfo(
                    symbol=symbol,
                    exchange=ExchangeId.BACKPACK,
                    current_rate=annualized_rate,
                    predicted_next_rate=annualized_rate,  # Simple prediction: same as current
                    timestamp=current_time,
                    next_payment_time=next_payment,
                    historical_volatility=float(item.get("rateVolatility", 0.005))  # Default if not provided
                )
        
        return result
    
    async def get_historical_funding_rates(
        self, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> pd.DataFrame:
        """Get historical funding rates from Backpack"""
        # Backpack API endpoint for historical funding rates
        endpoint = f"/api/v1/fundingHistory"
        
        # Convert times to milliseconds timestamp
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        
        params = {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 500  # Maximum records to fetch
        }
        
        try:
            # In a real implementation, this would make actual API requests
            # For demonstration, we'll generate simulated data
            
            # Create date range for 8-hour intervals
            dates = pd.date_range(start=start_time, end=end_time, freq='8H')
            
            # Placeholder for results
            results = []
            
            # Generate simulated funding rate data
            for date in dates:
                # Simulate funding rates with some randomness and daily pattern
                hour = date.hour
                day_factor = 1.0 if hour <= 8 else (0.8 if hour <= 16 else 1.2)
                simulated_rate = 0.008 * day_factor * (1 + 0.3 * np.random.randn())
                
                results.append({
                    'timestamp': date,
                    'symbol': symbol,
                    'funding_rate': simulated_rate,
                    'funding_rate_annualized': simulated_rate * 3 * 365  # 8-hour rate to annualized
                })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"Error fetching historical funding rates from Backpack: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    async def get_market_price(self, symbol: str) -> float:
        """Get current market price for an asset on Backpack"""
        try:
            ticker_data = await self._request("GET", f"/api/v1/ticker/price?symbol={symbol}")
            return float(ticker_data.get("price", 0))
        except Exception as e:
            logger.error(f"Error fetching market price from Backpack: {e}")
            return 0.0
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get current position for an asset on Backpack"""
        try:
            positions_data = await self._request("GET", "/api/v1/positions", signed=True)
            
            for position in positions_data:
                if position.get("symbol") == symbol:
                    return Position(
                        symbol=symbol,
                        exchange=ExchangeId.BACKPACK,
                        size=float(position.get("positionAmt", 0)),
                        entry_price=float(position.get("entryPrice", 0)),
                        mark_price=float(position.get("markPrice", 0)),
                        pnl=float(position.get("unrealizedProfit", 0)),
                        liquidation_price=float(position.get("liquidationPrice", 0)),
                        side=PositionSide.LONG if float(position.get("positionAmt", 0)) > 0 else PositionSide.SHORT
                    )
            
            return None  # No position found for this symbol
        except Exception as e:
            logger.error(f"Error fetching position from Backpack: {e}")
            return None
    
    async def open_position(
        self, 
        symbol: str, 
        side: PositionSide, 
        size: float, 
        price: Optional[float] = None
    ) -> bool:
        """Open a new position on Backpack"""
        try:
            order_side = "BUY" if side == PositionSide.LONG else "SELL"
            order_type = "MARKET" if price is None else "LIMIT"
            
            order_params = {
                "symbol": symbol,
                "side": order_side,
                "type": order_type,
                "quantity": str(size),
            }
            
            if price is not None:
                order_params["price"] = str(price)
            
            # In a real implementation, this would place an actual order
            # For demonstration, we'll just simulate a successful order
            
            logger.info(f"Simulated opening {side.name} position of {size} {symbol} on Backpack")
            return True
            
        except Exception as e:
            logger.error(f"Error opening position on Backpack: {e}")
            return False
    
    async def close_position(self, symbol: str) -> bool:
        """Close an existing position on Backpack"""
        try:
            # First get the current position to determine the closing order details
            position = await self.get_position(symbol)
            
            if position is None or position.size == 0:
                logger.info(f"No position to close for {symbol} on Backpack")
                return True  # No position to close is considered success
            
            # Determine the closing order side (opposite of position side)
            close_side = "SELL" if position.side == PositionSide.LONG else "BUY"
            
            # Close using a market order for the full position size
            close_params = {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": str(abs(position.size)),
                "reduceOnly": "true"  # Ensure this only reduces the position
            }
            
            # In a real implementation, this would place an actual order
            # For demonstration, we'll just simulate a successful close
            
            logger.info(f"Simulated closing position of {abs(position.size)} {symbol} on Backpack")
            return True
            
        except Exception as e:
            logger.error(f"Error closing position on Backpack: {e}")
            return False
    
    async def _request(self, method: str, endpoint: str, params: dict = None, signed: bool = False) -> dict:
        """Make an API request to Backpack"""
        # Placeholder for actual HTTP request implementation
        # In production: implement proper HTTP requests with authentication
        
        # Generate simulated responses based on the endpoint
        if endpoint == "/api/v1/exchangeInfo":
            return {
                "symbols": [
                    {"symbol": "BTC-PERP", "tickSize": "0.1", "minQty": "0.001", "maxLeverage": "20"},
                    {"symbol": "ETH-PERP", "tickSize": "0.01", "minQty": "0.01", "maxLeverage": "20"},
                    {"symbol": "SOL-PERP", "tickSize": "0.001", "minQty": "0.1", "maxLeverage": "20"}
                ]
            }
        elif endpoint == "/api/v1/fundingInfo":
            return [
                {"symbol": "BTC-PERP", "fundingRate": "0.0010", "rateVolatility": "0.0002"},
                {"symbol": "ETH-PERP", "fundingRate": "0.0012", "rateVolatility": "0.0003"},
                {"symbol": "SOL-PERP", "fundingRate": "0.0020", "rateVolatility": "0.0005"}
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
        
# -------------------------------------------------------------------------
# Strategy Implementation
# -------------------------------------------------------------------------

class FundingRateArbitrageStrategy:
    """
    Implementation of funding rate arbitrage strategy
    """
    
    def __init__(
        self,
        exchanges: Dict[ExchangeId, ExchangeAdapter],
        config: dict
    ):
        self.exchanges = exchanges
        self.config = config
        self.positions: Dict[str, Position] = {}  # Currently open positions
        self.opportunities: List[ArbitrageOpportunity] = []  # Current opportunities
        
        # Strategy parameters
        self.min_funding_rate = config.get("min_funding_rate", 5.0)  # Min annualized funding rate (%)
        self.max_position_size = config.get("max_position_size", 1000)  # USD
        self.kelly_fraction = config.get("kelly_fraction", 0.3)  # Fraction of Kelly criterion to use
        self.max_positions = config.get("max_positions", 5)  # Maximum number of simultaneous positions
        self.stop_loss_pct = config.get("stop_loss_pct", 0.02)  # 2% stop loss
        
    async def initialize(self):
        """Initialize the strategy"""
        logger.info("Initializing funding rate arbitrage strategy")
        
        # Initialize all exchange adapters
        for exchange in self.exchanges.values():
            await exchange.initialize()
            
        logger.info("Strategy initialization complete")
        
    async def update_opportunities(self):
        """Update the list of arbitrage opportunities"""
        logger.info("Updating arbitrage opportunities")
        
        all_opportunities = []
        
        # Get funding rates from all exchanges
        funding_rates = {}
        for exchange_id, adapter in self.exchanges.items():
            exchange_rates = await adapter.get_funding_rates()
            funding_rates[exchange_id] = exchange_rates
            
        # Single exchange opportunities - focus on Hyperliquid
        hyperliquid_rates = funding_rates.get(ExchangeId.HYPERLIQUID, {})
        
        for symbol, rate_info in hyperliquid_rates.items():
            # If funding rate is significantly positive, we want to go long
            # If significantly negative, we want to go short
            if abs(rate_info.current_rate) >= self.min_funding_rate:
                side = PositionSide.LONG if rate_info.current_rate > 0 else PositionSide.SHORT
                
                # Calculate expected return from this opportunity
                # Annualized funding rate divided by 8760 (hours in a year) = hourly return
                expected_hourly_return = rate_info.current_rate / 8760
                
                # Calculate optimal position size using Kelly criterion
                # f* = (p*b - q) / b where p=win probability, q=lose probability, b=odds ratio
                # Simplified for funding: f* = expected_return / variance
                # Using historical volatility as proxy for variance
                kelly_position = expected_hourly_return / (rate_info.historical_volatility ** 2)
                
                # Apply Kelly fraction and cap maximum position size
                optimal_size = min(
                    self.max_position_size,
                    kelly_position * self.kelly_fraction * self.max_position_size
                )
                
                # Create opportunity
                opportunity = ArbitrageOpportunity(
                    asset=self.exchanges[ExchangeId.HYPERLIQUID].assets[symbol],
                    funding_rate=rate_info.current_rate,
                    expected_return=expected_hourly_return,
                    optimal_size=optimal_size,
                    side=side,
                    confidence=0.8,  # Confidence level - could be calculated from historical data
                    timestamp=datetime.utcnow()
                )
                
                all_opportunities.append(opportunity)
                
        # Sort opportunities by expected return
        all_opportunities.sort(key=lambda x: x.expected_return, reverse=True)
        
        # Update the opportunities list
        self.opportunities = all_opportunities
        
        logger.info(f"Found {len(self.opportunities)} arbitrage opportunities")
        
    async def execute(self):
        """Execute the arbitrage strategy"""
        logger.info("Executing arbitrage strategy")
        
        # Update opportunities
        await self.update_opportunities()
        
        # Get currently open positions
        open_positions = set(self.positions.keys())
        
        # Check if we need to close any positions
        for symbol in list(self.positions.keys()):
            position = self.positions[symbol]
            
            # Get current funding rate
            exchange = self.exchanges[position.exchange]
            funding_rates = await exchange.get_funding_rates()
            
            if symbol in funding_rates:
                current_rate = funding_rates[symbol].current_rate
                
                # Close position if funding rate has changed sign (crossed zero)
                # or if it's no longer significant
                if (position.side == PositionSide.LONG and current_rate <= 0) or \
                   (position.side == PositionSide.SHORT and current_rate >= 0) or \
                   abs(current_rate) < self.min_funding_rate * 0.5:  # Half of min threshold
                    
                    logger.info(f"Closing position for {symbol} due to funding rate change")
                    success = await exchange.close_position(symbol)
                    
                    if success:
                        del self.positions[symbol]
                        open_positions.remove(symbol)
        
        # Open new positions if we have capacity
        available_slots = self.max_positions - len(open_positions)
        
        if available_slots > 0:
            # Get top opportunities that we don't already have positions in
            for opportunity in self.opportunities:
                if opportunity.asset.symbol not in open_positions and available_slots > 0:
                    logger.info(f"Opening new position for {opportunity.asset.symbol}")
                    
                    exchange = self.exchanges[opportunity.asset.exchange]
                    
                    # Get current market price
                    price = await exchange.get_market_price(opportunity.asset.symbol)
                    
                    # Open position
                    success = await exchange.open_position(
                        symbol=opportunity.asset.symbol,
                        side=opportunity.side,
                        size=opportunity.optimal_size
                    )
                    
                    if success:
                        # Create position object
                        position = Position(
                            symbol=opportunity.asset.symbol,
                            exchange=opportunity.asset.exchange,
                            side=opportunity.side,
                            size=opportunity.optimal_size,
                            entry_price=price,
                            entry_time=datetime.utcnow(),
                            expected_funding_rate=opportunity.funding_rate,
                            stop_loss_price=price * (1 - self.stop_loss_pct if opportunity.side == PositionSide.LONG else 1 + self.stop_loss_pct)
                        )
                        
                        # Add to positions
                        self.positions[opportunity.asset.symbol] = position
                        available_slots -= 1
                    
                    if available_slots <= 0:
                        break
        
    async def monitor_positions(self):
        """Monitor open positions and collect funding payments"""
        logger.info("Monitoring positions")
        
        for symbol, position in list(self.positions.items()):
            exchange = self.exchanges[position.exchange]
            
            # Get current position details
            current_position = await exchange.get_position(symbol)
            
            if current_position is None:
                # Position no longer exists
                logger.warning(f"Position for {symbol} no longer exists - may have been stopped out")
                del self.positions[symbol]
                continue
            
            # Update collected funding
            position.collected_funding = current_position.collected_funding
            
            # Update current P&L
            position.current_pnl = current_position.current_pnl
            
            # Check stop loss
            if position.stop_loss_price is not None:
                current_price = await exchange.get_market_price(symbol)
                
                if (position.side == PositionSide.LONG and current_price <= position.stop_loss_price) or \
                   (position.side == PositionSide.SHORT and current_price >= position.stop_loss_price):
                    
                    logger.warning(f"Stop loss triggered for {symbol}")
                    success = await exchange.close_position(symbol)
                    
                    if success:
                        del self.positions[symbol]
                        
    async def run(self):
        """Run the strategy"""
        logger.info("Starting funding rate arbitrage strategy")
        
        await self.initialize()
        
        while True:
            try:
                # Execute strategy
                await self.execute()
                
                # Monitor positions
                await self.monitor_positions()
                
                # Wait for next cycle
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in strategy execution: {e}", exc_info=True)
                await asyncio.sleep(10)  # Wait before retrying

# -------------------------------------------------------------------------
# Risk Management
# -------------------------------------------------------------------------

class RiskManager:
    """Risk management for funding rate arbitrage"""
    
    def __init__(self, config: dict):
        self.config = config
        self.position_limits = config.get("position_limits", {})
        self.max_drawdown = config.get("max_drawdown", 0.1)  # 10% max drawdown
        self.initial_capital = config.get("initial_capital", 10000)
        self.current_capital = self.initial_capital
        
    def check_position_size(self, symbol: str, exchange: ExchangeId, size: float) -> float:
        """Check and possibly adjust position size based on risk limits"""
        # Get symbol-specific limit if exists, otherwise use default
        max_size = self.position_limits.get(
            symbol, 
            self.position_limits.get(
                exchange.value, 
                self.position_limits.get("default", float('inf'))
            )
        )
        
        # Adjust size if necessary
        return min(size, max_size)
    
    def update_capital(self, new_capital: float):
        """Update current capital"""
        self.current_capital = new_capital
        
        # Check if drawdown limit exceeded
        drawdown = 1 - (self.current_capital / self.initial_capital)
        
        if drawdown > self.max_drawdown:
            logger.warning(f"Maximum drawdown exceeded: {drawdown:.2%}")
            return False
        
        return True
    
    def get_position_limits(self) -> dict:
        """Get all position limits"""
        return self.position_limits
    
    def get_drawdown(self) -> float:
        """Get current drawdown percentage"""
        return 1 - (self.current_capital / self.initial_capital)

# -------------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------------

async def main():
    """Main entry point"""
    logger.info("Starting CyberDeltaEngine Funding Rate Arbitrage")
    
    # Configuration
    config = {
        "min_funding_rate": 10.0,  # 10% annualized funding rate threshold
        "max_position_size": 1000,  # $1000 max position size
        "kelly_fraction": 0.3,     # 30% of Kelly criterion
        "max_positions": 5,        # Maximum 5 positions at once
        "stop_loss_pct": 0.02,     # 2% stop loss
        "api_keys": {
            "hyperliquid": {
                "api_key": "YOUR_API_KEY",
                "api_secret": "YOUR_API_SECRET"
            }
        }
    }
    
    # Risk management configuration
    risk_config = {
        "position_limits": {
            "default": 1000,
            "hyperliquid": 2000,
            "BTC-PERP": 500
        },
        "max_drawdown": 0.1,
        "initial_capital": 10000
    }
    
    # Create risk manager
    risk_manager = RiskManager(risk_config)
    
    # Create exchange adapters
    exchanges = {
        ExchangeId.HYPERLIQUID: HyperliquidAdapter(
            api_key=config["api_keys"]["hyperliquid"]["api_key"],
            api_secret=config["api_keys"]["hyperliquid"]["api_secret"]
        ),
        ExchangeId.BACKPACK: BackpackAdapter(
            api_key=config["api_keys"]["backpack"]["api_key"],
            api_secret=config["api_keys"]["backpack"]["api_secret"]
        )
    }
    
    # Create strategy
    strategy = FundingRateArbitrageStrategy(exchanges, config)
    
    # Run strategy
    await strategy.run()

if __name__ == "__main__":
    asyncio.run(main()) 