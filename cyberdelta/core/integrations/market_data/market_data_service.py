"""Production market data service for historical prices and volatility calculations."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional
import numpy as np

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.config.risk_parameters import MarketDataProvider
from cyberdelta.core.symbols import Symbol

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.protocols import CacheServiceProtocol


class RealMarketDataService(MarketDataProvider):
    """Production market data service with real exchange integration.
    
    Provides:
    - Historical price data from exchanges
    - Volatility calculations based on actual price movements
    - Correlation matrix computation
    - Expected returns based on strategy and market conditions
    - Daily volume data
    """
    
    def __init__(
        self,
        app_settings: AppSettings,
        cache_service: CacheServiceProtocol[str, Any] | None = None,
        api_clients: dict[str, Any] | None = None,
    ) -> None:
        """Initialize market data service."""
        self.app_settings = app_settings
        self.cache_service = cache_service
        self.api_clients = api_clients or {}
        self.logger = get_logger(self.__class__.__name__)
        
        # Configuration
        self.volatility_lookback_days = 30  # Days to look back for volatility calc
        self.correlation_lookback_days = 60  # Days for correlation matrix
        self.cache_ttl = 3600  # 1 hour cache for market data
        
        # Internal caches
        self._price_history_cache: dict[str, list[dict[str, Any]]] = {}
        self._last_cache_update: dict[str, datetime] = {}
    
    async def get_symbol_volatility(self, symbol: Symbol) -> Decimal:
        """Get real volatility from market data."""
        try:
            # Get historical prices - use Symbol value for string operations
            price_history = await self._get_price_history(
                symbol, 
                self.volatility_lookback_days
            )
            
            if not price_history or len(price_history) < 2:
                # Fallback to default
                return await self._get_default_volatility(symbol)
            
            # Calculate returns
            prices = [float(p["close"]) for p in price_history]
            returns = np.diff(np.log(prices))
            
            # Calculate annualized volatility
            daily_vol = np.std(returns)
            annualized_vol = daily_vol * np.sqrt(252)  # Trading days per year
            
            return Decimal(str(daily_vol))  # Return daily volatility
            
        except Exception as e:
            self.logger.error(
                "volatility_calculation_failed",
                symbol=symbol,
                error=str(e)
            )
            return await self._get_default_volatility(symbol)
    
    async def get_daily_volume(self, symbol: Symbol) -> Decimal:
        """Get real daily volume from market data."""
        try:
            # Try to get volume from exchanges using generic approach
            for exchange_id, api_client in self.api_clients.items():
                try:
                    volume = await self._get_generic_volume(api_client, symbol, exchange_id)
                    if volume > 0:
                        return volume
                        
                except Exception:
                    continue
            
            # Fallback to estimates
            return await self._estimate_daily_volume(symbol)
            
        except Exception as e:
            self.logger.error(
                "volume_fetch_failed",
                symbol=symbol,
                error=str(e)
            )
            return await self._estimate_daily_volume(symbol)
    
    async def get_correlation_matrix(self, symbols: list[Symbol]) -> Dict[str, Dict[str, Decimal]]:
        """Get real correlation matrix from market data."""
        try:
            # Get price histories for all symbols
            price_histories = {}
            min_length = float('inf')
            
            for symbol in symbols:
                history = await self._get_price_history(
                    symbol,
                    self.correlation_lookback_days
                )
                if history and len(history) > 2:
                    price_histories[symbol.value] = history
                    min_length = min(min_length, len(history))
            
            if len(price_histories) < 2 or min_length < 10:
                # Not enough data for correlation
                return self._get_default_correlation_matrix(symbols)
            
            # Align price histories and calculate returns
            returns_data = {}
            for symbol_key, history in price_histories.items():
                # Use only the most recent min_length prices
                prices = [float(p["close"]) for p in history[-int(min_length):]]
                returns = np.diff(np.log(prices))
                returns_data[symbol_key] = returns
            
            # Calculate correlation matrix
            correlation_matrix: dict[str, dict[str, Decimal]] = {}
            symbols_with_data = list(returns_data.keys())
            
            for i, symbol1 in enumerate(symbols_with_data):
                correlation_matrix[symbol1] = {}
                for j, symbol2 in enumerate(symbols_with_data):
                    if i == j:
                        correlation_matrix[symbol1][symbol2] = Decimal("1.0")
                    else:
                        corr = np.corrcoef(
                            returns_data[symbol1],
                            returns_data[symbol2]
                        )[0, 1]
                        correlation_matrix[symbol1][symbol2] = Decimal(str(corr))
            
            # Fill in missing symbols with defaults
            for symbol in symbols:
                symbol_key = symbol.value
                if symbol_key not in correlation_matrix:
                    correlation_matrix[symbol_key] = self._get_default_correlations(symbol, symbols)
            
            return correlation_matrix
            
        except Exception as e:
            self.logger.error(
                "correlation_calculation_failed",
                symbols=symbols,
                error=str(e)
            )
            return self._get_default_correlation_matrix(symbols)
    
    async def get_expected_return(self, symbol: Symbol, strategy: str) -> Decimal:
        """Get expected return based on strategy and market conditions."""
        try:
            # Get recent price momentum
            price_history = await self._get_price_history(symbol, 30)
            
            if not price_history or len(price_history) < 2:
                return self._get_default_expected_return(strategy)
            
            # Calculate momentum metrics
            prices = [float(p["close"]) for p in price_history]
            
            # 30-day return
            month_return = (prices[-1] - prices[0]) / prices[0]
            
            # Recent volatility
            recent_returns = np.diff(np.log(prices[-10:]))  # Last 10 days
            recent_vol = np.std(recent_returns)
            
            # Base expected return on strategy
            if strategy == "momentum":
                # Momentum strategies expect continuation
                if month_return > 0:
                    expected_return = Decimal(str(abs(month_return) * 0.3))  # 30% of recent return
                else:
                    expected_return = Decimal("0.01")  # Minimal expectation
            
            elif strategy == "arbitrage":
                # Arbitrage expects small consistent returns
                expected_return = Decimal("0.01")  # 1% expected
            
            elif strategy == "mean_reversion":
                # Mean reversion expects reversal
                if month_return > 0.2:  # Strong up move
                    expected_return = Decimal("0.02")  # Expect pullback
                elif month_return < -0.2:  # Strong down move
                    expected_return = Decimal("0.03")  # Expect bounce
                else:
                    expected_return = Decimal("0.015")
            
            else:
                # Default strategy
                expected_return = Decimal("0.02")
            
            # Adjust for volatility
            if recent_vol > 0.05:  # High volatility
                expected_return *= Decimal("1.2")  # Higher potential
            elif recent_vol < 0.01:  # Low volatility
                expected_return *= Decimal("0.8")  # Lower potential
            
            return expected_return
            
        except Exception as e:
            self.logger.error(
                "expected_return_calculation_failed",
                symbol=symbol,
                strategy=strategy,
                error=str(e)
            )
            return self._get_default_expected_return(strategy)
    
    async def _get_price_history(
        self, 
        symbol: Symbol, 
        days: int
    ) -> List[Dict[str, Any]]:
        """Get historical price data from exchanges."""
        # Check cache first
        cache_key = f"{symbol.value}:{days}"
        if cache_key in self._price_history_cache:
            last_update = self._last_cache_update.get(cache_key)
            if last_update and (datetime.now(UTC) - last_update).seconds < self.cache_ttl:
                return self._price_history_cache[cache_key]
        
        # Fetch from exchanges
        for exchange_id, api_client in self.api_clients.items():
            try:
                if hasattr(api_client, 'get_historical_prices'):
                    # Exchange supports historical data
                    end_time = datetime.now(UTC)
                    start_time = end_time - timedelta(days=days)
                    
                    history = await api_client.get_historical_prices(
                        symbol.value,
                        interval="1d",
                        start_time=start_time,
                        end_time=end_time
                    )
                    
                    if history and len(history) > 0:
                        # Cache the result
                        typed_history: list[dict[str, Any]] = history
                        self._price_history_cache[cache_key] = typed_history
                        self._last_cache_update[cache_key] = datetime.now(UTC)
                        return typed_history
                
            except Exception as e:
                self.logger.warning(
                    "historical_price_fetch_failed",
                    exchange_id=exchange_id,
                    symbol=symbol,
                    error=str(e)
                )
                continue
        
        # No data available
        return []
    
    async def _get_generic_volume(
        self, 
        api_client: Any, 
        symbol: Symbol,
        exchange_id: str
    ) -> Decimal:
        """Get volume from any exchange using generic API interface."""
        # Try ticker first (most common approach)
        try:
            ticker = await api_client.get_ticker(symbol.value)
            
            # Try common volume fields in order of preference
            volume_candidates = [
                ticker.get("volume"),
                ticker.get("volume24h"),
                ticker.get("vol"),
                ticker.get("baseVolume"),
                ticker.get("quoteVolume")
            ]
            
            for volume in volume_candidates:
                if volume is not None and volume != 0:
                    return Decimal(str(volume))
                    
        except Exception as e:
            self.logger.debug(f"Ticker volume fetch failed for {symbol.value} on {exchange_id}: {e}")
        
        # Try market data endpoint as fallback
        try:
            if hasattr(api_client, 'get_market_data'):
                market_data = await api_client.get_market_data(symbol.value)
                volume_24h = market_data.get("volume24h") or market_data.get("volume")
                if volume_24h:
                    return Decimal(str(volume_24h))
        except Exception as e:
            self.logger.debug(f"Market data volume fetch failed for {symbol.value} on {exchange_id}: {e}")
        
        return Decimal("0")
    
    async def _get_default_volatility(self, symbol: Symbol) -> Decimal:
        """Get default volatility for symbol."""
        symbol_upper = symbol.value.upper()
        
        if "BTC" in symbol_upper:
            return Decimal("0.04")  # 4% daily
        elif "ETH" in symbol_upper:
            return Decimal("0.05")  # 5% daily
        elif any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI", "BUSD"]):
            return Decimal("0.001")  # 0.1% daily
        elif any(alt in symbol_upper for alt in ["SOL", "AVAX", "MATIC", "DOT"]):
            return Decimal("0.06")  # 6% daily
        else:
            return Decimal("0.08")  # 8% daily default
    
    async def _estimate_daily_volume(self, symbol: Symbol) -> Decimal:
        """Estimate daily volume for symbol."""
        symbol_upper = symbol.value.upper()
        
        if "BTC" in symbol_upper:
            return Decimal("1000000000")  # $1B
        elif "ETH" in symbol_upper:
            return Decimal("500000000")   # $500M
        elif any(stable in symbol_upper for stable in ["USDC", "USDT"]):
            return Decimal("5000000000")  # $5B
        elif any(alt in symbol_upper for alt in ["SOL", "AVAX", "MATIC"]):
            return Decimal("100000000")   # $100M
        else:
            return Decimal("10000000")    # $10M default
    
    def _get_default_correlation_matrix(
        self, 
        symbols: list[Symbol]
    ) -> Dict[str, Dict[str, Decimal]]:
        """Get default correlation matrix."""
        matrix: dict[str, dict[str, Decimal]] = {}
        
        for symbol1 in symbols:
            symbol1_key = symbol1.value
            matrix[symbol1_key] = {}
            for symbol2 in symbols:
                symbol2_key = symbol2.value
                if symbol1 == symbol2:
                    matrix[symbol1_key][symbol2_key] = Decimal("1.0")
                else:
                    # Default correlations based on asset class
                    corr = self._estimate_correlation(symbol1_key, symbol2_key)
                    matrix[symbol1_key][symbol2_key] = corr
        
        return matrix
    
    def _estimate_correlation(self, symbol1: str, symbol2: str) -> Decimal:
        """Estimate correlation between two symbols."""
        s1_upper = symbol1.upper()
        s2_upper = symbol2.upper()
        
        # Stablecoins have low correlation with crypto
        stables = ["USDC", "USDT", "DAI", "BUSD"]
        if any(s in s1_upper for s in stables) and any(s in s2_upper for s in stables):
            return Decimal("0.9")  # Stables highly correlated with each other
        elif any(s in s1_upper for s in stables) or any(s in s2_upper for s in stables):
            return Decimal("0.1")  # Low correlation between stable and crypto
        
        # Major cryptos are moderately correlated
        majors = ["BTC", "ETH"]
        if any(m in s1_upper for m in majors) and any(m in s2_upper for m in majors):
            return Decimal("0.7")  # BTC-ETH correlation
        
        # Altcoins correlate with majors
        if any(m in s1_upper + s2_upper for m in majors):
            return Decimal("0.6")
        
        # Default moderate correlation for other pairs
        return Decimal("0.5")
    
    def _get_default_correlations(
        self, 
        symbol: Symbol, 
        all_symbols: list[Symbol]
    ) -> Dict[str, Decimal]:
        """Get default correlations for a symbol."""
        correlations = {}
        for other_symbol in all_symbols:
            if symbol == other_symbol:
                correlations[other_symbol.value] = Decimal("1.0")
            else:
                correlations[other_symbol.value] = self._estimate_correlation(symbol.value, other_symbol.value)
        return correlations
    
    def _get_default_expected_return(self, strategy: str) -> Decimal:
        """Get default expected return for strategy."""
        strategy_returns = {
            "momentum": Decimal("0.03"),
            "arbitrage": Decimal("0.01"),
            "mean_reversion": Decimal("0.02"),
            "trend_following": Decimal("0.025"),
        }
        return strategy_returns.get(strategy, Decimal("0.02"))