# FastAPI Service Wrappers: Exposing Existing APIs and Trading Logic

> **🚨 CURRENT STATUS: PROPOSAL ONLY - NOT IMPLEMENTED**
>
> This document describes **proposed FastAPI service wrappers** that do not currently exist in the CyberDeltaEngine codebase.
>
> **Actual Current State:**
> - No FastAPI implementation exists
> - No service wrappers exist
> - No REST APIs exist
> - No service-oriented architecture - single monolithic main.py
> - No external API endpoints
> - CyberDeltaEngine runs as a single command-line process

## Overview (Updated June 2025)

This document details how to create FastAPI service wrappers around your existing CyberDelta components within the 8-9 week timeline. These services expose your production-ready trading infrastructure (including sophisticated features like auto-lending detection and margin support) through modern REST APIs while preserving all existing logic unchanged. The wrapper approach ensures zero risk to proven trading algorithms and supports the reduced 2-person team structure.

**⚠️ IMPLEMENTATION STATUS: This is a design proposal. No FastAPI services have been implemented.**

## Service Architecture

### Service Separation Strategy
```
FastAPI Services (New Wrappers)
├── Market Data Service (Port 8001)     # Wraps cyberdelta.apis.*
├── Trading Engine Service (Port 8002)  # Wraps cyberdelta.core.*
└── Configuration Service (Port 8003)   # Wraps cyberdelta.config.*

All services connect to:
├── Existing cyberdelta components (Unchanged)
├── Shared PostgreSQL database
└── Redis message broker
```

### Core Principle: Wrapper Pattern
Each FastAPI service is a thin wrapper that:
1. **Imports existing cyberdelta modules directly**
2. **Calls existing methods without modification**
3. **Adds REST API interface and documentation**
4. **Provides authentication and rate limiting**
5. **Publishes events via message broker**
6. **Preserves all sophisticated features** (auto-lending, margin calculations, etc.)
7. **Uses existing test suite** for validation

## FastAPI Market Data Service

### Purpose and Scope
- **Wrap existing API clients**: Production-ready `cyberdelta.apis.hyperliquid`, `cyberdelta.apis.backpack`
  - Preserves auto-lending detection and handling
  - Maintains margin/collateral calculations
  - Keeps sophisticated rate limiting (weight-based for Hyperliquid)
- **Expose market data endpoints**: Real-time and historical data
- **Provide WebSocket hub**: Distribute real-time data to multiple consumers
- **Add external rate limiting**: Protect public endpoints
- **Enable external integration**: Third-party tools and monitoring

### Project Structure
```
fastapi_market_data/
├── main.py                    # FastAPI application entry point
├── config.py                  # Service configuration
├── routers/
│   ├── __init__.py
│   ├── tickers.py            # Ticker data endpoints
│   ├── candles.py            # OHLCV data endpoints
│   ├── funding_rates.py      # Funding rate endpoints
│   ├── orderbook.py          # Order book endpoints
│   ├── trades.py             # Trade data endpoints
│   └── websocket.py          # WebSocket endpoints
├── adapters/                 # Wrappers around existing APIs
│   ├── __init__.py
│   ├── hyperliquid_adapter.py
│   ├── backpack_adapter.py
│   └── base_adapter.py
├── services/
│   ├── __init__.py
│   ├── market_data_service.py
│   ├── websocket_manager.py
│   └── cache_service.py
├── middleware/
│   ├── __init__.py
│   ├── rate_limiting.py
│   ├── authentication.py
│   └── cors.py
├── models/                   # Pydantic response models
│   ├── __init__.py
│   ├── ticker_models.py
│   ├── candle_models.py
│   └── error_models.py
└── requirements.txt
```

### Implementation Details

#### 1. Main FastAPI Application
```python
# fastapi_market_data/main.py
import sys
import os
from pathlib import Path

# Add existing cyberdelta to Python path
cyberdelta_path = Path(__file__).parent.parent / "cyberdelta"
sys.path.insert(0, str(cyberdelta_path))

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from routers import tickers, candles, funding_rates, websocket
from middleware.rate_limiting import RateLimitMiddleware
from middleware.authentication import AuthenticationMiddleware
from services.market_data_service import MarketDataService
from config import settings

# Create FastAPI app
app = FastAPI(
    title="CyberDelta Market Data API",
    description="High-performance market data API wrapping existing CyberDelta components",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(AuthenticationMiddleware)

# Include routers
app.include_router(tickers.router, prefix="/api/v1/tickers", tags=["tickers"])
app.include_router(candles.router, prefix="/api/v1/candles", tags=["candles"])
app.include_router(funding_rates.router, prefix="/api/v1/funding", tags=["funding"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])

# Global services
market_data_service = MarketDataService()

@app.on_event("startup")
async def startup_event():
    """Initialize services using existing cyberdelta components"""
    await market_data_service.start()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services"""
    await market_data_service.stop()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "market_data",
        "connections": market_data_service.get_connection_count(),
        "uptime": market_data_service.get_uptime_seconds()
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
```

#### 2. Hyperliquid Adapter
```python
# fastapi_market_data/adapters/hyperliquid_adapter.py
import sys
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
import asyncio
from datetime import datetime, timedelta

# Import existing cyberdelta components
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_manager import get_app_settings
from cyberdelta.config.secrets_manager import get_secrets_config
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HLRawCandle

class HyperliquidAdapter:
    """Adapter to expose existing Hyperliquid API through FastAPI

    Preserves all production features:
    - EIP-712 signature handling
    - Weight-based rate limiting
    - Comprehensive error handling
    - WebSocket auto-reconnection
    """

    def __init__(self):
        # Use existing configuration system - no changes to existing code
        self.config = get_app_settings()
        self.secrets = get_secrets_config()

        # Initialize existing API client exactly as it's done currently
        self.hl_api = HyperliquidAPI(
            self.config.exchanges.hyperliquid,
            self.secrets.exchanges.hyperliquid
        )

        self.is_connected = False

    async def start(self):
        """Start adapter using existing API client"""
        try:
            # Use existing WebSocket connection method
            await self.hl_api.connect_websocket()
            self.is_connected = True
        except Exception as e:
            raise RuntimeError(f"Failed to start Hyperliquid adapter: {e}")

    async def stop(self):
        """Stop adapter using existing API client"""
        if self.is_connected:
            await self.hl_api.close()
            self.is_connected = False

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get ticker data using existing API client"""
        try:
            # Direct call to existing method - no changes to core logic
            ticker_result = await self.hl_api.get_ticker(symbol)

            # Transform using existing data structures
            return {
                "exchange": "hyperliquid",
                "symbol": symbol,
                "last_price": float(ticker_result.last_price),
                "bid_price": float(ticker_result.bid_price) if ticker_result.bid_price else None,
                "ask_price": float(ticker_result.ask_price) if ticker_result.ask_price else None,
                "volume_24h": float(ticker_result.volume_24h) if ticker_result.volume_24h else None,
                "price_change_24h": float(ticker_result.price_change_24h) if ticker_result.price_change_24h else None,
                "price_change_pct_24h": float(ticker_result.price_change_pct_24h) if ticker_result.price_change_pct_24h else None,
                "timestamp": ticker_result.timestamp.isoformat(),
                "server_time": datetime.utcnow().isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get ticker for {symbol}: {e}")

    async def get_all_tickers(self) -> List[Dict[str, Any]]:
        """Get all tickers using existing API client"""
        try:
            # Use existing method to get all available symbols
            all_mids = await self.hl_api.get_all_mids()

            tickers = []
            for symbol, price_data in all_mids.items():
                tickers.append({
                    "exchange": "hyperliquid",
                    "symbol": symbol,
                    "last_price": float(price_data),
                    "timestamp": datetime.utcnow().isoformat()
                })

            return tickers
        except Exception as e:
            raise RuntimeError(f"Failed to get all tickers: {e}")

    async def get_candles(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get candle data using existing API client"""
        try:
            # Use existing candle fetching method
            candles = await self.hl_api.get_candles(
                symbol=symbol,
                interval=interval,
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )

            # Transform existing candle objects to API response format
            candle_data = []
            for candle in candles:
                candle_data.append({
                    "symbol": symbol,
                    "timestamp": candle.timestamp.isoformat(),
                    "open": float(candle.open_price),
                    "high": float(candle.high_price),
                    "low": float(candle.low_price),
                    "close": float(candle.close_price),
                    "volume": float(candle.volume),
                    "trades_count": candle.trades_count if hasattr(candle, 'trades_count') else None
                })

            return candle_data
        except Exception as e:
            raise RuntimeError(f"Failed to get candles for {symbol}: {e}")

    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Get funding rate using existing API client"""
        try:
            # Use existing funding rate method
            funding_data = await self.hl_api.get_funding_rate(symbol)

            return {
                "exchange": "hyperliquid",
                "symbol": symbol,
                "funding_rate": float(funding_data.funding_rate),
                "predicted_rate": float(funding_data.predicted_rate) if funding_data.predicted_rate else None,
                "next_funding_time": funding_data.next_funding_time.isoformat() if funding_data.next_funding_time else None,
                "mark_price": float(funding_data.mark_price) if funding_data.mark_price else None,
                "index_price": float(funding_data.index_price) if funding_data.index_price else None,
                "timestamp": funding_data.timestamp.isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get funding rate for {symbol}: {e}")

    async def get_orderbook(self, symbol: str, depth: int = 20) -> Dict[str, Any]:
        """Get orderbook using existing API client"""
        try:
            # Use existing orderbook method
            orderbook = await self.hl_api.get_orderbook(symbol, depth)

            return {
                "exchange": "hyperliquid",
                "symbol": symbol,
                "bids": [[float(price), float(size)] for price, size in orderbook.bids],
                "asks": [[float(price), float(size)] for price, size in orderbook.asks],
                "timestamp": orderbook.timestamp.isoformat(),
                "sequence": orderbook.sequence if hasattr(orderbook, 'sequence') else None
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get orderbook for {symbol}: {e}")

    async def subscribe_to_ticker(self, symbol: str):
        """Subscribe to ticker updates using existing WebSocket"""
        try:
            # Use existing WebSocket subscription method
            await self.hl_api.subscribe_to_ticker(symbol)
        except Exception as e:
            raise RuntimeError(f"Failed to subscribe to ticker {symbol}: {e}")

    async def subscribe_to_trades(self, symbol: str):
        """Subscribe to trade updates using existing WebSocket"""
        try:
            # Use existing WebSocket subscription method
            await self.hl_api.subscribe_to_trades(symbol)
        except Exception as e:
            raise RuntimeError(f"Failed to subscribe to trades {symbol}: {e}")

    def get_connection_status(self) -> Dict[str, Any]:
        """Get connection status"""
        return {
            "connected": self.is_connected,
            "exchange": "hyperliquid",
            "websocket_connected": self.hl_api.is_websocket_connected() if hasattr(self.hl_api, 'is_websocket_connected') else self.is_connected,
            "last_heartbeat": datetime.utcnow().isoformat()
        }
```

#### 3. Market Data Service
```python
# fastapi_market_data/services/market_data_service.py
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from adapters.hyperliquid_adapter import HyperliquidAdapter
from adapters.backpack_adapter import BackpackAdapter
from services.websocket_manager import WebSocketManager
from services.cache_service import CacheService

class MarketDataService:
    """Service coordinating all market data operations"""

    def __init__(self):
        # Initialize adapters for existing APIs
        self.hyperliquid = HyperliquidAdapter()
        self.backpack = BackpackAdapter()

        # Supporting services
        self.websocket_manager = WebSocketManager()
        self.cache_service = CacheService()

        # Service state
        self.is_running = False
        self.start_time: Optional[datetime] = None

        # Exchange mapping
        self.exchanges = {
            "hyperliquid": self.hyperliquid,
            "backpack": self.backpack
        }

    async def start(self):
        """Start all market data services"""
        try:
            self.start_time = datetime.utcnow()

            # Start exchange adapters
            await self.hyperliquid.start()
            await self.backpack.start()

            # Start supporting services
            await self.websocket_manager.start()

            self.is_running = True
            logging.info("Market data service started successfully")

        except Exception as e:
            logging.error(f"Failed to start market data service: {e}")
            raise

    async def stop(self):
        """Stop all market data services"""
        try:
            # Stop exchange adapters
            await self.hyperliquid.stop()
            await self.backpack.stop()

            # Stop supporting services
            await self.websocket_manager.stop()

            self.is_running = False
            logging.info("Market data service stopped")

        except Exception as e:
            logging.error(f"Error stopping market data service: {e}")

    async def get_ticker(self, exchange: str, symbol: str) -> Dict[str, Any]:
        """Get ticker data from specified exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}")

        # Check cache first
        cached_ticker = await self.cache_service.get_ticker(exchange, symbol)
        if cached_ticker:
            return cached_ticker

        # Get from exchange adapter
        adapter = self.exchanges[exchange]
        ticker_data = await adapter.get_ticker(symbol)

        # Cache the result
        await self.cache_service.set_ticker(exchange, symbol, ticker_data)

        return ticker_data

    async def get_all_tickers(self, exchange: str) -> List[Dict[str, Any]]:
        """Get all tickers from specified exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}")

        adapter = self.exchanges[exchange]
        return await adapter.get_all_tickers()

    async def get_candles(
        self,
        exchange: str,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get candle data from specified exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}")

        adapter = self.exchanges[exchange]
        return await adapter.get_candles(symbol, interval, start_time, end_time, limit)

    async def get_funding_rate(self, exchange: str, symbol: str) -> Dict[str, Any]:
        """Get funding rate from specified exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}")

        adapter = self.exchanges[exchange]
        return await adapter.get_funding_rate(symbol)

    async def get_orderbook(self, exchange: str, symbol: str, depth: int = 20) -> Dict[str, Any]:
        """Get orderbook from specified exchange"""
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}")

        adapter = self.exchanges[exchange]
        return await adapter.get_orderbook(symbol, depth)

    def get_connection_count(self) -> int:
        """Get total number of active connections"""
        return self.websocket_manager.get_connection_count()

    def get_uptime_seconds(self) -> float:
        """Get service uptime in seconds"""
        if not self.start_time:
            return 0.0
        return (datetime.utcnow() - self.start_time).total_seconds()

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive service status"""
        exchange_statuses = {}
        for name, adapter in self.exchanges.items():
            exchange_statuses[name] = adapter.get_connection_status()

        return {
            "service": "market_data",
            "running": self.is_running,
            "uptime_seconds": self.get_uptime_seconds(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "exchanges": exchange_statuses,
            "websocket_connections": self.get_connection_count(),
            "cache_stats": self.cache_service.get_stats()
        }
```

#### 4. FastAPI Routers
```python
# fastapi_market_data/routers/tickers.py
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from datetime import datetime

from services.market_data_service import MarketDataService
from models.ticker_models import TickerResponse, TickerListResponse
from middleware.authentication import get_current_user

router = APIRouter()

# Dependency to get market data service
async def get_market_data_service() -> MarketDataService:
    # This would be injected from the main app
    from main import market_data_service
    return market_data_service

@router.get("/{exchange}/{symbol}", response_model=TickerResponse)
async def get_ticker(
    exchange: str,
    symbol: str,
    service: MarketDataService = Depends(get_market_data_service),
    current_user = Depends(get_current_user)
):
    """
    Get ticker data for a specific symbol on an exchange.

    This endpoint wraps the existing CyberDelta API clients to provide
    standardized REST access to market data.
    """
    try:
        ticker_data = await service.get_ticker(exchange, symbol)
        return TickerResponse(**ticker_data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{exchange}", response_model=TickerListResponse)
async def get_all_tickers(
    exchange: str,
    service: MarketDataService = Depends(get_market_data_service),
    current_user = Depends(get_current_user)
):
    """
    Get all ticker data for an exchange.

    This endpoint provides access to all available symbols on the specified exchange
    using the existing CyberDelta API infrastructure.
    """
    try:
        tickers = await service.get_all_tickers(exchange)
        return TickerListResponse(
            exchange=exchange,
            tickers=tickers,
            count=len(tickers),
            timestamp=datetime.utcnow().isoformat()
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{exchange}/{symbol}/history")
async def get_ticker_history(
    exchange: str,
    symbol: str,
    hours: int = Query(24, ge=1, le=168, description="Hours of history to fetch (1-168)"),
    service: MarketDataService = Depends(get_market_data_service),
    current_user = Depends(get_current_user)
):
    """
    Get historical ticker data for analysis.

    This endpoint leverages existing CyberDelta data collection to provide
    historical price information for analysis and charting.
    """
    try:
        # This would use existing data storage/collection mechanisms
        # For now, we'll get recent candles as approximation
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        candles = await service.get_candles(
            exchange=exchange,
            symbol=symbol,
            interval="1h",
            start_time=start_time,
            end_time=end_time,
            limit=hours
        )

        # Transform candles to ticker-like format
        history = [
            {
                "timestamp": candle["timestamp"],
                "price": candle["close"],
                "volume": candle["volume"]
            }
            for candle in candles
        ]

        return {
            "exchange": exchange,
            "symbol": symbol,
            "history": history,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
```

### Response Models
```python
# fastapi_market_data/models/ticker_models.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class TickerResponse(BaseModel):
    """Standard ticker response model"""
    exchange: str = Field(..., description="Exchange name")
    symbol: str = Field(..., description="Trading pair symbol")
    last_price: float = Field(..., description="Last traded price")
    bid_price: Optional[float] = Field(None, description="Best bid price")
    ask_price: Optional[float] = Field(None, description="Best ask price")
    volume_24h: Optional[float] = Field(None, description="24-hour trading volume")
    price_change_24h: Optional[float] = Field(None, description="24-hour price change")
    price_change_pct_24h: Optional[float] = Field(None, description="24-hour price change percentage")
    timestamp: str = Field(..., description="Data timestamp (ISO format)")
    server_time: str = Field(..., description="Server timestamp (ISO format)")

    class Config:
        schema_extra = {
            "example": {
                "exchange": "hyperliquid",
                "symbol": "BTC-USD",
                "last_price": 50000.00,
                "bid_price": 49999.50,
                "ask_price": 50000.50,
                "volume_24h": 1234567.89,
                "price_change_24h": 500.00,
                "price_change_pct_24h": 1.01,
                "timestamp": "2024-01-01T12:00:00Z",
                "server_time": "2024-01-01T12:00:01Z"
            }
        }

class TickerListResponse(BaseModel):
    """Response model for multiple tickers"""
    exchange: str = Field(..., description="Exchange name")
    tickers: List[TickerResponse] = Field(..., description="List of ticker data")
    count: int = Field(..., description="Number of tickers returned")
    timestamp: str = Field(..., description="Response timestamp (ISO format)")
```

## FastAPI Trading Engine Service

### Purpose and Scope
- **Wrap existing trading engine**: Production-ready `cyberdelta.core.engine`, `cyberdelta.core.strategy_manager`
- **Strategy management APIs**: Start, stop, configure strategies
- **Portfolio APIs**: Current positions, balances, PnL with margin calculations
- **Order management**: Place, cancel, monitor orders
- **Risk management**: Risk metrics and controls with circuit breakers
- **Performance tracking**: Historical analysis with existing monitoring components

### Key Implementation
```python
# fastapi_trading_engine/adapters/engine_adapter.py
import sys
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import existing cyberdelta components
from cyberdelta.core.engine import Engine
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

class TradingEngineAdapter:
    """Adapter to expose existing trading engine through FastAPI

    Preserves all production features:
    - Enhanced Backpack integration with auto-lending
    - Sophisticated portfolio tracking with margin calculations
    - Risk management with circuit breakers
    - Performance monitoring and historical tracking
    """

    def __init__(self):
        # Initialize existing components exactly as they are currently
        self.engine = Engine(name="CyberDelta_API")
        self.strategy_manager = StrategyManager(...)
        self.portfolio_tracker = PortfolioTracker(...)
        self.risk_manager = RiskManager(...)
        self.performance_tracker = PerformanceTracker(...)  # For historical data

        # Load existing strategies
        self._load_strategies()

    def _load_strategies(self):
        """Load strategies using existing strategy system"""
        # Use existing strategy configuration and loading logic
        funding_strategy = FundingRateArbitrageStrategy(
            name="HL-BP-Funding",
            symbol="BTC-USD",
            data_handler=self.strategy_manager.data_handler,
            portfolio_tracker=self.portfolio_tracker,
            risk_manager=self.risk_manager,
            params={
                "funding_threshold": 0.01,
                "max_price_spread_pct": 0.5,
                "min_profit_usd": 10.0
            }
        )

        # Use existing engine methods
        self.engine.add_strategy(funding_strategy)

    async def list_strategies(self) -> List[Dict[str, Any]]:
        """List all strategies using existing engine"""
        strategies = []
        for name, strategy in self.engine.strategies.items():
            strategies.append({
                'id': name,
                'name': name,
                'type': strategy.__class__.__name__,
                'symbol': strategy.symbol,
                'enabled': strategy.enabled,
                'status': 'active' if strategy.enabled else 'inactive',
                'config': getattr(strategy, 'params', {}),
                'performance': await self._get_strategy_performance(name)
            })
        return strategies

    async def start_strategy(self, strategy_name: str) -> Dict[str, Any]:
        """Start strategy using existing engine"""
        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")

        try:
            # Use existing engine method - no changes to core logic
            self.engine.enable_strategy(strategy_name)

            return {
                'strategy_name': strategy_name,
                'status': 'started',
                'message': f'Strategy {strategy_name} started successfully',
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to start strategy {strategy_name}: {e}")

    async def stop_strategy(self, strategy_name: str) -> Dict[str, Any]:
        """Stop strategy using existing engine"""
        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")

        try:
            # Use existing engine method - no changes to core logic
            self.engine.disable_strategy(strategy_name)

            return {
                'strategy_name': strategy_name,
                'status': 'stopped',
                'message': f'Strategy {strategy_name} stopped successfully',
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to stop strategy {strategy_name}: {e}")

    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary using existing portfolio tracker"""
        try:
            # Use existing portfolio tracker method
            summary = self.portfolio_tracker.get_portfolio_summary()

            return {
                'total_value': float(summary.total_value),
                'total_pnl': float(summary.total_pnl),
                'daily_pnl': float(summary.daily_pnl),
                'unrealized_pnl': float(summary.unrealized_pnl),
                'realized_pnl': float(summary.realized_pnl),
                'positions_count': len(summary.positions),
                'balances_count': len(summary.balances),
                'last_update': summary.last_update.isoformat(),
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get portfolio summary: {e}")

    async def _get_strategy_performance(self, strategy_name: str) -> Dict[str, Any]:
        """Get strategy performance using existing portfolio tracker"""
        try:
            # Use existing performance calculation
            performance = self.portfolio_tracker.get_strategy_performance(strategy_name)

            return {
                'total_pnl': float(performance.total_pnl),
                'daily_pnl': float(performance.daily_pnl),
                'win_rate': float(performance.win_rate),
                'total_trades': performance.total_trades,
                'sharpe_ratio': float(performance.sharpe_ratio) if performance.sharpe_ratio else None,
                'max_drawdown': float(performance.max_drawdown) if performance.max_drawdown else None
            }
        except Exception as e:
            return {
                'total_pnl': 0.0,
                'daily_pnl': 0.0,
                'win_rate': 0.0,
                'total_trades': 0,
                'sharpe_ratio': None,
                'max_drawdown': None,
                'error': str(e)
            }
```

## Benefits of FastAPI Service Wrappers

### 1. **Preserve All Existing Logic**
- Zero changes to proven trading algorithms including auto-lending detection
- All existing error handling and edge cases preserved
- Existing performance optimizations maintained (weight-based rate limiting, etc.)
- Risk management systems unchanged including circuit breakers
- Comprehensive test coverage through existing VCR test suite

### 2. **Modern API Interface**
- RESTful endpoints with OpenAPI documentation
- Automatic request/response validation
- Rate limiting and authentication
- Standardized error responses

### 3. **External Integration**
- Third-party tools can access market data
- Monitoring systems can track performance
- External analysis tools can query data
- API keys for controlled access

### 4. **Scalability and Performance**
- FastAPI's async performance (20,000+ req/s)
- Independent scaling of services
- Caching and rate limiting
- Load balancing capabilities

### 5. **Development Efficiency**
- Automatic API documentation generation
- Type safety with Pydantic models
- Easy testing with built-in test client
- Consistent error handling patterns

This approach gives you modern, documented APIs while preserving all your valuable trading infrastructure exactly as it works today, including all the sophisticated enhancements like auto-lending detection, margin calculations, and comprehensive testing that have been built into the system.

### Key Success Factors for Service Wrappers

1. **Import, Don't Rebuild**: Always import existing cyberdelta modules directly
2. **Wrap, Don't Replace**: Add API layer without changing underlying logic
3. **Test with Existing Suite**: Use VCR cassettes to validate wrapper behavior
4. **Preserve Configuration**: Use existing config and secrets management
5. **Maintain Error Handling**: All existing error recovery stays intact
