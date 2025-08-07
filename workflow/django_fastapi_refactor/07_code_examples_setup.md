# Code Examples & Complete Setup Guide

> **🚨 CURRENT STATUS: PROPOSAL ONLY - NOT IMPLEMENTED**
>
> This document provides code examples for a **proposed migration** that has not been implemented.
>
> **Actual Current State:**
> - No Django/FastAPI/HTMX code exists
> - No adapter layer or service architecture
> - No database integration
> - Single monolithic command-line application

## Overview (Updated June 2025)

This document provides complete code examples, project structure, and setup instructions for implementing the minimal migration to Django + FastAPI + HTMX architecture. All examples preserve existing CyberDelta functionality (including sophisticated features like auto-lending and margin support) while adding modern interfaces. The timeline has been optimized to 8-9 weeks with a 2-person team based on the production-ready state of the current codebase.

**⚠️ IMPLEMENTATION STATUS: These are proposed code examples. None of this code has been written.**

## Complete Project Structure

```
CyberDeltaEngine/
├── cyberdelta/                     # EXISTING CODE - UNCHANGED (Production-Ready)
│   ├── apis/                      # ✅ Keep exactly as-is (Enhanced with auto-lending)
│   ├── core/                      # ✅ Keep exactly as-is (Proven trading engine)
│   ├── strategies/                # ✅ Keep exactly as-is (Funding arbitrage framework)
│   ├── validation/                # ✅ Keep exactly as-is (Circuit breakers)
│   ├── config/                    # ✅ Keep exactly as-is (YAML-based config)
│   ├── utils/                     # ✅ Keep exactly as-is (State management)
│   └── domain/monitoring/         # ✅ Keep exactly as-is (Monitoring services only)
├── shared/                        # NEW - Adapter layer
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── base_adapter.py
│   │   ├── trading_adapter.py
│   │   ├── market_data_adapter.py
│   │   ├── config_adapter.py
│   │   └── database_adapter.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── models.py
│   │   ├── migrations/
│   │   └── setup.sql
│   ├── messaging/
│   │   ├── __init__.py
│   │   ├── broker.py
│   │   └── schemas.py
│   └── utils/
│       ├── __init__.py
│       ├── auth.py
│       └── logging.py
├── services/                      # NEW - FastAPI services
│   ├── market_data/
│   │   ├── main.py
│   │   ├── routers/
│   │   ├── middleware/
│   │   ├── models/
│   │   └── requirements.txt
│   └── trading_engine/
│       ├── main.py
│       ├── routers/
│       ├── middleware/
│       ├── models/
│       └── requirements.txt
├── web/                          # NEW - Django application
│   ├── manage.py
│   ├── config/
│   ├── apps/
│   │   ├── dashboard/
│   │   ├── accounts/
│   │   └── configuration/
│   ├── static/
│   ├── templates/
│   └── requirements.txt
├── docker/                       # NEW - Containerization
│   ├── docker-compose.yml
│   ├── Dockerfile.fastapi
│   ├── Dockerfile.django
│   └── nginx.conf
├── scripts/                      # NEW - Deployment scripts
│   ├── setup.sh
│   ├── deploy.sh
│   └── rollback.sh
└── docs/                        # NEW - Documentation
    ├── api/
    ├── setup/
    └── migration/
```

## Complete Code Examples

### 1. Base Adapter Framework

```python
# shared/adapters/base_adapter.py
import sys
import os
import abc
import asyncio
from typing import Any, Dict, Optional, Type
from pathlib import Path

class BaseAdapter(abc.ABC):
    """Base adapter class for integrating with existing CyberDelta components"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self._setup_cyberdelta_path()
        self._initialized = False

    def _setup_cyberdelta_path(self):
        """Add existing cyberdelta to Python path"""
        cyberdelta_path = Path(__file__).parent.parent.parent / "cyberdelta"
        if str(cyberdelta_path) not in sys.path:
            sys.path.insert(0, str(cyberdelta_path))

    @abc.abstractmethod
    async def initialize(self):
        """Initialize the adapter with existing components"""
        pass

    @abc.abstractmethod
    async def shutdown(self):
        """Clean shutdown of adapter"""
        pass

    async def health_check(self) -> Dict[str, Any]:
        """Health check for monitoring"""
        return {
            'status': 'healthy' if self._initialized else 'not_initialized',
            'adapter_type': self.__class__.__name__,
            'config_loaded': bool(self.config)
        }

# shared/adapters/trading_adapter.py
import asyncio
from typing import Dict, List, Any, Optional
from decimal import Decimal

from .base_adapter import BaseAdapter

# Import existing CyberDelta components
from cyberdelta.core.engine import Engine
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.config.config_manager import get_app_settings
from cyberdelta.config.secrets_manager import get_secrets_config

class TradingAdapter(BaseAdapter):
    """Adapter exposing existing trading engine through modern interfaces"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        # Store references to existing components
        self.engine: Optional[Engine] = None
        self.portfolio_tracker: Optional[PortfolioTracker] = None
        self.risk_manager: Optional[RiskManager] = None
        self.performance_tracker: Optional[PerformanceTracker] = None
        self.strategies: Dict[str, Any] = {}

    async def initialize(self):
        """Initialize using existing CyberDelta components"""
        try:
            # Use existing configuration system
            app_config = get_app_settings()
            secrets_config = get_secrets_config()

            # Initialize existing components exactly as they are
            self.engine = Engine(name="CyberDelta_Service")

            # Initialize portfolio tracker with existing configuration
            self.portfolio_tracker = PortfolioTracker(
                config=app_config.portfolio,
                exchanges=app_config.exchanges
            )

            # Initialize risk manager with existing configuration
            self.risk_manager = RiskManager(
                config=app_config.risk_management,
                portfolio_tracker=self.portfolio_tracker
            )

            # Load existing strategies
            await self._load_strategies(app_config, secrets_config)

            self._initialized = True

        except Exception as e:
            raise RuntimeError(f"Failed to initialize TradingAdapter: {e}")

    async def _load_strategies(self, app_config, secrets_config):
        """Load strategies using existing strategy system"""
        # Load funding rate arbitrage strategy with existing configuration
        funding_strategy = FundingRateArbitrageStrategy(
            name="HL-BP-Funding",
            symbol="BTC-USD",
            data_handler=None,  # Will be set by engine
            portfolio_tracker=self.portfolio_tracker,
            risk_manager=self.risk_manager,
            params={
                "funding_threshold": 0.01,
                "max_price_spread_pct": 0.5,
                "min_profit_usd": 10.0,
                **self.config.get("strategy_params", {})
            }
        )

        # Add strategy to engine using existing methods
        self.engine.add_strategy(funding_strategy)
        self.strategies["HL-BP-Funding"] = funding_strategy

    async def shutdown(self):
        """Shutdown using existing component methods"""
        if self.engine:
            await self.engine.stop()

        if self.portfolio_tracker:
            await self.portfolio_tracker.stop()

        self._initialized = False

    # Public API methods that wrap existing functionality

    async def list_strategies(self) -> List[Dict[str, Any]]:
        """List all strategies using existing engine"""
        if not self._initialized:
            raise RuntimeError("Adapter not initialized")

        strategies = []
        for name, strategy in self.engine.strategies.items():
            # Get performance data using existing portfolio tracker
            performance = await self._get_strategy_performance(name)

            strategies.append({
                'id': name,
                'name': name,
                'type': strategy.__class__.__name__,
                'symbol': strategy.symbol,
                'enabled': strategy.enabled,
                'status': 'active' if strategy.enabled else 'inactive',
                'performance': performance,
                'config': getattr(strategy, 'params', {}),
                'last_update': strategy.last_update.isoformat() if hasattr(strategy, 'last_update') else None
            })

        return strategies

    async def start_strategy(self, strategy_name: str) -> Dict[str, Any]:
        """Start strategy using existing engine"""
        if not self._initialized:
            raise RuntimeError("Adapter not initialized")

        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")

        try:
            # Use existing engine method - zero changes to core logic
            await self.engine.enable_strategy(strategy_name)

            return {
                'strategy_name': strategy_name,
                'status': 'started',
                'message': f'Strategy {strategy_name} started successfully',
                'timestamp': asyncio.get_event_loop().time()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to start strategy {strategy_name}: {e}")

    async def stop_strategy(self, strategy_name: str) -> Dict[str, Any]:
        """Stop strategy using existing engine"""
        if not self._initialized:
            raise RuntimeError("Adapter not initialized")

        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")

        try:
            # Use existing engine method - zero changes to core logic
            await self.engine.disable_strategy(strategy_name)

            return {
                'strategy_name': strategy_name,
                'status': 'stopped',
                'message': f'Strategy {strategy_name} stopped successfully',
                'timestamp': asyncio.get_event_loop().time()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to stop strategy {strategy_name}: {e}")

    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary using existing portfolio tracker"""
        if not self._initialized:
            raise RuntimeError("Adapter not initialized")

        try:
            # Use existing portfolio tracker method
            summary = await self.portfolio_tracker.get_portfolio_summary()

            return {
                'total_value': float(summary.total_value),
                'total_pnl': float(summary.total_pnl),
                'daily_pnl': float(summary.daily_pnl),
                'unrealized_pnl': float(summary.unrealized_pnl),
                'realized_pnl': float(summary.realized_pnl),
                'positions_count': len(summary.positions),
                'balances_count': len(summary.balances),
                'last_update': summary.last_update.isoformat(),
                'positions': [
                    {
                        'symbol': pos.symbol,
                        'size': float(pos.size),
                        'notional_value': float(pos.notional_value),
                        'unrealized_pnl': float(pos.unrealized_pnl),
                        'entry_price': float(pos.entry_price),
                        'mark_price': float(pos.mark_price)
                    }
                    for pos in summary.positions
                ],
                'balances': [
                    {
                        'asset': bal.asset,
                        'total': float(bal.total),
                        'available': float(bal.available),
                        'locked': float(bal.locked)
                    }
                    for bal in summary.balances
                ]
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get portfolio summary: {e}")

    async def _get_strategy_performance(self, strategy_name: str) -> Dict[str, Any]:
        """Get strategy performance using existing portfolio tracker"""
        try:
            # Use existing performance calculation
            performance = await self.portfolio_tracker.get_strategy_performance(strategy_name)

            return {
                'total_pnl': float(performance.total_pnl),
                'daily_pnl': float(performance.daily_pnl),
                'win_rate': float(performance.win_rate),
                'total_trades': performance.total_trades,
                'sharpe_ratio': float(performance.sharpe_ratio) if performance.sharpe_ratio else None,
                'max_drawdown': float(performance.max_drawdown) if performance.max_drawdown else None,
                'profit_factor': float(performance.profit_factor) if hasattr(performance, 'profit_factor') else None
            }
        except Exception as e:
            # Return default performance if calculation fails
            return {
                'total_pnl': 0.0,
                'daily_pnl': 0.0,
                'win_rate': 0.0,
                'total_trades': 0,
                'sharpe_ratio': None,
                'max_drawdown': None,
                'profit_factor': None,
                'error': str(e)
            }
```

### 2. FastAPI Market Data Service

```python
# services/market_data/main.py
import sys
import os
from pathlib import Path
from contextlib import asynccontextmanager

# Add shared modules to path
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from adapters.market_data_adapter import MarketDataAdapter
from routers import tickers, candles, funding_rates, websocket
from middleware.rate_limiting import RateLimitMiddleware
from middleware.authentication import AuthenticationMiddleware
from models.response_models import ErrorResponse

# Global adapter instance
market_data_adapter: Optional[MarketDataAdapter] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle management for FastAPI app"""
    global market_data_adapter

    # Startup
    try:
        market_data_adapter = MarketDataAdapter()
        await market_data_adapter.initialize()
        print("Market Data Service started successfully")
        yield
    except Exception as e:
        print(f"Failed to start Market Data Service: {e}")
        raise
    finally:
        # Shutdown
        if market_data_adapter:
            await market_data_adapter.shutdown()
        print("Market Data Service stopped")

# Create FastAPI app
app = FastAPI(
    title="CyberDelta Market Data API",
    description="High-performance market data API wrapping existing CyberDelta components",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
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

# Dependency to get adapter
async def get_market_data_adapter() -> MarketDataAdapter:
    """Dependency to inject market data adapter"""
    if not market_data_adapter:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return market_data_adapter

@app.get("/health")
async def health_check(adapter: MarketDataAdapter = Depends(get_market_data_adapter)):
    """Health check endpoint"""
    health_status = await adapter.health_check()

    return {
        "status": "healthy" if health_status["status"] == "healthy" else "unhealthy",
        "service": "market_data",
        "timestamp": datetime.utcnow().isoformat(),
        "details": health_status
    }

@app.get("/metrics")
async def get_metrics(adapter: MarketDataAdapter = Depends(get_market_data_adapter)):
    """Service metrics endpoint"""
    return await adapter.get_metrics()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )

# services/market_data/routers/tickers.py
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from datetime import datetime

from adapters.market_data_adapter import MarketDataAdapter
from models.ticker_models import TickerResponse, TickerListResponse
from middleware.authentication import get_current_user

router = APIRouter()

@router.get("/{exchange}/{symbol}", response_model=TickerResponse)
async def get_ticker(
    exchange: str,
    symbol: str,
    adapter: MarketDataAdapter = Depends(get_market_data_adapter),
    current_user = Depends(get_current_user)
):
    """
    Get ticker data for a specific symbol on an exchange.

    This endpoint wraps the existing CyberDelta API clients to provide
    standardized REST access to market data.
    """
    try:
        ticker_data = await adapter.get_ticker(exchange, symbol)
        return TickerResponse(**ticker_data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{exchange}", response_model=TickerListResponse)
async def get_all_tickers(
    exchange: str,
    adapter: MarketDataAdapter = Depends(get_market_data_adapter),
    current_user = Depends(get_current_user)
):
    """Get all ticker data for an exchange"""
    try:
        tickers = await adapter.get_all_tickers(exchange)
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

# services/market_data/models/ticker_models.py
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

### 3. Django HTMX Dashboard

```python
# web/apps/dashboard/views.py
import sys
import os
from pathlib import Path

# Add shared modules to path
shared_path = Path(__file__).parent.parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.views.decorators.http import require_http_methods
import json
import asyncio
from datetime import datetime, timedelta

from adapters.trading_adapter import TradingAdapter
from adapters.market_data_adapter import MarketDataAdapter

class DashboardView(LoginRequiredMixin, TemplateView):
    """Main dashboard view with HTMX integration"""
    template_name = 'dashboard/index.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.trading_adapter = None
        self.market_adapter = None

    async def _get_adapters(self):
        """Initialize adapters for dashboard data"""
        if not self.trading_adapter:
            self.trading_adapter = TradingAdapter()
            await self.trading_adapter.initialize()

        if not self.market_adapter:
            self.market_adapter = MarketDataAdapter()
            await self.market_adapter.initialize()

        return self.trading_adapter, self.market_adapter

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Get dashboard data using existing cyberdelta components
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            trading_adapter, market_adapter = loop.run_until_complete(self._get_adapters())

            # Get data using existing components
            portfolio_data = loop.run_until_complete(trading_adapter.get_portfolio_summary())
            strategies_data = loop.run_until_complete(trading_adapter.list_strategies())

            context.update({
                'portfolio': portfolio_data,
                'strategies': strategies_data,
                'user': self.request.user,
                'page_title': 'Trading Dashboard'
            })

        except Exception as e:
            context.update({
                'error': str(e),
                'portfolio': {},
                'strategies': []
            })

        return context

class PerformanceChartView(LoginRequiredMixin, TemplateView):
    """Performance chart component using Plotly.js"""
    template_name = 'dashboard/components/performance_chart.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Get parameters from request
        strategies = self.request.GET.getlist('strategies')
        time_range = self.request.GET.get('time_range', '24h')

        if not strategies:
            strategies = ['HL-BP-Funding']  # Default strategy

        try:
            # Get performance data using existing components
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            trading_adapter = TradingAdapter()
            loop.run_until_complete(trading_adapter.initialize())

            # Get performance data for each strategy
            performance_data = {
                'timestamps': [],
                'strategies': {}
            }

            for strategy_name in strategies:
                strategy_performance = loop.run_until_complete(
                    trading_adapter._get_strategy_performance(strategy_name)
                )
                performance_data['strategies'][strategy_name] = strategy_performance

            # Create Plotly figure data
            plotly_data = self._create_plotly_figure(performance_data)

            context.update({
                'chart_data': json.dumps(plotly_data),
                'selected_strategies': strategies,
                'time_range': time_range,
                'available_strategies': ['HL-BP-Funding'],  # Get from adapter
                'time_range_options': [
                    {'value': '1h', 'label': '1 Hour'},
                    {'value': '6h', 'label': '6 Hours'},
                    {'value': '24h', 'label': '24 Hours'},
                    {'value': '7d', 'label': '7 Days'},
                    {'value': '30d', 'label': '30 Days'},
                ]
            })

        except Exception as e:
            context.update({
                'error': str(e),
                'chart_data': '{}',
                'selected_strategies': strategies,
                'time_range': time_range
            })

        return context

    def _create_plotly_figure(self, performance_data):
        """Create Plotly figure data for performance chart"""
        traces = []

        # Create sample data for demonstration
        import numpy as np
        from datetime import datetime, timedelta

        # Generate sample timestamps
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        timestamps = [
            (start_time + timedelta(minutes=i*15)).isoformat()
            for i in range(96)  # 15-minute intervals for 24 hours
        ]

        for strategy_name, strategy_data in performance_data['strategies'].items():
            # Generate sample cumulative returns
            cumulative_returns = np.cumsum(np.random.normal(0.001, 0.02, 96))

            traces.append({
                'x': timestamps,
                'y': cumulative_returns.tolist(),
                'type': 'scatter',
                'mode': 'lines',
                'name': strategy_name,
                'line': {'width': 2}
            })

        layout = {
            'title': {
                'text': 'Strategy Performance',
                'x': 0.5,
                'xanchor': 'center'
            },
            'xaxis': {'title': 'Time'},
            'yaxis': {'title': 'Cumulative Return (%)', 'tickformat': '.2%'},
            'hovermode': 'x unified',
            'showlegend': True,
            'height': 400,
            'margin': {'l': 60, 'r': 60, 't': 80, 'b': 60}
        }

        return {'data': traces, 'layout': layout}

@require_http_methods(["POST"])
def strategy_control(request, strategy_name, action):
    """Strategy start/stop control with HTMX response"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        trading_adapter = TradingAdapter()
        loop.run_until_complete(trading_adapter.initialize())

        if action == 'start':
            result = loop.run_until_complete(trading_adapter.start_strategy(strategy_name))
        elif action == 'stop':
            result = loop.run_until_complete(trading_adapter.stop_strategy(strategy_name))
        else:
            result = {'success': False, 'error': 'Invalid action'}

        if request.headers.get('HX-Request'):
            # Return HTMX partial update
            return render(request, 'dashboard/partials/strategy_status.html', {
                'strategy_name': strategy_name,
                'result': result
            })
        else:
            # Return JSON for API calls
            return JsonResponse(result)

    except Exception as e:
        error_result = {'success': False, 'error': str(e)}

        if request.headers.get('HX-Request'):
            return render(request, 'dashboard/partials/strategy_status.html', {
                'strategy_name': strategy_name,
                'result': error_result
            })
        else:
            return JsonResponse(error_result, status=500)
```

### 4. Docker Configuration

```dockerfile
# docker/Dockerfile.fastapi
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY services/market_data/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cyberdelta/ ./cyberdelta/
COPY shared/ ./shared/
COPY services/market_data/ ./

# Expose port
EXPOSE 8001

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]

# docker/Dockerfile.django
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY web/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cyberdelta/ ./cyberdelta/
COPY shared/ ./shared/
COPY web/ ./

# Collect static files
RUN python manage.py collectstatic --noinput

# Expose port
EXPOSE 8000

# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "config.wsgi:application"]
```

```yaml
# docker/docker-compose.yml
version: '3.8'

services:
  postgres:
    image: timescale/timescaledb:latest-pg15
    environment:
      POSTGRES_DB: cyberdelta_db
      POSTGRES_USER: cyberdelta
      POSTGRES_PASSWORD: cyberdelta_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./shared/database/setup.sql:/docker-entrypoint-initdb.d/setup.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cyberdelta"]
      interval: 30s
      timeout: 10s
      retries: 3

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 30s
      timeout: 10s
      retries: 3

  market-data-service:
    build:
      context: ..
      dockerfile: docker/Dockerfile.fastapi
    ports:
      - "8001:8001"
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_password@postgres:5432/cyberdelta_db
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ../cyberdelta:/app/cyberdelta:ro
      - ../shared:/app/shared:ro

  trading-service:
    build:
      context: ..
      dockerfile: docker/Dockerfile.fastapi
    working_dir: /app/services/trading_engine
    ports:
      - "8002:8002"
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_password@postgres:5432/cyberdelta_db
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]

  web-dashboard:
    build:
      context: ..
      dockerfile: docker/Dockerfile.django
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_password@postgres:5432/cyberdelta_db
      - REDIS_URL=redis://redis:6379/0
      - DJANGO_SETTINGS_MODULE=config.settings.production
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ../cyberdelta:/app/cyberdelta:ro
      - ../shared:/app/shared:ro

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - web-dashboard
      - market-data-service
      - trading-service

volumes:
  postgres_data:
  redis_data:
```

### 5. Setup and Deployment Scripts

```bash
#!/bin/bash
# scripts/setup.sh - Complete environment setup

set -e

echo "Setting up CyberDelta v2 environment..."

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Docker required but not installed. Aborting." >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose required but not installed. Aborting." >&2; exit 1; }

# Create directory structure
echo "Creating project structure..."
mkdir -p {shared/{adapters,database,messaging,utils},services/{market_data,trading_engine},web,docker,scripts,docs}

# Copy existing cyberdelta directory
if [ ! -d "cyberdelta" ]; then
    echo "Error: cyberdelta directory not found. Please run from project root."
    exit 1
fi

# Create Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install base requirements
echo "Installing base requirements..."
pip install --upgrade pip
pip install fastapi uvicorn django pydantic asyncpg redis

# Setup environment variables
echo "Creating environment configuration..."
cat > .env << EOF
# Database Configuration
DATABASE_URL=postgresql://cyberdelta:cyberdelta_password@localhost:5432/cyberdelta_db
REDIS_URL=redis://localhost:6379/0

# Service Configuration
MARKET_DATA_SERVICE_URL=http://localhost:8001
TRADING_SERVICE_URL=http://localhost:8002
WEB_DASHBOARD_URL=http://localhost:8000

# Security
SECRET_KEY=$(python -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())')
ALLOWED_HOSTS=localhost,127.0.0.1

# Development
DEBUG=true
LOG_LEVEL=INFO
EOF

# Initialize database
echo "Initializing database..."
docker-compose -f docker/docker-compose.yml up -d postgres redis
sleep 10

# Wait for database to be ready
until docker exec cyberdelta_postgres_1 pg_isready -U cyberdelta; do
    echo "Waiting for database to be ready..."
    sleep 2
done

# Run database migrations
echo "Running database setup..."
docker exec cyberdelta_postgres_1 psql -U cyberdelta -d cyberdelta_db -f /docker-entrypoint-initdb.d/setup.sql

echo "Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Start services: docker-compose -f docker/docker-compose.yml up"
echo "3. Visit dashboard: http://localhost:8000"
echo "4. Check API docs: http://localhost:8001/docs"

#!/bin/bash
# scripts/deploy.sh - Production deployment script

set -e

echo "Deploying CyberDelta v2 to production..."

# Environment validation
if [ "$NODE_ENV" != "production" ]; then
    echo "Warning: NODE_ENV is not set to production"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Backup existing system
echo "Creating backup of current system..."
sudo systemctl stop cyberdelta-v1 || true
sudo cp -r /opt/cyberdelta/current /opt/cyberdelta/backup-$(date +%Y%m%d-%H%M%S)

# Deploy new version
echo "Deploying new version..."
sudo mkdir -p /opt/cyberdelta/v2
sudo cp -r . /opt/cyberdelta/v2/

# Build and start services
echo "Building services..."
cd /opt/cyberdelta/v2
sudo docker-compose -f docker/docker-compose.yml build
sudo docker-compose -f docker/docker-compose.yml up -d

# Health check
echo "Performing health checks..."
sleep 30

curl -f http://localhost:8001/health || { echo "Market data service health check failed"; exit 1; }
curl -f http://localhost:8002/health || { echo "Trading service health check failed"; exit 1; }
curl -f http://localhost:8000/ || { echo "Web dashboard health check failed"; exit 1; }

# Update nginx configuration
echo "Updating load balancer configuration..."
sudo cp docker/nginx.conf /etc/nginx/sites-available/cyberdelta-v2
sudo ln -sf /etc/nginx/sites-available/cyberdelta-v2 /etc/nginx/sites-enabled/cyberdelta
sudo nginx -t && sudo systemctl reload nginx

echo "Deployment completed successfully!"
echo ""
echo "Services are running at:"
echo "- Dashboard: https://yourdomain.com"
echo "- Market Data API: https://yourdomain.com/api/v1"
echo "- Trading API: https://yourdomain.com/trading/v1"

#!/bin/bash
# scripts/rollback.sh - Emergency rollback script

set -e

echo "Rolling back to previous CyberDelta version..."

# Stop new services
echo "Stopping v2 services..."
sudo docker-compose -f /opt/cyberdelta/v2/docker/docker-compose.yml down

# Restore previous system
echo "Restoring v1 system..."
sudo systemctl start cyberdelta-v1

# Restore nginx configuration
echo "Restoring load balancer configuration..."
sudo ln -sf /etc/nginx/sites-available/cyberdelta-v1 /etc/nginx/sites-enabled/cyberdelta
sudo nginx -t && sudo systemctl reload nginx

echo "Rollback completed successfully!"
echo "Original system restored and running."
```

## Development Workflow

### 1. Local Development Setup

```bash
# Clone repository and setup environment
git clone <your-repo>
cd CyberDeltaEngine
./scripts/setup.sh

# Activate virtual environment
source venv/bin/activate

# Start development services (PostgreSQL + TimescaleDB + Redis)
docker-compose -f docker/docker-compose.yml up -d postgres redis

# Run FastAPI market data service
cd services/market_data
uvicorn main:app --reload --port 8001

# Run Django dashboard (in another terminal)
cd web
python manage.py runserver 8000

# Run FastAPI trading service (in another terminal)
cd services/trading_engine
uvicorn main:app --reload --port 8002

# Verify existing functionality still works
cd cyberdelta
python -m pytest tests/ -v  # Run existing test suite
```

### 2. Testing Strategy

```python
# tests/test_adapters.py - Test adapter integration
import pytest
import asyncio
from shared.adapters.trading_adapter import TradingAdapter

@pytest.mark.asyncio
async def test_trading_adapter_initialization():
    """Test that adapter properly initializes with existing components"""
    adapter = TradingAdapter()
    await adapter.initialize()

    assert adapter._initialized == True
    assert adapter.engine is not None
    assert adapter.portfolio_tracker is not None

    await adapter.shutdown()

@pytest.mark.asyncio
async def test_strategy_lifecycle():
    """Test strategy start/stop through adapter"""
    adapter = TradingAdapter()
    await adapter.initialize()

    # Test list strategies
    strategies = await adapter.list_strategies()
    assert len(strategies) > 0

    # Test start strategy
    strategy_name = strategies[0]['name']
    result = await adapter.start_strategy(strategy_name)
    assert result['status'] == 'started'

    # Test stop strategy
    result = await adapter.stop_strategy(strategy_name)
    assert result['status'] == 'stopped'

    await adapter.shutdown()

# tests/test_api_endpoints.py - Test FastAPI endpoints
import pytest
from fastapi.testclient import TestClient
from services.market_data.main import app

client = TestClient(app)

def test_health_endpoint():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_ticker_endpoint():
    """Test ticker data endpoint"""
    response = client.get("/api/v1/tickers/hyperliquid/BTC-USD")
    assert response.status_code == 200
    data = response.json()
    assert "exchange" in data
    assert "symbol" in data
    assert "last_price" in data
```

## Monitoring and Maintenance

### 1. Health Monitoring

```python
# shared/monitoring/health_checker.py
import asyncio
import aiohttp
from typing import Dict, List, Any
from datetime import datetime

class SystemHealthChecker:
    """Monitor health of all services"""

    def __init__(self):
        self.services = {
            'market_data': 'http://localhost:8001/health',
            'trading_engine': 'http://localhost:8002/health',
            'web_dashboard': 'http://localhost:8000/health/',
        }

    async def check_all_services(self) -> Dict[str, Any]:
        """Check health of all services"""
        results = {}

        async with aiohttp.ClientSession() as session:
            for service_name, url in self.services.items():
                try:
                    async with session.get(url, timeout=5) as response:
                        if response.status == 200:
                            data = await response.json()
                            results[service_name] = {
                                'status': 'healthy',
                                'response_time': response.headers.get('X-Response-Time', 'unknown'),
                                'details': data
                            }
                        else:
                            results[service_name] = {
                                'status': 'unhealthy',
                                'error': f'HTTP {response.status}'
                            }
                except Exception as e:
                    results[service_name] = {
                        'status': 'error',
                        'error': str(e)
                    }

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'overall_status': 'healthy' if all(
                r.get('status') == 'healthy' for r in results.values()
            ) else 'degraded',
            'services': results
        }
```

### 2. Performance Monitoring

```python
# shared/monitoring/performance_monitor.py
import time
import psutil
import asyncio
from typing import Dict, Any

class PerformanceMonitor:
    """Monitor system performance metrics"""

    async def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system performance metrics"""
        return {
            'timestamp': time.time(),
            'cpu': {
                'usage_percent': psutil.cpu_percent(interval=1),
                'load_average': psutil.getloadavg(),
                'cores': psutil.cpu_count()
            },
            'memory': {
                'usage_percent': psutil.virtual_memory().percent,
                'available_gb': psutil.virtual_memory().available / (1024**3),
                'total_gb': psutil.virtual_memory().total / (1024**3)
            },
            'disk': {
                'usage_percent': psutil.disk_usage('/').percent,
                'free_gb': psutil.disk_usage('/').free / (1024**3)
            },
            'network': {
                'bytes_sent': psutil.net_io_counters().bytes_sent,
                'bytes_recv': psutil.net_io_counters().bytes_recv
            }
        }
```

This complete setup guide provides everything needed to implement the minimal migration while preserving all existing CyberDelta functionality. The architecture ensures your valuable trading infrastructure remains unchanged while gaining modern interfaces and capabilities.

## Updated Implementation Summary

### What Changed from Original Plan
1. **Timeline**: Reduced from 12 weeks to 8-9 weeks due to production-ready codebase
2. **Team Size**: Reduced from 4 people to 2 people with clearer scope
3. **Budget**: Reduced from $45,000 to $35,000 based on mature foundation
4. **Complexity**: Lower than expected due to sophisticated existing features

### Key Success Factors
1. **Preserve Everything**: The existing codebase is production-ready with advanced features
2. **Wrapper Pattern**: All new components import and wrap existing functionality
3. **Database First**: PostgreSQL + TimescaleDB for persistence from day one
4. **Test Coverage**: Use existing VCR test suite to validate all adapters
5. **Incremental Deployment**: Services can be deployed independently

### Expected Outcomes
- **Zero downtime migration** with instant rollback capability
- **100% feature parity** plus historical analysis and multi-user support
- **90% smaller frontend bundle** (HTMX vs React/Dash)
- **Production-ready APIs** for external integration
- **Historical data retention** for analysis and compliance

The approach transforms CyberDeltaEngine from an excellent single-user trading system into a production-grade platform while preserving all the sophisticated trading logic that already works exceptionally well.
