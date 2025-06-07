# Django + FastAPI Refactor: Architecture Analysis

## Executive Summary

This document proposes a hybrid architecture combining Django for the web interface and backend services with FastAPI for high-performance trading engine APIs. This approach leverages the strengths of both frameworks while addressing the current system's limitations.

## Current Architecture Analysis

### Existing CyberDeltaEngine Structure
```
Current System (Async Python + Dash)
├── main.py (Entry point with asyncio)
├── core/
│   ├── engine.py (Trading engine)
│   ├── strategy_manager.py
│   ├── execution_handler.py
│   ├── risk_manager.py
│   └── data_handler.py
├── apis/
│   ├── hyperliquid/ (Async API client)
│   ├── backpack/ (Async API client)
│   └── base/ (Exchange abstractions)
├── strategies/
│   └── funding_rate_arbitrage.py
├── monitoring/
│   └── real_time_dashboard.py (Dash/React)
└── config/ (YAML-based configuration)
```

### Current System Strengths
- **High Performance**: Async architecture for real-time trading
- **Type Safety**: Extensive Pydantic models and type hints
- **Modular Design**: Clear separation of concerns
- **Exchange Abstractions**: Well-designed API layer
- **Comprehensive Testing**: Unit and integration test coverage

### Current System Limitations
- **Dashboard Complexity**: Dash/React introduces build complexity
- **State Persistence**: All state is in-memory, lost on restart
- **Scalability**: Single-process architecture limits scaling
- **Configuration Management**: File-based config vs database-driven
- **User Management**: No authentication or multi-user support
- **API Access**: No external API for integration or monitoring

## Proposed Hybrid Architecture

### High-Level Design Philosophy

**Separation of Concerns:**
- **Django**: Web UI, user management, configuration, monitoring, analytics
- **FastAPI**: High-performance trading engine, real-time data processing, strategy execution
- **Shared Database**: PostgreSQL + TimescaleDB for persistence and analytics
- **Message Queue**: Redis/RabbitMQ for inter-service communication

### Architecture Overview
```
┌─────────────────────────────────────────────────────────────┐
│                    Client Layer                             │
├─────────────────────────────────────────────────────────────┤
│  HTMX Dashboard  │  React Admin  │  Mobile App  │  API Clients │
└─────────────────────────────────────────────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │      Load Balancer      │
                    │       (Nginx)          │
                    └─────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                       │                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Django Web App │    │   FastAPI Core  │    │  FastAPI Market │
│                 │    │  Trading Engine │    │  Data Service   │
│ • HTMX Dashboard│    │                 │    │                 │
│ • User Management│   │ • Strategy Exec │    │ • WebSocket Hub │
│ • Configuration │    │ • Risk Mgmt     │    │ • Data Ingestion│
│ • Analytics     │    │ • Order Routing │    │ • Rate Limiting │
│ • Monitoring    │    │ • Portfolio Mgmt│    │ • Market Data   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                        │
        └───────────────────────┼────────────────────────┘
                                │
        ┌───────────────────────────────────────────────────┐
        │              Message Queue Layer                  │
        │            (Redis/RabbitMQ)                       │
        │  • Strategy Signals  • Risk Alerts  • Updates    │
        └───────────────────────────────────────────────────┘
                                │
        ┌───────────────────────────────────────────────────┐
        │                Database Layer                     │
        │         PostgreSQL + TimescaleDB                  │
        │  • User Data  • Config  • Time-series Data        │
        └───────────────────────────────────────────────────┘
                                │
        ┌───────────────────────────────────────────────────┐
        │              External APIs                        │
        │    • Hyperliquid  • Backpack  • Future Exchanges │
        └───────────────────────────────────────────────────┘
```

## Service Architecture Details

### 1. Django Web Application

**Primary Responsibilities:**
- User interface and experience
- User authentication and authorization
- Configuration management
- Historical analytics and reporting
- System monitoring and alerting
- Admin interface for system management

**Technology Stack:**
- Django 5.x with async support
- HTMX for reactive UI (replacing Dash/React)
- TailwindCSS for styling
- Django Channels for WebSocket (dashboard updates)
- Django REST Framework for internal APIs
- PostgreSQL for relational data

**Key Features:**
```python
# Django Apps Structure
django_cyberdelta/
├── apps/
│   ├── accounts/          # User management
│   ├── dashboard/         # HTMX dashboard
│   ├── configuration/     # System config management
│   ├── analytics/         # Historical analysis
│   ├── monitoring/        # System health monitoring
│   ├── api/              # Internal REST APIs
│   └── notifications/     # Alerts and notifications
├── services/             # Business logic services
├── tasks/               # Celery background tasks
└── static/              # HTMX + TailwindCSS assets
```

### 2. FastAPI Trading Engine

**Primary Responsibilities:**
- Real-time strategy execution
- Order management and routing
- Risk management and validation
- Portfolio tracking and updates
- High-frequency data processing

**Technology Stack:**
- FastAPI with async/await
- Pydantic v2 for data validation
- SQLAlchemy 2.0 with async support
- Redis for caching and pub/sub
- WebSocket for real-time communication
- Structured logging with correlation IDs

**Key Features:**
```python
# FastAPI Engine Structure
fastapi_engine/
├── routers/
│   ├── strategies/        # Strategy management endpoints
│   ├── orders/           # Order management
│   ├── portfolio/        # Portfolio operations
│   ├── risk/            # Risk management
│   └── websocket/       # Real-time data streams
├── services/
│   ├── strategy_executor.py
│   ├── order_router.py
│   ├── risk_validator.py
│   └── portfolio_tracker.py
├── models/              # Pydantic models
├── dependencies/        # FastAPI dependencies
└── middleware/          # Custom middleware
```

### 3. FastAPI Market Data Service

**Primary Responsibilities:**
- Exchange API management
- Market data ingestion and normalization
- WebSocket connection management
- Data caching and distribution
- Rate limiting and error handling

**Key Features:**
```python
# FastAPI Market Data Structure
fastapi_market_data/
├── routers/
│   ├── market_data/      # Market data endpoints
│   ├── exchanges/        # Exchange status and info
│   └── websocket/        # Real-time market data
├── services/
│   ├── hyperliquid_service.py
│   ├── backpack_service.py
│   ├── data_normalizer.py
│   └── websocket_manager.py
├── models/              # Market data models
└── background/          # Background data collection
```

## Inter-Service Communication

### 1. Database Sharing
```python
# Shared database models between Django and FastAPI
# Using SQLAlchemy models that map to Django models

# FastAPI SQLAlchemy model
class Strategy(Base):
    __tablename__ = 'strategies_strategy'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    is_active = Column(Boolean, default=True)
    config = Column(JSON)
    
# Django model (same table)
class Strategy(models.Model):
    name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    config = models.JSONField()
    
    class Meta:
        db_table = 'strategies_strategy'
```

### 2. Message Queue Integration
```python
# Redis pub/sub for real-time communication
class MessageBroker:
    def __init__(self):
        self.redis = redis.Redis(host='redis', port=6379, db=0)
    
    async def publish_strategy_signal(self, signal: TradeSignal):
        """Publish trading signal from FastAPI to Django"""
        await self.redis.publish(
            'strategy_signals',
            signal.model_dump_json()
        )
    
    async def publish_risk_alert(self, alert: RiskAlert):
        """Publish risk alert to monitoring dashboard"""
        await self.redis.publish(
            'risk_alerts',
            alert.model_dump_json()
        )

# Django Channels consumer for real-time updates
class DashboardConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # Subscribe to Redis channels
        self.redis_client = redis.Redis()
        await self.redis_client.subscribe('strategy_signals', 'risk_alerts')
        await self.accept()
    
    async def receive_redis_message(self, message):
        # Forward Redis messages to WebSocket clients
        await self.send(text_data=message['data'])
```

### 3. REST API Communication
```python
# Django calling FastAPI for strategy operations
class StrategyService:
    def __init__(self):
        self.fastapi_base_url = settings.FASTAPI_ENGINE_URL
        self.session = httpx.AsyncClient()
    
    async def start_strategy(self, strategy_id: int):
        """Start strategy via FastAPI"""
        response = await self.session.post(
            f"{self.fastapi_base_url}/strategies/{strategy_id}/start"
        )
        return response.json()
    
    async def get_strategy_performance(self, strategy_id: int):
        """Get real-time strategy performance"""
        response = await self.session.get(
            f"{self.fastapi_base_url}/strategies/{strategy_id}/performance"
        )
        return response.json()

# FastAPI calling Django for configuration
class ConfigService:
    def __init__(self):
        self.django_api_url = settings.DJANGO_API_URL
        self.session = httpx.AsyncClient()
    
    async def get_strategy_config(self, strategy_id: int):
        """Get strategy configuration from Django"""
        response = await self.session.get(
            f"{self.django_api_url}/api/strategies/{strategy_id}/"
        )
        return response.json()
```

## Data Flow Architecture

### 1. Real-time Trading Flow
```
Market Data → FastAPI Market Data Service → Redis Pub/Sub → FastAPI Engine
                                                                    ↓
Risk Validation ← Portfolio Updates ← Strategy Execution ← Market Analysis
        ↓                    ↓                 ↓
Django Dashboard ← Redis Pub/Sub ← Order Execution → Exchange APIs
```

### 2. Configuration Management Flow
```
User (Django Admin) → Django Models → Database → FastAPI Config Service
                                                        ↓
                                             Strategy Engine Updates
```

### 3. Monitoring and Analytics Flow
```
FastAPI Metrics → Redis/Database → Django Analytics → HTMX Dashboard
                                                            ↓
                                                   Real-time Updates
```

## Technology Stack Comparison

### Current vs Proposed

| Component | Current | Proposed Django | Proposed FastAPI |
|-----------|---------|-----------------|------------------|
| **Web Framework** | Dash (React) | Django + HTMX | FastAPI |
| **Frontend** | React/Plotly | HTMX + TailwindCSS | API endpoints |
| **Database** | In-memory | PostgreSQL + TimescaleDB | SQLAlchemy async |
| **Real-time** | Async loops | Django Channels | WebSocket + Pub/Sub |
| **Configuration** | YAML files | Django admin + DB | FastAPI dependencies |
| **API** | Internal only | Django REST Framework | FastAPI with docs |
| **Authentication** | None | Django auth + JWT | FastAPI security |
| **Background Tasks** | Async tasks | Celery | FastAPI BackgroundTasks |

## Performance Characteristics

### FastAPI Engine Benefits
- **High Throughput**: ~60,000 requests/second for trading operations
- **Low Latency**: <1ms for order processing with async I/O
- **Automatic Documentation**: OpenAPI/Swagger for all endpoints
- **Type Safety**: Pydantic validation with automatic error responses
- **WebSocket Support**: Native async WebSocket for real-time data

### Django Web App Benefits
- **Rich Admin Interface**: Built-in admin for configuration management
- **User Management**: Complete authentication and authorization system
- **Template System**: Server-side rendering with HTMX for interactivity
- **ORM**: Mature ORM with complex query capabilities
- **Ecosystem**: Vast ecosystem of packages and integrations

## Migration Strategy

### Phase 1: Infrastructure (2 weeks)
- Set up PostgreSQL + TimescaleDB
- Create shared database models
- Implement message queue infrastructure
- Set up development environment

### Phase 2: FastAPI Engine (3 weeks)
- Migrate core trading engine to FastAPI
- Implement strategy execution service
- Create order management endpoints
- Set up real-time WebSocket APIs

### Phase 3: FastAPI Market Data (2 weeks)
- Migrate exchange API clients to FastAPI service
- Implement market data WebSocket hub
- Create data normalization pipeline
- Set up caching and rate limiting

### Phase 4: Django Web App (3 weeks)
- Create Django project structure
- Implement user authentication
- Build configuration management interface
- Create HTMX dashboard components

### Phase 5: Integration & Testing (2 weeks)
- Inter-service communication testing
- End-to-end integration testing
- Performance benchmarking
- Security testing

## Benefits of Hybrid Architecture

### 1. Performance Optimization
- **Trading Engine**: FastAPI's async performance for time-critical operations
- **Web Interface**: Django's mature ecosystem for complex web features
- **Scalability**: Independent scaling of trading vs web components

### 2. Development Experience
- **Type Safety**: Pydantic models shared between services
- **API Documentation**: Automatic OpenAPI docs for FastAPI services
- **Admin Interface**: Django admin for easy configuration management
- **Testing**: Specialized testing approaches for each service type

### 3. Operational Benefits
- **Deployment Flexibility**: Independent deployment and scaling
- **Monitoring**: Service-specific monitoring and alerting
- **Fault Isolation**: Failures in one service don't affect others
- **Technology Evolution**: Can upgrade or replace services independently

This hybrid architecture provides the best of both worlds: FastAPI's performance for trading operations and Django's maturity for web application features.