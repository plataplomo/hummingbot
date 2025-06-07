# Django + FastAPI Refactor: Implementation Guide

## Overview

This guide provides practical implementation details for the Django + FastAPI + HTMX refactor, including code examples, configuration files, and deployment instructions.

## Project Structure

```
django_fastapi_cyberdelta/
├── django_web/                    # Django web application
│   ├── manage.py
│   ├── config/
│   │   ├── settings/
│   │   │   ├── base.py
│   │   │   ├── development.py
│   │   │   ├── production.py
│   │   │   └── testing.py
│   │   ├── urls.py
│   │   ├── wsgi.py
│   │   └── asgi.py
│   ├── apps/
│   │   ├── accounts/
│   │   ├── dashboard/
│   │   ├── configuration/
│   │   ├── analytics/
│   │   ├── monitoring/
│   │   └── api/
│   ├── templates/
│   ├── static/
│   └── requirements/
├── fastapi_engine/                # FastAPI trading engine
│   ├── main.py
│   ├── routers/
│   ├── services/
│   ├── models/
│   ├── dependencies/
│   └── requirements.txt
├── fastapi_market_data/           # FastAPI market data service
│   ├── main.py
│   ├── routers/
│   ├── services/
│   └── requirements.txt
├── shared/                        # Shared utilities
│   ├── database/
│   ├── messaging/
│   ├── monitoring/
│   └── testing/
├── docker/                        # Docker configurations
├── scripts/                       # Migration and deployment scripts
├── tests/                         # Integration tests
└── docs/                          # Documentation
```

## Configuration Management

### Django Settings

```python
# django_web/config/settings/base.py
import os
from pathlib import Path
from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent.parent

def get_env_variable(var_name, default=None):
    """Get environment variable or raise exception"""
    try:
        return os.environ[var_name]
    except KeyError:
        if default is not None:
            return default
        error_msg = f"Set the {var_name} environment variable"
        raise ImproperlyConfigured(error_msg)

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': get_env_variable('POSTGRES_DB', 'cyberdelta'),
        'USER': get_env_variable('POSTGRES_USER', 'cyberdelta'),
        'PASSWORD': get_env_variable('POSTGRES_PASSWORD'),
        'HOST': get_env_variable('POSTGRES_HOST', 'localhost'),
        'PORT': get_env_variable('POSTGRES_PORT', '5432'),
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES'",
        },
    }
}

# Redis
REDIS_URL = get_env_variable('REDIS_URL', 'redis://localhost:6379/0')

# Caches
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': REDIS_URL,
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
        }
    }
}

# Channels
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [REDIS_URL],
        },
    },
}

# Django Apps
DJANGO_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

THIRD_PARTY_APPS = [
    'rest_framework',
    'rest_framework.authtoken',
    'django_filters',
    'corsheaders',
    'channels',
    'django_extensions',
    'django_celery_beat',
    'django_celery_results',
]

LOCAL_APPS = [
    'apps.accounts',
    'apps.dashboard',
    'apps.configuration',
    'apps.analytics',
    'apps.monitoring',
    'apps.api',
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# Middleware
MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'apps.monitoring.middleware.PerformanceMiddleware',
]

ROOT_URLCONF = 'config.urls'

# Templates
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'apps.dashboard.context_processors.dashboard_context',
            ],
        },
    },
]

# Static files
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [BASE_DIR / 'static']
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# REST Framework
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.TokenAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 50,
    'DEFAULT_FILTER_BACKENDS': [
        'django_filters.rest_framework.DjangoFilterBackend',
        'rest_framework.filters.OrderingFilter',
        'rest_framework.filters.SearchFilter',
    ],
}

# Celery
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TASK_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'

# FastAPI Services
FASTAPI_ENGINE_URL = get_env_variable('FASTAPI_ENGINE_URL', 'http://localhost:8001')
FASTAPI_MARKET_DATA_URL = get_env_variable('FASTAPI_MARKET_DATA_URL', 'http://localhost:8002')

# Logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'django.log',
            'formatter': 'verbose',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'apps': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

# Security
SECRET_KEY = get_env_variable('SECRET_KEY')
DEBUG = False
ALLOWED_HOSTS = ['*']  # Configure appropriately for production

# CORS
CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

CORS_ALLOW_CREDENTIALS = True

# WebSocket
ASGI_APPLICATION = 'config.asgi.application'

# Custom settings
TRADING_ENGINE_ENABLED = get_env_variable('TRADING_ENGINE_ENABLED', 'false').lower() == 'true'
MARKET_DATA_COLLECTION_ENABLED = get_env_variable('MARKET_DATA_COLLECTION_ENABLED', 'true').lower() == 'true'
```

```python
# django_web/config/settings/development.py
from .base import *

DEBUG = True

ALLOWED_HOSTS = ['localhost', '127.0.0.1', '0.0.0.0']

# Database
DATABASES['default']['NAME'] = 'cyberdelta_dev'

# Logging - more verbose in development
LOGGING['handlers']['console']['level'] = 'DEBUG'
LOGGING['loggers']['apps']['level'] = 'DEBUG'

# Debug toolbar
if DEBUG:
    INSTALLED_APPS += ['debug_toolbar']
    MIDDLEWARE += ['debug_toolbar.middleware.DebugToolbarMiddleware']
    INTERNAL_IPS = ['127.0.0.1']

# CORS - allow all origins in development
CORS_ALLOW_ALL_ORIGINS = True

# Cache - use dummy cache in development
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.dummy.DummyCache',
    }
}
```

```python
# django_web/config/settings/production.py
from .base import *
import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.integrations.celery import CeleryIntegration

DEBUG = False

ALLOWED_HOSTS = get_env_variable('ALLOWED_HOSTS', '').split(',')

# Security settings
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True
X_FRAME_OPTIONS = 'DENY'

# Session security
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_AGE = 3600  # 1 hour

CSRF_COOKIE_SECURE = True
CSRF_COOKIE_HTTPONLY = True

# Sentry error tracking
sentry_sdk.init(
    dsn=get_env_variable('SENTRY_DSN', ''),
    integrations=[
        DjangoIntegration(),
        CeleryIntegration(),
    ],
    traces_sample_rate=0.1,
    send_default_pii=True
)

# Logging - structured logging for production
LOGGING['formatters']['json'] = {
    'format': '{"level": "%(levelname)s", "time": "%(asctime)s", "module": "%(module)s", "message": "%(message)s"}',
}

LOGGING['handlers']['file']['formatter'] = 'json'
LOGGING['handlers']['console']['formatter'] = 'json'
```

### FastAPI Configuration

```python
# fastapi_engine/config.py
import os
from typing import Optional
from pydantic import BaseSettings, PostgresDsn, validator

class Settings(BaseSettings):
    # Database
    postgres_server: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_user: str = os.getenv("POSTGRES_USER", "cyberdelta")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")
    postgres_db: str = os.getenv("POSTGRES_DB", "cyberdelta")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database_url: Optional[PostgresDsn] = None

    @validator("database_url", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: dict) -> str:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get("postgres_user"),
            password=values.get("postgres_password"),
            host=values.get("postgres_server"),
            port=str(values.get("postgres_port")),
            path=f"/{values.get('postgres_db') or ''}",
        )

    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # FastAPI
    api_title: str = "CyberDelta Trading Engine"
    api_version: str = "1.0.0"
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # External services
    django_api_url: str = os.getenv("DJANGO_API_URL", "http://localhost:8000")
    market_data_service_url: str = os.getenv("MARKET_DATA_SERVICE_URL", "http://localhost:8002")
    
    # Trading settings
    trading_enabled: bool = os.getenv("TRADING_ENABLED", "false").lower() == "true"
    risk_checks_enabled: bool = os.getenv("RISK_CHECKS_ENABLED", "true").lower() == "true"
    max_concurrent_strategies: int = int(os.getenv("MAX_CONCURRENT_STRATEGIES", "10"))
    
    # Monitoring
    metrics_enabled: bool = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    class Config:
        case_sensitive = True

settings = Settings()
```

## Database Models and Migrations

### Shared Database Models

```python
# shared/database/models.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON, Numeric, ForeignKey, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class TimestampMixin:
    """Mixin for timestamp fields"""
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

class Exchange(Base, TimestampMixin):
    __tablename__ = 'exchanges_exchange'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False, index=True)
    display_name = Column(String(100), nullable=False)
    is_active = Column(Boolean, default=True, index=True)
    api_base_url = Column(String(255))
    websocket_url = Column(String(255))
    rate_limit_per_minute = Column(Integer, default=600)
    
    # Relationships
    trading_pairs = relationship("TradingPair", back_populates="exchange")
    accounts = relationship("Account", back_populates="exchange")

class TradingPair(Base, TimestampMixin):
    __tablename__ = 'exchanges_tradingpair'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    base_asset = Column(String(10), nullable=False)
    quote_asset = Column(String(10), nullable=False)
    pair_type = Column(String(20), default='spot', index=True)  # spot, perpetual, future
    is_active = Column(Boolean, default=True, index=True)
    
    # Trading specifications
    min_order_size = Column(Numeric(20, 8))
    max_order_size = Column(Numeric(20, 8))
    price_precision = Column(Integer, default=8)
    quantity_precision = Column(Integer, default=8)
    tick_size = Column(Numeric(20, 8))
    step_size = Column(Numeric(20, 8))
    
    # Relationships
    exchange = relationship("Exchange", back_populates="trading_pairs")
    tickers = relationship("Ticker", back_populates="trading_pair")
    candles = relationship("Candle", back_populates="trading_pair")
    funding_rates = relationship("FundingRate", back_populates="trading_pair")
    
    # Indexes
    __table_args__ = (
        Index('idx_exchange_symbol', 'exchange_id', 'symbol'),
        Index('idx_active_pairs', 'exchange_id', 'is_active'),
    )

class Strategy(Base, TimestampMixin):
    __tablename__ = 'strategies_strategy'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, index=True)
    strategy_type = Column(String(50), nullable=False, index=True)
    description = Column(Text)
    is_active = Column(Boolean, default=False, index=True)
    
    # Configuration
    config = Column(JSON, nullable=False)
    
    # Position limits
    max_position_size = Column(Numeric(20, 8))
    max_daily_loss = Column(Numeric(20, 8))
    risk_limit = Column(Numeric(10, 4))
    
    # Strategy-specific exchanges and symbols
    long_exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'))
    short_exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'))
    long_symbol = Column(String(20))
    short_symbol = Column(String(20))
    
    # Relationships
    long_exchange = relationship("Exchange", foreign_keys=[long_exchange_id])
    short_exchange = relationship("Exchange", foreign_keys=[short_exchange_id])
    orders = relationship("Order", back_populates="strategy")
    signals = relationship("TradeSignal", back_populates="strategy")

# Time-series data models (TimescaleDB hypertables)
class Ticker(Base):
    __tablename__ = 'market_data_ticker'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Price data
    last_price = Column(Numeric(20, 8), nullable=False)
    bid_price = Column(Numeric(20, 8))
    ask_price = Column(Numeric(20, 8))
    
    # Volume data
    volume_24h = Column(Numeric(20, 8))
    quote_volume_24h = Column(Numeric(20, 8))
    
    # Price changes
    price_change_24h = Column(Numeric(10, 4))
    price_change_pct_24h = Column(Numeric(10, 4))
    
    # Additional fields
    open_price_24h = Column(Numeric(20, 8))
    high_price_24h = Column(Numeric(20, 8))
    low_price_24h = Column(Numeric(20, 8))
    
    # Relationships
    trading_pair = relationship("TradingPair", back_populates="tickers")
    
    # Indexes for time-series queries
    __table_args__ = (
        Index('idx_ticker_time_pair', 'timestamp', 'trading_pair_id'),
        Index('idx_ticker_pair_time', 'trading_pair_id', 'timestamp'),
    )

class Candle(Base):
    __tablename__ = 'market_data_candle'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    interval = Column(String(10), nullable=False, index=True)  # 1m, 5m, 15m, 1h, 4h, 1d
    
    # OHLCV data
    open_price = Column(Numeric(20, 8), nullable=False)
    high_price = Column(Numeric(20, 8), nullable=False)
    low_price = Column(Numeric(20, 8), nullable=False)
    close_price = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=False)
    quote_volume = Column(Numeric(20, 8))
    
    # Additional metrics
    trades_count = Column(Integer)
    taker_buy_volume = Column(Numeric(20, 8))
    taker_buy_quote_volume = Column(Numeric(20, 8))
    
    # Relationships
    trading_pair = relationship("TradingPair", back_populates="candles")
    
    # Indexes
    __table_args__ = (
        Index('idx_candle_time_pair_interval', 'timestamp', 'trading_pair_id', 'interval'),
        Index('idx_candle_pair_interval_time', 'trading_pair_id', 'interval', 'timestamp'),
    )

class FundingRate(Base):
    __tablename__ = 'market_data_fundingrate'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Funding rate data
    funding_rate = Column(Numeric(10, 8), nullable=False)
    predicted_rate = Column(Numeric(10, 8))
    next_funding_time = Column(DateTime)
    
    # Additional metrics
    mark_price = Column(Numeric(20, 8))
    index_price = Column(Numeric(20, 8))
    open_interest = Column(Numeric(20, 8))
    
    # Relationships
    trading_pair = relationship("TradingPair", back_populates="funding_rates")
    
    # Indexes
    __table_args__ = (
        Index('idx_funding_time_pair', 'timestamp', 'trading_pair_id'),
        Index('idx_funding_pair_time', 'trading_pair_id', 'timestamp'),
    )

class Order(Base, TimestampMixin):
    __tablename__ = 'trading_order'
    
    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey('strategies_strategy.id'), nullable=False, index=True)
    account_id = Column(Integer, ForeignKey('trading_account.id'), nullable=False, index=True)
    
    # Order details
    exchange_order_id = Column(String(100), index=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'), nullable=False)
    side = Column(String(10), nullable=False)  # buy, sell
    order_type = Column(String(20), nullable=False)  # market, limit, stop_loss, etc.
    status = Column(String(20), nullable=False, index=True)  # pending, filled, canceled, etc.
    
    # Quantities and prices
    quantity = Column(Numeric(20, 8), nullable=False)
    filled_quantity = Column(Numeric(20, 8), default=0)
    price = Column(Numeric(20, 8))  # limit price
    average_fill_price = Column(Numeric(20, 8))
    
    # Timestamps
    submitted_at = Column(DateTime, default=func.now())
    filled_at = Column(DateTime)
    canceled_at = Column(DateTime)
    
    # Fees
    fee = Column(Numeric(20, 8))
    fee_currency = Column(String(10))
    
    # Relationships
    strategy = relationship("Strategy", back_populates="orders")
    account = relationship("Account", back_populates="orders")
    trading_pair = relationship("TradingPair")
    trades = relationship("Trade", back_populates="order")

class Trade(Base, TimestampMixin):
    __tablename__ = 'trading_trade'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('trading_order.id'), nullable=False, index=True)
    
    # Trade details
    exchange_trade_id = Column(String(100), index=True)
    price = Column(Numeric(20, 8), nullable=False)
    quantity = Column(Numeric(20, 8), nullable=False)
    side = Column(String(10), nullable=False)
    
    # Fees
    fee = Column(Numeric(20, 8))
    fee_currency = Column(String(10))
    
    # PnL (calculated)
    realized_pnl = Column(Numeric(20, 8))
    
    # Timestamps
    executed_at = Column(DateTime, nullable=False, index=True)
    
    # Relationships
    order = relationship("Order", back_populates="trades")

class TradeSignal(Base, TimestampMixin):
    __tablename__ = 'trading_signal'
    
    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey('strategies_strategy.id'), nullable=False, index=True)
    
    # Signal details
    signal_type = Column(String(50), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)
    side = Column(String(10), nullable=False)  # buy, sell
    quantity = Column(Numeric(20, 8))
    price = Column(Numeric(20, 8))
    
    # Signal metadata
    confidence = Column(Numeric(5, 4))  # 0.0 to 1.0
    metadata = Column(JSON)
    
    # Processing status
    status = Column(String(20), default='pending', index=True)  # pending, processed, rejected
    processed_at = Column(DateTime)
    rejection_reason = Column(Text)
    
    # Relationships
    strategy = relationship("Strategy", back_populates="signals")

class Account(Base, TimestampMixin):
    __tablename__ = 'trading_account'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'), nullable=False, index=True)
    
    # Account details
    account_type = Column(String(20), nullable=False)  # spot, margin, futures
    is_active = Column(Boolean, default=True, index=True)
    
    # API credentials (encrypted)
    api_key_encrypted = Column(Text)
    api_secret_encrypted = Column(Text)
    passphrase_encrypted = Column(Text)
    
    # Account limits
    max_order_value = Column(Numeric(20, 8))
    daily_trade_limit = Column(Numeric(20, 8))
    
    # Relationships
    exchange = relationship("Exchange", back_populates="accounts")
    orders = relationship("Order", back_populates="account")
    balances = relationship("Balance", back_populates="account")

class Balance(Base, TimestampMixin):
    __tablename__ = 'trading_balance'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('trading_account.id'), nullable=False, index=True)
    
    # Balance details
    asset = Column(String(10), nullable=False, index=True)
    free_balance = Column(Numeric(20, 8), nullable=False)
    locked_balance = Column(Numeric(20, 8), default=0)
    total_balance = Column(Numeric(20, 8), nullable=False)
    
    # USD value (calculated)
    usd_value = Column(Numeric(20, 8))
    
    # Timestamp for balance snapshot
    snapshot_time = Column(DateTime, nullable=False, index=True)
    
    # Relationships
    account = relationship("Account", back_populates="balances")
    
    # Indexes
    __table_args__ = (
        Index('idx_balance_account_asset', 'account_id', 'asset'),
        Index('idx_balance_time', 'snapshot_time'),
    )
```

### Django Models

```python
# django_web/apps/exchanges/models.py
from django.db import models
from django.utils import timezone
from django.core.validators import MinValueValidator, MaxValueValidator

class Exchange(models.Model):
    name = models.CharField(max_length=50, unique=True, db_index=True)
    display_name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True, db_index=True)
    api_base_url = models.URLField(blank=True)
    websocket_url = models.URLField(blank=True)
    rate_limit_per_minute = models.IntegerField(default=600)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'exchanges_exchange'
        ordering = ['display_name']
    
    def __str__(self):
        return self.display_name
    
    @property
    def active_trading_pairs_count(self):
        return self.trading_pairs.filter(is_active=True).count()

class TradingPair(models.Model):
    PAIR_TYPES = [
        ('spot', 'Spot'),
        ('perpetual', 'Perpetual'),
        ('future', 'Future'),
    ]
    
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='trading_pairs')
    symbol = models.CharField(max_length=20, db_index=True)
    base_asset = models.CharField(max_length=10)
    quote_asset = models.CharField(max_length=10)
    pair_type = models.CharField(max_length=20, choices=PAIR_TYPES, default='spot', db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    
    # Trading specifications
    min_order_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    max_order_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    price_precision = models.IntegerField(default=8)
    quantity_precision = models.IntegerField(default=8)
    tick_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    step_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'exchanges_tradingpair'
        unique_together = ['exchange', 'symbol']
        indexes = [
            models.Index(fields=['exchange', 'symbol']),
            models.Index(fields=['exchange', 'is_active']),
        ]
    
    def __str__(self):
        return f"{self.exchange.name}:{self.symbol}"
    
    @property
    def full_name(self):
        return f"{self.exchange.display_name} {self.symbol}"

# django_web/apps/strategies/models.py
class Strategy(models.Model):
    STRATEGY_TYPES = [
        ('funding_rate_arbitrage', 'Funding Rate Arbitrage'),
        ('grid_trading', 'Grid Trading'),
        ('dca', 'Dollar Cost Averaging'),
        ('momentum', 'Momentum Trading'),
    ]
    
    STATUS_CHOICES = [
        ('inactive', 'Inactive'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('error', 'Error'),
    ]
    
    name = models.CharField(max_length=100, db_index=True)
    strategy_type = models.CharField(max_length=50, choices=STRATEGY_TYPES, db_index=True)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=False, db_index=True)
    
    # Configuration
    config = models.JSONField(default=dict)
    
    # Position limits
    max_position_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    max_daily_loss = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    risk_limit = models.DecimalField(
        max_digits=10, 
        decimal_places=4, 
        validators=[MinValueValidator(0), MaxValueValidator(1)],
        help_text="Risk limit as percentage (0.0 to 1.0)"
    )
    
    # Strategy-specific exchanges and symbols
    long_exchange = models.ForeignKey(
        'exchanges.Exchange', 
        on_delete=models.CASCADE, 
        related_name='long_strategies',
        null=True, blank=True
    )
    short_exchange = models.ForeignKey(
        'exchanges.Exchange', 
        on_delete=models.CASCADE, 
        related_name='short_strategies',
        null=True, blank=True
    )
    long_symbol = models.CharField(max_length=20, blank=True)
    short_symbol = models.CharField(max_length=20, blank=True)
    
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'strategies_strategy'
        ordering = ['-created_at']
    
    def __str__(self):
        return self.name
    
    @property
    def status(self):
        if not self.is_active:
            return 'inactive'
        # Additional logic to determine status based on recent activity
        return 'active'
    
    def get_performance_metrics(self, days=30):
        """Get strategy performance metrics for the last N days"""
        from django.utils import timezone
        from datetime import timedelta
        
        end_date = timezone.now()
        start_date = end_date - timedelta(days=days)
        
        # Get orders and trades for this strategy
        orders = self.orders.filter(
            created_at__gte=start_date,
            status='filled'
        )
        
        total_trades = orders.count()
        if total_trades == 0:
            return {
                'total_trades': 0,
                'total_pnl': 0,
                'win_rate': 0,
                'avg_trade_pnl': 0,
            }
        
        # Calculate metrics
        trades_data = orders.aggregate(
            total_pnl=models.Sum('trades__realized_pnl'),
            avg_pnl=models.Avg('trades__realized_pnl')
        )
        
        winning_trades = orders.filter(trades__realized_pnl__gt=0).count()
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        return {
            'total_trades': total_trades,
            'total_pnl': trades_data['total_pnl'] or 0,
            'win_rate': win_rate,
            'avg_trade_pnl': trades_data['avg_pnl'] or 0,
        }
```

This implementation guide provides the foundational code structure, configuration management, and database models needed for the Django + FastAPI + HTMX refactor. The next sections would continue with service implementations, API endpoints, and deployment configurations.