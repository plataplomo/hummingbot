# Django + HTMX Frontend Architecture

## Overview

This document details the frontend architecture using Django + HTMX + TailwindCSS, replacing the current Dash/React dashboard with a more maintainable, performant solution that eliminates build complexity while preserving all functionality.

## Current Dashboard Analysis

### Existing Dash Implementation
```python
# Current: cyberdelta/monitoring/real_time_dashboard.py
class RealTimeDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_callbacks(self):
        @self.app.callback(
            Output('performance-chart', 'figure'),
            [Input('strategy-dropdown', 'value'),
             Input('interval-component', 'n_intervals')]
        )
        def update_performance_chart(selected_strategies, n):
            # Complex callback logic
            return create_plotly_figure(selected_strategies)
```

### Current Dashboard Features
- Real-time performance monitoring with Plotly charts
- Strategy selection and filtering
- Funding rate heatmaps
- Trade analysis tables with pagination
- Portfolio overview and risk metrics
- Real-time WebSocket updates
- Drawdown visualization
- Alert notifications

### Pain Points
- **React Dependency**: 200MB+ bundle size with React ecosystem
- **Build Complexity**: Hidden webpack/babel configuration
- **Type Safety Issues**: No proper TypeScript integration
- **Performance Overhead**: React virtual DOM for simple interactions
- **Limited Customization**: Constrained by Dash component library
- **Debugging Difficulty**: Complex callback chains hard to debug

## Proposed Django + HTMX Architecture

### Technology Stack
```
Frontend Stack:
├── HTMX (14KB) - HTML over the wire
├── TailwindCSS - Utility-first CSS framework
├── Alpine.js (15KB) - Minimal client-side reactivity
├── Plotly.js - Charts and visualizations (unchanged)
└── Django Templates - Server-side rendering
```

### Project Structure
```
django_cyberdelta/
├── apps/
│   ├── dashboard/
│   │   ├── views.py              # HTMX view handlers
│   │   ├── urls.py               # URL routing
│   │   ├── models.py             # Dashboard-specific models
│   │   ├── services.py           # Business logic services
│   │   ├── templates/
│   │   │   ├── dashboard/
│   │   │   │   ├── base.html     # Base layout
│   │   │   │   ├── index.html    # Main dashboard
│   │   │   │   ├── components/   # Reusable components
│   │   │   │   │   ├── performance_chart.html
│   │   │   │   │   ├── metrics_table.html
│   │   │   │   │   ├── strategy_selector.html
│   │   │   │   │   ├── funding_heatmap.html
│   │   │   │   │   ├── trade_table.html
│   │   │   │   │   ├── portfolio_overview.html
│   │   │   │   │   └── risk_monitor.html
│   │   │   │   ├── partials/     # HTMX partial updates
│   │   │   │   │   ├── chart_data.html
│   │   │   │   │   ├── metrics_row.html
│   │   │   │   │   ├── trade_row.html
│   │   │   │   │   └── alert_toast.html
│   │   │   │   └── layouts/      # Different layout options
│   │   │   │       ├── desktop.html
│   │   │   │       └── mobile.html
│   │   ├── static/
│   │   │   ├── css/
│   │   │   │   ├── dashboard.css # Custom styles
│   │   │   │   └── components.css # Component-specific styles
│   │   │   ├── js/
│   │   │   │   ├── dashboard.js  # Dashboard-specific logic
│   │   │   │   ├── websocket.js  # WebSocket handling
│   │   │   │   └── charts.js     # Chart utilities
│   │   │   └── img/
│   │   ├── websockets.py         # Django Channels consumers
│   │   ├── templatetags/         # Custom template tags
│   │   └── tests/
│   ├── accounts/                 # User management
│   ├── api/                      # Internal REST APIs
│   └── notifications/            # Alert system
├── static/                       # Global static files
│   ├── css/
│   │   └── tailwind.css         # TailwindCSS output
│   ├── js/
│   │   ├── htmx.min.js          # HTMX library
│   │   ├── alpine.min.js        # Alpine.js
│   │   └── plotly.min.js        # Plotly for charts
│   └── fonts/
└── templates/
    ├── base.html                # Global base template
    └── partials/                # Global partials
```

## Core HTMX Implementation

### 1. Base Template with HTMX Setup

```html
<!-- templates/base.html -->
<!DOCTYPE html>
<html lang="en" class="h-full bg-gray-50">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}CyberDelta Dashboard{% endblock %}</title>
    
    <!-- TailwindCSS -->
    <link href="{% static 'css/tailwind.css' %}" rel="stylesheet">
    
    <!-- Core Libraries -->
    <script src="{% static 'js/htmx.min.js' %}"></script>
    <script src="{% static 'js/alpine.min.js' %}" defer></script>
    <script src="{% static 'js/plotly.min.js' %}"></script>
    
    <!-- HTMX Extensions -->
    <script src="https://unpkg.com/htmx.org/dist/ext/ws.js"></script>
    
    <!-- Custom Styles -->
    <link href="{% static 'css/dashboard.css' %}" rel="stylesheet">
    
    <!-- CSRF Token for HTMX -->
    <meta name="csrf-token" content="{{ csrf_token }}">
</head>
<body class="h-full" hx-headers='{"X-CSRFToken": "{{ csrf_token }}"}'>
    
    <!-- Main Container with WebSocket Connection -->
    <div id="main-app" 
         class="h-full flex flex-col"
         hx-ext="ws" 
         ws-connect="/ws/dashboard/"
         x-data="dashboardApp()">
        
        <!-- Navigation -->
        <nav class="bg-gray-900 text-white shadow-lg">
            <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div class="flex justify-between h-16">
                    <div class="flex items-center">
                        <div class="flex-shrink-0">
                            <h1 class="text-xl font-bold">CyberDelta</h1>
                        </div>
                        <div class="hidden md:block">
                            <div class="ml-10 flex items-baseline space-x-4">
                                <a href="#" 
                                   class="nav-link"
                                   hx-get="{% url 'dashboard:overview' %}"
                                   hx-target="#main-content"
                                   hx-push-url="true">Overview</a>
                                <a href="#" 
                                   class="nav-link"
                                   hx-get="{% url 'dashboard:strategies' %}"
                                   hx-target="#main-content"
                                   hx-push-url="true">Strategies</a>
                                <a href="#" 
                                   class="nav-link"
                                   hx-get="{% url 'dashboard:trading' %}"
                                   hx-target="#main-content"
                                   hx-push-url="true">Trading</a>
                                <a href="#" 
                                   class="nav-link"
                                   hx-get="{% url 'dashboard:analytics' %}"
                                   hx-target="#main-content"
                                   hx-push-url="true">Analytics</a>
                            </div>
                        </div>
                    </div>
                    
                    <!-- User Menu -->
                    <div class="flex items-center">
                        <div class="relative" x-data="{ open: false }">
                            <button @click="open = !open" 
                                    class="flex items-center text-sm rounded-full focus:outline-none">
                                <span class="sr-only">Open user menu</span>
                                <img class="h-8 w-8 rounded-full" 
                                     src="{% static 'img/avatar.png' %}" 
                                     alt="User avatar">
                            </button>
                            <!-- Dropdown menu -->
                            <div x-show="open" 
                                 @click.away="open = false"
                                 class="origin-top-right absolute right-0 mt-2 w-48 rounded-md shadow-lg">
                                <div class="py-1 rounded-md bg-white shadow-xs">
                                    <a href="#" class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">Settings</a>
                                    <a href="{% url 'accounts:logout' %}" class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">Logout</a>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </nav>
        
        <!-- Main Content Area -->
        <main id="main-content" class="flex-1 overflow-y-auto">
            {% block content %}{% endblock %}
        </main>
        
        <!-- Status Bar -->
        <div id="status-bar" class="bg-gray-800 text-white px-4 py-2 flex justify-between items-center text-sm">
            <div id="connection-status" 
                 x-text="connectionStatus"
                 :class="{'text-green-400': connected, 'text-red-400': !connected}">
                Connecting...
            </div>
            <div id="last-update" x-text="'Last update: ' + lastUpdate">
                Never
            </div>
        </div>
    </div>
    
    <!-- Toast Notifications -->
    <div id="toast-container" 
         class="fixed top-4 right-4 z-50 space-y-2"
         x-show="notifications.length > 0">
        <template x-for="notification in notifications" :key="notification.id">
            <div class="toast-notification"
                 :class="notification.type"
                 x-show="notification.visible"
                 x-transition:enter="transform ease-out duration-300 transition"
                 x-transition:enter-start="translate-y-2 opacity-0 sm:translate-y-0 sm:translate-x-2"
                 x-transition:enter-end="translate-y-0 opacity-100 sm:translate-x-0"
                 x-transition:leave="transition ease-in duration-100"
                 x-transition:leave-start="opacity-100"
                 x-transition:leave-end="opacity-0">
                <div class="flex items-start">
                    <div class="flex-1">
                        <p class="text-sm font-medium" x-text="notification.title"></p>
                        <p class="text-sm" x-text="notification.message"></p>
                    </div>
                    <button @click="dismissNotification(notification.id)" 
                            class="ml-4 flex-shrink-0">
                        <span class="sr-only">Close</span>
                        <svg class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd" />
                        </svg>
                    </button>
                </div>
            </div>
        </template>
    </div>
    
    <!-- Global JavaScript -->
    <script src="{% static 'js/dashboard.js' %}"></script>
    
    <!-- Block for page-specific scripts -->
    {% block extra_js %}{% endblock %}
</body>
</html>
```

### 2. Alpine.js State Management

```javascript
// static/js/dashboard.js
function dashboardApp() {
    return {
        // Connection state
        connected: false,
        connectionStatus: 'Connecting...',
        lastUpdate: 'Never',
        
        // Notifications
        notifications: [],
        notificationId: 0,
        
        // Dashboard data
        strategies: {},
        metrics: {},
        realtimeData: {},
        
        init() {
            this.setupWebSocket();
            this.setupHTMXEventHandlers();
            console.log('Dashboard app initialized');
        },
        
        setupWebSocket() {
            // Handle WebSocket connection status
            document.body.addEventListener('htmx:wsOpen', (event) => {
                this.connected = true;
                this.connectionStatus = 'Connected';
                this.addNotification('success', 'Connected', 'WebSocket connection established');
            });
            
            document.body.addEventListener('htmx:wsClose', (event) => {
                this.connected = false;
                this.connectionStatus = 'Disconnected';
                this.addNotification('error', 'Disconnected', 'Lost connection to server');
            });
            
            document.body.addEventListener('htmx:wsError', (event) => {
                this.connected = false;
                this.connectionStatus = 'Error';
                this.addNotification('error', 'Connection Error', 'WebSocket connection failed');
            });
            
            // Handle incoming messages
            document.body.addEventListener('htmx:wsAfterMessage', (event) => {
                this.handleWebSocketMessage(event.detail);
            });
        },
        
        setupHTMXEventHandlers() {
            // Handle HTMX request start/end for loading indicators
            document.body.addEventListener('htmx:beforeRequest', (event) => {
                this.showLoadingIndicator(event.target);
            });
            
            document.body.addEventListener('htmx:afterRequest', (event) => {
                this.hideLoadingIndicator(event.target);
                this.lastUpdate = new Date().toLocaleTimeString();
            });
            
            // Handle HTMX errors
            document.body.addEventListener('htmx:responseError', (event) => {
                this.addNotification('error', 'Request Failed', 
                    `Failed to load ${event.detail.xhr.responseURL}`);
            });
        },
        
        handleWebSocketMessage(detail) {
            try {
                const data = JSON.parse(detail.message);
                
                switch(data.type) {
                    case 'strategy_update':
                        this.updateStrategyData(data.data);
                        break;
                    case 'metrics_update':
                        this.updateMetrics(data.data);
                        break;
                    case 'trade_executed':
                        this.handleTradeUpdate(data.data);
                        break;
                    case 'risk_alert':
                        this.handleRiskAlert(data.data);
                        break;
                    case 'market_data_update':
                        this.updateMarketData(data.data);
                        break;
                }
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        },
        
        updateStrategyData(strategyData) {
            this.strategies[strategyData.id] = strategyData;
            
            // Update strategy performance displays
            const strategyElements = document.querySelectorAll(`[data-strategy-id="${strategyData.id}"]`);
            strategyElements.forEach(element => {
                this.updateElementContent(element, strategyData);
            });
        },
        
        updateMetrics(metricsData) {
            this.metrics = { ...this.metrics, ...metricsData };
            
            // Trigger HTMX update for metrics table
            htmx.ajax('GET', '/dashboard/components/metrics-table/', {
                target: '#metrics-table-container',
                swap: 'innerHTML'
            });
        },
        
        handleTradeUpdate(tradeData) {
            this.addNotification('info', 'Trade Executed', 
                `${tradeData.side} ${tradeData.quantity} ${tradeData.symbol} at $${tradeData.price}`);
            
            // Add new trade row to trade table if visible
            const tradeTable = document.getElementById('trade-table-body');
            if (tradeTable) {
                htmx.ajax('GET', `/dashboard/components/trade-row/?trade_id=${tradeData.id}`, {
                    target: '#trade-table-body',
                    swap: 'afterbegin'
                });
            }
        },
        
        handleRiskAlert(alertData) {
            this.addNotification('warning', 'Risk Alert', alertData.message);
        },
        
        updateMarketData(marketData) {
            this.realtimeData = { ...this.realtimeData, ...marketData };
            
            // Update price displays
            Object.keys(marketData).forEach(symbol => {
                const priceElements = document.querySelectorAll(`[data-symbol="${symbol}"] .price`);
                priceElements.forEach(element => {
                    element.textContent = marketData[symbol].price;
                    element.classList.add('price-updated');
                    setTimeout(() => element.classList.remove('price-updated'), 1000);
                });
            });
        },
        
        addNotification(type, title, message) {
            const notification = {
                id: ++this.notificationId,
                type: type,
                title: title,
                message: message,
                visible: true
            };
            
            this.notifications.unshift(notification);
            
            // Auto-dismiss after 5 seconds
            setTimeout(() => {
                this.dismissNotification(notification.id);
            }, 5000);
            
            // Keep only last 10 notifications
            if (this.notifications.length > 10) {
                this.notifications = this.notifications.slice(0, 10);
            }
        },
        
        dismissNotification(id) {
            const notification = this.notifications.find(n => n.id === id);
            if (notification) {
                notification.visible = false;
                // Remove after transition
                setTimeout(() => {
                    this.notifications = this.notifications.filter(n => n.id !== id);
                }, 300);
            }
        },
        
        showLoadingIndicator(element) {
            const indicator = element.querySelector('.loading-indicator');
            if (indicator) {
                indicator.classList.remove('hidden');
            }
        },
        
        hideLoadingIndicator(element) {
            const indicator = element.querySelector('.loading-indicator');
            if (indicator) {
                indicator.classList.add('hidden');
            }
        },
        
        updateElementContent(element, data) {
            // Update data attributes and content based on element type
            const contentElements = element.querySelectorAll('[data-field]');
            contentElements.forEach(contentElement => {
                const field = contentElement.getAttribute('data-field');
                if (data[field] !== undefined) {
                    contentElement.textContent = data[field];
                }
            });
        }
    }
}

// Global utility functions
window.dashboardUtils = {
    formatCurrency: (amount, currency = 'USD') => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: currency
        }).format(amount);
    },
    
    formatPercentage: (value, decimals = 2) => {
        return (value * 100).toFixed(decimals) + '%';
    },
    
    formatNumber: (value, decimals = 2) => {
        return parseFloat(value).toFixed(decimals);
    },
    
    timeAgo: (timestamp) => {
        const now = new Date();
        const time = new Date(timestamp);
        const diffInSeconds = Math.floor((now - time) / 1000);
        
        if (diffInSeconds < 60) return `${diffInSeconds}s ago`;
        if (diffInSeconds < 3600) return `${Math.floor(diffInSeconds / 60)}m ago`;
        if (diffInSeconds < 86400) return `${Math.floor(diffInSeconds / 3600)}h ago`;
        return `${Math.floor(diffInSeconds / 86400)}d ago`;
    }
};
```

### 3. Performance Chart Component with HTMX

```python
# apps/dashboard/views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.views.generic import TemplateView
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
import json
import plotly.graph_objects as go
import plotly.utils

class PerformanceChartView(TemplateView):
    template_name = 'dashboard/components/performance_chart.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get parameters from request
        strategies = self.request.GET.getlist('strategies')
        time_range = self.request.GET.get('time_range', '24h')
        
        # Get performance data
        performance_data = self.get_performance_data(strategies, time_range)
        
        # Create Plotly figure
        fig = self.create_performance_figure(performance_data)
        
        context.update({
            'chart_json': json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder),
            'selected_strategies': strategies,
            'time_range': time_range,
            'available_strategies': self.get_available_strategies(),
            'time_range_options': [
                {'value': '1h', 'label': '1 Hour'},
                {'value': '6h', 'label': '6 Hours'},
                {'value': '24h', 'label': '24 Hours'},
                {'value': '7d', 'label': '7 Days'},
                {'value': '30d', 'label': '30 Days'},
            ]
        })
        
        return context
    
    def get_performance_data(self, strategies, time_range):
        """Get strategy performance data from FastAPI service"""
        from .services import PerformanceService
        
        service = PerformanceService()
        return service.get_strategy_performance(strategies, time_range)
    
    def create_performance_figure(self, performance_data):
        """Create Plotly figure for performance chart"""
        fig = go.Figure()
        
        for strategy_data in performance_data:
            fig.add_trace(go.Scatter(
                x=strategy_data['timestamps'],
                y=strategy_data['cumulative_returns'],
                mode='lines',
                name=strategy_data['name'],
                line=dict(width=2),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                            'Time: %{x}<br>' +
                            'Return: %{y:.2%}<br>' +
                            '<extra></extra>'
            ))
        
        fig.update_layout(
            title={
                'text': 'Strategy Performance',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='Time',
            yaxis_title='Cumulative Return',
            yaxis_tickformat='.2%',
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=60, r=60, t=80, b=60),
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
    
    def get_available_strategies(self):
        """Get list of available strategies"""
        from .services import StrategyService
        
        service = StrategyService()
        return service.get_active_strategies()
```

```html
<!-- apps/dashboard/templates/dashboard/components/performance_chart.html -->
<div id="performance-chart-container" class="bg-white rounded-lg shadow-lg p-6">
    <!-- Chart Controls -->
    <div class="flex flex-wrap items-center justify-between mb-4 space-y-2 sm:space-y-0">
        <div class="flex items-center space-x-4">
            <!-- Strategy Selector -->
            <div class="relative">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                    Strategies
                </label>
                <select name="strategies" 
                        multiple
                        class="form-multiselect block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                        hx-get="{% url 'dashboard:performance_chart' %}"
                        hx-target="#performance-chart-container"
                        hx-trigger="change"
                        hx-include="[name='time_range']"
                        hx-indicator="#chart-loading">
                    {% for strategy in available_strategies %}
                        <option value="{{ strategy.id }}" 
                                {% if strategy.id|stringformat:"s" in selected_strategies %}selected{% endif %}>
                            {{ strategy.name }}
                        </option>
                    {% endfor %}
                </select>
            </div>
            
            <!-- Time Range Selector -->
            <div class="relative">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                    Time Range
                </label>
                <div class="flex rounded-md shadow-sm" role="group">
                    {% for option in time_range_options %}
                        <button type="button"
                                class="btn-group {% if option.value == time_range %}btn-active{% endif %}"
                                hx-get="{% url 'dashboard:performance_chart' %}"
                                hx-target="#performance-chart-container"
                                hx-include="[name='strategies']"
                                hx-vals='{"time_range": "{{ option.value }}"}'
                                hx-indicator="#chart-loading">
                            {{ option.label }}
                        </button>
                    {% endfor %}
                </div>
            </div>
        </div>
        
        <!-- Chart Actions -->
        <div class="flex items-center space-x-2">
            <button class="btn btn-sm btn-outline"
                    hx-get="{% url 'dashboard:export_performance' %}"
                    hx-include="[name='strategies'], [name='time_range']">
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                Export
            </button>
            <button class="btn btn-sm btn-outline"
                    hx-get="{% url 'dashboard:performance_chart' %}"
                    hx-target="#performance-chart-container"
                    hx-include="[name='strategies'], [name='time_range']">
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Refresh
            </button>
        </div>
    </div>
    
    <!-- Loading Indicator -->
    <div id="chart-loading" class="htmx-indicator flex items-center justify-center py-8">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
        <span class="ml-2 text-sm text-gray-600">Updating chart...</span>
    </div>
    
    <!-- Chart Container -->
    <div id="performance-chart" class="relative" style="min-height: 400px;">
        <!-- Plotly chart will be rendered here -->
    </div>
    
    <!-- Chart Statistics -->
    <div class="mt-4 grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div class="text-center p-3 bg-gray-50 rounded-lg">
            <div class="text-lg font-semibold text-gray-900" id="total-return">
                {% if performance_data %}{{ performance_data.total_return|floatformat:2 }}%{% else %}--{% endif %}
            </div>
            <div class="text-sm text-gray-500">Total Return</div>
        </div>
        <div class="text-center p-3 bg-gray-50 rounded-lg">
            <div class="text-lg font-semibold text-gray-900" id="sharpe-ratio">
                {% if performance_data %}{{ performance_data.sharpe_ratio|floatformat:2 }}{% else %}--{% endif %}
            </div>
            <div class="text-sm text-gray-500">Sharpe Ratio</div>
        </div>
        <div class="text-center p-3 bg-gray-50 rounded-lg">
            <div class="text-lg font-semibold text-gray-900" id="max-drawdown">
                {% if performance_data %}{{ performance_data.max_drawdown|floatformat:2 }}%{% else %}--{% endif %}
            </div>
            <div class="text-sm text-gray-500">Max Drawdown</div>
        </div>
    </div>
</div>

<script>
    // Render Plotly chart
    document.addEventListener('DOMContentLoaded', function() {
        const chartData = {{ chart_json|safe }};
        
        // Configure responsive behavior
        const config = {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
            toImageButtonOptions: {
                format: 'png',
                filename: 'strategy_performance',
                height: 500,
                width: 1000,
                scale: 1
            }
        };
        
        Plotly.newPlot('performance-chart', chartData.data, chartData.layout, config);
        
        // Handle window resize
        window.addEventListener('resize', function() {
            Plotly.Plots.resize('performance-chart');
        });
    });
</script>
```

### 4. Real-time Metrics Table

```python
# apps/dashboard/views.py
class MetricsTableView(TemplateView):
    template_name = 'dashboard/components/metrics_table.html'
    
    @method_decorator(cache_page(30))  # Cache for 30 seconds
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get current metrics from FastAPI service
        from .services import MetricsService
        service = MetricsService()
        
        metrics = service.get_current_metrics()
        
        context.update({
            'metrics': metrics,
            'last_update': timezone.now(),
            'is_htmx': self.request.headers.get('HX-Request', False)
        })
        
        return context
```

```html
<!-- apps/dashboard/templates/dashboard/components/metrics_table.html -->
<div id="metrics-table-container" 
     class="bg-white rounded-lg shadow-lg overflow-hidden"
     hx-trigger="load, every 10s"
     hx-get="{% url 'dashboard:metrics_table' %}"
     hx-swap="innerHTML">
    
    <div class="px-6 py-4 border-b border-gray-200">
        <h3 class="text-lg font-medium text-gray-900">Strategy Performance</h3>
        <p class="text-sm text-gray-500">
            Last updated: <span x-text="$store.dashboard.lastUpdate">{{ last_update|time:"H:i:s" }}</span>
        </p>
    </div>
    
    <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Strategy
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Status
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        PnL 24h
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Total Return
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Sharpe Ratio
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Max Drawdown
                    </th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Actions
                    </th>
                </tr>
            </thead>
            <tbody id="metrics-table-body" class="bg-white divide-y divide-gray-200">
                {% for metric in metrics %}
                    {% include 'dashboard/partials/metrics_row.html' with metric=metric %}
                {% endfor %}
            </tbody>
        </table>
    </div>
    
    {% if not metrics %}
        <div class="text-center py-8">
            <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            <h3 class="mt-2 text-sm font-medium text-gray-900">No strategies running</h3>
            <p class="mt-1 text-sm text-gray-500">Start a strategy to see performance metrics.</p>
        </div>
    {% endif %}
</div>
```

```html
<!-- apps/dashboard/templates/dashboard/partials/metrics_row.html -->
<tr class="hover:bg-gray-50 transition-colors duration-200" 
    data-strategy-id="{{ metric.strategy.id }}">
    
    <!-- Strategy Name -->
    <td class="px-6 py-4 whitespace-nowrap">
        <div class="flex items-center">
            <div class="flex-shrink-0 h-10 w-10">
                <div class="h-10 w-10 rounded-full bg-indigo-500 flex items-center justify-center">
                    <span class="text-sm font-medium text-white">
                        {{ metric.strategy.name|first|upper }}
                    </span>
                </div>
            </div>
            <div class="ml-4">
                <div class="text-sm font-medium text-gray-900">
                    {{ metric.strategy.name }}
                </div>
                <div class="text-sm text-gray-500">
                    {{ metric.strategy.symbol }}
                </div>
            </div>
        </div>
    </td>
    
    <!-- Status -->
    <td class="px-6 py-4 whitespace-nowrap">
        <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full
                   {% if metric.status == 'active' %}bg-green-100 text-green-800
                   {% elif metric.status == 'paused' %}bg-yellow-100 text-yellow-800
                   {% elif metric.status == 'error' %}bg-red-100 text-red-800
                   {% else %}bg-gray-100 text-gray-800{% endif %}">
            {{ metric.status|title }}
        </span>
    </td>
    
    <!-- PnL 24h -->
    <td class="px-6 py-4 whitespace-nowrap text-sm
              {% if metric.pnl_24h >= 0 %}text-green-600{% else %}text-red-600{% endif %}">
        <span data-field="pnl_24h">
            {% if metric.pnl_24h >= 0 %}+{% endif %}${{ metric.pnl_24h|floatformat:2 }}
        </span>
    </td>
    
    <!-- Total Return -->
    <td class="px-6 py-4 whitespace-nowrap text-sm
              {% if metric.total_return >= 0 %}text-green-600{% else %}text-red-600{% endif %}">
        <span data-field="total_return">
            {% if metric.total_return >= 0 %}+{% endif %}{{ metric.total_return|floatformat:2 }}%
        </span>
    </td>
    
    <!-- Sharpe Ratio -->
    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
        <span data-field="sharpe_ratio">{{ metric.sharpe_ratio|floatformat:2 }}</span>
    </td>
    
    <!-- Max Drawdown -->
    <td class="px-6 py-4 whitespace-nowrap text-sm text-red-600">
        <span data-field="max_drawdown">{{ metric.max_drawdown|floatformat:2 }}%</span>
    </td>
    
    <!-- Actions -->
    <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
        <div class="flex items-center justify-end space-x-2">
            {% if metric.status == 'active' %}
                <button class="text-yellow-600 hover:text-yellow-900"
                        hx-post="{% url 'dashboard:pause_strategy' metric.strategy.id %}"
                        hx-confirm="Are you sure you want to pause this strategy?"
                        hx-target="closest tr"
                        hx-swap="outerHTML">
                    Pause
                </button>
            {% else %}
                <button class="text-green-600 hover:text-green-900"
                        hx-post="{% url 'dashboard:start_strategy' metric.strategy.id %}"
                        hx-confirm="Are you sure you want to start this strategy?"
                        hx-target="closest tr"
                        hx-swap="outerHTML">
                    Start
                </button>
            {% endif %}
            
            <button class="text-indigo-600 hover:text-indigo-900"
                    hx-get="{% url 'dashboard:strategy_details' metric.strategy.id %}"
                    hx-target="#modal-container"
                    hx-trigger="click">
                Details
            </button>
        </div>
    </td>
</tr>
```

This Django + HTMX frontend architecture provides:

1. **Performance**: Server-side rendering with minimal JavaScript
2. **Interactivity**: HTMX for reactive updates without React complexity
3. **Real-time**: WebSocket integration for live data updates
4. **Maintainability**: Standard Django patterns with component-based templates
5. **Type Safety**: Django's robust type system and template safety
6. **Accessibility**: Progressive enhancement with standard HTML forms

The next steps involve implementing the migration timeline and creating additional components for the complete dashboard replacement.