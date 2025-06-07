# Django Refactor: Dashboard HTMX Migration

## Overview

This document outlines the migration from the current Dash-based dashboard to a Django + HTMX solution. The goal is to eliminate the React dependency while maintaining all functionality and improving performance.

## Current Dashboard Analysis

### Current Dash Architecture
```python
# cyberdelta/monitoring/real_time_dashboard.py
class RealTimeDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)  # Creates React app
        self.setup_layout()             # Defines React components
        self.setup_callbacks()          # React-style callbacks
    
    def setup_callbacks(self):
        @app.callback(
            Output('performance-chart', 'figure'),
            Input('strategy-selector', 'value'),
            Input('time-range-selector', 'value')
        )
        def update_performance_chart(strategies, time_range):
            # Python callback that updates React component
            return create_plotly_figure(strategies, time_range)
```

### Current Dashboard Features
1. **Real-time performance monitoring**
2. **Strategy selection and filtering**
3. **Performance charts and metrics**
4. **Funding rate heatmaps**
5. **Trade analysis tables**
6. **Real-time WebSocket updates**
7. **Drawdown visualization**
8. **Portfolio overview**

### Pain Points with Current Approach
- **React dependency**: Dash uses React under the hood
- **Build complexity**: Hidden webpack/babel complexity
- **Type issues**: No proper type stubs for Dash components
- **Performance**: React virtual DOM overhead
- **Bundle size**: Large JavaScript payload
- **Limited customization**: Constrained by Dash component library

## Proposed HTMX Architecture

### Django + HTMX Structure
```
dashboard/
├── views.py              # Django views handling HTMX requests
├── templates/
│   ├── dashboard/
│   │   ├── base.html     # Base template with HTMX setup
│   │   ├── index.html    # Main dashboard page
│   │   ├── components/   # Reusable HTMX components
│   │   │   ├── performance_chart.html
│   │   │   ├── metrics_table.html
│   │   │   ├── strategy_selector.html
│   │   │   └── funding_heatmap.html
│   │   └── partials/     # HTMX partial updates
│   │       ├── chart_data.html
│   │       ├── metrics_row.html
│   │       └── trade_row.html
├── static/
│   ├── css/
│   │   └── dashboard.css # Custom styling
│   ├── js/
│   │   ├── htmx.min.js   # 14KB HTMX library
│   │   ├── alpine.min.js # 15KB Alpine.js (optional)
│   │   └── plotly.min.js # For charts only
│   └── img/
└── websockets.py         # Django Channels WebSocket consumers
```

### Base Template Structure
```html
<!-- templates/dashboard/base.html -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CyberDelta Dashboard</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://unpkg.com/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="{% static 'css/dashboard.css' %}" rel="stylesheet">
</head>
<body>
    <div id="main-container" 
         hx-ws="connect:/ws/dashboard/"
         hx-trigger="load">
        {% block content %}{% endblock %}
    </div>
    
    <!-- Global error handling -->
    <div id="error-toast" class="toast" style="display: none;"></div>
</body>
</html>
```

## Feature-by-Feature Migration

### 1. Strategy Performance Chart

#### Current Dash Implementation
```python
@app.callback(
    Output('performance-chart', 'figure'),
    Input('strategy-selector', 'value'),
    Input('time-range-selector', 'value')
)
def update_performance_chart(strategies, time_range):
    data = get_performance_data(strategies, time_range)
    return px.line(data, x='date', y='cumulative_return', color='strategy')
```

#### HTMX Implementation
```python
# views.py
def performance_chart(request):
    strategies = request.GET.getlist('strategies')
    time_range = request.GET.get('time_range', '1d')
    
    data = get_performance_data(strategies, time_range)
    
    # Create Plotly figure server-side
    fig = create_performance_figure(data)
    chart_json = fig.to_json()
    
    return render(request, 'dashboard/components/performance_chart.html', {
        'chart_json': chart_json,
        'strategies': strategies,
        'time_range': time_range
    })
```

```html
<!-- templates/dashboard/components/performance_chart.html -->
<div id="performance-chart-container" class="chart-container">
    <div class="chart-controls">
        <select name="strategies" 
                multiple
                hx-get="{% url 'dashboard:performance_chart' %}"
                hx-target="#performance-chart-container"
                hx-trigger="change"
                hx-include="[name='time_range']">
            {% for strategy in available_strategies %}
                <option value="{{ strategy.id }}" 
                        {% if strategy.id in strategies %}selected{% endif %}>
                    {{ strategy.name }}
                </option>
            {% endfor %}
        </select>
        
        <select name="time_range"
                hx-get="{% url 'dashboard:performance_chart' %}"
                hx-target="#performance-chart-container"
                hx-trigger="change"
                hx-include="[name='strategies']">
            <option value="1h" {% if time_range == '1h' %}selected{% endif %}>1 Hour</option>
            <option value="1d" {% if time_range == '1d' %}selected{% endif %}>1 Day</option>
            <option value="1w" {% if time_range == '1w' %}selected{% endif %}>1 Week</option>
        </select>
    </div>
    
    <div id="performance-chart"></div>
    
    <script>
        // Render Plotly chart
        Plotly.newPlot('performance-chart', {{ chart_json|safe }});
    </script>
</div>
```

### 2. Real-time Metrics Table

#### HTMX + WebSocket Implementation
```python
# websockets.py (Django Channels)
class DashboardConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("dashboard", self.channel_name)
        await self.accept()
    
    async def metrics_update(self, event):
        """Send metrics update to client"""
        await self.send(text_data=json.dumps({
            'type': 'metrics_update',
            'html': event['html']
        }))

# views.py
def metrics_table(request):
    metrics = calculate_current_metrics()
    return render(request, 'dashboard/components/metrics_table.html', {
        'metrics': metrics
    })

# Background task to push updates
@periodic_task(run_every=crontab(second='*/5'))  # Every 5 seconds
def update_dashboard_metrics():
    metrics = calculate_current_metrics()
    html = render_to_string('dashboard/partials/metrics_row.html', {
        'metrics': metrics
    })
    
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)("dashboard", {
        "type": "metrics_update",
        "html": html
    })
```

```html
<!-- templates/dashboard/components/metrics_table.html -->
<div id="metrics-table" 
     hx-ws="connect:/ws/dashboard/"
     class="metrics-container">
    <table class="table">
        <thead>
            <tr>
                <th>Strategy</th>
                <th>PnL 24h</th>
                <th>Total Return</th>
                <th>Sharpe Ratio</th>
                <th>Max Drawdown</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody id="metrics-body">
            {% for metric in metrics %}
                {% include 'dashboard/partials/metrics_row.html' with metric=metric %}
            {% endfor %}
        </tbody>
    </table>
</div>

<script>
    // Handle WebSocket messages
    document.body.addEventListener('htmx:wsAfterMessage', function(event) {
        const data = JSON.parse(event.detail.message);
        if (data.type === 'metrics_update') {
            document.getElementById('metrics-body').innerHTML = data.html;
        }
    });
</script>
```

### 3. Funding Rate Heatmap

#### HTMX Implementation with Alpine.js
```python
# views.py
def funding_heatmap(request):
    time_range = request.GET.get('time_range', '24h')
    funding_data = get_funding_rates_matrix(time_range)
    
    return render(request, 'dashboard/components/funding_heatmap.html', {
        'funding_data': funding_data,
        'time_range': time_range
    })
```

```html
<!-- templates/dashboard/components/funding_heatmap.html -->
<div id="funding-heatmap" 
     x-data="fundingHeatmap()"
     class="heatmap-container">
    
    <div class="heatmap-controls">
        <button x-on:click="timeRange = '1h'; updateHeatmap()"
                :class="timeRange === '1h' ? 'active' : ''"
                class="btn">1H</button>
        <button x-on:click="timeRange = '24h'; updateHeatmap()"
                :class="timeRange === '24h' ? 'active' : ''"
                class="btn">24H</button>
        <button x-on:click="timeRange = '7d'; updateHeatmap()"
                :class="timeRange === '7d' ? 'active' : ''"
                class="btn">7D</button>
    </div>
    
    <div id="heatmap-chart"></div>
    
    <script>
        function fundingHeatmap() {
            return {
                timeRange: '{{ time_range }}',
                updateHeatmap() {
                    htmx.ajax('GET', 
                        `{% url 'dashboard:funding_heatmap' %}?time_range=${this.timeRange}`,
                        {target: '#funding-heatmap'}
                    );
                }
            }
        }
        
        // Initial chart render
        const fundingData = {{ funding_data|safe }};
        Plotly.newPlot('heatmap-chart', [{
            z: fundingData.values,
            x: fundingData.exchanges,
            y: fundingData.pairs,
            type: 'heatmap',
            colorscale: 'RdBu'
        }]);
    </script>
</div>
```

### 4. Trade Analysis Table with Infinite Scroll

```python
# views.py
def trade_analysis(request):
    page = int(request.GET.get('page', 1))
    strategy = request.GET.get('strategy')
    
    trades = get_trades_paginated(page=page, strategy=strategy, per_page=50)
    
    if request.headers.get('HX-Request'):
        # HTMX request - return only new rows
        return render(request, 'dashboard/partials/trade_rows.html', {
            'trades': trades,
            'page': page
        })
    else:
        # Full page request
        return render(request, 'dashboard/components/trade_analysis.html', {
            'trades': trades,
            'page': page
        })
```

```html
<!-- templates/dashboard/components/trade_analysis.html -->
<div id="trade-analysis" class="trade-container">
    <div class="trade-filters">
        <select name="strategy"
                hx-get="{% url 'dashboard:trade_analysis' %}"
                hx-target="#trade-tbody"
                hx-trigger="change">
            <option value="">All Strategies</option>
            {% for strategy in strategies %}
                <option value="{{ strategy.id }}">{{ strategy.name }}</option>
            {% endfor %}
        </select>
    </div>
    
    <table class="table">
        <thead>
            <tr>
                <th>Time</th>
                <th>Strategy</th>
                <th>Pair</th>
                <th>Side</th>
                <th>Size</th>
                <th>Price</th>
                <th>PnL</th>
            </tr>
        </thead>
        <tbody id="trade-tbody">
            {% include 'dashboard/partials/trade_rows.html' %}
        </tbody>
    </table>
    
    <!-- Infinite scroll trigger -->
    <div hx-get="{% url 'dashboard:trade_analysis' %}?page={{ page|add:1 }}"
         hx-target="#trade-tbody"
         hx-swap="beforeend"
         hx-trigger="revealed"
         hx-include="[name='strategy']">
        <div class="loading">Loading more trades...</div>
    </div>
</div>
```

## Real-time Updates Strategy

### WebSocket Integration
```python
# routing.py
websocket_urlpatterns = [
    re_path(r'ws/dashboard/$', DashboardConsumer.as_asgi()),
    re_path(r'ws/trades/$', TradeUpdatesConsumer.as_asgi()),
    re_path(r'ws/funding/$', FundingRateConsumer.as_asgi()),
]

# consumers.py
class DashboardConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("dashboard_updates", self.channel_name)
        await self.accept()
    
    async def trade_update(self, event):
        """Handle new trade notifications"""
        await self.send(text_data=json.dumps({
            'type': 'trade_update',
            'trade': event['trade_data']
        }))
    
    async def metrics_update(self, event):
        """Handle metrics updates"""
        await self.send(text_data=json.dumps({
            'type': 'metrics_update',
            'metrics': event['metrics_data']
        }))

# Background tasks
@shared_task
def broadcast_trade_update(trade_data):
    """Broadcast new trade to dashboard"""
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)("dashboard_updates", {
        "type": "trade_update",
        "trade_data": trade_data
    })

@shared_task
def broadcast_metrics_update():
    """Broadcast updated metrics to dashboard"""
    metrics = calculate_current_metrics()
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)("dashboard_updates", {
        "type": "metrics_update",
        "metrics_data": metrics
    })
```

### Client-side WebSocket Handling
```html
<script>
document.body.addEventListener('htmx:wsAfterMessage', function(event) {
    const data = JSON.parse(event.detail.message);
    
    switch(data.type) {
        case 'trade_update':
            // Add new trade row to table
            const tradeRow = createTradeRow(data.trade);
            document.getElementById('trade-tbody').prepend(tradeRow);
            break;
            
        case 'metrics_update':
            // Update metrics display
            updateMetricsDisplay(data.metrics);
            break;
            
        case 'funding_update':
            // Update funding rate heatmap
            updateFundingHeatmap(data.funding_rates);
            break;
    }
});

function createTradeRow(trade) {
    const row = document.createElement('tr');
    row.innerHTML = `
        <td>${trade.timestamp}</td>
        <td>${trade.strategy}</td>
        <td>${trade.pair}</td>
        <td class="${trade.side}">${trade.side}</td>
        <td>${trade.size}</td>
        <td>${trade.price}</td>
        <td class="${trade.pnl >= 0 ? 'positive' : 'negative'}">${trade.pnl}</td>
    `;
    return row;
}
</script>
```

## Performance Optimizations

### 1. Server-Side Rendering Benefits
```python
# Current Dash approach (client-side)
# 1. Load empty page
# 2. Download React bundle (200MB+)
# 3. Parse and execute JavaScript
# 4. Make API calls for data
# 5. Render components client-side

# Django + HTMX approach (server-side)
# 1. Load complete HTML page with data
# 2. Download HTMX (14KB)
# 3. Ready to use immediately
```

### 2. Caching Strategy
```python
# views.py
from django.views.decorators.cache import cache_page
from django.core.cache import cache

@cache_page(60)  # Cache for 1 minute
def performance_chart(request):
    cache_key = f"perf_chart_{request.GET.urlencode()}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return cached_data
    
    # Generate chart data
    data = expensive_chart_calculation()
    response = render(request, 'chart.html', {'data': data})
    
    cache.set(cache_key, response, 60)
    return response
```

### 3. Database Query Optimization
```python
# Efficient data queries for dashboard
def get_dashboard_metrics():
    """Optimized query for dashboard metrics"""
    return Strategy.objects.select_related('current_performance')\
        .prefetch_related('recent_trades')\
        .annotate(
            daily_pnl=Sum('trades__pnl', 
                filter=Q(trades__created_at__gte=timezone.now() - timedelta(days=1))),
            total_trades=Count('trades')
        )
```

## Migration Timeline

### Phase 1: Basic Layout (1 week)
- Set up Django dashboard app
- Create base templates with HTMX
- Implement basic navigation
- Set up WebSocket infrastructure

### Phase 2: Core Components (2 weeks)
- Migrate performance charts
- Implement metrics tables
- Create strategy selectors
- Add real-time updates

### Phase 3: Advanced Features (2 weeks)
- Funding rate heatmaps
- Trade analysis tables
- Infinite scroll implementation
- Advanced filtering

### Phase 4: Polish & Optimization (1 week)
- Performance optimization
- Error handling
- User experience improvements
- Mobile responsiveness

## Benefits of HTMX Migration

### Performance Benefits
- **Faster initial load**: No large JavaScript bundle
- **Better SEO**: Server-side rendering
- **Lower memory usage**: No React virtual DOM
- **Reduced bandwidth**: Only HTML updates, not JSON + rendering

### Development Benefits
- **Simpler debugging**: Server-side rendering easier to debug
- **Better type safety**: Django has proper type stubs
- **Familiar patterns**: Standard Django views and templates
- **No build step**: Direct file editing, immediate feedback

### User Experience Benefits
- **Faster interactions**: No client-side rendering delay
- **Progressive enhancement**: Works without JavaScript
- **Better accessibility**: Standard HTML forms and interactions
- **Mobile performance**: Less JavaScript execution on mobile devices

## Potential Challenges

### 1. Chart Interactivity
- **Challenge**: Plotly charts need JavaScript for interactivity
- **Solution**: Use Plotly.js directly, update via HTMX + WebSocket

### 2. Complex State Management
- **Challenge**: Some dashboard state is complex
- **Solution**: Combine HTMX with Alpine.js for local state

### 3. Real-time Performance
- **Challenge**: WebSocket updates need to be efficient
- **Solution**: Use Django Channels with Redis backing, selective updates

### 4. Migration Complexity
- **Challenge**: Large existing dashboard codebase
- **Solution**: Gradual migration, component by component

This HTMX migration will eliminate the React dependency while providing a more maintainable, performant, and type-safe dashboard solution.