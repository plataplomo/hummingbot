# Code Report: CyberDeltaEngine - Core Engine Components

## 1. Core Engine (`cyberdelta/core/engine.py`)

**Purpose**: Orchestrates the overall trading logic, manages strategies, and handles the main event loop.

**Key Responsibilities**:
- Adding, removing, enabling, and disabling strategies.
- Registering handlers for trade signals.
- Processing incoming market data and routing it to relevant strategies.
- Receiving `TradeSignal` objects from strategies.
- Forwarding signals to registered handlers (like `ExecutionHandler`).
- Maintaining a high-level view of engine status and uptime.

**Code Snippet (`Engine.process_market_data`)**:
```python
    def process_market_data(self, data: MarketData) -> None:
        """
        Process market data through all strategies for the relevant symbol

        Args:
            data: MarketData object containing market information
        """
        if not self.is_running:
            logger.warning("Engine is not running, ignoring market data")
            return

        self.last_update_time = datetime.now()

        # Process through relevant strategies
        for strategy in self.strategies.values():
            # Check if strategy is enabled and matches the symbol
            if strategy.symbol == data.symbol and strategy.enabled:
                try:
                    # Process data and potentially get a signal
                    signal = strategy.process_data(data)
                    if signal:
                        # Handle the generated signal
                        self._handle_signal(signal, strategy.name)
                except Exception as e:
                    logger.error(
                        f"Error processing data in strategy '{strategy.name}': {e}",
                        exc_info=True
                    )
```

## 2. Data Handler (`cyberdelta/core/data_handler.py`)

**Purpose**: Centralized collection, management, and distribution of market data from various exchanges.

**Key Responsibilities**:
- Establishing and maintaining WebSocket connections for real-time data streams.
- Fetching data via REST APIs (e.g., initial state, funding rates).
- Storing latest market data (tickers, order books, funding rates).
- Normalizing data formats across different exchanges.
- Tracking data freshness and identifying stale data.
- Providing clean and consistent data access to other components (Strategies, Risk Manager).

**Code Snippet (`DataHandler._handle_websocket_message`)**:
```python
    async def _handle_websocket_message(self, exchange_id: str, message: Dict[str, Any]):
        """
        Handle a WebSocket message.

        Args:
            exchange_id: Exchange identifier
            message: WebSocket message
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client available for {exchange_id}")
            return

        try:
            # Delegate parsing and handling to the specific API client
            await client.handle_ws_message(message)

            # Example of internal update based on parsed data (can be triggered by client handler)
            # message_type = client.get_message_type(message)
            # if message_type == "ticker":
            #     symbol, ticker_data = client.parse_ticker_message(message)
            #     if symbol and ticker_data:
            #         self._update_ticker(exchange_id, symbol, ticker_data)
            # ... other message types ...

        except Exception as e:
            logger.error(
                f"Error handling WebSocket message from {exchange_id}: {str(e)}",
                exc_info=True
            )
```

## 3. Execution Handler (`cyberdelta/core/execution_handler.py`)

**Purpose**: Responsible for receiving trade signals and executing them on the appropriate exchanges.

**Key Responsibilities**:
- Receiving `TradeSignal` objects (typically from the `Engine` or `SignalQueue`).
- Checking `CircuitBreaker` status before execution.
- Translating abstract signals into specific exchange API calls (`place_order`, `cancel_order`).
- Handling order execution logic (e.g., ensuring atomicity for multi-leg trades - see `SynchronizedOrderExecutor`).
- Managing order lifecycle (tracking fills, cancellations).
- Updating the `PortfolioTracker` with execution results (fills).
- Implementing retry logic for failed orders.

**Code Snippet (`ExecutionHandler.handle_signal`)**:
```python
    async def handle_signal(self, signal: TradeSignal):
        """
        Handle an incoming trade signal.

        Args:
            signal: The TradeSignal object to execute.
        """
        logger.info(f"Handling signal: {signal.signal_id} for {signal.symbol}")

        # 1. Pre-Execution Checks (Circuit Breaker, Risk Limits)
        if not await self._pre_execution_checks(signal):
            return

        # 2. Select Executor based on signal type/complexity
        #    (Simple executor for single orders, Synchronized for multi-leg)
        executor = self._get_executor(signal)

        # 3. Execute the signal
        try:
            results = await executor.execute(signal)
            logger.info(f"Execution results for signal {signal.signal_id}: {results}")

            # 4. Post-Execution Updates (Portfolio Tracker)
            await self._post_execution_updates(signal, results)

        except APIError as e:
            logger.error(f"API error executing signal {signal.signal_id}: {e}")
            # Implement retry logic or error handling
        except Exception as e:
            logger.error(
                f"Unexpected error executing signal {signal.signal_id}: {e}",
                exc_info=True
            )
            # Potentially trigger safety mechanisms
```

## 4. Portfolio Tracker (`cyberdelta/core/portfolio_tracker.py`)

**Purpose**: Maintains the system's internal state regarding positions, balances, and performance across all connected exchanges.

**Key Responsibilities**:
- Tracking current positions (symbol, size, entry price, side) per exchange.
- Monitoring available and total balances for each asset.
- Calculating unrealized and realized Profit and Loss (PnL).
- Tracking performance metrics (e.g., overall returns, drawdowns).
- Updating state based on fills received from the `ExecutionHandler`.
- Providing current portfolio state to `RiskManager` and `Strategies`.
- Persisting and restoring state (via `StateManager`).
- Potentially interacting with `PositionReconciliationSystem` to correct state.

**Code Snippet (`PortfolioTracker.update_position_from_fill`)**:
```python
    def update_position_from_fill(self, fill: Dict[str, Any]):
        """
        Update position based on a new fill event.

        Args:
            fill: Dictionary representing the fill details (exchange, symbol, side, price, quantity).
        """
        exchange = fill['exchange']
        symbol = fill['symbol']
        side = OrderSide[fill['side']] # Ensure OrderSide enum
        price = float(fill['price'])
        quantity = float(fill['quantity'])
        timestamp = fill.get('timestamp', datetime.now())

        with self.lock:
            # ... (logic to fetch or create position entry) ...

            position = self.positions[exchange][symbol]
            current_qty = position.quantity
            current_side = position.side

            # ... (complex logic to update average entry price, size, PnL) ...
            # Handle closing, opening, increasing, decreasing, flipping positions

            # Example: Simplified update for opening/increasing a position
            if position.quantity == 0 or current_side == side:
                new_total_value = (position.avg_entry_price * current_qty) + (price * quantity)
                new_total_qty = current_qty + quantity
                position.avg_entry_price = new_total_value / new_total_qty
                position.quantity = new_total_qty
                position.side = side
            else: # Reducing or flipping position
                # ... (realized PnL calculation, update remaining quantity/side) ...
                pass

            position.last_update_time = timestamp
            logger.info(f"Updated position for {exchange}/{symbol} from fill: {position}")

            # Persist state changes periodically or trigger save
            self._needs_save = True
```

## 5. Risk Manager (`cyberdelta/core/risk_manager.py`)

**Purpose**: Assesses risk associated with potential trades and determines appropriate position sizing based on predefined rules and portfolio state.

**Key Responsibilities**:
- Receiving potential `ArbitrageOpportunity` objects.
- Evaluating opportunity risk vs. reward (e.g., using utility functions).
- Calculating maximum allowable position size based on:
    - Configured risk limits (e.g., max exposure per trade/symbol/portfolio).
    - Available capital/margin (`PortfolioTracker` data).
    - Market volatility (`DataHandler` data).
    - Kelly criterion or other sizing models.
- Potentially rejecting opportunities that exceed risk thresholds.
- Returning `SizedOpportunity` objects with calculated sizes or rejecting the opportunity.

**Code Snippet (`RiskManager.size_opportunity`)**:
```python
    def size_opportunity(self, opportunity: ArbitrageOpportunity) -> Optional[SizedOpportunity]:
        """
        Calculate the appropriate position size for a given opportunity based on risk parameters.

        Args:
            opportunity: The potential arbitrage opportunity.

        Returns:
            SizedOpportunity with calculated sizes, or None if rejected.
        """
        logger.debug(f"Sizing opportunity: {opportunity}")

        # 1. Basic Validation Checks
        if not self._is_opportunity_valid(opportunity):
            logger.warning(f"Opportunity {opportunity.symbol} rejected by initial validation.")
            return None

        # 2. Calculate Max Size based on Capital and Risk Limits
        max_capital_risk = self._calculate_max_capital_at_risk(opportunity)
        max_size_based_on_capital = max_capital_risk # Simplified, convert to position size

        # 3. Calculate Size based on Sizing Model (e.g., Kelly Criterion)
        # kelly_fraction = self._calculate_kelly_fraction(opportunity)
        # optimal_size_kelly = kelly_fraction * self.portfolio_tracker.get_total_equity()
        optimal_size_model = 10000 # Placeholder for model calculation

        # 4. Determine Final Size (Min of constraints)
        final_size_usd = min(max_size_based_on_capital, optimal_size_model, self.max_position_size_usd)

        # 5. Check against minimum size and other constraints
        if final_size_usd < self.min_position_size_usd:
            logger.info(f"Calculated size ${final_size_usd:.2f} below minimum ${self.min_position_size_usd:.2f}")
            return None

        # 6. Calculate Risk-Adjusted Return or other metrics
        risk_adjusted_return = self._calculate_risk_adjusted_return(opportunity, final_size_usd)

        # 7. Create SizedOpportunity
        #    (Need to split final_size_usd into long_size/short_size based on prices)
        #    Requires prices from DataHandler
        long_size, short_size = self._allocate_usd_size(final_size_usd, opportunity)

        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            long_size=long_size,
            short_size=short_size,
            allocation_percentage=(final_size_usd / self.portfolio_tracker.get_total_equity()) * 100,
            risk_adjusted_return=risk_adjusted_return,
            timestamp=datetime.now()
        )

        logger.info(f"Sized opportunity: {sized_opportunity}")
        return sized_opportunity
```

These core components form the backbone of the CyberDeltaEngine, working together to handle data, generate signals, manage risk, execute trades, and track performance. 