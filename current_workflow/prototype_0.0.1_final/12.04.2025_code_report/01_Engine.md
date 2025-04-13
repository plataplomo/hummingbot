# CyberDeltaEngine: Code Review Report (v0.0.1) - Core Engine Components

This section provides a detailed review of the main components constituting the core logic of CyberDeltaEngine, focusing on their roles, interactions, and implementation details.

## 1. `Engine` (`cyberdelta/core/engine.py`)

*   **Intended Responsibility:** Act as the central orchestrator, managing strategy lifecycles, routing normalized market data from the `DataHandler` to active strategies, and potentially routing generated signals/opportunities towards the `SignalQueue` or `RiskManager`.
*   **Current Implementation & Interactions:**
    *   Initializes with references to `DataHandler`, strategies, `SignalQueue`, `RiskManager`, `ExecutionHandler`, etc.
    *   Provides methods for managing strategies: `add_strategy`, `remove_strategy`, `enable_strategy`, `disable_strategy`.
    *   The `run` method starts the core components (`DataHandler`, `ExecutionHandler`, potentially others) and manages their lifecycles.
    *   Contains placeholders or logic for routing market data (`process_market_data`) and handling signals (`_handle_signal_from_strategy`).
    *   **Observed Behavior:** In the current `v0.0.1` implementation targeting funding rate arbitrage, the `FundingRateArbitrageStrategy` interacts *directly* with the `DataHandler` to pull necessary data (tickers, funding rates) and sends signals *directly* to the `SignalQueue`. The `Engine`'s role in active data/signal routing for this specific strategy seems minimal. Its primary active role is orchestrating the startup and shutdown of components and managing the main event loop.

*   **Code Snippet (Conceptual `run` method):**
    ```python
    # cyberdelta/core/engine.py (Conceptual)
    class Engine:
        # ... __init__ with component references ...

        async def run(self):
            self.logger.info("Starting CyberDeltaEngine components...")
            tasks = []
            try:
                # Start data handler first
                if self.data_handler:
                    tasks.append(asyncio.create_task(self.data_handler.run(), name="DataHandler"))

                # Start execution handler (for order updates, etc.)
                if self.execution_handler:
                    tasks.append(asyncio.create_task(self.execution_handler.run(), name="ExecutionHandler")) # Assuming it has a run loop

                # Start portfolio tracker (if it needs background tasks like reconciliation)
                if self.portfolio_tracker and hasattr(self.portfolio_tracker, 'run'):
                     tasks.append(asyncio.create_task(self.portfolio_tracker.run(), name="PortfolioTracker"))

                # Start active strategies
                for strategy in self.strategies.values():
                    if strategy.is_enabled():
                        tasks.append(asyncio.create_task(strategy.run(), name=f"Strategy_{strategy.name}"))

                # Start signal queue processing (if needed as separate task)
                if hasattr(self.signal_queue, 'run'): # e.g., for periodic cleaning
                    tasks.append(asyncio.create_task(self.signal_queue.run(), name="SignalQueue"))

                self.logger.info(f"Engine running with {len(tasks)} core tasks.")
                if tasks:
                    await asyncio.gather(*tasks)
                else:
                    self.logger.warning("No tasks to run. Engine exiting.")

            except asyncio.CancelledError:
                self.logger.info("Engine run cancelled.")
            except Exception as e:
                self.logger.error(f"Engine run encountered critical error: {e}", exc_info=True)
            finally:
                self.logger.info("Engine shutting down components...")
                await self.stop() # Ensure stop logic is called
                self.logger.info("Engine shutdown complete.")

        async def stop(self):
            # ... logic to gracefully stop all component tasks ...
            # Signal strategies, data_handler, execution_handler etc. to stop
            pass

        # Potential data routing (currently bypassed by strategy)
        # async def process_market_data(self, data: MarketData):
        #     for strategy in self.strategies.values():
        #          if strategy.is_enabled() and strategy.subscribes_to(data.symbol):
        #              await strategy.on_market_data(data)

        # Potential signal handling (currently bypassed by strategy)
        # async def _handle_signal_from_strategy(self, signal: TradeSignal):
        #      await self.signal_queue.add_signal(signal, signal.utility_score) # Example
    ```

*   **Observations & Concerns:**
    *   **Role Mismatch:** The current implementation deviates significantly from the typical "Engine routes data" pattern. This isn't inherently wrong, but the `Engine` class name and its methods imply a more central routing role than it performs.
    *   **Complexity:** If future strategies require data routing, the `Engine` will need significant changes, potentially impacting the existing direct-access strategy.
*   **Recommendations:**
    *   **Option A (Align):** Refactor `FundingRateArbitrageStrategy` to receive data via `Engine.process_market_data` (or `strategy.on_market_data`) and send signals via `Engine._handle_signal_from_strategy`. This centralizes flow control in the `Engine`.
    *   **Option B (Adapt):** Rename or repurpose the `Engine` to reflect its current role (e.g., `CoreServiceManager`, `ApplicationOrchestrator`). Acknowledge that strategies might have different data access patterns (pull vs. push).
    *   **Decision:** For `v0.0.1`, the direct access model used by the strategy is simpler. **Recommendation is Option B:** Adapt the `Engine`'s documentation/naming to reflect its orchestrator role rather than forcing a data routing pattern not currently used. Ensure the `run` and `stop` methods correctly manage all component lifecycles.

## 2. `DataHandler` (`cyberdelta/core/data_handler.py`)

*   **Responsibility:** Connect to exchanges (via API clients), subscribe to market data (tickers, funding, order books), normalize data into internal models, track freshness, and provide access methods for strategies or other components.
*   **Implementation Details:**
    *   Uses `API Clients` (`HyperliquidAPI`, `BackpackAPI`) for WebSocket connections and data parsing.
    *   Maintains persistent WebSocket connections (`_maintain_websocket_connection`) with exponential backoff for retries.
    *   Handles incoming messages in `_handle_websocket_message`, delegating parsing to the appropriate API client.
    *   Stores normalized data:
        *   `self.tickers: dict[str, dict[str, Ticker]]` (exchange -> symbol -> Ticker object)
        *   `self.funding_rates: dict[str, dict[str, FundingRate]]` (exchange -> symbol -> FundingRate object)
        *   `self.orderbooks: dict[str, dict[str, OrderBook]]` (exchange -> symbol -> OrderBook object)
    *   Tracks last update times (`self.last_update_times`).
    *   Provides synchronous access methods: `get_ticker`, `get_funding_rate`, `get_orderbook`, `get_last_update_time`.
    *   Includes `_collect_initial_data` to fetch starting data via REST on startup.
    *   Observer pattern (`register_observer`, `_notify_observers`) exists but seems unused in the primary strategy flow.

*   **Code Snippet (Data Storage & Access):**
    ```python
    # cyberdelta/core/data_handler.py (Illustrative)
    from cyberdelta.core.models import Ticker, FundingRate, OrderBook

    class DataHandler:
        def __init__(self, config, api_clients, symbol_mapper):
            # ... other initializations ...
            self.tickers: dict[str, dict[str, Ticker]] = defaultdict(dict)
            self.funding_rates: dict[str, dict[str, FundingRate]] = defaultdict(dict)
            self.orderbooks: dict[str, dict[str, OrderBook]] = defaultdict(dict)
            self.last_update_times: dict[str, datetime] = {} # Keyed by "ticker:EXCHANGE:SYMBOL", etc.
            self._data_lock = asyncio.Lock() # Protect concurrent access/modification

        async def _handle_websocket_message(self, exchange_name: str, message: Any):
            client = self.api_clients.get(exchange_name)
            if not client: return

            try:
                # Delegate parsing to the specific API client
                parsed_data = client.parse_ws_message(message) # Expects client to return standardized models or None

                async with self._data_lock:
                    now = datetime.now(UTC)
                    if isinstance(parsed_data, Ticker):
                        self.tickers[exchange_name][parsed_data.symbol] = parsed_data
                        self.last_update_times[f"ticker:{exchange_name}:{parsed_data.symbol}"] = now
                    elif isinstance(parsed_data, FundingRate):
                        self.funding_rates[exchange_name][parsed_data.symbol] = parsed_data
                        self.last_update_times[f"funding:{exchange_name}:{parsed_data.symbol}"] = now
                    elif isinstance(parsed_data, OrderBook):
                         # Optional: Limit order book depth stored if necessary
                        self.orderbooks[exchange_name][parsed_data.symbol] = parsed_data
                        self.last_update_times[f"book:{exchange_name}:{parsed_data.symbol}"] = now
                    # ... handle other data types (trades, etc.) if needed ...

                # Observer pattern call (if used)
                # if parsed_data: await self._notify_observers(parsed_data)

            except Exception as e:
                self.logger.error(f"Error handling {exchange_name} WS message: {e}", exc_info=True)

        # --- Synchronous Access Methods ---
        # NOTE: These are synchronous, potential bottleneck if called very frequently
        # from highly concurrent code without care. Consider async getters if needed.
        def get_ticker(self, exchange: str, symbol: str) -> Ticker | None:
             # No lock needed for read if updates are atomic enough,
             # but lock ensures absolute latest assignment is seen if called mid-update.
             # Consider if lock overhead is worth it for reads vs potential for slightly stale data.
             # For now, assuming reads are okay without lock if updates are fast/atomic.
            return self.tickers.get(exchange, {}).get(symbol)

        # ... similar getters for get_funding_rate, get_orderbook ...

        def get_last_update_time(self, data_key: str) -> datetime | None:
            # data_key format: "ticker:EXCHANGE:SYMBOL"
            return self.last_update_times.get(data_key)

    ```

*   **Observations & Strengths:**
    *   **Clear Responsibility:** Well-defined role as the market data hub.
    *   **Normalization:** Correctly uses internal models (`Ticker`, `FundingRate`, `OrderBook`) for storing normalized data (assuming recent fixes addressed previous inconsistencies).
    *   **Resilience:** Includes WebSocket reconnection logic.
*   **Concerns & Areas for Improvement:**
    *   **Locking:** Uses `asyncio.Lock` for updates. Ensure getter methods are also safe if they can be called concurrently with updates (reads during writes might be acceptable depending on tolerance for slightly stale data vs lock contention).
    *   **Initial Data Sync:** Ensure `_collect_initial_data` is robust and handles potential startup failures gracefully.
    *   **Observer Pattern:** Remains unused; remove if not planned for `v0.0.1`.
*   **Recommendations:**
    *   **Verify Model Usage:** Double-check that *all* data storage (`tickers`, `funding_rates`, `orderbooks`) consistently uses the correct `cyberdelta.core.models` types and that API client parsing methods return these types.
    *   **Review Locking Strategy:** Confirm the necessity and scope of `_data_lock`. For read-heavy access via getters, consider if the lock is always needed or if atomic assignments in `_handle_websocket_message` are sufficient. Add comments explaining the rationale.
    *   **Remove Observer Pattern:** If not used by `v0.0.1`, remove the observer-related code (`register_observer`, `_notify_observers`, `self.observers`) to simplify the component.

## 3. `ExecutionHandler` (`cyberdelta/core/execution_handler.py`)

*   **Responsibility:** Receive execution instructions (sized signals/opportunities), translate them into API calls using `API Clients`, place orders, monitor their lifecycle (fills, cancellations), handle basic retries, and report confirmed fills to the `PortfolioTracker`.
*   **Implementation Details:**
    *   Likely receives a sized object (e.g., `TradeSignal` with quantity, or a dedicated `ExecutionOrder` object) from `RiskManager`.
    *   Uses `API Clients` (`HyperliquidAPI`, `BackpackAPI`) via dependency injection.
    *   Uses `SymbolMapper` to get exchange-specific symbols.
    *   Checks `CircuitBreakerSystem` via `can_execute` before attempting to place orders.
    *   Implements order placement logic, potentially with retries (`_place_order_with_retry`).
    *   **Crucially:** Needs a mechanism to receive and process fill updates. The ideal way is via WebSocket updates pushed from the `API Client` layer, likely processed initially by `PortfolioTracker` which then might notify the `ExecutionHandler` or the originating strategy/component if needed. Relying on polling (`_get_order_status`) is highly discouraged due to latency and inefficiency.
    *   Manages state of "active" executions if complex multi-leg execution or follow-up actions are needed.
    *   Basic logic for handling partial fills (`_compensate_position`) exists but requires significant validation.

*   **Code Snippet (Conceptual Order Placement & Fill Handling):**
    ```python
    # cyberdelta/core/execution_handler.py (Conceptual)
    class ExecutionHandler:
        def __init__(self, config, api_clients, portfolio_tracker, circuit_breakers, symbol_mapper):
            # ... initializations ...
            self.portfolio_tracker = portfolio_tracker # For reporting fills
            self.api_clients = api_clients
            self.circuit_breakers = circuit_breakers
            self.symbol_mapper = symbol_mapper
            # self.active_executions = {} # Maybe needed for complex orders/compensation

        async def execute_signal(self, signal: TradeSignal): # Assume signal is sized and validated
            exchange = signal.metadata.get("exchange") # Or inferred
            symbol = signal.symbol # Assume internal symbol
            if not exchange:
                self.logger.error(f"Cannot execute signal {signal.signal_id}: Missing exchange info.")
                return

            # 1. Map symbol
            exchange_symbol = self.symbol_mapper.get_exchange_symbol(symbol, exchange)
            if not exchange_symbol:
                self.logger.error(f"Cannot execute signal {signal.signal_id}: Symbol mapping failed for {symbol} on {exchange}.")
                return

            # 2. Check Circuit Breaker
            can_exec, reason = self.circuit_breakers.can_execute(exchange, exchange_symbol)
            if not can_exec:
                self.logger.warning(f"Execution BLOCKED for {exchange}:{exchange_symbol} by circuit breaker: {reason}")
                return

            # 3. Get API Client
            api_client = self.api_clients.get(exchange)
            if not api_client:
                self.logger.error(f"No API client found for exchange: {exchange}")
                return

            # 4. Place Order (Simplified - assumes simple limit/market order signal)
            try:
                # Determine order params from TradeSignal
                order_params = {
                    "symbol": exchange_symbol,
                    "side": signal.side,
                    "quantity": signal.quantity,
                    "order_type": OrderType.MARKET if signal.price is None else OrderType.LIMIT,
                    "price": signal.price,
                    "client_order_id": f"cde-{signal.signal_id[:8]}-{int(time.time())}" # Example CID
                    # ... other params like time_in_force, post_only ...
                }
                self.logger.info(f"Placing order on {exchange}: {order_params}")
                # The create_order method should ideally return the initial Order object or confirmation
                order_result = await api_client.create_order(**order_params)

                if order_result and order_result.status not in [OrderStatus.REJECTED, OrderStatus.FAILED, OrderStatus.EXPIRED]:
                    self.logger.info(f"Order placed successfully on {exchange}: ID {order_result.order_id}, CID {order_params['client_order_id']}")
                    # PortfolioTracker should ideally receive WS update for this new order
                    # await self.portfolio_tracker.update_order(order_result) # Maybe? Or let WS handle.
                else:
                    self.logger.error(f"Order placement FAILED or REJECTED on {exchange} for signal {signal.signal_id}. Result: {order_result}")

            except Exception as e:
                 self.logger.error(f"Error placing order for signal {signal.signal_id} on {exchange}: {e}", exc_info=True)

        # --- Fill Handling ---
        # This handler should NOT be polling. It should be passive.
        # Fills should arrive via WebSocket -> API Client -> PortfolioTracker.
        # PortfolioTracker updates its state.
        # If ExecutionHandler needs to know about a fill (e.g., to trigger next leg),
        # PortfolioTracker could use a callback/event system.

        # Example (Illustrative Callback - if needed):
        # async def on_fill_received(self, trade: Trade):
        #      self.logger.info(f"ExecutionHandler notified of fill: {trade.id} for order {trade.order_id}")
             # ... logic to handle fill, e.g., trigger compensation, mark execution complete ...
    ```
*   **Configuration Example (`config.yaml`):**
    ```yaml
    execution_handler:
      default_order_type: "MARKET" # Or "LIMIT"
      limit_order_slippage_bps: 5 # Basis points for limit order price adjustment from signal price
      max_retries: 3
      retry_delay_seconds: 1.0
      # Configuration for partial fill compensation (if enabled)
      compensation:
        enabled: false
        mode: "MARKET" # How to execute compensation order (MARKET or LIMIT)
        threshold_pct: 0.9 # e.g., compensate if less than 90% filled after timeout
        timeout_seconds: 10
    ```

*   **Observations & Strengths:**
    *   Integrates with `CircuitBreakerSystem` and `SymbolMapper`.
    *   Basic structure for order placement exists.
*   **Concerns & Areas for Improvement:**
    *   **Fill Handling Mechanism:** HIGH PRIORITY - The mechanism for receiving and processing fill updates needs clarification and likely redesign away from polling towards real-time WebSocket updates processed by `PortfolioTracker`.
    *   **Atomicity/Legging Risk:** For arbitrage, placing two legs requires careful handling to minimize the risk that one order fills and the other fails or is delayed significantly. The current simple `execute_signal` doesn't address multi-leg atomicity.
    *   **Partial Fill Compensation:** The `_compensate_position` logic needs rigorous testing and clear configuration. Is it necessary for v0.0.1? Arbitrage often requires full fills on both legs.
    *   **State:** Does the handler need to persist its `active_executions`? If so, integrate with `StateManager`.
*   **Recommendations:**
    *   **Redesign Fill Handling:** Ensure fills are processed via WebSocket updates routed through `API Client -> PortfolioTracker`. Define how/if `ExecutionHandler` is notified if it needs to react to fills. Remove polling logic (`_get_order_status`).
    *   **Address Legging Risk:** For `v0.0.1`, determine the strategy for handling multi-leg orders. Options:
        *   Place sequentially, accept risk (simplest, riskiest).
        *   Place simultaneously (`asyncio.gather`), handle failures individually (better).
        *   Use more complex order types if supported by exchanges (e.g., GTT + Post-Only, less common).
        *   Implement robust cancellation logic if one leg fails after the other is placed.
        Document the chosen approach and its risks.
    *   **Review Compensation:** Evaluate the need and robustness of `_compensate_position` for `v0.0.1`. Consider disabling or simplifying if full fills are the primary goal.
    *   **Clarify State Persistence:** Determine if `ExecutionHandler` state needs saving via `StateManager`.

## 4. `PortfolioTracker` (`cyberdelta/core/portfolio_tracker.py`)

*   **Responsibility:** Maintain the application's internal, real-time view of account balances and open positions across all exchanges. Act as the single source of truth for portfolio state queried by other components (`RiskManager`, `Strategy`). Perform periodic reconciliation against exchange data.
*   **Implementation Details:**
    *   Stores state using internal models:
        *   `self._balances: dict[str, dict[str, Balance]]` (exchange -> asset -> Balance object)
        *   `self._positions: dict[str, dict[str, Position]]` (exchange -> symbol -> Position object)
        *   `self._orders: dict[str, dict[str, Order]]` (exchange -> order_id -> Order object) - Tracking open orders accurately is vital.
    *   Uses `asyncio.Lock` (`_lock`) to protect concurrent read/write access to state dictionaries.
    *   `initialize()` fetches initial balances and positions from exchanges via `API Clients` on startup.
    *   **Update Methods:** `update_balance`, `update_position`, `update_order`, `process_trade`. These methods **must** be called in response to real-time data, primarily WebSocket updates parsed by the `API Clients`.
    *   Provides getter methods: `get_balance`, `get_position`, `get_order`, `get_total_capital_usd`, `get_exchange_exposure_usd`, etc. These should use the `_lock`.
    *   Integrates with `PositionReconciliationSystem` (which likely triggers `reconcile_exchange`).
    *   `reconcile_exchange` fetches current state from the exchange and compares it with the local state, logging discrepancies.

*   **Code Snippet (Conceptual Update & Access with Lock):**
    ```python
    # cyberdelta/core/portfolio_tracker.py (Conceptual)
    class PortfolioTracker:
        def __init__(self, config, api_clients, state_manager=None):
            # ... initializations ...
            self._balances: dict[str, dict[str, Balance]] = defaultdict(dict)
            self._positions: dict[str, dict[str, Position]] = defaultdict(dict)
            self._orders: dict[str, dict[str, Order]] = defaultdict(dict) # Store by ORDER ID
            self._lock = asyncio.Lock()
            self.api_clients = api_clients
            self.state_manager = state_manager # For persistence
            # ...

        async def initialize(self):
            async with self._lock: # Lock during full initialization
                self.logger.info("Initializing portfolio state...")
                # Load persisted state if available
                # await self._load_state()
                # Fetch fresh state from all exchanges
                for exchange_name, client in self.api_clients.items():
                    try:
                        balances = await client.get_balances()
                        positions = await client.get_positions() # May be empty for spot
                        open_orders = await client.get_open_orders()

                        self._balances[exchange_name] = {b.asset: b for b in balances}
                        self._positions[exchange_name] = {p.symbol: p for p in positions}
                        self._orders[exchange_name] = {o.order_id: o for o in open_orders}
                        self.logger.info(f"Initialized state for {exchange_name}.")
                    except Exception as e:
                        self.logger.error(f"Failed to initialize state for {exchange_name}: {e}", exc_info=True)
                # Persist initial state?
                # await self._save_state()

        # --- Update Methods (Called by API Client WS Handlers) ---
        async def update_order(self, order: Order, exchange_name: str):
             # Assume order is a complete Order object from WS update
            async with self._lock:
                self.logger.debug(f"Updating order {order.order_id} on {exchange_name}: Status {order.status}")
                if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                    self._orders.get(exchange_name, {}).pop(order.order_id, None) # Remove closed/failed order
                else:
                    if exchange_name not in self._orders: self._orders[exchange_name] = {}
                    self._orders[exchange_name][order.order_id] = order

        async def process_trade(self, trade: Trade, exchange_name: str):
            # Assume trade is a Trade object from WS update
            async with self._lock:
                 self.logger.debug(f"Processing trade {trade.id} on {exchange_name} for order {trade.order_id}")
                # 1. Update position based on trade side and quantity
                position = self._positions.get(exchange_name, {}).get(trade.symbol)
                # ... complex logic to update position size, entry price, PnL ...
                # self._positions[exchange_name][trade.symbol] = updated_position

                # 2. Update balances based on trade cost, fees
                base_asset, quote_asset = self.symbol_mapper.split_symbol(trade.symbol) # Needs SymbolMapper
                # ... logic to adjust base and quote asset balances ...

                # 3. Update associated order's filled quantity
                order = self._orders.get(exchange_name, {}).get(trade.order_id)
                if order:
                     # Ensure Decimal conversion if needed
                    order.filled_quantity = (order.filled_quantity or Decimal(0)) + trade.quantity
                    order.remaining_quantity = order.quantity - order.filled_quantity
                    # Potentially update order status based on fill
                    if order.remaining_quantity <= Decimal(0):
                         order.status = OrderStatus.FILLED
                         self._orders.get(exchange_name, {}).pop(trade.order_id, None) # Remove filled order
                    else:
                         order.status = OrderStatus.PARTIALLY_FILLED

                # Persist state changes periodically or on significant events
                # await self._save_state() # Careful about frequency

        # --- Getter Methods ---
        async def get_position(self, exchange: str, symbol: str) -> Position | None:
            async with self._lock:
                return self._positions.get(exchange, {}).get(symbol)

        async def get_balance(self, exchange: str, asset: str) -> Balance | None:
            async with self._lock:
                 return self._balances.get(exchange, {}).get(asset)

        # ... other getters (get_open_orders, get_total_capital_usd etc.) using lock ...

        # --- Persistence ---
        # async def _save_state(self):
        #     if self.state_manager:
        #         state = {'balances': self._balances, 'positions': self._positions, 'orders': self._orders}
        #         await self.state_manager.save_state('portfolio_tracker', state)
        # async def _load_state(self):
        #      if self.state_manager:
        #          state = await self.state_manager.load_state('portfolio_tracker')
        #          if state:
        #              self._balances = state.get('balances', defaultdict(dict))
        #              # ... load positions, orders, ensure type conversions ...

    ```

*   **Observations & Strengths:**
    *   Provides the necessary central state management role.
    *   Uses appropriate data models and includes initialization logic.
    *   Correctly identifies the need for locking (`_lock`).
*   **Concerns & Areas for Improvement:**
    *   **Update Trigger Mechanism:** How `update_*` and `process_trade` are called is critical. This *must* be driven by real-time WebSocket updates parsed by `API Clients`, not polling from `ExecutionHandler`.
    *   **State Consistency:** Accurately updating balances, positions, and especially open orders based on trades/events is complex. Requires careful logic (handling fees, costs, partial fills, order status transitions) and thorough testing.
    *   **Concurrency:** The `asyncio.Lock` is essential. Ensure *all* state modifications and reads that need consistency acquire the lock.
    *   **Persistence:** State persistence (`_save_state`, `_load_state`) via `StateManager` needs implementation and testing, including handling data format changes across versions. How often state is saved needs consideration (performance vs. data loss risk).
*   **Recommendations:**
    *   **Confirm WS Update Flow:** Verify that `API Clients` parse WebSocket account updates (orders, fills, balance changes) and call the corresponding `PortfolioTracker.update_*` / `process_trade` methods.
    *   **Test State Logic:** Implement extensive unit tests for the state update logic within `process_trade`, covering different scenarios (buy/sell, fees, partial fills, impact on positions and balances).
    *   **Implement Persistence:** Implement `_save_state` and `_load_state` using `StateManager`. Decide on a save frequency (e.g., after N updates, every M seconds, on shutdown).
    *   **Review Lock Granularity:** Ensure the lock protects all necessary operations. If certain getters don't strictly need the absolute latest state, they *might* omit the lock for performance, but this needs careful justification.

## 5. `RiskManager` (`cyberdelta/core/risk_manager.py`)

*   **Responsibility:** Receive prioritized signals/opportunities (e.g., from `SignalQueue`), validate them against configured risk parameters and current portfolio state, calculate the appropriate execution size, and pass the validated/sized instruction to the `ExecutionHandler`.
*   **Implementation Details:**
    *   Loads risk rules from configuration (`max_total_exposure_usd`, `max_position_size_usd`, `kelly_fraction`, `max_drawdown_pct`, etc.).
    *   Queries `PortfolioTracker` for current state (balances, positions, total exposure). Requires async access and locking considerations if `PortfolioTracker` uses locks.
    *   Checks `CircuitBreakerSystem` status.
    *   Calculates position size, potentially using Kelly Criterion (`_calculate_kelly_size`) adjusted by various factors (volatility, drawdown state, maximums).
    *   Core logic likely resides in a method like `validate_and_size_signal(signal: TradeSignal) -> TradeSignal | None`.

*   **Code Snippet (Conceptual Validation & Sizing):**
    ```python
    # cyberdelta/core/risk_manager.py (Conceptual)
    class RiskManager:
        def __init__(self, config, portfolio_tracker, circuit_breakers):
            self.config = config
            self.portfolio_tracker = portfolio_tracker
            self.circuit_breakers = circuit_breakers
            self.max_total_exposure = Decimal(config.get("risk_manager.max_total_exposure_usd", "10000"))
            self.max_pos_size_usd = Decimal(config.get("risk_manager.max_position_size_usd", "5000"))
            self.kelly_fraction = float(config.get("risk_manager.kelly_fraction", "0.1"))
            # ... load other params ...

        async def validate_and_size_signal(self, signal: TradeSignal) -> TradeSignal | None:
            exchange = signal.metadata.get("exchange") # Or inferred
            symbol = signal.symbol
            if not exchange: # Basic check
                 self.logger.warning(f"Risk check failed for {signal.signal_id}: Missing exchange.")
                 return None

            # 1. Check Circuit Breaker
            can_exec, reason = self.circuit_breakers.can_execute(exchange, symbol) # Use mapped symbol if needed
            if not can_exec:
                self.logger.warning(f"Risk check failed for {signal.signal_id}: Circuit breaker tripped ({reason}).")
                return None

            # 2. Get Current Portfolio State (Requires async access to PortfolioTracker)
            current_exposure = await self.portfolio_tracker.get_total_exposure_usd()
            current_position = await self.portfolio_tracker.get_position(exchange, symbol)
            available_balance = await self.portfolio_tracker.get_available_margin(exchange) # Or similar

            # 3. Check Global Exposure Limit
            if current_exposure >= self.max_total_exposure:
                 self.logger.warning(f"Risk check failed for {signal.signal_id}: Max total exposure reached.")
                 return None

            # 4. Calculate Initial Size (e.g., using Kelly)
            # Ensure utility/edge/volatility are present in signal metadata or calculated
            edge = signal.metadata.get("expected_profit_pct") # Example
            volatility = signal.metadata.get("basis_volatility") # Example
            if edge is None or volatility is None or volatility <= 0:
                 self.logger.warning(f"Risk check failed for {signal.signal_id}: Missing edge/volatility for sizing.")
                 return None

            # Careful with float vs Decimal here! Kelly often uses floats.
            # Assume signal.price exists if needed for USD conversion
            if signal.price is None: # Need price for sizing
                 self.logger.warning(f"Risk check failed for {signal.signal_id}: Missing price for sizing.")
                 return None

            kelly_size_fraction = (edge / (volatility**2)) * self.kelly_fraction
            proposed_size_usd = available_balance * Decimal(str(kelly_size_fraction)) # Convert carefully

            # 5. Apply Constraints
            # Max position size (USD)
            sized_usd = min(proposed_size_usd, self.max_pos_size_usd)
            # Max total exposure
            sized_usd = min(sized_usd, self.max_total_exposure - current_exposure)
            # Avoid increasing position beyond max if already exists
            if current_position and current_position.side == signal.side:
                current_pos_usd = current_position.size * current_position.entry_price # Approximate
                sized_usd = min(sized_usd, self.max_pos_size_usd - current_pos_usd)

            # Handle reducing/closing existing opposite position? More complex.
            # Add drawdown checks, etc.

            # Convert USD size to quantity
            if sized_usd <= Decimal(0):
                self.logger.info(f"Risk check: Calculated size is zero or negative for {signal.signal_id}.")
                return None

            final_quantity = sized_usd / signal.price
            final_quantity = final_quantity.quantize(Decimal("0.000001")) # Example rounding

            # Check minimum order size
            min_order_size = Decimal(self.config.get(f"exchanges.{exchange}.min_order_size", "0.001"))
            if final_quantity < min_order_size:
                 self.logger.info(f"Risk check: Final quantity {final_quantity} below min size for {signal.signal_id}.")
                 return None

            # 6. Return Sized Signal (modify in place or create new?)
            sized_signal = dataclasses.replace(signal, quantity=final_quantity)
            self.logger.info(f"Risk check PASSED for {signal.signal_id}. Sized quantity: {final_quantity}")
            return sized_signal

    ```

*   **Configuration Example (`config.yaml`):**
    ```yaml
    risk_manager:
      # Global Limits
      max_total_exposure_usd: 50000 # Max total value of all positions
      max_drawdown_pct_global: 0.10 # 10% global portfolio drawdown limit triggers reduced sizing / halt
      # Per-Position/Trade Limits
      max_position_size_usd: 10000 # Max USD value for any single position
      max_trade_size_usd: 5000   # Max USD value for any single trade/signal
      # Sizing Method (Example: Kelly)
      sizing_method: "kelly"
      kelly_fraction: 0.05 # Bet 5% of the calculated optimal Kelly fraction
      min_edge_pct: 0.001 # Minimum perceived edge (e.g., 0.1%) to consider trade
      volatility_lookback_period: "1h" # For volatility calculation (if done here)
      # Strategy Specific Overrides (Optional)
      strategy_overrides:
        funding_rate_arbitrage:
          max_position_size_usd: 15000 # Allow larger size for this specific strategy
          kelly_fraction: 0.08
    ```

*   **Observations & Strengths:**
    *   Centralizes risk validation logic.
    *   Configurable parameters allow tuning.
    *   Integrates portfolio state and circuit breakers.
*   **Concerns & Areas for Improvement:**
    *   **Complexity:** The combination of various limits and sizing calculations (Kelly, max caps, drawdown adjustments) can become very complex. Needs clear logic and extensive testing.
    *   **Data Dependency:** Highly dependent on accurate, low-latency data from `PortfolioTracker`. Stale data leads to incorrect risk assessment.
    *   **Float vs. Decimal:** Calculations involving percentages, Kelly fraction (float), and financial values (`Decimal`) require careful handling to maintain precision and avoid errors (`decimal.mdc`).
    *   **Testing:** Sizing logic is notoriously difficult to test. Requires mocking `PortfolioTracker` state and testing numerous edge cases.
*   **Recommendations:**
    *   **Unit Test Extensively:** Create comprehensive unit tests for `validate_and_size_signal` (or equivalent), covering different portfolio states, signal inputs, and configuration parameters. Test edge cases (zero balance, max exposure reached, zero volatility, etc.).
    *   **Decimal Hygiene:** Strictly adhere to `decimal.mdc` guidelines. Perform explicit conversions between `float` (e.g., Kelly fraction, volatility) and `Decimal` (financial values) only where necessary and document the rationale. Use `Decimal` for all intermediate and final financial calculations (size, exposure).
    *   **Simplify Logic:** If possible, simplify the interaction between different risk constraints. Document the exact order of application. Add detailed logging within the sizing steps.
    *   **Async Safety:** Ensure calls to `PortfolioTracker` getters are `await`ed correctly and handle the async nature.

## 6. `SignalQueue` (`cyberdelta/core/signal_queue.py`)

*   **Responsibility:** Store potential `TradeSignal` objects generated by strategies, prioritize them based on a utility score, manage their expiration, integrate with circuit breakers.
*   **Implementation Details:**
    *   Uses `heapq` on a list (`self.signal_queue`) storing tuples like `(-priority_score, timestamp, count, signal)`. Negative score turns min-heap into max-heap for priority. `count` ensures FIFO for same score/timestamp.
    *   `add_signal(signal: TradeSignal, priority_score: float)` adds a signal. Requires `priority_score` to be passed explicitly.
    *   `get_next_signal() -> TradeSignal | None` retrieves and removes the highest priority, *valid* (unexpired, passes post-get CB check) signal.
    *   `_clean_expired_signals()` removes signals where `signal.expiration < datetime.now(UTC)`.
    *   `_trim_queue()` removes lowest priority signals if `len(self.signal_queue) > self.max_queue_size`.
    *   `_calculate_expiration(signal)` determines expiration, potentially based on `signal.confidence`.
    *   Integrates `CircuitBreakerSystem` checks:
        *   `_check_circuit_breakers_pre_add`: Checks before adding a signal.
        *   `_check_circuit_breakers_post_get` / `_check_circuit_breakers`: Checks before returning a signal via `get_next_signal`. Requires inferring exchange from signal metadata or symbol.
    *   Uses `asyncio.Lock` (`_async_lock`) and potentially `threading.Lock` (`_sync_lock`) - focus should be on `asyncio.Lock` for the async engine core.

*   **Code Snippet (Conceptual Add & Get):**
    ```python
    # cyberdelta/core/signal_queue.py (Conceptual)
    import heapq
    from datetime import datetime, timedelta, UTC
    import asyncio
    from collections import Counter # For FIFO tie-breaking

    class PrioritySignalQueue:
        def __init__(self, config, circuit_breaker_system):
            self.max_queue_size = config.get("signal_queue.max_size", 1000)
            self.default_expiration_seconds = config.get("signal_queue.default_expiration_seconds", 60)
            # ... other config ...
            self.signal_queue: list[tuple[float, datetime, int, TradeSignal]] = [] # Min-heap (using negative priority)
            self.circuit_breaker_system = circuit_breaker_system
            self._lock = asyncio.Lock() # Primary lock for async operations
            self._counter = Counter() # Simple counter for FIFO tie-breaking

        async def add_signal(self, signal: TradeSignal, priority_score: float):
            async with self._lock:
                # 1. Pre-add Circuit Breaker Check
                if not await self._check_circuit_breakers(signal, is_pre_add=True):
                    self.logger.warning(f"Signal {signal.signal_id} rejected pre-add due to circuit breaker.")
                    return

                # 2. Calculate Expiration if needed
                if signal.expiration is None:
                    signal.expiration = self._calculate_expiration(signal)

                # 3. Check if expired before even adding
                if datetime.now(UTC) >= signal.expiration:
                    self.logger.debug(f"Signal {signal.signal_id} already expired before adding.")
                    return

                # 4. Add to Heap
                # Use negative score for max-heap behavior, timestamp for secondary sort, counter for FIFO
                timestamp = signal.timestamp or datetime.now(UTC)
                count = next(self._counter)
                heap_item = (-priority_score, timestamp, count, signal)
                heapq.heappush(self.signal_queue, heap_item)
                self.logger.debug(f"Added signal {signal.signal_id} with priority {-heap_item[0]}")

                # 5. Trim Queue if over size
                self._trim_queue() # No lock needed, called within locked context

        async def get_next_signal(self) -> TradeSignal | None:
            async with self._lock:
                while self.signal_queue:
                    # 1. Peek highest priority
                    neg_score, timestamp, count, signal = self.signal_queue[0] # Peek

                    # 2. Check Expiration
                    if datetime.now(UTC) >= signal.expiration:
                        heapq.heappop(self.signal_queue) # Remove expired
                        self.logger.debug(f"Removed expired signal {signal.signal_id} from queue head.")
                        continue # Try next signal

                    # 3. Post-get Circuit Breaker Check
                    if not await self._check_circuit_breakers(signal, is_pre_add=False):
                        heapq.heappop(self.signal_queue) # Remove blocked signal
                        self.logger.warning(f"Signal {signal.signal_id} removed post-get due to circuit breaker.")
                        continue # Try next signal

                    # 4. Valid signal found - pop and return
                    heapq.heappop(self.signal_queue)
                    self.logger.info(f"Returning signal {signal.signal_id} with priority {-neg_score}")
                    return signal
                else:
                    # Queue is empty or only contains expired/blocked signals
                    return None

        def _calculate_expiration(self, signal: TradeSignal) -> datetime:
            # Example: Longer expiration for higher confidence
            confidence = signal.confidence or 0.5 # Default confidence
            base_seconds = self.default_expiration_seconds
            # Scale duration based on confidence (e.g., 0->0.5x, 0.5->1x, 1->1.5x)
            duration_scale = 0.5 + confidence
            actual_seconds = base_seconds * duration_scale
            return datetime.now(UTC) + timedelta(seconds=actual_seconds)

        def _trim_queue(self):
            # Assumes called within lock
            while len(self.signal_queue) > self.max_queue_size:
                 heapq.heappop(self.signal_queue) # Removes lowest priority (highest neg_score)

        async def _check_circuit_breakers(self, signal: TradeSignal, is_pre_add: bool) -> bool:
             # Logic to infer exchange(s) from signal.metadata or signal.symbol
             # Call self.circuit_breaker_system.can_execute(exchange, symbol)
             # Return True if allowed, False if blocked
             # ... implementation from previous review ...
             return True # Placeholder

        # ... other methods like peek, count, clear ...

    ```
*   **Configuration Example (`config.yaml`):**
    ```yaml
    signal_queue:
      max_size: 1000 # Max signals to hold
      default_expiration_seconds: 30 # Base lifetime for signals without explicit expiration
      cleanup_interval_seconds: 10 # How often to run _clean_expired_signals (if run as background task)
      # Configuration for inferring exchange if not in metadata
      exchange_inference:
        enabled: true
        symbol_patterns:
          - pattern: ".*-PERP" # Matches HYPERLIQUID-BTC-PERP
            exchange: "hyperliquid"
          - pattern: ".*-(SPOT|PERP)" # Example if Backpack uses this
            exchange: "backpack"
          - pattern: "\w+_\w+" # Example: BTC_BACKPACK
            exchange_index: 1 # Infer from second part
            target_exchange: "backpack"
          - pattern: "\w+:\w+" # Example: BTC:DYDX
            exchange_index: 1
            target_exchange: "dydx"
    ```

*   **Observations & Strengths:**
    *   Efficient priority handling using `heapq`.
    *   Includes expiration and queue size management.
    *   Integrates circuit breaker safety checks.
*   **Concerns & Areas for Improvement:**
    *   **Input Data:** Relies on `priority_score` being passed in `add_signal` and potentially `confidence` and exchange info in `signal.metadata` for expiration/CB checks. Ensure the calling component (`Strategy`) provides this consistently.
    *   **Async Focus:** Remove `threading.Lock` (`_sync_lock`) if the engine is purely async. Ensure all relevant operations acquire the `_lock`.
    *   **Heap Consistency:** Previous review noted potential use of `signal_heap` vs `signal_queue`. Ensure only `self.signal_queue` is used.
    *   **Exchange Inference:** The logic in `_infer_exchange_from_symbol` (needed for `_check_circuit_breakers`) needs to be robust and configurable to handle different symbol formats accurately.
*   **Recommendations:**
    *   **Standardize Input:** Document that `add_signal` requires a `TradeSignal` and a `float` priority score. Ensure strategy provides `metadata` including confidence and exchange hints if possible.
    *   **Consolidate Locks:** Use only `asyncio.Lock` (`_lock`) for all async operations.
    *   **Verify Heap Usage:** Confirm only one heap structure (`self.signal_queue`) is used throughout the class.
    *   **Refine Exchange Inference:** Make the inference logic in `_check_circuit_breakers` more robust, potentially driven by patterns in `config.yaml` as shown in the example.

